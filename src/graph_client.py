"""Microsoft Graph API client for SharePoint access.

Authenticates with MSAL client-credentials flow and exposes methods for
site discovery, library listing, recursive crawling, file download, and
delta-based incremental sync.
"""

import logging
import os
import time
from collections.abc import Generator
from typing import Any
from urllib.parse import parse_qs, urlparse

import msal
import requests

from config import config
from utils.throttle import retry_with_backoff

logger = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

SUPPORTED_EXTENSIONS = {".docx", ".pdf", ".pptx", ".xlsx", ".doc", ".txt"}

STREAM_THRESHOLD = 10 * 1024 * 1024  # 10 MB
STREAM_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB chunks


class GraphClient:
    """Microsoft Graph client using client-credentials (app-only) auth."""

    # ------------------------------------------------------------------
    # Construction & authentication
    # ------------------------------------------------------------------

    def __init__(
        self,
        client_id: str | None = None,
        tenant_id: str | None = None,
        client_secret: str | None = None,
    ):
        self._client_id = client_id or config.azure_client_id
        self._tenant_id = tenant_id or config.azure_tenant_id
        self._client_secret = client_secret or config.azure_client_secret

        self._app = msal.ConfidentialClientApplication(
            client_id=self._client_id,
            client_credential=self._client_secret,
            authority=f"https://login.microsoftonline.com/{self._tenant_id}",
        )

        self._token: str | None = None
        self._token_expires_at: float = 0.0

        # Caches
        self._site_id_cache: dict[str, str] = {}

        # Eagerly acquire the first token
        self._acquire_token()

    def _acquire_token(self) -> str:
        """Acquire (or refresh) an access token via the client-credentials flow."""
        result = self._app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )
        if "access_token" not in result:
            raise RuntimeError(
                f"Token acquisition failed: {result.get('error_description', result)}"
            )

        self._token = result["access_token"]
        # expires_in is in seconds; shave 60 s to refresh before actual expiry
        self._token_expires_at = time.monotonic() + result.get("expires_in", 3600) - 60
        logger.debug("Acquired Graph API token (expires in %ss)", result.get("expires_in"))
        return self._token

    def _ensure_token(self) -> str:
        """Return a valid token, refreshing if it has expired."""
        if self._token is None or time.monotonic() >= self._token_expires_at:
            return self._acquire_token()
        return self._token

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._ensure_token()}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Low-level HTTP with auto-refresh on 401
    # ------------------------------------------------------------------

    @retry_with_backoff()
    def _get(self, url: str, params: dict | None = None) -> dict[str, Any]:
        """GET with auto-retry on 429/503 and auto-refresh on 401."""
        resp = requests.get(url, headers=self._headers, params=params, timeout=30)

        if resp.status_code == 401:
            self._acquire_token()
            resp = requests.get(url, headers=self._headers, params=params, timeout=30)

        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Site discovery
    # ------------------------------------------------------------------

    def get_site_id(self, site_name: str | None = None) -> str:
        """Look up a SharePoint site by display name.  Result is cached.

        Args:
            site_name: Search term passed to ``/sites?search=``.
                       Defaults to ``config.sharepoint_site_name``.
        """
        site_name = site_name or config.sharepoint_site_name

        if site_name in self._site_id_cache:
            return self._site_id_cache[site_name]

        data = self._get(f"{GRAPH_BASE}/sites", params={"search": site_name})
        sites = data.get("value", [])
        if not sites:
            raise RuntimeError(f"SharePoint site '{site_name}' not found")

        site_id = sites[0]["id"]
        self._site_id_cache[site_name] = site_id
        logger.info("Resolved site '%s' -> %s", site_name, site_id)
        return site_id

    # ------------------------------------------------------------------
    # Document libraries
    # ------------------------------------------------------------------

    def list_document_libraries(self, site_id: str) -> list[dict]:
        """Return non-system document libraries for a site.

        Each dict contains: ``id``, ``name``, ``webUrl``.
        """
        data = self._get(f"{GRAPH_BASE}/sites/{site_id}/drives")
        drives = data.get("value", [])

        libraries = []
        for drive in drives:
            # Skip system / hidden libraries
            drive_type = drive.get("driveType", "")
            if drive_type not in ("documentLibrary", ""):
                continue

            libraries.append({
                "id": drive["id"],
                "name": drive.get("name", ""),
                "webUrl": drive.get("webUrl", ""),
            })

        logger.info("Found %d document libraries on site %s", len(libraries), site_id)
        return libraries

    # ------------------------------------------------------------------
    # Recursive crawl
    # ------------------------------------------------------------------

    def crawl_library(
        self,
        drive_id: str,
        folder_path: str = "/",
        *,
        site_name: str | None = None,
        library_name: str | None = None,
    ) -> Generator[dict, None, None]:
        """Recursively crawl a document library and yield metadata for each document.

        Yields a dict per file with keys:
            id, name, file_type, size, sharepoint_path, last_modified,
            created, author, download_url, library_name, site_name

        Folders listed in ``config.excluded_folders`` are skipped.
        Only files with extensions in ``SUPPORTED_EXTENSIONS`` are yielded.
        """
        site_name = site_name or config.sharepoint_site_name
        library_name = library_name or ""

        yield from self._crawl_folder(
            drive_id=drive_id,
            item_id="root",
            current_path=folder_path.rstrip("/"),
            site_name=site_name,
            library_name=library_name,
        )

    def _crawl_folder(
        self,
        drive_id: str,
        item_id: str,
        current_path: str,
        site_name: str,
        library_name: str,
    ) -> Generator[dict, None, None]:
        """Internal recursive helper — paginate children and descend into folders."""
        if item_id == "root":
            url: str | None = f"{GRAPH_BASE}/drives/{drive_id}/root/children"
        else:
            url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/children"

        while url:
            data = self._get(url)

            for item in data.get("value", []):
                name = item.get("name", "")
                child_path = f"{current_path}/{name}"

                # --- Folder handling ---
                if "folder" in item:
                    if name in config.excluded_folders:
                        logger.info("Skipping excluded folder: %s", child_path)
                        continue
                    # Recurse into subfolder (folders are NOT yielded)
                    yield from self._crawl_folder(
                        drive_id=drive_id,
                        item_id=item["id"],
                        current_path=child_path,
                        site_name=site_name,
                        library_name=library_name,
                    )
                    continue

                # --- File handling ---
                if "file" not in item:
                    continue

                ext = os.path.splitext(name)[1].lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    logger.debug("Skipping unsupported extension %s: %s", ext, child_path)
                    continue

                created_by = item.get("createdBy", {}).get("user", {})

                yield {
                    "id": item["id"],
                    "name": name,
                    "file_type": ext,
                    "size": item.get("size", 0),
                    "sharepoint_path": child_path,
                    "last_modified": item.get("lastModifiedDateTime", ""),
                    "created": item.get("createdDateTime", ""),
                    "author": created_by.get("displayName", created_by.get("email", "")),
                    "download_url": item.get("@microsoft.graph.downloadUrl", ""),
                    "etag": item.get("eTag", ""),
                    "content_type": item.get("file", {}).get("mimeType", "application/octet-stream"),
                    "library_name": library_name,
                    "site_name": site_name,
                }

            url = data.get("@odata.nextLink")

    # ------------------------------------------------------------------
    # File download
    # ------------------------------------------------------------------

    def download_file(self, download_url: str) -> bytes:
        """Download file content from a Graph ``@microsoft.graph.downloadUrl``.

        Files larger than 10 MB are streamed in chunks to avoid holding
        the full payload in memory during the HTTP transfer.
        """
        # HEAD to decide strategy
        head = requests.head(download_url, timeout=30, allow_redirects=True)
        head.raise_for_status()
        content_length = int(head.headers.get("Content-Length", 0))

        if content_length > STREAM_THRESHOLD:
            logger.info("Streaming large file (%d bytes)", content_length)
            return self._download_streamed(download_url)

        resp = requests.get(download_url, timeout=120, allow_redirects=True)
        resp.raise_for_status()
        return resp.content

    @staticmethod
    def _download_streamed(url: str) -> bytes:
        chunks: list[bytes] = []
        with requests.get(url, stream=True, timeout=300, allow_redirects=True) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=STREAM_CHUNK_SIZE):
                chunks.append(chunk)
        return b"".join(chunks)

    # ------------------------------------------------------------------
    # Delta (incremental sync)
    # ------------------------------------------------------------------

    def get_delta(
        self,
        drive_id: str,
        delta_token: str | None = None,
    ) -> tuple[list[dict], str]:
        """Fetch incremental changes via the Graph delta API.

        Args:
            drive_id: The drive to query.
            delta_token: Opaque token from a previous delta response.
                         ``None`` requests a full initial delta.

        Returns:
            ``(changes, new_delta_token)`` where *changes* is a flat list
            of every changed item (with ``"deleted"`` key for removals)
            and *new_delta_token* is the token to pass on the next call.
        """
        if delta_token:
            url: str | None = (
                f"{GRAPH_BASE}/drives/{drive_id}/root/delta?token={delta_token}"
            )
        else:
            url = f"{GRAPH_BASE}/drives/{drive_id}/root/delta"

        changes: list[dict] = []
        new_delta_token = ""

        while url:
            data = self._get(url)
            changes.extend(data.get("value", []))

            # Follow pagination
            url = data.get("@odata.nextLink")

            # When there are no more pages, Graph returns a deltaLink
            delta_link = data.get("@odata.deltaLink", "")
            if delta_link:
                new_delta_token = self._extract_token(delta_link)

        return changes, new_delta_token

    @staticmethod
    def _extract_token(delta_link: str) -> str:
        """Pull the bare ``token=`` value from a full deltaLink URL."""
        parsed = urlparse(delta_link)
        qs = parse_qs(parsed.query)
        tokens = qs.get("token", [])
        return tokens[0] if tokens else delta_link
