"""Translate SharePoint paths to S3 keys while preserving hierarchy.

Key format:
    source/{site}/{library}/{relative_path}
    extracted/{site}/{library}/{relative_path_stem}.json
"""

import os
import re
from urllib.parse import quote

# S3 limits
S3_TAG_VALUE_MAX = 256
S3_KEY_MAX_BYTES = 1024


def _sanitize_component(component: str) -> str:
    """Sanitize a single path component (file or folder name).

    - Replace spaces with hyphens.
    - Strip characters that are not alphanumeric, hyphens, underscores,
      or periods.
    - Collapse consecutive hyphens.
    """
    component = component.replace(" ", "-")
    component = re.sub(r"[^\w.\-]", "", component)
    component = re.sub(r"-{2,}", "-", component)
    return component


def _sanitize_path(path: str) -> str:
    """Sanitize a full relative path, preserving ``/`` separators.

    - Strip leading/trailing slashes.
    - Collapse double (or more) slashes.
    - Sanitize each component individually.
    """
    # Collapse multiple slashes, then strip edges
    path = re.sub(r"/{2,}", "/", path)
    path = path.strip("/")

    if not path:
        return ""

    parts = path.split("/")
    sanitized = "/".join(_sanitize_component(p) for p in parts if p)
    return sanitized


class PathMapper:
    """Bidirectional mapper between SharePoint paths and S3 keys."""

    def __init__(
        self,
        bucket: str,
        source_prefix: str = "source",
        extracted_prefix: str = "extracted",
    ):
        self.bucket = bucket
        self.source_prefix = source_prefix
        self.extracted_prefix = extracted_prefix

    # ------------------------------------------------------------------
    # SharePoint -> S3
    # ------------------------------------------------------------------

    def to_s3_source_key(
        self,
        site_name: str,
        library_name: str,
        sharepoint_path: str,
    ) -> str:
        """Build an S3 source key from SharePoint coordinates.

        Example:
            site="Dynamo", library="HR-Policies",
            path="/2025/Employee-Handbook.docx"
          → "source/Dynamo/HR-Policies/2025/Employee-Handbook.docx"
        """
        site_part = _sanitize_component(site_name)
        lib_part = _sanitize_component(library_name)
        path_part = _sanitize_path(sharepoint_path)

        segments = [self.source_prefix, site_part, lib_part]
        if path_part:
            segments.append(path_part)

        key = "/".join(segments)

        # Guard against exceeding S3 key limit (1024 bytes)
        key_bytes = key.encode("utf-8")
        if len(key_bytes) > S3_KEY_MAX_BYTES:
            # Truncate the path portion to fit
            overhead = len(f"{self.source_prefix}/{site_part}/{lib_part}/".encode("utf-8"))
            max_path_bytes = S3_KEY_MAX_BYTES - overhead
            truncated = key_bytes[:overhead + max_path_bytes].decode("utf-8", errors="ignore")
            key = truncated

        return key

    def to_s3_extracted_key(self, source_key: str) -> str:
        """Swap prefix from source to extracted and change extension to .json.

        Example:
            "source/Dynamo/HR-Policies/2025/Employee-Handbook.docx"
          → "extracted/Dynamo/HR-Policies/2025/Employee-Handbook.json"
        """
        # Strip the source prefix
        if source_key.startswith(self.source_prefix + "/"):
            relative = source_key[len(self.source_prefix) + 1:]
        else:
            relative = source_key

        # Replace extension with .json (or append .json if no extension)
        root, ext = os.path.splitext(relative)
        if ext:
            relative = root + ".json"
        else:
            relative = relative + ".json"

        return f"{self.extracted_prefix}/{relative}"

    # ------------------------------------------------------------------
    # S3 -> SharePoint (reverse mapping)
    # ------------------------------------------------------------------

    def source_key_to_sharepoint_path(
        self,
        source_key: str,
    ) -> tuple[str, str, str]:
        """Extract site name, library name, and relative path from an S3 source key.

        Example:
            "source/Dynamo/HR-Policies/2025/Employee-Handbook.docx"
          → ("Dynamo", "HR-Policies", "2025/Employee-Handbook.docx")

        Returns:
            (site_name, library_name, relative_path)

        Raises:
            ValueError: If the key does not have enough segments.
        """
        if source_key.startswith(self.source_prefix + "/"):
            remainder = source_key[len(self.source_prefix) + 1:]
        else:
            remainder = source_key

        parts = remainder.split("/", 2)

        if len(parts) < 2:
            raise ValueError(
                f"Cannot parse S3 key '{source_key}': expected at least "
                f"'{self.source_prefix}/{{site}}/{{library}}/...'"
            )

        site_name = parts[0]
        library_name = parts[1]
        relative_path = parts[2] if len(parts) > 2 else ""

        return site_name, library_name, relative_path

    # ------------------------------------------------------------------
    # S3 object tags
    # ------------------------------------------------------------------

    @staticmethod
    def build_s3_tags(item: dict) -> dict[str, str]:
        """Build S3 object tags from a Graph API item dict.

        Produces up to 7 tags (within S3's 10-tag limit).  Values longer
        than 256 characters are truncated.

        Expected item keys (from ``GraphClient.crawl_library``):
            site_name, library_name, sharepoint_path, author,
            last_modified, content_type, file_type
        """
        def _truncate(value: str) -> str:
            if len(value) <= S3_TAG_VALUE_MAX:
                return value
            return value[: S3_TAG_VALUE_MAX - 3] + "..."

        sp_path = item.get("sharepoint_path", "")
        # URL-encode the path for safe storage as a tag value
        encoded_path = quote(sp_path, safe="/")

        tags = {
            "sp-site": _truncate(item.get("site_name", "")),
            "sp-library": _truncate(item.get("library_name", "")),
            "sp-path": _truncate(encoded_path),
            "sp-author": _truncate(item.get("author", "")),
            "sp-last-modified": _truncate(item.get("last_modified", "")),
            "sp-content-type": _truncate(item.get("content_type", "")),
            "file-type": _truncate(
                item.get("file_type", "").lstrip(".")
            ),
        }

        # Drop tags with empty values to stay clean
        return {k: v for k, v in tags.items() if v}
