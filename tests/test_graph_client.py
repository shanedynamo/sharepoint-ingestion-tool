"""Tests for the Microsoft Graph API client."""

import sys
import time
from unittest.mock import MagicMock, patch, call

import pytest
import requests

# Ensure src/ is on the path so bare imports work like they do at runtime
sys.path.insert(0, "src")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ok_response(json_data, status=200, headers=None):
    """Build a mock requests.Response that behaves like a successful call."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.json.return_value = json_data
    resp.headers = headers or {}
    resp.raise_for_status = MagicMock()
    return resp


def _make_graph_item(*, item_id, name, is_folder=False, size=1024, etag="etag-1",
                     mime="application/pdf", download_url="https://dl.example.com/f",
                     parent_path="/drives/d1/root:/General",
                     author="Alice", created="2024-01-01T00:00:00Z",
                     modified="2024-06-01T00:00:00Z"):
    """Build a realistic Graph drive-item dict."""
    item = {
        "id": item_id,
        "name": name,
        "size": size,
        "eTag": etag,
        "createdDateTime": created,
        "lastModifiedDateTime": modified,
        "createdBy": {"user": {"displayName": author}},
        "parentReference": {"path": parent_path},
    }
    if is_folder:
        item["folder"] = {"childCount": 0}
    else:
        item["file"] = {"mimeType": mime}
        item["@microsoft.graph.downloadUrl"] = download_url
    return item


# ---------------------------------------------------------------------------
# Fixture: a GraphClient with MSAL fully mocked
# ---------------------------------------------------------------------------

@pytest.fixture
def graph_client():
    with patch("graph_client.msal.ConfidentialClientApplication") as mock_app_cls:
        mock_app_cls.return_value.acquire_token_for_client.return_value = {
            "access_token": "fake-token",
            "expires_in": 3600,
        }
        from graph_client import GraphClient
        client = GraphClient(
            client_id="test-id",
            tenant_id="test-tenant",
            client_secret="test-secret",
        )
        yield client


# ===================================================================
# Authentication
# ===================================================================

class TestAuthentication:
    def test_token_acquired_on_init(self, graph_client):
        assert graph_client._token == "fake-token"

    def test_token_expiry_tracked(self, graph_client):
        assert graph_client._token_expires_at > time.monotonic()

    def test_ensure_token_returns_cached(self, graph_client):
        """Calling _ensure_token before expiry returns the cached token."""
        token = graph_client._ensure_token()
        assert token == "fake-token"

    def test_ensure_token_refreshes_when_expired(self, graph_client):
        """Force expiry and verify a new token is acquired."""
        graph_client._token_expires_at = 0  # expired
        graph_client._app.acquire_token_for_client.return_value = {
            "access_token": "new-token",
            "expires_in": 3600,
        }
        token = graph_client._ensure_token()
        assert token == "new-token"

    def test_acquire_token_raises_on_failure(self, graph_client):
        graph_client._app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "bad creds",
        }
        with pytest.raises(RuntimeError, match="bad creds"):
            graph_client._acquire_token()

    def test_headers_include_bearer(self, graph_client):
        headers = graph_client._headers
        assert headers["Authorization"] == "Bearer fake-token"


# ===================================================================
# _get – low-level HTTP
# ===================================================================

class TestGetMethod:
    @patch("graph_client.requests.get")
    def test_returns_json(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({"value": [1, 2]})
        result = graph_client._get("https://example.com/api")
        assert result == {"value": [1, 2]}

    @patch("graph_client.requests.get")
    def test_auto_refreshes_on_401(self, mock_get, graph_client):
        """First call returns 401, second (after refresh) returns 200."""
        resp_401 = MagicMock(spec=requests.Response)
        resp_401.status_code = 401
        resp_401.raise_for_status = MagicMock()

        resp_200 = _ok_response({"ok": True})

        mock_get.side_effect = [resp_401, resp_200]
        graph_client._app.acquire_token_for_client.return_value = {
            "access_token": "refreshed-token",
            "expires_in": 3600,
        }

        result = graph_client._get("https://example.com/api")
        assert result == {"ok": True}
        assert graph_client._token == "refreshed-token"


# ===================================================================
# get_site_id
# ===================================================================

class TestGetSiteId:
    @patch("graph_client.requests.get")
    def test_returns_site_id(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [{"id": "site-123", "displayName": "Dynamo"}]
        })
        site_id = graph_client.get_site_id("Dynamo")
        assert site_id == "site-123"

    @patch("graph_client.requests.get")
    def test_caches_result(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [{"id": "site-123"}]
        })
        graph_client.get_site_id("Dynamo")
        graph_client.get_site_id("Dynamo")
        # Only one HTTP call despite two lookups
        mock_get.assert_called_once()

    @patch("graph_client.requests.get")
    def test_raises_when_not_found(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({"value": []})
        with pytest.raises(RuntimeError, match="not found"):
            graph_client.get_site_id("NonExistent")

    @patch("graph_client.requests.get")
    def test_defaults_to_config_site_name(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [{"id": "site-cfg"}]
        })
        graph_client.get_site_id()  # no argument
        call_url = mock_get.call_args[0][0]
        assert "/sites" in call_url


# ===================================================================
# list_document_libraries
# ===================================================================

class TestListDocumentLibraries:
    @patch("graph_client.requests.get")
    def test_returns_document_libraries(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [
                {"id": "d1", "name": "Documents", "webUrl": "https://sp/docs", "driveType": "documentLibrary"},
                {"id": "d2", "name": "Site Assets", "webUrl": "https://sp/assets", "driveType": "documentLibrary"},
            ]
        })
        libs = graph_client.list_document_libraries("site-1")
        assert len(libs) == 2
        assert libs[0] == {"id": "d1", "name": "Documents", "webUrl": "https://sp/docs"}

    @patch("graph_client.requests.get")
    def test_filters_system_drives(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [
                {"id": "d1", "name": "Documents", "webUrl": "", "driveType": "documentLibrary"},
                {"id": "d2", "name": "Personal", "webUrl": "", "driveType": "personal"},
            ]
        })
        libs = graph_client.list_document_libraries("site-1")
        assert len(libs) == 1
        assert libs[0]["name"] == "Documents"


# ===================================================================
# crawl_library
# ===================================================================

class TestCrawlLibrary:
    @patch("graph_client.requests.get")
    def test_yields_supported_files(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [
                _make_graph_item(item_id="f1", name="report.pdf"),
                _make_graph_item(item_id="f2", name="notes.txt"),
                _make_graph_item(item_id="f3", name="image.png"),  # unsupported
            ]
        })

        docs = list(graph_client.crawl_library("drive-1"))
        names = [d["name"] for d in docs]
        assert "report.pdf" in names
        assert "notes.txt" in names
        assert "image.png" not in names

    @patch("graph_client.requests.get")
    def test_yields_correct_fields(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [
                _make_graph_item(
                    item_id="f1", name="report.pdf", size=2048,
                    author="Bob", download_url="https://dl/report",
                ),
            ]
        })

        docs = list(graph_client.crawl_library("drive-1", library_name="Documents"))
        assert len(docs) == 1
        doc = docs[0]

        assert doc["id"] == "f1"
        assert doc["name"] == "report.pdf"
        assert doc["file_type"] == ".pdf"
        assert doc["size"] == 2048
        assert doc["sharepoint_path"] == "/report.pdf"
        assert doc["author"] == "Bob"
        assert doc["download_url"] == "https://dl/report"
        assert doc["library_name"] == "Documents"
        assert doc["site_name"] == "Dynamo"

    @patch("graph_client.requests.get")
    def test_recurses_into_subfolders(self, mock_get, graph_client):
        """Root lists a folder; that folder contains a file."""
        root_resp = _ok_response({
            "value": [
                _make_graph_item(item_id="folder-1", name="Reports", is_folder=True),
            ]
        })
        sub_resp = _ok_response({
            "value": [
                _make_graph_item(item_id="f1", name="Q4.pdf"),
            ]
        })
        mock_get.side_effect = [root_resp, sub_resp]

        docs = list(graph_client.crawl_library("drive-1"))
        assert len(docs) == 1
        assert docs[0]["sharepoint_path"] == "/Reports/Q4.pdf"

    @patch("graph_client.requests.get")
    def test_skips_excluded_folders(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [
                _make_graph_item(item_id="folder-d", name="Drafts", is_folder=True),
                _make_graph_item(item_id="f1", name="final.docx"),
            ]
        })

        docs = list(graph_client.crawl_library("drive-1"))
        # Only the file, not the excluded folder's contents
        assert len(docs) == 1
        assert docs[0]["name"] == "final.docx"

    @patch("graph_client.requests.get")
    def test_handles_pagination(self, mock_get, graph_client):
        """Two pages of results are concatenated."""
        page1 = _ok_response({
            "value": [_make_graph_item(item_id="f1", name="a.pdf")],
            "@odata.nextLink": "https://graph.microsoft.com/next-page",
        })
        page2 = _ok_response({
            "value": [_make_graph_item(item_id="f2", name="b.docx")],
        })
        mock_get.side_effect = [page1, page2]

        docs = list(graph_client.crawl_library("drive-1"))
        assert len(docs) == 2

    @patch("graph_client.requests.get")
    def test_skips_non_file_non_folder_items(self, mock_get, graph_client):
        """Items without 'file' or 'folder' keys are ignored."""
        mock_get.return_value = _ok_response({
            "value": [
                {"id": "x", "name": "weird-item"},  # no file/folder key
                _make_graph_item(item_id="f1", name="ok.pdf"),
            ]
        })
        docs = list(graph_client.crawl_library("drive-1"))
        assert len(docs) == 1


# ===================================================================
# download_file
# ===================================================================

class TestDownloadFile:
    @patch("graph_client.requests.get")
    @patch("graph_client.requests.head")
    def test_small_file_direct_download(self, mock_head, mock_get, graph_client):
        mock_head.return_value = _ok_response({}, headers={"Content-Length": "500"})
        mock_resp = MagicMock()
        mock_resp.content = b"file-bytes"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        data = graph_client.download_file("https://dl.example.com/small.pdf")
        assert data == b"file-bytes"

    @patch("graph_client.requests.get")
    @patch("graph_client.requests.head")
    def test_large_file_streamed(self, mock_head, mock_get, graph_client):
        """Files >10 MB use streaming download."""
        mock_head.return_value = _ok_response(
            {}, headers={"Content-Length": str(20 * 1024 * 1024)}
        )

        # Simulate streaming context manager
        mock_stream_resp = MagicMock()
        mock_stream_resp.__enter__ = MagicMock(return_value=mock_stream_resp)
        mock_stream_resp.__exit__ = MagicMock(return_value=False)
        mock_stream_resp.raise_for_status = MagicMock()
        mock_stream_resp.iter_content.return_value = [b"chunk1", b"chunk2"]
        mock_get.return_value = mock_stream_resp

        data = graph_client.download_file("https://dl.example.com/big.pdf")
        assert data == b"chunk1chunk2"
        mock_get.assert_called_once_with(
            "https://dl.example.com/big.pdf",
            stream=True, timeout=300, allow_redirects=True,
        )


# ===================================================================
# get_delta
# ===================================================================

class TestGetDelta:
    @patch("graph_client.requests.get")
    def test_initial_delta_no_token(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [
                {"id": "item-1", "name": "new.pdf", "file": {"mimeType": "application/pdf"}},
            ],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/d1/root/delta?token=abc123",
        })

        changes, token = graph_client.get_delta("d1")
        assert len(changes) == 1
        assert token == "abc123"

        # URL should NOT contain ?token= for initial call
        called_url = mock_get.call_args[0][0]
        assert "?token=" not in called_url

    @patch("graph_client.requests.get")
    def test_delta_with_existing_token(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [{"id": "item-2", "name": "changed.docx"}],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/d1/root/delta?token=def456",
        })

        changes, token = graph_client.get_delta("d1", delta_token="prev-token")
        assert len(changes) == 1
        assert token == "def456"

        called_url = mock_get.call_args[0][0]
        assert "?token=prev-token" in called_url

    @patch("graph_client.requests.get")
    def test_delta_with_pagination(self, mock_get, graph_client):
        page1 = _ok_response({
            "value": [{"id": "i1"}],
            "@odata.nextLink": "https://graph.microsoft.com/next",
        })
        page2 = _ok_response({
            "value": [{"id": "i2"}],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/d1/root/delta?token=final",
        })
        mock_get.side_effect = [page1, page2]

        changes, token = graph_client.get_delta("d1")
        assert len(changes) == 2
        assert token == "final"

    @patch("graph_client.requests.get")
    def test_delta_includes_deleted_items(self, mock_get, graph_client):
        mock_get.return_value = _ok_response({
            "value": [
                {"id": "item-del", "deleted": {"state": "deleted"}},
                {"id": "item-ok", "name": "alive.pdf"},
            ],
            "@odata.deltaLink": "https://graph.microsoft.com/v1.0/drives/d1/root/delta?token=t1",
        })

        changes, _ = graph_client.get_delta("d1")
        deleted = [c for c in changes if "deleted" in c]
        assert len(deleted) == 1
        assert deleted[0]["id"] == "item-del"


# ===================================================================
# _extract_token
# ===================================================================

class TestExtractToken:
    def test_parses_token_from_url(self, graph_client):
        link = "https://graph.microsoft.com/v1.0/drives/d1/root/delta?token=abc123"
        assert graph_client._extract_token(link) == "abc123"

    def test_returns_full_link_as_fallback(self, graph_client):
        link = "https://graph.microsoft.com/v1.0/drives/d1/root/delta"
        assert graph_client._extract_token(link) == link


# ===================================================================
# Throttle / retry_with_backoff
# ===================================================================

class TestRetryWithBackoff:
    @patch("utils.throttle.time.sleep")
    def test_retries_on_429(self, mock_sleep):
        from utils.throttle import retry_with_backoff

        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=1.0)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                resp = MagicMock()
                resp.status_code = 429
                resp.headers = {}
                exc = requests.exceptions.HTTPError(response=resp)
                raise exc
            return "ok"

        assert flaky() == "ok"
        assert call_count == 3
        assert mock_sleep.call_count == 2

    @patch("utils.throttle.time.sleep")
    def test_uses_retry_after_header(self, mock_sleep):
        from utils.throttle import retry_with_backoff

        call_count = 0

        @retry_with_backoff(max_retries=2, base_delay=1.0)
        def throttled():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                resp = MagicMock()
                resp.status_code = 429
                resp.headers = {"Retry-After": "7"}
                raise requests.exceptions.HTTPError(response=resp)
            return "done"

        assert throttled() == "done"
        mock_sleep.assert_called_once_with(7.0)

    @patch("utils.throttle.time.sleep")
    def test_raises_after_max_retries(self, mock_sleep):
        from utils.throttle import retry_with_backoff

        @retry_with_backoff(max_retries=2, base_delay=0.1)
        def always_fails():
            resp = MagicMock()
            resp.status_code = 429
            resp.headers = {}
            raise requests.exceptions.HTTPError(response=resp)

        with pytest.raises(requests.exceptions.HTTPError):
            always_fails()
        # 2 retries = 2 sleeps (attempt 0 fails -> sleep -> attempt 1 fails -> sleep -> attempt 2 fails -> raise)
        assert mock_sleep.call_count == 2

    def test_does_not_retry_non_throttle_errors(self):
        from utils.throttle import retry_with_backoff

        @retry_with_backoff(max_retries=3)
        def bad_request():
            resp = MagicMock()
            resp.status_code = 400
            resp.headers = {}
            raise requests.exceptions.HTTPError(response=resp)

        with pytest.raises(requests.exceptions.HTTPError):
            bad_request()

    @patch("utils.throttle.time.sleep")
    def test_exponential_backoff_delays(self, mock_sleep):
        from utils.throttle import retry_with_backoff

        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=1.0)
        def slow():
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                resp = MagicMock()
                resp.status_code = 503
                resp.headers = {}
                raise requests.exceptions.HTTPError(response=resp)
            return "finally"

        assert slow() == "finally"
        delays = [c[0][0] for c in mock_sleep.call_args_list]
        assert delays == [1.0, 2.0, 4.0]  # 1*2^0, 1*2^1, 1*2^2
