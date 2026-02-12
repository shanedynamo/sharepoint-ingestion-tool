"""Tests for the daily sync Lambda handler."""

import json
import sys
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, "src")

MOCK_LIBRARIES = [{"id": "drive-1", "name": "Documents", "webUrl": "https://sp/docs"}]


def _setup_graph(mock_graph, delta_return):
    """Wire up the common graph mock methods."""
    mock_graph.get_site_id.return_value = "site-1"
    mock_graph.list_document_libraries.return_value = MOCK_LIBRARIES
    mock_graph.get_delta.return_value = delta_return


def _new_file_item(**overrides):
    """Return a Graph delta item representing a new or modified file."""
    base = {
        "id": "item-1",
        "name": "doc.pdf",
        "file": {"mimeType": "application/pdf"},
        "lastModifiedDateTime": "2025-06-01T10:00:00Z",
        "size": 2048,
        "parentReference": {"path": "/drives/drive-1/root:/General"},
        "@microsoft.graph.downloadUrl": "https://dl/doc.pdf",
    }
    base.update(overrides)
    return base


def _deleted_item(**overrides):
    """Return a Graph delta item representing a deletion."""
    base = {
        "id": "item-del",
        "name": "old-file.pdf",
        "deleted": {"state": "deleted"},
        "parentReference": {"path": "/drives/drive-1/root:/General"},
    }
    base.update(overrides)
    return base


def _patch_all():
    """Stack decorators for all daily_sync dependencies."""
    return [
        patch("daily_sync.PathMapper"),
        patch("daily_sync.DocumentRegistry"),
        patch("daily_sync.DeltaTracker"),
        patch("daily_sync.S3Client"),
        patch("daily_sync.GraphClient"),
    ]


class TestDailySyncHandler:
    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_creates_new_file(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        mock_graph = MockGraph.return_value
        _setup_graph(mock_graph, ([_new_file_item()], "new-token"))
        mock_graph.download_file.return_value = b"pdf-content"

        MockDelta.return_value.get_delta_token.return_value = None
        MockRegistry.return_value.get_document.return_value = None

        mock_mapper = MockMapper.return_value
        mock_mapper.to_s3_source_key.return_value = "source/Dynamo/Documents/General/doc.pdf"

        from daily_sync import handler
        result = handler({}, None)

        body = json.loads(result["body"])
        assert result["statusCode"] == 200
        assert body["created"] == 1
        assert body["updated"] == 0
        mock_graph.download_file.assert_called_once_with("https://dl/doc.pdf")
        MockRegistry.return_value.register_document.assert_called_once()
        MockDelta.return_value.save_delta_token.assert_called_once()

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_updates_existing_file(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        mock_graph = MockGraph.return_value
        _setup_graph(mock_graph, (
            [_new_file_item(lastModifiedDateTime="2025-06-02T10:00:00Z")],
            "new-token",
        ))
        mock_graph.download_file.return_value = b"updated"

        MockDelta.return_value.get_delta_token.return_value = "old-token"
        MockRegistry.return_value.get_document.return_value = {
            "sp_last_modified": "2025-06-01T10:00:00Z",
        }

        MockMapper.return_value.to_s3_source_key.return_value = "source/key.pdf"

        from daily_sync import handler
        body = json.loads(handler({}, None)["body"])

        assert body["updated"] == 1
        assert body["created"] == 0

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_skips_unchanged(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        mock_graph = MockGraph.return_value
        _setup_graph(mock_graph, ([_new_file_item()], "delta-token"))

        MockDelta.return_value.get_delta_token.return_value = "old-token"
        MockRegistry.return_value.get_document.return_value = {
            "sp_last_modified": "2025-06-01T10:00:00Z",
        }
        MockMapper.return_value.to_s3_source_key.return_value = "source/key.pdf"

        from daily_sync import handler
        body = json.loads(handler({}, None)["body"])

        assert body["skipped"] == 1
        mock_graph.download_file.assert_not_called()

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_processes_deletion(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        mock_graph = MockGraph.return_value
        _setup_graph(mock_graph, ([_deleted_item()], "delta-token"))
        MockDelta.return_value.get_delta_token.return_value = None

        mock_mapper = MockMapper.return_value
        mock_mapper.to_s3_source_key.return_value = "source/Dynamo/Documents/General/old-file.pdf"

        from daily_sync import handler
        body = json.loads(handler({}, None)["body"])

        assert body["deleted"] == 1
        MockS3.return_value.delete_document.assert_called_once_with(
            "source/Dynamo/Documents/General/old-file.pdf",
        )
        MockRegistry.return_value.delete_document.assert_called_once_with(
            "source/Dynamo/Documents/General/old-file.pdf",
        )

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_skips_folders(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        folder_item = {
            "id": "folder-1",
            "name": "Subfolder",
            "folder": {"childCount": 5},
            "parentReference": {"path": "/drives/drive-1/root:"},
        }
        mock_graph = MockGraph.return_value
        _setup_graph(mock_graph, ([folder_item], "token"))
        MockDelta.return_value.get_delta_token.return_value = None

        from daily_sync import handler
        body = json.loads(handler({}, None)["body"])

        # Folders are silently skipped — not counted in any stat
        assert body["created"] == 0
        assert body["skipped"] == 0
        mock_graph.download_file.assert_not_called()

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_skips_no_download_url(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        item = _new_file_item()
        del item["@microsoft.graph.downloadUrl"]
        mock_graph = MockGraph.return_value
        _setup_graph(mock_graph, ([item], "token"))
        MockDelta.return_value.get_delta_token.return_value = None
        MockRegistry.return_value.get_document.return_value = None
        MockMapper.return_value.to_s3_source_key.return_value = "source/key.pdf"

        from daily_sync import handler
        body = json.loads(handler({}, None)["body"])

        assert body["skipped"] == 1
        mock_graph.download_file.assert_not_called()

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_no_libraries_returns_early(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        mock_graph = MockGraph.return_value
        mock_graph.get_site_id.return_value = "site-1"
        mock_graph.list_document_libraries.return_value = []

        from daily_sync import handler
        result = handler({}, None)

        body = json.loads(result["body"])
        assert "error" in body
        MockDelta.return_value.save_delta_token.assert_not_called()

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_download_failure_continues(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        """A single file download failure should not crash the handler."""
        items = [
            _new_file_item(id="item-1", name="fail.pdf"),
            _new_file_item(id="item-2", name="ok.pdf"),
        ]
        mock_graph = MockGraph.return_value
        _setup_graph(mock_graph, (items, "token"))
        mock_graph.download_file.side_effect = [
            RuntimeError("network error"),
            b"ok-content",
        ]

        MockDelta.return_value.get_delta_token.return_value = None
        MockRegistry.return_value.get_document.return_value = None
        MockMapper.return_value.to_s3_source_key.return_value = "source/key.pdf"

        from daily_sync import handler
        body = json.loads(handler({}, None)["body"])

        assert body["errors"] == 1
        assert body["created"] == 1
        # Delta token still saved
        MockDelta.return_value.save_delta_token.assert_called_once()

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_delta_api_failure_continues_other_libraries(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        mock_graph = MockGraph.return_value
        mock_graph.get_site_id.return_value = "site-1"
        mock_graph.list_document_libraries.return_value = [
            {"id": "drive-bad", "name": "Bad"},
            {"id": "drive-ok", "name": "OK"},
        ]
        mock_graph.get_delta.side_effect = [
            RuntimeError("Graph API error"),
            ([_new_file_item()], "ok-token"),
        ]
        mock_graph.download_file.return_value = b"data"

        MockDelta.return_value.get_delta_token.return_value = None
        MockRegistry.return_value.get_document.return_value = None
        MockMapper.return_value.to_s3_source_key.return_value = "source/key.pdf"

        from daily_sync import handler
        body = json.loads(handler({}, None)["body"])

        assert body["errors"] == 1
        assert body["created"] == 1
        # save_delta_token called only for the OK library
        MockDelta.return_value.save_delta_token.assert_called_once()

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_uploads_to_s3_with_tags(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        mock_graph = MockGraph.return_value
        _setup_graph(mock_graph, ([_new_file_item()], "token"))
        mock_graph.download_file.return_value = b"content"

        MockDelta.return_value.get_delta_token.return_value = None
        MockRegistry.return_value.get_document.return_value = None
        MockMapper.return_value.to_s3_source_key.return_value = "source/key.pdf"

        from daily_sync import handler
        handler({}, None)

        mock_s3 = MockS3.return_value
        mock_s3.upload_document.assert_called_once()
        call_kwargs = mock_s3.upload_document.call_args[1]
        assert call_kwargs["content"] == b"content"
        assert call_kwargs["s3_key"] == "source/key.pdf"
        assert "tags" in call_kwargs

    @patch("daily_sync.PathMapper")
    @patch("daily_sync.DocumentRegistry")
    @patch("daily_sync.DeltaTracker")
    @patch("daily_sync.S3Client")
    @patch("daily_sync.GraphClient")
    def test_multiple_libraries_processed(
        self, MockGraph, MockS3, MockDelta, MockRegistry, MockMapper,
    ):
        mock_graph = MockGraph.return_value
        mock_graph.get_site_id.return_value = "site-1"
        mock_graph.list_document_libraries.return_value = [
            {"id": "drive-a", "name": "HR"},
            {"id": "drive-b", "name": "Legal"},
        ]
        mock_graph.get_delta.side_effect = [
            ([_new_file_item(name="a.pdf")], "tok-a"),
            ([_new_file_item(name="b.pdf")], "tok-b"),
        ]
        mock_graph.download_file.return_value = b"data"

        MockDelta.return_value.get_delta_token.return_value = None
        MockRegistry.return_value.get_document.return_value = None
        MockMapper.return_value.to_s3_source_key.return_value = "source/key.pdf"

        from daily_sync import handler
        body = json.loads(handler({}, None)["body"])

        assert body["created"] == 2
        assert MockDelta.return_value.save_delta_token.call_count == 2


class TestExtractSpPath:
    def test_strips_drive_prefix(self):
        from daily_sync import _extract_sp_path

        result = _extract_sp_path(
            "/drives/drive-1/root:/General", "doc.pdf", "drive-1",
        )
        assert result == "/General/doc.pdf"

    def test_no_prefix_passes_through(self):
        from daily_sync import _extract_sp_path

        result = _extract_sp_path("/some/other/path", "file.txt", "drive-1")
        assert result == "/some/other/path/file.txt"
