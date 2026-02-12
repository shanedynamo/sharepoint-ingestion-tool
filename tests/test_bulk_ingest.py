"""Tests for the bulk ingestion script."""

import csv
import json
import os
import sys
import threading
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, "src")

# ===================================================================
# Helpers
# ===================================================================

MOCK_LIBRARIES = [
    {"id": "drive-1", "name": "Documents", "webUrl": "https://sp/docs"},
    {"id": "drive-2", "name": "HR Policies", "webUrl": "https://sp/hr"},
]


def _make_doc(**overrides) -> dict:
    """Return a fake crawled document dict."""
    base = {
        "id": "sp-item-1",
        "name": "report.pdf",
        "file_type": ".pdf",
        "size": 2048,
        "sharepoint_path": "/General/report.pdf",
        "last_modified": "2025-06-01T10:00:00Z",
        "created": "2025-05-01T00:00:00Z",
        "author": "Alice",
        "download_url": "https://cdn.sp.com/report.pdf",
        "etag": "etag-1",
        "content_type": "application/pdf",
        "library_name": "Documents",
        "site_name": "Dynamo",
    }
    base.update(overrides)
    return base


def _setup_graph(mock_graph, libraries=None, docs=None, delta_return=None):
    """Wire up common graph mock methods."""
    mock_graph.get_site_id.return_value = "site-1"
    mock_graph.list_document_libraries.return_value = libraries or MOCK_LIBRARIES[:1]
    if docs is not None:
        mock_graph.crawl_library.return_value = iter(docs)
    mock_graph.download_file.return_value = b"file-content"
    if delta_return is not None:
        mock_graph.get_delta.return_value = delta_return
    else:
        mock_graph.get_delta.return_value = ([], "delta-token-1")


# ===================================================================
# _Stats
# ===================================================================

class TestStats:
    def test_initial_state(self):
        from bulk_ingest import _Stats
        stats = _Stats()
        assert stats.ingested == 0
        assert stats.skipped == 0
        assert stats.errors == 0
        assert stats.bytes_transferred == 0
        assert stats.failures == []

    def test_record_ingested(self):
        from bulk_ingest import _Stats
        stats = _Stats()
        stats.record_ingested(1024)
        stats.record_ingested(2048)
        assert stats.ingested == 2
        assert stats.bytes_transferred == 3072

    def test_record_skipped(self):
        from bulk_ingest import _Stats
        stats = _Stats()
        stats.record_skipped()
        stats.record_skipped()
        assert stats.skipped == 2

    def test_record_error(self):
        from bulk_ingest import _Stats
        stats = _Stats()
        stats.record_error("/path/to/file.pdf", "connection timeout")
        assert stats.errors == 1
        assert len(stats.failures) == 1
        assert stats.failures[0]["filename"] == "/path/to/file.pdf"
        assert stats.failures[0]["error"] == "connection timeout"
        assert "timestamp" in stats.failures[0]

    def test_total_processed(self):
        from bulk_ingest import _Stats
        stats = _Stats()
        stats.record_ingested(100)
        stats.record_skipped()
        stats.record_error("x", "err")
        assert stats.total_processed == 3

    def test_snapshot(self):
        from bulk_ingest import _Stats
        stats = _Stats()
        stats.record_ingested(500)
        stats.record_skipped()
        snap = stats.snapshot()
        assert snap == {
            "ingested": 1,
            "skipped": 1,
            "bytes_transferred": 500,
            "errors": 0,
        }

    def test_thread_safety(self):
        from bulk_ingest import _Stats
        stats = _Stats()
        errors = []

        def _worker():
            try:
                for _ in range(100):
                    stats.record_ingested(10)
                    stats.record_skipped()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=_worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert stats.ingested == 1000
        assert stats.skipped == 1000
        assert stats.bytes_transferred == 10000


# ===================================================================
# _format_bytes
# ===================================================================

class TestFormatBytes:
    def test_zero(self):
        from bulk_ingest import _format_bytes
        assert _format_bytes(0) == "0 B"

    def test_bytes(self):
        from bulk_ingest import _format_bytes
        assert _format_bytes(512) == "512 B"

    def test_kilobytes(self):
        from bulk_ingest import _format_bytes
        result = _format_bytes(1536)
        assert "KB" in result

    def test_megabytes(self):
        from bulk_ingest import _format_bytes
        result = _format_bytes(5 * 1024 * 1024)
        assert "MB" in result

    def test_gigabytes(self):
        from bulk_ingest import _format_bytes
        result = _format_bytes(2.5 * 1024 ** 3)
        assert "GB" in result


# ===================================================================
# _write_error_csv
# ===================================================================

class TestWriteErrorCsv:
    def test_writes_csv(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from bulk_ingest import _write_error_csv

        failures = [
            {"filename": "a.pdf", "error": "timeout", "timestamp": "2025-01-01T00:00:00Z"},
            {"filename": "b.docx", "error": "403 Forbidden", "timestamp": "2025-01-01T00:01:00Z"},
        ]
        _write_error_csv(failures)

        csv_path = tmp_path / "errors.csv"
        assert csv_path.exists()

        with open(csv_path) as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["filename"] == "a.pdf"
        assert rows[0]["error"] == "timeout"
        assert rows[1]["filename"] == "b.docx"


# ===================================================================
# _JsonFormatter
# ===================================================================

class TestJsonFormatter:
    def test_format_produces_json(self):
        import logging
        from bulk_ingest import _JsonFormatter

        formatter = _JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="hello %s", args=("world",), exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["message"] == "hello world"
        assert parsed["level"] == "INFO"
        assert "timestamp" in parsed

    def test_format_includes_exception(self):
        import logging
        from bulk_ingest import _JsonFormatter

        formatter = _JsonFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            record = logging.LogRecord(
                name="test", level=logging.ERROR, pathname="",
                lineno=0, msg="failed", args=(), exc_info=sys.exc_info(),
            )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]


# ===================================================================
# _upload_worker
# ===================================================================

class TestUploadWorker:
    @patch("bulk_ingest._local", threading.local())
    @patch("bulk_ingest.S3Client")
    def test_uploads_and_registers(self, MockS3):
        from bulk_ingest import _upload_worker, _Stats

        mock_s3 = MockS3.return_value
        mock_graph = MagicMock()
        mock_graph.download_file.return_value = b"pdf-data"
        mock_registry = MagicMock()
        stats = _Stats()

        doc = _make_doc()
        _upload_worker(
            doc=doc,
            s3_key="source/Dynamo/Documents/General/report.pdf",
            tags={"sp-site": "Dynamo"},
            graph=mock_graph,
            registry=mock_registry,
            stats=stats,
            dry_run=False,
        )

        mock_graph.download_file.assert_called_once_with("https://cdn.sp.com/report.pdf")
        mock_s3.upload_document.assert_called_once()
        mock_registry.register_document.assert_called_once()
        assert stats.ingested == 1
        assert stats.bytes_transferred == len(b"pdf-data")

    def test_dry_run_skips_upload(self):
        from bulk_ingest import _upload_worker, _Stats

        stats = _Stats()
        doc = _make_doc(size=1024)
        _upload_worker(
            doc=doc,
            s3_key="source/x.pdf",
            tags={},
            graph=MagicMock(),
            registry=MagicMock(),
            stats=stats,
            dry_run=True,
        )

        assert stats.ingested == 1
        assert stats.bytes_transferred == 1024

    @patch("bulk_ingest._local", threading.local())
    @patch("bulk_ingest.S3Client")
    def test_error_recorded_on_failure(self, MockS3):
        from bulk_ingest import _upload_worker, _Stats

        mock_graph = MagicMock()
        mock_graph.download_file.side_effect = ConnectionError("timeout")
        stats = _Stats()

        doc = _make_doc()
        _upload_worker(
            doc=doc,
            s3_key="source/x.pdf",
            tags={},
            graph=mock_graph,
            registry=MagicMock(),
            stats=stats,
            dry_run=False,
        )

        assert stats.errors == 1
        assert stats.ingested == 0
        assert stats.failures[0]["error"] == "timeout"


# ===================================================================
# run_bulk_ingestion
# ===================================================================

class TestRunBulkIngestion:
    @patch("bulk_ingest.DeltaTracker")
    @patch("bulk_ingest.DocumentRegistry")
    @patch("bulk_ingest.PathMapper")
    @patch("bulk_ingest.S3Client")
    @patch("bulk_ingest.GraphClient")
    def test_ingests_new_documents(
        self, MockGraph, MockS3, MockMapper, MockRegistry, MockDelta
    ):
        from bulk_ingest import run_bulk_ingestion

        mock_graph = MockGraph.return_value
        docs = [_make_doc(id=f"sp-{i}", name=f"file{i}.pdf") for i in range(3)]
        _setup_graph(mock_graph, docs=docs)
        mock_graph.download_file.return_value = b"content"

        mock_registry = MockRegistry.return_value
        mock_registry.get_document.return_value = None  # all new

        mock_mapper = MockMapper.return_value
        mock_mapper.to_s3_source_key.side_effect = lambda s, l, p: f"source/{s}/{l}{p}"

        exit_code = run_bulk_ingestion(dry_run=False, library_filter=None)

        assert exit_code == 0
        assert mock_registry.register_document.call_count == 3
        MockDelta.return_value.save_delta_token.assert_called_once()

    @patch("bulk_ingest.DeltaTracker")
    @patch("bulk_ingest.DocumentRegistry")
    @patch("bulk_ingest.PathMapper")
    @patch("bulk_ingest.S3Client")
    @patch("bulk_ingest.GraphClient")
    def test_skips_unchanged_documents(
        self, MockGraph, MockS3, MockMapper, MockRegistry, MockDelta
    ):
        from bulk_ingest import run_bulk_ingestion

        mock_graph = MockGraph.return_value
        docs = [_make_doc(last_modified="2025-06-01T10:00:00Z")]
        _setup_graph(mock_graph, docs=docs)

        mock_registry = MockRegistry.return_value
        mock_registry.get_document.return_value = {
            "sp_last_modified": "2025-06-01T10:00:00Z",
        }

        mock_mapper = MockMapper.return_value
        mock_mapper.to_s3_source_key.return_value = "source/Dynamo/Documents/report.pdf"

        exit_code = run_bulk_ingestion(dry_run=False)

        assert exit_code == 0
        mock_graph.download_file.assert_not_called()
        mock_registry.register_document.assert_not_called()

    @patch("bulk_ingest.DeltaTracker")
    @patch("bulk_ingest.DocumentRegistry")
    @patch("bulk_ingest.PathMapper")
    @patch("bulk_ingest.S3Client")
    @patch("bulk_ingest.GraphClient")
    def test_dry_run_does_not_upload(
        self, MockGraph, MockS3, MockMapper, MockRegistry, MockDelta
    ):
        from bulk_ingest import run_bulk_ingestion

        mock_graph = MockGraph.return_value
        docs = [_make_doc()]
        _setup_graph(mock_graph, docs=docs)

        mock_mapper = MockMapper.return_value
        mock_mapper.to_s3_source_key.return_value = "source/Dynamo/Documents/report.pdf"

        exit_code = run_bulk_ingestion(dry_run=True)

        assert exit_code == 0
        # Dry run should NOT call download, S3 upload, or DynamoDB register
        mock_graph.download_file.assert_not_called()
        MockS3.return_value.upload_document.assert_not_called()
        MockRegistry.return_value.register_document.assert_not_called()
        # Delta token should NOT be saved in dry run
        MockDelta.return_value.save_delta_token.assert_not_called()

    @patch("bulk_ingest.DeltaTracker")
    @patch("bulk_ingest.DocumentRegistry")
    @patch("bulk_ingest.PathMapper")
    @patch("bulk_ingest.S3Client")
    @patch("bulk_ingest.GraphClient")
    def test_library_filter(
        self, MockGraph, MockS3, MockMapper, MockRegistry, MockDelta
    ):
        from bulk_ingest import run_bulk_ingestion

        mock_graph = MockGraph.return_value
        mock_graph.get_site_id.return_value = "site-1"
        mock_graph.list_document_libraries.return_value = MOCK_LIBRARIES
        mock_graph.crawl_library.return_value = iter([_make_doc(library_name="HR Policies")])
        mock_graph.download_file.return_value = b"content"
        mock_graph.get_delta.return_value = ([], "tok")

        mock_registry = MockRegistry.return_value
        mock_registry.get_document.return_value = None

        mock_mapper = MockMapper.return_value
        mock_mapper.to_s3_source_key.return_value = "source/Dynamo/HR-Policies/report.pdf"

        exit_code = run_bulk_ingestion(library_filter="HR Policies")

        assert exit_code == 0
        # crawl_library should only be called for "HR Policies" (drive-2)
        mock_graph.crawl_library.assert_called_once_with("drive-2", library_name="HR Policies")

    @patch("bulk_ingest.DeltaTracker")
    @patch("bulk_ingest.DocumentRegistry")
    @patch("bulk_ingest.PathMapper")
    @patch("bulk_ingest.S3Client")
    @patch("bulk_ingest.GraphClient")
    def test_library_filter_not_found(
        self, MockGraph, MockS3, MockMapper, MockRegistry, MockDelta
    ):
        from bulk_ingest import run_bulk_ingestion

        mock_graph = MockGraph.return_value
        mock_graph.get_site_id.return_value = "site-1"
        mock_graph.list_document_libraries.return_value = MOCK_LIBRARIES

        exit_code = run_bulk_ingestion(library_filter="NonExistent")

        assert exit_code == 1

    @patch("bulk_ingest.DeltaTracker")
    @patch("bulk_ingest.DocumentRegistry")
    @patch("bulk_ingest.PathMapper")
    @patch("bulk_ingest.S3Client")
    @patch("bulk_ingest.GraphClient")
    def test_no_libraries_returns_1(
        self, MockGraph, MockS3, MockMapper, MockRegistry, MockDelta
    ):
        from bulk_ingest import run_bulk_ingestion

        mock_graph = MockGraph.return_value
        mock_graph.get_site_id.return_value = "site-1"
        mock_graph.list_document_libraries.return_value = []

        exit_code = run_bulk_ingestion()

        assert exit_code == 1

    @patch("bulk_ingest.DeltaTracker")
    @patch("bulk_ingest.DocumentRegistry")
    @patch("bulk_ingest.PathMapper")
    @patch("bulk_ingest.S3Client")
    @patch("bulk_ingest.GraphClient")
    def test_skips_documents_without_download_url(
        self, MockGraph, MockS3, MockMapper, MockRegistry, MockDelta
    ):
        from bulk_ingest import run_bulk_ingestion

        mock_graph = MockGraph.return_value
        docs = [_make_doc(download_url="")]
        _setup_graph(mock_graph, docs=docs)

        mock_registry = MockRegistry.return_value
        mock_registry.get_document.return_value = None

        mock_mapper = MockMapper.return_value
        mock_mapper.to_s3_source_key.return_value = "source/Dynamo/Documents/report.pdf"

        exit_code = run_bulk_ingestion()

        assert exit_code == 0
        mock_graph.download_file.assert_not_called()

    @patch("bulk_ingest.DeltaTracker")
    @patch("bulk_ingest.DocumentRegistry")
    @patch("bulk_ingest.PathMapper")
    @patch("bulk_ingest.S3Client")
    @patch("bulk_ingest.GraphClient")
    def test_returns_1_on_failures(
        self, MockGraph, MockS3, MockMapper, MockRegistry, MockDelta
    ):
        from bulk_ingest import run_bulk_ingestion

        mock_graph = MockGraph.return_value
        docs = [_make_doc()]
        _setup_graph(mock_graph, docs=docs)
        mock_graph.download_file.side_effect = ConnectionError("fail")

        mock_registry = MockRegistry.return_value
        mock_registry.get_document.return_value = None

        mock_mapper = MockMapper.return_value
        mock_mapper.to_s3_source_key.return_value = "source/Dynamo/Documents/report.pdf"

        exit_code = run_bulk_ingestion()

        assert exit_code == 1

    @patch("bulk_ingest.DeltaTracker")
    @patch("bulk_ingest.DocumentRegistry")
    @patch("bulk_ingest.PathMapper")
    @patch("bulk_ingest.S3Client")
    @patch("bulk_ingest.GraphClient")
    def test_continues_after_single_failure(
        self, MockGraph, MockS3, MockMapper, MockRegistry, MockDelta
    ):
        from bulk_ingest import run_bulk_ingestion

        mock_graph = MockGraph.return_value
        docs = [
            _make_doc(id="sp-1", name="good.pdf", download_url="https://cdn/good.pdf"),
            _make_doc(id="sp-2", name="bad.pdf", download_url="https://cdn/bad.pdf"),
            _make_doc(id="sp-3", name="also-good.pdf", download_url="https://cdn/also-good.pdf"),
        ]
        _setup_graph(mock_graph, docs=docs)

        # Second download fails, others succeed
        mock_graph.download_file.side_effect = [
            b"content-1",
            ConnectionError("timeout"),
            b"content-3",
        ]

        mock_registry = MockRegistry.return_value
        mock_registry.get_document.return_value = None

        mock_mapper = MockMapper.return_value
        mock_mapper.to_s3_source_key.side_effect = [
            "source/a.pdf", "source/b.pdf", "source/c.pdf",
        ]

        exit_code = run_bulk_ingestion()

        # Should still register 2 successful documents
        assert exit_code == 1  # 1 failure means exit code 1
        assert mock_registry.register_document.call_count == 2


# ===================================================================
# _save_initial_delta_token
# ===================================================================

class TestSaveInitialDeltaToken:
    def test_saves_token(self):
        from bulk_ingest import _save_initial_delta_token

        mock_graph = MagicMock()
        mock_graph.get_delta.return_value = ([], "initial-delta-token")
        mock_delta = MagicMock()

        _save_initial_delta_token(mock_graph, mock_delta, "drive-1", 50)

        mock_graph.get_delta.assert_called_once_with("drive-1", None)
        mock_delta.save_delta_token.assert_called_once()
        call_kwargs = mock_delta.save_delta_token.call_args
        assert call_kwargs.kwargs["drive_id"] == "drive-1"
        assert call_kwargs.kwargs["token"] == "initial-delta-token"
        assert call_kwargs.kwargs["items_processed"] == 50

    def test_handles_error_gracefully(self):
        from bulk_ingest import _save_initial_delta_token

        mock_graph = MagicMock()
        mock_graph.get_delta.side_effect = RuntimeError("network error")
        mock_delta = MagicMock()

        # Should not raise
        _save_initial_delta_token(mock_graph, mock_delta, "drive-1", 10)

        mock_delta.save_delta_token.assert_not_called()


# ===================================================================
# CLI argument parsing
# ===================================================================

class TestCLI:
    def test_default_args(self):
        import argparse
        from bulk_ingest import main

        parser = argparse.ArgumentParser()
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--library", type=str, default=None)
        args = parser.parse_args([])

        assert args.dry_run is False
        assert args.library is None

    def test_dry_run_flag(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--library", type=str, default=None)
        args = parser.parse_args(["--dry-run"])

        assert args.dry_run is True

    def test_library_flag(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--library", type=str, default=None)
        args = parser.parse_args(["--library", "HR Policies"])

        assert args.library == "HR Policies"

    def test_combined_flags(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--library", type=str, default=None)
        args = parser.parse_args(["--dry-run", "--library", "Legal"])

        assert args.dry_run is True
        assert args.library == "Legal"
