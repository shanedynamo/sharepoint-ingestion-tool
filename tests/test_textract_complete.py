"""Tests for the textract_complete Lambda handler."""

import json
import sys
from unittest.mock import MagicMock, patch, ANY

import pytest

sys.path.insert(0, "src")


def _sns_event(job_id: str, status: str, s3_key: str = "source/Dynamo/HR/doc.pdf"):
    """Build a minimal SNS event wrapping a Textract completion message."""
    message = {
        "JobId": job_id,
        "Status": status,
        "DocumentLocation": {"S3ObjectName": s3_key},
    }
    return {
        "Records": [
            {"Sns": {"Message": json.dumps(message)}},
        ],
    }


def _sample_doc(**overrides):
    base = {
        "s3_source_key": "source/Dynamo/HR/doc.pdf",
        "sp_item_id": "sp-1",
        "sp_path": "/HR/doc.pdf",
        "sp_library": "HR",
        "sp_last_modified": "2025-06-01T10:00:00Z",
        "file_type": ".pdf",
        "size_bytes": 1024,
        "content_type": "application/pdf",
    }
    base.update(overrides)
    return base


def _textract_result():
    return {
        "JobId": "job-123",
        "JobStatus": "SUCCEEDED",
        "Blocks": [
            {"Id": "l1", "BlockType": "LINE", "Text": "Hello", "Page": 1},
        ],
    }


class TestTextractCompleteHandler:
    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_successful_job_builds_twin(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        MockTextract.return_value.get_document_analysis.return_value = _textract_result()
        MockRegistry.return_value.get_document.return_value = _sample_doc()
        MockBuilder.return_value.build_twin_from_textract.return_value = {"twin": True}
        MockMapper.return_value.to_s3_extracted_key.return_value = "extracted/Dynamo/HR/doc.json"

        from textract_complete import handler
        result = handler(_sns_event("job-123", "SUCCEEDED"), None)

        body = json.loads(result["body"])
        assert body["twins_built"] == 1
        assert body["failed"] == 0

        MockTextract.return_value.get_document_analysis.assert_called_once_with("job-123")
        MockBuilder.return_value.build_twin_from_textract.assert_called_once()
        MockS3.return_value.upload_json_twin.assert_called_once()

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_updates_registry_on_success(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        MockTextract.return_value.get_document_analysis.return_value = _textract_result()
        MockRegistry.return_value.get_document.return_value = _sample_doc()
        MockBuilder.return_value.build_twin_from_textract.return_value = {"twin": True}
        MockMapper.return_value.to_s3_extracted_key.return_value = "extracted/key.json"

        from textract_complete import handler
        handler(_sns_event("job-123", "SUCCEEDED"), None)

        MockRegistry.return_value.update_textract_status.assert_called_once_with(
            "source/Dynamo/HR/doc.pdf", "completed",
            job_id="job-123", twin_key="extracted/key.json",
        )

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_failed_job_updates_registry(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        from textract_complete import handler
        result = handler(_sns_event("job-fail", "FAILED"), None)

        body = json.loads(result["body"])
        assert body["failed"] == 1
        assert body["twins_built"] == 0

        MockRegistry.return_value.update_textract_status.assert_called_once_with(
            "source/Dynamo/HR/doc.pdf", "failed", job_id="job-fail",
        )
        # Should NOT attempt to get Textract results
        MockTextract.return_value.get_document_analysis.assert_not_called()

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_missing_registry_entry(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        MockRegistry.return_value.get_document.return_value = None

        from textract_complete import handler
        result = handler(_sns_event("job-123", "SUCCEEDED"), None)

        body = json.loads(result["body"])
        assert body["errors"] == 1
        assert body["twins_built"] == 0
        MockTextract.return_value.get_document_analysis.assert_not_called()

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_carries_source_tags(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        MockTextract.return_value.get_document_analysis.return_value = _textract_result()
        MockRegistry.return_value.get_document.return_value = _sample_doc()
        MockBuilder.return_value.build_twin_from_textract.return_value = {}
        MockMapper.return_value.to_s3_extracted_key.return_value = "extracted/key.json"

        from textract_complete import handler
        handler(_sns_event("job-123", "SUCCEEDED"), None)

        call_kwargs = MockS3.return_value.upload_json_twin.call_args
        tags = call_kwargs[1]["tags"] if "tags" in call_kwargs[1] else call_kwargs[0][2]
        assert "sp-library" in tags
        assert tags["sp-library"] == "HR"

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_textract_retrieval_error_marks_failed(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        MockRegistry.return_value.get_document.return_value = _sample_doc()
        MockTextract.return_value.get_document_analysis.side_effect = RuntimeError("API error")

        from textract_complete import handler
        result = handler(_sns_event("job-123", "SUCCEEDED"), None)

        body = json.loads(result["body"])
        assert body["errors"] == 1
        # Should mark as failed in registry
        MockRegistry.return_value.update_textract_status.assert_called_with(
            "source/Dynamo/HR/doc.pdf", "failed",
        )

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_twin_upload_error_marks_failed(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        MockTextract.return_value.get_document_analysis.return_value = _textract_result()
        MockRegistry.return_value.get_document.return_value = _sample_doc()
        MockBuilder.return_value.build_twin_from_textract.return_value = {}
        MockMapper.return_value.to_s3_extracted_key.return_value = "extracted/key.json"
        MockS3.return_value.upload_json_twin.side_effect = RuntimeError("S3 error")

        from textract_complete import handler
        result = handler(_sns_event("job-123", "SUCCEEDED"), None)

        body = json.loads(result["body"])
        assert body["errors"] == 1

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_multiple_records(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        MockTextract.return_value.get_document_analysis.return_value = _textract_result()
        MockRegistry.return_value.get_document.return_value = _sample_doc()
        MockBuilder.return_value.build_twin_from_textract.return_value = {}
        MockMapper.return_value.to_s3_extracted_key.return_value = "extracted/key.json"

        event = {
            "Records": [
                {"Sns": {"Message": json.dumps({
                    "JobId": "job-1", "Status": "SUCCEEDED",
                    "DocumentLocation": {"S3ObjectName": "source/a.pdf"},
                })}},
                {"Sns": {"Message": json.dumps({
                    "JobId": "job-2", "Status": "SUCCEEDED",
                    "DocumentLocation": {"S3ObjectName": "source/b.pdf"},
                })}},
            ],
        }

        from textract_complete import handler
        result = handler(event, None)

        body = json.loads(result["body"])
        assert body["twins_built"] == 2

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_mixed_success_and_failure(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        MockTextract.return_value.get_document_analysis.return_value = _textract_result()
        MockRegistry.return_value.get_document.return_value = _sample_doc()
        MockBuilder.return_value.build_twin_from_textract.return_value = {}
        MockMapper.return_value.to_s3_extracted_key.return_value = "extracted/key.json"

        event = {
            "Records": [
                {"Sns": {"Message": json.dumps({
                    "JobId": "job-ok", "Status": "SUCCEEDED",
                    "DocumentLocation": {"S3ObjectName": "source/ok.pdf"},
                })}},
                {"Sns": {"Message": json.dumps({
                    "JobId": "job-fail", "Status": "FAILED",
                    "DocumentLocation": {"S3ObjectName": "source/fail.pdf"},
                })}},
            ],
        }

        from textract_complete import handler
        result = handler(event, None)

        body = json.loads(result["body"])
        assert body["twins_built"] == 1
        assert body["failed"] == 1

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_empty_event(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        from textract_complete import handler
        result = handler({"Records": []}, None)

        body = json.loads(result["body"])
        assert body == {"twins_built": 0, "failed": 0, "errors": 0}

    @patch("textract_complete.PathMapper")
    @patch("textract_complete.DigitalTwinBuilder")
    @patch("textract_complete.DocumentRegistry")
    @patch("textract_complete.S3Client")
    @patch("textract_complete.TextractClient")
    def test_error_in_one_record_does_not_crash_others(
        self, MockTextract, MockS3, MockRegistry, MockBuilder, MockMapper,
    ):
        """First record throws, second should still succeed."""
        MockTextract.return_value.get_document_analysis.side_effect = [
            RuntimeError("boom"),
            _textract_result(),
        ]
        MockRegistry.return_value.get_document.return_value = _sample_doc()
        MockBuilder.return_value.build_twin_from_textract.return_value = {}
        MockMapper.return_value.to_s3_extracted_key.return_value = "extracted/key.json"

        event = {
            "Records": [
                {"Sns": {"Message": json.dumps({
                    "JobId": "job-err", "Status": "SUCCEEDED",
                    "DocumentLocation": {"S3ObjectName": "source/err.pdf"},
                })}},
                {"Sns": {"Message": json.dumps({
                    "JobId": "job-ok", "Status": "SUCCEEDED",
                    "DocumentLocation": {"S3ObjectName": "source/ok.pdf"},
                })}},
            ],
        }

        from textract_complete import handler
        result = handler(event, None)

        body = json.loads(result["body"])
        assert body["errors"] == 1
        assert body["twins_built"] == 1
