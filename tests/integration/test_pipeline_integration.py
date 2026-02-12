"""Integration tests for the end-to-end pipeline against LocalStack.

Textract is NOT available in LocalStack, so it remains mocked.
S3, DynamoDB, and SNS/SQS use real LocalStack services.
"""

import json
from unittest.mock import patch, MagicMock

import pytest

from s3_client import S3Client
from document_registry import DocumentRegistry
from utils.path_mapper import PathMapper


pytestmark = pytest.mark.integration

BUCKET = "dynamo-ai-documents"
REGION = "us-east-1"


@pytest.fixture
def s3(localstack_env):
    return S3Client(bucket=BUCKET, region=REGION)


@pytest.fixture
def registry(localstack_env):
    return DocumentRegistry(region=REGION)


@pytest.fixture
def mapper():
    return PathMapper(BUCKET, "source", "extracted")


class TestDirectExtractPipeline:
    """End-to-end: upload a PPTX → invoke trigger handler → verify twin + registry."""

    def test_pptx_direct_extract(
        self, s3, registry, sample_pptx, clean_s3, clean_registry_table,
    ):
        source_key = "source/Dynamo/HR/presentation.pptx"
        content = sample_pptx.read_bytes()

        # 1. Upload the fixture to S3
        s3.upload_document(content, source_key, content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation")

        # 2. Register the document (as the ingest pipeline would)
        registry.register_document({
            "s3_source_key": source_key,
            "sp_item_id": "sp-pptx-001",
            "sp_path": "/HR/presentation.pptx",
            "sp_library": "HR",
            "sp_last_modified": "2025-06-01T10:00:00Z",
            "file_type": ".pptx",
            "size_bytes": len(content),
            "textract_status": "pending",
        })

        # 3. Build S3 event and invoke the trigger handler
        event = {
            "Records": [{
                "s3": {
                    "bucket": {"name": BUCKET},
                    "object": {"key": source_key},
                },
            }],
        }

        # Textract is not available in LocalStack — mock it.
        # But S3 and DynamoDB are real.
        with patch("textract_trigger.TextractClient"):
            from textract_trigger import handler
            result = handler(event, None)

        body = json.loads(result["body"])
        assert body["direct_extracts"] == 1
        assert body["errors"] == 0

        # 4. Verify registry was updated
        doc = registry.get_document(source_key)
        assert doc["textract_status"] == "completed"
        assert doc["s3_twin_key"] is not None

        # 5. Verify JSON twin was created in S3
        twin_key = doc["s3_twin_key"]
        assert s3.document_exists(twin_key)

    def test_txt_plain_text_extract(
        self, s3, registry, clean_s3, clean_registry_table,
    ):
        source_key = "source/Dynamo/Legal/readme.txt"
        content = b"This is a plain text document for testing."

        s3.upload_document(content, source_key, content_type="text/plain")
        registry.register_document({
            "s3_source_key": source_key,
            "sp_item_id": "sp-txt-001",
            "sp_path": "/Legal/readme.txt",
            "sp_library": "Legal",
            "sp_last_modified": "2025-06-01T10:00:00Z",
            "file_type": ".txt",
            "size_bytes": len(content),
            "textract_status": "pending",
        })

        event = {
            "Records": [{
                "s3": {
                    "bucket": {"name": BUCKET},
                    "object": {"key": source_key},
                },
            }],
        }

        with patch("textract_trigger.TextractClient"):
            from textract_trigger import handler
            result = handler(event, None)

        body = json.loads(result["body"])
        assert body["direct_extracts"] == 1

        doc = registry.get_document(source_key)
        assert doc["textract_status"] == "completed"
        assert s3.document_exists(doc["s3_twin_key"])


class TestTextractDirectPipeline:
    """For PDF/DOCX, Textract must be mocked but S3 + DynamoDB are real."""

    def test_pdf_triggers_textract_job(
        self, s3, registry, sample_pdf, clean_s3, clean_registry_table,
    ):
        source_key = "source/Dynamo/HR/handbook.pdf"
        content = sample_pdf.read_bytes()

        s3.upload_document(content, source_key, content_type="application/pdf")
        registry.register_document({
            "s3_source_key": source_key,
            "sp_item_id": "sp-pdf-001",
            "sp_path": "/HR/handbook.pdf",
            "sp_library": "HR",
            "sp_last_modified": "2025-06-01T10:00:00Z",
            "file_type": ".pdf",
            "size_bytes": len(content),
            "textract_status": "pending",
        })

        event = {
            "Records": [{
                "s3": {
                    "bucket": {"name": BUCKET},
                    "object": {"key": source_key},
                },
            }],
        }

        mock_textract = MagicMock()
        mock_textract.start_document_analysis.return_value = "mock-job-id-123"

        with patch("textract_trigger.TextractClient", return_value=mock_textract):
            from textract_trigger import handler
            result = handler(event, None)

        body = json.loads(result["body"])
        assert body["textract_jobs"] == 1

        # Verify registry was updated to processing
        doc = registry.get_document(source_key)
        assert doc["textract_status"] == "processing"
        assert doc["textract_job_id"] == "mock-job-id-123"


class TestPathMapperWithRealS3:
    """Verify PathMapper generates keys that work with real S3."""

    def test_round_trip_key_mapping(self, s3, mapper, clean_s3):
        source_key = mapper.to_s3_source_key("Dynamo", "HR-Policies", "2025/Employee Handbook.pdf")
        twin_key = mapper.to_s3_extracted_key(source_key)

        # Upload to both keys
        s3.upload_document(b"source content", source_key)
        s3.upload_document(b"twin content", twin_key)

        assert s3.document_exists(source_key)
        assert s3.document_exists(twin_key)

        # Reverse mapping
        site, library, rel_path = mapper.source_key_to_sharepoint_path(source_key)
        assert site == "Dynamo"
        assert library == "HR-Policies"
        assert "Employee" in rel_path

    def test_extracted_key_has_json_extension(self, mapper):
        source = "source/Dynamo/Legal/contract.docx"
        twin = mapper.to_s3_extracted_key(source)
        assert twin == "extracted/Dynamo/Legal/contract.json"


class TestSNStoSQS:
    """Verify SNS -> SQS subscription works in LocalStack."""

    def test_publish_and_receive(self, sns_client, sqs_client, sns_topic_arn, sqs_queue_url):
        message = json.dumps({
            "JobId": "test-job-123",
            "Status": "SUCCEEDED",
            "API": "StartDocumentAnalysis",
            "DocumentLocation": {
                "S3ObjectName": "source/Dynamo/HR/test.pdf",
                "S3Bucket": BUCKET,
            },
        })

        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject="AmazonTextract",
        )

        # Poll SQS for the message
        resp = sqs_client.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )

        messages = resp.get("Messages", [])
        assert len(messages) >= 1

        body = json.loads(messages[0]["Body"])
        # SNS wraps the message
        inner = json.loads(body["Message"])
        assert inner["JobId"] == "test-job-123"
        assert inner["Status"] == "SUCCEEDED"

        # Clean up
        sqs_client.delete_message(
            QueueUrl=sqs_queue_url,
            ReceiptHandle=messages[0]["ReceiptHandle"],
        )
