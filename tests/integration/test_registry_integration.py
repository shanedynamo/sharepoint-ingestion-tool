"""Integration tests for DocumentRegistry against LocalStack DynamoDB."""

import pytest

from document_registry import DocumentRegistry


pytestmark = pytest.mark.integration

TABLE_NAME = "sp-ingest-document-registry"
REGION = "us-east-1"


def _sample_doc(**overrides) -> dict:
    """Return a minimal valid document dict."""
    base = {
        "s3_source_key": "source/Dynamo/HR/handbook.pdf",
        "sp_item_id": "sp-item-001",
        "sp_path": "/HR/handbook.pdf",
        "sp_library": "HR",
        "sp_last_modified": "2025-06-01T10:00:00Z",
        "file_type": ".pdf",
        "size_bytes": 1024,
    }
    base.update(overrides)
    return base


@pytest.fixture
def registry(localstack_env):
    """Return a DocumentRegistry wired to LocalStack."""
    return DocumentRegistry(table_name=TABLE_NAME, region=REGION)


class TestRegisterAndGet:
    def test_register_and_retrieve(self, registry, clean_registry_table):
        doc = _sample_doc()
        registry.register_document(doc)

        result = registry.get_document(doc["s3_source_key"])

        assert result is not None
        assert result["s3_source_key"] == doc["s3_source_key"]
        assert result["sp_item_id"] == "sp-item-001"
        assert result["sp_library"] == "HR"
        assert result["textract_status"] == "pending"
        assert "ingested_at" in result
        assert "updated_at" in result

    def test_get_nonexistent_returns_none(self, registry, clean_registry_table):
        result = registry.get_document("source/no/such/key.pdf")
        assert result is None

    def test_register_upsert(self, registry, clean_registry_table):
        doc = _sample_doc()
        registry.register_document(doc)

        updated = _sample_doc(size_bytes=2048, sp_item_id="sp-item-002")
        registry.register_document(updated)

        result = registry.get_document(doc["s3_source_key"])
        assert result["size_bytes"] == 2048
        assert result["sp_item_id"] == "sp-item-002"


class TestUpdateTextractStatus:
    def test_update_status_to_processing(self, registry, clean_registry_table):
        doc = _sample_doc()
        registry.register_document(doc)

        registry.update_textract_status(
            doc["s3_source_key"], "processing", job_id="job-123",
        )

        result = registry.get_document(doc["s3_source_key"])
        assert result["textract_status"] == "processing"
        assert result["textract_job_id"] == "job-123"

    def test_update_status_to_completed_with_twin(self, registry, clean_registry_table):
        doc = _sample_doc()
        registry.register_document(doc)

        registry.update_textract_status(
            doc["s3_source_key"],
            "completed",
            job_id="job-456",
            twin_key="extracted/Dynamo/HR/handbook.json",
        )

        result = registry.get_document(doc["s3_source_key"])
        assert result["textract_status"] == "completed"
        assert result["s3_twin_key"] == "extracted/Dynamo/HR/handbook.json"


class TestQueryByTextractStatusGSI:
    def test_query_pending(self, registry, clean_registry_table):
        # Insert docs with different statuses
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/HR/pending1.pdf",
            textract_status="pending",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/HR/pending2.pdf",
            textract_status="pending",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/HR/completed1.pdf",
            textract_status="completed",
        ))

        pending = registry.get_pending_textract()

        pending_keys = {d["s3_source_key"] for d in pending}
        assert "source/Dynamo/HR/pending1.pdf" in pending_keys
        assert "source/Dynamo/HR/pending2.pdf" in pending_keys
        assert "source/Dynamo/HR/completed1.pdf" not in pending_keys

    def test_query_failed(self, registry, clean_registry_table):
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/HR/failed1.pdf",
            textract_status="failed",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/HR/ok.pdf",
            textract_status="completed",
        ))

        failed = registry.get_failed_textract()

        assert len(failed) == 1
        assert failed[0]["s3_source_key"] == "source/Dynamo/HR/failed1.pdf"


class TestQueryByLibraryGSI:
    def test_query_by_library(self, registry, dynamodb_resource, clean_registry_table):
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/HR/doc1.pdf",
            sp_library="HR",
            sp_last_modified="2025-06-01T10:00:00Z",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/Finance/doc2.pdf",
            sp_library="Finance",
            sp_last_modified="2025-07-01T10:00:00Z",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/HR/doc3.pdf",
            sp_library="HR",
            sp_last_modified="2025-08-01T10:00:00Z",
        ))

        # Query the GSI directly via boto3
        from boto3.dynamodb.conditions import Key
        table = dynamodb_resource.Table(TABLE_NAME)
        resp = table.query(
            IndexName="sp_library-index",
            KeyConditionExpression=Key("sp_library").eq("HR"),
        )

        hr_keys = {item["s3_source_key"] for item in resp["Items"]}
        assert "source/Dynamo/HR/doc1.pdf" in hr_keys
        assert "source/Dynamo/HR/doc3.pdf" in hr_keys
        assert "source/Dynamo/Finance/doc2.pdf" not in hr_keys


class TestDeleteDocument:
    def test_delete_removes_item(self, registry, clean_registry_table):
        doc = _sample_doc()
        registry.register_document(doc)

        registry.delete_document(doc["s3_source_key"])

        result = registry.get_document(doc["s3_source_key"])
        assert result is None


class TestGetStats:
    def test_stats_aggregation(self, registry, clean_registry_table):
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/HR/a.pdf",
            sp_library="HR", file_type=".pdf", textract_status="pending",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/HR/b.docx",
            sp_library="HR", file_type=".docx", textract_status="completed",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/Dynamo/Finance/c.xlsx",
            sp_library="Finance", file_type=".xlsx", textract_status="pending",
        ))

        stats = registry.get_stats()

        assert stats["total"] == 3
        assert stats["by_type"][".pdf"] == 1
        assert stats["by_type"][".docx"] == 1
        assert stats["by_type"][".xlsx"] == 1
        assert stats["by_library"]["HR"] == 2
        assert stats["by_library"]["Finance"] == 1
        assert stats["by_status"]["pending"] == 2
        assert stats["by_status"]["completed"] == 1
