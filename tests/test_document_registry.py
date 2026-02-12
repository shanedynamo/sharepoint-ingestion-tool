"""Tests for DocumentRegistry using moto to mock DynamoDB."""

import sys
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, "src")

TABLE_NAME = "test-document-registry"
REGION = "us-east-1"


@pytest.fixture
def dynamo_env(monkeypatch):
    """Set env vars so config + DocumentRegistry use our test table."""
    monkeypatch.setenv("DYNAMODB_REGISTRY_TABLE", TABLE_NAME)
    monkeypatch.setenv("AWS_REGION", REGION)
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


def _create_table():
    """Create the document-registry DynamoDB table with GSIs in moto."""
    dynamo = boto3.client("dynamodb", region_name=REGION)
    dynamo.create_table(
        TableName=TABLE_NAME,
        KeySchema=[{"AttributeName": "s3_source_key", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "s3_source_key", "AttributeType": "S"},
            {"AttributeName": "textract_status", "AttributeType": "S"},
            {"AttributeName": "ingested_at", "AttributeType": "S"},
            {"AttributeName": "sp_library", "AttributeType": "S"},
            {"AttributeName": "sp_last_modified", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "textract_status-index",
                "KeySchema": [
                    {"AttributeName": "textract_status", "KeyType": "HASH"},
                    {"AttributeName": "ingested_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "sp_library-index",
                "KeySchema": [
                    {"AttributeName": "sp_library", "KeyType": "HASH"},
                    {"AttributeName": "sp_last_modified", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )


def _sample_doc(**overrides) -> dict:
    """Return a minimal valid document dict with optional overrides."""
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
def registry(dynamo_env):
    """Create the DynamoDB table and return a DocumentRegistry instance."""
    with mock_aws():
        _create_table()

        import importlib
        import config as config_mod
        importlib.reload(config_mod)

        from document_registry import DocumentRegistry
        reg = DocumentRegistry(table_name=TABLE_NAME, region=REGION)
        yield reg


# ===================================================================
# register_document
# ===================================================================

class TestRegisterDocument:
    def test_registers_and_retrieves(self, registry):
        doc = _sample_doc()
        registry.register_document(doc)

        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result is not None
        assert result["sp_item_id"] == "sp-item-001"
        assert result["sp_path"] == "/HR/handbook.pdf"
        assert result["sp_library"] == "HR"
        assert result["file_type"] == ".pdf"
        assert result["size_bytes"] == 1024

    def test_upsert_updates_existing(self, registry):
        registry.register_document(_sample_doc(size_bytes=100))
        registry.register_document(_sample_doc(size_bytes=200))

        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["size_bytes"] == 200

    def test_default_textract_status_is_pending(self, registry):
        registry.register_document(_sample_doc())
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["textract_status"] == "pending"

    def test_custom_textract_status(self, registry):
        registry.register_document(_sample_doc(textract_status="completed"))
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["textract_status"] == "completed"

    def test_sets_timestamps(self, registry):
        registry.register_document(_sample_doc())
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["ingested_at"]
        assert result["updated_at"]
        assert "T" in result["ingested_at"]  # ISO format

    def test_preserves_custom_ingested_at(self, registry):
        registry.register_document(_sample_doc(ingested_at="2025-01-01T00:00:00Z"))
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["ingested_at"] == "2025-01-01T00:00:00Z"

    def test_null_optional_fields(self, registry):
        registry.register_document(_sample_doc())
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["s3_twin_key"] is None
        assert result["textract_job_id"] is None

    def test_sp_last_modified_stored(self, registry):
        registry.register_document(_sample_doc(sp_last_modified="2025-06-15T14:30:00Z"))
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["sp_last_modified"] == "2025-06-15T14:30:00Z"


# ===================================================================
# update_textract_status
# ===================================================================

class TestUpdateTextractStatus:
    def test_updates_status(self, registry):
        registry.register_document(_sample_doc())
        registry.update_textract_status(
            "source/Dynamo/HR/handbook.pdf", "processing",
        )
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["textract_status"] == "processing"

    def test_updates_job_id(self, registry):
        registry.register_document(_sample_doc())
        registry.update_textract_status(
            "source/Dynamo/HR/handbook.pdf", "processing", job_id="job-123",
        )
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["textract_job_id"] == "job-123"

    def test_updates_twin_key(self, registry):
        registry.register_document(_sample_doc())
        registry.update_textract_status(
            "source/Dynamo/HR/handbook.pdf",
            "completed",
            twin_key="extracted/Dynamo/HR/handbook.json",
        )
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["s3_twin_key"] == "extracted/Dynamo/HR/handbook.json"

    def test_updates_multiple_fields(self, registry):
        registry.register_document(_sample_doc())
        registry.update_textract_status(
            "source/Dynamo/HR/handbook.pdf",
            "completed",
            job_id="job-456",
            twin_key="extracted/Dynamo/HR/handbook.json",
        )
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result["textract_status"] == "completed"
        assert result["textract_job_id"] == "job-456"
        assert result["s3_twin_key"] == "extracted/Dynamo/HR/handbook.json"

    def test_updates_timestamp(self, registry):
        registry.register_document(_sample_doc())
        original = registry.get_document("source/Dynamo/HR/handbook.pdf")
        original_updated = original["updated_at"]

        registry.update_textract_status(
            "source/Dynamo/HR/handbook.pdf", "processing",
        )
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        # updated_at should be different (or at least set)
        assert result["updated_at"] >= original_updated


# ===================================================================
# get_document
# ===================================================================

class TestGetDocument:
    def test_returns_document(self, registry):
        registry.register_document(_sample_doc())
        result = registry.get_document("source/Dynamo/HR/handbook.pdf")
        assert result is not None
        assert result["s3_source_key"] == "source/Dynamo/HR/handbook.pdf"

    def test_returns_none_for_missing(self, registry):
        assert registry.get_document("source/nonexistent.pdf") is None


# ===================================================================
# get_pending_textract
# ===================================================================

class TestGetPendingTextract:
    def test_returns_pending_documents(self, registry):
        registry.register_document(_sample_doc(
            s3_source_key="source/a.pdf", textract_status="pending",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/b.pdf", textract_status="completed",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/c.pdf", textract_status="pending",
        ))

        pending = registry.get_pending_textract()
        keys = {d["s3_source_key"] for d in pending}
        assert keys == {"source/a.pdf", "source/c.pdf"}

    def test_returns_empty_when_none_pending(self, registry):
        registry.register_document(_sample_doc(textract_status="completed"))
        assert registry.get_pending_textract() == []


# ===================================================================
# get_failed_textract
# ===================================================================

class TestGetFailedTextract:
    def test_returns_failed_documents(self, registry):
        registry.register_document(_sample_doc(
            s3_source_key="source/fail1.pdf", textract_status="failed",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/ok.pdf", textract_status="completed",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/fail2.pdf", textract_status="failed",
        ))

        failed = registry.get_failed_textract()
        keys = {d["s3_source_key"] for d in failed}
        assert keys == {"source/fail1.pdf", "source/fail2.pdf"}

    def test_returns_empty_when_none_failed(self, registry):
        registry.register_document(_sample_doc(textract_status="pending"))
        assert registry.get_failed_textract() == []


# ===================================================================
# delete_document
# ===================================================================

class TestDeleteDocument:
    def test_deletes_document(self, registry):
        registry.register_document(_sample_doc())
        assert registry.get_document("source/Dynamo/HR/handbook.pdf") is not None

        registry.delete_document("source/Dynamo/HR/handbook.pdf")
        assert registry.get_document("source/Dynamo/HR/handbook.pdf") is None

    def test_delete_nonexistent_succeeds(self, registry):
        # Should not raise
        registry.delete_document("source/nonexistent.pdf")

    def test_delete_does_not_affect_other_documents(self, registry):
        registry.register_document(_sample_doc(s3_source_key="source/a.pdf"))
        registry.register_document(_sample_doc(s3_source_key="source/b.pdf"))

        registry.delete_document("source/a.pdf")
        assert registry.get_document("source/a.pdf") is None
        assert registry.get_document("source/b.pdf") is not None


# ===================================================================
# get_stats
# ===================================================================

class TestGetStats:
    def test_empty_table(self, registry):
        stats = registry.get_stats()
        assert stats["total"] == 0
        assert stats["by_type"] == {}
        assert stats["by_status"] == {}
        assert stats["by_library"] == {}

    def test_aggregates_by_type_status_library(self, registry):
        registry.register_document(_sample_doc(
            s3_source_key="source/a.pdf", file_type=".pdf",
            sp_library="HR", textract_status="pending",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/b.pdf", file_type=".pdf",
            sp_library="HR", textract_status="completed",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/c.docx", file_type=".docx",
            sp_library="Legal", textract_status="pending",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/d.xlsx", file_type=".xlsx",
            sp_library="Finance", textract_status="failed",
        ))

        stats = registry.get_stats()
        assert stats["total"] == 4

        assert stats["by_type"] == {".pdf": 2, ".docx": 1, ".xlsx": 1}
        assert stats["by_status"] == {"pending": 2, "completed": 1, "failed": 1}
        assert stats["by_library"] == {"HR": 2, "Legal": 1, "Finance": 1}

    def test_stats_reflect_deletions(self, registry):
        registry.register_document(_sample_doc(s3_source_key="source/a.pdf"))
        registry.register_document(_sample_doc(s3_source_key="source/b.pdf"))
        registry.delete_document("source/a.pdf")

        stats = registry.get_stats()
        assert stats["total"] == 1


# ===================================================================
# __init__
# ===================================================================

class TestInit:
    def test_explicit_params(self, dynamo_env):
        with mock_aws():
            _create_table()
            from document_registry import DocumentRegistry
            reg = DocumentRegistry(table_name=TABLE_NAME, region=REGION)
            assert reg._table_name == TABLE_NAME
            assert reg._region == REGION

    def test_defaults_from_config(self, dynamo_env):
        with mock_aws():
            _create_table()
            import importlib
            import config as config_mod
            importlib.reload(config_mod)
            import document_registry as reg_mod
            importlib.reload(reg_mod)
            reg = reg_mod.DocumentRegistry()
            assert reg._table_name == TABLE_NAME


# ===================================================================
# GSI queries
# ===================================================================

class TestGSIQueries:
    def test_textract_status_index_sorted_by_ingested_at(self, registry):
        registry.register_document(_sample_doc(
            s3_source_key="source/old.pdf",
            textract_status="pending",
            ingested_at="2025-01-01T00:00:00Z",
        ))
        registry.register_document(_sample_doc(
            s3_source_key="source/new.pdf",
            textract_status="pending",
            ingested_at="2025-06-01T00:00:00Z",
        ))

        pending = registry.get_pending_textract()
        assert len(pending) == 2
        # GSI sorts by ingested_at (range key)
        assert pending[0]["ingested_at"] <= pending[1]["ingested_at"]

    def test_only_matching_status_returned(self, registry):
        for status in ("pending", "processing", "completed", "failed"):
            registry.register_document(_sample_doc(
                s3_source_key=f"source/{status}.pdf",
                textract_status=status,
            ))

        pending = registry.get_pending_textract()
        assert len(pending) == 1
        assert pending[0]["textract_status"] == "pending"

        failed = registry.get_failed_textract()
        assert len(failed) == 1
        assert failed[0]["textract_status"] == "failed"


# ===================================================================
# Integration: full document lifecycle
# ===================================================================

class TestLifecycle:
    def test_full_document_lifecycle(self, registry):
        s3_key = "source/Dynamo/Legal/contract.pdf"

        # 1. Register document
        registry.register_document({
            "s3_source_key": s3_key,
            "sp_item_id": "sp-999",
            "sp_path": "/Legal/contract.pdf",
            "sp_library": "Legal",
            "sp_last_modified": "2025-03-01T09:00:00Z",
            "file_type": ".pdf",
            "size_bytes": 50000,
        })

        doc = registry.get_document(s3_key)
        assert doc["textract_status"] == "pending"
        assert doc["textract_job_id"] is None
        assert doc["s3_twin_key"] is None

        # 2. Start Textract processing
        registry.update_textract_status(s3_key, "processing", job_id="job-abc")
        doc = registry.get_document(s3_key)
        assert doc["textract_status"] == "processing"
        assert doc["textract_job_id"] == "job-abc"

        # 3. Textract completes -> twin built
        twin_key = "extracted/Dynamo/Legal/contract.json"
        registry.update_textract_status(
            s3_key, "completed", job_id="job-abc", twin_key=twin_key,
        )
        doc = registry.get_document(s3_key)
        assert doc["textract_status"] == "completed"
        assert doc["s3_twin_key"] == twin_key

        # 4. Verify not in pending/failed lists
        assert registry.get_pending_textract() == []
        assert registry.get_failed_textract() == []

        # 5. Stats reflect the document
        stats = registry.get_stats()
        assert stats["total"] == 1
        assert stats["by_type"] == {".pdf": 1}
        assert stats["by_status"] == {"completed": 1}
        assert stats["by_library"] == {"Legal": 1}

        # 6. Delete
        registry.delete_document(s3_key)
        assert registry.get_document(s3_key) is None
        assert registry.get_stats()["total"] == 0
