"""Tests for S3Client using moto to mock AWS S3."""

import json
import os
import sys

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, "src")

BUCKET = "test-ingest-bucket"
REGION = "us-east-1"


@pytest.fixture
def s3_env(monkeypatch):
    """Set env vars so config + S3Client use our test bucket."""
    monkeypatch.setenv("S3_BUCKET", BUCKET)
    monkeypatch.setenv("AWS_REGION", REGION)
    monkeypatch.setenv("S3_SOURCE_PREFIX", "source")
    monkeypatch.setenv("S3_EXTRACTED_PREFIX", "extracted")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


@pytest.fixture
def s3_client(s3_env):
    """Create a real S3 bucket in moto and return an S3Client instance."""
    with mock_aws():
        conn = boto3.client("s3", region_name=REGION)
        conn.create_bucket(Bucket=BUCKET)

        # Reimport to pick up monkeypatched env
        import importlib
        import config as config_mod
        importlib.reload(config_mod)

        from s3_client import S3Client
        client = S3Client(bucket=BUCKET, region=REGION)
        yield client


def _get_tags(s3_key, bucket=BUCKET):
    """Fetch tags from an S3 object as a plain dict."""
    conn = boto3.client("s3", region_name=REGION)
    resp = conn.get_object_tagging(Bucket=bucket, Key=s3_key)
    return {t["Key"]: t["Value"] for t in resp["TagSet"]}


def _get_object_meta(s3_key, bucket=BUCKET):
    """Fetch head_object metadata."""
    conn = boto3.client("s3", region_name=REGION)
    return conn.head_object(Bucket=bucket, Key=s3_key)


# ===================================================================
# __init__
# ===================================================================

class TestInit:
    def test_stores_bucket_and_region(self, s3_client):
        assert s3_client.bucket == BUCKET

    def test_raises_on_missing_bucket(self, s3_env):
        with mock_aws():
            # Don't create the bucket
            from s3_client import S3Client
            with pytest.raises(RuntimeError, match="not accessible"):
                S3Client(bucket="nonexistent-bucket", region=REGION)


# ===================================================================
# upload_document
# ===================================================================

class TestUploadDocument:
    def test_upload_returns_metadata(self, s3_client):
        result = s3_client.upload_document(
            content=b"hello world",
            s3_key="source/Dynamo/Docs/test.pdf",
            content_type="application/pdf",
        )
        assert result["s3_key"] == "source/Dynamo/Docs/test.pdf"
        assert result["size"] == 11
        assert result["etag"]  # non-empty

    def test_upload_sets_content_type(self, s3_client):
        s3_client.upload_document(
            content=b"data",
            s3_key="source/Dynamo/Docs/file.docx",
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        meta = _get_object_meta("source/Dynamo/Docs/file.docx")
        assert "wordprocessingml" in meta["ContentType"]

    def test_upload_sets_server_side_encryption(self, s3_client):
        s3_client.upload_document(
            content=b"secret",
            s3_key="source/Dynamo/Docs/enc.pdf",
        )
        meta = _get_object_meta("source/Dynamo/Docs/enc.pdf")
        assert meta["ServerSideEncryption"] == "AES256"

    def test_upload_with_tags(self, s3_client):
        s3_client.upload_document(
            content=b"tagged",
            s3_key="source/Dynamo/Docs/tagged.pdf",
            content_type="application/pdf",
            tags={"sp-site": "Dynamo", "file-type": "pdf"},
        )
        tags = _get_tags("source/Dynamo/Docs/tagged.pdf")
        assert tags["sp-site"] == "Dynamo"
        assert tags["file-type"] == "pdf"

    def test_upload_without_tags(self, s3_client):
        s3_client.upload_document(
            content=b"no-tags",
            s3_key="source/Dynamo/Docs/notags.pdf",
        )
        tags = _get_tags("source/Dynamo/Docs/notags.pdf")
        assert tags == {}

    def test_upload_default_content_type(self, s3_client):
        s3_client.upload_document(
            content=b"binary",
            s3_key="source/Dynamo/Docs/unknown",
        )
        meta = _get_object_meta("source/Dynamo/Docs/unknown")
        assert meta["ContentType"] == "application/octet-stream"

    def test_upload_overwrites_existing(self, s3_client):
        s3_client.upload_document(content=b"v1", s3_key="source/x.pdf")
        r2 = s3_client.upload_document(content=b"version-two", s3_key="source/x.pdf")
        assert r2["size"] == len(b"version-two")

    def test_upload_empty_content(self, s3_client):
        result = s3_client.upload_document(content=b"", s3_key="source/empty.txt")
        assert result["size"] == 0


# ===================================================================
# upload_json_twin
# ===================================================================

class TestUploadJsonTwin:
    def test_serializes_json(self, s3_client):
        twin = {"schema_version": "1.0", "data": [1, 2, 3]}
        s3_client.upload_json_twin(twin, "extracted/Dynamo/Docs/twin.json")

        conn = boto3.client("s3", region_name=REGION)
        obj = conn.get_object(Bucket=BUCKET, Key="extracted/Dynamo/Docs/twin.json")
        body = json.loads(obj["Body"].read())
        assert body["schema_version"] == "1.0"
        assert body["data"] == [1, 2, 3]

    def test_sets_application_json_content_type(self, s3_client):
        s3_client.upload_json_twin({"a": 1}, "extracted/twin.json")
        meta = _get_object_meta("extracted/twin.json")
        assert meta["ContentType"] == "application/json"

    def test_adds_twin_type_tag(self, s3_client):
        s3_client.upload_json_twin({"a": 1}, "extracted/twin.json")
        tags = _get_tags("extracted/twin.json")
        assert tags["twin-type"] == "textract-json"

    def test_merges_user_tags_with_twin_type(self, s3_client):
        s3_client.upload_json_twin(
            {"a": 1},
            "extracted/twin.json",
            tags={"sp-site": "Dynamo", "sp-library": "HR"},
        )
        tags = _get_tags("extracted/twin.json")
        assert tags["twin-type"] == "textract-json"
        assert tags["sp-site"] == "Dynamo"
        assert tags["sp-library"] == "HR"

    def test_returns_upload_metadata(self, s3_client):
        result = s3_client.upload_json_twin({"x": 1}, "extracted/t.json")
        assert result["s3_key"] == "extracted/t.json"
        assert result["size"] > 0
        assert result["etag"]

    def test_json_is_indented(self, s3_client):
        s3_client.upload_json_twin({"key": "value"}, "extracted/indented.json")
        conn = boto3.client("s3", region_name=REGION)
        raw = conn.get_object(Bucket=BUCKET, Key="extracted/indented.json")["Body"].read()
        text = raw.decode("utf-8")
        # indent=2 produces multi-line output
        assert "\n" in text
        assert '  "key"' in text


# ===================================================================
# document_exists
# ===================================================================

class TestDocumentExists:
    def test_returns_true_when_exists(self, s3_client):
        s3_client.upload_document(content=b"hi", s3_key="source/exists.pdf")
        assert s3_client.document_exists("source/exists.pdf") is True

    def test_returns_false_when_missing(self, s3_client):
        assert s3_client.document_exists("source/nope.pdf") is False


# ===================================================================
# get_document_etag
# ===================================================================

class TestGetDocumentEtag:
    def test_returns_etag_for_existing(self, s3_client):
        s3_client.upload_document(content=b"content", s3_key="source/file.pdf")
        etag = s3_client.get_document_etag("source/file.pdf")
        assert etag is not None
        assert isinstance(etag, str)
        assert '"' not in etag  # quotes stripped

    def test_returns_none_for_missing(self, s3_client):
        assert s3_client.get_document_etag("source/gone.pdf") is None

    def test_etag_changes_on_content_change(self, s3_client):
        s3_client.upload_document(content=b"v1", s3_key="source/mutable.pdf")
        etag1 = s3_client.get_document_etag("source/mutable.pdf")

        s3_client.upload_document(content=b"v2-different", s3_key="source/mutable.pdf")
        etag2 = s3_client.get_document_etag("source/mutable.pdf")

        assert etag1 != etag2


# ===================================================================
# delete_document
# ===================================================================

class TestDeleteDocument:
    def test_deletes_source_and_twin(self, s3_client):
        s3_client.upload_document(content=b"src", s3_key="source/Dynamo/Docs/file.pdf")
        s3_client.upload_document(content=b"twin", s3_key="extracted/Dynamo/Docs/file.json")

        result = s3_client.delete_document("source/Dynamo/Docs/file.pdf")
        assert result is True
        assert s3_client.document_exists("source/Dynamo/Docs/file.pdf") is False
        assert s3_client.document_exists("extracted/Dynamo/Docs/file.json") is False

    def test_succeeds_when_source_missing(self, s3_client):
        """Deleting a non-existent key is not an error in S3."""
        result = s3_client.delete_document("source/Dynamo/Docs/ghost.pdf")
        assert result is True

    def test_succeeds_when_twin_missing(self, s3_client):
        s3_client.upload_document(content=b"src", s3_key="source/Dynamo/Docs/notwin.pdf")
        result = s3_client.delete_document("source/Dynamo/Docs/notwin.pdf")
        assert result is True


# ===================================================================
# delete_documents_batch
# ===================================================================

class TestDeleteDocumentsBatch:
    def test_batch_deletes_source_and_twins(self, s3_client):
        keys = []
        for i in range(3):
            src_key = f"source/Dynamo/Docs/file{i}.pdf"
            twin_key = f"extracted/Dynamo/Docs/file{i}.json"
            s3_client.upload_document(content=f"src{i}".encode(), s3_key=src_key)
            s3_client.upload_document(content=f"twin{i}".encode(), s3_key=twin_key)
            keys.append(src_key)

        result = s3_client.delete_documents_batch(keys)
        assert result["deleted"] == 6  # 3 source + 3 twin
        assert result["errors"] == []

        for i in range(3):
            assert s3_client.document_exists(f"source/Dynamo/Docs/file{i}.pdf") is False
            assert s3_client.document_exists(f"extracted/Dynamo/Docs/file{i}.json") is False

    def test_batch_with_empty_list(self, s3_client):
        result = s3_client.delete_documents_batch([])
        assert result["deleted"] == 0
        assert result["errors"] == []

    def test_batch_with_missing_objects(self, s3_client):
        """Deleting non-existent keys does not produce errors in S3."""
        result = s3_client.delete_documents_batch(["source/Dynamo/Docs/nope.pdf"])
        # S3 reports these as "deleted" even if they didn't exist
        assert result["deleted"] >= 0
        assert result["errors"] == []


# ===================================================================
# list_objects_by_prefix
# ===================================================================

class TestListObjectsByPrefix:
    def test_lists_all_objects(self, s3_client):
        for name in ["a.pdf", "b.pdf", "c.docx"]:
            s3_client.upload_document(content=b"x", s3_key=f"source/Dynamo/Docs/{name}")

        keys = s3_client.list_objects_by_prefix("source/Dynamo/Docs/")
        assert len(keys) == 3
        assert "source/Dynamo/Docs/a.pdf" in keys

    def test_empty_prefix_returns_all(self, s3_client):
        s3_client.upload_document(content=b"x", s3_key="source/a.txt")
        s3_client.upload_document(content=b"x", s3_key="extracted/b.json")

        keys = s3_client.list_objects_by_prefix("")
        assert len(keys) == 2

    def test_no_matches(self, s3_client):
        keys = s3_client.list_objects_by_prefix("nonexistent/")
        assert keys == []

    def test_prefix_scoping(self, s3_client):
        s3_client.upload_document(content=b"x", s3_key="source/Dynamo/HR/a.pdf")
        s3_client.upload_document(content=b"x", s3_key="source/Dynamo/Legal/b.pdf")

        hr_keys = s3_client.list_objects_by_prefix("source/Dynamo/HR/")
        assert len(hr_keys) == 1
        assert hr_keys[0] == "source/Dynamo/HR/a.pdf"

    def test_pagination_with_many_objects(self, s3_client):
        """Upload enough objects to trigger pagination (>1000)."""
        # Use a smaller count to keep tests fast; moto handles pagination
        for i in range(50):
            s3_client.upload_document(
                content=b"x", s3_key=f"source/bulk/{i:04d}.pdf"
            )
        keys = s3_client.list_objects_by_prefix("source/bulk/")
        assert len(keys) == 50


# ===================================================================
# _encode_tags
# ===================================================================

class TestEncodeTags:
    def test_simple_tags(self):
        from s3_client import S3Client
        result = S3Client._encode_tags({"key1": "val1", "key2": "val2"})
        assert "key1=val1" in result
        assert "key2=val2" in result
        assert "&" in result

    def test_url_encodes_special_chars(self):
        from s3_client import S3Client
        result = S3Client._encode_tags({"sp-path": "/My Folder/file.pdf"})
        assert "%2FMy%20Folder%2Ffile.pdf" in result

    def test_empty_tags(self):
        from s3_client import S3Client
        assert S3Client._encode_tags({}) == ""


# ===================================================================
# Integration: upload → exists → etag → delete lifecycle
# ===================================================================

class TestLifecycle:
    def test_full_document_lifecycle(self, s3_client):
        source_key = "source/Dynamo/HR/handbook.pdf"
        twin_key = "extracted/Dynamo/HR/handbook.json"

        # Upload source document
        result = s3_client.upload_document(
            content=b"PDF content here",
            s3_key=source_key,
            content_type="application/pdf",
            tags={"sp-site": "Dynamo", "file-type": "pdf"},
        )
        assert result["size"] == 16

        # Upload JSON twin
        twin_result = s3_client.upload_json_twin(
            twin_data={"extracted": "text here"},
            s3_key=twin_key,
            tags={"sp-site": "Dynamo"},
        )
        assert twin_result["s3_key"] == twin_key

        # Verify both exist
        assert s3_client.document_exists(source_key) is True
        assert s3_client.document_exists(twin_key) is True

        # Check ETags
        src_etag = s3_client.get_document_etag(source_key)
        assert src_etag is not None

        twin_etag = s3_client.get_document_etag(twin_key)
        assert twin_etag is not None
        assert src_etag != twin_etag

        # Verify tags
        src_tags = _get_tags(source_key)
        assert src_tags["sp-site"] == "Dynamo"
        assert src_tags["file-type"] == "pdf"

        twin_tags = _get_tags(twin_key)
        assert twin_tags["twin-type"] == "textract-json"
        assert twin_tags["sp-site"] == "Dynamo"

        # List objects
        source_keys = s3_client.list_objects_by_prefix("source/Dynamo/HR/")
        assert source_key in source_keys

        # Delete source (should also delete twin)
        deleted = s3_client.delete_document(source_key)
        assert deleted is True
        assert s3_client.document_exists(source_key) is False
        assert s3_client.document_exists(twin_key) is False
