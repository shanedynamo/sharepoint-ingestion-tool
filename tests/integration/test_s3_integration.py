"""Integration tests for S3 operations against LocalStack."""

import json

import pytest

from s3_client import S3Client
from utils.path_mapper import PathMapper


pytestmark = pytest.mark.integration

BUCKET = "dynamo-ai-documents"
REGION = "us-east-1"


@pytest.fixture
def s3(localstack_env):
    """Return an S3Client wired to LocalStack."""
    return S3Client(bucket=BUCKET, region=REGION)


class TestUploadDocument:
    def test_upload_and_verify_exists(self, s3, clean_s3):
        content = b"Hello, integration test!"
        key = "source/Dynamo/HR/test-doc.pdf"

        result = s3.upload_document(content, key, content_type="application/pdf")

        assert result["s3_key"] == key
        assert result["size"] == len(content)
        assert result["etag"]
        assert s3.document_exists(key)

    def test_upload_with_tags(self, s3, s3_client_raw, clean_s3):
        content = b"Tagged document content"
        key = "source/Dynamo/HR/tagged-doc.pdf"
        tags = {"sp-site": "Dynamo", "sp-library": "HR", "file-type": "pdf"}

        s3.upload_document(content, key, tags=tags)

        resp = s3_client_raw.get_object_tagging(Bucket=BUCKET, Key=key)
        actual_tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert actual_tags["sp-site"] == "Dynamo"
        assert actual_tags["sp-library"] == "HR"
        assert actual_tags["file-type"] == "pdf"

    def test_upload_json_twin(self, s3, s3_client_raw, clean_s3):
        twin_data = {
            "schema_version": "2.0",
            "extracted_text": "Hello world",
            "pages": [{"page_number": 1, "text": "Hello world"}],
        }
        key = "extracted/Dynamo/HR/test-doc.json"

        result = s3.upload_json_twin(twin_data, key)

        assert result["s3_key"] == key
        # Verify content
        resp = s3_client_raw.get_object(Bucket=BUCKET, Key=key)
        body = json.loads(resp["Body"].read())
        assert body["schema_version"] == "2.0"
        assert body["extracted_text"] == "Hello world"

        # Verify twin-type tag
        tag_resp = s3_client_raw.get_object_tagging(Bucket=BUCKET, Key=key)
        actual_tags = {t["Key"]: t["Value"] for t in tag_resp["TagSet"]}
        assert actual_tags["twin-type"] == "textract-json"


class TestDocumentExists:
    def test_exists_returns_true(self, s3, clean_s3):
        key = "source/Dynamo/test/exists.txt"
        s3.upload_document(b"content", key)
        assert s3.document_exists(key) is True

    def test_exists_returns_false(self, s3, clean_s3):
        assert s3.document_exists("source/Dynamo/no/such/file.txt") is False


class TestGetDocumentEtag:
    def test_etag_returned(self, s3, clean_s3):
        key = "source/Dynamo/test/etag.txt"
        s3.upload_document(b"etag content", key)
        etag = s3.get_document_etag(key)
        assert etag is not None
        assert isinstance(etag, str)
        assert len(etag) > 0

    def test_etag_none_for_missing(self, s3, clean_s3):
        assert s3.get_document_etag("source/no/such/key.txt") is None


class TestDeleteDocument:
    def test_delete_removes_source_and_twin(self, s3, clean_s3):
        source_key = "source/Dynamo/HR/to-delete.pdf"
        twin_key = "extracted/Dynamo/HR/to-delete.json"

        s3.upload_document(b"source", source_key)
        s3.upload_document(b"twin", twin_key)

        result = s3.delete_document(source_key)

        assert result is True
        assert s3.document_exists(source_key) is False
        assert s3.document_exists(twin_key) is False

    def test_delete_nonexistent_succeeds(self, s3, clean_s3):
        result = s3.delete_document("source/Dynamo/no/such/file.pdf")
        assert result is True


class TestListObjectsByPrefix:
    def test_list_returns_matching_keys(self, s3, clean_s3):
        prefix = "source/Dynamo/ListTest/"
        keys = [f"{prefix}file{i}.txt" for i in range(5)]
        for key in keys:
            s3.upload_document(b"content", key)

        listed = s3.list_objects_by_prefix(prefix)

        assert sorted(listed) == sorted(keys)

    def test_list_empty_prefix(self, s3, clean_s3):
        listed = s3.list_objects_by_prefix("source/Dynamo/empty-prefix/")
        assert listed == []
