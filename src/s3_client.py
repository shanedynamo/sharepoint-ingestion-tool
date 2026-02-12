"""S3 operations for document storage, retrieval, tagging, and deletion."""

import json
import logging
from urllib.parse import quote

import boto3
from botocore.exceptions import ClientError

from config import config
from utils.path_mapper import PathMapper

logger = logging.getLogger(__name__)

# S3 delete_objects accepts at most 1000 keys per call.
DELETE_BATCH_LIMIT = 1000


class S3Client:
    """Manages all S3 interactions for the SharePoint ingest pipeline."""

    def __init__(
        self,
        bucket: str | None = None,
        region: str | None = None,
    ):
        self.bucket = bucket or config.s3_bucket
        self._region = region or config.aws_region
        self._s3 = boto3.client("s3", region_name=self._region)
        self._mapper = PathMapper(
            self.bucket, config.s3_source_prefix, config.s3_extracted_prefix,
        )

        # Verify bucket exists
        try:
            self._s3.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            raise RuntimeError(
                f"S3 bucket '{self.bucket}' is not accessible (HTTP {code})"
            ) from exc

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def upload_document(
        self,
        content: bytes,
        s3_key: str,
        content_type: str = "application/octet-stream",
        tags: dict[str, str] | None = None,
    ) -> dict:
        """Upload a document to S3 with optional tags.

        Returns ``{"s3_key": ..., "etag": ..., "size": ...}``.
        """
        kwargs: dict = {
            "Bucket": self.bucket,
            "Key": s3_key,
            "Body": content,
            "ContentType": content_type,
            "ServerSideEncryption": "AES256",
        }
        if tags:
            kwargs["Tagging"] = self._encode_tags(tags)

        resp = self._s3.put_object(**kwargs)
        etag = resp.get("ETag", "").strip('"')
        size = len(content)

        logger.info("Uploaded s3://%s/%s (%d bytes)", self.bucket, s3_key, size)
        return {"s3_key": s3_key, "etag": etag, "size": size}

    def upload_json_twin(
        self,
        twin_data: dict,
        s3_key: str,
        tags: dict[str, str] | None = None,
    ) -> dict:
        """Serialize *twin_data* to JSON and upload to S3.

        Adds ``twin-type=textract-json`` to the tag set.
        Returns the same dict as :meth:`upload_document`.
        """
        merged_tags = dict(tags) if tags else {}
        merged_tags["twin-type"] = "textract-json"

        body = json.dumps(twin_data, indent=2, default=str).encode("utf-8")
        return self.upload_document(
            content=body,
            s3_key=s3_key,
            content_type="application/json",
            tags=merged_tags,
        )

    # ------------------------------------------------------------------
    # Query / existence
    # ------------------------------------------------------------------

    def document_exists(self, s3_key: str) -> bool:
        """Return True if an object exists at *s3_key*."""
        try:
            self._s3.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False
            raise

    def get_document_etag(self, s3_key: str) -> str | None:
        """Return the ETag of an existing object, or None if it does not exist."""
        try:
            resp = self._s3.head_object(Bucket=self.bucket, Key=s3_key)
            return resp["ETag"].strip('"')
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return None
            raise

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete_document(self, s3_key: str) -> bool:
        """Delete a source document and its corresponding extracted twin.

        Returns True if both deletions succeeded (or objects didn't exist).
        """
        twin_key = self._mapper.to_s3_extracted_key(s3_key)

        source_ok = self._delete_single(s3_key)
        twin_ok = self._delete_single(twin_key)

        logger.info(
            "Deleted s3://%s/%s (source=%s, twin=%s)",
            self.bucket, s3_key, source_ok, twin_ok,
        )
        return source_ok and twin_ok

    def delete_documents_batch(self, s3_keys: list[str]) -> dict:
        """Batch-delete source documents and their twins.

        Handles the 1000-object-per-call S3 limit internally.
        Returns ``{"deleted": count, "errors": [...]}``.
        """
        # Build the full set: source keys + their twin keys
        all_keys: list[str] = []
        for key in s3_keys:
            all_keys.append(key)
            all_keys.append(self._mapper.to_s3_extracted_key(key))

        deleted = 0
        errors: list[dict] = []

        for i in range(0, len(all_keys), DELETE_BATCH_LIMIT):
            batch = all_keys[i : i + DELETE_BATCH_LIMIT]
            objects = [{"Key": k} for k in batch]

            resp = self._s3.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": objects, "Quiet": False},
            )
            deleted += len(resp.get("Deleted", []))
            for err in resp.get("Errors", []):
                errors.append({"key": err["Key"], "code": err["Code"], "message": err["Message"]})

        logger.info("Batch delete: %d deleted, %d errors", deleted, len(errors))
        return {"deleted": deleted, "errors": errors}

    def _delete_single(self, s3_key: str) -> bool:
        """Delete a single object.  Returns True on success (including 'not found')."""
        try:
            self._s3.delete_object(Bucket=self.bucket, Key=s3_key)
            return True
        except ClientError:
            logger.exception("Failed to delete s3://%s/%s", self.bucket, s3_key)
            return False

    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------

    def list_objects_by_prefix(self, prefix: str) -> list[str]:
        """Return all object keys under *prefix*, paginating automatically."""
        keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _encode_tags(tags: dict[str, str]) -> str:
        """Encode a tag dict to the ``Key1=Value1&Key2=Value2`` format
        expected by S3 ``put_object(Tagging=...)``."""
        parts = []
        for k, v in tags.items():
            parts.append(f"{quote(k, safe='')}={quote(v, safe='')}")
        return "&".join(parts)
