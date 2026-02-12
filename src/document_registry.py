"""DynamoDB document registry for tracking every ingested document."""

import logging
from collections import Counter
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key

from config import config

logger = logging.getLogger(__name__)


class DocumentRegistry:
    """Tracks every document through the ingest -> extract -> twin lifecycle.

    Table schema
    ------------
    - Table: sp-ingest-document-registry
    - PK: s3_source_key (String)
    - GSI: textract_status-index (PK: textract_status, SK: ingested_at)
    - GSI: sp_library-index (PK: sp_library, SK: sp_last_modified)
    """

    def __init__(
        self,
        table_name: str | None = None,
        region: str | None = None,
    ):
        self._table_name = table_name or config.dynamodb_registry_table
        self._region = region or config.aws_region
        dynamo = boto3.resource("dynamodb", region_name=self._region)
        self._table = dynamo.Table(self._table_name)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def register_document(self, doc: dict) -> None:
        """Upsert a document record.

        Required keys in *doc*:
            s3_source_key, sp_item_id, sp_path, sp_library,
            file_type, size_bytes.

        Optional keys (defaults applied if missing):
            sp_last_modified, s3_twin_key, textract_status,
            textract_job_id.
        """
        now = datetime.now(timezone.utc).isoformat()
        item = {
            "s3_source_key": doc["s3_source_key"],
            "sp_item_id": doc["sp_item_id"],
            "sp_path": doc["sp_path"],
            "sp_library": doc["sp_library"],
            "sp_last_modified": doc.get("sp_last_modified", ""),
            "s3_twin_key": doc.get("s3_twin_key"),
            "textract_status": doc.get("textract_status", "pending"),
            "textract_job_id": doc.get("textract_job_id"),
            "file_type": doc["file_type"],
            "size_bytes": doc["size_bytes"],
            "ingested_at": doc.get("ingested_at", now),
            "updated_at": now,
        }
        self._table.put_item(Item=item)
        logger.info("Registered document: %s", doc["s3_source_key"])

    def update_textract_status(
        self,
        s3_key: str,
        status: str,
        job_id: str | None = None,
        twin_key: str | None = None,
    ) -> None:
        """Update the Textract processing status for a document."""
        now = datetime.now(timezone.utc).isoformat()
        update_expr = "SET textract_status = :status, updated_at = :now"
        values: dict = {":status": status, ":now": now}

        if job_id is not None:
            update_expr += ", textract_job_id = :jid"
            values[":jid"] = job_id

        if twin_key is not None:
            update_expr += ", s3_twin_key = :tkey"
            values[":tkey"] = twin_key

        self._table.update_item(
            Key={"s3_source_key": s3_key},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=values,
        )
        logger.info("Updated textract_status for %s -> %s", s3_key, status)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_document(self, s3_key: str) -> dict | None:
        """Retrieve a document record by its S3 source key."""
        resp = self._table.get_item(Key={"s3_source_key": s3_key})
        return resp.get("Item")

    def get_pending_textract(self) -> list[dict]:
        """Query for documents where textract_status = 'pending'.

        Uses the textract_status-index GSI.  Paginates automatically.
        """
        return self._query_by_textract_status("pending")

    def get_failed_textract(self) -> list[dict]:
        """Query for documents where textract_status = 'failed'.

        Uses the textract_status-index GSI.  Paginates automatically.
        """
        return self._query_by_textract_status("failed")

    def _query_by_textract_status(self, status: str) -> list[dict]:
        """Paginated GSI query helper."""
        items: list[dict] = []
        kwargs: dict = {
            "IndexName": "textract_status-index",
            "KeyConditionExpression": Key("textract_status").eq(status),
        }
        while True:
            resp = self._table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
        return items

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete_document(self, s3_key: str) -> None:
        """Remove a document from the registry."""
        self._table.delete_item(Key={"s3_source_key": s3_key})
        logger.info("Deleted registry entry: %s", s3_key)

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        """Scan and aggregate: total docs, by type, by status, by library."""
        by_type: Counter = Counter()
        by_status: Counter = Counter()
        by_library: Counter = Counter()
        total = 0

        kwargs: dict = {}
        while True:
            resp = self._table.scan(**kwargs)
            for item in resp.get("Items", []):
                total += 1
                by_type[item.get("file_type", "unknown")] += 1
                by_status[item.get("textract_status", "unknown")] += 1
                by_library[item.get("sp_library", "unknown")] += 1
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key

        return {
            "total": total,
            "by_type": dict(by_type),
            "by_status": dict(by_status),
            "by_library": dict(by_library),
        }
