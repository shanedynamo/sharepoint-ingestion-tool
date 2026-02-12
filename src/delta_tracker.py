"""DynamoDB-backed delta token storage for Graph API incremental sync."""

import logging

import boto3

from config import config

logger = logging.getLogger(__name__)


class DeltaTracker:
    """Stores and retrieves Graph API delta tokens in DynamoDB.

    Table schema
    ------------
    - Table: sp-ingest-delta-tokens
    - PK: drive_id (String)
    - Attributes: delta_token, last_sync_at, items_processed, sync_count
    """

    def __init__(
        self,
        table_name: str | None = None,
        region: str | None = None,
    ):
        self._table_name = table_name or config.dynamodb_delta_table
        self._region = region or config.aws_region
        dynamo = boto3.resource("dynamodb", region_name=self._region)
        self._table = dynamo.Table(self._table_name)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_delta_token(self, drive_id: str) -> str | None:
        """Get the stored delta token for a given drive ID.

        Returns None if no token exists (triggers full crawl).
        """
        resp = self._table.get_item(Key={"drive_id": drive_id})
        item = resp.get("Item")
        if item:
            logger.info(
                "Found delta token for drive %s (last sync: %s)",
                drive_id,
                item.get("last_sync_at", "unknown"),
            )
            return item.get("delta_token")
        return None

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def save_delta_token(
        self,
        drive_id: str,
        token: str,
        last_sync: str,
        items_processed: int,
    ) -> None:
        """Store/update the delta token with sync metadata.

        Uses an atomic ADD on ``sync_count`` so it increments on each save
        without risk of lost updates.
        """
        self._table.update_item(
            Key={"drive_id": drive_id},
            UpdateExpression=(
                "SET delta_token = :token, "
                "last_sync_at = :sync, "
                "items_processed = :count "
                "ADD sync_count :one"
            ),
            ExpressionAttributeValues={
                ":token": token,
                ":sync": last_sync,
                ":count": items_processed,
                ":one": 1,
            },
        )
        logger.info(
            "Saved delta token for drive %s (%d items processed)",
            drive_id,
            items_processed,
        )
