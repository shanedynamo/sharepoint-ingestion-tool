"""Integration tests for DeltaTracker against LocalStack DynamoDB."""

from decimal import Decimal

import pytest

from delta_tracker import DeltaTracker


pytestmark = pytest.mark.integration

TABLE_NAME = "sp-ingest-delta-tokens"
REGION = "us-east-1"


@pytest.fixture
def tracker(localstack_env):
    """Return a DeltaTracker wired to LocalStack."""
    return DeltaTracker(table_name=TABLE_NAME, region=REGION)


class TestSaveAndRetrieve:
    def test_save_and_get_token(self, tracker, clean_delta_table):
        tracker.save_delta_token(
            drive_id="drive-001",
            token="delta-token-abc",
            last_sync="2025-06-01T12:00:00Z",
            items_processed=42,
        )

        token = tracker.get_delta_token("drive-001")
        assert token == "delta-token-abc"

    def test_get_nonexistent_returns_none(self, tracker, clean_delta_table):
        token = tracker.get_delta_token("no-such-drive")
        assert token is None

    def test_save_overwrites_token(self, tracker, clean_delta_table):
        tracker.save_delta_token(
            drive_id="drive-002",
            token="first-token",
            last_sync="2025-06-01T12:00:00Z",
            items_processed=10,
        )
        tracker.save_delta_token(
            drive_id="drive-002",
            token="second-token",
            last_sync="2025-06-02T12:00:00Z",
            items_processed=20,
        )

        token = tracker.get_delta_token("drive-002")
        assert token == "second-token"


class TestAtomicSyncCount:
    def test_sync_count_increments(self, tracker, dynamodb_resource, clean_delta_table):
        drive_id = "drive-counter"

        tracker.save_delta_token(
            drive_id=drive_id,
            token="token-v1",
            last_sync="2025-06-01T12:00:00Z",
            items_processed=5,
        )

        # Verify sync_count is 1 after first save
        table = dynamodb_resource.Table(TABLE_NAME)
        item = table.get_item(Key={"drive_id": drive_id})["Item"]
        assert item["sync_count"] == Decimal("1")

        # Save again
        tracker.save_delta_token(
            drive_id=drive_id,
            token="token-v2",
            last_sync="2025-06-02T12:00:00Z",
            items_processed=10,
        )

        item = table.get_item(Key={"drive_id": drive_id})["Item"]
        assert item["sync_count"] == Decimal("2")
        assert item["delta_token"] == "token-v2"
        assert item["items_processed"] == Decimal("10")

    def test_multiple_increments(self, tracker, dynamodb_resource, clean_delta_table):
        drive_id = "drive-multi"

        for i in range(5):
            tracker.save_delta_token(
                drive_id=drive_id,
                token=f"token-{i}",
                last_sync=f"2025-06-0{i + 1}T12:00:00Z",
                items_processed=i * 10,
            )

        table = dynamodb_resource.Table(TABLE_NAME)
        item = table.get_item(Key={"drive_id": drive_id})["Item"]
        assert item["sync_count"] == Decimal("5")
        assert item["delta_token"] == "token-4"
