"""Tests for DeltaTracker using moto to mock DynamoDB."""

import sys

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, "src")

TABLE_NAME = "test-delta-tokens"
REGION = "us-east-1"


@pytest.fixture
def dynamo_env(monkeypatch):
    """Set env vars so config + DeltaTracker use our test table."""
    monkeypatch.setenv("DYNAMODB_DELTA_TABLE", TABLE_NAME)
    monkeypatch.setenv("AWS_REGION", REGION)
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")


def _create_table():
    """Create the delta-tokens DynamoDB table in moto."""
    dynamo = boto3.client("dynamodb", region_name=REGION)
    dynamo.create_table(
        TableName=TABLE_NAME,
        KeySchema=[{"AttributeName": "drive_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "drive_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


@pytest.fixture
def tracker(dynamo_env):
    """Create the DynamoDB table and return a DeltaTracker instance."""
    with mock_aws():
        _create_table()

        import importlib
        import config as config_mod
        importlib.reload(config_mod)

        from delta_tracker import DeltaTracker
        dt = DeltaTracker(table_name=TABLE_NAME, region=REGION)
        yield dt


# ===================================================================
# get_delta_token
# ===================================================================

class TestGetDeltaToken:
    def test_returns_none_when_no_token(self, tracker):
        assert tracker.get_delta_token("drive-1") is None

    def test_returns_none_for_unknown_drive(self, tracker):
        tracker.save_delta_token("drive-1", "tok-1", "2025-01-01T00:00:00Z", 10)
        assert tracker.get_delta_token("drive-999") is None


# ===================================================================
# save_delta_token
# ===================================================================

class TestSaveDeltaToken:
    def test_stores_and_retrieves_token(self, tracker):
        tracker.save_delta_token("drive-1", "token-abc", "2025-01-15T12:00:00Z", 42)
        assert tracker.get_delta_token("drive-1") == "token-abc"

    def test_stores_metadata(self, tracker):
        tracker.save_delta_token("drive-1", "tok", "2025-06-01T08:30:00Z", 7)

        # Read the raw item to verify metadata
        table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
        item = table.get_item(Key={"drive_id": "drive-1"})["Item"]

        assert item["last_sync_at"] == "2025-06-01T08:30:00Z"
        assert item["items_processed"] == 7
        assert item["sync_count"] == 1

    def test_updates_existing_token(self, tracker):
        tracker.save_delta_token("drive-1", "old-token", "2025-01-01T00:00:00Z", 5)
        tracker.save_delta_token("drive-1", "new-token", "2025-01-02T00:00:00Z", 12)

        assert tracker.get_delta_token("drive-1") == "new-token"

    def test_atomic_sync_count_increment(self, tracker):
        tracker.save_delta_token("drive-1", "t1", "2025-01-01T00:00:00Z", 10)
        tracker.save_delta_token("drive-1", "t2", "2025-01-02T00:00:00Z", 20)
        tracker.save_delta_token("drive-1", "t3", "2025-01-03T00:00:00Z", 30)

        table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
        item = table.get_item(Key={"drive_id": "drive-1"})["Item"]

        assert item["sync_count"] == 3
        assert item["delta_token"] == "t3"
        assert item["items_processed"] == 30

    def test_sync_count_starts_at_one(self, tracker):
        tracker.save_delta_token("drive-1", "tok", "2025-01-01T00:00:00Z", 0)

        table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
        item = table.get_item(Key={"drive_id": "drive-1"})["Item"]
        assert item["sync_count"] == 1


# ===================================================================
# Multiple drives
# ===================================================================

class TestMultipleDrives:
    def test_independent_tokens_per_drive(self, tracker):
        tracker.save_delta_token("drive-a", "token-a", "2025-01-01T00:00:00Z", 5)
        tracker.save_delta_token("drive-b", "token-b", "2025-01-02T00:00:00Z", 10)

        assert tracker.get_delta_token("drive-a") == "token-a"
        assert tracker.get_delta_token("drive-b") == "token-b"

    def test_updating_one_drive_does_not_affect_other(self, tracker):
        tracker.save_delta_token("drive-a", "tok-a1", "2025-01-01T00:00:00Z", 5)
        tracker.save_delta_token("drive-b", "tok-b1", "2025-01-01T00:00:00Z", 3)
        tracker.save_delta_token("drive-a", "tok-a2", "2025-01-02T00:00:00Z", 8)

        assert tracker.get_delta_token("drive-a") == "tok-a2"
        assert tracker.get_delta_token("drive-b") == "tok-b1"

    def test_sync_counts_are_independent(self, tracker):
        tracker.save_delta_token("drive-a", "ta", "2025-01-01T00:00:00Z", 1)
        tracker.save_delta_token("drive-a", "ta2", "2025-01-02T00:00:00Z", 2)
        tracker.save_delta_token("drive-b", "tb", "2025-01-01T00:00:00Z", 1)

        table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
        a = table.get_item(Key={"drive_id": "drive-a"})["Item"]
        b = table.get_item(Key={"drive_id": "drive-b"})["Item"]

        assert a["sync_count"] == 2
        assert b["sync_count"] == 1


# ===================================================================
# __init__
# ===================================================================

class TestInit:
    def test_explicit_params(self, dynamo_env):
        with mock_aws():
            _create_table()
            from delta_tracker import DeltaTracker
            dt = DeltaTracker(table_name=TABLE_NAME, region=REGION)
            assert dt._table_name == TABLE_NAME
            assert dt._region == REGION

    def test_defaults_from_config(self, dynamo_env):
        with mock_aws():
            _create_table()
            import importlib
            import config as config_mod
            importlib.reload(config_mod)
            import delta_tracker as dt_mod
            importlib.reload(dt_mod)
            dt = dt_mod.DeltaTracker()
            assert dt._table_name == TABLE_NAME


# ===================================================================
# Integration: full sync cycle
# ===================================================================

class TestLifecycle:
    def test_full_sync_lifecycle(self, tracker):
        drive = "drive-lifecycle"

        # Initially no token -> full crawl
        assert tracker.get_delta_token(drive) is None

        # First sync
        tracker.save_delta_token(drive, "delta-1", "2025-01-01T00:00:00Z", 100)
        assert tracker.get_delta_token(drive) == "delta-1"

        # Second sync with updated token
        tracker.save_delta_token(drive, "delta-2", "2025-01-02T12:00:00Z", 15)
        assert tracker.get_delta_token(drive) == "delta-2"

        # Verify accumulated metadata
        table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)
        item = table.get_item(Key={"drive_id": drive})["Item"]
        assert item["sync_count"] == 2
        assert item["last_sync_at"] == "2025-01-02T12:00:00Z"
        assert item["items_processed"] == 15
