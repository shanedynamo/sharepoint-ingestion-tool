"""Integration test configuration — connects to LocalStack for real AWS calls.

All integration tests are skipped when LocalStack is not reachable.
"""

import os
import importlib

import boto3
import pytest
import urllib.request

LOCALSTACK_URL = "http://localhost:4566"
REGION = "us-east-1"
BUCKET = "dynamo-ai-documents"
DELTA_TABLE = "sp-ingest-delta-tokens"
REGISTRY_TABLE = "sp-ingest-document-registry"
SNS_TOPIC_NAME = "textract-notifications"
SQS_QUEUE_NAME = "textract-complete-queue"


def _localstack_available() -> bool:
    """Return True if LocalStack health endpoint responds."""
    try:
        req = urllib.request.Request(
            f"{LOCALSTACK_URL}/_localstack/health", method="GET",
        )
        with urllib.request.urlopen(req, timeout=2):
            return True
    except Exception:
        return False


# Skip the entire integration test directory if LocalStack isn't running.
pytestmark = pytest.mark.integration

pytest_plugins = []


def pytest_collection_modifyitems(config, items):
    """Skip integration tests when LocalStack is unreachable."""
    if _localstack_available():
        return
    skip = pytest.mark.skip(reason="LocalStack not reachable at localhost:4566")
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def localstack_env():
    """Set environment variables so all boto3 clients point at LocalStack."""
    env_overrides = {
        "AWS_ENDPOINT_URL": LOCALSTACK_URL,
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_DEFAULT_REGION": REGION,
        "AWS_REGION": REGION,
        "S3_BUCKET": BUCKET,
        "S3_SOURCE_PREFIX": "source",
        "S3_EXTRACTED_PREFIX": "extracted",
        "DYNAMODB_DELTA_TABLE": DELTA_TABLE,
        "DYNAMODB_REGISTRY_TABLE": REGISTRY_TABLE,
    }
    original = {}
    for key, value in env_overrides.items():
        original[key] = os.environ.get(key)
        os.environ[key] = value

    # Reload config module to pick up new env vars
    import config as config_mod
    importlib.reload(config_mod)

    yield

    # Restore original environment
    for key, orig_value in original.items():
        if orig_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = orig_value
    importlib.reload(config_mod)


@pytest.fixture(scope="session")
def s3_client_raw(localstack_env):
    """Low-level boto3 S3 client pointing at LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_URL,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="session")
def dynamodb_resource(localstack_env):
    """boto3 DynamoDB resource pointing at LocalStack."""
    return boto3.resource(
        "dynamodb",
        endpoint_url=LOCALSTACK_URL,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="session")
def sns_client(localstack_env):
    """boto3 SNS client pointing at LocalStack."""
    return boto3.client(
        "sns",
        endpoint_url=LOCALSTACK_URL,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="session")
def sqs_client(localstack_env):
    """boto3 SQS client pointing at LocalStack."""
    return boto3.client(
        "sqs",
        endpoint_url=LOCALSTACK_URL,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="session")
def sns_topic_arn(sns_client):
    """Return the ARN of the textract-notifications topic."""
    resp = sns_client.create_topic(Name=SNS_TOPIC_NAME)
    return resp["TopicArn"]


@pytest.fixture(scope="session")
def sqs_queue_url(sqs_client):
    """Return the URL of the textract-complete-queue."""
    resp = sqs_client.create_queue(QueueName=SQS_QUEUE_NAME)
    return resp["QueueUrl"]


@pytest.fixture
def clean_s3(s3_client_raw):
    """Clean all objects from the test bucket after each test."""
    yield
    paginator = s3_client_raw.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            s3_client_raw.delete_objects(
                Bucket=BUCKET, Delete={"Objects": objects},
            )


@pytest.fixture
def clean_delta_table(dynamodb_resource):
    """Clean all items from the delta tokens table after each test."""
    yield
    table = dynamodb_resource.Table(DELTA_TABLE)
    scan = table.scan(ProjectionExpression="drive_id")
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"drive_id": item["drive_id"]})


@pytest.fixture
def clean_registry_table(dynamodb_resource):
    """Clean all items from the registry table after each test."""
    yield
    table = dynamodb_resource.Table(REGISTRY_TABLE)
    scan = table.scan(ProjectionExpression="s3_source_key")
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"s3_source_key": item["s3_source_key"]})
