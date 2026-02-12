#!/usr/bin/env bash
# Initialise LocalStack resources for integration testing.
# This script is mounted into LocalStack's ready.d directory and runs
# automatically when the container starts, or can be run manually.
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="us-east-1"

aws="aws --endpoint-url=$ENDPOINT --region $REGION"

echo "==> Creating S3 bucket..."
$aws s3 mb s3://dynamo-ai-documents 2>/dev/null || true

echo "==> Creating DynamoDB table: sp-ingest-delta-tokens..."
$aws dynamodb create-table \
    --table-name sp-ingest-delta-tokens \
    --key-schema AttributeName=drive_id,KeyType=HASH \
    --attribute-definitions AttributeName=drive_id,AttributeType=S \
    --billing-mode PAY_PER_REQUEST \
    2>/dev/null || true

echo "==> Creating DynamoDB table: sp-ingest-document-registry..."
$aws dynamodb create-table \
    --table-name sp-ingest-document-registry \
    --key-schema AttributeName=s3_source_key,KeyType=HASH \
    --attribute-definitions \
        AttributeName=s3_source_key,AttributeType=S \
        AttributeName=textract_status,AttributeType=S \
        AttributeName=ingested_at,AttributeType=S \
        AttributeName=sp_library,AttributeType=S \
        AttributeName=sp_last_modified,AttributeType=S \
    --global-secondary-indexes \
        'IndexName=textract_status-index,KeySchema=[{AttributeName=textract_status,KeyType=HASH},{AttributeName=ingested_at,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
        'IndexName=sp_library-index,KeySchema=[{AttributeName=sp_library,KeyType=HASH},{AttributeName=sp_last_modified,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    --billing-mode PAY_PER_REQUEST \
    2>/dev/null || true

echo "==> Creating SNS topic: textract-notifications..."
TOPIC_ARN=$($aws sns create-topic --name textract-notifications --query 'TopicArn' --output text)
echo "    Topic ARN: $TOPIC_ARN"

echo "==> Creating SQS queue: textract-complete-queue..."
QUEUE_URL=$($aws sqs create-queue --queue-name textract-complete-queue --query 'QueueUrl' --output text)
QUEUE_ARN=$($aws sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
echo "    Queue URL: $QUEUE_URL"
echo "    Queue ARN: $QUEUE_ARN"

echo "==> Subscribing SQS queue to SNS topic..."
$aws sns subscribe \
    --topic-arn "$TOPIC_ARN" \
    --protocol sqs \
    --notification-endpoint "$QUEUE_ARN" \
    >/dev/null

echo "==> LocalStack initialisation complete."
