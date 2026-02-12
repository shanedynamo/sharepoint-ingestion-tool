#!/usr/bin/env bash
# ===================================================================
# retry-failed-textract.sh — Re-trigger Textract for failed documents
#
# Usage:
#   ./scripts/retry-failed-textract.sh              Retry all failed
#   ./scripts/retry-failed-textract.sh --dry-run     Show what would be retried
#   ./scripts/retry-failed-textract.sh --limit 50    Retry up to 50 documents
# ===================================================================
set -euo pipefail

# Constants
REGISTRY_TABLE="sp-ingest-document-registry"
S3_BUCKET="dynamo-ai-documents"
REGION="us-east-1"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# Options
DRY_RUN=false
LIMIT=0  # 0 = no limit

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)  DRY_RUN=true; shift ;;
        --limit)    LIMIT="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--dry-run] [--limit N]"
            echo ""
            echo "  --dry-run   Show failed documents without retrying"
            echo "  --limit N   Retry at most N documents (default: all)"
            exit 0
            ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
    esac
done

# Pre-flight
if ! command -v aws &>/dev/null; then
    echo -e "${RED}ERROR: aws CLI not found${NC}"
    exit 1
fi

if ! python3 -c "import boto3" 2>/dev/null; then
    echo -e "${RED}ERROR: boto3 not available for Python3${NC}"
    exit 1
fi

echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}${BOLD}  Retry Failed Textract Jobs${NC}"
echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if $DRY_RUN; then
    echo -e "${YELLOW}DRY RUN — no changes will be made${NC}"
    echo ""
fi

# Query for failed documents and process them
python3 << 'PYEOF'
import boto3
import json
import os
import sys
from datetime import datetime, timezone

DRY_RUN = os.environ.get("DRY_RUN", "false") == "true"
LIMIT = int(os.environ.get("LIMIT", "0"))
REGION = os.environ.get("REGION", "us-east-1")
TABLE = os.environ.get("REGISTRY_TABLE", "sp-ingest-document-registry")
BUCKET = os.environ.get("S3_BUCKET", "dynamo-ai-documents")

RED = "\033[0;31m"
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
CYAN = "\033[0;36m"
DIM = "\033[2m"
NC = "\033[0m"

ddb = boto3.resource("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
table = ddb.Table(TABLE)

# Query the textract_status-index GSI for failed documents
failed_docs = []
kwargs = {
    "IndexName": "textract_status-index",
    "KeyConditionExpression": boto3.dynamodb.conditions.Key("textract_status").eq("failed"),
    "ProjectionExpression": "s3_source_key, sp_path, sp_library, file_type, updated_at",
}

while True:
    resp = table.query(**kwargs)
    failed_docs.extend(resp.get("Items", []))
    if "LastEvaluatedKey" not in resp:
        break
    kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

if not failed_docs:
    print(f"{GREEN}No failed Textract jobs found.{NC}")
    sys.exit(0)

print(f"Found {YELLOW}{len(failed_docs)}{NC} failed document(s)")
print()

if LIMIT > 0:
    failed_docs = failed_docs[:LIMIT]
    print(f"{DIM}Processing up to {LIMIT} document(s){NC}")
    print()

retried = 0
errors = 0

for doc in failed_docs:
    s3_key = doc["s3_source_key"]
    sp_path = doc.get("sp_path", "?")
    file_type = doc.get("file_type", "?")
    updated = doc.get("updated_at", "?")

    print(f"  {CYAN}{s3_key}{NC}")
    print(f"    SP path: {sp_path}  |  type: {file_type}  |  failed at: {updated}")

    if DRY_RUN:
        print(f"    {DIM}(would retry){NC}")
        retried += 1
        continue

    try:
        # Step 1: Verify the source object exists in S3
        try:
            s3.head_object(Bucket=BUCKET, Key=s3_key)
        except s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"    {RED}SKIP: source object not found in S3{NC}")
                errors += 1
                continue
            raise

        # Step 2: Copy the S3 object to itself to generate a new PutObject event
        s3.copy_object(
            Bucket=BUCKET,
            Key=s3_key,
            CopySource={"Bucket": BUCKET, "Key": s3_key},
            MetadataDirective="COPY",
            TaggingDirective="COPY",
        )

        # Step 3: Update registry status to "pending"
        now = datetime.now(timezone.utc).isoformat()
        table.update_item(
            Key={"s3_source_key": s3_key},
            UpdateExpression="SET textract_status = :s, updated_at = :t REMOVE textract_job_id",
            ExpressionAttributeValues={
                ":s": "pending",
                ":t": now,
            },
        )

        print(f"    {GREEN}Retried successfully{NC}")
        retried += 1

    except Exception as e:
        print(f"    {RED}ERROR: {e}{NC}")
        errors += 1

# Summary
print()
print(f"{CYAN}{'━' * 44}{NC}")
if DRY_RUN:
    print(f"  Would retry: {YELLOW}{retried}{NC} document(s)")
else:
    print(f"  Retried:     {GREEN}{retried}{NC} document(s)")
    if errors > 0:
        print(f"  Errors:      {RED}{errors}{NC}")
    print()
    print(f"  {DIM}Documents will be re-processed by the textract_trigger Lambda.{NC}")
    print(f"  {DIM}Monitor progress: ./scripts/monitor-bulk-ingest.sh --stats{NC}")
print()
PYEOF
