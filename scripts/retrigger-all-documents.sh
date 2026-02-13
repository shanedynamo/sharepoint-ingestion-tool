#!/usr/bin/env bash
# ===================================================================
# retrigger-all-documents.sh — Re-trigger textract pipeline for all
#   documents in S3 that haven't been processed yet (status=pending).
#
# This is needed after the initial bulk upload, because the S3 events
# were lost while the Lambda had import errors.
#
# Usage:
#   ./scripts/retrigger-all-documents.sh                 Process all pending
#   ./scripts/retrigger-all-documents.sh --dry-run       Show what would be processed
#   ./scripts/retrigger-all-documents.sh --limit 100     Process up to 100
#   ./scripts/retrigger-all-documents.sh --batch-size 5  Send 5 records per Lambda invoke
#   ./scripts/retrigger-all-documents.sh --delay 2       Wait 2s between batches
# ===================================================================
set -euo pipefail

# Constants
REGISTRY_TABLE="sp-ingest-document-registry"
S3_BUCKET="dynamo-ai-documents"
LAMBDA_FUNCTION="sp-ingest-textract-trigger"
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
LIMIT=0             # 0 = no limit
BATCH_SIZE=1        # records per Lambda invocation (1 for Textract to avoid throttle)
DELAY=1             # seconds between batches

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)       DRY_RUN=true; shift ;;
        --limit)         LIMIT="$2"; shift 2 ;;
        --batch-size)    BATCH_SIZE="$2"; shift 2 ;;
        --delay)         DELAY="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--dry-run] [--limit N] [--batch-size N] [--delay SECS]"
            echo ""
            echo "  --dry-run        Show pending documents without processing"
            echo "  --limit N        Process at most N documents (default: all)"
            echo "  --batch-size N   Records per Lambda invocation (default: 1)"
            echo "  --delay SECS     Seconds between batches (default: 1)"
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
echo -e "${CYAN}${BOLD}  Re-trigger Textract Pipeline (All Pending)${NC}"
echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if $DRY_RUN; then
    echo -e "${YELLOW}DRY RUN — no Lambda invocations will be made${NC}"
    echo ""
fi

export DRY_RUN LIMIT BATCH_SIZE DELAY REGION
export REGISTRY_TABLE S3_BUCKET LAMBDA_FUNCTION

python3 << 'PYEOF'
import boto3
import json
import os
import sys
import time
from datetime import datetime, timezone
from collections import Counter

DRY_RUN = os.environ.get("DRY_RUN", "false") == "true"
LIMIT = int(os.environ.get("LIMIT", "0"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1"))
DELAY = float(os.environ.get("DELAY", "1"))
REGION = os.environ.get("REGION", "us-east-1")
TABLE = os.environ.get("REGISTRY_TABLE", "sp-ingest-document-registry")
BUCKET = os.environ.get("S3_BUCKET", "dynamo-ai-documents")
LAMBDA_FN = os.environ.get("LAMBDA_FUNCTION", "sp-ingest-textract-trigger")

RED = "\033[0;31m"
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
CYAN = "\033[0;36m"
BOLD = "\033[1m"
DIM = "\033[2m"
NC = "\033[0m"

ddb = boto3.resource("dynamodb", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
table = ddb.Table(TABLE)

# ─── Step 1: Query all pending documents from the GSI ───
print(f"Querying registry for pending documents...")
pending_docs = []
kwargs = {
    "IndexName": "textract_status-index",
    "KeyConditionExpression": boto3.dynamodb.conditions.Key("textract_status").eq("pending"),
    "ProjectionExpression": "s3_source_key, file_type, sp_library",
}

while True:
    resp = table.query(**kwargs)
    pending_docs.extend(resp.get("Items", []))
    if "LastEvaluatedKey" not in resp:
        break
    kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

if not pending_docs:
    print(f"{GREEN}No pending documents found. All documents have been processed.{NC}")
    sys.exit(0)

# Count by file type
type_counts = Counter(d.get("file_type", "unknown") for d in pending_docs)
lib_counts = Counter(d.get("sp_library", "unknown") for d in pending_docs)

print(f"Found {YELLOW}{len(pending_docs)}{NC} pending document(s)")
print()
print(f"  By type:")
for ft, count in sorted(type_counts.items(), key=lambda x: -x[1]):
    print(f"    {ft:10s} {count}")
print()
print(f"  By library (top 10):")
for lib, count in sorted(lib_counts.items(), key=lambda x: -x[1])[:10]:
    print(f"    {lib:30s} {count}")
if len(lib_counts) > 10:
    print(f"    ... and {len(lib_counts) - 10} more libraries")
print()

if LIMIT > 0:
    pending_docs = pending_docs[:LIMIT]
    print(f"{DIM}Processing up to {LIMIT} document(s){NC}")
    print()

# ─── Step 2: Build synthetic S3 events and invoke Lambda ───
def build_s3_event(keys):
    """Build a synthetic S3 PutObject event for the textract_trigger Lambda."""
    records = []
    for key in keys:
        records.append({
            "eventSource": "aws:s3",
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {"name": BUCKET},
                "object": {"key": key},
            }
        })
    return {"Records": records}


def invoke_lambda(event):
    """Invoke the textract trigger Lambda and return the response."""
    resp = lam.invoke(
        FunctionName=LAMBDA_FN,
        InvocationType="RequestResponse",
        Payload=json.dumps(event),
    )
    payload = json.loads(resp["Payload"].read())
    return payload


# Process in batches
total = len(pending_docs)
processed = 0
textract_jobs = 0
direct_extracts = 0
skipped = 0
errors = 0
invoke_errors = 0

batch = []
start_time = time.time()

for i, doc in enumerate(pending_docs):
    s3_key = doc["s3_source_key"]
    batch.append(s3_key)

    if len(batch) < BATCH_SIZE and i < total - 1:
        continue

    # Process this batch
    processed += len(batch)
    pct = processed / total * 100
    elapsed = time.time() - start_time
    rate = processed / elapsed if elapsed > 0 else 0

    key_display = batch[0] if len(batch) == 1 else f"{batch[0]} (+{len(batch)-1} more)"
    sys.stdout.write(
        f"\r  [{processed:4d}/{total}] ({pct:5.1f}%) {rate:.1f}/s  {key_display[:70]:<70s}"
    )
    sys.stdout.flush()

    if not DRY_RUN:
        try:
            event = build_s3_event(batch)
            result = invoke_lambda(event)

            if "statusCode" in result and result["statusCode"] == 200:
                body = json.loads(result.get("body", "{}"))
                textract_jobs += body.get("textract_jobs", 0)
                direct_extracts += body.get("direct_extracts", 0)
                skipped += body.get("skipped", 0)
                errors += body.get("errors", 0)
            elif "errorMessage" in result:
                # Lambda function error
                print(f"\n    {RED}Lambda error: {result['errorMessage'][:100]}{NC}")
                invoke_errors += len(batch)
            else:
                errors += len(batch)

        except Exception as e:
            print(f"\n    {RED}Invoke error: {e}{NC}")
            invoke_errors += len(batch)

        # Throttle between batches
        if DELAY > 0 and i < total - 1:
            time.sleep(DELAY)

    batch = []

elapsed = time.time() - start_time
print()
print()

# ─── Step 3: Summary ───
print(f"{CYAN}{'━' * 50}{NC}")
print(f"  {BOLD}Results{NC}")
print(f"{CYAN}{'━' * 50}{NC}")
print(f"  Total pending:     {total}")
print(f"  Processed:         {processed}")
if DRY_RUN:
    print(f"  {DIM}(dry run — no Lambda invocations made){NC}")
else:
    print(f"  Textract jobs:     {GREEN}{textract_jobs}{NC}")
    print(f"  Direct extracts:   {GREEN}{direct_extracts}{NC}")
    print(f"  Skipped:           {YELLOW}{skipped}{NC}")
    print(f"  Extract errors:    {RED if errors else GREEN}{errors}{NC}")
    print(f"  Invoke errors:     {RED if invoke_errors else GREEN}{invoke_errors}{NC}")
    print(f"  Elapsed:           {elapsed:.1f}s")
    print()
    if textract_jobs > 0:
        print(f"  {DIM}Textract jobs are async. Monitor completion with:{NC}")
        print(f"  {DIM}  aws dynamodb query --table-name {TABLE} \\{NC}")
        print(f"  {DIM}    --index-name textract_status-index \\{NC}")
        print(f"  {DIM}    --key-condition-expression 'textract_status = :s' \\{NC}")
        print(f"  {DIM}    --expression-attribute-values '{{\":s\":{{\"S\":\"completed\"}}}}' \\{NC}")
        print(f"  {DIM}    --select COUNT{NC}")
print()
PYEOF
