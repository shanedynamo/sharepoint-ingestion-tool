#!/usr/bin/env bash
# ===================================================================
# reconcile.sh — Reconcile S3 source documents with extracted twins
#
# Usage:
#   ./scripts/reconcile.sh                      Report only (read-only)
#   ./scripts/reconcile.sh --fix                Re-trigger missing twins
#   ./scripts/reconcile.sh --fix --delete-orphans  Also delete orphaned twins
# ===================================================================
set -euo pipefail

# Constants
REGISTRY_TABLE="sp-ingest-document-registry"
S3_BUCKET="dynamo-ai-documents"
SOURCE_PREFIX="source/"
EXTRACTED_PREFIX="extracted/"
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
FIX=false
DELETE_ORPHANS=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fix)             FIX=true; shift ;;
        --delete-orphans)  DELETE_ORPHANS=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--fix] [--delete-orphans]"
            echo ""
            echo "  --fix              Re-trigger Textract for source docs missing twins"
            echo "  --delete-orphans   Delete extracted twins with no matching source"
            echo ""
            echo "Without flags, prints a read-only reconciliation report."
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
echo -e "${CYAN}${BOLD}  S3 Reconciliation Report${NC}"
echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

export FIX DELETE_ORPHANS REGION S3_BUCKET SOURCE_PREFIX EXTRACTED_PREFIX REGISTRY_TABLE

python3 << 'PYEOF'
import boto3
import os
import sys
from datetime import datetime, timezone
from pathlib import PurePosixPath

FIX = os.environ.get("FIX", "false") == "true"
DELETE_ORPHANS = os.environ.get("DELETE_ORPHANS", "false") == "true"
REGION = os.environ.get("REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "dynamo-ai-documents")
SOURCE_PREFIX = os.environ.get("SOURCE_PREFIX", "source/")
EXTRACTED_PREFIX = os.environ.get("EXTRACTED_PREFIX", "extracted/")
TABLE = os.environ.get("REGISTRY_TABLE", "sp-ingest-document-registry")

RED = "\033[0;31m"
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
CYAN = "\033[0;36m"
BOLD = "\033[1m"
DIM = "\033[2m"
NC = "\033[0m"

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
table = ddb.Table(TABLE)


def list_s3_keys(prefix):
    """List all object keys under a prefix."""
    keys = []
    kwargs = {"Bucket": BUCKET, "Prefix": prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            # Skip "directory" markers
            if not key.endswith("/"):
                keys.append(key)
        if not resp.get("IsTruncated"):
            break
        kwargs["ContinuationToken"] = resp["NextContinuationToken"]
    return keys


def source_to_extracted(source_key):
    """Convert a source key to its expected extracted twin key.

    source/Dynamo/HR/doc.pdf -> extracted/Dynamo/HR/doc.json
    """
    if source_key.startswith(SOURCE_PREFIX):
        relative = source_key[len(SOURCE_PREFIX):]
    else:
        relative = source_key

    p = PurePosixPath(relative)
    twin_relative = str(p.with_suffix(".json"))
    return EXTRACTED_PREFIX + twin_relative


def extracted_to_source_stem(extracted_key):
    """Convert an extracted key to its source stem (without extension).

    extracted/Dynamo/HR/doc.json -> source/Dynamo/HR/doc
    """
    if extracted_key.startswith(EXTRACTED_PREFIX):
        relative = extracted_key[len(EXTRACTED_PREFIX):]
    else:
        relative = extracted_key

    p = PurePosixPath(relative)
    stem_relative = str(p.with_suffix(""))
    return SOURCE_PREFIX + stem_relative


# ---------------------------------------------------------------
# Step 1: List all source and extracted objects
# ---------------------------------------------------------------
print(f"  {DIM}Listing S3 source/ objects...{NC}", flush=True)
source_keys = list_s3_keys(SOURCE_PREFIX)
print(f"  {DIM}Listing S3 extracted/ objects...{NC}", flush=True)
extracted_keys = list_s3_keys(EXTRACTED_PREFIX)

print(f"\n  Source documents:   {BOLD}{len(source_keys)}{NC}")
print(f"  Extracted twins:   {BOLD}{len(extracted_keys)}{NC}")
print()

# ---------------------------------------------------------------
# Step 2: Build lookup sets
# ---------------------------------------------------------------

# Map each source key to its expected extracted key
source_to_twin = {k: source_to_extracted(k) for k in source_keys}

# Set of expected twin keys
expected_twins = set(source_to_twin.values())

# Set of actual extracted keys
actual_twins = set(extracted_keys)

# Build reverse lookup: for each extracted key, find source stem
# (we need this because source could be .pdf, .docx, .pptx, etc.)
extracted_stems = {}
for ek in extracted_keys:
    stem = extracted_to_source_stem(ek)
    extracted_stems[ek] = stem

# Build source stems set for reverse lookup
source_stems = {}
for sk in source_keys:
    p = PurePosixPath(sk)
    stem = str(p.with_suffix(""))
    source_stems[stem] = sk

# ---------------------------------------------------------------
# Step 3: Find missing twins (source exists, twin does not)
# ---------------------------------------------------------------
missing_twins = []
for source_key, expected_twin in source_to_twin.items():
    if expected_twin not in actual_twins:
        missing_twins.append(source_key)

# ---------------------------------------------------------------
# Step 4: Find orphaned twins (twin exists, source does not)
# ---------------------------------------------------------------
orphaned_twins = []
for ek in extracted_keys:
    stem = extracted_to_source_stem(ek)
    # Remove the prefix for stem lookup
    stem_no_prefix = stem[len(SOURCE_PREFIX):] if stem.startswith(SOURCE_PREFIX) else stem
    # Check if any source key matches this stem
    found = False
    for sk in source_keys:
        sk_no_prefix = sk[len(SOURCE_PREFIX):] if sk.startswith(SOURCE_PREFIX) else sk
        sk_stem = str(PurePosixPath(sk_no_prefix).with_suffix(""))
        if sk_stem == stem_no_prefix:
            found = True
            break
    if not found:
        orphaned_twins.append(ek)

# ---------------------------------------------------------------
# Step 5: Report
# ---------------------------------------------------------------
print(f"{CYAN}{'━' * 44}{NC}")
print(f"{CYAN}{BOLD}  Reconciliation Results{NC}")
print(f"{CYAN}{'━' * 44}{NC}")
print()

if missing_twins:
    print(f"  {YELLOW}Missing twins ({len(missing_twins)} source docs without extracted JSON):{NC}")
    for sk in missing_twins[:20]:
        print(f"    {sk}")
    if len(missing_twins) > 20:
        print(f"    {DIM}... and {len(missing_twins) - 20} more{NC}")
    print()
else:
    print(f"  {GREEN}No missing twins — all source documents have extracted JSON.{NC}")
    print()

if orphaned_twins:
    print(f"  {YELLOW}Orphaned twins ({len(orphaned_twins)} extracted JSON without source doc):{NC}")
    for ek in orphaned_twins[:20]:
        print(f"    {ek}")
    if len(orphaned_twins) > 20:
        print(f"    {DIM}... and {len(orphaned_twins) - 20} more{NC}")
    print()
else:
    print(f"  {GREEN}No orphaned twins — all extracted JSON has a matching source.{NC}")
    print()

# ---------------------------------------------------------------
# Step 6: Fix missing twins (re-trigger Textract)
# ---------------------------------------------------------------
if missing_twins and FIX:
    print(f"{CYAN}{'━' * 44}{NC}")
    print(f"  {BOLD}Re-triggering Textract for {len(missing_twins)} missing twin(s)...{NC}")
    print()

    retried = 0
    errors = 0
    now = datetime.now(timezone.utc).isoformat()

    for sk in missing_twins:
        try:
            # Copy S3 object to itself to generate a new PutObject event
            s3.copy_object(
                Bucket=BUCKET,
                Key=sk,
                CopySource={"Bucket": BUCKET, "Key": sk},
                MetadataDirective="COPY",
                TaggingDirective="COPY",
            )

            # Update registry status to pending
            table.update_item(
                Key={"s3_source_key": sk},
                UpdateExpression="SET textract_status = :s, updated_at = :t",
                ExpressionAttributeValues={
                    ":s": "pending",
                    ":t": now,
                },
            )

            retried += 1
        except Exception as e:
            print(f"    {RED}ERROR retrying {sk}: {e}{NC}")
            errors += 1

    print(f"  Re-triggered: {GREEN}{retried}{NC}")
    if errors:
        print(f"  Errors:       {RED}{errors}{NC}")
    print()

elif missing_twins and not FIX:
    print(f"  {DIM}To re-trigger Textract for missing twins: {BOLD}./scripts/reconcile.sh --fix{NC}")
    print()

# ---------------------------------------------------------------
# Step 7: Delete orphaned twins
# ---------------------------------------------------------------
if orphaned_twins and DELETE_ORPHANS:
    print(f"{CYAN}{'━' * 44}{NC}")
    print(f"  {BOLD}Deleting {len(orphaned_twins)} orphaned twin(s)...{NC}")
    print()

    deleted = 0
    errors = 0

    for ek in orphaned_twins:
        try:
            s3.delete_object(Bucket=BUCKET, Key=ek)

            # Also clean up registry if it references this twin
            # Find the source key that would have generated this twin
            stem = extracted_to_source_stem(ek)
            # We don't know the original extension, so just log it
            print(f"    {DIM}Deleted: {ek}{NC}")
            deleted += 1
        except Exception as e:
            print(f"    {RED}ERROR deleting {ek}: {e}{NC}")
            errors += 1

    print()
    print(f"  Deleted: {GREEN}{deleted}{NC}")
    if errors:
        print(f"  Errors:  {RED}{errors}{NC}")
    print()

elif orphaned_twins and not DELETE_ORPHANS:
    print(f"  {DIM}To delete orphaned twins: {BOLD}./scripts/reconcile.sh --fix --delete-orphans{NC}")
    print()

# ---------------------------------------------------------------
# Summary
# ---------------------------------------------------------------
print(f"{CYAN}{'━' * 44}{NC}")
print(f"  {BOLD}Summary{NC}")
print(f"{CYAN}{'━' * 44}{NC}")
print()
print(f"  Source documents:    {BOLD}{len(source_keys)}{NC}")
print(f"  Extracted twins:     {BOLD}{len(extracted_keys)}{NC}")
print(f"  Missing twins:       {YELLOW if missing_twins else GREEN}{len(missing_twins)}{NC}")
print(f"  Orphaned twins:      {YELLOW if orphaned_twins else GREEN}{len(orphaned_twins)}{NC}")

if not missing_twins and not orphaned_twins:
    print()
    print(f"  {GREEN}{BOLD}All documents are in sync.{NC}")

print()
PYEOF
