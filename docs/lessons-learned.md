# Lessons Learned: SharePoint Ingestion Pipeline

Comprehensive notes from the initial deployment and operation of the SharePoint document ingestion pipeline. This document covers configuration pitfalls, error resolutions, and operational knowledge for future client deployments.

---

## Table of Contents

1. [Lambda Layer and PYTHONPATH](#1-lambda-layer-and-pythonpath)
2. [Lambda Layer Build Process](#2-lambda-layer-build-process)
3. [Textract Limitations](#3-textract-limitations)
4. [S3 Tag Sanitization](#4-s3-tag-sanitization)
5. [Document Extraction by File Type](#5-document-extraction-by-file-type)
6. [Legacy File Formats](#6-legacy-file-formats)
7. [DynamoDB Status Tracking](#7-dynamodb-status-tracking)
8. [S3 Event Notification Reliability](#8-s3-event-notification-reliability)
9. [Lambda Timeout and Large Files](#9-lambda-timeout-and-large-files)
10. [Retrigger Strategy](#10-retrigger-strategy)
11. [SharePoint Graph API Configuration](#11-sharepoint-graph-api-configuration)
12. [Terraform and Live Updates](#12-terraform-and-live-updates)
13. [Deployment Checklist](#13-deployment-checklist)

---

## 1. Lambda Layer and PYTHONPATH

### The Problem

All three Lambda functions failed with `ModuleNotFoundError` for every package installed in the Lambda layer (`docx`, `pptx`, `openpyxl`, `msal`, etc.), even though the layer was correctly built and attached.

### Root Cause

Setting the `PYTHONPATH` environment variable on a Lambda function **overrides** the Lambda runtime's default `sys.path`, which normally includes `/opt/python` (where layer packages are extracted). Our Terraform config set:

```hcl
PYTHONPATH = "/var/task/src"
```

This meant `sys.path` became `['/var/task', '/var/runtime', '/var/task/src']` — with `/opt/python` completely absent. Every layer package was invisible to the Python interpreter.

### The Fix

Append `/opt/python` to the `PYTHONPATH`:

```hcl
PYTHONPATH = "/var/task/src:/opt/python"
```

This must be set on **all Lambda functions** that use a layer.

### How to Diagnose

If layer imports fail, add temporary diagnostic logging to the handler:

```python
import sys, os
print("sys.path:", sys.path)
print("/opt/python contents:", os.listdir("/opt/python") if os.path.exists("/opt/python") else "NOT FOUND")
```

If `/opt/python` is missing from `sys.path` but exists on disk, `PYTHONPATH` is the culprit.

### Key Takeaway

> **Never set `PYTHONPATH` without including `/opt/python`** when using Lambda layers. This is one of the most common and hardest-to-diagnose Lambda layer issues because the layer is correctly built and attached — the packages are on disk, just not on the Python path.

---

## 2. Lambda Layer Build Process

### The Problem

Installing Python packages with a single `pip install --target` command can fail on Lambda because:

1. Some packages need Linux x86_64 binary wheels (Lambda runs Amazon Linux), but `pip` on macOS downloads macOS wheels by default.
2. Some packages are pure Python (e.g., `python-docx`, `python-dotenv`) and don't publish platform-specific wheels, so `--only-binary=:all:` rejects them.

### The Fix: Two-Stage Build

The build script (`scripts/build-lambda.sh`) uses two separate `pip install` commands:

**Stage 1 — Binary packages** (with platform constraints):

```bash
pip install \
    --target "$LAYER_PKG_DIR" \
    --python-version 3.11 \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --only-binary=:all: \
    --upgrade \
    msal requests python-pptx openpyxl pyyaml
```

**Stage 2 — Pure Python packages** (no platform constraints, no transitive deps):

```bash
pip install \
    --target "$LAYER_PKG_DIR" \
    --upgrade \
    --no-deps \
    python-docx python-dotenv
```

The `--no-deps` flag in stage 2 prevents overwriting binary dependencies (like `lxml`) that were correctly installed in stage 1.

### Layer Size Limits

- Lambda layers have a **250 MB uncompressed** limit.
- Our layer is approximately 75 MB uncompressed — well within limits.
- Strip `__pycache__`, `*.dist-info`, and `tests/` directories to reduce zip size.

### Verification

After building, verify the layer contains expected packages:

```bash
unzip -l dist/lambda-layer.zip | grep -E "(docx|pptx|openpyxl|msal)/" | head -10
```

---

## 3. Textract Limitations

### The Problem

AWS Textract was initially configured to process DOCX files. It failed because **Textract only supports PDF and image files** (JPEG, PNG, TIFF). It does NOT support:

- DOCX / DOC (Word)
- PPTX / PPT (PowerPoint)
- XLSX / XLS (Excel)
- Any other Office format

### The Fix

Route documents by file type:

| File Type | Extraction Method | Where It Runs |
|-----------|------------------|---------------|
| `.pdf` | AWS Textract (async `StartDocumentAnalysis`) | Textract service |
| `.docx` | `python-docx` (direct text extraction) | Lambda in-process |
| `.pptx` | `python-pptx` (direct text extraction) | Lambda in-process |
| `.xlsx` | `openpyxl` (direct text extraction) | Lambda in-process |
| `.txt` | UTF-8 read | Lambda in-process |
| `.doc`, `.ppt`, `.xls` | LibreOffice headless conversion | EC2 only |

### Key Takeaway

> **Only PDFs go to Textract.** All Office formats must be extracted using Python libraries in Lambda or converted to PDF via LibreOffice on EC2. The extraction routing logic lives in `src/utils/file_converter.py`.

---

## 4. S3 Tag Sanitization

### The Problem

SharePoint file paths and metadata often contain special characters (parentheses, brackets, commas, Unicode, etc.). When these values were used as S3 object tags, the `PutObject` call failed with:

```
InvalidTag: The TagValue you have provided is invalid
```

### Root Cause

S3 tag values only allow: **letters, digits, spaces, and `+ - = . _ : / @`**. Characters like `(`, `)`, `#`, `&`, etc. that commonly appear in SharePoint paths are rejected.

### The Fix

All tag values pass through `PathMapper._sanitize_tag_value()` before being applied:

```python
import re

def _sanitize_tag_value(value: str) -> str:
    return re.sub(r"[^\w\s+\-=.:/@]", "_", value)
```

This replaces any disallowed character with an underscore. Tag values are also truncated to 256 characters (S3 limit).

### Where It Applies

- `textract_complete.py` — tags copied from source document to twin
- `utils/path_mapper.py` — tags applied during initial ingestion

### Key Takeaway

> **Always sanitize S3 tag values** when the source data comes from SharePoint or any external system. Assume paths will contain parentheses, special characters, and Unicode.

---

## 5. Document Extraction by File Type

### Architecture

The `textract_trigger` Lambda routes each document to the correct extraction strategy based on file extension:

```
.pdf  → Textract async (StartDocumentAnalysis)
        → SNS notification when complete
        → textract_complete Lambda builds JSON twin

.docx → python-docx extracts text in-process
.pptx → python-pptx extracts text in-process
.xlsx → openpyxl extracts text in-process
.txt  → UTF-8 read

All non-PDF types → JSON twin built immediately in textract_trigger Lambda
```

### Text Extraction Quality Notes

- **DOCX**: Extracts paragraphs and table cell text. Does not extract images, headers/footers, or footnotes.
- **PPTX**: Extracts text frames and table cells, organized by slide number. Does not extract speaker notes or embedded objects.
- **XLSX**: Extracts cell values (using `data_only=True` so formulas return their computed value, not the formula string). Organized by sheet name.
- **PDF via Textract**: Full OCR + layout analysis. Extracts text, tables, and forms. Highest fidelity extraction.

### Key Takeaway

> PDF extraction via Textract produces the richest output (OCR, tables, forms). Office format extraction is text-only. If a client needs high-fidelity extraction from Office files, convert to PDF first using LibreOffice on EC2.

---

## 6. Legacy File Formats

### The Problem

Legacy Office formats (`.doc`, `.ppt`, `.xls`) use binary formats that cannot be read by the pure-Python libraries available in Lambda (`python-docx`, `python-pptx`, `openpyxl` only support the modern XML-based formats).

### Expected Failures

Documents with these extensions will fail with:

```
Legacy format '.doc' is not supported in Lambda mode. Use convert_to_pdf (LibreOffice) on EC2 instead.
```

This is **expected behavior**, not a bug.

### Resolution Options

1. **EC2 bulk processing**: Run the bulk ingest with LibreOffice installed. The `convert_to_pdf()` method handles legacy formats via `libreoffice --headless --convert-to pdf`.
2. **Docker**: Use the `docker/Dockerfile.bulk` image which includes LibreOffice.
3. **Accept the gap**: In our initial deployment, only 6 out of 2,702 documents (0.2%) were legacy `.doc` files. Depending on the client's document mix, this may be acceptable.

### Key Takeaway

> Legacy Office formats require LibreOffice, which is not available in Lambda. Plan for EC2-based processing or accept that these files will be skipped. Quantify the legacy format count during pre-deployment assessment.

---

## 7. DynamoDB Status Tracking

### Status Values

The `textract_status` field in the document registry uses **lowercase** values:

| Status | Meaning |
|--------|---------|
| `pending` | Document ingested into S3, not yet processed |
| `processing` | Textract job submitted, awaiting completion |
| `completed` | JSON twin successfully created in S3 |
| `failed` | Extraction failed (see `error_message` field) |

### Gotcha: Case Sensitivity

DynamoDB queries are **case-sensitive**. Querying for `"FAILED"` when the actual value is `"failed"` returns zero results. Always use lowercase when querying status values.

### GSI Queries

The registry table has two GSIs for efficient queries:

- `textract_status-index` — partition key: `textract_status`, sort key: `ingested_at`
- `sp_library-index` — partition key: `sp_library`, sort key: `sp_last_modified`

To count documents by status:

```python
table.query(
    IndexName="textract_status-index",
    KeyConditionExpression="textract_status = :s",
    ExpressionAttributeValues={":s": "pending"},
    Select="COUNT"
)
```

### Key Takeaway

> Always use lowercase status values. When debugging, scan for actual distinct values rather than assuming the expected case.

---

## 8. S3 Event Notification Reliability

### The Problem

During bulk retriggering of 2,082 documents via S3 copy-in-place (to fire `ObjectCreated` events), only ~878 documents were actually processed by the Lambda. The remaining documents stayed in `pending` status.

### Root Cause

S3 event notifications are **best-effort delivery**, not guaranteed. Under high throughput (copying thousands of objects rapidly), events can be dropped. There is no built-in retry mechanism for dropped S3 events.

### The Fix: Direct Lambda Invocation

Instead of relying on S3 events for retriggering, invoke the Lambda directly with a synthetic S3 event payload:

```python
import boto3, json

lambda_client = boto3.client("lambda")

event = {
    "Records": [{
        "s3": {
            "bucket": {"name": "dynamo-ai-documents"},
            "object": {"key": "source/path/to/document.pdf"}
        }
    }]
}

# Async invocation — returns 202 immediately
lambda_client.invoke(
    FunctionName="sp-ingest-textract-trigger",
    InvocationType="Event",
    Payload=json.dumps(event)
)
```

Using `InvocationType="Event"` (async) is recommended for bulk retriggering because:
- Returns immediately (no waiting for Lambda execution)
- Lambda handles retries automatically on failure
- Can dispatch hundreds of invocations per second

### Retriggering Rate

A delay of 0.2 seconds between invocations (~5/sec) avoids Lambda throttling while completing 1,000 documents in about 3 minutes.

### Key Takeaway

> **Never rely on S3 events for bulk operations.** For initial load or retrigger scenarios, invoke Lambda directly with async invocations. Reserve S3 events for steady-state incremental processing where event volume is low.

---

## 9. Lambda Timeout and Large Files

### The Problem

Some Lambda invocations timed out at the 300-second (5-minute) limit when processing batches of large documents (especially PPTX files with many slides or XLSX files with large datasets).

### Observations

- The `textract_trigger` Lambda has a 300-second timeout and 1024 MB memory.
- Processing 5 large documents in a single invocation can exceed the timeout.
- Textract async jobs are not affected (they run server-side), but the direct extraction of large Office files happens in-process.

### Recommendations

1. **One document per invocation** when retriggering — avoids batch timeout failures.
2. **Monitor Lambda duration** via CloudWatch Metrics. If p99 duration approaches the timeout, consider:
   - Increasing the timeout (max 900 seconds for Lambda)
   - Increasing memory (which also increases CPU allocation)
   - Splitting large files into separate processing
3. **S3 event-triggered invocations** naturally process one file at a time, so this is only a concern during bulk retriggering.

### Key Takeaway

> For bulk retriggering, use one document per Lambda invocation with async invocation type. Batch processing introduces timeout risk with large files.

---

## 10. Retrigger Strategy

### When to Retrigger

- After deploying code fixes (e.g., PYTHONPATH fix, new extraction support)
- After a bulk ingestion when some documents failed
- After infrastructure changes that may have disrupted processing

### Recommended Retrigger Process

1. **Query current status** to understand the scope:

```bash
aws dynamodb scan --table-name sp-ingest-document-registry \
    --projection-expression "textract_status" \
    --output json | python3 -c "
import sys, json
data = json.load(sys.stdin)
counts = {}
for item in data['Items']:
    s = item['textract_status']['S']
    counts[s] = counts.get(s, 0) + 1
for s, c in sorted(counts.items(), key=lambda x: -x[1]):
    print(f'{s}: {c}')
"
```

2. **Reset failed documents** to pending (if appropriate):

```python
# Only reset 'failed' items that should be retried
table.update_item(
    Key={"s3_source_key": key},
    UpdateExpression="SET textract_status = :s",
    ExpressionAttributeValues={":s": "pending"}
)
```

3. **Retrigger via async Lambda invocation** (not S3 copy):

```python
for key in pending_keys:
    lambda_client.invoke(
        FunctionName="sp-ingest-textract-trigger",
        InvocationType="Event",
        Payload=json.dumps({"Records": [{"s3": {"bucket": {"name": BUCKET}, "object": {"key": key}}}]})
    )
    time.sleep(0.2)  # 5/sec to avoid throttling
```

4. **Monitor progress** by polling DynamoDB status counts every few minutes.

### Key Takeaway

> Keep retrigger scripts in the project. They will be needed for every deployment. The combination of async Lambda invocation + DynamoDB status polling is the most reliable approach.

---

## 11. SharePoint Graph API Configuration

### Azure AD App Registration

The pipeline requires an Azure AD app registration with:

- **Application permission**: `Sites.Read.All` (Microsoft Graph)
- **Admin consent** granted for the tenant
- Credentials stored in AWS Secrets Manager at `sp-ingest/azure-credentials`

### Delta Sync

The daily sync Lambda uses the Graph API delta endpoint to fetch only changed files since the last run. The delta token is stored in DynamoDB (`sp-ingest-delta-tokens` table).

### Excluded Folders

SharePoint libraries often contain system folders that should be excluded. Set via the `EXCLUDED_FOLDERS` environment variable:

```
EXCLUDED_FOLDERS=Drafts,drafts,Forms,_private
```

**Gotcha**: When setting this via AWS CLI `--environment` shorthand syntax, commas in the value conflict with the CLI's comma separator. Use JSON format instead:

```bash
aws lambda update-function-configuration \
    --function-name sp-ingest-daily-sync \
    --environment '{"Variables":{"EXCLUDED_FOLDERS":"Drafts,drafts"}}'
```

### Key Takeaway

> Always use JSON format for `--environment` when values contain commas. Verify excluded folders with the client before deployment — common exclusions include `Drafts`, `Forms`, `_private`, and `_catalogs`.

---

## 12. Terraform and Live Updates

### The Problem

During debugging, Lambda configurations were updated directly via AWS CLI (`aws lambda update-function-configuration`). This caused Terraform state drift — Terraform didn't know about the manual changes.

### Best Practice

1. **Make all changes in Terraform** (`terraform/lambda.tf`) first.
2. For urgent hotfixes, update via AWS CLI **and** update the Terraform code simultaneously.
3. Run `terraform apply` afterward to sync state.
4. Never rely solely on manual AWS CLI changes — they will be overwritten on the next `terraform apply`.

### Lambda Cold Start Forcing

After updating a Lambda's code or configuration, existing warm containers may still run the old code. To force a cold start:

```bash
# Update an environment variable to invalidate warm containers
aws lambda update-function-configuration \
    --function-name sp-ingest-textract-trigger \
    --environment '{"Variables":{"FORCE_COLD_START":"2026-02-13T12:00:00"}}'
```

Any environment variable change forces all warm containers to be recycled.

### Key Takeaway

> Terraform is the source of truth. Manual changes are acceptable for debugging but must be reflected in Terraform code immediately.

---

## 13. Deployment Checklist

Use this checklist when deploying the pipeline to a new client environment:

### Pre-Deployment

- [ ] Azure AD app registration created with `Sites.Read.All` permission
- [ ] Admin consent granted for the app registration
- [ ] SharePoint site name confirmed with client
- [ ] Excluded folders list confirmed with client
- [ ] Legacy format count estimated (`.doc`, `.ppt`, `.xls`) — plan for EC2 if significant
- [ ] AWS account provisioned with required services (S3, DynamoDB, Lambda, Textract, SNS, EventBridge, Secrets Manager)

### Infrastructure Deployment

- [ ] `terraform init && terraform apply` completes without errors
- [ ] Azure credentials stored in Secrets Manager at `sp-ingest/azure-credentials`
- [ ] Lambda layer built with two-stage pip install (`scripts/build-lambda.sh`)
- [ ] Lambda code deployed (`scripts/deploy.sh`)
- [ ] Verify `PYTHONPATH` includes `/opt/python` on all Lambda functions
- [ ] Verify Lambda layer is attached to all three functions

### Validation

- [ ] Test daily sync with a small SharePoint library
- [ ] Verify S3 objects appear under `source/` prefix
- [ ] Verify DynamoDB registry entries created
- [ ] Manually trigger textract_trigger for one PDF — verify Textract job starts
- [ ] Manually trigger textract_trigger for one DOCX — verify direct extraction completes
- [ ] Verify JSON twin appears under `extracted/` prefix
- [ ] Check CloudWatch logs for errors on all three Lambda functions

### Bulk Ingestion

- [ ] Run bulk ingestion (`scripts/run-bulk-ingest.sh` or Docker)
- [ ] Monitor progress via DynamoDB status counts
- [ ] After completion, check for `failed` and `pending` documents
- [ ] Retrigger any remaining documents using async Lambda invocation
- [ ] Verify final completion rate (target: >99%)

### Post-Deployment

- [ ] Verify EventBridge cron rule is active for daily sync
- [ ] Confirm SNS topic subscription for Textract notifications
- [ ] Set up CloudWatch alarms for Lambda errors
- [ ] Run `terraform apply` to sync any manual changes made during debugging
- [ ] Document any client-specific deviations from standard deployment

---

## Quick Reference: Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: No module named 'docx'` | `PYTHONPATH` missing `/opt/python` | Add `:/opt/python` to `PYTHONPATH` env var |
| `InvalidTag: The TagValue you have provided is invalid` | Special characters in S3 tag values | Sanitize tags with `_sanitize_tag_value()` |
| `Legacy format '.doc' is not supported in Lambda mode` | Binary Office format in Lambda | Process on EC2 with LibreOffice or skip |
| Textract fails on DOCX | Textract only supports PDF/images | Route DOCX to `python-docx` extraction |
| Documents stuck in `pending` after bulk retrigger | S3 events dropped under load | Use direct async Lambda invocation |
| Lambda timeout (300s) on large batch | Too many large files per invocation | Use 1 document per invocation |
| DynamoDB query returns 0 for known status | Case mismatch (`FAILED` vs `failed`) | Use lowercase status values |
| `EXCLUDED_FOLDERS` not working via CLI | Comma conflicts with CLI syntax | Use JSON format for `--environment` |
