# SharePoint Ingestion Pipeline - Operations Runbook

## Architecture Overview

```
SharePoint (Graph API)
    |
    v
[daily_sync Lambda] --- EventBridge cron(0 7 * * ? *) --- 7 AM UTC daily
    |
    v
S3 source/{site}/{library}/{path}/{file}
    |
    v  (S3 ObjectCreated event)
[textract_trigger Lambda]
    |
    +---> PDF/DOCX ---> Textract async job
    |                        |
    |                        v  (SNS notification)
    |                   [textract_complete Lambda]
    |                        |
    +---> PPTX/XLSX -------->+---> S3 extracted/{...}.json
    +---> TXT -------------->+
```

**Key Resources:**

| Resource | Name |
|----------|------|
| S3 Bucket | `dynamo-ai-documents` |
| Registry Table | `sp-ingest-document-registry` |
| Delta Tokens Table | `sp-ingest-delta-tokens` |
| SNS (Textract) | `sp-ingest-textract-notifications` |
| SNS (Alerts) | `sp-ingest-alerts` |
| CloudWatch Dashboard | `SP-Ingest-Pipeline` |
| Region | `us-east-1` |

---

## Daily Operations

### Check if the daily sync ran successfully

**1. Check CloudWatch logs:**

```bash
# View recent daily sync logs
aws logs tail /sp-ingest/daily-sync --since 24h --format short

# Search for the completion message
aws logs filter-log-events \
  --log-group-name /sp-ingest/daily-sync \
  --start-time $(python3 -c "import time; print(int((time.time() - 86400) * 1000))") \
  --filter-pattern '"Daily sync complete"' \
  --query "events[].message" --output text
```

**2. Check delta tokens table for last sync time:**

```bash
# Scan the delta-tokens table (typically has one entry per drive)
aws dynamodb scan \
  --table-name sp-ingest-delta-tokens \
  --projection-expression "drive_id, last_sync_at, items_processed, sync_count" \
  --output table
```

If `last_sync_at` is older than 26 hours, the daily sync may have failed. Check the CloudWatch alarm `sp-ingest-daily-sync-missing`.

**3. Check Lambda errors:**

```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=sp-ingest-daily-sync \
  --start-time $(date -u -v-24H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d '24 hours ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 3600 --statistics Sum
```

**4. Check the CloudWatch dashboard:**

Open: `https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=SP-Ingest-Pipeline`

### Manually trigger a sync

```bash
# Invoke the daily sync Lambda
aws lambda invoke \
  --function-name sp-ingest-daily-sync \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  /dev/stdout 2>/dev/null | python3 -m json.tool

# Or invoke with logging output
aws lambda invoke \
  --function-name sp-ingest-daily-sync \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  --log-type Tail \
  --query 'LogResult' --output text /dev/stdout | base64 -d
```

### Check Textract pipeline health

```bash
# Query document registry for textract_status counts via the GSI
python3 -c "
import boto3
from boto3.dynamodb.conditions import Key

ddb = boto3.resource('dynamodb', region_name='us-east-1')
table = ddb.Table('sp-ingest-document-registry')

total = table.scan(Select='COUNT')['Count']
print(f'Total documents: {total}')
print()

for status in ['pending', 'processing', 'completed', 'failed', 'direct_extracted']:
    try:
        resp = table.query(
            IndexName='textract_status-index',
            Select='COUNT',
            KeyConditionExpression=Key('textract_status').eq(status),
        )
        count = resp['Count']
    except Exception:
        count = 0
    print(f'  {status:20s} {count}')
"
```

Or use the monitoring script:

```bash
./scripts/monitor-bulk-ingest.sh --stats
```

---

## Retry Failed Textract Jobs

Use the dedicated retry script:

```bash
# Dry run (shows what would be retried)
./scripts/retry-failed-textract.sh --dry-run

# Retry all failed jobs
./scripts/retry-failed-textract.sh

# Retry with a limit
./scripts/retry-failed-textract.sh --limit 50
```

**How it works:**
1. Queries DynamoDB for all documents with `textract_status = "failed"`
2. For each failed document, copies the S3 source object to itself (triggers a new `PutObject` event)
3. Updates the registry entry to `textract_status = "pending"`
4. The S3 event re-triggers the `textract_trigger` Lambda automatically

**Manual retry for a single document:**

```bash
S3_KEY="source/Dynamo/HR-Policies/2025/Employee-Handbook.pdf"

# Copy object to itself (generates new S3 event)
aws s3 cp "s3://dynamo-ai-documents/$S3_KEY" "s3://dynamo-ai-documents/$S3_KEY" \
  --metadata-directive COPY

# Update registry status
aws dynamodb update-item \
  --table-name sp-ingest-document-registry \
  --key "{\"s3_source_key\": {\"S\": \"$S3_KEY\"}}" \
  --update-expression "SET textract_status = :s, updated_at = :t" \
  --expression-attribute-values "{\":s\": {\"S\": \"pending\"}, \":t\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}"
```

---

## Reconciliation

Run the reconciliation script to find mismatches between S3 and the registry:

```bash
# Full reconciliation report (read-only)
./scripts/reconcile.sh

# Re-trigger missing twins
./scripts/reconcile.sh --fix

# Also clean up orphaned twins
./scripts/reconcile.sh --fix --delete-orphans
```

**What it checks:**
1. Lists all objects in `s3://dynamo-ai-documents/source/`
2. Lists all objects in `s3://dynamo-ai-documents/extracted/`
3. Finds source documents that don't have a corresponding twin in `extracted/`
4. Finds orphaned twins in `extracted/` that have no corresponding source
5. Optionally re-triggers Textract for missing twins
6. Optionally deletes orphaned twins

---

## Add a New Library to Ingest

**No action needed** if the new library is under the same SharePoint site configured in `SHAREPOINT_SITE_NAME`. The daily sync uses the Microsoft Graph delta API, which automatically discovers new document libraries within the site.

The next daily sync run (or manual trigger) will pick up all documents in the new library.

**To verify discovery:**
```bash
# Trigger a manual sync and check logs
aws lambda invoke --function-name sp-ingest-daily-sync --payload '{}' \
  --cli-binary-format raw-in-base64-out /dev/stdout

# Check if documents from the new library appeared
aws dynamodb query \
  --table-name sp-ingest-document-registry \
  --index-name sp_library-index \
  --key-condition-expression "sp_library = :lib" \
  --expression-attribute-values '{":lib": {"S": "New-Library-Name"}}' \
  --select COUNT
```

---

## Exclude a Folder

**1. Update the `EXCLUDED_FOLDERS` environment variable on the daily sync Lambda:**

```bash
# Get current value
aws lambda get-function-configuration \
  --function-name sp-ingest-daily-sync \
  --query "Environment.Variables.EXCLUDED_FOLDERS" --output text

# Update (comma-separated list of folder names)
aws lambda update-function-configuration \
  --function-name sp-ingest-daily-sync \
  --environment "Variables={
    $(aws lambda get-function-configuration \
      --function-name sp-ingest-daily-sync \
      --query "Environment.Variables" --output json \
    | python3 -c "
import sys, json
env = json.load(sys.stdin)
env['EXCLUDED_FOLDERS'] = 'Drafts,drafts,Archive'
print(','.join(f'{k}={v}' for k, v in env.items()))
")
  }"
```

Or update in Terraform (`terraform/variables.tf`):

```hcl
variable "excluded_folders" {
  default = "Drafts,drafts,Archive"
}
```

Then run `terraform apply`.

**2. Already-ingested documents from that folder remain in S3.** To remove them manually:

```bash
# Find documents from the excluded folder
aws dynamodb scan \
  --table-name sp-ingest-document-registry \
  --filter-expression "contains(sp_path, :folder)" \
  --expression-attribute-values '{":folder": {"S": "/Archive/"}}' \
  --projection-expression "s3_source_key, s3_twin_key" \
  --output json
```

Then delete the S3 objects and registry entries as needed.

---

## Force Full Re-Ingestion

Delete the delta token for the relevant drive to force the next sync to perform a full crawl instead of an incremental delta.

**1. Find the drive_id:**

```bash
aws dynamodb scan \
  --table-name sp-ingest-delta-tokens \
  --output table
```

**2. Delete the delta token:**

```bash
aws dynamodb delete-item \
  --table-name sp-ingest-delta-tokens \
  --key '{"drive_id": {"S": "DRIVE_ID_HERE"}}'
```

**3. Trigger the sync:**

```bash
aws lambda invoke --function-name sp-ingest-daily-sync --payload '{}' \
  --cli-binary-format raw-in-base64-out /dev/stdout
```

The Lambda will detect no stored delta token and perform a full crawl of the drive, uploading all documents.

> **Warning:** A full re-crawl will re-upload all documents, generating new S3 events and re-triggering Textract for all files. This will incur Textract costs. For a site with thousands of documents, expect $50-150 in Textract charges.

---

## Troubleshooting

### Lambda Timeout

**Symptoms:** CloudWatch logs show `Task timed out after X seconds`

**Checks:**
```bash
# Check recent timeout errors
aws logs filter-log-events \
  --log-group-name /sp-ingest/daily-sync \
  --filter-pattern "Task timed out" \
  --start-time $(python3 -c "import time; print(int((time.time() - 86400) * 1000))") \
  --query "events[].message" --output text
```

**Resolution:**
- Daily sync (900s): If timing out, the site may have too many changes. Consider:
  - Running manual syncs more frequently to reduce delta size
  - Increasing memory (higher memory = more CPU) in Terraform
- Textract trigger (300s): Large files may take long to download/convert. Increase memory to 2048MB.
- Textract complete (300s): Large Textract responses with many pages. Increase timeout.

Update in `terraform/lambda.tf` and run `terraform apply`.

### Graph API 429 (Throttled)

**Symptoms:** CloudWatch logs show `429 Too Many Requests` or `throttled`

**Resolution:**
- The daily sync handler includes automatic retry with exponential backoff for 429 responses
- If persistent, the SharePoint site may have rate limits. Reduce the number of concurrent document downloads
- Check if multiple systems are hitting the same Graph API tenant simultaneously
- Microsoft Graph throttling limits: ~10,000 requests per 10 minutes per app

### Textract Throttling

**Symptoms:** `LimitExceededException` or `ProvisionedThroughputExceededException` in `/sp-ingest/textract-trigger` logs

**Resolution:**
- AWS default limit: 25-100 concurrent async Textract jobs (varies by region/account)
- Request a service quota increase via AWS console: **Service Quotas > Amazon Textract > Concurrent asynchronous document analysis jobs**
- The S3 trigger will automatically retry on failure, but excessive throttling causes delays

### Missing Documents

**Symptoms:** A document exists in SharePoint but not in S3/registry

**Checks:**
```bash
# Search registry by SharePoint path
aws dynamodb scan \
  --table-name sp-ingest-document-registry \
  --filter-expression "contains(sp_path, :name)" \
  --expression-attribute-values '{":name": {"S": "Employee-Handbook"}}' \
  --projection-expression "s3_source_key, textract_status, sp_path" \
  --output table
```

**Resolution:**
1. Run the reconciliation script: `./scripts/reconcile.sh`
2. Check if the document is in an excluded folder
3. Check if the file type is supported (`.pdf`, `.docx`, `.pptx`, `.xlsx`, `.txt`)
4. Force a full re-ingestion (delete delta token) if the document was added before the pipeline was set up

### Wrong Metadata Tags

S3 object tags are set at upload time and are not updated automatically when SharePoint metadata changes.

**Resolution:** Re-upload the document by copying it to itself:

```bash
S3_KEY="source/Dynamo/HR-Policies/2025/Employee-Handbook.pdf"
aws s3 cp "s3://dynamo-ai-documents/$S3_KEY" "s3://dynamo-ai-documents/$S3_KEY" \
  --metadata-directive COPY
```

This triggers a new S3 event, which re-runs the Textract pipeline. To fully refresh tags, delete the document and let the next daily sync re-upload it with current metadata.

### DynamoDB Throttling

**Symptoms:** CloudWatch alarm `sp-ingest-dynamo-throttle-*` fires

**Resolution:**
- Both tables use on-demand (pay-per-request) billing, which auto-scales
- DynamoDB may briefly throttle during sudden large spikes (>2x previous peak)
- The throttling usually resolves within minutes as DynamoDB auto-scales
- If persistent, check for hot partition keys (unlikely with `s3_source_key` as PK)

---

## Cost Monitoring

### Estimated Costs

| Service | Usage | Estimated Cost |
|---------|-------|---------------|
| **Textract** | Document analysis | ~$1.50 per 1,000 pages |
| **S3** | Document storage | Minimal (<$1/month for thousands of docs) |
| **Lambda** | Daily sync + triggers | Minimal (<$1/month) |
| **DynamoDB** | On-demand reads/writes | Minimal (<$1/month) |
| **CloudWatch** | Logs + metrics | ~$1-3/month |
| **Secrets Manager** | 3 secrets | ~$1.20/month |

### One-Time Bulk Load

For an initial bulk ingestion of several thousand documents:
- **Textract:** $50-150 (depends on total page count)
- **EC2 (t3.xlarge):** ~$0.17/hour (typically runs 2-6 hours)
- **S3 transfer:** Minimal

### Daily Sync Steady State

Expected daily cost: **< $1/day**
- Lambda invocations: 3 functions, a few invocations per day
- Textract: Only for new/modified documents (typically 0-10 per day)
- DynamoDB: A few hundred read/write operations per sync

### Monitoring Costs

```bash
# Check current month's Textract spend
aws ce get-cost-and-usage \
  --time-period Start=$(date -u +%Y-%m-01),End=$(date -u +%Y-%m-%d) \
  --granularity MONTHLY \
  --filter '{"Dimensions": {"Key": "SERVICE", "Values": ["Amazon Textract"]}}' \
  --metrics BlendedCost \
  --query "ResultsByTime[0].Total.BlendedCost" --output text
```

---

## Useful Scripts Reference

| Script | Purpose |
|--------|---------|
| `scripts/deploy.sh` | Full deployment orchestration |
| `scripts/build-lambda.sh` | Build Lambda layer + code zip |
| `scripts/validate-deployment.sh` | Post-deploy infrastructure validation |
| `scripts/run-bulk-ingest.sh` | Launch/monitor/teardown bulk EC2 |
| `scripts/monitor-bulk-ingest.sh` | Real-time bulk ingestion monitoring |
| `scripts/retry-failed-textract.sh` | Retry failed Textract jobs |
| `scripts/reconcile.sh` | S3 vs registry reconciliation |

---

## CloudWatch Log Groups

| Log Group | Source |
|-----------|--------|
| `/sp-ingest/daily-sync` | Daily sync Lambda |
| `/sp-ingest/textract-trigger` | Textract trigger Lambda |
| `/sp-ingest/textract-complete` | Textract complete Lambda |
| `/sp-ingest/bulk-ingest` | Bulk ingestion EC2 |

All log groups have 30-day retention.

---

## CloudWatch Alarms

| Alarm | Condition | Action |
|-------|-----------|--------|
| `sp-ingest-daily-sync-errors` | Any Lambda error | SNS alert |
| `sp-ingest-textract-complete-errors` | >3 errors in 1 hour | SNS alert |
| `sp-ingest-daily-sync-missing` | No invocation in 26 hours | SNS alert |
| `sp-ingest-dynamo-throttle-delta-tokens` | Any throttled request | SNS alert |
| `sp-ingest-dynamo-throttle-registry` | Any throttled request | SNS alert |

Alerts are sent to the `sp-ingest-alerts` SNS topic. Subscribe via:

```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:sp-ingest-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com
```
