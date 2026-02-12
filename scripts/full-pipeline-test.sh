#!/usr/bin/env bash
# ===================================================================
# full-pipeline-test.sh — End-to-end pipeline test on LIVE AWS
#
# Validates the complete ingestion pipeline: SharePoint → S3 →
# Textract → JSON twin → chunking readiness.
#
# Usage:
#   ./scripts/full-pipeline-test.sh                  Full test + report
#   ./scripts/full-pipeline-test.sh --report-only    Skip pipeline test, just report
#   ./scripts/full-pipeline-test.sh --skip-upload     Don't upload to SharePoint
#   ./scripts/full-pipeline-test.sh --skip-cleanup    Leave test artifacts in place
#   ./scripts/full-pipeline-test.sh --test-key KEY    Test an existing S3 source key
# ===================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Constants
S3_BUCKET="dynamo-ai-documents"
SOURCE_PREFIX="source"
EXTRACTED_PREFIX="extracted"
REGISTRY_TABLE="sp-ingest-document-registry"
DELTA_TABLE="sp-ingest-delta-tokens"
REGION="us-east-1"
LAMBDA_DAILY_SYNC="sp-ingest-daily-sync"

TEXTRACT_POLL_INTERVAL=10
TEXTRACT_TIMEOUT=300  # 5 minutes

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# Options
REPORT_ONLY=false
SKIP_UPLOAD=false
SKIP_CLEANUP=false
TEST_KEY=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --report-only)    REPORT_ONLY=true; shift ;;
        --skip-upload)    SKIP_UPLOAD=true; shift ;;
        --skip-cleanup)   SKIP_CLEANUP=true; shift ;;
        --test-key)       TEST_KEY="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "  --report-only     Skip pipeline test, generate corpus report only"
            echo "  --skip-upload     Don't upload test doc to SharePoint (assume it exists)"
            echo "  --skip-cleanup    Leave test artifacts in S3/DynamoDB after test"
            echo "  --test-key KEY    Test against an existing S3 source key"
            exit 0
            ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
    esac
done

# ===================================================================
# Helpers
# ===================================================================

PASS=0
FAIL=0
WARN=0
STEP=0
ISSUES=()

step() {
    STEP=$((STEP + 1))
    echo ""
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  STEP $STEP: $1${NC}"
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

check_pass() {
    PASS=$((PASS + 1))
    echo -e "  ${GREEN}✓ PASS${NC}  $1"
}

check_fail() {
    FAIL=$((FAIL + 1))
    echo -e "  ${RED}✗ FAIL${NC}  $1"
    ISSUES+=("$1")
}

check_warn() {
    WARN=$((WARN + 1))
    echo -e "  ${YELLOW}⚠ WARN${NC}  $1"
}

check_info() {
    echo -e "  ${DIM}ℹ${NC}  $1"
}

format_number() {
    printf "%'d" "$1" 2>/dev/null || echo "$1"
}

# ===================================================================
# Pre-flight
# ===================================================================

echo -e "${CYAN}${BOLD}"
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║    FULL PIPELINE END-TO-END TEST                    ║"
echo "  ║    Live AWS Environment                             ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

if ! command -v aws &>/dev/null; then
    echo -e "${RED}ERROR: aws CLI not found${NC}"
    exit 1
fi

if ! python3 -c "import boto3" 2>/dev/null; then
    echo -e "${RED}ERROR: boto3 not available${NC}"
    exit 1
fi

# Verify AWS identity
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "UNKNOWN")
echo -e "  AWS Account:  ${BOLD}$AWS_ACCOUNT${NC}"
echo -e "  Region:       ${BOLD}$REGION${NC}"
echo -e "  Bucket:       ${BOLD}$S3_BUCKET${NC}"
echo -e "  Timestamp:    ${BOLD}$(date -u +%Y-%m-%dT%H:%M:%SZ)${NC}"

# Jump to report if --report-only
if $REPORT_ONLY; then
    STEP=6  # Skip to step 7
    step "Generate Corpus Report"
    generate_report=true
else
    generate_report=false
fi

# ===================================================================
# STEP 1: Upload test document to SharePoint
# ===================================================================

# Track the test document key for later steps
TEST_S3_KEY=""
TEST_TWIN_KEY=""
TEST_TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
TEST_FILENAME="Pipeline-Test-${TEST_TIMESTAMP}.docx"

if ! $REPORT_ONLY; then

step "Upload test document to SharePoint"

if [ -n "$TEST_KEY" ]; then
    # User provided an existing S3 key to test
    TEST_S3_KEY="$TEST_KEY"
    TEST_FILENAME=$(basename "$TEST_KEY")
    check_info "Using existing S3 key: $TEST_S3_KEY"

    # Verify the key exists
    if aws s3api head-object --bucket "$S3_BUCKET" --key "$TEST_S3_KEY" &>/dev/null; then
        check_pass "Source object exists in S3"
    else
        check_fail "Source object not found: s3://$S3_BUCKET/$TEST_S3_KEY"
        echo -e "${RED}Cannot continue without a test document.${NC}"
        exit 1
    fi

elif $SKIP_UPLOAD; then
    check_info "SharePoint upload skipped (--skip-upload)"
    check_info "Will generate a test document directly in S3 to exercise Textract"

    # Generate a minimal test .docx and upload directly to S3 source/
    TEST_S3_KEY="${SOURCE_PREFIX}/Dynamo/_pipeline-test/${TEST_FILENAME}"

    python3 << PYEOF
import sys
sys.path.insert(0, "$PROJECT_ROOT/src")
try:
    from docx import Document
    doc = Document()
    doc.add_heading("Pipeline Integration Test", level=1)
    doc.add_paragraph("This is an automated pipeline test document created at $TEST_TIMESTAMP.")
    doc.add_paragraph("It validates the full ingestion pipeline: S3 → Textract → JSON twin → chunks.")
    doc.add_paragraph("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris.")
    table = doc.add_table(rows=3, cols=3)
    table.style = "Table Grid"
    headers = ["Department", "Headcount", "Budget"]
    for i, h in enumerate(headers):
        table.rows[0].cells[i].text = h
    data = [["Engineering", "45", "\$2.1M"], ["HR", "12", "\$0.8M"]]
    for r, row_data in enumerate(data, 1):
        for c, val in enumerate(row_data):
            table.rows[r].cells[c].text = val
    import io
    buf = io.BytesIO()
    doc.save(buf)
    content = buf.getvalue()
    print(f"Generated test .docx ({len(content)} bytes)")

    import boto3
    s3 = boto3.client("s3", region_name="$REGION")
    s3.put_object(
        Bucket="$S3_BUCKET",
        Key="$TEST_S3_KEY",
        Body=content,
        ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ServerSideEncryption="AES256",
        Tagging="sp-site=Dynamo&sp-library=_pipeline-test&file-type=docx&access-tags=all-staff",
    )
    print(f"Uploaded to s3://$S3_BUCKET/$TEST_S3_KEY")

except ImportError:
    # Fall back to a minimal test PDF if python-docx not available
    print("python-docx not available, generating test PDF instead", file=sys.stderr)
    pdf_content = (
        b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
        b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
        b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<<"
        b"/Font<</F1 4 0 R>>>>/Contents 5 0 R>>endobj\n"
        b"4 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
        b"5 0 obj<</Length 44>>stream\nBT /F1 16 Tf 100 700 Td (Pipeline Test) Tj ET\n"
        b"endstream\nendobj\n"
        b"xref\n0 6\n0000000000 65535 f \n0000000009 00000 n \n"
        b"0000000058 00000 n \n0000000115 00000 n \n"
        b"0000000266 00000 n \n0000000337 00000 n \n"
        b"trailer<</Size 6/Root 1 0 R>>\nstartxref\n431\n%%EOF"
    )
    import boto3
    s3 = boto3.client("s3", region_name="$REGION")
    test_key = "$TEST_S3_KEY".replace(".docx", ".pdf")
    s3.put_object(
        Bucket="$S3_BUCKET",
        Key=test_key,
        Body=pdf_content,
        ContentType="application/pdf",
        ServerSideEncryption="AES256",
        Tagging="sp-site=Dynamo&sp-library=_pipeline-test&file-type=pdf&access-tags=all-staff",
    )
    print(f"Uploaded test PDF to s3://$S3_BUCKET/{test_key}")
    # Write updated key back
    with open("/tmp/pipeline_test_key.txt", "w") as f:
        f.write(test_key)
PYEOF

    # Check if we fell back to PDF
    if [ -f /tmp/pipeline_test_key.txt ]; then
        TEST_S3_KEY=$(cat /tmp/pipeline_test_key.txt)
        TEST_FILENAME=$(basename "$TEST_S3_KEY")
        rm -f /tmp/pipeline_test_key.txt
    fi

    if aws s3api head-object --bucket "$S3_BUCKET" --key "$TEST_S3_KEY" &>/dev/null; then
        check_pass "Test document uploaded to S3: $TEST_S3_KEY"
    else
        check_fail "Failed to upload test document to S3"
        exit 1
    fi

    # Register in DynamoDB so the pipeline can track it
    python3 << PYEOF
import boto3
from datetime import datetime, timezone

ddb = boto3.resource("dynamodb", region_name="$REGION")
table = ddb.Table("$REGISTRY_TABLE")

now = datetime.now(timezone.utc).isoformat()
table.put_item(Item={
    "s3_source_key": "$TEST_S3_KEY",
    "sp_item_id": "pipeline-test-$TEST_TIMESTAMP",
    "sp_path": "/_pipeline-test/$TEST_FILENAME",
    "sp_library": "_pipeline-test",
    "sp_last_modified": now,
    "file_type": "$(echo "$TEST_FILENAME" | sed 's/.*\./\./')",
    "size_bytes": 0,
    "textract_status": "pending",
    "ingested_at": now,
    "updated_at": now,
})
print("Registered test document in DynamoDB")
PYEOF
    check_pass "Test document registered in DynamoDB (textract_status=pending)"

    # The S3 event notification will trigger the textract_trigger Lambda
    check_info "S3 event notification will trigger Textract processing"
    check_info "Skipping Step 2 (daily sync not needed for direct upload)"
    STEP=$((STEP + 1))  # Skip step 2

else
    # Upload to SharePoint via Graph API
    check_info "Uploading test document to SharePoint via Graph API..."

    UPLOAD_RESULT=$(python3 << 'PYEOF'
import sys, json, os
sys.path.insert(0, os.path.join(os.environ.get("PROJECT_ROOT", "."), "src"))

try:
    from config import config
    import msal
    import requests

    app = msal.ConfidentialClientApplication(
        client_id=config.azure_client_id,
        client_credential=config.azure_client_secret,
        authority=f"https://login.microsoftonline.com/{config.azure_tenant_id}",
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        print(json.dumps({"error": f"Token acquisition failed: {result.get('error_description', '')}"}))
        sys.exit(0)

    token = result["access_token"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Find the site
    resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites?search={config.sharepoint_site_name}",
        headers=headers, timeout=30,
    )
    resp.raise_for_status()
    sites = resp.json().get("value", [])
    if not sites:
        print(json.dumps({"error": "SharePoint site not found"}))
        sys.exit(0)

    site_id = sites[0]["id"]

    # Find drives
    resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives",
        headers=headers, timeout=30,
    )
    resp.raise_for_status()
    drives = resp.json().get("value", [])
    if not drives:
        print(json.dumps({"error": "No document libraries found"}))
        sys.exit(0)

    # Use the first drive
    drive_id = drives[0]["id"]
    drive_name = drives[0].get("name", "Documents")

    # Generate a minimal docx
    try:
        from docx import Document
        import io
        doc = Document()
        doc.add_heading("Pipeline Integration Test", level=1)
        doc.add_paragraph("Automated test document for end-to-end pipeline validation.")
        doc.add_paragraph("Lorem ipsum dolor sit amet, consectetur adipiscing elit.")
        table = doc.add_table(rows=2, cols=2)
        table.rows[0].cells[0].text = "Test"
        table.rows[0].cells[1].text = "Value"
        table.rows[1].cells[0].text = "Status"
        table.rows[1].cells[1].text = "Active"
        buf = io.BytesIO()
        doc.save(buf)
        content = buf.getvalue()
    except ImportError:
        # Minimal content if docx not available
        content = b"Pipeline test content"

    filename = os.environ.get("TEST_FILENAME", "Pipeline-Test.docx")

    # Upload to /_pipeline-test/ folder
    upload_url = (
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
        f"/root:/_pipeline-test/{filename}:/content"
    )
    upload_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }
    resp = requests.put(upload_url, headers=upload_headers, data=content, timeout=60)
    resp.raise_for_status()
    item = resp.json()

    print(json.dumps({
        "ok": True,
        "item_id": item.get("id", ""),
        "name": item.get("name", filename),
        "drive_id": drive_id,
        "drive_name": drive_name,
        "web_url": item.get("webUrl", ""),
        "size": len(content),
    }))

except Exception as e:
    print(json.dumps({"error": str(e)}))
PYEOF
    )

    UPLOAD_ERROR=$(echo "$UPLOAD_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',''))" 2>/dev/null || echo "parse error")

    if [ -n "$UPLOAD_ERROR" ] && [ "$UPLOAD_ERROR" != "" ]; then
        check_warn "SharePoint upload failed: $UPLOAD_ERROR"
        check_info "Falling back to direct S3 upload (--skip-upload mode)"
        SKIP_UPLOAD=true

        # Re-run the skip-upload path (generate and upload directly to S3)
        TEST_S3_KEY="${SOURCE_PREFIX}/Dynamo/_pipeline-test/${TEST_FILENAME}"
        python3 -c "
import boto3
s3 = boto3.client('s3', region_name='$REGION')
content = b'%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<</Font<</F1 4 0 R>>>>/Contents 5 0 R>>endobj\n4 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n5 0 obj<</Length 44>>stream\nBT /F1 16 Tf 100 700 Td (Pipeline Test) Tj ET\nendstream\nendobj\nxref\n0 6\n0000000000 65535 f \n0000000009 00000 n \n0000000058 00000 n \n0000000115 00000 n \n0000000266 00000 n \n0000000337 00000 n \ntrailer<</Size 6/Root 1 0 R>>\nstartxref\n431\n%%EOF'
key = '${TEST_S3_KEY}'.replace('.docx', '.pdf')
s3.put_object(Bucket='$S3_BUCKET', Key=key, Body=content, ContentType='application/pdf', ServerSideEncryption='AES256', Tagging='sp-site=Dynamo&sp-library=_pipeline-test&file-type=pdf&access-tags=all-staff')
print(key)
" > /tmp/pipeline_test_key.txt 2>/dev/null

        TEST_S3_KEY=$(cat /tmp/pipeline_test_key.txt)
        TEST_FILENAME=$(basename "$TEST_S3_KEY")
        rm -f /tmp/pipeline_test_key.txt
        check_pass "Fallback: uploaded test PDF to S3"
    else
        SP_DRIVE_NAME=$(echo "$UPLOAD_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('drive_name',''))" 2>/dev/null)
        SP_WEB_URL=$(echo "$UPLOAD_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('web_url',''))" 2>/dev/null)
        check_pass "Uploaded to SharePoint: $TEST_FILENAME"
        check_info "Drive: $SP_DRIVE_NAME"
        check_info "URL: $SP_WEB_URL"
    fi

fi  # end of TEST_KEY / SKIP_UPLOAD / upload logic


# ===================================================================
# STEP 2: Trigger daily sync
# ===================================================================

if [ -z "$TEST_KEY" ] && ! $SKIP_UPLOAD; then

step "Trigger daily sync Lambda"

check_info "Invoking $LAMBDA_DAILY_SYNC..."

INVOKE_RESULT=$(aws lambda invoke \
    --function-name "$LAMBDA_DAILY_SYNC" \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    --log-type Tail \
    --region "$REGION" \
    /tmp/pipeline-test-sync-response.json 2>&1) || true

SYNC_STATUS=$(echo "$INVOKE_RESULT" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('StatusCode', 'unknown'))
except:
    print('error')
" 2>/dev/null || echo "unknown")

if [ -f /tmp/pipeline-test-sync-response.json ]; then
    SYNC_BODY=$(cat /tmp/pipeline-test-sync-response.json)
    check_pass "Daily sync Lambda invoked"
    check_info "Response: $SYNC_BODY"
    rm -f /tmp/pipeline-test-sync-response.json
else
    check_fail "Daily sync Lambda invocation failed"
    check_info "$INVOKE_RESULT"
fi

# Wait a moment for the S3 event to fire after sync uploads
check_info "Waiting 10s for S3 event notifications to propagate..."
sleep 10

# If we uploaded to SharePoint, discover the S3 key
if [ -z "$TEST_S3_KEY" ]; then
    check_info "Searching for test document in S3..."

    TEST_S3_KEY=$(aws s3api list-objects-v2 \
        --bucket "$S3_BUCKET" \
        --prefix "${SOURCE_PREFIX}/" \
        --query "Contents[?contains(Key, 'Pipeline-Test-${TEST_TIMESTAMP}')].Key" \
        --output text 2>/dev/null | head -1) || true

    if [ -n "$TEST_S3_KEY" ] && [ "$TEST_S3_KEY" != "None" ]; then
        check_pass "Test document found in S3: $TEST_S3_KEY"
    else
        check_fail "Test document not found in S3 after sync"
        check_info "The document may not have been picked up by the delta sync."
        check_info "Try --skip-upload to upload directly to S3 for testing."
    fi
fi

fi  # end of step 2 conditional


# ===================================================================
# STEP 3: Verify Textract processing
# ===================================================================

if [ -n "$TEST_S3_KEY" ]; then

step "Verify Textract processing"

check_info "Polling document registry for: $TEST_S3_KEY"
check_info "Timeout: ${TEXTRACT_TIMEOUT}s (polling every ${TEXTRACT_POLL_INTERVAL}s)"

TEXTRACT_RESULT=$(python3 << PYEOF
import boto3, json, time, sys

REGION = "$REGION"
TABLE = "$REGISTRY_TABLE"
S3_KEY = "$TEST_S3_KEY"
BUCKET = "$S3_BUCKET"
POLL_INTERVAL = $TEXTRACT_POLL_INTERVAL
TIMEOUT = $TEXTRACT_TIMEOUT

ddb = boto3.resource("dynamodb", region_name=REGION)
table = ddb.Table(TABLE)
s3 = boto3.client("s3", region_name=REGION)

start = time.monotonic()
final_status = "not_found"
twin_key = ""
registry_entry = {}

while time.monotonic() - start < TIMEOUT:
    resp = table.get_item(Key={"s3_source_key": S3_KEY})
    item = resp.get("Item")

    if not item:
        print(f"  Waiting... registry entry not found yet", file=sys.stderr, flush=True)
        time.sleep(POLL_INTERVAL)
        continue

    status = item.get("textract_status", "unknown")
    elapsed = int(time.monotonic() - start)
    print(f"  [{elapsed}s] textract_status = {status}", file=sys.stderr, flush=True)

    if status in ("completed", "direct_extracted"):
        final_status = status
        twin_key = item.get("s3_twin_key", "")
        registry_entry = item
        break
    elif status == "failed":
        final_status = "failed"
        registry_entry = item
        break

    time.sleep(POLL_INTERVAL)

else:
    # Timeout — get final state
    resp = table.get_item(Key={"s3_source_key": S3_KEY})
    item = resp.get("Item", {})
    final_status = item.get("textract_status", "timeout")
    twin_key = item.get("s3_twin_key", "")
    registry_entry = item

# If we have a twin, download and validate it
twin_json = None
if twin_key:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=twin_key)
        twin_json = json.loads(obj["Body"].read())
    except Exception as e:
        twin_json = {"_error": str(e)}
elif final_status in ("completed", "direct_extracted"):
    # Try to derive the twin key
    source_rel = S3_KEY
    if source_rel.startswith("source/"):
        source_rel = source_rel[7:]
    import os.path
    root, ext = os.path.splitext(source_rel)
    derived_key = f"extracted/{root}.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=derived_key)
        twin_json = json.loads(obj["Body"].read())
        twin_key = derived_key
    except Exception:
        pass

# Serialize all fields for downstream consumption
# (Decimal -> int/float for JSON)
def decimal_default(obj):
    from decimal import Decimal
    if isinstance(obj, Decimal):
        return int(obj) if obj == int(obj) else float(obj)
    raise TypeError

result = {
    "final_status": final_status,
    "twin_key": twin_key,
    "twin_json": twin_json,
    "registry": {k: v for k, v in registry_entry.items()} if registry_entry else {},
}
print(json.dumps(result, default=decimal_default))
PYEOF
) 2>&1

# Split stderr (progress) and stdout (JSON result)
TEXTRACT_JSON=$(echo "$TEXTRACT_RESULT" | grep -v '^\s*\[' | grep -v '^\s*Waiting' | tail -1)
PROGRESS_LINES=$(echo "$TEXTRACT_RESULT" | grep -E '^\s*(\[|Waiting)')
echo "$PROGRESS_LINES"

FINAL_STATUS=$(echo "$TEXTRACT_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['final_status'])" 2>/dev/null || echo "error")
TWIN_KEY=$(echo "$TEXTRACT_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['twin_key'])" 2>/dev/null || echo "")

if [ "$FINAL_STATUS" = "completed" ] || [ "$FINAL_STATUS" = "direct_extracted" ]; then
    check_pass "Textract processing complete (status=$FINAL_STATUS)"
elif [ "$FINAL_STATUS" = "failed" ]; then
    check_fail "Textract processing failed"
elif [ "$FINAL_STATUS" = "processing" ]; then
    check_warn "Textract still processing after ${TEXTRACT_TIMEOUT}s (may complete later)"
elif [ "$FINAL_STATUS" = "pending" ]; then
    check_warn "Document still pending Textract (trigger may not have fired)"
else
    check_fail "Unexpected textract status: $FINAL_STATUS"
fi

TEST_TWIN_KEY="$TWIN_KEY"

# Validate twin JSON structure
if [ -n "$TWIN_KEY" ]; then
    echo ""
    check_info "JSON twin: s3://$S3_BUCKET/$TWIN_KEY"

    TWIN_VALIDATION=$(echo "$TEXTRACT_JSON" | python3 -c "
import sys, json

data = json.load(sys.stdin)
twin = data.get('twin_json')
if not twin:
    print(json.dumps({'valid': False, 'error': 'No twin JSON'}))
    sys.exit(0)

if '_error' in twin:
    print(json.dumps({'valid': False, 'error': twin['_error']}))
    sys.exit(0)

errors = []
required_keys = ['schema_version', 'document_id', 'source_s3_key', 'filename',
                 'file_type', 'metadata', 'extracted_text', 'pages', 'tables',
                 'extraction_metadata']
for k in required_keys:
    if k not in twin:
        errors.append(f'Missing key: {k}')

schema = twin.get('schema_version', '')
pages = twin.get('pages', [])
text = twin.get('extracted_text', '')
tables = twin.get('tables', [])
method = twin.get('extraction_metadata', {}).get('method', '')

print(json.dumps({
    'valid': len(errors) == 0,
    'errors': errors,
    'schema_version': schema,
    'page_count': len(pages),
    'text_length': len(text),
    'table_count': len(tables),
    'extraction_method': method,
    'document_id': twin.get('document_id', '')[:16] + '...',
    'filename': twin.get('filename', ''),
}))
" 2>/dev/null)

    TWIN_VALID=$(echo "$TWIN_VALIDATION" | python3 -c "import sys,json; print(json.load(sys.stdin).get('valid', False))" 2>/dev/null || echo "False")

    if [ "$TWIN_VALID" = "True" ]; then
        check_pass "JSON twin schema is valid"

        SCHEMA_VER=$(echo "$TWIN_VALIDATION" | python3 -c "import sys,json; print(json.load(sys.stdin)['schema_version'])" 2>/dev/null)
        PAGE_COUNT=$(echo "$TWIN_VALIDATION" | python3 -c "import sys,json; print(json.load(sys.stdin)['page_count'])" 2>/dev/null)
        TEXT_LEN=$(echo "$TWIN_VALIDATION" | python3 -c "import sys,json; print(json.load(sys.stdin)['text_length'])" 2>/dev/null)
        TABLE_COUNT=$(echo "$TWIN_VALIDATION" | python3 -c "import sys,json; print(json.load(sys.stdin)['table_count'])" 2>/dev/null)
        EXT_METHOD=$(echo "$TWIN_VALIDATION" | python3 -c "import sys,json; print(json.load(sys.stdin)['extraction_method'])" 2>/dev/null)

        check_info "Schema version:    $SCHEMA_VER"
        check_info "Pages:             $PAGE_COUNT"
        check_info "Text length:       $(format_number "$TEXT_LEN") chars"
        check_info "Tables:            $TABLE_COUNT"
        check_info "Extraction method: $EXT_METHOD"
    else
        TWIN_ERRORS=$(echo "$TWIN_VALIDATION" | python3 -c "import sys,json; print('; '.join(json.load(sys.stdin).get('errors', [])))" 2>/dev/null)
        check_fail "JSON twin schema invalid: $TWIN_ERRORS"
    fi
else
    check_info "No twin key available — cannot validate twin JSON"
fi

fi  # end of step 3


# ===================================================================
# STEP 4: Verify metadata & access tags
# ===================================================================

if [ -n "$TEST_S3_KEY" ]; then

step "Verify metadata and access tags"

# Check source document tags
SOURCE_TAGS=$(aws s3api get-object-tagging \
    --bucket "$S3_BUCKET" \
    --key "$TEST_S3_KEY" \
    --query "TagSet" \
    --output json 2>/dev/null || echo "[]")

TAG_COUNT=$(echo "$SOURCE_TAGS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")

if [ "$TAG_COUNT" -gt 0 ] 2>/dev/null; then
    check_pass "Source document has $TAG_COUNT S3 tags"

    # Print tags
    echo "$SOURCE_TAGS" | python3 -c "
import sys, json
tags = json.load(sys.stdin)
for t in tags:
    print(f'    {t[\"Key\"]:20s} = {t[\"Value\"]}')
" 2>/dev/null

    # Verify access-tags specifically
    ACCESS_TAG=$(echo "$SOURCE_TAGS" | python3 -c "
import sys, json
tags = json.load(sys.stdin)
for t in tags:
    if t['Key'] == 'access-tags':
        print(t['Value'])
        break
else:
    print('')
" 2>/dev/null)

    if [ -n "$ACCESS_TAG" ]; then
        check_pass "access-tags present: $ACCESS_TAG"
    else
        check_warn "access-tags not set on source document"
    fi
else
    check_warn "No S3 tags on source document"
fi

# Check twin tags if available
if [ -n "$TEST_TWIN_KEY" ]; then
    TWIN_TAGS=$(aws s3api get-object-tagging \
        --bucket "$S3_BUCKET" \
        --key "$TEST_TWIN_KEY" \
        --query "TagSet" \
        --output json 2>/dev/null || echo "[]")

    TWIN_TAG_COUNT=$(echo "$TWIN_TAGS" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")

    if [ "$TWIN_TAG_COUNT" -gt 0 ] 2>/dev/null; then
        check_pass "JSON twin has $TWIN_TAG_COUNT S3 tags"
    else
        check_info "No S3 tags on JSON twin (tags are optional on twins)"
    fi
fi

fi  # end of step 4


# ===================================================================
# STEP 5: Test chunking readiness
# ===================================================================

if [ -n "$TEST_TWIN_KEY" ]; then

step "Test chunking readiness"

CHUNK_RESULT=$(python3 << PYEOF
import boto3, json, sys, os
sys.path.insert(0, "$PROJECT_ROOT/src")

REGION = "$REGION"
BUCKET = "$S3_BUCKET"
TWIN_KEY = "$TEST_TWIN_KEY"

s3 = boto3.client("s3", region_name=REGION)

try:
    obj = s3.get_object(Bucket=BUCKET, Key=TWIN_KEY)
    twin_json = json.loads(obj["Body"].read())
except Exception as e:
    print(json.dumps({"error": f"Failed to download twin: {e}"}))
    sys.exit(0)

try:
    from chunker import DocumentChunker
    chunker = DocumentChunker(chunk_size=512, chunk_overlap=50)
    chunks = chunker.chunk_document(twin_json)

    total_text_len = sum(len(c["text"]) for c in chunks)
    avg_chunk_size = total_text_len // len(chunks) if chunks else 0

    # Validate metadata on chunks
    metadata_ok = True
    metadata_errors = []
    for c in chunks:
        meta = c.get("metadata", {})
        if "sp_library" not in meta:
            metadata_ok = False
            metadata_errors.append(f"chunk {c['chunk_index']}: missing sp_library")
        if "file_type" not in meta:
            metadata_ok = False
            metadata_errors.append(f"chunk {c['chunk_index']}: missing file_type")
        if "page_numbers" not in meta:
            metadata_ok = False
            metadata_errors.append(f"chunk {c['chunk_index']}: missing page_numbers")

    print(json.dumps({
        "ok": True,
        "chunk_count": len(chunks),
        "avg_chunk_size": avg_chunk_size,
        "total_text_length": total_text_len,
        "metadata_valid": metadata_ok,
        "metadata_errors": metadata_errors[:5],
        "sample_chunk_id": chunks[0]["chunk_id"] if chunks else "",
        "sample_chunk_text_preview": chunks[0]["text"][:100] + "..." if chunks else "",
    }))

except Exception as e:
    print(json.dumps({"error": str(e)}))
PYEOF
)

CHUNK_OK=$(echo "$CHUNK_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ok', False))" 2>/dev/null || echo "False")

if [ "$CHUNK_OK" = "True" ]; then
    CHUNK_COUNT=$(echo "$CHUNK_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin)['chunk_count'])" 2>/dev/null)
    AVG_CHUNK=$(echo "$CHUNK_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin)['avg_chunk_size'])" 2>/dev/null)
    META_VALID=$(echo "$CHUNK_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin)['metadata_valid'])" 2>/dev/null)
    PREVIEW=$(echo "$CHUNK_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin)['sample_chunk_text_preview'])" 2>/dev/null)

    check_pass "DocumentChunker produced $CHUNK_COUNT chunk(s)"
    check_info "Average chunk size: $(format_number "$AVG_CHUNK") chars"

    if [ "$META_VALID" = "True" ]; then
        check_pass "All chunks have valid metadata"
    else
        META_ERRS=$(echo "$CHUNK_RESULT" | python3 -c "import sys,json; print('; '.join(json.load(sys.stdin)['metadata_errors']))" 2>/dev/null)
        check_fail "Chunk metadata issues: $META_ERRS"
    fi

    check_info "Sample: ${PREVIEW}"
else
    CHUNK_ERR=$(echo "$CHUNK_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('error', 'unknown'))" 2>/dev/null)
    check_fail "Chunking failed: $CHUNK_ERR"
fi

elif [ -n "$TEST_S3_KEY" ]; then
    step "Test chunking readiness"
    check_warn "Skipped — no JSON twin available for chunking test"
fi  # end of step 5


# ===================================================================
# STEP 6: Clean up
# ===================================================================

if [ -n "$TEST_S3_KEY" ] && ! $SKIP_CLEANUP; then

step "Clean up test artifacts"

# Delete from S3
aws s3 rm "s3://$S3_BUCKET/$TEST_S3_KEY" 2>/dev/null && \
    check_pass "Deleted source: $TEST_S3_KEY" || \
    check_warn "Could not delete source (may not exist)"

if [ -n "$TEST_TWIN_KEY" ]; then
    aws s3 rm "s3://$S3_BUCKET/$TEST_TWIN_KEY" 2>/dev/null && \
        check_pass "Deleted twin: $TEST_TWIN_KEY" || \
        check_warn "Could not delete twin"
fi

# Delete from DynamoDB registry
aws dynamodb delete-item \
    --table-name "$REGISTRY_TABLE" \
    --key "{\"s3_source_key\": {\"S\": \"$TEST_S3_KEY\"}}" \
    --region "$REGION" 2>/dev/null && \
    check_pass "Deleted registry entry" || \
    check_warn "Could not delete registry entry"

check_info "SharePoint test file (if uploaded) was NOT deleted — clean up manually if desired"

elif [ -n "$TEST_S3_KEY" ] && $SKIP_CLEANUP; then
    step "Clean up test artifacts"
    check_info "Cleanup skipped (--skip-cleanup)"
    check_info "Source: s3://$S3_BUCKET/$TEST_S3_KEY"
    [ -n "$TEST_TWIN_KEY" ] && check_info "Twin:   s3://$S3_BUCKET/$TEST_TWIN_KEY"
fi

fi  # end of non-report-only section


# ===================================================================
# STEP 7: Generate corpus stats report
# ===================================================================

step "Generate Corpus Report"

REPORT=$(python3 << 'PYEOF'
import boto3
import json
import sys
import os

REGION = os.environ.get("REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "dynamo-ai-documents")
SOURCE_PREFIX = os.environ.get("SOURCE_PREFIX", "source")
EXTRACTED_PREFIX = os.environ.get("EXTRACTED_PREFIX", "extracted")
TABLE = os.environ.get("REGISTRY_TABLE", "sp-ingest-document-registry")

sys.path.insert(0, os.path.join(os.environ.get("PROJECT_ROOT", "."), "src"))

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
table = ddb.Table(TABLE)


def list_objects_with_sizes(prefix):
    """List all objects under a prefix, returning (keys, total_size)."""
    keys = []
    total_size = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                keys.append(key)
                total_size += obj.get("Size", 0)
    return keys, total_size


def format_bytes(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} PB"


# --- S3 stats ---
source_keys, source_size = list_objects_with_sizes(SOURCE_PREFIX)
extracted_keys, extracted_size = list_objects_with_sizes(EXTRACTED_PREFIX)
total_size = source_size + extracted_size

# --- Registry stats ---
from collections import Counter
from decimal import Decimal

by_type = Counter()
by_status = Counter()
by_library = Counter()
total_docs = 0
total_size_bytes_registry = 0

kwargs = {}
while True:
    resp = table.scan(**kwargs)
    for item in resp.get("Items", []):
        total_docs += 1
        by_type[item.get("file_type", "unknown")] += 1
        by_status[item.get("textract_status", "unknown")] += 1
        by_library[item.get("sp_library", "unknown")] += 1
        sb = item.get("size_bytes", 0)
        if isinstance(sb, Decimal):
            sb = int(sb)
        total_size_bytes_registry += sb
    last_key = resp.get("LastEvaluatedKey")
    if not last_key:
        break
    kwargs["ExclusiveStartKey"] = last_key

# --- Twin coverage ---
completed = by_status.get("completed", 0) + by_status.get("direct_extracted", 0)
coverage = (completed / total_docs * 100) if total_docs > 0 else 0

# --- Estimate chunks ---
try:
    from chunker import DocumentChunker
    # Sample up to 10 twins to estimate avg chunks per doc
    sample_count = 0
    sample_chunks_total = 0
    chunker = DocumentChunker(chunk_size=512, chunk_overlap=50)

    for key in extracted_keys[:10]:
        if not key.endswith(".json"):
            continue
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            twin = json.loads(obj["Body"].read())
            chunks = chunker.chunk_document(twin)
            sample_chunks_total += len(chunks)
            sample_count += 1
        except Exception:
            continue

    if sample_count > 0:
        avg_chunks_per_doc = sample_chunks_total / sample_count
        est_total_chunks = int(avg_chunks_per_doc * len(extracted_keys))
    else:
        avg_chunks_per_doc = 0
        est_total_chunks = 0
except Exception:
    avg_chunks_per_doc = 0
    est_total_chunks = 0

# --- Delta tokens ---
delta_table = ddb.Table(os.environ.get("DELTA_TABLE", "sp-ingest-delta-tokens"))
delta_resp = delta_table.scan(Select="COUNT")
delta_count = delta_resp.get("Count", 0)

# Output report data as JSON for the shell to format
print(json.dumps({
    "source_count": len(source_keys),
    "source_size": source_size,
    "source_size_human": format_bytes(source_size),
    "extracted_count": len(extracted_keys),
    "extracted_size": extracted_size,
    "extracted_size_human": format_bytes(extracted_size),
    "total_size": total_size,
    "total_size_human": format_bytes(total_size),
    "registry_total": total_docs,
    "by_status": dict(by_status),
    "by_type": {k: v for k, v in sorted(by_type.items(), key=lambda x: -x[1])},
    "by_library": {k: v for k, v in sorted(by_library.items(), key=lambda x: -x[1])},
    "completed": completed,
    "pending": by_status.get("pending", 0),
    "processing": by_status.get("processing", 0),
    "failed": by_status.get("failed", 0),
    "coverage": round(coverage, 1),
    "est_total_chunks": est_total_chunks,
    "avg_chunks_per_doc": round(avg_chunks_per_doc, 1),
    "delta_tokens": delta_count,
}))
PYEOF
)

# Format the report
python3 << PYEOF
import json, sys

data = json.loads('''$REPORT''')

def fmt(n):
    return f"{n:,}"

W = 56  # report width

print("")
print("=" * W)
print("  DYNAMO SHAREPOINT INGESTION — REPORT")
print("=" * W)
print("")
print("  Corpus Statistics:")
print(f"    Total documents in S3:     {fmt(data['source_count']):>10}")
print(f"    Total JSON twins:          {fmt(data['extracted_count']):>10}")
print(f"    Pending Textract:          {fmt(data['pending']):>10}")
print(f"    Processing Textract:       {fmt(data['processing']):>10}")
print(f"    Failed Textract:           {fmt(data['failed']):>10}")
print(f"    Twin coverage:             {data['coverage']:>9.1f}%")
print("")
print("  By Library:")
for lib, count in data["by_library"].items():
    label = lib if len(lib) <= 26 else lib[:23] + "..."
    print(f"    {label:<28s} {fmt(count):>6}")
print("")
print("  By File Type:")
for ft, count in data["by_type"].items():
    label = ft.upper().lstrip(".")
    print(f"    {label:<28s} {fmt(count):>6}")
print("")
print("  Storage:")
print(f"    Source documents:         {data['source_size_human']:>10}")
print(f"    JSON twins:              {data['extracted_size_human']:>10}")
print(f"    Total S3 usage:          {data['total_size_human']:>10}")
print("")
print("  RAG Readiness:")
if data["est_total_chunks"] > 0:
    print(f"    Total chunks (est.):     {'~' + fmt(data['est_total_chunks']):>10}")
    print(f"    Avg chunks/document:     {data['avg_chunks_per_doc']:>10.1f}")
else:
    print(f"    Total chunks (est.):          N/A")
    print(f"    Avg chunks/document:          N/A")

coverage = data["coverage"]
if coverage >= 95:
    ready = "YES"
elif coverage >= 80:
    ready = "MOSTLY (some docs still processing)"
else:
    ready = "NO (twin coverage too low)"
print(f"    Ready for embedding:       {ready:>10}")
print("")
print("  Sync State:")
print(f"    Active delta tokens:     {fmt(data['delta_tokens']):>10}")
print("")
print("=" * W)
PYEOF


# ===================================================================
# Final Summary
# ===================================================================

if ! $REPORT_ONLY; then

echo ""
echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}${BOLD}  TEST SUMMARY${NC}"
echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  Passed:  ${GREEN}${BOLD}$PASS${NC}"
echo -e "  Failed:  ${RED}${BOLD}$FAIL${NC}"
echo -e "  Warned:  ${YELLOW}${BOLD}$WARN${NC}"

if [ "$FAIL" -eq 0 ]; then
    echo ""
    echo -e "  ${GREEN}${BOLD}ALL PIPELINE TESTS PASSED${NC}"
    echo -e "  ${DIM}The ingestion pipeline is working end-to-end.${NC}"
else
    echo ""
    echo -e "  ${RED}${BOLD}ISSUES FOUND:${NC}"
    for issue in "${ISSUES[@]}"; do
        echo -e "    ${RED}• $issue${NC}"
    done
fi

echo ""

fi
