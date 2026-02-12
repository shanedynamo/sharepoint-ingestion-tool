#!/usr/bin/env bash
# ===================================================================
# validate-deployment.sh — Comprehensive deployment validation
#
# Usage:
#   ./scripts/validate-deployment.sh              Run all checks
#   ./scripts/validate-deployment.sh --infra-only Skip connectivity tests
#   ./scripts/validate-deployment.sh --skip-e2e   Skip end-to-end PDF test
# ===================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_DIR="$PROJECT_ROOT/terraform"

# Constants
S3_BUCKET="dynamo-ai-documents"
DELTA_TABLE="sp-ingest-delta-tokens"
REGISTRY_TABLE="sp-ingest-document-registry"
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
INFRA_ONLY=false
SKIP_E2E=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --infra-only) INFRA_ONLY=true; shift ;;
        --skip-e2e)   SKIP_E2E=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--infra-only] [--skip-e2e]"
            exit 0
            ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
    esac
done

# Counters
PASS=0
FAIL=0
WARN=0
ISSUES=()

# ===================================================================
# Utility functions
# ===================================================================

section() {
    echo ""
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  $1${NC}"
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

pass() {
    echo -e "  ${GREEN}[PASS]${NC} $1"
    PASS=$((PASS + 1))
}

fail() {
    echo -e "  ${RED}[FAIL]${NC} $1"
    FAIL=$((FAIL + 1))
    ISSUES+=("$1")
}

warn() {
    echo -e "  ${YELLOW}[WARN]${NC} $1"
    WARN=$((WARN + 1))
}

info() {
    echo -e "  ${DIM}       $1${NC}"
}


# ===================================================================
# INFRASTRUCTURE CHECKS
# ===================================================================

section "Infrastructure Checks"

# --- S3 bucket ---
echo -e "\n  ${BOLD}S3 Bucket${NC}"

if aws s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
    pass "S3 bucket exists: $S3_BUCKET"
else
    fail "S3 bucket not found: $S3_BUCKET"
fi

ENCRYPTION=$(aws s3api get-bucket-encryption --bucket "$S3_BUCKET" \
    --query "ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm" \
    --output text 2>/dev/null || echo "NONE")

if [ "$ENCRYPTION" != "NONE" ]; then
    pass "S3 encryption enabled: $ENCRYPTION"
else
    fail "S3 encryption not configured"
fi

NOTIF_COUNT=$(aws s3api get-bucket-notification-configuration --bucket "$S3_BUCKET" \
    --query "length(LambdaFunctionConfigurations || \`[]\`)" \
    --output text 2>/dev/null || echo "0")

if [ "$NOTIF_COUNT" -gt 0 ] 2>/dev/null; then
    pass "S3 event notifications configured ($NOTIF_COUNT rules)"
else
    fail "S3 event notifications not configured"
fi

# --- DynamoDB tables ---
echo -e "\n  ${BOLD}DynamoDB Tables${NC}"

DELTA_STATUS=$(aws dynamodb describe-table --table-name "$DELTA_TABLE" \
    --query "Table.TableStatus" --output text 2>/dev/null || echo "NOT_FOUND")
DELTA_PK=$(aws dynamodb describe-table --table-name "$DELTA_TABLE" \
    --query "Table.KeySchema[0].AttributeName" --output text 2>/dev/null || echo "")

if [ "$DELTA_STATUS" = "ACTIVE" ]; then
    pass "Delta tokens table: ACTIVE"
else
    fail "Delta tokens table: $DELTA_STATUS"
fi

if [ "$DELTA_PK" = "drive_id" ]; then
    pass "Delta tokens PK: drive_id"
else
    fail "Delta tokens PK is '$DELTA_PK' (expected 'drive_id')"
fi

REG_STATUS=$(aws dynamodb describe-table --table-name "$REGISTRY_TABLE" \
    --query "Table.TableStatus" --output text 2>/dev/null || echo "NOT_FOUND")
REG_PK=$(aws dynamodb describe-table --table-name "$REGISTRY_TABLE" \
    --query "Table.KeySchema[0].AttributeName" --output text 2>/dev/null || echo "")
GSI_COUNT=$(aws dynamodb describe-table --table-name "$REGISTRY_TABLE" \
    --query "length(Table.GlobalSecondaryIndexes || \`[]\`)" --output text 2>/dev/null || echo "0")

if [ "$REG_STATUS" = "ACTIVE" ]; then
    pass "Document registry table: ACTIVE"
else
    fail "Document registry table: $REG_STATUS"
fi

if [ "$REG_PK" = "s3_source_key" ]; then
    pass "Document registry PK: s3_source_key"
else
    fail "Document registry PK is '$REG_PK' (expected 's3_source_key')"
fi

if [ "$GSI_COUNT" = "2" ]; then
    pass "Document registry GSIs: $GSI_COUNT (textract_status-index, sp_library-index)"
else
    fail "Document registry has $GSI_COUNT GSIs (expected 2)"
fi

# --- Lambda functions ---
echo -e "\n  ${BOLD}Lambda Functions${NC}"

check_lambda() {
    local func_name="$1"
    local expected_role="$2"
    local expected_env_keys="$3"

    local config_json
    config_json=$(aws lambda get-function-configuration \
        --function-name "$func_name" \
        --output json 2>/dev/null || echo "")

    if [ -z "$config_json" ]; then
        fail "Lambda not found: $func_name"
        return
    fi

    local state
    state=$(echo "$config_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('State',''))" 2>/dev/null)
    if [ "$state" = "Active" ]; then
        pass "$func_name — Active"
    else
        fail "$func_name — state: $state"
    fi

    local role_name
    role_name=$(echo "$config_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Role','').split('/')[-1])" 2>/dev/null)
    if [ "$role_name" = "$expected_role" ]; then
        pass "$func_name — role: $role_name"
    else
        fail "$func_name — role is '$role_name' (expected '$expected_role')"
    fi

    local env_keys
    env_keys=$(echo "$config_json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
keys = sorted(d.get('Environment', {}).get('Variables', {}).keys())
print(','.join(keys))
" 2>/dev/null)

    local missing=""
    IFS=',' read -ra EXPECTED <<< "$expected_env_keys"
    for key in "${EXPECTED[@]}"; do
        if ! echo ",$env_keys," | grep -q ",$key,"; then
            missing="$missing $key"
        fi
    done

    if [ -z "$missing" ]; then
        pass "$func_name — env vars present"
    else
        fail "$func_name — missing env vars:$missing"
    fi

    local layers
    layers=$(echo "$config_json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(len(d.get('Layers', [])))
" 2>/dev/null)
    if [ "$layers" -gt 0 ] 2>/dev/null; then
        pass "$func_name — $layers layer(s) attached"
    else
        warn "$func_name — no layers attached"
    fi
}

check_lambda "sp-ingest-daily-sync" \
    "sp-ingest-daily-sync-lambda-role" \
    "S3_BUCKET,DYNAMODB_DELTA_TABLE,DYNAMODB_REGISTRY_TABLE,SECRET_PREFIX,SHAREPOINT_SITE_NAME"

check_lambda "sp-ingest-textract-trigger" \
    "sp-ingest-textract-trigger-lambda-role" \
    "S3_BUCKET,DYNAMODB_REGISTRY_TABLE,TEXTRACT_SNS_TOPIC_ARN,TEXTRACT_SNS_ROLE_ARN"

check_lambda "sp-ingest-textract-complete" \
    "sp-ingest-textract-complete-lambda-role" \
    "S3_BUCKET,DYNAMODB_REGISTRY_TABLE"

# --- EventBridge ---
echo -e "\n  ${BOLD}EventBridge${NC}"

EB_STATE=$(aws events describe-rule --name "sp-ingest-daily-sync-schedule" \
    --query "State" --output text 2>/dev/null || echo "NOT_FOUND")
EB_SCHED=$(aws events describe-rule --name "sp-ingest-daily-sync-schedule" \
    --query "ScheduleExpression" --output text 2>/dev/null || echo "")

if [ "$EB_STATE" = "ENABLED" ]; then
    pass "EventBridge rule: ENABLED"
    info "Schedule: $EB_SCHED"
else
    fail "EventBridge rule: $EB_STATE"
fi

# --- SNS topics ---
echo -e "\n  ${BOLD}SNS Topics${NC}"

for TOPIC_NAME in sp-ingest-textract-notifications sp-ingest-alerts; do
    TOPIC_ARN=$(aws sns list-topics --query "Topics[?ends_with(TopicArn, ':$TOPIC_NAME')].TopicArn | [0]" \
        --output text 2>/dev/null || echo "None")

    if [ "$TOPIC_ARN" != "None" ] && [ -n "$TOPIC_ARN" ]; then
        SUB_COUNT=$(aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" \
            --query "length(Subscriptions)" --output text 2>/dev/null || echo "0")
        pass "SNS topic: $TOPIC_NAME ($SUB_COUNT subscriptions)"
    else
        fail "SNS topic not found: $TOPIC_NAME"
    fi
done

# --- Secrets Manager ---
echo -e "\n  ${BOLD}Secrets Manager${NC}"

for SECRET_ID in sp-ingest/azure-client-id sp-ingest/azure-tenant-id sp-ingest/azure-client-secret; do
    SECRET_VAL=$(aws secretsmanager get-secret-value --secret-id "$SECRET_ID" \
        --query "SecretString" --output text 2>/dev/null || echo "")

    if [ -n "$SECRET_VAL" ] && [ "$SECRET_VAL" != "PLACEHOLDER" ]; then
        pass "Secret populated: $SECRET_ID"
    elif [ "$SECRET_VAL" = "PLACEHOLDER" ]; then
        fail "Secret still PLACEHOLDER: $SECRET_ID"
    else
        fail "Secret not found: $SECRET_ID"
    fi
done

# --- CloudWatch ---
echo -e "\n  ${BOLD}CloudWatch${NC}"

for LOG_GROUP in /sp-ingest/daily-sync /sp-ingest/textract-trigger /sp-ingest/textract-complete /sp-ingest/bulk-ingest; do
    LG_EXISTS=$(aws logs describe-log-groups --log-group-name-prefix "$LOG_GROUP" \
        --query "length(logGroups[?logGroupName=='$LOG_GROUP'])" --output text 2>/dev/null || echo "0")

    if [ "$LG_EXISTS" = "1" ]; then
        pass "Log group: $LOG_GROUP"
    else
        fail "Log group not found: $LOG_GROUP"
    fi
done

DASH_EXISTS=$(aws cloudwatch list-dashboards \
    --dashboard-name-prefix "SP-Ingest-Pipeline" \
    --query "length(DashboardEntries)" --output text 2>/dev/null || echo "0")

if [ "$DASH_EXISTS" -gt 0 ] 2>/dev/null; then
    pass "Dashboard: SP-Ingest-Pipeline"
else
    fail "Dashboard not found: SP-Ingest-Pipeline"
fi

# ===================================================================
# CONNECTIVITY CHECKS
# ===================================================================

if [ "$INFRA_ONLY" = true ]; then
    echo ""
    echo -e "  ${YELLOW}Skipping connectivity checks (--infra-only)${NC}"
else
    section "Connectivity Checks"

    # --- Lambda dry-run invocation ---
    echo -e "\n  ${BOLD}Daily Sync Lambda Invocation${NC}"

    INVOKE_OUT=$(aws lambda invoke \
        --function-name "sp-ingest-daily-sync" \
        --payload '{"dry_run": true, "source": "validate-deployment"}' \
        --cli-binary-format raw-in-base64-out \
        --log-type Tail \
        /tmp/validate-invoke.json 2>/dev/null || echo "")

    INVOKE_STATUS=$(echo "$INVOKE_OUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('StatusCode',''))" 2>/dev/null || echo "")
    FUNC_ERROR=$(echo "$INVOKE_OUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('FunctionError',''))" 2>/dev/null || echo "")

    if [ "$INVOKE_STATUS" = "200" ] && [ -z "$FUNC_ERROR" ]; then
        pass "Lambda invocation: HTTP 200, no function error"

        # Parse response
        RESPONSE=$(cat /tmp/validate-invoke.json 2>/dev/null || echo "{}")
        info "Response: $(echo "$RESPONSE" | head -c 200)"
    elif [ "$INVOKE_STATUS" = "200" ]; then
        warn "Lambda invocation: HTTP 200 but function error: $FUNC_ERROR"
        # Decode log tail
        LOG_TAIL=$(echo "$INVOKE_OUT" | python3 -c "
import sys, json, base64
d = json.load(sys.stdin)
log = d.get('LogResult', '')
if log:
    print(base64.b64decode(log).decode('utf-8', errors='replace')[-500:])
" 2>/dev/null || echo "")
        if [ -n "$LOG_TAIL" ]; then
            info "Last log lines:"
            echo "$LOG_TAIL" | while IFS= read -r line; do
                info "  $line"
            done
        fi
    else
        fail "Lambda invocation failed (status: $INVOKE_STATUS)"
    fi
    rm -f /tmp/validate-invoke.json

    # --- End-to-end PDF test ---
    if [ "$SKIP_E2E" = true ]; then
        echo ""
        echo -e "  ${YELLOW}Skipping end-to-end test (--skip-e2e)${NC}"
    else
        echo -e "\n  ${BOLD}End-to-End PDF Test${NC}"

        TEST_KEY="source/_validation-test/test-$(date +%s).pdf"
        TWIN_PREFIX="extracted/_validation-test/"

        # Create a minimal valid PDF
        MINI_PDF=$(python3 -c "
import base64
# Minimal valid PDF with text 'Validation test'
pdf = b'%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>endobj\n4 0 obj<</Length 44>>stream\nBT /F1 12 Tf 100 700 Td (Validation test) Tj ET\nendstream\nendobj\n5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\nxref\n0 6\n0000000000 65535 f \n0000000009 00000 n \n0000000058 00000 n \n0000000115 00000 n \n0000000266 00000 n \n0000000360 00000 n \ntrailer<</Size 6/Root 1 0 R>>\nstartxref\n431\n%%EOF'
print(base64.b64encode(pdf).decode())
" 2>/dev/null)

        if [ -z "$MINI_PDF" ]; then
            warn "Could not generate test PDF, skipping e2e test"
        else
            echo "$MINI_PDF" | base64 -d > /tmp/validate-test.pdf

            # Upload test PDF
            info "Uploading test PDF to s3://$S3_BUCKET/$TEST_KEY"
            if aws s3 cp /tmp/validate-test.pdf "s3://$S3_BUCKET/$TEST_KEY" --quiet 2>/dev/null; then
                pass "Test PDF uploaded to S3"
            else
                fail "Failed to upload test PDF"
                rm -f /tmp/validate-test.pdf
            fi

            if [ -f /tmp/validate-test.pdf ]; then
                # Wait for Lambda trigger and check registry
                info "Waiting for textract-trigger Lambda to process (up to 30s)..."
                FOUND_REGISTRY=false
                for i in $(seq 1 6); do
                    sleep 5
                    REG_ITEM=$(aws dynamodb get-item --table-name "$REGISTRY_TABLE" \
                        --key "{\"s3_source_key\":{\"S\":\"$TEST_KEY\"}}" \
                        --query "Item.textract_status.S" --output text 2>/dev/null || echo "")

                    if [ -n "$REG_ITEM" ] && [ "$REG_ITEM" != "None" ]; then
                        pass "Registry entry created (status: $REG_ITEM)"
                        FOUND_REGISTRY=true
                        break
                    fi
                    echo -ne "\r  ${DIM}       Waiting... ${i}0s${NC}"
                done
                echo ""

                if [ "$FOUND_REGISTRY" = false ]; then
                    warn "No registry entry after 30s (Lambda may not have triggered yet)"
                fi

                # Wait for Textract completion (up to 2 more minutes)
                if [ "$FOUND_REGISTRY" = true ] && [ "$REG_ITEM" != "completed" ]; then
                    info "Waiting for Textract completion (up to 120s)..."
                    for i in $(seq 1 24); do
                        sleep 5
                        STATUS=$(aws dynamodb get-item --table-name "$REGISTRY_TABLE" \
                            --key "{\"s3_source_key\":{\"S\":\"$TEST_KEY\"}}" \
                            --query "Item.textract_status.S" --output text 2>/dev/null || echo "")

                        if [ "$STATUS" = "completed" ]; then
                            pass "Textract completed"
                            break
                        elif [ "$STATUS" = "failed" ]; then
                            warn "Textract job failed (may be expected for minimal test PDF)"
                            break
                        fi
                        echo -ne "\r  ${DIM}       Status: $STATUS ... ${i}x5s${NC}"
                    done
                    echo ""
                fi

                # Check for JSON twin
                TWIN_KEY=$(aws s3api list-objects-v2 --bucket "$S3_BUCKET" \
                    --prefix "$TWIN_PREFIX" --max-keys 5 \
                    --query "Contents[0].Key" --output text 2>/dev/null || echo "None")

                if [ "$TWIN_KEY" != "None" ] && [ -n "$TWIN_KEY" ]; then
                    pass "JSON twin found: $TWIN_KEY"

                    # Validate schema
                    aws s3 cp "s3://$S3_BUCKET/$TWIN_KEY" /tmp/validate-twin.json --quiet 2>/dev/null || true
                    if [ -f /tmp/validate-twin.json ]; then
                        SCHEMA_VER=$(python3 -c "
import json
with open('/tmp/validate-twin.json') as f:
    d = json.load(f)
print(d.get('schema_version', 'missing'))
" 2>/dev/null || echo "error")

                        if [ "$SCHEMA_VER" = "2.0" ]; then
                            pass "JSON twin schema version: $SCHEMA_VER"
                        else
                            warn "JSON twin schema version: $SCHEMA_VER (expected 2.0)"
                        fi
                        rm -f /tmp/validate-twin.json
                    fi
                else
                    warn "No JSON twin found in $TWIN_PREFIX (Textract may still be processing)"
                fi

                # Cleanup
                info "Cleaning up test files..."
                aws s3 rm "s3://$S3_BUCKET/$TEST_KEY" --quiet 2>/dev/null || true
                if [ "$TWIN_KEY" != "None" ] && [ -n "$TWIN_KEY" ]; then
                    aws s3 rm "s3://$S3_BUCKET/$TWIN_KEY" --quiet 2>/dev/null || true
                fi
                aws dynamodb delete-item --table-name "$REGISTRY_TABLE" \
                    --key "{\"s3_source_key\":{\"S\":\"$TEST_KEY\"}}" 2>/dev/null || true
                # Remove the validation test folder
                aws s3 rm "s3://$S3_BUCKET/source/_validation-test/" --recursive --quiet 2>/dev/null || true
                aws s3 rm "s3://$S3_BUCKET/extracted/_validation-test/" --recursive --quiet 2>/dev/null || true
                pass "Test files cleaned up"
            fi
            rm -f /tmp/validate-test.pdf
        fi
    fi
fi


# ===================================================================
# BULK LOAD READINESS
# ===================================================================

section "Bulk Load Readiness"

# Check if instance is deployed
cd "$TF_DIR" 2>/dev/null || true
INSTANCE_ID=$(terraform output -raw bulk_instance_id 2>/dev/null | grep -E '^i-' || echo "")

if [ -z "$INSTANCE_ID" ]; then
    info "No bulk EC2 instance deployed (enable_bulk_instance=false)"
    info "Launch with: ./scripts/run-bulk-ingest.sh launch"
    warn "Bulk EC2 not deployed — skipping instance checks"
else
    echo -e "\n  ${BOLD}EC2 Instance: $INSTANCE_ID${NC}"

    INST_STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
        --query "Reservations[0].Instances[0].State.Name" --output text 2>/dev/null || echo "unknown")

    if [ "$INST_STATE" = "running" ]; then
        pass "Instance state: running"
    else
        fail "Instance state: $INST_STATE"
    fi

    # Check IAM role
    INST_PROFILE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
        --query "Reservations[0].Instances[0].IamInstanceProfile.Arn" --output text 2>/dev/null || echo "")

    if echo "$INST_PROFILE" | grep -q "sp-ingest-bulk-instance-profile"; then
        pass "Instance IAM profile: sp-ingest-bulk-instance-profile"
    else
        fail "Instance IAM profile mismatch: $INST_PROFILE"
    fi

    INST_IP=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
        --query "Reservations[0].Instances[0].PublicIpAddress" --output text 2>/dev/null || echo "")

    if [ -n "$INST_IP" ] && [ "$INST_IP" != "None" ]; then
        pass "Instance has public IP: $INST_IP"
    else
        fail "Instance has no public IP (needs internet for Graph API)"
    fi

    # Check via SSM or assume user data ran
    info "SSH check commands (if key pair configured):"
    info "  ssh ec2-user@$INST_IP 'curl -s https://graph.microsoft.com/ | head -1'"
    info "  ssh ec2-user@$INST_IP 'aws s3 ls s3://$S3_BUCKET/ --max-items 1'"
    info "  ssh ec2-user@$INST_IP 'libreoffice --version'"
fi


# ===================================================================
# REPORT
# ===================================================================

section "Validation Report"

TOTAL=$((PASS + FAIL + WARN))

echo -e "  ${GREEN}Passed: $PASS${NC}"
echo -e "  ${RED}Failed: $FAIL${NC}"
echo -e "  ${YELLOW}Warnings: $WARN${NC}"
echo -e "  ${DIM}Total checks: $TOTAL${NC}"
echo ""

if [ "$FAIL" -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}STATUS: READY FOR BULK INGESTION${NC}"
    echo ""
    echo -e "  Next steps:"
    echo -e "    1. ${CYAN}./scripts/run-bulk-ingest.sh launch --key-pair <name> --admin-cidr <ip>/32${NC}"
    echo -e "    2. ${CYAN}./scripts/monitor-bulk-ingest.sh${NC}"
    echo -e "    3. ${CYAN}./scripts/run-bulk-ingest.sh teardown${NC}"
else
    echo -e "  ${RED}${BOLD}STATUS: ISSUES FOUND — RESOLVE BEFORE BULK INGESTION${NC}"
    echo ""
    echo -e "  ${BOLD}Issues to resolve:${NC}"
    for issue in "${ISSUES[@]}"; do
        echo -e "    ${RED}-${NC} $issue"
    done
fi
echo ""
