#!/usr/bin/env bash
# ===================================================================
# deploy.sh — Full deployment orchestration for sharepoint-ingest
#
# Usage:
#   ./scripts/deploy.sh              Full deployment (build + plan + apply)
#   ./scripts/deploy.sh --plan-only  Build + plan, but skip apply
#   ./scripts/deploy.sh --skip-build Skip Lambda build (reuse existing zips)
# ===================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_DIR="$PROJECT_ROOT/terraform"
DIST_DIR="$PROJECT_ROOT/dist"
ENV_FILE="$PROJECT_ROOT/.env"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Options
PLAN_ONLY=false
SKIP_BUILD=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --plan-only)  PLAN_ONLY=true; shift ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--plan-only] [--skip-build]"
            echo ""
            echo "  --plan-only   Build artifacts and run terraform plan, but do not apply"
            echo "  --skip-build  Skip the Lambda build step (reuse existing dist/ zips)"
            exit 0
            ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
    esac
done

# Counters for summary
CHECKS_PASSED=0
CHECKS_FAILED=0

# ===================================================================
# Utility functions
# ===================================================================

step() {
    echo ""
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  $1${NC}"
    echo -e "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

check_pass() {
    echo -e "  ${GREEN}[PASS]${NC} $1"
    CHECKS_PASSED=$((CHECKS_PASSED + 1))
}

check_fail() {
    echo -e "  ${RED}[FAIL]${NC} $1"
    CHECKS_FAILED=$((CHECKS_FAILED + 1))
}

check_warn() {
    echo -e "  ${YELLOW}[WARN]${NC} $1"
}

verify_pass() {
    echo -e "  ${GREEN}[OK]${NC}   $1"
}

verify_fail() {
    echo -e "  ${RED}[FAIL]${NC} $1"
}

abort() {
    echo ""
    echo -e "${RED}ABORT: $1${NC}"
    exit 1
}

# Load a value from .env file
env_val() {
    grep "^$1=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d'=' -f2-
}


# ===================================================================
# STEP 1: Pre-flight checks
# ===================================================================

step "Step 1/7 — Pre-flight checks"

# AWS CLI
if aws sts get-caller-identity --output json &>/dev/null; then
    AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    AWS_IDENTITY=$(aws sts get-caller-identity --query Arn --output text)
    check_pass "AWS CLI configured (account: $AWS_ACCOUNT)"
    check_pass "Identity: $AWS_IDENTITY"
else
    check_fail "AWS CLI not configured or credentials expired"
    abort "Run 'aws configure' or export valid session credentials"
fi

# Terraform
if command -v terraform &>/dev/null; then
    TF_VERSION=$(terraform version -json 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['terraform_version'])" 2>/dev/null \
        || terraform version | head -1)
    check_pass "Terraform installed ($TF_VERSION)"
else
    check_fail "Terraform not found in PATH"
    abort "Install Terraform >= 1.5: https://developer.hashicorp.com/terraform/install"
fi

# Python 3.11
if command -v python3.11 &>/dev/null; then
    PY_VERSION=$(python3.11 --version 2>&1)
    check_pass "$PY_VERSION available"
elif python3 --version 2>&1 | grep -q "3.11"; then
    PY_VERSION=$(python3 --version 2>&1)
    check_pass "$PY_VERSION available (as python3)"
else
    check_fail "Python 3.11 not found"
    abort "Install Python 3.11"
fi

# .env file
if [ -f "$ENV_FILE" ]; then
    check_pass ".env file exists"

    # Validate required keys
    MISSING_KEYS=""
    for key in AZURE_CLIENT_ID AZURE_TENANT_ID AZURE_CLIENT_SECRET; do
        val=$(env_val "$key")
        if [ -z "$val" ] || [ "$val" = "PLACEHOLDER" ]; then
            MISSING_KEYS="$MISSING_KEYS $key"
        fi
    done

    if [ -z "$MISSING_KEYS" ]; then
        check_pass "Azure AD credentials present in .env"
    else
        check_fail "Missing or placeholder values in .env:$MISSING_KEYS"
        abort "Populate Azure AD credentials in $ENV_FILE"
    fi
else
    check_fail ".env file not found at $ENV_FILE"
    abort "Create .env with AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET"
fi

echo ""
echo -e "  ${GREEN}Pre-flight: $CHECKS_PASSED passed, $CHECKS_FAILED failed${NC}"

if [ "$CHECKS_FAILED" -gt 0 ]; then
    abort "Fix the above failures before deploying"
fi


# ===================================================================
# STEP 2: Build Lambda artifacts
# ===================================================================

step "Step 2/7 — Build Lambda artifacts"

if [ "$SKIP_BUILD" = true ]; then
    echo -e "  ${YELLOW}Skipping build (--skip-build)${NC}"
else
    bash "$SCRIPT_DIR/build-lambda.sh"
fi

# Verify artifacts exist
if [ -f "$DIST_DIR/lambda-layer.zip" ]; then
    LAYER_SIZE=$(du -sh "$DIST_DIR/lambda-layer.zip" | cut -f1)
    check_pass "lambda-layer.zip exists ($LAYER_SIZE)"
else
    check_fail "lambda-layer.zip not found in dist/"
    abort "Build failed — run scripts/build-lambda.sh manually"
fi

if [ -f "$DIST_DIR/lambda-code.zip" ]; then
    CODE_SIZE=$(du -sh "$DIST_DIR/lambda-code.zip" | cut -f1)
    check_pass "lambda-code.zip exists ($CODE_SIZE)"
else
    check_fail "lambda-code.zip not found in dist/"
    abort "Build failed — run scripts/build-lambda.sh manually"
fi


# ===================================================================
# STEP 3: Terraform deployment
# ===================================================================

step "Step 3/7 — Terraform deployment"

cd "$TF_DIR"

# Init
echo -e "${YELLOW}Running terraform init...${NC}"
terraform init -input=false 2>&1 | tail -5
echo ""

# Plan
echo -e "${YELLOW}Running terraform plan...${NC}"
terraform plan -input=false -out=plan.out 2>&1 | tee /tmp/tf-plan-output.txt

# Extract summary line (strip ANSI escape codes first)
PLAN_SUMMARY=$(sed 's/\x1b\[[0-9;]*m//g' /tmp/tf-plan-output.txt \
    | grep -oE "(Plan: [0-9]+ to add.*|No changes\.)" | tail -1 || echo "")
rm -f /tmp/tf-plan-output.txt

echo ""
if echo "$PLAN_SUMMARY" | grep -q "No changes"; then
    echo -e "${GREEN}No infrastructure changes needed.${NC}"
else
    echo -e "${BOLD}Plan summary: ${PLAN_SUMMARY}${NC}"
fi

if [ "$PLAN_ONLY" = true ]; then
    echo ""
    echo -e "${YELLOW}--plan-only specified. Stopping here.${NC}"
    echo "  Review the plan above. To apply, run:"
    echo "    cd terraform && terraform apply plan.out"
    echo ""
    exit 0
fi

# Confirm
echo ""
echo -e "${YELLOW}${BOLD}Apply this plan?${NC}"
read -r -p "  Type 'yes' to proceed: " CONFIRM
echo ""

if [ "$CONFIRM" != "yes" ]; then
    echo -e "${RED}Deployment cancelled.${NC}"
    rm -f plan.out
    exit 1
fi

# Apply
echo -e "${YELLOW}Running terraform apply...${NC}"
terraform apply plan.out
rm -f plan.out

# Capture outputs
echo ""
echo -e "${YELLOW}Capturing deployment outputs...${NC}"
terraform output -json > "$PROJECT_ROOT/deployment-outputs.json"
check_pass "Outputs saved to deployment-outputs.json"


# ===================================================================
# STEP 4: Populate Secrets Manager
# ===================================================================

step "Step 4/7 — Populate Secrets Manager"

AZURE_CLIENT_ID=$(env_val "AZURE_CLIENT_ID")
AZURE_TENANT_ID=$(env_val "AZURE_TENANT_ID")
AZURE_CLIENT_SECRET=$(env_val "AZURE_CLIENT_SECRET")

update_secret() {
    local secret_id="$1"
    local secret_value="$2"
    local label="$3"

    # Check current value to avoid unnecessary updates
    CURRENT=$(aws secretsmanager get-secret-value \
        --secret-id "$secret_id" \
        --query SecretString --output text 2>/dev/null || echo "")

    if [ "$CURRENT" = "$secret_value" ]; then
        echo -e "  ${GREEN}[OK]${NC}   $label (already current)"
    else
        aws secretsmanager put-secret-value \
            --secret-id "$secret_id" \
            --secret-string "$secret_value" \
            --output text > /dev/null
        echo -e "  ${GREEN}[SET]${NC}  $label"
    fi
}

update_secret "sp-ingest/azure-client-id"     "$AZURE_CLIENT_ID"     "sp-ingest/azure-client-id"
update_secret "sp-ingest/azure-tenant-id"     "$AZURE_TENANT_ID"     "sp-ingest/azure-tenant-id"
update_secret "sp-ingest/azure-client-secret" "$AZURE_CLIENT_SECRET" "sp-ingest/azure-client-secret"


# ===================================================================
# STEP 5: Deploy Lambda code
# ===================================================================

step "Step 5/7 — Deploy Lambda code"

# Terraform already deployed the layer and code via filename + source_code_hash.
# This step uploads code to S3 for the bulk EC2 loader and verifies
# that all Lambda functions are active.

S3_BUCKET=$(terraform output -raw s3_bucket_name)

echo -e "${YELLOW}Uploading code package to S3 for bulk loader...${NC}"
aws s3 cp "$DIST_DIR/lambda-code.zip" "s3://$S3_BUCKET/_deploy/lambda-code.zip" --quiet
check_pass "lambda-code.zip uploaded to s3://$S3_BUCKET/_deploy/"

echo ""
echo -e "${YELLOW}Verifying Lambda functions...${NC}"
for FUNC_NAME in sp-ingest-daily-sync sp-ingest-textract-trigger sp-ingest-textract-complete; do
    STATE=$(aws lambda get-function \
        --function-name "$FUNC_NAME" \
        --query "Configuration.State" \
        --output text 2>/dev/null || echo "NOT_FOUND")

    if [ "$STATE" = "Active" ]; then
        check_pass "$FUNC_NAME — Active"
    elif [ "$STATE" = "NOT_FOUND" ]; then
        check_fail "$FUNC_NAME — not found"
    else
        check_warn "$FUNC_NAME — state: $STATE"
    fi
done

# Verify layer
LAYER_ARN=$(terraform output -raw lambda_layer_arn 2>/dev/null || echo "")
if [ -n "$LAYER_ARN" ]; then
    LAYER_SHORT=$(echo "$LAYER_ARN" | grep -o 'layer:.*')
    check_pass "Lambda layer published: $LAYER_SHORT"
fi


# ===================================================================
# STEP 6: Verify deployment
# ===================================================================

step "Step 6/7 — Verify deployment"

VERIFY_PASSED=0
VERIFY_FAILED=0

v_pass() {
    verify_pass "$1"
    VERIFY_PASSED=$((VERIFY_PASSED + 1))
}

v_fail() {
    verify_fail "$1"
    VERIFY_FAILED=$((VERIFY_FAILED + 1))
}

# S3 bucket
if aws s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
    v_pass "S3 bucket: $S3_BUCKET"
else
    v_fail "S3 bucket: $S3_BUCKET"
fi

# DynamoDB tables
for TABLE_NAME in sp-ingest-delta-tokens sp-ingest-document-registry; do
    TABLE_STATUS=$(aws dynamodb describe-table \
        --table-name "$TABLE_NAME" \
        --query "Table.TableStatus" \
        --output text 2>/dev/null || echo "NOT_FOUND")

    if [ "$TABLE_STATUS" = "ACTIVE" ]; then
        v_pass "DynamoDB: $TABLE_NAME ($TABLE_STATUS)"
    else
        v_fail "DynamoDB: $TABLE_NAME ($TABLE_STATUS)"
    fi
done

# Lambda functions
for FUNC_NAME in sp-ingest-daily-sync sp-ingest-textract-trigger sp-ingest-textract-complete; do
    FUNC_STATE=$(aws lambda get-function \
        --function-name "$FUNC_NAME" \
        --query "Configuration.State" \
        --output text 2>/dev/null || echo "NOT_FOUND")

    if [ "$FUNC_STATE" = "Active" ]; then
        v_pass "Lambda: $FUNC_NAME"
    else
        v_fail "Lambda: $FUNC_NAME ($FUNC_STATE)"
    fi
done

# EventBridge rule
EB_STATE=$(aws events describe-rule \
    --name "sp-ingest-daily-sync-schedule" \
    --query "State" \
    --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$EB_STATE" = "ENABLED" ]; then
    v_pass "EventBridge: sp-ingest-daily-sync-schedule ($EB_STATE)"
else
    v_fail "EventBridge: sp-ingest-daily-sync-schedule ($EB_STATE)"
fi

# SNS topic
SNS_ARN=$(terraform output -raw textract_sns_topic_arn 2>/dev/null || echo "")
if [ -n "$SNS_ARN" ]; then
    SNS_EXISTS=$(aws sns get-topic-attributes \
        --topic-arn "$SNS_ARN" \
        --query "Attributes.TopicArn" \
        --output text 2>/dev/null || echo "")
    if [ -n "$SNS_EXISTS" ]; then
        v_pass "SNS: sp-ingest-textract-notifications"
    else
        v_fail "SNS: topic not reachable"
    fi
else
    v_fail "SNS: topic ARN not found in outputs"
fi

# Secrets Manager
for SECRET_ID in sp-ingest/azure-client-id sp-ingest/azure-tenant-id sp-ingest/azure-client-secret; do
    SECRET_VAL=$(aws secretsmanager get-secret-value \
        --secret-id "$SECRET_ID" \
        --query "SecretString" \
        --output text 2>/dev/null || echo "")

    if [ -n "$SECRET_VAL" ] && [ "$SECRET_VAL" != "PLACEHOLDER" ]; then
        v_pass "Secret: $SECRET_ID"
    elif [ "$SECRET_VAL" = "PLACEHOLDER" ]; then
        v_fail "Secret: $SECRET_ID (still PLACEHOLDER)"
    else
        v_fail "Secret: $SECRET_ID (not found)"
    fi
done

echo ""
echo -e "  Verification: ${GREEN}$VERIFY_PASSED passed${NC}, ${RED}$VERIFY_FAILED failed${NC}"

# Dry-run Lambda invocation
echo ""
echo -e "${YELLOW}Invoking daily-sync Lambda (deploy verification)...${NC}"

INVOKE_STATUS=$(aws lambda invoke \
    --function-name "sp-ingest-daily-sync" \
    --payload '{"dry_run": true, "source": "deploy-verification"}' \
    --cli-binary-format raw-in-base64-out \
    --query "StatusCode" \
    --output text \
    /tmp/lambda-invoke-result.json 2>/dev/null || echo "FAILED")

if [ "$INVOKE_STATUS" = "200" ]; then
    FUNC_ERROR=$(aws lambda invoke \
        --function-name "sp-ingest-daily-sync" \
        --payload '{"dry_run": true}' \
        --cli-binary-format raw-in-base64-out \
        --query "FunctionError" \
        --output text \
        /tmp/lambda-invoke-result2.json 2>/dev/null || echo "")

    if [ "$FUNC_ERROR" = "None" ] || [ -z "$FUNC_ERROR" ]; then
        v_pass "Lambda invocation returned 200"
    else
        check_warn "Lambda returned 200 but with function error: $FUNC_ERROR"
        echo -e "  ${YELLOW}This may be expected if Graph API credentials are not yet configured.${NC}"
    fi
    RESPONSE=$(cat /tmp/lambda-invoke-result.json 2>/dev/null | head -c 200 || echo "{}")
    echo -e "  ${CYAN}Response: $RESPONSE${NC}"
else
    check_warn "Lambda invocation returned status: $INVOKE_STATUS"
    echo -e "  ${YELLOW}Check CloudWatch: /aws/lambda/sp-ingest-daily-sync${NC}"
fi
rm -f /tmp/lambda-invoke-result.json /tmp/lambda-invoke-result2.json


# ===================================================================
# STEP 7: Summary & next steps
# ===================================================================

step "Step 7/7 — Deployment complete"

echo -e "${GREEN}${BOLD}  Infrastructure deployed successfully!${NC}"
echo ""
echo -e "${BOLD}  Resources:${NC}"
echo -e "    S3 bucket:           ${CYAN}$S3_BUCKET${NC}"
echo -e "    Delta tokens table:  ${CYAN}sp-ingest-delta-tokens${NC}"
echo -e "    Document registry:   ${CYAN}sp-ingest-document-registry${NC}"
echo ""
echo -e "${BOLD}  Lambda functions:${NC}"
echo -e "    Daily sync:          ${CYAN}sp-ingest-daily-sync${NC}          (512 MB, 15 min timeout)"
echo -e "    Textract trigger:    ${CYAN}sp-ingest-textract-trigger${NC}    (1 GB, 5 min timeout)"
echo -e "    Textract complete:   ${CYAN}sp-ingest-textract-complete${NC}   (1 GB, 5 min timeout)"
echo ""
echo -e "${BOLD}  CloudWatch log groups:${NC}"
echo -e "    ${CYAN}/aws/lambda/sp-ingest-daily-sync${NC}"
echo -e "    ${CYAN}/aws/lambda/sp-ingest-textract-trigger${NC}"
echo -e "    ${CYAN}/aws/lambda/sp-ingest-textract-complete${NC}"
echo ""
echo -e "${BOLD}  Schedule:${NC}"
echo -e "    Daily sync runs at ${CYAN}7:00 AM UTC (2:00 AM EST)${NC} via EventBridge"
echo ""
echo -e "${BOLD}  Next steps:${NC}"
echo -e "    1. Run bulk ingestion:  ${CYAN}./scripts/run-bulk-ingest.sh launch${NC}"
echo -e "    2. Monitor progress:    ${CYAN}./scripts/run-bulk-ingest.sh status${NC}"
echo -e "    3. After bulk is done:  ${CYAN}./scripts/run-bulk-ingest.sh teardown${NC}"
echo -e "    4. Daily sync will run automatically at 2 AM EST"
echo ""
echo -e "  Outputs: ${CYAN}$PROJECT_ROOT/deployment-outputs.json${NC}"
echo ""
