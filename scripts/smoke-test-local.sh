#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Smoke test: validates the complete pipeline locally before AWS deployment.
#
# Prerequisites:
#   1. Docker running with LocalStack:
#        docker compose -f docker/docker-compose.local.yml up -d
#   2. .env file with Azure AD credentials filled in
#
# Usage:
#   ./scripts/smoke-test-local.sh              # normal run
#   SMOKE_VERBOSE=1 ./scripts/smoke-test-local.sh   # full tracebacks
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

echo "========================================"
echo "  Smoke Test: Pre-Deployment Validation"
echo "========================================"
echo ""

# ------------------------------------------------------------------
# Prerequisite checks
# ------------------------------------------------------------------
echo "Checking prerequisites..."

# 1. Docker running
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}ERROR: Docker is not running.${NC}"
    echo "  Start Docker Desktop, then:"
    echo "  docker compose -f docker/docker-compose.local.yml up -d"
    exit 1
fi
echo "  [OK] Docker is running"

# 2. LocalStack healthy
if ! curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; then
    echo -e "${RED}ERROR: LocalStack is not reachable at localhost:4566.${NC}"
    echo "  Start it with:"
    echo "  docker compose -f docker/docker-compose.local.yml up -d"
    exit 1
fi
echo "  [OK] LocalStack is healthy"

# 3. .env file with real credentials
if [ ! -f .env ]; then
    echo -e "${RED}ERROR: .env file not found.${NC}"
    echo "  Copy .env.example to .env and fill in Azure AD credentials."
    exit 1
fi

if grep -q '<your-client-id>' .env 2>/dev/null; then
    echo -e "${RED}ERROR: .env contains placeholder values.${NC}"
    echo "  Fill in real Azure AD credentials in .env"
    exit 1
fi
echo "  [OK] .env file found with credentials"

# 4. Python virtual environment
if [ ! -f .venv/bin/python ]; then
    echo -e "${RED}ERROR: Virtual environment not found at .venv/${NC}"
    echo "  Create it with: python3.11 -m venv .venv && .venv/bin/pip install -e '.[dev]'"
    exit 1
fi
echo "  [OK] Python virtual environment found"

# 5. Verify LocalStack resources are initialised
S3_CHECK=$(aws --endpoint-url=http://localhost:4566 --region us-east-1 \
    s3 ls s3://dynamo-ai-documents 2>&1) || true
if echo "$S3_CHECK" | grep -q "NoSuchBucket\|does not exist"; then
    echo -e "${YELLOW}LocalStack resources not initialised. Running init script...${NC}"
    bash scripts/init-localstack.sh
fi
echo "  [OK] LocalStack resources initialised"

echo ""

# ------------------------------------------------------------------
# Export LocalStack AWS configuration
# ------------------------------------------------------------------
export AWS_ENDPOINT_URL="http://localhost:4566"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_DEFAULT_REGION="us-east-1"

# ------------------------------------------------------------------
# Run the smoke test suite
# ------------------------------------------------------------------
exec .venv/bin/python scripts/smoke_test.py
