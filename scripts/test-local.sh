#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/docker/docker-compose.local.yml"

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
cleanup() {
    echo "==> Stopping LocalStack..."
    docker compose -f "$COMPOSE_FILE" down --volumes 2>/dev/null || true
}

wait_for_localstack() {
    local max_attempts=30
    local attempt=0
    echo "==> Waiting for LocalStack to be ready..."
    while [ $attempt -lt $max_attempts ]; do
        if curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; then
            echo "    LocalStack is ready."
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    echo "ERROR: LocalStack did not become ready in time."
    return 1
}

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
cd "$PROJECT_ROOT"

# Parse flags
SKIP_UNIT=false
SKIP_INTEGRATION=false
KEEP_RUNNING=false
for arg in "$@"; do
    case $arg in
        --skip-unit) SKIP_UNIT=true ;;
        --skip-integration) SKIP_INTEGRATION=true ;;
        --keep-running) KEEP_RUNNING=true ;;
    esac
done

# Tear down on exit unless --keep-running
if [ "$KEEP_RUNNING" = false ]; then
    trap cleanup EXIT
fi

# 1. Start LocalStack
echo "==> Starting LocalStack..."
docker compose -f "$COMPOSE_FILE" up -d
wait_for_localstack

# 2. Run unit tests
if [ "$SKIP_UNIT" = false ]; then
    echo ""
    echo "=========================================="
    echo "  Unit Tests"
    echo "=========================================="
    python -m pytest tests/ -v --tb=short --ignore=tests/integration
    echo ""
fi

# 3. Run integration tests
if [ "$SKIP_INTEGRATION" = false ]; then
    echo ""
    echo "=========================================="
    echo "  Integration Tests (LocalStack)"
    echo "=========================================="
    python -m pytest tests/integration/ -v --tb=short -m integration
    echo ""
fi

echo "=========================================="
echo "  All tests passed."
echo "=========================================="
