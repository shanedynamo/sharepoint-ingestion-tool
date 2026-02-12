#!/usr/bin/env bash
# ===================================================================
# build-lambda.sh — Build Lambda deployment artifacts
#
# Outputs:
#   dist/lambda-layer.zip  — shared Python dependencies
#   dist/lambda-code.zip   — application source code
# ===================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$PROJECT_ROOT/dist"
BUILD_DIR="$PROJECT_ROOT/.build"
PYTHON_VERSION="python3.11"

# Locate pip — prefer project venv, fall back to system
if [ -f "$PROJECT_ROOT/.venv/bin/pip" ]; then
    PIP="$PROJECT_ROOT/.venv/bin/pip"
elif command -v pip3 &>/dev/null; then
    PIP="pip3"
elif command -v pip &>/dev/null; then
    PIP="pip"
else
    echo "ERROR: pip not found. Activate a virtualenv or install pip."
    exit 1
fi

echo "============================================"
echo "  Building Lambda deployment artifacts"
echo "============================================"
echo "  Using pip: $PIP"
echo ""

# -------------------------------------------------------------------
# Clean previous builds
# -------------------------------------------------------------------
echo "[1/5] Cleaning previous builds..."
rm -rf "$BUILD_DIR"
mkdir -p "$DIST_DIR" "$BUILD_DIR/layer" "$BUILD_DIR/code"

# -------------------------------------------------------------------
# Build the Lambda layer (shared dependencies)
# -------------------------------------------------------------------
echo "[2/5] Installing layer dependencies..."

LAYER_PKG_DIR="$BUILD_DIR/layer/python"
mkdir -p "$LAYER_PKG_DIR"

$PIP install \
    --target "$LAYER_PKG_DIR" \
    --python-version 3.11 \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --only-binary=:all: \
    --upgrade \
    msal \
    requests \
    python-pptx \
    openpyxl \
    python-dotenv \
    2>&1 | tail -5

# Remove unnecessary files to reduce layer size
echo "[3/5] Packaging Lambda layer..."
find "$BUILD_DIR/layer" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR/layer" -type d -name "*.dist-info" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR/layer" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR/layer" -type f -name "*.pyc" -delete 2>/dev/null || true

cd "$BUILD_DIR/layer"
zip -r -q "$DIST_DIR/lambda-layer.zip" python/

LAYER_SIZE=$(du -sh "$DIST_DIR/lambda-layer.zip" | cut -f1)
echo "  Layer archive: $LAYER_SIZE"

# -------------------------------------------------------------------
# Build the function code package
# -------------------------------------------------------------------
echo "[4/5] Packaging Lambda function code..."

# Copy src/ as a Python package
cp -r "$PROJECT_ROOT/src" "$BUILD_DIR/code/src"

# Remove __pycache__ and .pyc files
find "$BUILD_DIR/code" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR/code" -type f -name "*.pyc" -delete 2>/dev/null || true

cd "$BUILD_DIR/code"
zip -r -q "$DIST_DIR/lambda-code.zip" src/

CODE_SIZE=$(du -sh "$DIST_DIR/lambda-code.zip" | cut -f1)
echo "  Code archive: $CODE_SIZE"

# -------------------------------------------------------------------
# Cleanup and summary
# -------------------------------------------------------------------
echo "[5/5] Cleaning up build directory..."
rm -rf "$BUILD_DIR"

echo ""
echo "============================================"
echo "  Build complete!"
echo "============================================"
echo "  dist/lambda-layer.zip  ($LAYER_SIZE)"
echo "  dist/lambda-code.zip   ($CODE_SIZE)"
echo ""
echo "Next steps:"
echo "  cd terraform && terraform plan"
echo "  cd terraform && terraform apply"
echo ""
