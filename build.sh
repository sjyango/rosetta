#!/usr/bin/env bash
#
# Rosetta — Build Script
# 将 rosetta 打包为单个 .pyz 文件 (zipapp)。
#
# 使用方法:
#   chmod +x build.sh
#   ./build.sh
#
# 产物:
#   dist/rosetta.pyz
#
# 运行方式:
#   python3 dist/rosetta.pyz --help
#   # 或直接执行 (Linux/macOS):
#   ./dist/rosetta.pyz --help
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Rosetta Build (zipapp) ==="
echo ""

# Check Python
if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 is required"
    exit 1
fi

# Prepare build directory
BUILD_DIR=$(mktemp -d)
trap "rm -rf $BUILD_DIR" EXIT

echo "[1/3] Preparing source files..."
cp -r rosetta "$BUILD_DIR/rosetta"

# Remove .pyc / __pycache__
find "$BUILD_DIR" -name '*.pyc' -delete
find "$BUILD_DIR" -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true

# Create top-level __main__.py with absolute import
cat > "$BUILD_DIR/__main__.py" << 'EOF'
import sys
from rosetta.cli import main
sys.exit(main())
EOF

# Build
echo "[2/3] Building rosetta.pyz..."
mkdir -p dist
python3 -m zipapp "$BUILD_DIR" -p "/usr/bin/env python3" -o dist/rosetta.pyz

# Verify
echo "[3/3] Verifying..."
python3 dist/rosetta.pyz --help >/dev/null 2>&1

echo ""
SIZE=$(du -sh dist/rosetta.pyz | cut -f1)
echo "✓ Build successful!"
echo "  Output: dist/rosetta.pyz ($SIZE)"
echo ""
echo "  运行方式:"
echo "    python3 dist/rosetta.pyz --help"
echo "    ./dist/rosetta.pyz --help"
echo ""
echo "  分发给他人时，对方需要:"
echo "    1. Python 3.8+"
echo "    2. pip install pymysql rich"
