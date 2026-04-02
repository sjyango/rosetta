#!/bin/bash
#
# Rosetta Uninstaller
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

INSTALL_DIR="${HOME}/.rosetta"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -d, --dir DIR   Installation directory (default: ~/.rosetta)"
            echo "  -h, --help      Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${YELLOW}Uninstalling Rosetta...${NC}"

# Remove installation directory
if [[ -d "$INSTALL_DIR" ]]; then
    rm -rf "$INSTALL_DIR"
    echo -e "${GREEN}✓ Removed $INSTALL_DIR${NC}"
else
    echo -e "${YELLOW}Installation directory not found: $INSTALL_DIR${NC}"
fi

# Remove PATH from shell rc
SHELL_RCS=("$HOME/.zshrc" "$HOME/.bashrc" "$HOME/.bash_profile" "$HOME/.profile")

for rc in "${SHELL_RCS[@]}"; do
    if [[ -f "$rc" ]] && grep -q "rosetta" "$rc"; then
        # Remove lines containing rosetta path export
        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' '/# Added by Rosetta installer/d' "$rc"
            sed -i '' '/rosetta\/\.venv\/bin/d' "$rc"
        else
            sed -i '/# Added by Rosetta installer/d' "$rc"
            sed -i '/rosetta\/\.venv\/bin/d' "$rc"
        fi
        echo -e "${GREEN}✓ Cleaned up $rc${NC}"
    fi
done

echo ""
echo -e "${GREEN}✓ Rosetta uninstalled successfully${NC}"
echo -e "${YELLOW}Please run 'source ~/.bashrc' (or restart your terminal) to update PATH${NC}"
echo ""
