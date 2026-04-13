#!/bin/bash
#
# Rosetta Uninstaller
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/sjyango/rosetta/main/uninstall.sh | bash
#     && source ~/.zshrc
#
# Or use eval to auto clean PATH in current shell:
#   eval "$(curl -fsSL https://raw.githubusercontent.com/sjyango/rosetta/main/uninstall.sh)"
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
        -y|--yes)
            SKIP_CONFIRM=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -d, --dir DIR   Installation directory (default: ~/.rosetta)"
            echo "  -y, --yes       Skip confirmation prompt"
            echo "  -h, --help      Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}"
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    Rosetta Uninstaller                        ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Check if installed
if [[ ! -d "$INSTALL_DIR" ]]; then
    echo -e "${YELLOW}Rosetta is not installed (directory not found: $INSTALL_DIR)${NC}"
    exit 0
fi

# Confirmation
if [[ "$SKIP_CONFIRM" != true ]]; then
    echo -e "${YELLOW}This will remove Rosetta and all its data from:$NC"
    echo -e "  ${BLUE}$INSTALL_DIR${NC}"
    echo ""
    read -p "Are you sure? [y/N] " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Uninstall cancelled.${NC}"
        exit 0
    fi
fi

VENV_DIR="$INSTALL_DIR/.venv"
ROSETTA_BIN="$VENV_DIR/bin"

# Detect shell rc file (same logic as install.sh)
SHELL_RC=""
if [[ -n "$ZSH_VERSION" ]] || grep -q zsh /proc/$PPID/cmdline 2>/dev/null || [[ "$(getent passwd $USER 2>/dev/null | cut -d: -f7)" == *zsh* ]]; then
    SHELL_RC="$HOME/.zshrc"
elif [[ -n "$BASH_VERSION" ]] || [[ "$(getent passwd $USER 2>/dev/null | cut -d: -f7)" == *bash* ]]; then
    SHELL_RC="$HOME/.bashrc"
elif [[ -f "$HOME/.zshrc" ]]; then
    SHELL_RC="$HOME/.zshrc"
elif [[ -f "$HOME/.bashrc" ]]; then
    SHELL_RC="$HOME/.bashrc"
else
    SHELL_RC="$HOME/.profile"
fi

# Display-friendly rc path (use ~ instead of $HOME)
SHELL_RC_DISPLAY="${SHELL_RC/#$HOME\//~/}"

# Remove PATH entries from all shell rc files
SHELL_RCS=("$HOME/.zshrc" "$HOME/.bashrc" "$HOME/.bash_profile" "$HOME/.profile")

for rc in "${SHELL_RCS[@]}"; do
    if [[ -f "$rc" ]] && grep -q "\.rosetta/\.venv/bin" "$rc"; then
        # Remove the two lines added by installer:
        #   # Added by Rosetta installer
        #   export PATH="<rosetta_venv_bin>:$PATH"
        if [[ "$OSTYPE" == "darwin"* ]]; then
            sed -i '' '/# Added by Rosetta installer/d' "$rc"
            sed -i '' "/\.rosetta\/\.venv\/bin/d" "$rc"
        else
            sed -i '/# Added by Rosetta installer/d' "$rc"
            sed -i "/\.rosetta\/\.venv\/bin/d" "$rc"
        fi
        echo -e "${GREEN}✓ Cleaned up $rc${NC}"
    fi
done

# Remove installation directory
rm -rf "$INSTALL_DIR"
echo -e "${GREEN}✓ Removed $INSTALL_DIR${NC}"

# Success message
echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              ✓ Rosetta uninstalled successfully               ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Handle subshell vs direct execution (same pattern as install.sh)
if [[ "$BASH_SOURCE" != "$0" || -p /dev/stdin ]]; then
    echo -e "${YELLOW}To update PATH in the current shell, run:${NC}"
    echo -e "  ${BLUE}source $SHELL_RC_DISPLAY${NC}"
    echo ""
    # Output PATH cleanup for eval mode
    echo "export PATH=\"\$(echo \"\$PATH\" | tr ':' '\n' | grep -v '\.rosetta/\.venv/bin' | tr '\n' ':')\""
else
    # Running directly, source the rc file
    source "$SHELL_RC" 2>/dev/null || true
    if ! command -v rosetta &> /dev/null; then
        echo -e "${GREEN}✓ rosetta command removed from PATH${NC}"
    else
        echo -e "${YELLOW}Run the following to update PATH:${NC}"
        echo -e "  ${BLUE}source $SHELL_RC_DISPLAY${NC}"
    fi
fi

echo ""
