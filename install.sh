#!/bin/bash
#
# Rosetta Installer
# Cross-DBMS SQL behavioral consistency verification tool
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/sjyango/rosetta/main/install.sh | bash
#     && source ~/.zshrc
#
# Or use eval to auto source:
#   eval "$(curl -fsSL https://raw.githubusercontent.com/sjyango/rosetta/main/install.sh)"
#
# Note: piping to bash runs in a subshell, so PATH changes cannot affect the
# parent shell. Use the "eval" form or manually `source ~/.zshrc` after install.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default settings
REPO_URL="https://github.com/sjyango/rosetta.git"
INSTALL_DIR="${HOME}/.rosetta"
BRANCH="release-1.0.1"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        -b|--branch)
            BRANCH="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -d, --dir DIR      Installation directory (default: ~/.rosetta)"
            echo "  -b, --branch BRANCH Git branch to install (default: release-1.0.1)"
            echo "  -h, --help         Show this help message"
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
echo "║                    Rosetta Installer                          ║"
echo "║        Cross-DBMS SQL Behavioral Consistency Verification     ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Check Python version
echo -e "${YELLOW}Checking Python version...${NC}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed.${NC}"
    echo "Please install Python 3.8 or higher and try again."
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
REQUIRED_VERSION="3.8"

if [[ $(echo -e "$PYTHON_VERSION\n$REQUIRED_VERSION" | sort -V | head -n1) != "$REQUIRED_VERSION" ]]; then
    echo -e "${RED}Error: Python $REQUIRED_VERSION or higher is required (found $PYTHON_VERSION).${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Python $PYTHON_VERSION detected${NC}"

# Check pip
echo -e "${YELLOW}Checking pip...${NC}"
if ! command -v pip3 &> /dev/null && ! python3 -m pip --version &> /dev/null; then
    echo -e "${RED}Error: pip is not installed.${NC}"
    echo "Please install pip and try again."
    exit 1
fi

echo -e "${GREEN}✓ pip is available${NC}"

# Check git
echo -e "${YELLOW}Checking git...${NC}"
if ! command -v git &> /dev/null; then
    echo -e "${RED}Error: git is not installed.${NC}"
    echo "Please install git and try again."
    exit 1
fi

echo -e "${GREEN}✓ git is available${NC}"

# Check python3-venv
echo -e "${YELLOW}Checking python3 venv module...${NC}"
if ! python3 -c "import venv" &> /dev/null; then
    echo -e "${RED}Error: python3-venv module is not available.${NC}"
    echo "Please install it (e.g., sudo apt install python3-venv) and try again."
    exit 1
fi
echo -e "${GREEN}✓ venv module is available${NC}"

# Create installation directory
echo -e "${YELLOW}Installing to $INSTALL_DIR...${NC}"
if [[ -d "$INSTALL_DIR" ]]; then
    echo -e "${YELLOW}Installation directory exists. Updating...${NC}"
    cd "$INSTALL_DIR"
    git fetch origin
    git checkout "$BRANCH"
    LOCAL_CHANGES=$(git status --porcelain)
    if [[ -n "$LOCAL_CHANGES" ]]; then
        echo -e "${YELLOW}Local changes detected, stashing before update...${NC}"
        git stash
        git reset --hard "origin/$BRANCH"
        git stash pop 2>/dev/null || true
    else
        git reset --hard "origin/$BRANCH"
    fi
else
    git clone -b "$BRANCH" "$REPO_URL" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
fi

echo -e "${GREEN}✓ Source code ready${NC}"

# Create virtual environment
VENV_DIR="$INSTALL_DIR/.venv"
echo -e "${YELLOW}Creating virtual environment...${NC}"
if [[ ! -d "$VENV_DIR" ]]; then
    python3 -m venv "$VENV_DIR"
fi

echo -e "${GREEN}✓ Virtual environment created${NC}"

# Activate and install
echo -e "${YELLOW}Installing dependencies...${NC}"
source "$VENV_DIR/bin/activate"
pip install --upgrade pip --quiet
pip install -e "$INSTALL_DIR" --quiet

echo -e "${GREEN}✓ Dependencies installed${NC}"

# Add to PATH
ROSETTA_BIN="$VENV_DIR/bin"
SHELL_RC=""
PATH_EXPORT="export PATH=\"$ROSETTA_BIN:\$PATH\""

# Detect shell and set rc file
# Check user's default login shell first (works even when script runs via `curl | bash`)
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

# Check if already in PATH
if [[ ":$PATH:" != *":$ROSETTA_BIN:"* ]]; then
    echo -e "${YELLOW}Adding rosetta to PATH...${NC}"
    
    # Check if already in shell rc
    if [[ -f "$SHELL_RC" ]] && grep -q "rosetta" "$SHELL_RC"; then
        echo -e "${GREEN}✓ Already configured in $SHELL_RC${NC}"
    else
        echo "" >> "$SHELL_RC"
        echo "# Added by Rosetta installer" >> "$SHELL_RC"
        echo "$PATH_EXPORT" >> "$SHELL_RC"
        echo -e "${GREEN}✓ Added to $SHELL_RC${NC}"
    fi
    
    # Add to current session
    export PATH="$ROSETTA_BIN:$PATH"
fi

# Create sample config if not exists
SAMPLE_CONFIG="$INSTALL_DIR/dbms_config.json"
if [[ ! -f "$SAMPLE_CONFIG" ]]; then
    cp "$INSTALL_DIR/dbms_config.sample.json" "$SAMPLE_CONFIG"
    echo -e "${GREEN}✓ Created sample dbms_config.json${NC}"
fi

# Success message
echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              ✓ Rosetta installed successfully!                ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "Installation directory: ${BLUE}$INSTALL_DIR${NC}"
echo -e "Version: ${BLUE}$(python3 -c "import sys; sys.path.insert(0, '$INSTALL_DIR'); from rosetta import __version__; print(__version__)" 2>/dev/null || echo "unknown")${NC}"
echo ""

# Display-friendly rc path (use ~ instead of $HOME)
SHELL_RC_DISPLAY="${SHELL_RC/#$HOME\//~/}"

# Check if running in a subshell (piped via curl | bash)
# In a subshell we cannot modify the parent's PATH, so output the export
# command so that `eval "$(curl ... | bash)"` works.
if [[ "$BASH_SOURCE" != "$0" || -p /dev/stdin ]]; then
    echo -e "${YELLOW}To activate rosetta in the current shell, run:${NC}"
    echo -e "  ${BLUE}source $SHELL_RC_DISPLAY${NC}"
    echo ""
    # Also output the export for eval mode
    echo "export PATH=\"$ROSETTA_BIN:\$PATH\""
else
    # Running directly, source the rc file to activate in current shell
    source "$SHELL_RC" 2>/dev/null || true
    if command -v rosetta &> /dev/null; then
        echo -e "${GREEN}✓ rosetta command is ready${NC}"
    else
        echo -e "${YELLOW}Run the following to activate rosetta:${NC}"
        echo -e "  ${BLUE}source $SHELL_RC_DISPLAY${NC}"
    fi
fi

echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo -e "  1. ${BLUE}rosetta config init${NC}  (generate sample dbms_config.json)"
echo -e "  2. ${BLUE}rosetta --help${NC}  (show available commands)"
echo ""
echo -e "${YELLOW}Quick start:${NC}"
echo -e "  ${BLUE}rosetta${NC}                # Launch interactive mode"
echo -e "  ${BLUE}rosetta status${NC}         # Check DBMS connections"
echo -e "  ${BLUE}rosetta bench --help${NC}   # Run benchmarks"
echo ""
echo -e "${YELLOW}Documentation:${NC}"
echo -e "  ${BLUE}https://github.com/sjyango/rosetta#readme${NC}"
echo ""
echo -e "${YELLOW}To uninstall:${NC}"
echo -e "  ${BLUE}eval \"\$(curl -fsSL https://raw.githubusercontent.com/sjyango/rosetta/main/uninstall.sh)\"${NC}"
echo ""
