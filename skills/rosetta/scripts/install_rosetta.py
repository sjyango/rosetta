#!/usr/bin/env python3
"""
Rosetta Installation Script

Automated installation script for rosetta cross-DBMS SQL testing tool.

Features:
- Smart detection of GITHUB_TOKEN/GH_TOKEN
- Download from GitHub Release (preferred)
- Fallback to source installation via git clone
- Automatic dependency management
- SHA256 checksum verification
- Idempotent installation

Usage:
    python install_rosetta.py [--version VERSION] [--force]
"""

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional, Tuple


# Constants
GITHUB_REPO = "sjyango/rosetta"
GITHUB_API_BASE = "https://api.github.com"
GITHUB_RELEASES_URL = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO}/releases"
DEFAULT_VERSION = "v1.0.0"

INSTALL_DIR = Path.home() / ".rosetta"
BIN_DIR = INSTALL_DIR / "bin"
CACHE_DIR = INSTALL_DIR / "cache"

REQUIRED_PYTHON_VERSION = (3, 8)
REQUIRED_PACKAGES = [
    "pymysql>=1.0",
    "rich>=13.0",
    "prompt_toolkit>=3.0",
]


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def print_info(msg: str) -> None:
    """Print info message."""
    print(f"{Colors.BLUE}ℹ{Colors.RESET} {msg}")


def print_success(msg: str) -> None:
    """Print success message."""
    print(f"{Colors.GREEN}✓{Colors.RESET} {msg}")


def print_warning(msg: str) -> None:
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠{Colors.RESET} {msg}")


def print_error(msg: str) -> None:
    """Print error message."""
    print(f"{Colors.RED}✗{Colors.RESET} {msg}", file=sys.stderr)


def print_step(step: str, msg: str) -> None:
    """Print step message."""
    print(f"{Colors.BOLD}[{step}]{Colors.RESET} {msg}")


def check_python_version() -> bool:
    """Check if Python version meets requirements."""
    if sys.version_info < REQUIRED_PYTHON_VERSION:
        print_error(
            f"Python {REQUIRED_PYTHON_VERSION[0]}.{REQUIRED_PYTHON_VERSION[1]}+ is required. "
            f"Current version: {sys.version_info.major}.{sys.version_info.minor}"
        )
        return False
    return True


def get_github_token() -> Optional[str]:
    """Get GitHub token from environment variables."""
    return os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")


def run_command(cmd: list, capture_output: bool = False) -> Tuple[int, str, str]:
    """Run a command and return exit code, stdout, stderr."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            timeout=300,  # 5 minutes timeout
        )
        return result.returncode, result.stdout or "", result.stderr or ""
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"
    except FileNotFoundError:
        return -1, "", f"Command not found: {cmd[0]}"
    except Exception as e:
        return -1, "", str(e)


def check_rosetta_installed() -> Tuple[bool, Optional[str]]:
    """Check if rosetta is already installed."""
    rosetta_path = BIN_DIR / "rosetta.pyz"
    if rosetta_path.exists():
        # Check if it's executable
        exit_code, stdout, _ = run_command(
            ["python3", str(rosetta_path), "--version"],
            capture_output=True,
        )
        if exit_code == 0:
            return True, stdout.strip()
    return False, None


def download_file(url: str, output_path: Path, headers: dict = None) -> bool:
    """Download a file from URL."""
    try:
        request = urllib.request.Request(url)
        if headers:
            for key, value in headers.items():
                request.add_header(key, value)
        
        with urllib.request.urlopen(request, timeout=30) as response:
            with open(output_path, "wb") as f:
                shutil.copyfileobj(response, f)
        return True
    except urllib.error.HTTPError as e:
        if e.code == 403:
            print_warning("GitHub API rate limit reached. Consider setting GITHUB_TOKEN.")
        print_error(f"Failed to download {url}: {e}")
        return False
    except urllib.error.URLError as e:
        print_error(f"Network error: {e}")
        return False
    except Exception as e:
        print_error(f"Download failed: {e}")
        return False


def get_release_info(version: str, token: Optional[str]) -> Optional[dict]:
    """Get release information from GitHub API."""
    url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPO}/releases/tags/{version}"
    headers = {"Accept": "application/vnd.github.v3+json"}
    
    if token:
        headers["Authorization"] = f"token {token}"
        print_info("Using GITHUB_TOKEN for authenticated access")
    else:
        print_warning("No GITHUB_TOKEN found, using anonymous access (may hit rate limits)")
    
    try:
        request = urllib.request.Request(url)
        for key, value in headers.items():
            request.add_header(key, value)
        
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print_error(f"Release {version} not found")
        elif e.code == 403:
            print_error("GitHub API rate limit exceeded. Please set GITHUB_TOKEN environment variable.")
        else:
            print_error(f"Failed to get release info: {e}")
        return None
    except Exception as e:
        print_error(f"Failed to fetch release information: {e}")
        return None


def verify_sha256(file_path: Path, expected_sha256: str) -> bool:
    """Verify SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    actual_sha256 = sha256_hash.hexdigest()
    
    if actual_sha256 != expected_sha256:
        print_error(f"SHA256 mismatch!")
        print_error(f"  Expected: {expected_sha256}")
        print_error(f"  Actual:   {actual_sha256}")
        return False
    return True


def install_from_release(version: str, token: Optional[str]) -> bool:
    """Install rosetta from GitHub Release."""
    print_step("Release", f"Installing rosetta {version} from GitHub Release")
    
    # Get release info
    release_info = get_release_info(version, token)
    if not release_info:
        return False
    
    # Find .pyz and .sha256 assets
    pyz_asset = None
    sha256_asset = None
    
    for asset in release_info.get("assets", []):
        name = asset["name"]
        if name.endswith(".pyz"):
            pyz_asset = asset
        elif name.endswith(".sha256"):
            sha256_asset = asset
    
    if not pyz_asset:
        print_error("No .pyz file found in release")
        return False
    
    print_info(f"Found release asset: {pyz_asset['name']}")
    
    # Download .pyz file
    pyz_path = CACHE_DIR / pyz_asset["name"]
    print_info(f"Downloading {pyz_asset['name']}...")
    
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"
    
    if not download_file(pyz_asset["browser_download_url"], pyz_path, headers):
        return False
    
    print_success(f"Downloaded {pyz_asset['name']}")
    
    # Download and verify SHA256 if available
    if sha256_asset:
        sha256_path = CACHE_DIR / sha256_asset["name"]
        print_info("Downloading SHA256 checksum...")
        
        if download_file(sha256_asset["browser_download_url"], sha256_path, headers):
            expected_sha256 = sha256_path.read_text().strip().split()[0]
            print_info("Verifying SHA256 checksum...")
            
            if not verify_sha256(pyz_path, expected_sha256):
                pyz_path.unlink()
                sha256_path.unlink()
                return False
            
            print_success("SHA256 checksum verified")
        else:
            print_warning("Could not download SHA256 file, skipping verification")
    
    # Install to bin directory
    target_path = BIN_DIR / "rosetta.pyz"
    shutil.copy2(pyz_path, target_path)
    
    # Make executable on Unix-like systems
    if platform.system() != "Windows":
        os.chmod(target_path, 0o755)
    
    print_success(f"Installed to {target_path}")
    return True


def install_from_source(version: str) -> bool:
    """Install rosetta from source via git clone."""
    print_step("Source", "Installing rosetta from source")
    
    # Check git
    exit_code, _, _ = run_command(["git", "--version"], capture_output=True)
    if exit_code != 0:
        print_error("git is required for source installation")
        return False
    
    # Create temporary directory for clone
    with tempfile.TemporaryDirectory() as tmpdir:
        clone_dir = Path(tmpdir) / "rosetta"
        
        # Clone repository
        print_info("Cloning rosetta repository...")
        exit_code, _, stderr = run_command(
            ["git", "clone", "--depth", "1", "--branch", version,
             f"https://github.com/{GITHUB_REPO}.git", str(clone_dir)],
            capture_output=True,
        )
        
        if exit_code != 0:
            print_error(f"Failed to clone repository: {stderr}")
            return False
        
        print_success("Repository cloned")
        
        # Install using pip
        print_info("Installing rosetta...")
        exit_code, _, stderr = run_command(
            [sys.executable, "-m", "pip", "install", "-e", "."],
            capture_output=True,
        )
        
        if exit_code != 0:
            print_error(f"Failed to install: {stderr}")
            return False
        
        print_success("Installed from source")
        return True


def install_dependencies() -> bool:
    """Install required Python packages."""
    print_step("Dependencies", "Installing required Python packages")
    
    for package in REQUIRED_PACKAGES:
        print_info(f"Installing {package}...")
        exit_code, _, stderr = run_command(
            [sys.executable, "-m", "pip", "install", package],
            capture_output=True,
        )
        
        if exit_code != 0:
            print_error(f"Failed to install {package}: {stderr}")
            return False
    
    print_success("All dependencies installed")
    return True


def verify_installation() -> bool:
    """Verify rosetta installation."""
    print_step("Verify", "Verifying installation")
    
    rosetta_path = BIN_DIR / "rosetta.pyz"
    if not rosetta_path.exists():
        # Check if installed via pip
        exit_code, stdout, _ = run_command(
            ["rosetta", "--version"],
            capture_output=True,
        )
        if exit_code == 0:
            print_success(f"Rosetta installed: {stdout.strip()}")
            return True
        print_error("Rosetta not found after installation")
        return False
    
    # Verify .pyz
    exit_code, stdout, _ = run_command(
        ["python3", str(rosetta_path), "--version"],
        capture_output=True,
    )
    
    if exit_code == 0:
        print_success(f"Rosetta installed: {stdout.strip()}")
        return True
    
    print_error("Rosetta verification failed")
    return False


def setup_environment() -> bool:
    """Setup installation directories."""
    try:
        INSTALL_DIR.mkdir(parents=True, exist_ok=True)
        BIN_DIR.mkdir(parents=True, exist_ok=True)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        print_error(f"Failed to setup directories: {e}")
        return False


def main() -> int:
    """Main installation flow."""
    parser = argparse.ArgumentParser(
        description="Install rosetta cross-DBMS SQL testing tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version",
        default=DEFAULT_VERSION,
        help=f"Version to install (default: {DEFAULT_VERSION})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reinstall even if already installed",
    )
    parser.add_argument(
        "--source",
        action="store_true",
        help="Install from source instead of release",
    )
    
    args = parser.parse_args()
    
    print(f"{Colors.BOLD}=== Rosetta Installation Script ==={Colors.RESET}\n")
    
    # Check Python version
    if not check_python_version():
        return 1
    
    # Check if already installed
    if not args.force:
        installed, version = check_rosetta_installed()
        if installed:
            print_success(f"Rosetta already installed: {version}")
            print_info("Use --force to reinstall")
            return 0
    
    # Setup directories
    if not setup_environment():
        return 1
    
    # Install dependencies first
    if not install_dependencies():
        return 1
    
    # Install rosetta
    success = False
    
    if args.source:
        # Force source installation
        success = install_from_source(args.version)
    else:
        # Try release first, fallback to source
        token = get_github_token()
        if install_from_release(args.version, token):
            success = True
        else:
            print_warning("Release installation failed, falling back to source installation")
            if install_from_source(args.version):
                success = True
    
    if not success:
        print_error("\nInstallation failed!")
        print_info("Please check:")
        print_info("  1. Network connection is available")
        print_info("  2. Python 3.8+ is installed")
        print_info("  3. pip is available")
        print_info("  4. (Optional) GITHUB_TOKEN is set for higher API limits")
        return 1
    
    # Verify installation
    if not verify_installation():
        print_error("\nInstallation verification failed!")
        return 1
    
    print(f"\n{Colors.GREEN}{Colors.BOLD}✓ Installation successful!{Colors.RESET}")
    print_info(f"Rosetta is now available at: {BIN_DIR / 'rosetta.pyz'}")
    print_info("\nQuick start:")
    print_info("  python3 ~/.rosetta/bin/rosetta.pyz config init")
    print_info("  python3 ~/.rosetta/bin/rosetta.pyz status")
    print_info("\nFor more information:")
    print_info("  python3 ~/.rosetta/bin/rosetta.pyz --help")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
