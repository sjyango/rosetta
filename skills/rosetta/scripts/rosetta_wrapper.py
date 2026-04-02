#!/usr/bin/env python3
"""
Rosetta Wrapper Script

A convenience wrapper for running rosetta commands with common workflows.

Features:
- Automatic rosetta path detection
- Configuration file management
- Common workflow shortcuts
- Error handling and user-friendly messages

Usage:
    python rosetta_wrapper.py <command> [options]
    python rosetta_wrapper.py --setup-config
    python rosetta_wrapper.py --check-connection
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple


# Constants
INSTALL_DIR = Path.home() / ".rosetta"
BIN_DIR = INSTALL_DIR / "bin"
ROSETTA_PYZ = BIN_DIR / "rosetta.pyz"
CONFIG_FILE = Path.cwd() / "dbms_config.json"


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
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


def find_rosetta() -> Optional[Path]:
    """Find rosetta executable."""
    # Check .pyz installation
    if ROSETTA_PYZ.exists():
        return ROSETTA_PYZ
    
    # Check if installed via pip
    rosetta_in_path = shutil.which("rosetta")
    if rosetta_in_path:
        return Path(rosetta_in_path)
    
    return None


def run_rosetta(args: List[str], capture_output: bool = False) -> Tuple[int, str, str]:
    """Run rosetta command with given arguments."""
    rosetta_path = find_rosetta()
    
    if not rosetta_path:
        print_error("Rosetta is not installed!")
        print_info("Please run: python install_rosetta.py")
        return 1, "", "Rosetta not installed"
    
    # Determine how to invoke rosetta
    if rosetta_path.suffix == ".pyz":
        cmd = ["python3", str(rosetta_path)] + args
    else:
        cmd = [str(rosetta_path)] + args
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
        )
        return result.returncode, result.stdout or "", result.stderr or ""
    except Exception as e:
        return 1, "", str(e)


def setup_config(output_path: Optional[Path] = None) -> bool:
    """Generate sample configuration file."""
    if output_path is None:
        output_path = CONFIG_FILE
    
    print_info(f"Generating configuration file: {output_path}")
    
    exit_code, stdout, stderr = run_rosetta(
        ["config", "init", "--output", str(output_path)],
        capture_output=True,
    )
    
    if exit_code == 0:
        print_success(f"Configuration file created: {output_path}")
        print_info("\nNext steps:")
        print_info(f"  1. Edit {output_path} with your database credentials")
        print_info("  2. Run: python rosetta_wrapper.py --check-connection")
        return True
    else:
        print_error(f"Failed to create configuration: {stderr}")
        return False


def check_connection(config_path: Optional[Path] = None) -> bool:
    """Check database connections."""
    args = ["status"]
    if config_path:
        args.extend(["--config", str(config_path)])
    
    print_info("Checking database connections...")
    exit_code = run_rosetta(args)[0]
    
    if exit_code == 0:
        print_success("All database connections are working")
        return True
    else:
        print_warning("Some database connections failed. Please check your configuration.")
        return False


def validate_config(config_path: Optional[Path] = None) -> bool:
    """Validate configuration file."""
    if config_path is None:
        config_path = CONFIG_FILE
    
    if not config_path.exists():
        print_error(f"Configuration file not found: {config_path}")
        print_info("Run: python rosetta_wrapper.py --setup-config")
        return False
    
    print_info(f"Validating configuration: {config_path}")
    exit_code, stdout, stderr = run_rosetta(
        ["config", "validate", "--config", str(config_path)],
        capture_output=True,
    )
    
    if exit_code == 0:
        print_success("Configuration is valid")
        return True
    else:
        print_error(f"Configuration validation failed: {stderr}")
        return False


def show_config(config_path: Optional[Path] = None) -> bool:
    """Show current configuration."""
    args = ["config", "show"]
    if config_path:
        args.extend(["--config", str(config_path)])
    
    exit_code = run_rosetta(args)[0]
    return exit_code == 0


def run_mtr_test(test_file: str, dbms: str, config_path: Optional[Path] = None,
                 baseline: str = "tdsql", database: str = "rosetta_mtr_test",
                 serve: bool = False) -> bool:
    """Run MTR consistency test."""
    args = [
        "mtr",
        "--dbms", dbms,
        "-t", test_file,
        "--baseline", baseline,
        "--database", database,
    ]
    
    if config_path:
        args.extend(["--config", str(config_path)])
    
    if serve:
        args.append("--serve")
    
    print_info(f"Running MTR test: {test_file}")
    print_info(f"Target DBMS: {dbms}")
    exit_code = run_rosetta(args)[0]
    
    if exit_code == 0:
        print_success("MTR test completed successfully")
        if serve:
            print_info("HTML report is being served. Press Ctrl+C to stop.")
        return True
    else:
        print_error("MTR test failed")
        return False


def run_benchmark(bench_file: str, dbms: str, config_path: Optional[Path] = None,
                  mode: str = "SERIAL", iterations: int = 1) -> bool:
    """Run performance benchmark."""
    args = [
        "bench",
        "--dbms", dbms,
        "--file", bench_file,
        "--mode", mode,
        "--iterations", str(iterations),
    ]
    
    if config_path:
        args.extend(["--config", str(config_path)])
    
    print_info(f"Running benchmark: {bench_file}")
    print_info(f"Target DBMS: {dbms}, Mode: {mode}")
    exit_code = run_rosetta(args)[0]
    
    if exit_code == 0:
        print_success("Benchmark completed successfully")
        return True
    else:
        print_error("Benchmark failed")
        return False


def exec_sql(sql: str, dbms: str, config_path: Optional[Path] = None,
             database: Optional[str] = None) -> bool:
    """Execute SQL statement."""
    args = [
        "exec",
        "--dbms", dbms,
        "--sql", sql,
    ]
    
    if config_path:
        args.extend(["--config", str(config_path)])
    
    if database:
        args.extend(["--database", database])
    
    print_info(f"Executing SQL on {dbms}: {sql[:50]}...")
    exit_code = run_rosetta(args)[0]
    
    return exit_code == 0


def list_results(result_type: str = "all", limit: int = 20) -> bool:
    """List historical results."""
    args = ["result", "list", "--type", result_type, "-n", str(limit)]
    exit_code = run_rosetta(args)[0]
    return exit_code == 0


def show_result(run_id: Optional[str] = None) -> bool:
    """Show result details."""
    args = ["result", "show"]
    if run_id:
        args.append(run_id)
    exit_code = run_rosetta(args)[0]
    return exit_code == 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Rosetta wrapper script for common workflows",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Setup configuration
  python rosetta_wrapper.py --setup-config
  
  # Check connections
  python rosetta_wrapper.py --check-connection
  
  # Run MTR test
  python rosetta_wrapper.py mtr -t test.test --dbms mysql,tdsql
  
  # Run benchmark
  python rosetta_wrapper.py bench --file bench.json --dbms mysql,tdsql
  
  # Execute SQL
  python rosetta_wrapper.py exec --sql "SELECT VERSION()" --dbms mysql
  
  # List results
  python rosetta_wrapper.py result list --type mtr
  
For more rosetta commands, run:
  python rosetta_wrapper.py --help-raw
        """,
    )
    
    # Global options
    parser.add_argument(
        "--config", "-c",
        help="Configuration file path",
    )
    
    # Convenience commands
    parser.add_argument(
        "--setup-config",
        action="store_true",
        help="Generate sample configuration file",
    )
    parser.add_argument(
        "--check-connection",
        action="store_true",
        help="Check database connections",
    )
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate configuration file",
    )
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="Show current configuration",
    )
    parser.add_argument(
        "--help-raw",
        action="store_true",
        help="Show rosetta's native help",
    )
    
    # Parse known args to allow passthrough
    args, remaining = parser.parse_known_args()
    
    # Handle convenience commands
    if args.setup_config:
        config_path = Path(args.config) if args.config else None
        return 0 if setup_config(config_path) else 1
    
    if args.check_connection:
        config_path = Path(args.config) if args.config else None
        return 0 if check_connection(config_path) else 1
    
    if args.validate_config:
        config_path = Path(args.config) if args.config else None
        return 0 if validate_config(config_path) else 1
    
    if args.show_config:
        config_path = Path(args.config) if args.config else None
        return 0 if show_config(config_path) else 1
    
    if args.help_raw:
        return run_rosetta(["--help"])[0]
    
    # Passthrough to rosetta
    if not remaining:
        parser.print_help()
        return 0
    
    # Add config if specified
    rosetta_args = remaining
    if args.config:
        rosetta_args = ["--config", args.config] + rosetta_args
    
    # Run rosetta command
    return run_rosetta(rosetta_args)[0]


if __name__ == "__main__":
    sys.exit(main())
