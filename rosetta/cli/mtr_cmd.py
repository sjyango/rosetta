"""
Handler for the 'mtr' command — run native MySQL MTR test suites.

This wraps the ./mtr binary in the MySQL test directory, supporting
common options like suite selection, record mode, optimistic transactions,
vector engine, parallel query, etc.

Configuration is read from the same dbms_config.json file under the
"mtr" top-level key.  CLI flags override config values.
"""

import json
import os
import re
import subprocess
import sys
from typing import TYPE_CHECKING, List

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter


# -----------------------------------------------------------------------
# Config loading
# -----------------------------------------------------------------------

def _load_mtr_config(config_path: str) -> dict:
    """
    Load the ``mtr`` section from the shared dbms_config.json.

    Returns a dict (possibly empty) with whatever keys the user has set.
    Required keys must all be present or the handler will report an error.
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("mtr", {})
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


# -----------------------------------------------------------------------
# Command builder
# -----------------------------------------------------------------------

def _build_mysqld_opts(mysqld_opts_list: List[str]) -> str:
    """Convert a list of mysqld options to CLI flags.

    Each item can be either ``key=value`` (auto-prefixed with ``--``)
    or ``--key=value`` (used as-is).
    """
    parts = []
    for opt in mysqld_opts_list:
        if opt.startswith("--"):
            parts.append(f"--mysqld={opt}")
        else:
            parts.append(f"--mysqld=--{opt}")
    return " ".join(parts)


def _build_command(cfg: dict) -> str:
    """Build the full ./mtr command string from resolved config dict."""
    parts = ["./mtr"]
    parts.append(f"--port-base={cfg['port_base']}")
    parts.append(f"--skip-test-list={cfg['skip_list']}")
    parts.append(f"--parallel={cfg['parallel']}")
    parts.append(f"--retry={cfg['retry']}")
    parts.append(f"--retry-failure={cfg['retry_failure']}")
    parts.append(f"--max-test-fail={cfg['max_test_fail']}")
    parts.append("--force")
    parts.append("--big-test")
    parts.append("--nounit-tests")
    parts.append("--nowarnings")
    parts.append(f"--testcase-timeout={cfg['testcase_timeout']}")
    parts.append(f"--suite-timeout={cfg['suite_timeout']}")
    parts.append("--report-unstable-tests")

    if cfg.get("mysqld_opts"):
        parts.append(cfg["mysqld_opts"])

    # Feature flags
    if cfg.get("optimistic"):
        parts.append("--mysqld=--tdsql_trans_type=1")
    if cfg.get("record"):
        parts.append("--record")
    if cfg.get("vector"):
        parts.append("--ve-protocol")
    if cfg.get("parallel_query"):
        parts.append("--parallel-query")
    if cfg.get("suite"):
        parts.append(f"--suite={cfg['suite']}")
    if cfg.get("cases"):
        parts.append(" ".join(cfg["cases"]))

    return " ".join(parts)


# -----------------------------------------------------------------------
# Output filtering
# -----------------------------------------------------------------------

# Patterns for noisy lines that should be suppressed from mtr output.
_SUPPRESSED_PATTERNS = [
    # mysqld daemon internal logs  e.g. [2026-04-16 23:38:28 ...] [WARN/INFO/ERROR] ...
    re.compile(r"^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"),
    # AsyncFileWriteLogger rotate messages
    re.compile(r"^AsyncFileWriteLogger"),
    # MySQL server thread exit error
    re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s+\d+\s+\[ERROR\].*my_thread_global_end"),
    # mysql-test-run "Could not parse variable list line" warnings (JSON config noise)
    re.compile(r"^mysql-test-run:\s+WARNING:\s+Could not parse variable list line"),
    # TDStoreServiceImpl noise
    re.compile(r"TDStoreServiceImpl"),
    # brpc init noise
    re.compile(r"bthread/task_control\.cpp"),
]


def _should_suppress(line: str) -> bool:
    """Return True if the line matches any suppressed pattern."""
    for pat in _SUPPRESSED_PATTERNS:
        if pat.search(line):
            return True
    return False


def _filter_output(proc, verbose: bool = False) -> int:
    """Read proc stdout line by line, printing only non-suppressed lines."""
    interrupted = False
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n")
            if verbose or not _should_suppress(line):
                print(line)
                sys.stdout.flush()
    except KeyboardInterrupt:
        interrupted = True
        proc.terminate()
    proc.wait()
    if interrupted:
        return -1
    return proc.returncode


# -----------------------------------------------------------------------
# Handler
# -----------------------------------------------------------------------

def handle_mtr(args, output: "OutputFormatter") -> CommandResult:
    """Handle the 'mtr' command — run native MySQL MTR test suites."""
    return _run_native_mtr(args, output)


def _run_native_mtr(args, output: "OutputFormatter") -> CommandResult:
    """
    Build and execute a native ./mtr command.

    Config resolution order (later wins):
      1. dbms_config.json ``mtr`` section (required)
      2. CLI flags

    All required settings must be present in the config file; otherwise
    an error message is returned guiding the user to configure them.

    Returns:
        CommandResult with execution status
    """
    # --- 1. Load config file ---
    config_path = getattr(args, "config", "dbms_config.json")
    file_cfg = _load_mtr_config(config_path)

    # Required config keys — must be set in dbms_config.json
    required_keys = [
        "test_dir", "skip_list", "base_port", "total_port",
        "parallel", "retry", "retry_failure", "max_test_fail",
        "testcase_timeout", "suite_timeout", "mysqld_opts",
    ]
    missing = [k for k in required_keys if k not in file_cfg]
    if missing:
        return CommandResult.failure(
            f"Missing required mtr config in {os.path.abspath(config_path)}: "
            f"{', '.join(missing)}\n"
            f"Please add them under the 'mtr' section. "
            f"Run 'rosetta config --sample' for a template.",
            command="mtr",
        )

    # Build resolved config from file
    cfg = {
        "test_dir": file_cfg["test_dir"],
        "skip_list": file_cfg["skip_list"],
        "base_port": file_cfg["base_port"],
        "total_port": file_cfg["total_port"],
        "parallel": file_cfg["parallel"],
        "retry": file_cfg["retry"],
        "retry_failure": file_cfg["retry_failure"],
        "max_test_fail": file_cfg["max_test_fail"],
        "testcase_timeout": file_cfg["testcase_timeout"],
        "suite_timeout": file_cfg["suite_timeout"],
        "optimistic": False,
        "record": False,
        "vector": False,
        "parallel_query": False,
        "suite": None,
        "cases": [],
    }

    # mysqld_opts: list → joined CLI flags
    opts = file_cfg["mysqld_opts"]
    if isinstance(opts, list):
        cfg["mysqld_opts"] = _build_mysqld_opts(opts)
    elif isinstance(opts, str):
        cfg["mysqld_opts"] = opts
    else:
        return CommandResult.failure(
            f"Invalid 'mysqld_opts' type in config: expected list or str, got {type(opts).__name__}",
            command="mtr",
        )

    # --- 2. CLI overrides ---
    if getattr(args, "test_dir", None):
        cfg["test_dir"] = args.test_dir
    if getattr(args, "skip_list", None):
        cfg["skip_list"] = args.skip_list
    if getattr(args, "parallel", None):
        cfg["parallel"] = args.parallel
    if getattr(args, "retry", None):
        cfg["retry"] = args.retry
    if getattr(args, "retry_failure", None):
        cfg["retry_failure"] = args.retry_failure
    if getattr(args, "max_test_fail", None):
        cfg["max_test_fail"] = args.max_test_fail
    if getattr(args, "testcase_timeout", None):
        cfg["testcase_timeout"] = args.testcase_timeout
    if getattr(args, "suite_timeout", None):
        cfg["suite_timeout"] = args.suite_timeout

    total_mode = getattr(args, "total", False)
    port_base = cfg["total_port"] if total_mode else cfg["base_port"]
    cfg["port_base"] = port_base

    cfg["optimistic"] = getattr(args, "optimistic", False)
    cfg["record"] = getattr(args, "record", False)
    cfg["vector"] = getattr(args, "vector", False)
    cfg["parallel_query"] = getattr(args, "parallel_query", False)
    cfg["suite"] = getattr(args, "suite", None)
    cfg["cases"] = getattr(args, "cases", [])

    test_dir = cfg["test_dir"]

    # Validate test directory
    if not os.path.isdir(test_dir):
        return CommandResult.failure(
            f"MySQL test directory not found: {test_dir}\n"
            f"Set 'mtr.test_dir' in {config_path}, or use --test-dir.",
            command="mtr",
        )

    # Validate mtr binary
    mtr_bin = os.path.join(test_dir, "mtr")
    if not os.path.isfile(mtr_bin) and not os.path.isfile(mtr_bin + ".py"):
        return CommandResult.failure(
            f"mtr binary not found in {test_dir}\n"
            f"Expected: {mtr_bin} or {mtr_bin}.py",
            command="mtr",
        )

    # Build command
    cmd = _build_command(cfg)

    # Print plan
    if not getattr(args, "json", False):
        print(f"Config file    : {os.path.abspath(config_path)}")
        print(f"Test directory : {test_dir}")
        print(f"Mode           : {'total' if total_mode else 'base'}")
        print(f"Port base      : {port_base}")
        print(f"Skip list      : {cfg['skip_list']}")
        if cfg["suite"]:
            print(f"Suite          : {cfg['suite']}")
        if cfg["cases"]:
            print(f"Cases          : {' '.join(cfg['cases'])}")
        if cfg["record"]:
            print(f"Record mode    : ON")
        if cfg["optimistic"]:
            print(f"Optimistic     : ON")
        if cfg["vector"]:
            print(f"Vector engine  : ON")
        if cfg["parallel_query"]:
            print(f"Parallel query : ON")
        print()

    # Change to test directory and execute
    original_dir = os.getcwd()
    try:
        os.chdir(test_dir)
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        verbose = getattr(args, "verbose", False)
        exit_code = _filter_output(proc, verbose=verbose)
    except Exception as e:
        return CommandResult.failure(f"Failed to execute mtr: {str(e)}", command="mtr")
    finally:
        os.chdir(original_dir)

    if exit_code == -1:
        if not getattr(args, "json", False):
            print("\nInterrupted by user.")
        return CommandResult.failure("MTR execution interrupted by user", command="mtr")
    elif exit_code == 0:
        return CommandResult.success(
            "mtr",
            {
                "test_dir": test_dir,
                "mode": "total" if total_mode else "base",
                "suite": cfg["suite"],
                "cases": cfg["cases"],
                "record": cfg["record"],
                "optimistic": cfg["optimistic"],
                "vector": cfg["vector"],
                "parallel_query": cfg["parallel_query"],
                "exit_code": exit_code,
            },
        )
    else:
        return CommandResult.failure(
            f"MTR execution failed with exit code {exit_code}",
            command="mtr",
        )
