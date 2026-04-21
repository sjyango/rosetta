"""
Handler for the 'mtr' command — run native MySQL MTR test suites.

This wraps the ./mtr binary in the MySQL test directory, supporting
common options like suite selection, record mode, optimistic transactions,
vector engine, parallel query, etc.

Supports running multiple modes (row/column/pq) in parallel via --mode.

Configuration is read from the same ~/.rosetta/config.json file under the
"mtr" top-level key.  CLI flags override config values.
"""

import concurrent.futures
import json
import os
import re
import subprocess
import sys
import threading
import time as _time
from typing import TYPE_CHECKING, Dict, List, Optional

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter


# -----------------------------------------------------------------------
# Mode definitions
# -----------------------------------------------------------------------

# Canonical mode names and their display labels
MTR_MODES = {
    "row":    {"label": "行存 (Row)",    "vector": False, "parallel_query": False},
    "col":    {"label": "列存 (Column)", "vector": True,  "parallel_query": False},
    "pq":     {"label": "PQ (Parallel)", "vector": False, "parallel_query": True},
}

# Aliases for convenience (column -> col)
_MODE_ALIASES = {"column": "col"}

# Port offset per mode (to avoid port conflicts when running in parallel)
# Each MTR worker uses ~30 ports, with --parallel=8 that's ~240 ports.
# Use 1000 offset per mode to be safe.
_MODE_PORT_OFFSETS = {"row": 0, "col": 1000, "pq": 2000}


# -----------------------------------------------------------------------
# Config loading
# -----------------------------------------------------------------------

def _load_mtr_config(config_path: str) -> dict:
    """
    Load the ``mtr`` section from ~/.rosetta/config.json.

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

    # Isolated var/tmp directories for parallel mode execution
    if cfg.get("vardir"):
        parts.append(f"--vardir={cfg['vardir']}")
    if cfg.get("tmpdir"):
        parts.append(f"--tmpdir={cfg['tmpdir']}")

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
    # var directory cleanup noise (chmod/delete failures on stale files)
    re.compile(r"^couldn't chmod\("),
    re.compile(r"^Couldn't delete file "),
    # SSL library warning (harmless)
    re.compile(r"\[Warning\].*CRYPTO_set_mem_functions failed"),
    # mysqld timestamp logs (e.g. 2026-04-18T15:04:09.835941+08:00 ...)
    re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[Z+-]"),
]


def _should_suppress(line: str) -> bool:
    """Return True if the line matches any suppressed pattern."""
    for pat in _SUPPRESSED_PATTERNS:
        if pat.search(line):
            return True
    return False


# Pattern for MTR progress lines: [  XX% ] test_name  worker  [ result ]  time
_MTR_PROGRESS_RE = re.compile(r"^\[\s*(\d+)%\]")


def _parse_mtr_progress(line: str) -> Optional[int]:
    """Extract progress percentage from an MTR output line.

    Returns the percentage (0-100) if found, else None.
    """
    m = _MTR_PROGRESS_RE.search(line.strip())
    if m:
        return int(m.group(1))
    return None


def _filter_output(proc, verbose: bool = False,
                   on_progress=None,
                   log_file=None) -> int:
    """Read proc stdout line by line, printing only non-suppressed lines.

    Args:
        proc: subprocess to read from
        verbose: if True, print all lines including suppressed ones
        on_progress: optional callback ``on_progress(percent, line)``
                     called when a progress percentage is detected
        log_file: optional file object to write all non-suppressed lines to
    """
    interrupted = False
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n")
            # Check for progress before filtering
            if on_progress:
                pct = _parse_mtr_progress(line)
                if pct is not None:
                    on_progress(pct, line)
            is_suppressed = _should_suppress(line)
            if log_file and not is_suppressed:
                log_file.write(line + "\n")
                log_file.flush()
            if verbose or not is_suppressed:
                print(line)
                sys.stdout.flush()
    except KeyboardInterrupt:
        interrupted = True
        proc.terminate()
    proc.wait()
    if interrupted:
        return -1
    return proc.returncode


def _parse_mtr_log_stats(log_path: str) -> dict:
    """Parse MTR log tail to extract test statistics.

    Looks for lines like:
      Total cases: 88
      Pass cases: 82
      Fail cases: 6
      Pass ratio: 93.18%
      Failing test(s): case1 case2 ...
    """
    stats: dict = {}
    if not log_path or not os.path.isfile(log_path):
        return stats

    try:
        # Read only last 2KB for efficiency
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 4096))
            tail = f.read()
    except Exception:
        return stats

    for line in tail.splitlines():
        line = line.strip()
        if line.startswith("Total cases:"):
            try:
                stats["total"] = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("Pass cases:"):
            try:
                stats["pass"] = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("Fail cases:"):
            try:
                stats["fail"] = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("Pass ratio:"):
            stats["pass_ratio"] = line.split(":", 1)[1].strip()
        elif line.startswith("Failing test(s):"):
            cases_str = line.split(":", 1)[1].strip()
            if cases_str:
                stats["failing_tests"] = cases_str.split()

    return stats


# -----------------------------------------------------------------------
# Handler
# -----------------------------------------------------------------------

def handle_mtr(args, output: "OutputFormatter") -> CommandResult:
    """Handle the 'mtr' command — run native MySQL MTR test suites.

    When ``--mode`` is given (e.g. ``--mode row,col,pq``), the handler
    launches each mode in parallel, with per-mode progress bars and log
    files.  Terminal output is kept minimal (progress bars + final table).
    """
    modes_str = getattr(args, "mode", None)
    if modes_str:
        # Multi-mode parallel execution
        requested = [m.strip().lower() for m in modes_str.split(",") if m.strip()]
        # Apply aliases (e.g. column -> col, all -> row,col,pq)
        expanded = []
        for m in requested:
            if m == "all":
                expanded.extend(list(MTR_MODES.keys()))
            else:
                expanded.append(_MODE_ALIASES.get(m, m))
        requested = expanded
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for m in requested:
            if m not in seen:
                seen.add(m)
                unique.append(m)
        requested = unique
        invalid = [m for m in requested if m not in MTR_MODES]
        if invalid:
            return CommandResult.failure(
                f"Unknown MTR mode(s): {', '.join(invalid)}. "
                f"Valid modes: {', '.join(MTR_MODES.keys())}",
                command="mtr",
            )
        if len(requested) < 2:
            # Single mode via --mode, just treat as normal
            mode_def = MTR_MODES[requested[0]]
            args.vector = mode_def["vector"]
            args.parallel_query = mode_def["parallel_query"]
            return _run_native_mtr(args, output)
        return _run_parallel_modes(args, output, requested)
    else:
        # Legacy single-mode execution
        return _run_native_mtr(args, output)


def _parse_mtr_mode_name(args) -> str:
    """Determine the human-readable mode name from args flags."""
    if getattr(args, "vector", False):
        return "col"
    elif getattr(args, "parallel_query", False):
        return "pq"
    return "row"


# -----------------------------------------------------------------------
# Multi-mode parallel runner
# -----------------------------------------------------------------------

def _run_parallel_modes(
    args, output: "OutputFormatter", modes: List[str]
) -> CommandResult:
    """Run multiple MTR modes in parallel with Rich progress UI.

    Each mode gets its own subprocess and log file.  The terminal shows
    a live progress panel with one row per mode, and after all modes
    finish, a summary table is printed.
    """
    from rich import box
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text

    console = Console(stderr=True)
    is_json = getattr(args, "json", False)

    # --- 1. Resolve shared config (validates once) ---
    from ..paths import CONFIG_FILE, MTR_LOGS_DIR
    config_path = getattr(args, "config", CONFIG_FILE)
    file_cfg = _load_mtr_config(config_path)

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

    test_dir = getattr(args, "test_dir", None) or file_cfg["test_dir"]
    if not os.path.isdir(test_dir):
        return CommandResult.failure(
            f"MySQL test directory not found: {test_dir}\n"
            f"Set 'mtr.test_dir' in {config_path}, or use --test-dir.",
            command="mtr",
        )
    mtr_bin = os.path.join(test_dir, "mtr")
    if not os.path.isfile(mtr_bin) and not os.path.isfile(mtr_bin + ".py"):
        return CommandResult.failure(
            f"mtr binary not found in {test_dir}",
            command="mtr",
        )

    total_mode = getattr(args, "total", False)
    base_port = file_cfg["total_port"] if total_mode else file_cfg["base_port"]

    # --- 2. Create log directory ---
    log_dir = os.path.join(
        MTR_LOGS_DIR,
        _time.strftime("%Y%m%d_%H%M%S"),
    )
    os.makedirs(log_dir, exist_ok=True)

    # --- 3. Build per-mode configs ---
    mode_cfgs = {}
    for mode_name in modes:
        mode_def = MTR_MODES[mode_name]
        cfg = {
            "test_dir": test_dir,
            "skip_list": getattr(args, "skip_list", None) or file_cfg["skip_list"],
            "parallel": getattr(args, "parallel", None) or file_cfg["parallel"],
            "retry": getattr(args, "retry", None) or file_cfg["retry"],
            "retry_failure": getattr(args, "retry_failure", None) or file_cfg["retry_failure"],
            "max_test_fail": getattr(args, "max_test_fail", None) or file_cfg["max_test_fail"],
            "testcase_timeout": getattr(args, "testcase_timeout", None) or file_cfg["testcase_timeout"],
            "suite_timeout": getattr(args, "suite_timeout", None) or file_cfg["suite_timeout"],
            "port_base": base_port + _MODE_PORT_OFFSETS[mode_name],
            "optimistic": getattr(args, "optimistic", False),
            "record": getattr(args, "record", False),
            "vector": mode_def["vector"],
            "parallel_query": mode_def["parallel_query"],
            "suite": getattr(args, "suite", None),
            "cases": getattr(args, "cases", []),
            # Isolated var/tmp directories per mode to prevent conflicts
            "vardir": os.path.join(test_dir, f"var_{mode_name}"),
            "tmpdir": os.path.join(test_dir, f"tmp_{mode_name}"),
        }
        opts = file_cfg["mysqld_opts"]
        if isinstance(opts, list):
            cfg["mysqld_opts"] = _build_mysqld_opts(opts)
        elif isinstance(opts, str):
            cfg["mysqld_opts"] = opts
        else:
            cfg["mysqld_opts"] = ""
        mode_cfgs[mode_name] = cfg

    # --- 4. Print plan ---
    if not is_json:
        console.print()
        plan_table = Table(
            show_header=True,
            header_style="bold cyan",
            expand=True,
            box=box.ROUNDED,
        )
        plan_table.add_column("Mode", style="bold", min_width=16)
        plan_table.add_column("Port Base", justify="right")
        plan_table.add_column("Vardir")
        plan_table.add_column("Flags")
        plan_table.add_column("Log File")

        for mode_name in modes:
            mode_def = MTR_MODES[mode_name]
            cfg = mode_cfgs[mode_name]
            flags = []
            if cfg["vector"]:
                flags.append("--ve-protocol")
            if cfg["parallel_query"]:
                flags.append("--parallel-query")
            if cfg["optimistic"]:
                flags.append("optimistic")
            if cfg["record"]:
                flags.append("--record")
            log_file = os.path.join(log_dir, f"{mode_name}.log")
            plan_table.add_row(
                mode_def["label"],
                str(cfg["port_base"]),
                os.path.basename(cfg["vardir"]),
                " ".join(flags) if flags else "(default)",
                os.path.abspath(log_file),
            )

        console.print(plan_table)

        # Config info panel
        info_lines = []
        info_lines.append(f"[bold]Config [/bold]   : {os.path.abspath(config_path)}")
        info_lines.append(f"[bold]Test dir[/bold]  : {test_dir}")
        info_lines.append(f"[bold]Log dir[/bold]   : {log_dir}")
        if getattr(args, "suite", None):
            info_lines.append(f"[bold]Suite[/bold]     : {args.suite}")
        if getattr(args, "cases", []):
            info_lines.append(f"[bold]Cases[/bold]     : {' '.join(args.cases)}")
        console.print(Panel(
            "\n".join(info_lines),
            title="[bold cyan]Configuration[/bold cyan]",
            title_align="left",
            padding=(0, 1),
        ))

        # Print actual MTR commands per mode
        for mode_name in modes:
            cfg = mode_cfgs[mode_name]
            cmd = _build_command(cfg)
            label = MTR_MODES[mode_name]["label"]
            console.print(Panel(
                f"[dim]{cmd}[/dim]",
                title=f"[bold cyan]{label}[/bold cyan]",
                title_align="left",
                padding=(0, 1),
            ))

    # --- 5. Execute modes in parallel with live progress ---
    results_lock = threading.Lock()
    mode_results: Dict[str, dict] = {}
    total_start_time = _time.monotonic()
    # Track state for progress display
    mode_state: Dict[str, dict] = {
        m: {"status": "waiting", "elapsed": 0.0, "exit_code": None,
            "last_line": "", "start_time": None, "progress": 0}
        for m in modes
    }

    def _run_single_mode(mode_name: str) -> dict:
        """Execute a single MTR mode, writing output to a log file."""
        cfg = mode_cfgs[mode_name]
        cmd = _build_command(cfg)
        log_path = os.path.join(log_dir, f"{mode_name}.log")

        with results_lock:
            mode_state[mode_name]["status"] = "running"
            mode_state[mode_name]["start_time"] = _time.monotonic()

        exit_code = -1
        try:
            proc = subprocess.Popen(
                cmd, shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True, bufsize=1,
                cwd=test_dir,
            )
            with open(log_path, "w", encoding="utf-8") as log_f:
                try:
                    for raw_line in proc.stdout:
                        line = raw_line.rstrip("\n")
                        stripped = line.strip()
                        # Filter noisy lines from log file
                        if _should_suppress(stripped):
                            continue
                        log_f.write(line + "\n")
                        log_f.flush()
                        # Parse progress percentage
                        pct = _parse_mtr_progress(stripped)
                        # Update last meaningful line for progress display
                        if stripped:
                            with results_lock:
                                if pct is not None:
                                    mode_state[mode_name]["progress"] = pct
                                mode_state[mode_name]["last_line"] = stripped[-80:]
                except KeyboardInterrupt:
                    proc.terminate()
                proc.wait()
                exit_code = proc.returncode
        except Exception as e:
            with open(log_path, "a", encoding="utf-8") as log_f:
                log_f.write(f"\n[ERROR] {e}\n")

        elapsed = _time.monotonic() - (mode_state[mode_name]["start_time"] or _time.monotonic())
        with results_lock:
            mode_state[mode_name]["status"] = "done"
            mode_state[mode_name]["exit_code"] = exit_code
            mode_state[mode_name]["elapsed"] = elapsed

        return {
            "mode": mode_name,
            "label": MTR_MODES[mode_name]["label"],
            "exit_code": exit_code,
            "elapsed": elapsed,
            "log_file": log_path,
            "port_base": cfg["port_base"],
        }

    def _build_progress_table() -> Table:
        """Build the live progress table."""
        table = Table(
            show_header=True,
            header_style="bold cyan",
            expand=True,
            padding=(0, 1),
            box=box.ROUNDED,
        )
        table.add_column("Mode", style="bold", min_width=16)
        table.add_column("Progress", min_width=14)
        table.add_column("Elapsed", justify="right", min_width=10)
        table.add_column("Log File", min_width=20, no_wrap=True)
        table.add_column("Latest Output", ratio=1, overflow="ellipsis", no_wrap=True)

        for m in modes:
            st = mode_state[m]
            label = MTR_MODES[m]["label"]
            elapsed_str = ""
            if st["status"] == "done" and st["elapsed"] > 0:
                # Use frozen elapsed time for completed modes
                elapsed = st["elapsed"]
            elif st["start_time"] is not None:
                # Live counting for running modes
                elapsed = _time.monotonic() - st["start_time"]
            else:
                elapsed = 0
            if elapsed > 0:
                mins, secs = divmod(int(elapsed), 60)
                hours, mins = divmod(mins, 60)
                if hours > 0:
                    elapsed_str = f"{hours}h{mins:02d}m{secs:02d}s"
                else:
                    elapsed_str = f"{mins:02d}m{secs:02d}s"

            # Build progress display
            pct = st.get("progress", 0)
            if st["status"] == "waiting":
                status = Text("⏳ Waiting", style="dim")
            elif st["status"] == "running":
                bar_filled = int(pct / 5)  # 20-char bar
                bar_empty = 20 - bar_filled
                bar_str = f"[yellow]{'█' * bar_filled}{'░' * bar_empty}[/yellow] {pct}%"
                status = Text.from_markup(bar_str)
            elif st["status"] == "done":
                if st["exit_code"] == 0:
                    status = Text("✅ Passed", style="green bold")
                else:
                    status = Text(f"❌ Failed({st['exit_code']})", style="red bold")
            else:
                status = Text(st["status"])

            # For done states, always show 100%
            if st["status"] == "done":
                pct_display = 100
            else:
                pct_display = pct

            # Log file path (absolute)
            log_path = os.path.abspath(os.path.join(log_dir, f"{m}.log"))

            table.add_row(label, status, elapsed_str, log_path, st.get("last_line", ""))

        return table

    interrupted = False
    # Both JSON and non-JSON modes show live progress on stderr
    with Live(
        _build_progress_table(),
        console=console,
        refresh_per_second=2,
        transient=False,
    ) as live:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(modes)
        ) as pool:
            futures = {
                pool.submit(_run_single_mode, m): m for m in modes
            }

            # Update progress while waiting
            while True:
                done_futures = {
                    f for f in futures if f.done()
                }
                live.update(_build_progress_table())

                if len(done_futures) == len(futures):
                    break
                _time.sleep(0.5)

            # Collect results
            for fut in futures:
                try:
                    result = fut.result()
                    mode_results[result["mode"]] = result
                except KeyboardInterrupt:
                    interrupted = True
                except Exception as e:
                    m = futures[fut]
                    mode_results[m] = {
                        "mode": m,
                        "label": MTR_MODES[m]["label"],
                        "exit_code": -1,
                        "elapsed": 0,
                        "log_file": os.path.join(log_dir, f"{m}.log"),
                        "error": str(e),
                    }

    if interrupted:
        if not is_json:
            console.print("\n[yellow bold]Interrupted by user.[/yellow bold]")
        return CommandResult.failure("MTR execution interrupted by user", command="mtr")

    # --- 6. Print final summary ---
    if not is_json:
        summary = Table(
            show_header=True,
            header_style="bold cyan",
            padding=(0, 1),
            box=box.ROUNDED,
            expand=True,
        )
        summary.add_column("Mode", style="bold", min_width=16)
        summary.add_column("Result", min_width=10)
        summary.add_column("Total", justify="center")
        summary.add_column("Pass", justify="center")
        summary.add_column("Fail", justify="center")
        summary.add_column("Pass Rate", justify="center")
        summary.add_column("Elapsed", justify="right", min_width=10)
        summary.add_column("Log File")

        mode_stats: Dict[str, dict] = {}
        for m in modes:
            r = mode_results.get(m, {})
            label = MTR_MODES[m]["label"]
            ec = r.get("exit_code", -1)
            elapsed = r.get("elapsed", 0)
            log_file = r.get("log_file", "")

            # Parse stats from log file
            stats = _parse_mtr_log_stats(log_file)
            mode_stats[m] = stats

            # Format elapsed
            mins, secs = divmod(int(elapsed), 60)
            hours, mins = divmod(mins, 60)
            if hours > 0:
                elapsed_str = f"{hours}h{mins:02d}m{secs:02d}s"
            else:
                elapsed_str = f"{mins:02d}m{secs:02d}s"

            if ec == 0:
                result_text = "[green bold]PASSED[/green bold]"
            else:
                result_text = "[red bold]FAILED[/red bold]"

            summary.add_row(
                label,
                result_text,
                str(stats.get("total", "-")),
                f"[green]{stats.get('pass', '-')}[/green]",
                f"[red]{stats.get('fail', '-')}[/red]" if stats.get("fail", 0) > 0 else str(stats.get("fail", "-")),
                stats.get("pass_ratio", "-"),
                elapsed_str,
                os.path.abspath(log_file) if log_file else "",
            )

        console.print(summary)

        # Show failed cases per mode
        for m in modes:
            stats = mode_stats.get(m, {})
            failing = stats.get("failing_tests", [])
            if failing:
                if not has_failures:
                    console.print()
                    has_failures = True
                label = MTR_MODES[m]["label"]
                cases_str = "\n".join(f"  [red]•[/red] {c}" for c in failing)
                console.print(Panel(
                    cases_str,
                    title=f"[bold red]{label} — Failed Cases ({len(failing)})[/bold red]",
                    title_align="left",
                    border_style="red",
                    padding=(0, 1),
                ))

    # --- 7. Build result ---
    any_failed = any(
        r.get("exit_code", -1) != 0 for r in mode_results.values()
    )
    # Parse stats for JSON output (reuse if already parsed)
    if not is_json:
        # Already parsed above
        all_mode_stats = mode_stats
    else:
        all_mode_stats = {}
        for m in modes:
            r = mode_results.get(m, {})
            all_mode_stats[m] = _parse_mtr_log_stats(r.get("log_file", ""))

    result_data = {
        "test_dir": test_dir,
        "modes": modes,
        "port_mode": "total" if total_mode else "base",
        "suite": getattr(args, "suite", None),
        "cases": getattr(args, "cases", []),
        "record": getattr(args, "record", False),
        "optimistic": getattr(args, "optimistic", False),
        "log_dir": log_dir,
        "total_elapsed_seconds": round(_time.monotonic() - total_start_time, 1),
        "mode_results": {
            m: {
                "label": MTR_MODES[m]["label"],
                "exit_code": r.get("exit_code", -1),
                "elapsed_seconds": round(r.get("elapsed", 0), 1),
                "log_file": r.get("log_file", ""),
                "total_cases": all_mode_stats.get(m, {}).get("total"),
                "pass_cases": all_mode_stats.get(m, {}).get("pass"),
                "fail_cases": all_mode_stats.get(m, {}).get("fail"),
                "pass_ratio": all_mode_stats.get(m, {}).get("pass_ratio"),
                "failing_tests": all_mode_stats.get(m, {}).get("failing_tests", []),
            }
            for m, r in mode_results.items()
        },
    }

    if any_failed:
        failed_names = [
            MTR_MODES[m]["label"]
            for m in modes
            if mode_results.get(m, {}).get("exit_code", -1) != 0
        ]
        return CommandResult.partial(
            command="mtr",
            data=result_data,
            warning=f"Some test cases failed in mode(s): {', '.join(failed_names)}",
        )
    return CommandResult.success("mtr", result_data)


def _run_native_mtr(args, output: "OutputFormatter") -> CommandResult:
    """
    Build and execute a native ./mtr command.

    Config resolution order (later wins):
      1. ~/.rosetta/config.json ``mtr`` section (required)
      2. CLI flags

    All required settings must be present in the config file; otherwise
    an error message is returned guiding the user to configure them.

    Returns:
        CommandResult with execution status
    """
    # --- 1. Load config file ---
    from ..paths import CONFIG_FILE
    config_path = getattr(args, "config", CONFIG_FILE)
    file_cfg = _load_mtr_config(config_path)

    # Required config keys — must be set in ~/.rosetta/config.json
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

    is_json = getattr(args, "json", False)

    # Print plan
    if not is_json:
        from rich.console import Console as _Console
        from rich.panel import Panel as _Panel
        console_plan = _Console(stderr=True)
        info_lines = []
        info_lines.append(f"[bold]Config [/bold]    : {os.path.abspath(config_path)}")
        info_lines.append(f"[bold]Test dir[/bold]   : {test_dir}")
        info_lines.append(f"[bold]Mode[/bold]       : {'total' if total_mode else 'base'}")
        info_lines.append(f"[bold]Port base[/bold]  : {port_base}")
        info_lines.append(f"[bold]Skip list[/bold]  : {cfg['skip_list']}")
        if cfg["suite"]:
            info_lines.append(f"[bold]Suite[/bold]      : {cfg['suite']}")
        if cfg["cases"]:
            info_lines.append(f"[bold]Cases[/bold]      : {' '.join(cfg['cases'])}")
        if cfg["record"]:
            info_lines.append(f"[bold]Record[/bold]     : ON")
        if cfg["optimistic"]:
            info_lines.append(f"[bold]Optimistic[/bold] : ON")
        if cfg["vector"]:
            info_lines.append(f"[bold]Vector[/bold]     : ON")
        if cfg["parallel_query"]:
            info_lines.append(f"[bold]PQ[/bold]         : ON")
        console_plan.print(_Panel(
            "\n".join(info_lines),
            title="[bold cyan]MTR Execution Plan[/bold cyan]",
            title_align="left",
            padding=(0, 1),
        ))

    # --- 4. Execute MTR ---
    original_dir = os.getcwd()
    mtr_start_time = _time.monotonic()

    # Build mode label for progress display
    mode_parts = []
    if cfg["vector"]:
        mode_parts.append("ve-protocol")
    if cfg["parallel_query"]:
        mode_parts.append("parallel-query")
    if cfg["optimistic"]:
        mode_parts.append("optimistic")
    mode_label = "+".join(mode_parts) if mode_parts else "row (default)"
    mode_name = _parse_mtr_mode_name(args)

    # Create log directory
    from ..paths import MTR_LOGS_DIR
    log_dir = os.path.join(
        MTR_LOGS_DIR,
        _time.strftime("%Y%m%d_%H%M%S"),
    )
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{mode_name}.log")

    verbose = getattr(args, "verbose", False)

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

        if not verbose:
            # Use Live Table (same style as parallel mode) for non-verbose
            from rich import box
            from rich.console import Console
            from rich.live import Live
            from rich.table import Table
            from rich.text import Text

            console = Console(stderr=True)

            # Track state for progress display
            progress_pct = 0
            last_line = ""
            done = False
            final_exit_code = -1

            def _build_single_progress_table() -> Table:
                nonlocal progress_pct, last_line, done, final_exit_code
                table = Table(
                    show_header=True,
                    header_style="bold cyan",
                    expand=True,
                    padding=(0, 1),
                    box=box.ROUNDED,
                )
                table.add_column("Mode", style="bold", min_width=16)
                table.add_column("Progress", min_width=14)
                table.add_column("Elapsed", justify="right", min_width=10)
                table.add_column("Log File", min_width=20, no_wrap=True)
                table.add_column("Latest Output", ratio=1, overflow="ellipsis", no_wrap=True)

                label = mode_label
                elapsed = _time.monotonic() - mtr_start_time
                mins, secs = divmod(int(elapsed), 60)
                hours, mins = divmod(mins, 60)
                if hours > 0:
                    elapsed_str = f"{hours}h{mins:02d}m{secs:02d}s"
                else:
                    elapsed_str = f"{mins:02d}m{secs:02d}s"

                if done:
                    if final_exit_code == 0:
                        status = Text("✅ Passed", style="green bold")
                    else:
                        status = Text(f"❌ Failed({final_exit_code})", style="red bold")
                else:
                    bar_filled = int(progress_pct / 5)  # 20-char bar
                    bar_empty = 20 - bar_filled
                    bar_str = f"[yellow]{'█' * bar_filled}{'░' * bar_empty}[/yellow] {progress_pct}%"
                    status = Text.from_markup(bar_str)

                table.add_row(label, status, elapsed_str,
                              os.path.abspath(log_path), last_line[-80:])
                return table

            with Live(
                _build_single_progress_table(),
                console=console,
                refresh_per_second=2,
                transient=is_json,
            ) as live:
                # Read MTR output in a background thread so the
                # Live display can keep refreshing Elapsed even
                # when no new output arrives.
                read_error = None

                def _reader():
                    nonlocal progress_pct, last_line, read_error
                    try:
                        for raw_line in proc.stdout:
                            line = raw_line.rstrip("\n")
                            stripped = line.strip()
                            if not _should_suppress(stripped):
                                log_f.write(line + "\n")
                                log_f.flush()
                            pct = _parse_mtr_progress(stripped)
                            if pct is not None:
                                progress_pct = pct
                            if stripped:
                                last_line = stripped
                    except Exception as e:
                        read_error = e

                with open(log_path, "w", encoding="utf-8") as log_f:
                    reader_thread = threading.Thread(target=_reader, daemon=True)
                    reader_thread.start()
                    try:
                        while reader_thread.is_alive():
                            live.update(_build_single_progress_table())
                            reader_thread.join(timeout=0.5)
                    except KeyboardInterrupt:
                        proc.terminate()
                        reader_thread.join(timeout=2)
                    # Final update after reader finishes
                    proc.wait()
                    final_exit_code = proc.returncode
                    done = True
                    progress_pct = 100
                    live.update(_build_single_progress_table())

            exit_code = final_exit_code
        else:
            # Verbose mode: print everything to stdout, no progress table
            with open(log_path, "w", encoding="utf-8") as log_f:
                exit_code = _filter_output(proc, verbose=True, log_file=log_f)

    except Exception as e:
        return CommandResult.failure(f"Failed to execute mtr: {str(e)}", command="mtr")
    finally:
        os.chdir(original_dir)

    if exit_code == -1:
        if not is_json:
            console_err = Console(stderr=True)
            console_err.print("\n[yellow bold]Interrupted by user.[/yellow bold]")
        return CommandResult.failure("MTR execution interrupted by user", command="mtr")

    # Print elapsed time (only for verbose mode; non-verbose already shows in Live Table)
    total_elapsed = _time.monotonic() - mtr_start_time
    if not is_json and verbose:
        from rich.console import Console as _Console
        console_out = _Console(stderr=True)
        total_mins, total_secs = divmod(int(total_elapsed), 60)
        total_hours, total_mins = divmod(total_mins, 60)
        if total_hours > 0:
            elapsed_str = f"{total_hours}h{total_mins:02d}m{total_secs:02d}s"
        else:
            elapsed_str = f"{total_mins:02d}m{total_secs:02d}s"
        console_out.print(f"\n  Log directory : {log_dir}")
        console_out.print(f"  Log file      : [bold]{os.path.abspath(log_path)}[/bold]")
        console_out.print(f"  Elapsed       : [bold]{elapsed_str}[/bold] ({round(total_elapsed, 1)}s)")

    # --- 5. Return result ---
    total_elapsed = round(_time.monotonic() - mtr_start_time, 1)
    result_data = {
        "test_dir": test_dir,
        "mode": "total" if total_mode else "base",
        "suite": cfg["suite"],
        "cases": cfg["cases"],
        "record": cfg["record"],
        "optimistic": cfg["optimistic"],
        "vector": cfg["vector"],
        "parallel_query": cfg["parallel_query"],
        "exit_code": exit_code,
        "elapsed_seconds": total_elapsed,
        "log_dir": log_dir,
        "log_file": os.path.abspath(log_path),
    }

    if exit_code == 0:
        return CommandResult.success("mtr", result_data)
    else:
        # MTR executed successfully but some test cases failed — this is a
        # partial failure, not a tool execution failure.
        return CommandResult.partial(
            command="mtr",
            data=result_data,
            warning=f"MTR completed with exit code {exit_code} (some test cases failed)",
        )
