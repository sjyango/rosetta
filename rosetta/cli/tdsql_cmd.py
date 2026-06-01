"""Handler for 'rosetta tdsql' — build, uninstall, install TDSQL instances."""

import os
import signal
import subprocess
import sys
import time
from typing import TYPE_CHECKING

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

_DEFAULT_TDSQL_CONFIG = {
    "source_dir": "/data/workspace/SQLEngine",
    "port_base": 5886,
    "parallel_jobs": 20,
    "compiler": "clang",
    "linker_debug": "lld",
    "linker_release": "mold",
}


def _load_tdsql_config(config_path: str) -> dict:
    """Load tdsql section from config.json, with defaults."""
    import json
    cfg = dict(_DEFAULT_TDSQL_CONFIG)
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        user_cfg = data.get("tdsql", {})
        cfg.update(user_cfg)
    except Exception:
        pass
    return cfg


# ---------------------------------------------------------------------------
# Live output runner
# ---------------------------------------------------------------------------

_LOG_DIR = os.path.expanduser("~/.rosetta/tdsql_logs")


def _run_with_live_output(cmd: str, cwd: str, label: str,
                          is_json: bool = False,
                          ignore_exit_code: bool = False) -> int:
    """Run a shell command with a live progress panel and log window.

    Displays a bordered panel with:
    - Title and command
    - Progress bar (parsed from cmake "[ XX%]" output)
    - Scrolling log window (last N lines)
    - Elapsed time

    All output is also saved to ~/.rosetta/tdsql_logs/<timestamp>_<label>.log

    Args:
        cmd: Shell command string to execute.
        cwd: Working directory.
        label: Human-readable label for the operation.
        is_json: If True, suppress live output (for JSON mode).

    Returns:
        Process exit code.
    """
    import re
    import datetime
    from collections import deque

    # Prepare log file
    os.makedirs(_LOG_DIR, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_label = re.sub(r'[^\w\-]', '_', label.lower())
    log_path = os.path.join(_LOG_DIR, f"{timestamp}_{safe_label}.log")

    # Header written to top of every log: label, command, cwd, start time
    log_header = (
        f"# ============================================================\n"
        f"# Rosetta TDSQL — {label}\n"
        f"# Started : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"# Cwd     : {cwd}\n"
        f"# Command : {cmd}\n"
        f"# ============================================================\n\n"
    )

    if is_json:
        # JSON mode: run silently, just capture output
        try:
            proc = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, text=True, bufsize=1,
                cwd=cwd, start_new_session=True,
            )
            with open(log_path, "w", encoding="utf-8") as lf:
                lf.write(log_header)
                for line in proc.stdout:
                    lf.write(line)
            proc.wait()
            return proc.returncode
        except Exception:
            return -1

    # Human mode: live panel with progress
    from rich.console import Console
    from rich.live import Live
    from rich.panel import Panel
    from rich.text import Text

    console = Console(stderr=True)
    print("\033[2J\033[H", end="", flush=True)

    # State
    log_lines = deque(maxlen=25)  # visible log window
    elapsed = [0.0]
    status_msg = ["Running..."]

    def _build_display():
        """Build the rich renderable for the live panel."""
        from rich.console import Group

        # Truncate each visible line to fit terminal width.
        # Keep the LEFT side (the informative prefix like "[ 77%] Building...")
        # and replace the overflow with a single ellipsis. Inner padding for the
        # outer panel + side borders eats ~6 columns, account for that.
        term_w = max(60, console.size.width)
        max_line_w = term_w - 6

        # Info panel (top) — shows label, the actual command, cwd, elapsed, log path
        info_panel = Panel(
            Text.from_markup(
                f"  [bold cyan]▶ {label}[/bold cyan]\n"
                f"  [dim]Cmd:[/dim]     [white]{cmd}[/white]\n"
                f"  [dim]Cwd:[/dim]     {cwd}\n"
                f"  [dim]Elapsed:[/dim] [bold]{elapsed[0]:.1f}s[/bold]  "
                f"[dim]Log:[/dim] {log_path}"
            ),
            border_style="cyan",
            padding=(0, 1),
            width=term_w,
        )

        # Log panel (bottom). Truncate each line on the LEFT-overflow side
        # (i.e. keep the head — "[ 77%] Building CXX...") rather than the tail.
        log_text = Text()
        for ln in log_lines:
            if len(ln) > max_line_w:
                shown = ln[: max_line_w - 1] + "…"
            else:
                shown = ln
            log_text.append(shown + "\n", style="dim")

        log_panel = Panel(
            log_text,
            title="[dim]Output[/dim]",
            subtitle=f"[dim]{status_msg[0]}[/dim]  [dim yellow]Ctrl+C to cancel[/dim yellow]",
            border_style="dim",
            padding=(0, 1),
            width=term_w,
        )

        return Group(info_panel, log_panel)

    proc = None
    try:
        proc = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1,
            cwd=cwd, start_new_session=True,
        )

        start_time = time.monotonic()
        start_wall = datetime.datetime.now()

        with open(log_path, "w", encoding="utf-8") as lf:
            lf.write(log_header)
            lf.flush()
            with Live(_build_display(), console=console,
                      refresh_per_second=4, transient=True) as live:
                try:
                    for raw_line in proc.stdout:
                        line = raw_line.rstrip("\n")
                        lf.write(raw_line)
                        lf.flush()
                        # Keep the FULL line; visual truncation is handled
                        # in _build_display() based on terminal width.
                        log_lines.append(line)
                        elapsed[0] = time.monotonic() - start_time

                        live.update(_build_display())
                except (ValueError, OSError):
                    pass

        proc.wait()
        rc = proc.returncode
        elapsed[0] = time.monotonic() - start_time
        end_wall = datetime.datetime.now()
        start_str = start_wall.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_wall.strftime("%Y-%m-%d %H:%M:%S")

        # Final status
        if rc == 0 or ignore_exit_code:
            status_msg[0] = "Completed successfully"
            console.print(Panel(
                f"[green bold]✓[/green bold] {label} — success "
                f"[dim]({elapsed[0]:.1f}s)[/dim]\n"
                f"[dim]Started:[/dim]  [cyan]{start_str}[/cyan]\n"
                f"[dim]Finished:[/dim] [cyan]{end_str}[/cyan]\n"
                f"[dim]Log: {log_path}[/dim]",
                border_style="green",
            ))
            if ignore_exit_code:
                rc = 0
        else:
            status_msg[0] = f"Failed (exit code {rc})"
            # Show last few lines of output for context
            tail = "\n".join(list(log_lines)[-5:])
            console.print(Panel(
                f"[red bold]✗[/red bold] {label} — failed (exit code {rc}) "
                f"[dim]({elapsed[0]:.1f}s)[/dim]\n"
                f"[dim]Started:[/dim]  [cyan]{start_str}[/cyan]\n"
                f"[dim]Finished:[/dim] [cyan]{end_str}[/cyan]\n\n"
                f"[dim]{tail}[/dim]\n\n"
                f"[bold yellow]📄 Full log:[/bold yellow] "
                f"[bold cyan underline]{log_path}[/bold cyan underline]\n"
                f"[dim]   👉 Run:[/dim] [bold]less {log_path}[/bold]   "
                f"[dim]or[/dim]  [bold]tail -100 {log_path}[/bold]",
                border_style="red",
                title="[bold red]Build Failed[/bold red]",
                title_align="left",
            ))
        return rc

    except KeyboardInterrupt:
        if proc and proc.poll() is None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        console.print(f"\n  [yellow bold]Cancelled by user.[/yellow bold]")
        console.print(f"  [dim]Partial log: {log_path}[/dim]\n")
        return -1


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def _do_build(cfg: dict, mode: str, is_json: bool = False,
              overrides: dict = None) -> int:
    """Compile TDSQL source code.

    Args:
        cfg: tdsql config dict.
        mode: "debug" | "release" | "asan"
        overrides: optional dict to override individual build options
                   per call. Keys: compiler, linker, parallel_jobs,
                   build_ut (bool), with_lance (bool), enable_lsan (bool),
                   verbose (bool).

    Returns:
        Exit code (0 = success).
    """
    if overrides:
        cfg = {**cfg, **overrides}

    source_dir = cfg["source_dir"]
    jobs = cfg.get("parallel_jobs", 20)
    compiler = cfg.get("compiler", "clang")
    linker_debug = cfg.get("linker_debug", "lld")
    linker_release = cfg.get("linker_release", "mold")

    # Per-mode linker default (overridable by `linker` in cfg)
    if "linker" in cfg and cfg["linker"]:
        linker = cfg["linker"]
    else:
        linker = linker_debug if mode == "debug" else linker_release

    # Optional toggles (defaults preserve previous behavior)
    build_ut = bool(cfg.get("build_ut", False))       # default: skip UT
    with_lance = cfg.get("with_lance", None)          # None → don't pass flag
    enable_lsan = bool(cfg.get("enable_lsan", False)) # only meaningful w/ asan
    verbose = bool(cfg.get("verbose", False))

    # Build the flag list
    flags = [f"-j {jobs}", f"--compiler={compiler}", f"--linker={linker}"]
    if not build_ut:
        flags.append("--no-build-ut")
    if with_lance is True:
        flags.append("--with-lance=on")
    elif with_lance is False:
        flags.append("--with-lance=off")
    if verbose:
        flags.append("--verbose")

    # Clean cached env files & param-diff files so user changes in make.sh
    # (e.g. WITH_LANCE_MODE=OFF) actually take effect on debug/asan rebuilds.
    # Note: the release env file is `.bash_make.env` (no `_release` suffix),
    # see make.sh: MAKE_RELEASE_ENV_FILE="$MAKE_SCRIPT_DIR/.bash_make.env"
    clean_cmd = (
        "rm -f .bash_make.env .bash_make_debug.env "
        "old_para.txt new_para.txt "
        "bld/CMakeCache.txt bld/old_para.txt bld/new_para.txt 2>/dev/null; "
    )

    # Environment shim for tooling that the user's shell may not have set up.
    # Specifically, libtirpc.pc lives in /usr/lib64/pkgconfig on TencentOS/RHEL,
    # but the toolchain's pkg-config defaults to /opt/rh/gcc-toolset-10/...
    # Without this, cmake's MYSQL_CHECK_RPC fails with:
    #   "Could not find rpc/rpc.h in /usr/include or /usr/include/tirpc"
    # Order matters: prepend so existing user paths still take precedence
    # for any other package, but our path becomes a fallback.
    env_setup = (
        'export PKG_CONFIG_PATH="/usr/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"; '
    )

    if mode == "debug":
        cmd = f"{env_setup}{clean_cmd}./make.sh --debug {' '.join(flags)}"
        label = "Build TDSQL (debug)"
    elif mode == "release":
        # Release reuses cache by design (faster incremental builds).
        cmd = f"{env_setup}./make.sh {' '.join(flags)}"
        label = "Build TDSQL (release)"
    elif mode == "asan":
        # NOTE: my_scripts.sh uses `-u -a 1` but `-u` takes an argument
        # in make.sh getopt, which causes IS_BUILD_UT to be set to "-a"
        # (visible as "build unittests: -a" in make.sh's banner).
        # Use the long-form flag instead for correct parsing.
        asan_flags = ["-a 1"] + flags
        if enable_lsan:
            asan_flags.append("--enable-lsan")
        cmd = f"{env_setup}{clean_cmd}./make.sh {' '.join(asan_flags)}"
        label = "Build TDSQL (asan)"
    else:
        return -1

    return _run_with_live_output(cmd, source_dir, label, is_json)


# ---------------------------------------------------------------------------
# Uninstall
# ---------------------------------------------------------------------------

def _do_uninstall(cfg: dict, is_json: bool = False) -> int:
    """Uninstall (cleanup) TDSQL instance.

    Note: cluster.sh cleanup may return non-zero even on success
    (e.g. when directories don't exist). We treat any execution
    that completes without crash as success.
    """
    source_dir = cfg["source_dir"]
    bld_dir = os.path.join(source_dir, "bld")

    cmd = "bash ../tdsql/cluster/cluster.sh cleanup --force"
    rc = _run_with_live_output(cmd, bld_dir, "Uninstall TDSQL", is_json,
                               ignore_exit_code=True)
    return rc



# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------

def _do_install(cfg: dict, port_base: int = None, is_json: bool = False) -> int:
    """Install (init) TDSQL instance.

    Full procedure from my_scripts.sh install():
    1. Enable fast port recycling (sysctl)
    2. Kill all remaining sqlengine/mc processes (by port, not by name)
    3. Clean reserved port records
    4. Kill processes on target ports (kill_cluster_by_port_base)
    5. Wait for ports to be fully released (including TIME_WAIT)
    6. Run cluster.sh init --port-base
    7. Restore kernel params
    8. Verify expected port is listening
    """
    source_dir = cfg["source_dir"]
    bld_dir = os.path.join(source_dir, "bld")
    if port_base is None:
        port_base = cfg["port_base"]

    expected_port = port_base + 5000
    install_root = os.path.join(source_dir, "mysql_install")

    install_script = f"""#!/bin/bash
set -e

PORT_BASE={port_base}
EXPECTED_PORT=$((PORT_BASE + 5000))
INSTALL_ROOT="{install_root}"

# ── Helper functions ──

kill_by_port() {{
  local port=$1
  local pid
  pid=$(ss -tlnp 2>/dev/null | grep ":${{port}} " | grep -oP 'pid=\\K[0-9]+' | head -1)
  if [ -n "$pid" ]; then
    echo "  port $port is occupied by pid=$pid, killing it..."
    kill -9 "$pid" 2>/dev/null || true
    sleep 3
    pid=$(ss -tlnp 2>/dev/null | grep ":${{port}} " | grep -oP 'pid=\\K[0-9]+' | head -1)
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null && sleep 2
  fi
}}

kill_cluster_by_port_base() {{
  local base=$1
  echo "Killing all processes on port_base=$base ..."
  for offset in 0 1 2; do
    kill_by_port $((base + offset))
  done
  for offset in 5000 5001 5002; do
    kill_by_port $((base + offset))
  done
  sleep 3
}}

is_port_fully_free() {{
  local port=$1
  if ss -tan 2>/dev/null | tail -n +2 | grep -qE ":${{port}}[[:space:]]"; then
    return 1
  fi
  return 0
}}

kill_time_wait() {{
  local port=$1
  ss --kill state time-wait "( sport = :${{port}} )" 2>/dev/null || true
  ss --kill state time-wait "( dport = :${{port}} )" 2>/dev/null || true
}}

wait_ports_free() {{
  local base=$1
  local max_wait=65
  local elapsed=0
  local -a ports_to_wait=(
    $((base + 0)) $((base + 1)) $((base + 2))
    $((base + 5000)) $((base + 5001)) $((base + 5002))
  )

  sysctl -w net.ipv4.tcp_tw_reuse=1 2>/dev/null || true

  echo "Killing TIME_WAIT connections on ports [${{ports_to_wait[*]}}]..."
  for p in "${{ports_to_wait[@]}}"; do
    kill_time_wait "$p"
  done
  sleep 1

  echo "Waiting for ports [${{ports_to_wait[*]}}] to be fully free..."
  while (( elapsed < max_wait )); do
    local all_free=true
    for p in "${{ports_to_wait[@]}}"; do
      if ! is_port_fully_free "$p"; then
        all_free=false
        break
      fi
    done
    if $all_free; then
      echo "All ports free after ${{elapsed}}s"
      return 0
    fi
    if (( elapsed > 0 && elapsed % 10 == 0 )); then
      for p in "${{ports_to_wait[@]}}"; do
        kill_time_wait "$p"
      done
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done

  echo "WARNING: some ports still in use after ${{max_wait}}s, force killing TIME_WAIT..."
  for p in "${{ports_to_wait[@]}}"; do
    kill_time_wait "$p"
  done
  sleep 2
}}

clean_reserved_ports() {{
  echo "Cleaning reserved ports records in $INSTALL_ROOT ..."
  find "$INSTALL_ROOT" -name "ports.toml" -delete 2>/dev/null || true
  find "$INSTALL_ROOT" -path "*/.cluster" -type d -exec rm -rf {{}} + 2>/dev/null || true
}}

# ── Main install procedure ──

echo "Install SQLEngine (PORT_BASE=$PORT_BASE -> SQLEngine=$EXPECTED_PORT)"

# Enable fast port recycling
sysctl -w net.ipv4.tcp_tw_reuse=1 2>/dev/null || true
sysctl -w net.ipv4.tcp_fin_timeout=5 2>/dev/null || true

# Kill all remaining processes by port (not by name to avoid killing self)
kill_cluster_by_port_base $PORT_BASE

# Clean reserved ports
clean_reserved_ports

# Wait for ports to be fully free (including TIME_WAIT)
wait_ports_free $PORT_BASE

# Init cluster
echo "Initializing cluster (port_base=$PORT_BASE)..."
bash ../tdsql/cluster/cluster.sh init --port-base $PORT_BASE

# Restore kernel params
sysctl -w net.ipv4.tcp_fin_timeout=60 2>/dev/null || true

# Verify
actual_port=$(ss -tlnp 2>/dev/null | grep -oP "(?<=:)${{EXPECTED_PORT}}(?=\\s)" | head -1)
if [ "$actual_port" = "$EXPECTED_PORT" ]; then
    echo "✅ SQLEngine listening on expected port $EXPECTED_PORT"
else
    echo "⚠️  WARNING: expected port $EXPECTED_PORT but it may have shifted!"
fi
"""
    return _run_with_live_output(
        f"bash -c '{install_script}'", bld_dir,
        f"Install TDSQL (port_base={port_base})", is_json)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def handle_tdsql(args, output: "OutputFormatter") -> CommandResult:
    """Handle 'rosetta tdsql' subcommands."""
    action = getattr(args, "tdsql_action", None)
    if not action:
        return CommandResult.failure(
            "No action specified. Use: rosetta tdsql {build|uninstall|install|reinstall}")

    is_json = getattr(args, "json", False)
    config_path = getattr(args, "config", None) or os.path.expanduser("~/.rosetta/config.json")
    cfg = _load_tdsql_config(config_path)

    def _build_overrides_from_args() -> dict:
        """Translate argparse build flags into a _do_build overrides dict."""
        ov = {}
        if getattr(args, "compiler", None):
            ov["compiler"] = args.compiler
        if getattr(args, "linker", None):
            ov["linker"] = args.linker
        if getattr(args, "jobs", None):
            ov["parallel_jobs"] = args.jobs
        if getattr(args, "build_ut", False):
            ov["build_ut"] = True
        wl = getattr(args, "with_lance", None)
        if wl == "on":
            ov["with_lance"] = True
        elif wl == "off":
            ov["with_lance"] = False
        if getattr(args, "enable_lsan", False):
            ov["enable_lsan"] = True
        if getattr(args, "build_verbose", False):
            ov["verbose"] = True
        return ov

    if action == "build":
        mode = getattr(args, "mode", "debug")
        rc = _do_build(cfg, mode, is_json, overrides=_build_overrides_from_args())
        if rc == 0:
            return CommandResult.success("tdsql build", {"mode": mode})
        return CommandResult.failure(f"Build failed (exit code {rc})")

    elif action == "uninstall":
        rc = _do_uninstall(cfg, is_json)
        if rc == 0:
            return CommandResult.success("tdsql uninstall")
        return CommandResult.failure(f"Uninstall failed (exit code {rc})")

    elif action == "deploy":
        # Uninstall + Install as atomic operation
        rc = _do_uninstall(cfg, is_json)
        if rc != 0:
            return CommandResult.failure(f"Uninstall failed (exit code {rc})")
        port_base = getattr(args, "port_base", None)
        rc = _do_install(cfg, port_base, is_json)
        if rc == 0:
            return CommandResult.success("tdsql deploy",
                                         {"port_base": port_base or cfg["port_base"]})
        return CommandResult.failure(f"Install failed (exit code {rc})")

    elif action == "install":
        port_base = getattr(args, "port_base", None)
        rc = _do_install(cfg, port_base, is_json)
        if rc == 0:
            return CommandResult.success("tdsql install",
                                         {"port_base": port_base or cfg["port_base"]})
        return CommandResult.failure(f"Install failed (exit code {rc})")

    elif action == "reinstall":
        mode = getattr(args, "mode", "debug")
        # Step 1: Build
        rc = _do_build(cfg, mode, is_json, overrides=_build_overrides_from_args())
        if rc != 0:
            return CommandResult.failure(f"Build failed (exit code {rc})")
        # Step 2: Uninstall
        rc = _do_uninstall(cfg, is_json)
        if rc != 0:
            return CommandResult.failure(f"Uninstall failed (exit code {rc})")
        # Step 3: Install
        rc = _do_install(cfg, None, is_json)
        if rc != 0:
            return CommandResult.failure(f"Install failed (exit code {rc})")
        return CommandResult.success("tdsql reinstall", {"mode": mode})

    else:
        return CommandResult.failure(f"Unknown tdsql action: {action}")
