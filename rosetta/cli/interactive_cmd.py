"""
Handler for the 'interactive' subcommand (and aliases 'repl', 'i').
"""

import sys
import time as _time
from typing import TYPE_CHECKING

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter


def handle_interactive(args, output: "OutputFormatter") -> CommandResult:
    """
    Handle the 'interactive' subcommand.
    
    Args:
        args: Parsed command-line arguments
        output: Output formatter
    
    Returns:
        CommandResult with session summary
    """
    import os
    import logging
    from ..config import load_config, filter_configs
    from ..interactive import InteractiveSession, BenchInteractiveSession
    from ..executor import ensure_service
    
    # Load config
    if not os.path.isfile(args.config):
        return CommandResult.failure(
            f"Config file not found: {args.config}\n"
            f"Run 'rosetta config init' to create a sample config, "
            f"or use '-c' to specify the config file path.",
        )
    
    all_configs = load_config(args.config)
    if not all_configs:
        return CommandResult.failure(
            f"No databases configured in {args.config}",
        )
    
    # Filter configs
    import concurrent.futures
    from ..executor import check_port
    from rich.console import Console
    _status_console = Console(stderr=True)

    # --- Separate enabled/disabled DBMS first ---
    enabled_configs = [c for c in all_configs if c.enabled]
    disabled_configs = [c for c in all_configs if not c.enabled]

    if disabled_configs:
        disabled_names = ", ".join(c.name for c in disabled_configs)
        _status_console.print(
            f"  [dim]⏭ Skipped (disabled): "
            f"[dim]{disabled_names}[/dim][/dim]")

    if not enabled_configs:
        return CommandResult.failure(
            "No enabled DBMS found in config.\n"
            "Set \"enabled\": true on at least one database entry, "
            "or use --dbms to specify targets.")

    if args.dbms:
        try:
            configs = filter_configs(enabled_configs, args.dbms)
        except ValueError as e:
            return CommandResult.failure(str(e))

        # Verify connectivity for explicitly requested (enabled) DBMS
        _CONNECT_TIMEOUT = 10

        dbms_names = ", ".join(c.name for c in configs)
        _start_time = _time.time()

        def _quick_check(cfg):
            ok = check_port(cfg.host, cfg.port, timeout=2.0)
            return ok, cfg

        reachable = {}
        unreachable = {}
        total_count = len(configs)

        with _status_console.status(
            f"  [dim]Checking connectivity to "
            f"[cyan]{dbms_names}[/cyan] ...[/dim]",
            spinner="dots",
        ):
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=total_count) as pool:
                futures = {pool.submit(_quick_check, c): c for c in configs}
                try:
                    for fut in concurrent.futures.as_completed(
                            futures, timeout=_CONNECT_TIMEOUT):
                        ok, cfg = fut.result()
                        if ok:
                            reachable[cfg.name] = cfg
                        else:
                            unreachable[cfg.name] = cfg
                except concurrent.futures.TimeoutError:
                    for f in futures:
                        if f.done():
                            try:
                                ok, cfg = f.result()
                                if ok:
                                    reachable[cfg.name] = cfg
                                else:
                                    unreachable[cfg.name] = cfg
                            except Exception:
                                pass
                        else:
                            cfg = futures[f]
                            unreachable[cfg.name] = cfg

        elapsed_total = f"{_time.time() - _start_time:.1f}s"

        if unreachable:
            lines = [f"  {name}: {cfg.host}:{cfg.port}"
                     for name, cfg in unreachable.items()]
            _status_console.print()
            return CommandResult.failure(
                f"The following enabled DBMS are NOT reachable — "
                f"service may be down:\n"
                + "\n".join(lines) +
                "\n\nTo disable a DBMS, set \"enabled\": false in config.")
        _status_console.print(
            f"  [green]✓[/green] [bold green]{len(reachable)} DBMS ready[/bold green]"
            f"  [dim]{elapsed_total}[/dim]")
        configs = list(reachable.values())

    else:
        # Auto-detect: only scan enabled DBMS
        _CONNECT_TIMEOUT = 8

        all_enabled_names = ", ".join(c.name for c in enabled_configs)
        _start_time = _time.time()

        def _quick_check(cfg):
            return check_port(cfg.host, cfg.port, timeout=2.0), cfg

        reachable_configs = []
        unreachable_enabled = []   # enabled but down → warning
        done_count = [0]
        total_count = len(enabled_configs)

        with _status_console.status(
            f"  [dim]Scanning enabled DBMS "
            f"([cyan]{all_enabled_names}[/cyan]) ... "
            f"(timeout {_CONNECT_TIMEOUT}s)[/dim]",
            spinner="dots",
        ) as status:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=total_count) as pool:
                futures = {pool.submit(_quick_check, c): c for c in enabled_configs}
                try:
                    for fut in concurrent.futures.as_completed(
                            futures, timeout=_CONNECT_TIMEOUT):
                        ok, cfg = fut.result()
                        done_count[0] += 1
                        if ok:
                            reachable_configs.append(cfg)
                        else:
                            unreachable_enabled.append(cfg)
                        # Update spinner text with progress
                        remaining = total_count - done_count[0]
                        if remaining > 0:
                            status.update(
                                f"  [dim]Scanning enabled DBMS ... "
                                f"{done_count[0]}/{total_count} checked[/dim]")
                except concurrent.futures.TimeoutError:
                    pass  # use whatever we have

        elapsed_total = f"{_time.time() - _start_time:.1f}s"

        if not reachable_configs:
            # No DBMS reachable — still allow entering interactive mode
            # so user can access TDSQL management, Config, etc.
            down_names = [f"{c.name}" for c in enabled_configs]
            _status_console.print(
                f"  [yellow]⚠[/yellow] No DBMS reachable: "
                f"[red]{', '.join(down_names)}[/red]")
            _status_console.print(
                f"  [dim]Entering interactive mode (TDSQL/Config available)...[/dim]")

        # Warn about enabled-but-down DBMS (non-fatal in auto-detect mode)
        elif unreachable_enabled:
            down_names = [f"{c.name}" for c in unreachable_enabled]
            _status_console.print(
                f"  [yellow]⚠[/yellow] Down: [red]{', '.join(down_names)}[/red]")

        detected_names = ", ".join(c.name for c in reachable_configs)
        _status_console.print(
            f"  [green]✓[/green] [bold green]{len(reachable_configs)} DBMS ready[/bold green]: "
            f"[cyan]{detected_names}[/cyan]  [dim]{elapsed_total}[/dim]")

        configs = reachable_configs if reachable_configs else enabled_configs

    if not configs:
        return CommandResult.failure("No databases configured")
    
    # Start interactive session
    # Note: For JSON output mode, we still launch interactive but inform user
    if output.format == "json":
        # In JSON mode, inform user that interactive mode is intended for human use
        return CommandResult.success(
            "interactive",
            {
                "message": "Interactive mode launched",
                "note": "Interactive mode is designed for human users. Run without -j/--json for best experience.",
                "dbms_targets": [c.name for c in configs],
                "database": args.database,
                "output_dir": os.path.abspath(args.output_dir),
                "serve": True,
                "port": args.port,
            },
        )
    
    # For human mode, actually launch the interactive session
    try:
        # Import the existing interactive logic from old CLI
        from ..cli import _enter_interactive, parse_args
        
        # Build args for legacy interactive mode
        legacy_args = parse_args([
            "-i",
            "--config", args.config,
            "--database", args.database,
            "--output-dir", args.output_dir,
        ])
        
        # Use filtered configs (either user-specified or auto-detected reachable)
        legacy_args.dbms = ",".join(c.name for c in enabled_configs)
        if args.port:
            legacy_args.port = args.port
        
        # serve is always on for interactive mode
        legacy_args.serve = True
        
        # Launch interactive session
        exit_code = _enter_interactive(legacy_args)
        
        return CommandResult.success("interactive")
    
    except KeyboardInterrupt:
        return CommandResult.success("interactive")
    except Exception as e:
        return CommandResult.failure(
            f"Interactive session failed: {str(e)}",
        )
