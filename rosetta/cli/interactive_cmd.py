"""
Handler for the 'interactive' subcommand (and aliases 'repl', 'i').
"""

import sys
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
    if args.dbms:
        try:
            configs = filter_configs(all_configs, args.dbms)
        except ValueError as e:
            return CommandResult.failure(str(e))
    else:
        # Auto-detect reachable DBMS
        reachable_configs = []
        for config in all_configs:
            if ensure_service(config):
                reachable_configs.append(config)
        
        if not reachable_configs:
            return CommandResult.failure(
                "No reachable DBMS found. Check your ~/.rosetta/config.json"
            )
        
        configs = reachable_configs
    
    if not configs:
        return CommandResult.failure("No databases selected")
    
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
        legacy_args.dbms = ",".join(c.name for c in configs)
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
