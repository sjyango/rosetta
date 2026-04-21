"""
Handler for the 'list' subcommand.
"""

import os
from typing import TYPE_CHECKING

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter


def handle_list(args, output: "OutputFormatter") -> CommandResult:
    """
    Handle the 'list' subcommand.
    
    Args:
        args: Parsed command-line arguments
        output: Output formatter
    
    Returns:
        CommandResult with list of resources
    """
    if args.resource == "dbms":
        return _handle_list_dbms(args, output)
    elif args.resource == "history":
        return _handle_list_history(args, output)
    else:
        return CommandResult.failure(
            f"Unknown list resource: {args.resource}",
        )


def _handle_list_dbms(args, output: "OutputFormatter") -> CommandResult:
    """
    List DBMS configurations.
    
    Args:
        args: Parsed arguments
        output: Output formatter
    
    Returns:
        CommandResult with DBMS list
    """
    from ..config import load_config
    from ..executor import DBConnection
    
    # Load config
    if not os.path.isfile(args.config):
        return CommandResult.failure(
            f"Config file not found: {args.config}\n"
            f"Run 'rosetta config init' to create a sample config, "
            f"or use '-c' to specify the config file path.",
        )
    
    all_configs = load_config(args.config)
    
    dbms_list = []
    for config in all_configs:
        # Try to get version info
        version = ""
        if config.enabled:
            try:
                db = DBConnection(config, database="mysql")
                db.connect(timeout=2)
                db.cursor.execute("SELECT VERSION()")
                row = db.cursor.fetchone()
                if row:
                    version = row[0]
                db.close()
            except Exception:
                version = ""
        
        dbms_list.append({
            "name": config.name,
            "host": config.host,
            "port": config.port,
            "user": config.user,
            "driver": config.driver,
            "enabled": config.enabled,
            "has_restart_cmd": bool(config.restart_cmd),
            "version": version,
        })
    
    return CommandResult.success(
        "list dbms",
        {
            "total": len(dbms_list),
            "enabled": sum(1 for d in dbms_list if d["enabled"]),
            "dbms": dbms_list,
        },
    )


def _handle_list_history(args, output: "OutputFormatter") -> CommandResult:
    """
    List execution history.
    
    Args:
        args: Parsed arguments
        output: Output formatter
    
    Returns:
        CommandResult with execution history
    """
    import json
    from pathlib import Path
    
    from ..paths import RESULTS_DIR as _DEFAULT_RESULTS
    output_dir = args.output_dir if hasattr(args, "output_dir") else _DEFAULT_RESULTS
    
    if not os.path.isdir(output_dir):
        return CommandResult.failure(
            f"Output directory not found: {output_dir}",
        )
    
    # Find all run directories
    run_dirs = []
    for entry in sorted(os.listdir(output_dir), reverse=True):
        entry_path = os.path.join(output_dir, entry)
        if not os.path.isdir(entry_path):
            continue
        if entry == "latest":
            continue
        
        # Check if it's a run directory (has result files or bench result)
        result_files = [
            f for f in os.listdir(entry_path)
            if f.endswith(".result") or f == "bench_result.json"
        ]
        
        if result_files:
            # Determine run type
            run_type = "mtr"
            if "bench_result.json" in result_files:
                run_type = "benchmark"
            
            # Get timestamp from directory name
            parts = entry.rsplit("_", 2)
            if len(parts) >= 3:
                timestamp_str = f"{parts[-2]}_{parts[-1]}"
            else:
                timestamp_str = entry
            
            # Try to load result JSON for more info
            result_info = {}
            bench_json = os.path.join(entry_path, "bench_result.json")
            if os.path.isfile(bench_json):
                try:
                    with open(bench_json, "r") as f:
                        result_info = json.load(f)
                except Exception:
                    pass
            
            run_dirs.append({
                "id": entry,
                "timestamp": timestamp_str,
                "type": run_type,
                "directory": entry,
                "workload": result_info.get("workload", ""),
            })
        
        if len(run_dirs) >= args.limit:
            break
    
    return CommandResult.success(
        "list history",
        {
            "total": len(run_dirs),
            "limit": args.limit,
            "runs": run_dirs,
        },
    )



