"""
Handler for the 'config' subcommand.
"""

import json
import os
from typing import TYPE_CHECKING

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter


def handle_config(args, output: "OutputFormatter") -> CommandResult:
    """
    Handle the 'config' subcommand.
    
    Args:
        args: Parsed command-line arguments
        output: Output formatter
    
    Returns:
        CommandResult with config information
    """
    if args.action == "show":
        return _handle_config_show(args, output)
    elif args.action == "validate":
        return _handle_config_validate(args, output)
    elif args.action == "init":
        return _handle_config_init(args, output)
    else:
        return CommandResult.failure(
            f"Unknown config action: {args.action}",
        )


def _handle_config_show(args, output: "OutputFormatter") -> CommandResult:
    """
    Show current configuration.
    
    Args:
        args: Parsed arguments
        output: Output formatter
    
    Returns:
        CommandResult with config details
    """
    from ..config import load_config
    
    if not os.path.isfile(args.config):
        return CommandResult.failure(
            f"Config file not found: {args.config}\n"
            f"Run 'rosetta config init' to create a sample config, "
            f"or use '-c' to specify the config file path.",
        )
    
    try:
        configs = load_config(args.config)
    except Exception as e:
        return CommandResult.failure(f"Failed to load config: {str(e)}")
    
    # Read raw JSON for display
    with open(args.config, "r", encoding="utf-8") as f:
        raw_config = json.load(f)
    
    return CommandResult.success(
        "config show",
        {
            "config_path": os.path.abspath(args.config),
            "total_dbms": len(configs),
            "enabled_dbms": sum(1 for c in configs if c.enabled),
            "databases": [
                {
                    "name": c.name,
                    "host": c.host,
                    "port": c.port,
                    "user": c.user,
                    "driver": c.driver,
                    "enabled": c.enabled,
                    "has_init_sql": bool(c.init_sql),
                    "skip_patterns_count": len(c.skip_patterns),
                }
                for c in configs
            ],
            "raw_config": raw_config,
        },
    )


def _handle_config_validate(args, output: "OutputFormatter") -> CommandResult:
    """
    Validate configuration file.
    
    Args:
        args: Parsed arguments
        output: Output formatter
    
    Returns:
        CommandResult with validation results
    """
    import socket
    from ..config import load_config
    from ..executor import check_port
    
    if not os.path.isfile(args.config):
        return CommandResult.failure(
            f"Config file not found: {args.config}\n"
            f"Run 'rosetta config init' to create a sample config, "
            f"or use '-c' to specify the config file path.",
        )
    
    errors = []
    warnings = []
    
    # Validate JSON structure
    try:
        with open(args.config, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return CommandResult.failure(
            f"Invalid JSON: {str(e)}",
        )
    
    # Check databases array
    if "databases" not in data:
        return CommandResult.failure(
            "Missing 'databases' key in config",
        )
    
    if not isinstance(data["databases"], list):
        return CommandResult.failure(
            "'databases' must be an array",
        )
    
    if len(data["databases"]) == 0:
        return CommandResult.failure(
            "No databases configured",
        )
    
    # Validate each database config
    for i, db in enumerate(data["databases"]):
        prefix = f"databases[{i}]"
        
        # Required fields
        if "name" not in db:
            errors.append(f"{prefix}: missing 'name' field")
        
        # Optional fields with defaults
        host = db.get("host", "127.0.0.1")
        port = db.get("port", 3306)
        
        # Validate types
        if not isinstance(host, str):
            errors.append(f"{prefix}.host: must be a string")
        
        if not isinstance(port, int):
            errors.append(f"{prefix}.port: must be an integer")
        
        # Check if port is valid
        if isinstance(port, int) and (port < 1 or port > 65535):
            errors.append(f"{prefix}.port: must be between 1 and 65535")
    
    # Try to load config
    try:
        configs = load_config(args.config)
    except Exception as e:
        errors.append(f"Failed to load config: {str(e)}")
        configs = []
    
    # Check connectivity for enabled databases
    connectivity = []
    for config in configs:
        if not config.enabled:
            continue
        
        reachable = check_port(config.host, config.port, timeout=2)
        connectivity.append({
            "name": config.name,
            "host": config.host,
            "port": config.port,
            "reachable": reachable,
        })
        
        if not reachable:
            warnings.append(
                f"{config.name} ({config.host}:{config.port}): not reachable"
            )
    
    if errors:
        return CommandResult.failure(
            "Config validation failed",
        )
    
    return CommandResult.success(
        "config validate",
        {
            "config_path": os.path.abspath(args.config),
            "valid": True,
            "total_dbms": len(configs),
            "enabled_dbms": sum(1 for c in configs if c.enabled),
            "errors": errors,
            "warnings": warnings,
            "connectivity": connectivity,
        },
    )


def _handle_config_init(args, output: "OutputFormatter") -> CommandResult:
    """
    Initialize ~/.rosetta directory and generate sample config.
    
    Creates the ~/.rosetta/ directory structure and generates a sample
    config.json if it doesn't already exist.
    
    Args:
        args: Parsed arguments
        output: Output formatter
    
    Returns:
        CommandResult with generated config path
    """
    from ..config import generate_sample_config
    from ..paths import CONFIG_FILE, ensure_home
    
    # Determine output path
    output_path = args.output if args.output else CONFIG_FILE
    
    # Ensure ~/.rosetta directory exists
    home = ensure_home()
    
    # Check if file already exists
    if os.path.isfile(output_path):
        # Preserve the original command name: "init" when called via
        # ``rosetta init``, "config init" when called via ``rosetta config init``
        command = getattr(args, 'command', None)
        cmd_name = "init" if command == "init" else "config init"
        return CommandResult.failure(
            f"Config already exists: {output_path}. "
            f"Edit it directly or use --output to specify a different path.",
            command=cmd_name,
        )
    
    # Generate sample config
    try:
        generate_sample_config(output_path)
    except Exception as e:
        command = getattr(args, 'command', None)
        cmd_name = "init" if command == "init" else "config init"
        return CommandResult.failure(
            f"Failed to generate config: {str(e)}",
            command=cmd_name,
        )
    
    return CommandResult.success(
        "config init",
        {
            "rosetta_home": home,
            "config_path": os.path.abspath(output_path),
            "message": f"Initialized {home}\n"
                       f"Config written to {output_path}\n"
                       f"Edit the database connections, then run: rosetta status",
        },
    )
