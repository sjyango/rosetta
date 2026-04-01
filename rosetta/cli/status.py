"""
Handler for the 'status' subcommand.
"""

import time
from typing import TYPE_CHECKING

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter


def handle_status(args, output: "OutputFormatter") -> CommandResult:
    """
    Handle the 'status' subcommand.
    
    Args:
        args: Parsed command-line arguments
        output: Output formatter
    
    Returns:
        CommandResult with status information
    """
    return _handle_status_dbms(args, output)


def _handle_status_dbms(args, output: "OutputFormatter") -> CommandResult:
    """
    Check DBMS connection status.
    
    Args:
        args: Parsed arguments
        output: Output formatter
    
    Returns:
        CommandResult with connection status for each DBMS
    """
    import os
    from ..config import load_config, filter_configs
    from ..executor import check_port
    
    # Import driver
    try:
        import pymysql
        pymysql_available = True
    except ImportError:
        pymysql_available = False
    
    try:
        import mysql.connector
        mysql_connector_available = True
    except ImportError:
        mysql_connector_available = False
    
    # Load config
    if not os.path.isfile(args.config):
        return CommandResult.failure(
            f"Config file not found: {args.config}",
        )
    
    all_configs = load_config(args.config)
    if not all_configs:
        return CommandResult.failure(
            f"No databases configured in {args.config}",
        )
    
    # Check all enabled DBMS (no filter by args.dbms for status)
    configs = [c for c in all_configs if c.enabled]
    
    if not configs:
        return CommandResult.failure("No enabled databases in config")
    
    # Check each DBMS
    dbms_status = []
    
    for config in configs:
        status = {
            "name": config.name,
            "host": config.host,
            "port": config.port,
            "driver": config.driver,
        }
        
        # Check port reachability
        start_time = time.time()
        port_reachable = check_port(config.host, config.port, timeout=args.timeout)
        elapsed_ms = round((time.time() - start_time) * 1000, 2)
        
        status["port_reachable"] = port_reachable
        status["latency_ms"] = elapsed_ms if port_reachable else None
        
        # Try actual database connection if port is reachable
        if port_reachable:
            conn = None
            cursor = None
            try:
                # Simple connection without DBConnection's database creation logic
                connect_kwargs = dict(
                    host=config.host,
                    port=config.port,
                    user=config.user,
                    password=config.password,
                    connect_timeout=10,
                )
                
                if config.driver == "mysql.connector":
                    if not mysql_connector_available:
                        raise ImportError("mysql-connector-python not installed")
                    conn = mysql.connector.connect(**connect_kwargs)
                else:
                    if not pymysql_available:
                        raise ImportError("pymysql not installed")
                    conn = pymysql.connect(**connect_kwargs)
                
                cursor = conn.cursor()
                
                # Get version info
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()
                status["connected"] = True
                status["version"] = version[0] if version else "unknown"
                status["error"] = None
                
            except Exception as e:
                status["connected"] = False
                status["version"] = None
                status["error"] = str(e)
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
        else:
            status["connected"] = False
            status["version"] = None
            status["error"] = f"Port {config.host}:{config.port} not reachable"
        
        dbms_status.append(status)
    
    # Summary
    total = len(dbms_status)
    connected = sum(1 for s in dbms_status if s.get("connected", False))
    reachable = sum(1 for s in dbms_status if s.get("port_reachable", False))
    
    return CommandResult.success(
        "status dbms",
        {
            "total": total,
            "connected": connected,
            "reachable": reachable,
            "disconnected": total - connected,
            "dbms": dbms_status,
        },
    )
