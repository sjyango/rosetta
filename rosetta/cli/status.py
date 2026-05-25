"""
Handler for the 'status' subcommand.
"""

import concurrent.futures
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
    from rich.console import Console

    _console = Console(stderr=True)

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

    try:
        import oracledb
        oracledb_available = True
    except ImportError:
        oracledb_available = False

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

    # Check all enabled DBMS (no filter by args.dbms for status)
    configs = [c for c in all_configs if c.enabled]

    if not configs:
        return CommandResult.failure("No enabled databases in config")

    # Check each DBMS in parallel with spinner
    def _check_one(config):
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
                protocol = getattr(config, 'protocol', 'mysql')

                if protocol == "oracle":
                    if not oracledb_available:
                        raise ImportError("python-oracledb not installed")
                    svc = getattr(config, 'service_name', '') or config.name
                    dsn = oracledb.makedsn(config.host, config.port,
                                           service_name=svc)
                    conn = oracledb.connect(
                        user=config.user,
                        password=config.password,
                        dsn=dsn,
                    )
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT banner FROM v$version WHERE ROWNUM = 1")
                else:
                    # MySQL protocol connection
                    connect_kwargs = dict(
                        host=config.host,
                        port=config.port,
                        user=config.user,
                        password=config.password,
                        connect_timeout=10,
                    )

                    if config.driver == "mysql.connector":
                        if not mysql_connector_available:
                            raise ImportError(
                                "mysql-connector-python not installed")
                        conn = mysql.connector.connect(**connect_kwargs)
                    else:
                        if not pymysql_available:
                            raise ImportError("pymysql not installed")
                        conn = pymysql.connect(**connect_kwargs)

                    cursor = conn.cursor()
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

        return status

    dbms_names_str = ", ".join(c.name for c in configs)
    dbms_status = []

    with _console.status(
        f"  [dim]Checking DBMS status ([cyan]{dbms_names_str}[/cyan]) ...[/dim]",
        spinner="dots",
    ):
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(configs)) as pool:
            futures = {pool.submit(_check_one, c): c for c in configs}
            try:
                for fut in concurrent.futures.as_completed(futures, timeout=30):
                    dbms_status.append(fut.result())
            except (TimeoutError, Exception):
                pass

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
