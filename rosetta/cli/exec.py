"""
Handler for the 'exec' subcommand - execute SQL statements.
"""

from typing import TYPE_CHECKING

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter


def handle_exec(args, output: "OutputFormatter") -> CommandResult:
    """
    Handle the 'exec' subcommand.
    
    Args:
        args: Parsed command-line arguments
        output: Output formatter
    
    Returns:
        CommandResult with execution results
    """
    import os
    import concurrent.futures
    import time as _time
    from ..config import load_config, filter_configs
    from ..executor import DBConnection, check_port
    from ..parser import TestFileParser
    
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
    
    # Filter configs
    if args.dbms:
        try:
            configs = filter_configs(all_configs, args.dbms)
        except ValueError as e:
            return CommandResult.failure(str(e))
    else:
        configs = [c for c in all_configs if c.enabled]
    
    if not configs:
        return CommandResult.failure("No databases selected")
    
    # Get SQL statements
    sql_text = None
    if args.sql:
        sql_text = args.sql
    elif args.file:
        if not os.path.isfile(args.file):
            return CommandResult.failure(
                f"SQL file not found: {args.file}",
            )
        with open(args.file, "r", encoding="utf-8") as f:
            sql_text = f.read()
    else:
        return CommandResult.failure(
            "Either --sql or --file is required",
        )
    
    # Parse SQL statements
    try:
        parsed = TestFileParser.parse_text(sql_text)
        statements = [s.text for s in parsed]
    except Exception as e:
        return CommandResult.failure(f"Parse error: {str(e)}")
    
    # Determine database (None means connect without selecting a database)
    database = args.database if args.database else None
    
    # Execute on each DBMS
    def _exec_on_dbms(config):
        """Execute all statements on one DBMS."""
        result = {
            "name": config.name,
            "statements": [],
            "error": None,
        }
        
        # Check port first
        if not check_port(config.host, config.port):
            result["error"] = f"Cannot reach {config.host}:{config.port}"
            return result
        
        # For exec without --database, connect directly without USE/CREATE
        if database is None:
            conn = None
            cursor = None
            try:
                connect_kwargs = dict(
                    host=config.host,
                    port=config.port,
                    user=config.user,
                    password=config.password,
                    connect_timeout=10,
                )
                if config.driver == "mysql.connector":
                    import mysql.connector
                    connect_kwargs["allow_local_infile"] = True
                    conn = mysql.connector.connect(**connect_kwargs)
                else:
                    import pymysql
                    connect_kwargs["local_infile"] = True
                    conn = pymysql.connect(**connect_kwargs)
                conn.autocommit = True
                cursor = conn.cursor()
            except Exception as e:
                result["error"] = f"Connection failed: {str(e)}"
                return result
            
            try:
                for sql in statements:
                    stmt_result = _exec_stmt(cursor, sql)
                    result["statements"].append(stmt_result)
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
            return result
        
        # With explicit --database, use DBConnection (creates DB + USE)
        db = DBConnection(config, database)
        try:
            db.connect()
        except Exception as e:
            result["error"] = f"Connection failed: {str(e)}"
            return result
        
        try:
            for sql in statements:
                stmt_result = _exec_stmt(db.cursor, sql)
                result["statements"].append(stmt_result)
        finally:
            db.close()
        
        return result
    
    # Execute in parallel
    results = {}
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(configs)) as pool:
        futures = {pool.submit(_exec_on_dbms, c): c for c in configs}
        for fut in concurrent.futures.as_completed(futures):
            r = fut.result()
            results[r["name"]] = r
    
    return CommandResult.success(
        "exec",
        {
            "sql": sql_text[:500],  # Truncate for JSON
            "total_statements": len(statements),
            "database": database,
            "dbms_targets": [c.name for c in configs],
            "results": results,
        },
    )


def _exec_stmt(cursor, sql: str) -> dict:
    """Execute a single SQL statement and return the result dict."""
    import time as _time
    stmt_result = {
        "sql": sql,
        "columns": None,
        "rows": None,
        "error": None,
        "affected_rows": 0,
        "elapsed_ms": 0,
    }
    try:
        t0 = _time.monotonic()
        cursor.execute(sql)
        if cursor.description:
            stmt_result["columns"] = [
                desc[0] for desc in cursor.description
            ]
            rows = cursor.fetchall()
            stmt_result["rows"] = [
                [_format_val(c) for c in row]
                for row in rows
            ]
            stmt_result["row_count"] = len(rows)
        else:
            stmt_result["affected_rows"] = cursor.rowcount or 0
        t1 = _time.monotonic()
        stmt_result["elapsed_ms"] = round((t1 - t0) * 1000, 3)
    except Exception as e:
        t1 = _time.monotonic()
        stmt_result["error"] = str(e)
        stmt_result["elapsed_ms"] = round((t1 - t0) * 1000, 3)
    return stmt_result


def _format_val(value) -> str:
    """Format a cell value for JSON serialization."""
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)
