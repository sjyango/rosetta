"""Adapter to integrate rosetta.mtr with existing Rosetta DBConnection.

Provides a DBConnector implementation that wraps rosetta.executor.DBConnection,
enabling the new MTR module to execute SQL against any DBMS that Rosetta supports.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Tuple

from ..executor import DBConnection
from ..models import DBMSConfig, Statement, StmtType

log = logging.getLogger("rosetta.mtr")


class RosettaDBConnector:
    """DBConnector implementation using Rosetta's existing DBConnection.

    This adapter bridges the new rosetta.mtr module with the existing
    database connection infrastructure, so MTR tests can run against
    any DBMS that Rosetta supports (MySQL, TiDB, etc.).

    Usage:
        from rosetta.mtr import MtrParser, MtrExecutor
        from rosetta.mtr.adapter import RosettaDBConnector

        config = DBMSConfig(name="mysql", host="127.0.0.1", port=3306)
        connector = RosettaDBConnector(config, database="test_mtr")
        connector.connect()  # Establish the default connection

        parser = MtrParser("test.test")
        test = parser.parse()

        executor = MtrExecutor(connector)
        result = executor.execute(test)
    """

    def __init__(self, config: DBMSConfig, database: str = "test_mtr"):
        """Initialize the connector adapter.

        Args:
            config: DBMSConfig for the target database.
            database: Database name to use.
        """
        self.config = config
        self.database = database
        self._db: Optional[DBConnection] = None
        self._connected = False
        self._last_error: Optional[Tuple[int, str, str]] = None

    def connect(self, host: str = "", port: int = 0,
                user: str = "", password: str = "",
                database: str = "", **kwargs) -> Any:
        """Create a database connection.

        If host/port/user/password are provided, they override the config.
        Returns a DBConnection object that can be used by MtrExecutor.
        """
        # Override config if connection params are provided
        cfg = DBMSConfig(
            name=self.config.name,
            host=host or self.config.host,
            port=port or self.config.port,
            user=user or self.config.user,
            password=password or self.config.password,
            driver=self.config.driver,
            skip_patterns=self.config.skip_patterns,
            init_sql=self.config.init_sql,
        )

        db = DBConnection(cfg, database or self.database)
        db.connect()
        self._db = db
        self._connected = True
        return db

    def execute(self, conn: Any, sql: str) -> Any:
        """Execute a SQL statement.

        Args:
            conn: A DBConnection object.
            sql: SQL statement to execute.

        Returns:
            A new cursor with the result, or None.
        """
        if not conn or not hasattr(conn, 'cursor') or conn.cursor is None:
            raise RuntimeError("Connection is not established")

        self._last_error = None

        try:
            # Use the underlying DB-API connection to create a fresh cursor
            # for each execution, avoiding shared cursor state issues.
            db_conn = conn.conn
            if db_conn is None:
                raise RuntimeError("Database connection object is None")

            cursor = db_conn.cursor()
            cursor.execute(sql)
            return cursor
        except Exception as e:
            # Extract error info from the pymysql exception itself
            error_code, sqlstate, error_message = 0, "", str(e)

            # pymysql raises OperationalError / ProgrammingError / InternalError
            # with args = (errno, errmsg)
            if hasattr(e, 'args') and isinstance(e.args, tuple) and len(e.args) >= 2:
                try:
                    error_code = int(e.args[0])
                except (ValueError, TypeError):
                    pass
                error_message = str(e.args[1]) if len(e.args) > 1 else str(e)

            # Some pymysql exceptions have errno attribute
            if not error_code and hasattr(e, 'errno'):
                error_code = e.errno

            self._last_error = (error_code, sqlstate, error_message)

            # Check for connection loss
            if hasattr(conn, '_is_connection_lost') and conn._is_connection_lost(e):
                log.warning("Connection lost, attempting reconnect...")
                if hasattr(conn, 'reconnect') and conn.reconnect():
                    cursor = conn.conn.cursor()
                    cursor.execute(sql)
                    self._last_error = None
                    return cursor
            raise

    def fetch_result(self, cursor: Any) -> Any:
        """Fetch all rows from a cursor."""
        try:
            if cursor.description:
                return cursor.fetchall()
        except Exception:
            pass
        return None

    def close(self, conn: Any) -> None:
        """Close a database connection."""
        if conn and hasattr(conn, 'close'):
            conn.close()

    def get_error_info(self, conn: Any) -> Tuple[int, str, str]:
        """Get error info from the last failed operation.

        Returns:
            (error_code, sqlstate, error_message)
        """
        # Prefer the error info captured from the pymysql exception
        # during execute(), which is more reliable than trying to
        # extract from a closed cursor.
        if self._last_error:
            return self._last_error

        # Fallback: try to extract from DBConnection's cursor
        try:
            if conn and hasattr(conn, 'cursor') and conn.cursor:
                err = conn.cursor._result
                if err and hasattr(err, 'errno'):
                    return (err.errno or 0,
                            getattr(err, 'sqlstate', '') or '',
                            getattr(err, 'errmsg', '') or '')
        except Exception:
            pass

        return (0, "", "Unknown error")

    def setup_default_connection(self, executor: Any) -> None:
        """Set up the default connection for an MtrExecutor.

        This creates the initial "default" connection that MTR tests
        expect to be available, and registers it with the executor's
        ConnectionManager.

        Args:
            executor: An MtrExecutor instance.
        """
        conn_obj = self.connect()
        from .connection import Connection
        conn = Connection(
            name="default",
            connector=conn_obj,
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            database=self.database,
        )
        executor.connections.add("default", conn)
        executor.connections.select("default")


def run_mtr_test(test_file_path: str, config: DBMSConfig,
                 database: str = "test_mtr",
                 mysql_test_dir: Optional[str] = None,
                 abort_on_error: bool = True,
                 on_progress=None) -> Any:
    """Convenience function to run an MTR test file against a DBMS.

    This is the simplest way to use the new MTR module with
    Rosetta's existing infrastructure.

    Args:
        test_file_path: Path to the .test file.
        config: DBMSConfig for the target database.
        database: Database name to use.
        mysql_test_dir: Root mysql-test directory for --source resolution.
        abort_on_error: Whether to abort on unexpected SQL errors
            (default True for standard MTR behavior; set False for
            cross-DBMS compare mode where errors should be logged
            but execution continues).

    Returns:
        ExecutionResult with output and status.

    Example:
        from rosetta.models import DBMSConfig
        from rosetta.mtr.adapter import run_mtr_test

        config = DBMSConfig(name="mysql", host="127.0.0.1", port=3306)
        result = run_mtr_test(
            "/path/to/mysql-test/t/select.test",
            config,
            mysql_test_dir="/path/to/mysql-test",
        )
        print(f"Commands: {result.commands_executed}")
        print(f"Output: {len(result.output_lines)} lines")
    """
    from .parser import MtrParser
    from .executor import MtrExecutor

    # Parse the test file
    parser = MtrParser(test_file_path, mysql_test_dir=mysql_test_dir)
    test = parser.parse()

    # Create connector and executor
    connector = RosettaDBConnector(config, database)
    executor = MtrExecutor(connector, mysql_test_dir=mysql_test_dir,
                           abort_on_error=abort_on_error,
                           on_progress=on_progress)

    # Set up the default connection
    connector.setup_default_connection(executor)

    # Execute the test
    return executor.execute(test)


def parse_mtr_to_statements(test_file_path: str,
                             mysql_test_dir: Optional[str] = None,
                             prefer_result: bool = False) -> list:
    """Parse an MTR .test file and convert to Rosetta Statement objects.

    This provides backward compatibility with Rosetta's existing
    Statement-based workflow. It parses the .test file using the
    new MTR parser and converts the commands to Statement objects
    that can be used with run_on_dbms().

    Args:
        test_file_path: Path to the .test file.
        mysql_test_dir: Root mysql-test directory.
        prefer_result: If True, try to use the .result file instead.

    Returns:
        List of Statement objects compatible with Rosetta's executor.
    """
    from .parser import MtrParser
    from .nodes import MtrCommandType

    parser = MtrParser(test_file_path, mysql_test_dir=mysql_test_dir)
    test = parser.parse()

    statements = []
    for cmd in test.commands:
        if cmd.cmd_type == MtrCommandType.SQL:
            stmt = Statement(
                stmt_type=StmtType.SQL,
                text=cmd.argument,
                line_no=cmd.line_no,
            )
            statements.append(stmt)
        elif cmd.cmd_type == MtrCommandType.ECHO:
            stmt = Statement(
                stmt_type=StmtType.ECHO,
                text=cmd.argument,
                line_no=cmd.line_no,
            )
            statements.append(stmt)
        elif cmd.cmd_type == MtrCommandType.ERROR:
            # The next SQL statement should expect this error
            # Store error info in the next statement (simplified)
            pass
        elif cmd.cmd_type == MtrCommandType.SORTED_RESULT:
            # Mark the next SQL statement
            pass
        # Skip other directives for backward-compatible mode

    return statements
