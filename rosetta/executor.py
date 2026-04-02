"""SQL execution engine for Rosetta."""

import logging
import re
import socket
import subprocess
import time
import traceback
from typing import List

from .models import DBMSConfig, Statement, StmtResult, StmtType

log = logging.getLogger("rosetta")

try:
    import mysql.connector
except ImportError:
    mysql_connector_available = False
else:
    mysql_connector_available = True

try:
    import pymysql
except ImportError:
    pymysql_available = False
else:
    pymysql_available = True


def format_cell(value) -> str:
    """Format a single cell value for output."""
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def format_result(stmt: Statement, result: StmtResult,
                  dbms_config: DBMSConfig) -> List[str]:
    """Format a statement result into lines matching MTR .result style."""
    output: List[str] = []

    if stmt.stmt_type == StmtType.ECHO:
        output.append(stmt.text)
        return output

    # Prefix the first line of the SQL with the source line number so that
    # duplicate SQL statements can be uniquely identified in reports.
    sql = stmt.text
    sql_lines = sql.split("\n")
    for i, sql_line in enumerate(sql_lines):
        if i == 0:
            output.append(f"[L{stmt.line_no}] {sql_line}")
        else:
            output.append(sql_line)

    if result.error:
        if stmt.expected_error:
            output.append(f"ERROR: {result.error}")
        else:
            output.append(f"ERROR (unexpected): {result.error}")
        return output

    if result.columns and result.rows is not None:
        output.append("\t".join(result.columns))
        for row in result.rows:
            output.append("\t".join(format_cell(c) for c in row))

    if result.warnings:
        output.append("Warnings:")
        for w in result.warnings:
            output.append(w)

    return output


class DBConnection:
    """Wraps a MySQL-protocol database connection."""

    def __init__(self, config: DBMSConfig, database: str):
        self.config = config
        self.database = database
        self.conn = None
        self.cursor = None
        self._query_timeout = 0
        self._skip_patterns = [re.compile(p, re.IGNORECASE)
                               for p in config.skip_patterns]

    def connect(self, query_timeout: int = 0):
        """Connect to the database.

        Only establishes the connection and switches to the target database.
        Does NOT drop or create the database — all DDL must be explicit
        in setup/teardown SQL defined by the user.

        Args:
            query_timeout: Query timeout in seconds. If > 0, set max_execution_time.
                           Pass -1 to reuse the previously saved value (for reconnect).
        """
        if query_timeout >= 0:
            self._query_timeout = query_timeout
        qt = self._query_timeout
        kwargs = dict(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            connect_timeout=10,  # Connection timeout in seconds
            read_timeout=max(60, qt * 2) if qt > 0 else 60,
        )

        if self.config.driver == "mysql.connector":
            if not mysql_connector_available:
                raise ImportError(
                    "mysql-connector-python is not installed. "
                    "Install via: pip install mysql-connector-python"
                )
            # Enable LOCAL INFILE for LOAD DATA operations
            kwargs["allow_local_infile"] = True
            self.conn = mysql.connector.connect(**kwargs)
        else:
            if not pymysql_available:
                raise ImportError(
                    "PyMySQL is not installed. "
                    "Install via: pip install pymysql"
                )
            # Enable LOCAL INFILE for LOAD DATA operations
            kwargs["local_infile"] = True
            self.conn = pymysql.connect(**kwargs)

        # Enable autocommit - use method for pymysql, attribute for mysql.connector
        if hasattr(self.conn, 'autocommit') and callable(self.conn.autocommit):
            self.conn.autocommit(True)  # pymysql style
        else:
            self.conn.autocommit = True  # mysql.connector style
        self.cursor = self.conn.cursor()

        # Ensure the database exists, then switch to it
        self.cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{self.database}`")
        self.cursor.execute(f"USE `{self.database}`")

        # Set query timeout at database level
        if qt > 0:
            timeout_ms = qt * 1000
            # Try different timeout settings for various DBMS
            for sql in [
                f"SET SESSION max_execution_time = {timeout_ms}",  # MySQL/TiDB
                f"SET SESSION tidb_max_execution_time = {timeout_ms}",  # TiDB specific
            ]:
                try:
                    self.cursor.execute(sql)
                except Exception:
                    pass  # Ignore if not supported

        for sql in self.config.init_sql:
            try:
                self.cursor.execute(sql)
            except Exception as e:
                log.warning("[%s] init_sql failed: %s — %s",
                            self.config.name, sql, e)

    def close(self):
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def should_skip(self, sql: str) -> bool:
        """Check if this SQL should be skipped for this DBMS."""
        for pat in self._skip_patterns:
            if pat.search(sql):
                return True
        return False

    def _is_connection_lost(self, err: Exception) -> bool:
        """Check if the error indicates a lost connection."""
        err_str = str(err)
        code = (getattr(err, 'args', (None,))[0]
                if hasattr(err, 'args') else None)
        if code in (0, 2006, 2013):
            return True
        if "Lost connection" in err_str or "gone away" in err_str:
            return True
        if "Connection refused" in err_str:
            return True
        # Connection object became None (e.g. after socket timeout)
        if "NoneType" in err_str and ("settimeout" in err_str or "attribute" in err_str):
            return True
        if self.conn is None or self.cursor is None:
            return True
        return False

    def reconnect(self):
        """Attempt to reconnect after a lost connection."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.close()
                time.sleep(2 ** attempt)
                self.connect(query_timeout=-1)  # reuse saved timeout
                log.info("[%s] Reconnected successfully (attempt %d)",
                         self.config.name, attempt + 1)
                return True
            except Exception as e:
                log.warning("[%s] Reconnect attempt %d failed: %s",
                            self.config.name, attempt + 1, e)
        log.error("[%s] All reconnect attempts failed", self.config.name)
        return False

    def execute(self, sql: str, sort_result: bool = False) -> StmtResult:
        """Execute a SQL statement and capture the result."""
        result = StmtResult(stmt=Statement(StmtType.SQL, sql, 0))
        try:
            self.cursor.execute(sql)

            if self.cursor.description:
                result.columns = [desc[0]
                                  for desc in self.cursor.description]
                rows = self.cursor.fetchall()
                if sort_result:
                    rows = sorted(rows,
                                  key=lambda r: [str(c) for c in r])
                result.rows = rows
            else:
                result.affected_rows = self.cursor.rowcount or 0

            try:
                self.cursor.execute("SHOW WARNINGS")
                warns = self.cursor.fetchall()
                if warns:
                    result.warnings = [
                        f"{w[0]}\t{w[1]}\t{w[2]}" for w in warns
                    ]
            except Exception:
                pass

        except Exception as e:
            result.error = str(e)
            if self._is_connection_lost(e):
                log.warning("[%s] Connection lost, attempting reconnect...",
                            self.config.name)
                if self.reconnect():
                    try:
                        self.cursor.execute(f"USE `{self.database}`")
                    except Exception:
                        pass

        return result


def check_port(host: str, port: int, timeout: float = 3.0) -> bool:
    """Check if a TCP port is reachable."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def ensure_service(config: DBMSConfig) -> bool:
    """Ensure the DBMS service is up; try restart if configured.

    Returns True if reachable, False otherwise.
    """
    name = config.name
    if check_port(config.host, config.port):
        return True

    log.warning("[%s] Port %s:%d is not reachable",
                name, config.host, config.port)

    if not config.restart_cmd:
        log.error("[%s] No restart_cmd configured, cannot recover", name)
        return False

    log.info("[%s] Attempting restart via: %s", name, config.restart_cmd)
    try:
        subprocess.run(
            config.restart_cmd, shell=True,
            timeout=60, check=False,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except Exception as e:
        log.error("[%s] restart_cmd failed: %s", name, e)
        return False

    for attempt in range(10):
        time.sleep(3)
        if check_port(config.host, config.port):
            log.info("[%s] Service is back up after restart", name)
            return True
        log.info("[%s] Waiting for service... (%d/10)", name, attempt + 1)

    log.error("[%s] Service did not come back after restart", name)
    return False


def run_on_dbms(config: DBMSConfig, statements: List[Statement],
                database: str,
                should_skip_fn=None,
                on_connect=None,
                on_progress=None,
                on_done=None) -> List[str]:
    """Execute all statements on a single DBMS and return output lines.

    Connects to the database and executes the given statements.
    Does NOT automatically drop or recreate the database — all DDL must
    be explicit in the statements themselves.

    Args:
        config: DBMS connection config.
        statements: Parsed statements to execute.
        database: Test database name.
        should_skip_fn: Optional callable(stmt) -> bool for global skips.
        on_connect: Optional callback(name, success, msg) called after connect.
        on_progress: Optional callback(error: bool) called per statement.
        on_done: Optional callback(name, executed, errors) called when done.

    Returns:
        List of output lines, or None if connection failed.
    """
    name = config.name

    if not ensure_service(config):
        log.error("[%s] Service unavailable, skipping", name)
        if on_connect:
            on_connect(name, False, "Service unavailable")
        return None

    db = DBConnection(config, database)
    output_lines: List[str] = []

    try:
        db.connect()
        log.debug("[%s] Connected, using database '%s'", name, database)
        if on_connect:
            on_connect(name, True, f"Connected ({config.host}:{config.port})")
    except Exception as e:
        log.error("[%s] Connection failed: %s", name, e)
        if on_connect:
            on_connect(name, False, str(e))
        return None

    total = len(statements)
    executed = 0
    errors = 0

    try:
        for i, stmt in enumerate(statements):
            if stmt.stmt_type == StmtType.ECHO:
                output_lines.extend(
                    format_result(stmt, StmtResult(stmt=stmt), config)
                )
                if on_progress:
                    on_progress(error=False)
                continue

            if should_skip_fn and should_skip_fn(stmt):
                if on_progress:
                    on_progress(error=False)
                continue

            if db.should_skip(stmt.text):
                if on_progress:
                    on_progress(error=False)
                continue

            result = db.execute(stmt.text, sort_result=stmt.sort_result)
            result.stmt = stmt
            output_lines.extend(format_result(stmt, result, config))
            executed += 1

            has_error = bool(result.error and not stmt.expected_error)
            if has_error:
                errors += 1
                log.warning("[%s] Error at line %d: %s — %s",
                            name, stmt.line_no,
                            stmt.text[:80], result.error)

            if on_progress:
                on_progress(error=has_error)

    except Exception as e:
        log.error("[%s] Fatal error: %s", name, e)
        log.error(traceback.format_exc())
        output_lines.append(f"FATAL ERROR: {e}")
    finally:
        db.close()

    log.debug("[%s] Done: %d executed, %d errors", name, executed, errors)
    if on_done:
        on_done(name, executed, errors)
    return output_lines
