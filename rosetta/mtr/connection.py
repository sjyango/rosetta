"""Connection management for MTR test execution.

Implements multi-connection support matching mysqltest.cc's connection
management: --connect, --disconnect, --connection, --dirty_close,
--change_user, --send_quit, --reset_connection.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

log = logging.getLogger("rosetta.mtr")


class DBConnector(Protocol):
    """Protocol for database connector implementations.

    Users must provide a concrete implementation that connects
    to their target DBMS.
    """

    def connect(self, host: str, port: int, user: str, password: str,
                database: str, **kwargs) -> Any:
        """Create a new database connection."""
        ...

    def execute(self, conn: Any, sql: str) -> Any:
        """Execute a SQL statement on the given connection."""
        ...

    def fetch_result(self, cursor: Any) -> Any:
        """Fetch the result from a cursor after execution."""
        ...

    def close(self, conn: Any) -> None:
        """Close a database connection."""
        ...

    def get_error_info(self, conn: Any) -> tuple:
        """Get error info: (error_code, sqlstate, error_message)."""
        ...


@dataclass
class Connection:
    """Represents a named database connection.

    Corresponds to struct st_connection in mysqltest.cc.
    """
    name: str
    connector: Any  # The underlying DB-API connection object
    host: str = "localhost"
    port: int = 3306
    user: str = ""
    database: str = ""
    pending: bool = False  # Has a pending send query
    pending_sql: str = ""  # The SQL that was sent but not reaped


class ConnectionManager:
    """Manages multiple named database connections.

    Provides the Python equivalent of mysqltest.cc's connection pool
    management (do_connect, do_close_connection, select_connection, etc.)
    """

    def __init__(self, default_connector: Optional[DBConnector] = None):
        self._connections: Dict[str, Connection] = {}
        self._current_name: str = ""
        self._default_connector = default_connector

    @property
    def current(self) -> Optional[Connection]:
        """Get the current active connection."""
        return self._connections.get(self._current_name)

    @property
    def current_name(self) -> str:
        """Get the name of the current active connection."""
        return self._current_name

    def add(self, name: str, conn: Connection) -> None:
        """Add a named connection to the pool."""
        self._connections[name] = conn

    def connect(self, name: str, host: str = "localhost",
                port: int = 3306, user: str = "root",
                password: str = "", database: str = "",
                connector: Optional[DBConnector] = None,
                **kwargs) -> Connection:
        """Create and add a new named connection.

        This is the Python equivalent of do_connect() in mysqltest.cc.

        Args:
            name: Connection name (used by --connection to switch).
            host: Database host.
            port: Database port.
            user: Database user.
            password: Database password.
            database: Default database.
            connector: DBConnector implementation.
            **kwargs: Additional connection options.

        Returns:
            The new Connection object.

        Raises:
            ConnectionError: If the connection cannot be established.
        """
        db_connector = connector or self._default_connector
        if not db_connector:
            raise ConnectionError("No DB connector provided")

        try:
            conn_obj = db_connector.connect(
                host=host, port=port, user=user,
                password=password, database=database,
                **kwargs
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect {name}@{host}:{port}: {e}") from e

        conn = Connection(
            name=name,
            connector=conn_obj,
            host=host,
            port=port,
            user=user,
            database=database,
        )
        self._connections[name] = conn
        self._current_name = name
        log.info("Connected: %s@%s:%d (db=%s)", name, host, port, database)
        return conn

    def disconnect(self, name: str, dirty: bool = False) -> None:
        """Close a named connection.

        This is the Python equivalent of do_close_connection() in mysqltest.cc.

        Args:
            name: The connection name to close.
            dirty: If True, don't send a proper close command (dirty_close).
        """
        conn = self._connections.get(name)
        if not conn:
            raise ConnectionError(f"Connection '{name}' not found")

        if not dirty:
            try:
                if self._default_connector and conn.connector:
                    self._default_connector.close(conn.connector)
            except Exception as e:
                log.warning("Error closing connection %s: %s", name, e)

        conn.connector = None
        conn.pending = False
        conn.pending_sql = ""

        # If this was the current connection, clear it
        if self._current_name == name:
            self._current_name = ""

        log.info("Disconnected: %s (dirty=%s)", name, dirty)

    def select(self, name: str) -> Connection:
        """Switch to a named connection.

        This is the Python equivalent of select_connection() in mysqltest.cc.

        Args:
            name: The connection name to switch to.

        Returns:
            The selected Connection object.

        Raises:
            ConnectionError: If the connection name doesn't exist.
        """
        conn = self._connections.get(name)
        if not conn:
            raise ConnectionError(
                f"Connection '{name}' not found in connection pool")

        if conn.connector is None:
            raise ConnectionError(
                f"Connection '{name}' has been closed")

        self._current_name = name
        log.debug("Switched to connection: %s", name)
        return conn

    def send_quit(self, name: str) -> None:
        """Send a quit command to the named connection.

        This is the Python equivalent of do_send_quit() in mysqltest.cc.
        """
        conn = self._connections.get(name)
        if not conn:
            raise ConnectionError(f"Connection '{name}' not found")

        # Close the connection without waiting for response
        try:
            if self._default_connector and conn.connector:
                self._default_connector.close(conn.connector)
        except Exception:
            pass

        conn.connector = None
        conn.pending = False

    def change_user(self, user: str = "", password: str = "",
                    database: str = "", reconnect: bool = True) -> None:
        """Change the user on the current connection.

        This is the Python equivalent of do_change_user() in mysqltest.cc.
        For non-MySQL DBMS, this may need custom implementation.
        """
        conn = self.current
        if not conn:
            raise ConnectionError("No current connection")

        log.info("Change user on %s: user=%s db=%s", conn.name, user, database)
        # Implementation depends on DB connector capabilities

    def reset_connection(self) -> None:
        """Reset the current session.

        This is the Python equivalent of do_reset_connection() in mysqltest.cc.
        """
        conn = self.current
        if not conn:
            raise ConnectionError("No current connection")

        log.info("Reset connection: %s", conn.name)
        # Implementation depends on DB connector capabilities

    def ping(self) -> None:
        """Ping the current connection.

        This is the Python equivalent of Q_PING in mysqltest.cc.
        """
        conn = self.current
        if not conn:
            raise ConnectionError("No current connection")

        log.debug("Ping connection: %s", conn.name)

    def mark_send(self, sql: str) -> None:
        """Mark a query as sent (pending reap) on the current connection."""
        conn = self.current
        if conn:
            conn.pending = True
            conn.pending_sql = sql

    def reap(self) -> str:
        """Get the pending SQL from a send operation.

        Returns:
            The SQL that was previously sent.
        """
        conn = self.current
        if not conn:
            return ""
        sql = conn.pending_sql
        conn.pending = False
        conn.pending_sql = ""
        return sql

    def list_connections(self) -> List[str]:
        """List all connection names."""
        return list(self._connections.keys())

    def close_all(self) -> None:
        """Close all connections."""
        for name in list(self._connections.keys()):
            try:
                self.disconnect(name)
            except Exception:
                pass
