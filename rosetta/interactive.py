"""Interactive terminal session for Rosetta.

Allows users to repeatedly submit MTR test paths and execute them without
restarting the program.  Base parameters (config, dbms, baseline, etc.) are
fixed at launch; only the test file path changes between iterations.
"""

import glob
import http.server
import json
import logging
import os
import socket
import socketserver
import subprocess
import threading
import time as _time
from pathlib import Path
from typing import Dict, List, Optional

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import InMemoryHistory, FileHistory
from prompt_toolkit.styles import Style

from .config import DEFAULT_TEST_DB
from .models import DBMSConfig
from .reporter.history import generate_index_html


# ---------------------------------------------------------------------------
# Filtered file history – skip built-in commands (help, quit, back, …)
# ---------------------------------------------------------------------------

_SKIP_COMMANDS = frozenset({
    "help", "h", "back", "b", "quit", "q",
    "status", "s", "history", "clear", "retry", "r",
})


class _FilteredFileHistory(FileHistory):
    """FileHistory that ignores REPL built-in commands."""

    def append_string(self, string: str) -> None:
        if string.strip().lower() not in _SKIP_COMMANDS:
            super().append_string(string)


# ---------------------------------------------------------------------------
from .ui import (console, flush_all, print_error, print_info,
                 print_summary, print_warning)

log = logging.getLogger("rosetta")


# ---------------------------------------------------------------------------
# Path auto-completion
# ---------------------------------------------------------------------------

class TestFileCompleter(Completer):
    """Auto-complete .test file paths and directories."""

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor.strip()
        if not text:
            text = "./"
        expanded = os.path.expanduser(text)
        if os.path.isdir(expanded):
            if not expanded.endswith("/"):
                expanded += "/"
        pattern = expanded + "*"

        for path in sorted(glob.glob(pattern)):
            if os.path.isdir(path):
                yield Completion(path + "/", start_position=-len(text),
                                 display=os.path.basename(path) + "/",
                                 display_meta="dir")
            elif path.endswith(".test"):
                yield Completion(path, start_position=-len(text),
                                 display=os.path.basename(path),
                                 display_meta="test")


# ---------------------------------------------------------------------------
# Prompt style
# ---------------------------------------------------------------------------

_PROMPT_STYLE = Style.from_dict({
    "prompt": "bold cyan",
    "path": "bold white",
    "placeholder": "dim #888888",
})


# ---------------------------------------------------------------------------
# Left-arrow "back" key binding for REPL sessions
# ---------------------------------------------------------------------------

class _BackSignal:
    """Sentinel object returned by app.exit() when left-arrow triggers back."""
    pass

_BACK = _BackSignal()


def _make_back_bindings():
    """Create key bindings: left-arrow on empty input = type 'back' + submit."""
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.filters import Condition

    kb = KeyBindings()

    @kb.add("left", filter=Condition(
        lambda: not getattr(kb, '_app', None)
        or not kb._app.current_buffer.text))
    def _left_back(event):
        buf = event.app.current_buffer
        if not buf.text:
            event.app.exit(result=_BACK)

    # Store app reference for the filter
    return kb


def _make_repl_bindings():
    """Create key bindings for REPL: left-arrow on empty input triggers back."""
    from prompt_toolkit.key_binding import KeyBindings

    kb = KeyBindings()

    @kb.add("left")
    def _left_back(event):
        buf = event.app.current_buffer
        if not buf.text:
            event.app.exit(result=_BACK)
        else:
            # Normal left-arrow behavior when there is text
            buf.cursor_left()

    return kb


# ---------------------------------------------------------------------------
# HTTP server management
# ---------------------------------------------------------------------------

class _SilentHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Threaded HTTPServer that silently handles connection errors.

    Uses ThreadingMixIn so that long-running requests (like SSE streams)
    don't block other requests (like /api/stop).
    """
    daemon_threads = True
    request_queue_size = 128  # Default 5 is too low for repeated SSE requests

    def handle_error(self, request, client_address):
        """Silently ignore connection reset/broken pipe errors."""
        import sys
        import traceback
        exc_type = sys.exc_info()[0]
        # Ignore normal disconnect errors
        if exc_type in (ConnectionResetError, BrokenPipeError, OSError):
            return
        # Log unexpected errors to help debug issues
        log.error("Server error from %s:\n%s",
                  client_address, traceback.format_exc())


class _APIHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler with API endpoints and suppressed logging."""

    # Use HTTP/1.1 to support persistent connections and chunked transfer,
    # which is critical for SSE streams over SSH tunnels / proxies.
    protocol_version = "HTTP/1.1"

    # Class-level reference set by ReportServer before creating instances.
    _configs: List[DBMSConfig] = []
    _all_configs: List[DBMSConfig] = []
    _database: str = ""
    _baseline: str = ""
    _traceless: bool = True
    # Cancellation event — set by /api/stop, cleared when a new execution starts
    _cancel_event: threading.Event = threading.Event()
    # Active DB connections that can be killed on stop
    _active_connections: list = []
    _active_connections_lock: threading.Lock = threading.Lock()

    @classmethod
    def _cleanup_connections(cls):
        """Force-close and clear all remaining active connections."""
        with cls._active_connections_lock:
            for db in cls._active_connections:
                try:
                    db.close()
                except Exception:
                    pass
            cls._active_connections.clear()

    def log_message(self, format, *args):  # noqa: A002
        pass  # Suppress all request logs

    def end_headers(self):                      # noqa: N802
        # Disable caching for all responses
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    # -- GET routing (redirect / → /index.html, serve API) -----------------

    def do_GET(self):                           # noqa: N802
        if self.path == "/":
            self.send_response(302)
            self.send_header("Location", "/index.html")
            self.end_headers()
            return
        if self.path == "/api/dbms":
            self._handle_dbms_list()
            return
        super().do_GET()

    # -- CORS ---------------------------------------------------------------

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods",
                         "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):                       # noqa: N802
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    # -- API routing --------------------------------------------------------

    def do_POST(self):                          # noqa: N802
        if self.path == "/api/execute":
            self._handle_execute_api()
        elif self.path == "/api/execute/stream":
            self._handle_execute_stream_api()
        elif self.path == "/api/stop":
            self._handle_stop_api()
        elif self.path == "/api/runs/delete":
            self._handle_runs_delete_api()
        else:
            self.send_error(404)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b"{}"
        return json.loads(body)

    def _respond_json(self, data: dict, status: int = 200):
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._send_cors_headers()
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    # -- Runs delete API ----------------------------------------------------

    def _handle_runs_delete_api(self):
        """POST /api/runs/delete — delete a run directory.

        Request body: {"dir_name": "test_name_20250101_120000"}
        Response: {"ok": true} or {"ok": false, "error": "..."}
        """
        import shutil

        try:
            body = self._read_json()
        except Exception:
            self._respond_json({"ok": False, "error": "invalid JSON"}, 400)
            return

        dir_name = body.get("dir_name", "")
        if not dir_name:
            self._respond_json({"ok": False, "error": "dir_name required"}, 400)
            return

        # Security: prevent path traversal
        if ".." in dir_name or "/" in dir_name or "\\" in dir_name:
            self._respond_json({"ok": False, "error": "invalid dir_name"}, 400)
            return

        # Get the serving directory (output_dir)
        # The handler is created with directory= output_dir
        target_dir = os.path.join(self.directory, dir_name)

        if not os.path.isdir(target_dir):
            self._respond_json({"ok": False, "error": "directory not found"}, 404)
            return

        try:
            shutil.rmtree(target_dir)
            log.info("Deleted run directory: %s", target_dir)
            # Regenerate index.html after deletion
            from .reporter.history import generate_index_html
            generate_index_html(self.directory)
            self._respond_json({"ok": True})
        except Exception as e:
            log.error("Failed to delete directory %s: %s", target_dir, e)
            self._respond_json({"ok": False, "error": str(e)}, 500)

    def _handle_stop_api(self):
        """POST /api/stop — cancel the currently running execution.

        Sets the cancel event AND forcibly closes all active DB connections
        so that any blocking cursor.execute() is interrupted immediately.
        """
        self._cancel_event.set()
        self._cleanup_connections()
        log.info("Execution stop requested via /api/stop")
        self._respond_json({"ok": True, "message": "Execution cancelled"})

    # -- Playground API -----------------------------------------------------

    def _handle_dbms_list(self):
        """GET /api/dbms — return all DBMS from config with active flags."""
        active_names = {c.name for c in self._configs}
        dbms_list = [{"name": c.name, "host": c.host, "port": c.port,
                      "active": c.name in active_names}
                     for c in self._all_configs]
        self._respond_json({
            "ok": True,
            "database": self._database,
            "baseline": self._baseline,
            "traceless": self._traceless,
            "dbms": dbms_list,
        })

    def _handle_execute_api(self):
        """POST /api/execute — execute SQL on selected DBMS targets.

        Request body: {"sql": "...", "dbms": ["tdsql", "mysql"]}
        Response: {"ok": true, "results": {"tdsql": {...}, "mysql": {...}}}
        """
        import concurrent.futures

        from .executor import DBConnection
        from .explain import is_explain_stmt, get_explain_variants

        # Kill any lingering execution from a previous request
        self._cancel_event.set()
        self._cleanup_connections()
        self._cancel_event.clear()

        try:
            body = self._read_json()
        except Exception:
            self._respond_json({"ok": False, "error": "invalid JSON"}, 400)
            return

        sql_text = body.get("sql", "").strip()
        if not sql_text:
            self._respond_json({"ok": False, "error": "sql is required"}, 400)
            return

        requested_dbms = body.get("dbms", [])
        sandbox = body.get("sandbox", True)
        configs_map = {c.name: c for c in self._all_configs}

        if not requested_dbms:
            requested_dbms = list(configs_map.keys())

        targets = []
        for name in requested_dbms:
            if name in configs_map:
                targets.append(configs_map[name])

        if not targets:
            self._respond_json(
                {"ok": False, "error": "no valid DBMS targets"}, 400)
            return

        database = self._database
        cancel = self._cancel_event

        # Use the new MTR parser to extract SQL statements,
        # supporting full MTR syntax (variables, conditionals, etc.)
        from .mtr import MtrParser
        from .mtr.nodes import MtrCommandType
        _mtr_parser = MtrParser("<playground>")
        try:
            _mtr_test = _mtr_parser.parse_text(sql_text)
        except Exception as e:
            self._respond_json(
                {"ok": False, "error": f"SQL parse error: {e}"}, 400)
            return
        stmts = [cmd.argument for cmd in _mtr_test.commands
                 if cmd.cmd_type in (MtrCommandType.SQL, MtrCommandType.EVAL,
                                     MtrCommandType.QUERY,
                                     MtrCommandType.QUERY_VERTICAL,
                                     MtrCommandType.QUERY_HORIZONTAL)]

        def _exec_on_dbms(config):
            """Execute all statements on one DBMS, return result dict."""
            import uuid

            result = {
                "name": config.name,
                "statements": [],
                "error": None,
                "cancelled": False,
            }

            # Sandbox mode: create a temp DB for isolation (skip for Oracle)
            use_sandbox = sandbox and config.protocol != "oracle"
            if use_sandbox:
                temp_db = f"_rosetta_sandbox_{uuid.uuid4().hex[:8]}"
            else:
                temp_db = None

            target_db = temp_db if use_sandbox else database
            db = DBConnection(config, target_db)
            with self._active_connections_lock:
                self._active_connections.append(db)
            try:
                db.connect()
            except Exception as e:
                with self._active_connections_lock:
                    try:
                        self._active_connections.remove(db)
                    except ValueError:
                        pass
                if cancel.is_set():
                    result["cancelled"] = True
                    return result
                result["error"] = f"Connection failed: {e}"
                return result

            try:
                for sql in stmts:
                    if cancel.is_set():
                        result["cancelled"] = True
                        break
                    stmt_result = {"sql": sql, "columns": None,
                                   "rows": None, "error": None,
                                   "affected_rows": 0,
                                   "elapsed_ms": 0}

                    # Multi-format EXPLAIN handling
                    if is_explain_stmt(sql):
                        variants = get_explain_variants(sql, config.protocol)
                        explain_results = []
                        total_elapsed = 0.0
                        for variant in variants:
                            if cancel.is_set():
                                result["cancelled"] = True
                                break
                            vr = {"format": variant["format"],
                                  "columns": None, "rows": None,
                                  "error": None, "elapsed_ms": 0}
                            try:
                                t0 = _time.monotonic()
                                db.cursor.execute(variant["sql"])
                                if db.cursor.description:
                                    vr["columns"] = [
                                        desc[0]
                                        for desc in db.cursor.description
                                    ]
                                    rows = db.cursor.fetchall()
                                    vr["rows"] = [
                                        [_format_val(c) for c in row]
                                        for row in rows
                                    ]
                                vr["elapsed_ms"] = round(
                                    (_time.monotonic() - t0) * 1000, 3)
                            except Exception as e:
                                vr["elapsed_ms"] = round(
                                    (_time.monotonic() - t0) * 1000, 3)
                                vr["error"] = str(e)
                            total_elapsed += vr["elapsed_ms"]
                            explain_results.append(vr)

                        # Use first successful variant as main result
                        for vr in explain_results:
                            if not vr["error"]:
                                stmt_result["columns"] = vr["columns"]
                                stmt_result["rows"] = vr["rows"]
                                break
                        stmt_result["elapsed_ms"] = round(total_elapsed, 3)
                        stmt_result["explain_formats"] = explain_results
                    else:
                        try:
                            t0 = _time.monotonic()
                            db.cursor.execute(sql)
                            if db.cursor.description:
                                stmt_result["columns"] = [
                                    desc[0]
                                    for desc in db.cursor.description
                                ]
                                rows = db.cursor.fetchall()
                                # Convert to serializable format
                                stmt_result["rows"] = [
                                    [_format_val(c) for c in row]
                                    for row in rows
                                ]
                            else:
                                stmt_result["affected_rows"] = (
                                    db.cursor.rowcount or 0)
                            t1 = _time.monotonic()
                            stmt_result["elapsed_ms"] = round(
                                (t1 - t0) * 1000, 3)
                        except Exception as e:
                            t1 = _time.monotonic()
                            if cancel.is_set():
                                result["cancelled"] = True
                                break
                            stmt_result["error"] = str(e)
                            # Extract error code if available (e.g., MySQL error code)
                            # Most DB-API exceptions have error code in args[0]
                            error_code = None
                            if hasattr(e, 'args') and e.args and isinstance(e.args[0], int):
                                error_code = e.args[0]
                            elif hasattr(e, 'errno'):
                                error_code = getattr(e, 'errno')
                            stmt_result["error_code"] = error_code
                            stmt_result["elapsed_ms"] = round(
                                (t1 - t0) * 1000, 3)

                    result["statements"].append(stmt_result)
            finally:
                # Sandbox cleanup: drop temp database
                if use_sandbox and temp_db:
                    try:
                        db.cursor.execute(
                            f"DROP DATABASE IF EXISTS `{temp_db}`")
                    except Exception:
                        pass
                with self._active_connections_lock:
                    try:
                        self._active_connections.remove(db)
                    except ValueError:
                        pass
                try:
                    db.close()
                except Exception:
                    pass

            return result

        # Execute in parallel across all DBMS targets
        # Watchdog: auto-kill if total execution exceeds timeout
        exec_timeout = max(30, len(stmts) * 30 + 15)
        watchdog_stop = threading.Event()

        def _watchdog():
            if watchdog_stop.wait(timeout=exec_timeout):
                return
            if not cancel.is_set():
                log.warning("Playground execution watchdog timeout "
                            "(%ds), force killing", exec_timeout)
                cancel.set()
                self._cleanup_connections()

        wd_thread = threading.Thread(target=_watchdog, daemon=True)
        wd_thread.start()

        results = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=len(targets)) as pool:
                futures = {pool.submit(_exec_on_dbms, c): c for c in targets}
                for fut in concurrent.futures.as_completed(futures):
                    r = fut.result()
                    results[r["name"]] = r
        finally:
            watchdog_stop.set()
            self._cleanup_connections()

        cancelled = any(r.get("cancelled") for r in results.values())
        self._respond_json({"ok": True, "results": results,
                            "cancelled": cancelled})

    def _handle_execute_stream_api(self):
        """POST /api/execute/stream — execute SQL on selected DBMS targets
        with Server-Sent Events progress updates.

        Request body: {"sql": "...", "dbms": ["tdsql", "mysql"]}
        SSE events:
          - event: stmt_progress  data: {"name": "...", "index": N, "total": N, "stmt_index": N, "stmt_total": N}
          - event: progress  data: {"name": "...", "index": N, "total": N, "result": {...}}
          - event: done      data: {"ok": true}
          - event: cancelled data: {"ok": false, "message": "Execution cancelled by user"}
          - event: error     data: {"error": "..."}
        """
        import concurrent.futures

        from .executor import DBConnection
        from .explain import is_explain_stmt, get_explain_variants

        # Kill any lingering execution from a previous request
        self._cancel_event.set()
        self._cleanup_connections()
        # Now reset for the new execution
        self._cancel_event.clear()

        try:
            body = self._read_json()
        except Exception:
            self._respond_json({"ok": False, "error": "invalid JSON"}, 400)
            return

        sql_text = body.get("sql", "").strip()
        if not sql_text:
            self._respond_json({"ok": False, "error": "sql is required"}, 400)
            return

        requested_dbms = body.get("dbms", [])
        sandbox = body.get("sandbox", True)
        configs_map = {c.name: c for c in self._all_configs}

        if not requested_dbms:
            requested_dbms = list(configs_map.keys())

        targets = []
        for name in requested_dbms:
            if name in configs_map:
                targets.append(configs_map[name])

        if not targets:
            self._respond_json(
                {"ok": False, "error": "no valid DBMS targets"}, 400)
            return

        database = self._database
        cancel = self._cancel_event

        from .mtr import MtrParser
        from .mtr.nodes import MtrCommandType
        _mtr_parser = MtrParser("<playground>")
        try:
            _mtr_test = _mtr_parser.parse_text(sql_text)
        except Exception as e:
            self._respond_json(
                {"ok": False, "error": f"SQL parse error: {e}"}, 400)
            return
        stmts = [cmd.argument for cmd in _mtr_test.commands
                 if cmd.cmd_type in (MtrCommandType.SQL, MtrCommandType.EVAL,
                                     MtrCommandType.QUERY,
                                     MtrCommandType.QUERY_VERTICAL,
                                     MtrCommandType.QUERY_HORIZONTAL)]
        total = len(targets)

        # Set up SSE response — use raw socket to avoid buffered wfile issues
        # that cause connection leaks under ThreadingMixIn.
        try:
            raw_sock = self.connection
            raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            raw_sock = None

        # Send HTTP response headers via the normal mechanism
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self._send_cors_headers()
        self.end_headers()
        self.wfile.flush()

        # Send an immediate SSE comment as heartbeat so the client receives
        # data right away (critical for SSH tunnels / proxies that may close
        # idle connections).
        try:
            heartbeat = b": heartbeat\n\n"
            if raw_sock:
                raw_sock.sendall(heartbeat)
            else:
                self.wfile.write(heartbeat)
                self.wfile.flush()
        except Exception:
            return  # Client already gone

        sse_lock = threading.Lock()
        _client_gone = False

        def _send_sse(event: str, data: dict):
            """Send a single SSE event to the client (thread-safe)."""
            nonlocal _client_gone
            with sse_lock:
                if _client_gone:
                    return
                try:
                    payload = json.dumps(data, ensure_ascii=False)
                    msg = f"event: {event}\ndata: {payload}\n\n".encode("utf-8")
                    if raw_sock:
                        raw_sock.sendall(msg)
                    else:
                        self.wfile.write(msg)
                        self.wfile.flush()
                except Exception:
                    _client_gone = True
                    # Client disconnected — cancel execution
                    if not cancel.is_set():
                        cancel.set()
                        self._cleanup_connections()

        def _exec_on_dbms(config, index):
            """Execute all statements on one DBMS, return result dict."""
            import uuid

            result = {
                "name": config.name,
                "statements": [],
                "error": None,
                "cancelled": False,
            }

            if cancel.is_set():
                result["cancelled"] = True
                _send_sse("progress", {
                    "name": config.name,
                    "index": index,
                    "total": total,
                    "result": result,
                })
                return result

            # Sandbox mode: create a temp DB for isolation (skip for Oracle)
            use_sandbox = sandbox and config.protocol != "oracle"
            if use_sandbox:
                temp_db = f"_rosetta_sandbox_{uuid.uuid4().hex[:8]}"
            else:
                temp_db = None

            target_db = temp_db if use_sandbox else database
            db = DBConnection(config, target_db)
            # Register connection BEFORE connect so /api/stop can kill it
            # even if connect() itself is blocking
            with self._active_connections_lock:
                self._active_connections.append(db)
            try:
                db.connect()
            except Exception as e:
                with self._active_connections_lock:
                    try:
                        self._active_connections.remove(db)
                    except ValueError:
                        pass
                if cancel.is_set():
                    result["cancelled"] = True
                    _send_sse("progress", {
                        "name": config.name,
                        "index": index,
                        "total": total,
                        "result": result,
                    })
                    return result
                result["error"] = f"Connection failed: {e}"
                _send_sse("progress", {
                    "name": config.name,
                    "index": index,
                    "total": total,
                    "result": result,
                })
                return result

            try:
                total_stmts = len(stmts)
                for si, sql in enumerate(stmts):
                    if cancel.is_set():
                        result["cancelled"] = True
                        break
                    stmt_result = {"sql": sql, "columns": None,
                                   "rows": None, "error": None,
                                   "affected_rows": 0,
                                   "elapsed_ms": 0}

                    # Multi-format EXPLAIN handling
                    if is_explain_stmt(sql):
                        variants = get_explain_variants(sql, config.protocol)
                        explain_results = []
                        total_elapsed = 0.0
                        for variant in variants:
                            if cancel.is_set():
                                result["cancelled"] = True
                                break
                            vr = {"format": variant["format"],
                                  "columns": None, "rows": None,
                                  "error": None, "elapsed_ms": 0}
                            try:
                                t0 = _time.monotonic()
                                db.cursor.execute(variant["sql"])
                                if db.cursor.description:
                                    vr["columns"] = [
                                        desc[0]
                                        for desc in db.cursor.description
                                    ]
                                    rows = db.cursor.fetchall()
                                    vr["rows"] = [
                                        [_format_val(c) for c in row]
                                        for row in rows
                                    ]
                                vr["elapsed_ms"] = round(
                                    (_time.monotonic() - t0) * 1000, 3)
                            except Exception as e:
                                vr["elapsed_ms"] = round(
                                    (_time.monotonic() - t0) * 1000, 3)
                                vr["error"] = str(e)
                            total_elapsed += vr["elapsed_ms"]
                            explain_results.append(vr)

                        # Use first successful variant as main result
                        for vr in explain_results:
                            if not vr["error"]:
                                stmt_result["columns"] = vr["columns"]
                                stmt_result["rows"] = vr["rows"]
                                break
                        stmt_result["elapsed_ms"] = round(total_elapsed, 3)
                        stmt_result["explain_formats"] = explain_results
                    else:
                        try:
                            t0 = _time.monotonic()
                            db.cursor.execute(sql)
                            if db.cursor.description:
                                stmt_result["columns"] = [
                                    desc[0]
                                    for desc in db.cursor.description
                                ]
                                rows = db.cursor.fetchall()
                                stmt_result["rows"] = [
                                    [_format_val(c) for c in row]
                                    for row in rows
                                ]
                            else:
                                stmt_result["affected_rows"] = (
                                    db.cursor.rowcount or 0)
                            t1 = _time.monotonic()
                            stmt_result["elapsed_ms"] = round(
                                (t1 - t0) * 1000, 3)
                        except Exception as e:
                            t1 = _time.monotonic()
                            # If cancelled, mark as cancelled rather than error
                            if cancel.is_set():
                                result["cancelled"] = True
                                break
                            stmt_result["error"] = str(e)
                            error_code = None
                            if hasattr(e, 'args') and e.args and isinstance(e.args[0], int):
                                error_code = e.args[0]
                            elif hasattr(e, 'errno'):
                                error_code = getattr(e, 'errno')
                            stmt_result["error_code"] = error_code
                            stmt_result["elapsed_ms"] = round(
                                (t1 - t0) * 1000, 3)

                    result["statements"].append(stmt_result)
                    # Send per-statement progress update
                    _send_sse("stmt_progress", {
                        "name": config.name,
                        "index": index,
                        "total": total,
                        "stmt_index": si + 1,
                        "stmt_total": total_stmts,
                    })
            finally:
                # Sandbox cleanup: drop temp database
                if use_sandbox and temp_db:
                    try:
                        db.cursor.execute(
                            f"DROP DATABASE IF EXISTS `{temp_db}`")
                    except Exception:
                        pass
                # Unregister and close
                with self._active_connections_lock:
                    try:
                        self._active_connections.remove(db)
                    except ValueError:
                        pass
                try:
                    db.close()
                except Exception:
                    pass

            _send_sse("progress", {
                "name": config.name,
                "index": index,
                "total": total,
                "result": result,
            })
            return result

        # Execute in parallel across all DBMS targets
        # Watchdog: auto-kill if total execution exceeds timeout
        exec_timeout = max(30, len(stmts) * 30 + 15)  # 30s per stmt + 15s connect
        watchdog_stop = threading.Event()

        def _watchdog():
            """Kill everything if execution stalls beyond timeout."""
            if watchdog_stop.wait(timeout=exec_timeout):
                return  # Normal completion — stop was signalled
            # Timeout reached — force cancel
            if not cancel.is_set():
                log.warning("Playground execution watchdog timeout "
                            "(%ds), force killing", exec_timeout)
                cancel.set()
                self._cleanup_connections()
                _send_sse("cancelled", {
                    "ok": False,
                    "message": f"Execution timed out after {exec_timeout}s"
                })

        wd_thread = threading.Thread(target=_watchdog, daemon=True)
        wd_thread.start()

        try:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=len(targets)) as pool:
                futures = {}
                for i, c in enumerate(targets):
                    futures[pool.submit(_exec_on_dbms, c, i + 1)] = c
                for fut in concurrent.futures.as_completed(futures):
                    fut.result()  # propagate exceptions if any

            if cancel.is_set():
                _send_sse("cancelled",
                          {"ok": False, "message": "Execution cancelled"})
            else:
                _send_sse("done", {"ok": True})
        except Exception as e:
            _send_sse("error", {"error": str(e)})
        finally:
            watchdog_stop.set()  # Tell watchdog to stop
            # Ensure all DB connections are cleaned up
            self._cleanup_connections()
            # SSE stream is done — close connection (HTTP/1.1 would try reuse)
            self.close_connection = True


def _format_val(value) -> str:
    """Format a cell value for JSON serialisation."""
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


class ReportServer:
    """Manages a background HTTP server for viewing HTML reports."""

    def __init__(self, directory: str, port: int = 0,
                 configs: Optional[List[DBMSConfig]] = None,
                 all_configs: Optional[List[DBMSConfig]] = None,
                 database: str = "",
                 baseline: str = "",
                 traceless: bool = True):
        self.directory = os.path.abspath(directory)
        self.port = port
        self.configs = configs or []
        self.all_configs = all_configs or self.configs
        self.database = database
        self.baseline = baseline
        self.traceless = traceless
        self.baseline = baseline
        self._server: Optional[http.server.HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def base_url(self) -> str:
        return f"http://localhost:{self.port}"

    def start(self) -> str:
        """Start the server and return the base URL."""
        if self.running:
            return self.base_url
        if self.port == 0:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", 0))
                self.port = s.getsockname()[1]
        os.makedirs(self.directory, exist_ok=True)
        # Pre-generate index and playground pages so / redirects work
        from .reporter.history import (generate_index_html,
                                       generate_playground_html)
        generate_index_html(self.directory)
        generate_playground_html(self.directory)
        directory = self.directory
        # Inject references into handler class
        _APIHandler._configs = self.configs
        _APIHandler._all_configs = self.all_configs
        _APIHandler._database = self.database
        _APIHandler._baseline = self.baseline
        _APIHandler._traceless = self.traceless
        handler = lambda *a, **kw: _APIHandler(
            *a, directory=directory, **kw)
        self._server = _SilentHTTPServer(
            ("0.0.0.0", self.port), handler)
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        daemon=True)
        self._thread.start()
        return self.base_url

    def stop(self):
        if self._server:
            t = threading.Thread(target=self._server.shutdown, daemon=True)
            t.start()
            t.join(timeout=3)
            # Close the listening socket so the port is released immediately.
            # shutdown() only stops serve_forever(); without server_close()
            # the socket stays open and the port remains occupied.
            try:
                self._server.server_close()
            except Exception:
                pass
            self._server = None
            self._thread = None


# ---------------------------------------------------------------------------
# Interactive session
# ---------------------------------------------------------------------------

class InteractiveSession:
    """Interactive REPL that accepts repeated test file submissions."""

    COMMANDS = {
        "help":    "Show available commands",
        "status":  "Show current configuration",
        "history": "Show executed tests in this session",
        "server":  "Show report server URL",
        "open":    "Open latest HTML report in IDE",
        "clear":   "Clear the screen",
        "back":    "Back to mode selection (also: b)",
        "quit":    "Exit (also: exit, q)",
    }

    def __init__(self, configs: List[DBMSConfig], output_dir: str,
                 database: str = DEFAULT_TEST_DB,
                 baseline: Optional[str] = None,
                 skip_explain: bool = True,
                 skip_analyze: bool = True,
                 skip_show_create: bool = True,
                 output_format: str = "all",
                 serve: bool = False, port: int = 19527,
                 all_configs: Optional[List[DBMSConfig]] = None,
                 report_server: Optional[ReportServer] = None):
        self.configs = configs
        self.all_configs = all_configs or configs
        self.output_dir = os.path.abspath(output_dir)
        self.database = database
        self.baseline = baseline
        self.skip_explain = skip_explain
        self.skip_analyze = skip_analyze
        self.skip_show_create = skip_show_create
        self.output_format = output_format
        self.serve = serve
        self.port = port
        self._run_history: List[Dict] = []
        self._report_server: Optional[ReportServer] = report_server


    # -- server helpers -----------------------------------------------------

    def _ensure_server(self) -> Optional[ReportServer]:
        # Reuse an externally-provided server that is already running
        if self._report_server and self._report_server.running:
            return self._report_server
        if not self.serve:
            return None
        # Stop previous server if it exists but is no longer running
        if self._report_server:
            self._report_server.stop()
        self._report_server = ReportServer(self.output_dir, self.port,
                                           configs=self.configs,
                                           all_configs=self.all_configs,
                                           database=self.database)
        try:
            self._report_server.start()
            return self._report_server
        except OSError as e:
            console.print(f"  [red]✗[/red] Server failed: {e}")
            return None

    def _open_in_ide(self, url: str):
        try:
            subprocess.Popen(["code", "--open-url", url],
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        except FileNotFoundError:
            pass

    # -- test execution -----------------------------------------------------

    def _run_test(self, test_file: str) -> bool:
        from .runner import RosettaRunner

        if not os.path.isfile(test_file):
            print_error(f"Test file not found: {test_file}")
            flush_all()
            return False

        run_stamp = _time.strftime("%Y%m%d_%H%M%S")
        test_name = Path(test_file).stem
        run_dir = os.path.join(self.output_dir, f"{test_name}_{run_stamp}")

        print_info("DBMS targets:",
                   ", ".join(c.name for c in self.configs))

        runner = RosettaRunner(
            test_file=test_file, configs=self.configs,
            output_dir=run_dir, database=self.database,
            baseline=self.baseline, skip_explain=self.skip_explain,
            skip_analyze=self.skip_analyze,
            skip_show_create=self.skip_show_create,
            output_format=self.output_format)

        comparisons = runner.run()

        if not comparisons:
            flush_all()
            self._run_history.append({
                "test": test_file, "time": _time.strftime("%H:%M:%S"),
                "status": "FAIL", "run_dir": run_dir})
            return False

        # Update 'latest' symlink
        latest_link = os.path.join(self.output_dir, "latest")
        try:
            if os.path.islink(latest_link):
                os.remove(latest_link)
            os.symlink(os.path.basename(run_dir), latest_link)
        except OSError:
            pass

        generate_index_html(self.output_dir)

        all_pass = print_summary(comparisons, runner.failed_connections)
        flush_all()

        passed = all_pass and not runner.failed_connections
        self._run_history.append({
            "test": test_file, "time": _time.strftime("%H:%M:%S"),
            "status": "PASS" if passed else "FAIL", "run_dir": run_dir})

        # Open in browser
        srv = self._ensure_server()
        if srv:
            html_file = f"{test_name}.html"
            html_path = os.path.join(run_dir, html_file)
            if os.path.isfile(html_path):
                url = (f"{srv.base_url}"
                       f"/{os.path.basename(run_dir)}/{html_file}")
                console.print(
                    f"\n  [cyan]📊 Report:[/cyan] "
                    f"[bold link={url}]{url}[/bold link]\n")
                self._open_in_ide(url)

        return passed

    # -- command handlers ---------------------------------------------------

    def _cmd_help(self):
        console.print("\n  [bold cyan]Available commands:[/bold cyan]")
        for cmd, desc in self.COMMANDS.items():
            console.print(f"    [bold]{cmd:10s}[/bold] {desc}")
        console.print(
            "\n  Or enter a [bold].test[/bold] file path to execute.\n")

    def _cmd_status(self):
        console.print(f"\n  [cyan]Config:[/cyan]")
        console.print(
            f"    DBMS:     "
            f"[bold]{', '.join(c.name for c in self.configs)}[/bold]")
        console.print(f"    Baseline: [bold]{self.baseline or 'none'}[/bold]")
        console.print(f"    Database: [bold]{self.database}[/bold]")
        console.print(f"    Output:   [bold]{self.output_dir}[/bold]")
        console.print(f"    Format:   [bold]{self.output_format}[/bold]")
        console.print(f"    Runs:     [bold]{len(self._run_history)}[/bold]")
        if self._report_server and self._report_server.running:
            console.print(
                f"    Server:   "
                f"[bold green]{self._report_server.base_url}[/bold green]")
        console.print()

    def _cmd_history(self):
        if not self._run_history:
            console.print("\n  [dim]No tests executed yet.[/dim]\n")
            return
        console.print(f"\n  [bold cyan]Session history "
                      f"({len(self._run_history)} runs):[/bold cyan]")
        for i, entry in enumerate(self._run_history, 1):
            status_style = ("green" if entry["status"] == "PASS"
                            else "red")
            console.print(
                f"    {i:3d}. [{status_style}]{entry['status']:4s}"
                f"[/{status_style}]  "
                f"[dim]{entry['time']}[/dim]  {entry['test']}")
        console.print()

    def _cmd_server(self):
        srv = self._ensure_server()
        if srv and srv.running:
            idx_url = f"{srv.base_url}/index.html"
            console.print(
                f"\n  [green]●[/green] Server running: "
                f"[bold link={idx_url}]{idx_url}[/bold link]\n")
        else:
            console.print("\n  [dim]Server not running "
                          "(use --serve to enable).[/dim]\n")

    def _cmd_open(self):
        latest = os.path.join(self.output_dir, "latest")
        if not os.path.islink(latest):
            console.print("\n  [dim]No results yet.[/dim]\n")
            return
        real_dir = os.path.realpath(latest)
        htmls = [f for f in os.listdir(real_dir) if f.endswith(".html")]
        if not htmls:
            console.print("\n  [dim]No HTML report found.[/dim]\n")
            return
        srv = self._ensure_server()
        if not srv:
            console.print("\n  [dim]Server not available.[/dim]\n")
            return
        url = (f"{srv.base_url}"
               f"/{os.path.basename(real_dir)}/{htmls[0]}")
        console.print(f"\n  Opening: [bold]{url}[/bold]\n")
        self._open_in_ide(url)

    # -- main loop ----------------------------------------------------------

    def run(self):
        """Start the interactive REPL.

        Returns ``"back"`` if the user typed ``back``/``b``,
        ``"quit"`` otherwise (including EOF / KeyboardInterrupt).
        """
        os.makedirs(self.output_dir, exist_ok=True)
        session: PromptSession = PromptSession(
            history=_FilteredFileHistory(os.path.join(self.output_dir, ".rosetta_history")),
            completer=TestFileCompleter(),
            style=_PROMPT_STYLE,
            complete_while_typing=True,
            multiline=False,
            key_bindings=_make_repl_bindings(),
        )

        _placeholder = HTML('<placeholder>Type a path, \'help\', ← back, or \'quit\'</placeholder>')

        # ASCII Logo banner
        from .ui import LOGO_LINES, LOGO_SUBTITLE
        from . import __version__
        console.print()
        for logo_line in LOGO_LINES:
            console.print(f"  [bold cyan]{logo_line}[/bold cyan]")
        console.print()
        console.print(f"  [dim]{LOGO_SUBTITLE}[/dim]")
        console.print(f"  [dim]v{__version__}[/dim]  [bold white]Test Mode[/bold white]")
        console.print()

        # Show status
        console.print(
            f"  [dim]DBMS:[/dim] "
            f"[bold]{', '.join(c.name for c in self.configs)}[/bold]  "
            f"[dim]Baseline:[/dim] "
            f"[bold]{self.baseline or 'auto'}[/bold]  "
            f"[dim]Database:[/dim] [bold]{self.database}[/bold]")

        # Start server early if requested
        srv = self._ensure_server()
        if srv and srv.running:
            console.print(
                f"  [dim]Server:[/dim] "
                f"[bold green]{srv.base_url}/index.html[/bold green]")
        console.print()

        run_count = 0
        exit_reason = "quit"

        while True:
            try:
                prompt_msg = HTML(
                    '<prompt>rosetta</prompt> <path>▶</path> ')
                user_input = session.prompt(
                    prompt_msg, placeholder=_placeholder)
            except (EOFError, KeyboardInterrupt):
                break

            if isinstance(user_input, _BackSignal):
                exit_reason = "back"
                break

            user_input = user_input.strip()
            if not user_input:
                continue

            cmd = user_input.lower()

            # Back to mode selection
            if cmd in ("back", "b"):
                exit_reason = "back"
                break

            # Exit commands
            if cmd in ("quit", "exit", "q"):
                break

            # Built-in commands
            if cmd == "help":
                self._cmd_help()
                continue
            if cmd == "status":
                self._cmd_status()
                continue
            if cmd == "history":
                self._cmd_history()
                continue
            if cmd == "server":
                self._cmd_server()
                continue
            if cmd == "open":
                self._cmd_open()
                continue
            if cmd == "clear":
                console.clear()
                continue

            # Treat as file path
            test_path = os.path.expanduser(user_input)
            if not os.path.isabs(test_path):
                test_path = os.path.abspath(test_path)

            run_count += 1
            console.print()
            console.rule(
                f"[bold cyan] Run #{run_count}: "
                f"{os.path.basename(test_path)} [/bold cyan]")
            console.print()

            self._run_test(test_path)

            console.print(
                "  [dim]Ready for next test. "
                "Type a path, 'help', 'back', or 'quit'.[/dim]\n")

        # Cleanup
        if exit_reason == "back":
            if self._report_server:
                self._report_server.stop()
        else:
            console.print()
            if self._run_history:
                console.print(
                    f"  [dim]Session complete: "
                    f"{len(self._run_history)} test(s) executed.[/dim]")
            if self._report_server:
                self._report_server.stop()
                console.print("  [dim]Report server stopped.[/dim]")
            console.print("  [bold cyan]Goodbye! 👋[/bold cyan]\n")

        return exit_reason


# ---------------------------------------------------------------------------
# Benchmark file auto-completion
# ---------------------------------------------------------------------------

class BenchFileCompleter(Completer):
    """Auto-complete .json / .sql benchmark file paths and directories."""

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor.strip()
        if not text:
            text = "./"
        expanded = os.path.expanduser(text)
        if os.path.isdir(expanded):
            if not expanded.endswith("/"):
                expanded += "/"
        pattern = expanded + "*"

        for path in sorted(glob.glob(pattern)):
            if os.path.isdir(path):
                yield Completion(path + "/", start_position=-len(text),
                                 display=os.path.basename(path) + "/",
                                 display_meta="dir")
            elif path.endswith(".json") or path.endswith(".sql"):
                yield Completion(path, start_position=-len(text),
                                 display=os.path.basename(path),
                                 display_meta="bench")


# ---------------------------------------------------------------------------
# Benchmark interactive session
# ---------------------------------------------------------------------------

class BenchInteractiveSession:
    """Interactive REPL for benchmark mode.

    Base parameters (config, dbms, iterations, warmup, concurrency, etc.)
    are fixed at launch; only the bench file path changes between runs.
    """

    COMMANDS = {
        "help":    "Show available commands",
        "status":  "Show current configuration",
        "history": "Show executed benchmarks in this session",
        "server":  "Show report server URL",
        "open":    "Open latest HTML report in IDE",
        "clear":   "Clear the screen",
        "back":    "Back to parameter selection (also: b)",
        "quit":    "Exit (also: exit, q)",
    }

    def __init__(self, configs: List[DBMSConfig], output_dir: str,
                 database: str = DEFAULT_TEST_DB,
                 iterations: int = 100,
                 warmup: int = 5,
                 concurrency: int = 0,
                 duration: float = 30.0,
                 ramp_up: float = 0.0,
                 bench_filter: Optional[str] = None,
                 repeat: int = 1,
                 parallel_dbms: bool = True,
                 output_format: str = "all",
                 serve: bool = False,
                 port: int = 19527,
                 profile: bool = False,
                 perf_freq: int = 99,
                 query_timeout: int = 5,
                 flamegraph_min_ms: int = 1000,
                 bench_mode: str = "serial",
                 report_server: Optional[ReportServer] = None):
        self.configs = configs
        self.output_dir = os.path.abspath(output_dir)
        self.database = database
        self.iterations = iterations
        self.warmup = warmup
        self.concurrency = concurrency
        self.duration = duration
        self.ramp_up = ramp_up
        self.bench_filter = bench_filter
        self.repeat = max(1, repeat)
        self.parallel_dbms = parallel_dbms
        self.output_format = output_format
        self.serve = serve
        self.port = port
        self.profile = profile
        self.perf_freq = perf_freq
        self.query_timeout = query_timeout
        self.flamegraph_min_ms = flamegraph_min_ms
        self.bench_mode = bench_mode
        self._run_history: List[Dict] = []
        self._report_server: Optional[ReportServer] = report_server

    # -- server helpers -----------------------------------------------------

    def _ensure_server(self) -> Optional[ReportServer]:
        # Reuse an externally-provided server that is already running
        if self._report_server and self._report_server.running:
            return self._report_server
        if not self.serve:
            return None
        # Stop previous server if it exists but is no longer running
        if self._report_server:
            self._report_server.stop()
        self._report_server = ReportServer(self.output_dir, self.port)
        try:
            self._report_server.start()
            return self._report_server
        except OSError as e:
            console.print(f"  [red]✗[/red] Server failed: {e}")
            return None

    def _open_in_ide(self, url: str):
        try:
            subprocess.Popen(["code", "--open-url", url],
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        except FileNotFoundError:
            pass

    # -- bench execution ----------------------------------------------------

    def _run_bench(self, bench_file: str) -> bool:
        """Execute one benchmark run (possibly with --repeat rounds)."""
        import threading
        import time as _time

        from .benchmark import BenchmarkLoader, run_benchmark, BenchWorkload
        from .models import BenchmarkConfig, WorkloadMode
        from .reporter.bench_text import write_bench_text_report
        from .reporter.bench_html import write_bench_html_report
        from .reporter.history import generate_index_html
        from .ui import (BenchProgress, flush_all, print_bench_summary,
                         print_error, print_info, print_phase,
                         print_report_file)

        # Determine mode
        if self.concurrency > 0:
            mode = WorkloadMode.CONCURRENT
        else:
            mode = WorkloadMode.SERIAL

        json_extra_config = {}  # Extra config from JSON file

        # Load workload
        if not os.path.isfile(bench_file):
            print_error(f"Bench file not found: {bench_file}")
            flush_all()
            return False

        try:
            workload = BenchmarkLoader.from_file(bench_file)
        except (FileNotFoundError, ValueError) as e:
            print_error(str(e))
            flush_all()
            return False

        # Read extra config from JSON file (database, skip_setup, skip_teardown)
        json_extra_config = {}
        if bench_file.endswith('.json'):
            import json as _json
            try:
                with open(bench_file, 'r') as f:
                    json_data = _json.load(f)
                    json_extra_config = {
                        'database': json_data.get('database'),
                        'skip_setup': json_data.get('skip_setup'),
                        'skip_teardown': json_data.get('skip_teardown'),
                    }
            except Exception:
                pass

        # Determine skip_setup/skip_teardown: instance attr overrides JSON
        json_skip_setup = json_extra_config.get('skip_setup')
        json_skip_teardown = json_extra_config.get('skip_teardown')
        inst_skip_setup = getattr(self, 'skip_setup', False)
        inst_skip_teardown = getattr(self, 'skip_teardown', False)
        final_skip_setup = inst_skip_setup if inst_skip_setup else (json_skip_setup if json_skip_setup is not None else False)
        final_skip_teardown = inst_skip_teardown if inst_skip_teardown else (json_skip_teardown if json_skip_teardown is not None else False)

        filter_queries = []
        if self.bench_filter:
            filter_queries = [
                n.strip() for n in self.bench_filter.split(",")
                if n.strip()
            ]

        bench_cfg = BenchmarkConfig(
            mode=mode,
            iterations=self.iterations,
            warmup=self.warmup,
            concurrency=self.concurrency if self.concurrency > 0 else 1,
            duration=self.duration,
            ramp_up=self.ramp_up,
            filter_queries=filter_queries,
            profile=self.profile,
            perf_freq=self.perf_freq,
            query_timeout=self.query_timeout,
            flamegraph_min_ms=self.flamegraph_min_ms,
            skip_setup=final_skip_setup,
            skip_teardown=final_skip_teardown,
        )

        # Apply filter
        display_workload = workload
        if filter_queries:
            try:
                display_workload = BenchmarkLoader.filter_queries(
                    workload, filter_queries)
            except ValueError as e:
                print_error(str(e))
                flush_all()
                return False

        # Display plan
        print_phase("Benchmark", workload.name)
        print_info("Mode:", mode.name)
        print_info("DBMS targets:",
                   ", ".join(c.name for c in self.configs))
        if self.parallel_dbms and len(self.configs) > 1:
            print_info("DBMS execution:",
                       "[bold green]parallel[/bold green]")
        elif not self.parallel_dbms and len(self.configs) > 1:
            print_info("DBMS execution:", "sequential")

        if mode == WorkloadMode.SERIAL:
            print_info("Queries:",
                       ", ".join(q.name for q in display_workload.queries))
            print_info("Iterations:",
                       f"{bench_cfg.iterations}  "
                       f"Warmup: {bench_cfg.warmup}")
        else:
            print_info("Queries:",
                       ", ".join(q.name for q in display_workload.queries))
            print_info("Concurrency:",
                       f"{bench_cfg.concurrency}  "
                       f"Duration: {bench_cfg.duration}s")
        if filter_queries:
            print_info("Filter:", ", ".join(filter_queries))
        if self.repeat > 1:
            print_info("Repeat:", f"{self.repeat} rounds")

        fmt = self.output_format
        output_dir = self.output_dir
        configs = self.configs

        def _run_one_round(round_num: int):
            """Execute a single benchmark round."""
            if self.repeat > 1:
                console.print(
                    f"\n[bold cyan]{'━' * 60}[/bold cyan]")
                console.print(
                    f"[bold cyan]  Round {round_num}/"
                    f"{self.repeat}[/bold cyan]")
                console.print(
                    f"[bold cyan]{'━' * 60}[/bold cyan]\n")

            run_stamp = _time.strftime("%Y%m%d_%H%M%S")
            run_dir = os.path.join(
                output_dir,
                f"bench_{workload.name}_{run_stamp}")
            os.makedirs(run_dir, exist_ok=True)

            print_phase("Execute")

            # Progress tracking
            progress_bars: Dict[str, BenchProgress] = {}
            _progress_lock = threading.Lock()

            n_queries = len(display_workload.queries)
            # CONCURRENT mode uses time-based progress
            is_time_based = (mode == WorkloadMode.CONCURRENT)
            if mode == WorkloadMode.CONCURRENT:
                duration = bench_cfg.duration if bench_cfg.duration > 0 else 30.0
                per_query = 100  # placeholder, not used for time-based
            else:
                duration = 0.0
                per_query = bench_cfg.iterations + bench_cfg.warmup

            # Create progress bars upfront (they will show "setup..." initially)
            if self.parallel_dbms and len(configs) > 1:
                for c in configs:
                    bp = BenchProgress(
                        c.name, n_queries, per_query,
                        is_concurrent=is_time_based, duration=duration)
                    bp.__enter__()
                    bp.set_status("[yellow]正在setup...[/yellow]")
                    progress_bars[c.name] = bp

            def on_setup_start(dbms_name):
                with _progress_lock:
                    if dbms_name not in progress_bars:
                        bp = BenchProgress(
                            dbms_name, n_queries, per_query,
                            is_concurrent=is_time_based, duration=duration)
                        bp.__enter__()
                        bp.set_status("[yellow]正在setup...[/yellow]")
                        progress_bars[dbms_name] = bp

            def on_setup_done(dbms_name, success):
                bp = progress_bars.get(dbms_name)
                if bp:
                    if success:
                        bp.set_status("[green]setup完毕[/green]")
                    else:
                        bp.set_status("[red]setup失败 — 跳过该DBMS[/red]")
                        # Close progress bar for failed DBMS
                        bp.__exit__(None, None, None)
                        bp.write_summary_to_buffer()

            def on_dbms_start(dbms_name):
                with _progress_lock:
                    if dbms_name not in progress_bars:
                        bp = BenchProgress(
                            dbms_name, n_queries, per_query,
                            is_concurrent=is_time_based, duration=duration)
                        bp.__enter__()
                        progress_bars[dbms_name] = bp

            def on_run_start():
                # Reset timers when query phase begins (all setups complete)
                # Keep "setup完毕" status visible until queries actually start
                with _progress_lock:
                    for bp in progress_bars.values():
                        bp.reset_timer()
                # Record start time for timer thread
                timer_start_time[0] = _time.monotonic()
                # Signal timer thread to start updating
                query_phase_started.set()

            def on_progress(dbms_name, query_name, iteration,
                            total, is_warmup=False):
                bp = progress_bars.get(dbms_name)
                if bp:
                    if is_time_based:
                        # In time-based mode (CONCURRENT), update time progress
                        bp.update_time(status=f"[cyan]{query_name}[/cyan]")
                    else:
                        # In serial mode, show per-query iteration progress
                        bp.advance(query_name=query_name,
                                   iteration=iteration,
                                   total=total,
                                   is_warmup=is_warmup)

            def on_dbms_done(dbms_name, dbms_result):
                bp = progress_bars.get(dbms_name)
                if bp:
                    bp.set_status(
                        f"[green]{dbms_result.total_queries} queries, "
                        f"{dbms_result.overall_qps:.1f} QPS[/green]")
                    bp.__exit__(None, None, None)
                    bp.write_summary_to_buffer()

            def on_profile_start(dbms_name, query_name):
                bp = progress_bars.get(dbms_name)
                if bp:
                    bp.set_status(
                        f"[red]🔥 profiling {query_name}[/red]")

            def on_profile_done(dbms_name, query_name, sample_count):
                bp = progress_bars.get(dbms_name)
                if bp:
                    bp.set_status(
                        f"[dim]🔥 {query_name}: "
                        f"{sample_count} samples[/dim]")

            # For time-based mode (CONCURRENT), timer thread updates progress
            timer_stop_event = None
            timer_thread = None
            query_phase_started = threading.Event()
            timer_start_time = [None]  # Will be set in on_run_start

            if is_time_based:
                timer_stop_event = threading.Event()

                def _timer_update():
                    # Wait until query phase starts (all setups complete)
                    query_phase_started.wait()
                    while not timer_stop_event.is_set():
                        # Check if we've exceeded the duration - stop updating progress
                        # (actual benchmark may take longer due to cleanup)
                        if timer_start_time[0] is not None:
                            elapsed = _time.monotonic() - timer_start_time[0]
                            if elapsed >= duration:
                                break
                        for bp in list(progress_bars.values()):
                            bp.update_time(status="")
                        _time.sleep(0.5)

                timer_thread = threading.Thread(target=_timer_update, daemon=True)
                timer_thread.start()

            try:
                # Determine database: JSON config overrides instance default
                json_database = json_extra_config.get('database')
                final_database = json_database if json_database else self.database

                # Prepare callbacks for progress tracking
                callbacks = {
                    'on_progress': on_progress,
                    'on_dbms_start': on_dbms_start,
                    'on_dbms_done': on_dbms_done,
                    'on_profile_start': on_profile_start if bench_cfg.profile else None,
                    'on_profile_done': on_profile_done if bench_cfg.profile else None,
                    'on_run_start': on_run_start,
                    'on_setup_start': on_setup_start,
                    'on_setup_done': on_setup_done,
                }

                # Use shared core function for benchmark execution
                from .runner import run_benchmark_with_progress
                run_dir, result = run_benchmark_with_progress(
                    configs=configs,
                    workload=workload,
                    bench_cfg=bench_cfg,
                    database=final_database,
                    output_dir=output_dir,
                    output_format=fmt,
                    parallel_dbms=self.parallel_dbms,
                    json_extra_config=json_extra_config,
                    callbacks=callbacks,
                    bench_file=bench_file,
                )
            finally:
                # Stop timer thread
                if timer_stop_event is not None:
                    timer_stop_event.set()
                    if timer_thread is not None:
                        timer_thread.join(timeout=1.0)

            # Reports - already generated by run_benchmark_with_progress
            print_phase("Reports")

            if fmt in ("text", "all"):
                text_path = os.path.join(run_dir, f"bench_{workload.name}.report.txt")
                print_report_file(text_path, label="text")

            if fmt in ("html", "all"):
                html_path = os.path.join(run_dir, f"bench_{workload.name}.html")
                print_report_file(html_path, label="html")

            # JSON - already saved by run_benchmark_with_progress
            json_path = os.path.join(run_dir, "bench_result.json")
            print_report_file(json_path, label="json")

            # Latest symlink and history index - already updated by run_benchmark_with_progress

            print_bench_summary(result)
            flush_all()

            return run_dir

        # Main loop for repeat rounds
        last_run_dir = None
        for rnd in range(1, self.repeat + 1):
            try:
                last_run_dir = _run_one_round(rnd)
            except KeyboardInterrupt:
                console.print(
                    f"\n[yellow]Interrupted at round {rnd}/"
                    f"{self.repeat}. Stopping.[/yellow]")
                flush_all()
                break
            if rnd < self.repeat:
                _time.sleep(1)

        if self.repeat > 1:
            console.print(
                f"\n[bold green]All {self.repeat} rounds "
                f"completed.[/bold green]")
            flush_all()

        success = last_run_dir is not None
        self._run_history.append({
            "bench_file": bench_file,
            "workload": workload.name,
            "time": _time.strftime("%H:%M:%S"),
            "status": "OK" if success else "FAIL",
            "run_dir": last_run_dir or "",
        })

        # Open in browser via server
        srv = self._ensure_server()
        if (srv and last_run_dir
                and fmt in ("html", "all")):
            html_file = f"bench_{workload.name}.html"
            html_path = os.path.join(last_run_dir, html_file)
            if os.path.isfile(html_path):
                url = (f"{srv.base_url}"
                       f"/{os.path.basename(last_run_dir)}"
                       f"/{html_file}")
                console.print(
                    f"\n  [cyan]📊 Report:[/cyan] "
                    f"[bold link={url}]{url}[/bold link]\n")
                self._open_in_ide(url)

        return success

    # -- command handlers ---------------------------------------------------

    def _cmd_help(self):
        console.print("\n  [bold cyan]Available commands:[/bold cyan]")
        for cmd, desc in self.COMMANDS.items():
            console.print(f"    [bold]{cmd:10s}[/bold] {desc}")
        console.print(
            "\n  Or enter a [bold].json / .sql[/bold] bench file path"
            " to execute.\n")

    def _cmd_status(self):
        console.print(f"\n  [cyan]Config:[/cyan]")
        console.print(
            f"    DBMS:        "
            f"[bold]{', '.join(c.name for c in self.configs)}[/bold]")
        console.print(f"    Database:    [bold]{self.database}[/bold]")
        console.print(f"    Iterations:  [bold]{self.iterations}[/bold]")
        console.print(f"    Warmup:      [bold]{self.warmup}[/bold]")
        if self.concurrency > 0:
            console.print(
                f"    Concurrency: [bold]{self.concurrency}[/bold]")
            console.print(
                f"    Duration:    [bold]{self.duration}s[/bold]")
        console.print(f"    Repeat:      [bold]{self.repeat}[/bold]")
        console.print(f"    Output:      [bold]{self.output_dir}[/bold]")
        console.print(f"    Format:      [bold]{self.output_format}[/bold]")
        console.print(
            f"    Runs:        [bold]{len(self._run_history)}[/bold]")
        if self._report_server and self._report_server.running:
            console.print(
                f"    Server:      "
                f"[bold green]{self._report_server.base_url}"
                f"[/bold green]")
        console.print()

    def _cmd_history(self):
        if not self._run_history:
            console.print("\n  [dim]No benchmarks executed yet.[/dim]\n")
            return
        console.print(
            f"\n  [bold cyan]Session history "
            f"({len(self._run_history)} runs):[/bold cyan]")
        for i, entry in enumerate(self._run_history, 1):
            status_style = ("green" if entry["status"] == "OK"
                            else "red")
            console.print(
                f"    {i:3d}. [{status_style}]{entry['status']:4s}"
                f"[/{status_style}]  "
                f"[dim]{entry['time']}[/dim]  "
                f"{entry['bench_file']}  "
                f"[dim]({entry['workload']})[/dim]")
        console.print()

    def _cmd_server(self):
        srv = self._ensure_server()
        if srv and srv.running:
            idx_url = f"{srv.base_url}/index.html"
            console.print(
                f"\n  [green]●[/green] Server running: "
                f"[bold link={idx_url}]{idx_url}[/bold link]\n")
        else:
            console.print("\n  [dim]Server not running "
                          "(use --serve to enable).[/dim]\n")

    def _cmd_open(self):
        latest = os.path.join(self.output_dir, "latest")
        if not os.path.islink(latest):
            console.print("\n  [dim]No results yet.[/dim]\n")
            return
        real_dir = os.path.realpath(latest)
        htmls = [f for f in os.listdir(real_dir) if f.endswith(".html")]
        if not htmls:
            console.print("\n  [dim]No HTML report found.[/dim]\n")
            return
        srv = self._ensure_server()
        if not srv:
            console.print("\n  [dim]Server not available.[/dim]\n")
            return
        url = (f"{srv.base_url}"
               f"/{os.path.basename(real_dir)}/{htmls[0]}")
        console.print(f"\n  Opening: [bold]{url}[/bold]\n")
        self._open_in_ide(url)

    # -- main loop ----------------------------------------------------------

    def run(self):
        """Start the interactive benchmark REPL.

        Returns ``"back"`` if the user typed ``back``/``b``,
        ``"quit"`` otherwise (including EOF / KeyboardInterrupt).
        """
        os.makedirs(self.output_dir, exist_ok=True)
        session: PromptSession = PromptSession(
            history=_FilteredFileHistory(os.path.join(self.output_dir, ".rosetta_bench_history")),
            completer=BenchFileCompleter(),
            style=_PROMPT_STYLE,
            complete_while_typing=True,
            multiline=False,
            key_bindings=_make_repl_bindings(),
        )

        _placeholder = HTML('<placeholder>Type a path, \'help\', ← back, or \'quit\'</placeholder>')

        # ASCII Logo banner
        from .ui import LOGO_LINES, LOGO_SUBTITLE
        from . import __version__
        console.print()
        for logo_line in LOGO_LINES:
            console.print(f"  [bold cyan]{logo_line}[/bold cyan]")
        console.print()
        console.print(f"  [dim]{LOGO_SUBTITLE}[/dim]")
        console.print(f"  [dim]v{__version__}[/dim]  [bold white]Benchmark Mode[/bold white]")
        console.print()

        # Show config
        if self.concurrency > 0:
            mode_str = "CONCURRENT"
            config_parts = [
                f"[dim]Mode:[/dim] [bold]{mode_str}[/bold]",
                f"[dim]Concurrency:[/dim] [bold]{self.concurrency}[/bold]",
            ]
            if self.duration > 0:
                config_parts.append(
                    f"[dim]Duration:[/dim] [bold]{self.duration}s[/bold]")
            if self.ramp_up > 0:
                config_parts.append(
                    f"[dim]Ramp-up:[/dim] [bold]{self.ramp_up}s[/bold]")
            if self.warmup > 0:
                config_parts.append(
                    f"[dim]Warmup:[/dim] [bold]{self.warmup}[/bold]")
        else:
            mode_str = "SERIAL"
            config_parts = [
                f"[dim]Mode:[/dim] [bold]{mode_str}[/bold]",
                f"[dim]Iterations:[/dim] [bold]{self.iterations}[/bold]",
                f"[dim]Warmup:[/dim] [bold]{self.warmup}[/bold]",
            ]
        console.print(
            f"  [dim]DBMS:[/dim] "
            f"[bold]{', '.join(c.name for c in self.configs)}[/bold]  "
            + "  ".join(config_parts))
        if self.repeat > 1:
            console.print(
                f"  [dim]Repeat:[/dim] [bold]{self.repeat}[/bold]  "
                f"[dim]Database:[/dim] [bold]{self.database}[/bold]")
        else:
            console.print(
                f"  [dim]Database:[/dim] [bold]{self.database}[/bold]")

        # Start server early if requested
        srv = self._ensure_server()
        if srv and srv.running:
            console.print(
                f"  [dim]Server:[/dim] "
                f"[bold green]{srv.base_url}[/bold green]")
        console.print()

        run_count = 0
        exit_reason = "quit"

        while True:
            try:
                prompt_msg = HTML(
                    '<prompt>rosetta</prompt> <path>▶</path> ')
                user_input = session.prompt(
                    prompt_msg, placeholder=_placeholder)
            except (EOFError, KeyboardInterrupt):
                break

            if isinstance(user_input, _BackSignal):
                exit_reason = "back"
                break

            user_input = user_input.strip()
            if not user_input:
                continue

            cmd = user_input.lower()

            # Back to parameter selection
            if cmd in ("back", "b"):
                exit_reason = "back"
                break

            # Exit
            if cmd in ("quit", "exit", "q"):
                break

            # Built-in commands
            if cmd == "help":
                self._cmd_help()
                continue
            if cmd == "status":
                self._cmd_status()
                continue
            if cmd == "history":
                self._cmd_history()
                continue
            if cmd == "server":
                self._cmd_server()
                continue
            if cmd == "open":
                self._cmd_open()
                continue
            if cmd == "clear":
                console.clear()
                continue

            # Treat as bench file path
            bench_path = os.path.expanduser(user_input)
            if not os.path.isabs(bench_path):
                bench_path = os.path.abspath(bench_path)

            run_count += 1
            console.print()
            console.rule(
                f"[bold cyan] Bench #{run_count}: "
                f"{os.path.basename(bench_path)} [/bold cyan]")
            console.print()

            self._run_bench(bench_path)

            console.print(
                "  [dim]Ready for next benchmark. "
                "Type a path, 'help', 'back', or 'quit'.[/dim]\n")

        # Cleanup
        if exit_reason == "back":
            # Silent cleanup — caller will clear the screen
            if self._report_server:
                self._report_server.stop()
        else:
            console.print()
            if self._run_history:
                console.print(
                    f"  [dim]Session complete: "
                    f"{len(self._run_history)} benchmark(s) "
                    f"executed.[/dim]")
            if self._report_server:
                self._report_server.stop()
            console.print("  [dim]Report server stopped.[/dim]")
            console.print("  [bold cyan]Goodbye! 👋[/bold cyan]\n")

        return exit_reason


# ---------------------------------------------------------------------------
# MTR interactive session
# ---------------------------------------------------------------------------

class MtrCaseCompleter(Completer):
    """Auto-complete test case names (basic path completion)."""

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor.strip()
        if not text:
            text = ""
        expanded = os.path.expanduser(text)
        pattern = expanded + "*"

        for path in sorted(glob.glob(pattern)):
            if os.path.isdir(path):
                yield Completion(path + "/", start_position=-len(text),
                                 display=os.path.basename(path) + "/",
                                 display_meta="dir")
            else:
                yield Completion(path, start_position=-len(text),
                                 display=os.path.basename(path),
                                 display_meta="case")


class MtrInteractiveSession:
    """Interactive REPL for native MTR mode.

    Base parameters (mode, parallel, vector, etc.) are fixed at launch;
    only the test case/suite name changes between runs.
    """

    COMMANDS = {
        "help":    "Show available commands",
        "status":  "Show current MTR configuration",
        "history": "Show executed MTR runs in this session",
        "clear":   "Clear the screen",
        "back":    "Back to parameter selection (also: b)",
        "quit":    "Exit (also: exit, q)",
    }

    _MTR_MODES = {
        "row":    {"label": "Row",    "vector": False, "parallel_query": False, "ps_protocol": False},
        "col":    {"label": "Column", "vector": True,  "parallel_query": False, "ps_protocol": False},
        "pq":     {"label": "Parallel Query", "vector": False, "parallel_query": True,  "ps_protocol": False},
        "ps":     {"label": "Prepared Statement", "vector": False, "parallel_query": False, "ps_protocol": True},
    }
    _MODE_PORT_OFFSETS = {"row": 0, "col": 1000, "pq": 2000, "ps": 3000}

    def __init__(self, configs, output_dir,
                 mtr_mode="row", parallel=8,
                 optimistic=False, record=False,
                 retry=3, suite_mode=False, suite=None, all_configs=None):
        self.configs = configs
        self.output_dir = os.path.abspath(output_dir)
        self.mtr_mode = mtr_mode
        self.parallel = parallel
        self.optimistic = optimistic
        self.record = record
        self.retry = retry
        self.suite_mode = suite_mode
        self.suite = suite
        self.all_configs = all_configs or []
        self._run_history = []

    def run(self):
        from .ui import LOGO_LINES, LOGO_SUBTITLE

        _hist_path = os.path.join(os.path.expanduser("~/.rosetta"), "mtr_history")
        os.makedirs(os.path.dirname(_hist_path), exist_ok=True)
        session = PromptSession(
            history=_FilteredFileHistory(_hist_path),
            style=_PROMPT_STYLE,
            multiline=False,
            completer=MtrCaseCompleter(),
            key_bindings=_make_repl_bindings(),
        )

        _mode_hint = "suite" if self.suite_mode else "case"
        _placeholder = HTML(
            f'<placeholder>Type a test {_mode_hint} name, \'help\', '
            f'← back, or \'quit\'</placeholder>')

        console.print()
        for logo_line in LOGO_LINES:
            console.print(f"  [bold cyan]{logo_line}[/bold cyan]")
        console.print()
        console.print(f"  [dim]{LOGO_SUBTITLE}[/dim]")
        from . import __version__
        console.print(f"  [dim]v{__version__}[/dim]  "
                      f"[bold white]MTR Mode[/bold white]")

        mode_label = self._get_mode_label()

        # DBMS info line (consistent with Test/Playground modes)
        dbms_names = ", ".join(c.name for c in self.configs)
        console.print(
            f"\n  [dim]DBMS:[/dim] [bold]{dbms_names}[/bold]  "
            f"[dim]Database:[/dim] [bold]cross_dbms_test_db[/bold]")

        # MTR parameters line
        console.print(
            f"  [dim]Mode:[/dim] [bold]{mode_label}[/bold]  "
            f"[dim]Parallel:[/dim] [bold]{self.parallel}[/bold]  "
            f"[dim]Retry:[/dim] [bold]{self.retry}[/bold]  "
            f"[dim]Suite:[/dim] [bold]{'ON' if self.suite_mode else 'Off'}[/bold]")

        # Flags line (only show enabled flags)
        flags = []
        if self.optimistic:
            flags.append("[bold yellow]Optimistic[/bold yellow]")
        if self.record:
            flags.append("[bold yellow]Record[/bold yellow]")
        if flags:
            console.print(f"  [dim]Flags:[/dim] {', '.join(flags)}")

        console.print()

        exit_reason = "quit"

        while True:
            try:
                prompt_msg = HTML(
                    '<prompt>rosetta</prompt> <path>▶</path> ')
                user_input = session.prompt(
                    prompt_msg, placeholder=_placeholder)
            except (EOFError, KeyboardInterrupt):
                break

            if isinstance(user_input, _BackSignal):
                exit_reason = "back"
                break

            user_input = user_input.strip()
            if not user_input:
                continue

            cmd = user_input.lower()

            if cmd in ("back", "b"):
                exit_reason = "back"
                break
            if cmd in ("quit", "exit", "q"):
                break
            if cmd == "help":
                self._cmd_help()
                continue
            if cmd == "status":
                self._cmd_status()
                continue
            if cmd == "history":
                self._cmd_history()
                continue
            if cmd == "clear":
                console.clear()
                continue

            self._run_mtr(user_input)

            _mode_hint = "suite" if self.suite_mode else "case"
            console.print(
                f"  [dim]Ready. Type a test {_mode_hint} name, 'help', "
                f"'back', or 'quit'.[/dim]\n")

        if exit_reason == "back":
            pass
        else:
            console.print()
            if self._run_history:
                console.print(
                    f"  [dim]Session complete: "
                    f"{len(self._run_history)} MTR run(s) executed.[/dim]")
            console.print("  [bold cyan]Goodbye! 👋[/bold cyan]\n")

        return exit_reason

    # -- helpers -------------------------------------------------------------

    def _get_mode_label(self):
        from rosetta.cli.mtr_cmd import _MODE_ALIASES
        mode = self.mtr_mode
        if mode == "all":
            return "All (row + col + pq + ps)"
        # Handle multi-mode string like "row,pq"
        if "," in mode:
            parts = [m.strip() for m in mode.split(",")]
            resolved = [_MODE_ALIASES.get(m, m) for m in parts]
            return ",".join(resolved)
        return self._MTR_MODES.get(mode, {}).get("label", mode)

    def _build_config_for_mode(self, mode_name):
        from rosetta.paths import CONFIG_FILE
        from rosetta.cli.mtr_cmd import (
            _load_mtr_config, MTR_MODES,
            _MODE_PORT_OFFSETS, _build_mysqld_opts,
        )

        file_cfg = _load_mtr_config(CONFIG_FILE)

        required_keys = [
            "test_dir", "skip_list", "base_port", "total_port",
            "parallel", "retry", "retry_failure", "max_test_fail",
            "testcase_timeout", "suite_timeout", "mysqld_opts",
        ]
        missing = [k for k in required_keys if k not in file_cfg]
        if missing:
            print_error(
                f"Missing required mtr config in {CONFIG_FILE}: "
                f"{', '.join(missing)}")
            print_info("Run 'rosetta config --sample' for a template.")
            flush_all()
            return None

        test_dir = file_cfg["test_dir"]
        if not os.path.isdir(test_dir):
            print_error(f"MySQL test directory not found: {test_dir}")
            flush_all()
            return None

        mode_def = MTR_MODES.get(mode_name, {})
        cfg = {
            "test_dir": test_dir,
            "skip_list": file_cfg["skip_list"],
            "parallel": self.parallel,
            "retry": self.retry,
            "retry_failure": file_cfg["retry_failure"],
            "max_test_fail": file_cfg["max_test_fail"],
            "testcase_timeout": file_cfg["testcase_timeout"],
            "suite_timeout": file_cfg["suite_timeout"],
            "port_base": file_cfg["base_port"]
                       + _MODE_PORT_OFFSETS.get(mode_name, 0),
            "optimistic": self.optimistic,
            "record": self.record,
            "vector": mode_def.get("vector", False),
            "parallel_query": mode_def.get("parallel_query", False),
            "ps_protocol": mode_def.get("ps_protocol", False),
            "suite": self.suite,
            "vardir": os.path.join(test_dir, f"var_{mode_name}"),
            "tmpdir": os.path.join(test_dir, f"tmp_{mode_name}"),
        }

        opts = file_cfg["mysqld_opts"]
        if isinstance(opts, list):
            cfg["mysqld_opts"] = _build_mysqld_opts(opts)
        elif isinstance(opts, str):
            cfg["mysqld_opts"] = opts
        else:
            cfg["mysqld_opts"] = ""
        return cfg

    def _run_mtr(self, cases_input):
        """Execute MTR with the given test case names or suite."""
        import concurrent.futures as _cf
        import threading as _threading
        from rich import box as _box
        from rich.console import Console as _RichConsole
        from rich.live import Live
        from rich.table import Table
        from rich.panel import Panel
        from rich.text import Text
        from rosetta.paths import MTR_LOGS_DIR
        from rosetta.cli.mtr_cmd import (
            _build_command, MTR_MODES, _MODE_PORT_OFFSETS,
            _should_suppress, _parse_mtr_progress, _parse_mtr_log_stats,
        )

        from rosetta.cli.mtr_cmd import _MODE_ALIASES

        # Expand multi-mode string (e.g. "row,pq" -> ["row", "pq"])
        if self.mtr_mode == "all":
            modes_to_run = list(MTR_MODES.keys())
        else:
            _requested = [m.strip().lower() for m in self.mtr_mode.split(",") if m.strip()]
            _expanded = []
            for m in _requested:
                if m == "all":
                    _expanded.extend(list(MTR_MODES.keys()))
                else:
                    _expanded.append(_MODE_ALIASES.get(m, m))
            # Deduplicate preserving order
            _seen = set()
            modes_to_run = []
            for m in _expanded:
                if m not in _seen:
                    _seen.add(m)
                    modes_to_run.append(m)

        mode_cfgs = {}
        valid = True

        # Parse input: support both comma and space separators
        _items = [c.strip() for c in cases_input.replace(",", " ").split() if c.strip()]

        # If Suite Mode is ON, treat all items as suite names (comma/space separated)
        if self.suite_mode:
            _suite_names = ",".join(_items)  # e.g. "tdsql,json"
            _cases = []
        else:
            _suite_names = None
            _cases = _items

        for mn in modes_to_run:
            cfg = self._build_config_for_mode(mn)
            if cfg is None:
                valid = False
                break
            cfg["cases"] = _cases
            cfg["suite"] = _suite_names
            mode_cfgs[mn] = cfg

        if not mode_cfgs or not valid:
            return

        test_dir = mode_cfgs[modes_to_run[0]]["test_dir"]

        log_dir = os.path.join(MTR_LOGS_DIR,
                               _time.strftime("%Y%m%d_%H%M%S"))
        os.makedirs(log_dir, exist_ok=True)

        # --- Plan table (same layout as CLI mode) ---
        from rosetta.paths import CONFIG_FILE
        _config_path = CONFIG_FILE

        plan_table = Table(
            show_header=True, header_style="bold cyan",
            expand=True, box=_box.ROUNDED,
        )
        plan_table.add_column("Mode", style="bold", min_width=16)
        plan_table.add_column("Port Base", justify="right")
        plan_table.add_column("Vardir")
        plan_table.add_column("Flags")
        plan_table.add_column("Log File")

        for mn in modes_to_run:
            md = MTR_MODES.get(mn, {})
            cfg = mode_cfgs[mn]
            flags = []
            if cfg["vector"]:
                flags.append("--ve-protocol")
            if cfg["parallel_query"]:
                flags.append("--parallel-query")
            if cfg["optimistic"]:
                flags.append("optimistic")
            if cfg["record"]:
                flags.append("--record")
            log_file = os.path.join(log_dir, f"{mn}.log")
            plan_table.add_row(
                md.get("label", mn),
                str(cfg["port_base"]),
                os.path.basename(cfg["vardir"]),
                " ".join(flags) if flags else "(default)",
                os.path.abspath(log_file),
            )

        console.print(plan_table)

        # Configuration panel (same as CLI mode)
        # Try to detect SQLEngine current git branch
        def _get_git_branch(path: str) -> Optional[str]:
            try:
                result = subprocess.run(
                    ["git", "-C", path, "rev-parse", "--abbrev-ref", "HEAD"],
                    capture_output=True, text=True, timeout=2,
                )
                if result.returncode == 0:
                    branch = result.stdout.strip()
                    if branch:
                        # Also get short commit hash for context
                        sha_result = subprocess.run(
                            ["git", "-C", path, "rev-parse", "--short", "HEAD"],
                            capture_output=True, text=True, timeout=2,
                        )
                        sha = sha_result.stdout.strip() if sha_result.returncode == 0 else ""
                        return f"{branch} ({sha})" if sha else branch
            except Exception:
                pass
            return None

        _branch = _get_git_branch(test_dir)
        info_lines = [
            f"[bold]Config [/bold]   : {os.path.abspath(_config_path)}",
            f"[bold]Test dir[/bold]  : {test_dir}",
            f"[bold]Log dir[/bold]   : {os.path.abspath(log_dir)}",
        ]
        if _branch:
            info_lines.append(f"[bold]Branch[/bold]    : [cyan]{_branch}[/cyan]")
        if _suite_names:
            info_lines.append(f"[bold]Suite[/bold]     : {_suite_names}")
        if _cases:
            info_lines.append(f"[bold]Cases[/bold]     : {' '.join(_cases)}")
        console.print(Panel(
            "\n".join(info_lines),
            title="[bold cyan]Configuration[/bold cyan]",
            title_align="left",
            padding=(0, 1),
        ))

        # Actual command panels per mode (same as CLI mode)
        for mn in modes_to_run:
            cfg = mode_cfgs[mn]
            cmd = _build_command(cfg)
            label = MTR_MODES[mn]["label"]
            console.print(Panel(
                f"[dim]{cmd}[/dim]",
                title=f"[bold cyan]{label}[/bold cyan]",
                title_align="left",
                padding=(0, 1),
            ))

        # Live progress
        live_console = _RichConsole(stderr=True)
        results_lock = _threading.Lock()
        mode_results = {}
        total_start = _time.monotonic()

        mode_state = {
            m: {"status": "waiting", "elapsed": 0.0, "exit_code": None,
                 "last_line": "", "start_time": None, "progress": 0}
            for m in modes_to_run
        }

        def _run_single(mn):
            cfg = mode_cfgs[mn]
            cmd = _build_command(cfg)
            log_path = os.path.join(log_dir, f"{mn}.log")

            with results_lock:
                mode_state[mn]["status"] = "running"
                mode_state[mn]["start_time"] = _time.monotonic()

            ec = -1
            _proc_ref = [None]  # hold ref so _kill_all_children can access it
            try:
                proc = subprocess.Popen(
                    cmd, shell=True, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True, bufsize=1,
                    cwd=test_dir, start_new_session=True)
                _proc_ref[0] = proc
                with results_lock:
                    mode_state[mn]["_proc"] = proc
                with open(log_path, "w", encoding="utf-8") as lf:
                    try:
                        for raw_line in proc.stdout:
                            if _cancel.is_set():
                                break
                            line = raw_line.rstrip("\n")
                            stripped = line.strip()
                            if not _should_suppress(stripped):
                                lf.write(line + "\n")
                                lf.flush()
                            pct = _parse_mtr_progress(stripped)
                            if pct is not None:
                                with results_lock:
                                    mode_state[mn]["progress"] = pct
                            if stripped:
                                with results_lock:
                                    mode_state[mn]["last_line"] = stripped[-80:]
                    except (ValueError, OSError):
                        pass  # pipe closed by kill

                    # Wait with aggressive timeout
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(os.getpgid(proc.pid), _signal.SIGKILL)
                            proc.wait(timeout=2)
                        except Exception:
                            try:
                                proc.kill()
                            except Exception:
                                pass
                    try:
                        ec = proc.returncode
                    except Exception:
                        ec = -1
            except Exception as e:
                with open(log_path, "a") as lf:
                    lf.write(f"\n[ERROR] {e}\n")
            finally:
                # Ensure proc is always cleaned up
                try:
                    p = _proc_ref[0]
                    if p and p.stdout and not p.stdout.closed:
                        p.stdout.close()
                except Exception:
                    pass

            elapsed = _time.monotonic() - \
                (mode_state[mn].get("start_time") or _time.monotonic())
            with results_lock:
                mode_state[mn]["status"] = "done"
                mode_state[mn]["exit_code"] = ec
                mode_state[mn]["elapsed"] = elapsed
            return {"mode": mn, "exit_code": ec, "elapsed": elapsed,
                    "log_file": log_path}

        def _build_progress():
            table = Table(show_header=True, header_style="bold cyan",
                          expand=True, padding=(0, 1), box=_box.ROUNDED)
            table.add_column("Mode", style="bold", min_width=16)
            table.add_column("Progress", min_width=14)
            table.add_column("Elapsed", justify="right", min_width=10)
            table.add_column("Log File", min_width=20, no_wrap=True)
            table.add_column("Latest Output", ratio=1, overflow="ellipsis", no_wrap=True)

            for mn in modes_to_run:
                st = mode_state[mn]
                label = MTR_MODES.get(mn, {}).get("label", mn)

                elapsed = st["elapsed"]
                if st["status"] == "running" and st.get("start_time"):
                    elapsed = _time.monotonic() - st["start_time"]
                mins, secs = divmod(int(elapsed), 60)
                hours, mins = divmod(mins, 60)
                es = f"{hours}h{mins:02d}m{secs:02d}s" \
                    if hours > 0 else f"{mins:02d}m{secs:02d}s"

                pct = st.get("progress", 0)
                if st["status"] == "waiting":
                    status = Text("Waiting", style="dim")
                elif st["status"] == "running":
                    bar_filled = int(pct / 5)
                    bar_empty = 20 - bar_filled
                    status = Text.from_markup(
                        f"[yellow]{'█' * bar_filled}"
                        f"{'░' * bar_empty}[/yellow] {pct}%")
                elif st["status"] == "done":
                    if st["exit_code"] == 0:
                        status = Text("PASSED", style="green bold")
                    else:
                        status = Text(
                            f"FAILED({st['exit_code']})", style="red bold")
                else:
                    status = Text(st["status"])
                _log_file = os.path.join(log_dir, f"{mn}.log")
                table.add_row(label, status, es, _log_file, st.get("last_line", ""))
            return table

        interrupted = False
        _cancel = _threading.Event()

        import signal as _signal

        # --- Cancel file (most reliable: works from any terminal) ---
        _CANCEL_FILE = "/tmp/.rosetta_mtr_cancel"
        try:
            os.remove(_CANCEL_FILE)
        except FileNotFoundError:
            pass

        def _check_cancel_file():
            """Check if cancel signal file exists."""
            return os.path.exists(_CANCEL_FILE)

        def _cleanup_cancel_file():
            """Remove cancel signal file after handling."""
            try:
                os.remove(_CANCEL_FILE)
            except FileNotFoundError:
                pass

        def _kill_all_children(force=False):
            """Force-kill all running MTR subprocesses."""
            for mn in modes_to_run:
                st = mode_state[mn]
                if st.get("status") != "running":
                    continue
                proc = st.get("_proc")
                if not proc:
                    continue
                # Try multiple strategies to ensure death
                for _attempt in range(3):
                    try:
                        if proc.poll() is not None:
                            break  # already dead
                        if force or _attempt >= 1:
                            os.killpg(os.getpgid(proc.pid), _signal.SIGKILL)
                        else:
                            os.killpg(os.getpgid(proc.pid), _signal.SIGTERM)
                        _time.sleep(0.2)
                    except (ProcessLookupError, OSError):
                        break  # already gone
                    except Exception:
                        pass
                # Also close stdout pipe to unblock any reader threads
                try:
                    proc.stdout.close()
                except Exception:
                    pass

        # Layer 1: Signal handler (may or may not work in prompt_toolkit)
        def _handle_sigint(signum, frame):
            if not _cancel.is_set():
                _cancel.set()
                with open(_CANCEL_FILE, "w") as _f:
                    _f.write(str(os.getpid()))
                console.print(
                    "\n[yellow bold]^C received, stopping... "
                    "(open another terminal: touch /tmp/.rosetta_mtr_cancel)[/yellow bold]\n")
                _kill_all_children()
            else:
                raise KeyboardInterrupt

        _orig_sig = _signal.signal(_signal.SIGINT, _handle_sigint)

        # Layer 2: Background cancel-file watcher thread
        def _watch_cancel_file():
            while not _cancel.is_set():
                if _check_cancel_file():
                    _cancel.set()
                    console.print(
                        "\n[yellow bold]Cancel signal received, "
                        "stopping MTR...[/yellow bold]\n")
                    _kill_all_children(force=True)
                    break
                _time.sleep(0.5)

        _file_watcher = _threading.Thread(target=_watch_cancel_file, daemon=True)
        _file_watcher.start()

        # Layer 3: TTY watcher (best-effort)
        _tty_stop = _threading.Event()

        def _watch_tty():
            import fcntl
            tty_fd = -1
            try:
                tty_fd = os.open("/dev/tty", os.O_RDONLY | os.O_NONBLOCK)
                while not _cancel.is_set() and not _tty_stop.is_set():
                    try:
                        rl, _, _ = _select.select([tty_fd], [], [], 0.5)
                        if rl:
                            ch = os.read(tty_fd, 1)
                            if ch in (b'\x03', b'q', b'Q'):
                                _cancel.set()
                                with open(_CANCEL_FILE, "w") as _f:
                                    _f.write(str(os.getpid()))
                                console.print("\n[yellow bold]Stopping..."
                                              "[/yellow bold]\n")
                                _kill_all_children(force=True)
                                break
                    except (OSError, BlockingIOError):
                        continue
                    except Exception:
                        break
            except Exception:
                pass
            finally:
                if tty_fd >= 0:
                    try:
                        # Remove O_NONBLOCK before closing to avoid
                        # affecting shared file description state
                        flags = fcntl.fcntl(tty_fd, fcntl.F_GETFL)
                        fcntl.fcntl(tty_fd, fcntl.F_SETFL,
                                    flags & ~os.O_NONBLOCK)
                    except Exception:
                        pass
                    try:
                        os.close(tty_fd)
                    except Exception:
                        pass

        import select as _select
        _tty_watcher = _threading.Thread(target=_watch_tty, daemon=True)
        _tty_watcher.start()

        with Live(_build_progress(), console=live_console,
                  refresh_per_second=2, transient=True) as live:
            try:
                with _cf.ThreadPoolExecutor(max_workers=len(modes_to_run)) as pool:
                    futures = {pool.submit(_run_single, m): m
                              for m in modes_to_run}
                    while True:
                        # Check ALL cancellation sources
                        if _cancel.is_set() or _check_cancel_file():
                            _cancel.set()
                            break
                        done = {f for f in futures if f.done()}
                        live.update(_build_progress())
                        if len(done) == len(futures):
                            break
                        _time.sleep(0.3)

                # Wait briefly then collect results
                _time.sleep(0.5)
                for fut in futures:
                    try:
                        r = fut.result(timeout=2)
                        mode_results[r["mode"]] = r
                    except Exception as _e:
                        m = futures[fut]
                        st = mode_state.get(m, {})
                        mode_results[m] = {
                            "mode": m,
                            "exit_code": -1,
                            "elapsed": st.get("elapsed", 0),
                            "log_file": os.path.join(log_dir, f"{m}.log"),
                            "error": "cancelled",
                        }
            finally:
                _kill_all_children(force=True)
                _cleanup_cancel_file()
                # Stop the TTY watcher thread and wait for it to exit
                _tty_stop.set()
                _tty_watcher.join(timeout=2)
                try:
                    _signal.signal(_signal.SIGINT, _orig_sig)
                except Exception:
                    pass
                # Restore terminal state in case it was corrupted
                try:
                    import termios
                    import sys
                    fd = sys.stdin.fileno()
                    termios.tcflush(fd, termios.TCIFLUSH)
                except Exception:
                    pass

        if interrupted or _cancel.is_set():
            console.print("\n[yellow bold]MTR execution cancelled by user.[/yellow bold]\n")
            return

        # Print final progress table (transient Live cleared it on exit)
        live_console.print(_build_progress())

        # Summary table (same format as CLI)
        summary = Table(
            show_header=True, header_style="bold cyan",
            padding=(0, 1), box=_box.ROUNDED, expand=True)
        summary.add_column("Mode", style="bold", min_width=16)
        summary.add_column("Result", min_width=10)
        summary.add_column("Total", justify="center")
        summary.add_column("Pass", justify="center")
        summary.add_column("Fail", justify="center")
        summary.add_column("Pass Rate", justify="center")
        summary.add_column("Elapsed", justify="right", min_width=10)

        has_failures = False
        for mn in modes_to_run:
            r = mode_results.get(mn, {})
            label = MTR_MODES.get(mn, {}).get("label", mn)
            ec = r.get("exit_code", -1)
            elapsed = r.get("elapsed", 0)
            log_file = r.get("log_file", "")
            stats = _parse_mtr_log_stats(log_file)

            mins, secs = divmod(int(elapsed), 60)
            hours, mins = divmod(mins, 60)
            es = f"{hours}h{mins:02d}m{secs:02d}s" \
                if hours > 0 else f"{mins:02d}m{secs:02d}s"

            result_text = "[green bold]PASSED[/green bold]" \
                if ec == 0 else "[red bold]FAILED[/red bold]"

            fail_count = stats.get("fail", 0)
            if fail_count > 0:
                has_failures = True

            summary.add_row(
                label, result_text,
                str(stats.get("total", "-")),
                f"[green]{stats.get('pass', '-')}[/green]",
                f"[red]{fail_count}[/red]" if fail_count > 0
                else str(stats.get("fail", "-")),
                stats.get("pass_ratio", "-"), es)

        console.print(summary)

        # Failed cases detail
        for mn in modes_to_run:
            r = mode_results.get(mn, {})
            log_file = r.get("log_file", "")
            stats = _parse_mtr_log_stats(log_file)
            failing = stats.get("failing_tests", [])
            if failing:
                label = MTR_MODES.get(mn, {}).get("label", mn)
                cases_text = "\n".join(
                    f"  [red]*[/red] {c}" for c in failing)
                console.print(Panel(
                    cases_text,
                    title=f"[bold red]{label} — "
                           f"Failed Cases ({len(failing)})[/bold red]",
                    title_align="left", border_style="red",
                    padding=(0, 1)))

        total_elapsed = round(_time.monotonic() - total_start, 1)
        console.print(f"\n  [dim]Log dir:[/dim] {log_dir}")
        console.print(f"  [dim]Elapsed:[/dim] {total_elapsed}s\n")

        self._run_history.append({
            "cases": cases_input, "modes": modes_to_run,
            "time": _time.strftime("%H:%M:%S"),
            "status": "PASS" if not has_failures else "FAIL",
            "log_dir": log_dir,
        })

    # -- command handlers ---------------------------------------------------

    def _cmd_help(self):
        console.print("\n  [bold cyan]Available commands:[/bold cyan]")
        for cmd, desc in self.COMMANDS.items():
            console.print(f"    [bold]{cmd:10s}[/bold] {desc}")
        console.print(
            "\n  Or enter a [bold]test case name[/bold] to execute an MTR run.\n"
            "  During execution, press [bold]^C[/bold] or [bold]q[/bold],\n"
            "  or from another terminal: [bold]touch /tmp/.rosetta_mtr_cancel[/bold]\n")

    def _cmd_status(self):
        mode_label = self._get_mode_label()
        console.print(f"\n  [cyan]MTR Config:[/cyan]")
        console.print(f"    Mode:       [bold]{mode_label}[/bold]")
        console.print(f"    Parallel:   [bold]{self.parallel}[/bold]")
        console.print(f"    Optimistic: {'On' if self.optimistic else 'Off'}")
        console.print(f"    Record (-r): {'On' if self.record else 'Off'}")
        console.print(f"    Retry:      [bold]{self.retry}[/bold]")
        if self.suite:
            console.print(f"    Suite:      [bold]{self.suite}[/bold]")
        console.print(f"    Runs:       [bold]{len(self._run_history)}[/bold]")
        console.print()

    def _cmd_history(self):
        if not self._run_history:
            console.print("\n  [dim]No MTR runs yet.[/dim]\n")
            return
        console.print(
            f"\n  [bold cyan]MTR History "
            f"({len(self._run_history)} runs):[/bold cyan]\n")
        for i, entry in enumerate(self._run_history, 1):
            s = "green" if entry["status"] == "PASS" else "red"
            modes_str = ",".join(entry["modes"])
            console.print(
                f"    {i:3d}. [{s}]{entry['status']:4s}[/{s}]  "
                f"[dim]{entry['time']}[/dim]  "
                f"{entry['cases']}  "
                f"[cyan]({modes_str})[/cyan]")
        console.print()
