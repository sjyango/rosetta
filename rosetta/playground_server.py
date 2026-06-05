"""
Rosetta Playground Server for With Platform.

Extended HTTP server that wraps the original ReportServer and adds:
- With Auth user info endpoint
- SQL history persistence via MySQL
- SQL favorites management
- User preferences

Usage:
    python -m rosetta.playground_server --config config.json --port 19527
"""

import argparse
import http.server
import json
import logging
import os
import signal
import socket
import sys
import threading
import time as _time
from typing import Optional

log = logging.getLogger("rosetta.playground")

# ── Database connection (lazy) ──────────────────────────────────────────

_db_conn = None
_db_lock = threading.Lock()

DB_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "11.142.154.110"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "with_ugmatclusdrxhadd"),
    "password": os.environ.get("MYSQL_PASSWORD", "Xe#RGXP8$a0XQQ"),
    "database": os.environ.get("MYSQL_DATABASE", "rf5otpny"),
}


def _get_db():
    """Get or create a MySQL database connection (thread-safe)."""
    global _db_conn
    try:
        import pymysql
    except ImportError:
        log.warning("pymysql not available, DB features disabled")
        return None
    with _db_lock:
        if _db_conn is None:
            try:
                _db_conn = pymysql.connect(
                    host=DB_CONFIG["host"],
                    port=DB_CONFIG["port"],
                    user=DB_CONFIG["user"],
                    password=DB_CONFIG["password"],
                    database=DB_CONFIG["database"],
                    charset="utf8mb4",
                    autocommit=True,
                    connect_timeout=5,
                )
                log.info("Connected to MySQL at %s:%s", DB_CONFIG["host"], DB_CONFIG["port"])
            except Exception as e:
                log.error("Failed to connect to MySQL: %s", e)
                return None
        else:
            try:
                _db_conn.ping(reconnect=True)
            except Exception:
                try:
                    import pymysql
                    _db_conn = pymysql.connect(
                        host=DB_CONFIG["host"],
                        port=DB_CONFIG["port"],
                        user=DB_CONFIG["user"],
                        password=DB_CONFIG["password"],
                        database=DB_CONFIG["database"],
                        charset="utf8mb4",
                        autocommit=True,
                        connect_timeout=5,
                    )
                except Exception as e:
                    log.error("Failed to reconnect MySQL: %s", e)
                    _db_conn = None
                    return None
        return _db_conn


def _db_execute(sql: str, params=None):
    """Execute SQL and return cursor."""
    db = _get_db()
    if db is None:
        return None
    cursor = db.cursor()
    cursor.execute(sql, params or ())
    return cursor


# ── With Auth ────────────────────────────────────────────────────────────

def _get_with_user(headers) -> dict:
    """Extract user info from With platform headers or return guest."""
    eng_name = headers.get("X-With-EngName", "")
    chn_name = headers.get("X-With-ChnName", "")
    dept_name = headers.get("X-With-DeptName", "")
    position = headers.get("X-With-PositionName", "")

    # Also try standard nginx/forwarded headers
    if not eng_name:
        eng_name = headers.get("X-Forwarded-User", "")

    return {
        "eng_name": eng_name,
        "chn_name": chn_name,
        "dept_name": dept_name,
        "position": position,
        "avatar_url": f"https://r.hrc.woa.com/photo/150/{eng_name}.png?default_when_absent=true" if eng_name else "",
    }


# ── Extended API Handler ─────────────────────────────────────────────────

class PlaygroundAPIHandler(http.server.SimpleHTTPRequestHandler):
    """Extended HTTP handler adding With-specific endpoints."""

    protocol_version = "HTTP/1.1"

    # Class-level config (set before starting server)
    _configs: list = []
    _all_configs: list = []
    _database: str = ""
    _baseline: str = ""
    _traceless: bool = True
    _cancel_event: threading.Event = threading.Event()
    _active_connections: list = []
    _active_connections_lock: threading.Lock = threading.Lock()

    @classmethod
    def _cleanup_connections(cls):
        with cls._active_connections_lock:
            for db in cls._active_connections:
                try:
                    db.close()
                except Exception:
                    pass
            cls._active_connections.clear()

    def log_message(self, format, *args):
        pass  # suppress

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

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

    def _get_user(self) -> dict:
        """Get user info, preferring With headers."""
        headers_lower = {k.lower(): v for k, v in self.headers.items()}
        return _get_with_user(headers_lower)

    # ── GET ───────────────────────────────────────────────────────────

    def do_GET(self):
        if self.path == "/":
            self.send_response(302)
            self.send_header("Location", "/playground.html")
            self.end_headers()
            return
        if self.path == "/api/dbms":
            self._handle_dbms_list()
            return
        if self.path == "/api/user":
            self._handle_user_info()
            return
        if self.path == "/api/history":
            self._handle_history_list()
            return
        if self.path == "/api/favorites":
            self._handle_favorites_list()
            return
        super().do_GET()

    # ── POST ──────────────────────────────────────────────────────────

    def do_POST(self):
        if self.path == "/api/execute":
            self._handle_execute_api()
            return
        if self.path == "/api/execute/stream":
            self._handle_execute_stream_api()
            return
        if self.path == "/api/stop":
            self._handle_stop_api()
            return
        if self.path == "/api/history/save":
            self._handle_history_save()
            return
        if self.path == "/api/favorites/add":
            self._handle_favorites_add()
            return
        if self.path == "/api/favorites/toggle":
            self._handle_favorites_toggle()
            return
        if self.path == "/api/dbms/test":
            self._handle_dbms_test()
            return
        self.send_error(404)

    # ── DELETE ────────────────────────────────────────────────────────

    def do_DELETE(self):
        if self.path.startswith("/api/favorites/"):
            fav_id = self.path.split("/")[-1]
            self._handle_favorites_delete(fav_id)
            return
        if self.path.startswith("/api/history/"):
            hist_id = self.path.split("/")[-1]
            self._handle_history_delete(hist_id)
            return
        self.send_error(404)

    # ── API: User Info ────────────────────────────────────────────────

    def _handle_user_info(self):
        user = self._get_user()
        self._respond_json({"ok": True, "user": user})

    # ── API: DBMS List ────────────────────────────────────────────────

    def _load_custom_dbms(self, eng_name: str) -> list:
        """Load custom DBMS configs for a user (from DB)."""
        # TODO: Implement custom DBMS persistence when needed
        return []

    def _handle_dbms_list(self):
        """GET /api/dbms — list all DBMS (built-in + custom merged)."""
        active_names = {c.name for c in self._configs}
        dbms_list = [{"name": c.name, "host": c.host, "port": c.port,
                      "active": c.name in active_names, "enabled": c.enabled,
                      "source": "builtin"}
                     for c in self._all_configs]
        # Merge custom configs
        user = self._get_user()
        eng_name = user.get("eng_name", "anonymous")
        custom_configs = self._load_custom_dbms(eng_name)
        for cc in custom_configs:
            dbms_list.append({
                "name": cc["name"], "host": cc["host"], "port": cc["port"],
                "active": cc["enabled"], "enabled": cc["enabled"],
                "source": "custom", "custom_id": cc["id"],
            })
        self._respond_json({
            "ok": True,
            "database": self._database,
            "baseline": self._baseline,
            "traceless": self._traceless,
            "dbms": dbms_list,
        })

    def _handle_dbms_test(self):
        """POST /api/dbms/test — test connection to a specific DBMS."""
        body = self._read_json()
        name = body.get("name", "").strip()
        if not name:
            self._respond_json({"ok": False, "error": "Missing 'name'"}, 400)
            return
        # Find config
        config = None
        for c in self._all_configs:
            if c.name == name:
                config = c
                break
        if config is None:
            self._respond_json({"ok": False, "error": f"Unknown DBMS: {name}"}, 404)
            return
        try:
            import pymysql
            conn = pymysql.connect(
                host=config.host,
                port=config.port,
                user=config.user,
                password=config.password,
                charset="utf8mb4",
                connect_timeout=5,
            )
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            self._respond_json({"ok": True, "success": True, "version": str(version)})
        except Exception as e:
            self._respond_json({"ok": True, "success": False, "error": str(e)})

    # ── API: History ──────────────────────────────────────────────────

    def _handle_history_list(self):
        user = self._get_user()
        eng_name = user.get("eng_name", "anonymous")
        try:
            cursor = _db_execute(
                "SELECT id, sql_text, dbms_targets, baseline, execution_time_ms, "
                "result_summary, created_at FROM sql_history "
                "WHERE user_name = %s ORDER BY created_at DESC LIMIT 50",
                (eng_name,)
            )
            if cursor is None:
                self._respond_json({"ok": True, "history": [], "note": "DB unavailable"})
                return

            rows = cursor.fetchall()
            history = [{
                "id": r[0],
                "sql_text": r[1],
                "dbms_targets": r[2],
                "baseline": r[3],
                "execution_time_ms": r[4],
                "result_summary": json.loads(r[5]) if isinstance(r[5], str) else r[5],
                "created_at": str(r[6]),
            } for r in rows]
            self._respond_json({"ok": True, "history": history})
        except Exception as e:
            self._respond_json({"ok": False, "error": str(e)}, 500)

    def _handle_history_save(self):
        try:
            body = self._read_json()
        except Exception:
            self._respond_json({"ok": False, "error": "invalid JSON"}, 400)
            return

        user = self._get_user()
        eng_name = user.get("eng_name", "anonymous")
        sql_text = body.get("sql_text", "")
        dbms_targets = body.get("dbms_targets", "")
        baseline = body.get("baseline", "")
        execution_time_ms = body.get("execution_time_ms", 0)
        result_summary = body.get("result_summary", {})

        try:
            _db_execute(
                "INSERT INTO sql_history (user_name, sql_text, dbms_targets, "
                "baseline, execution_time_ms, result_summary) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (eng_name, sql_text, dbms_targets, baseline,
                 execution_time_ms, json.dumps(result_summary, ensure_ascii=False))
            )
            self._respond_json({"ok": True})
        except Exception as e:
            self._respond_json({"ok": False, "error": str(e)}, 500)

    def _handle_history_delete(self, hist_id: str):
        user = self._get_user()
        eng_name = user.get("eng_name", "anonymous")
        try:
            _db_execute(
                "DELETE FROM sql_history WHERE id = %s AND user_name = %s",
                (int(hist_id), eng_name)
            )
            self._respond_json({"ok": True})
        except Exception as e:
            self._respond_json({"ok": False, "error": str(e)}, 500)

    # ── API: Favorites ────────────────────────────────────────────────

    def _handle_favorites_list(self):
        user = self._get_user()
        eng_name = user.get("eng_name", "anonymous")
        try:
            cursor = _db_execute(
                "SELECT id, sql_text, title, dbms_targets, baseline, created_at "
                "FROM sql_favorites WHERE user_name = %s ORDER BY created_at DESC",
                (eng_name,)
            )
            if cursor is None:
                self._respond_json({"ok": True, "favorites": [], "note": "DB unavailable"})
                return

            rows = cursor.fetchall()
            favorites = [{
                "id": r[0],
                "sql_text": r[1],
                "title": r[2],
                "dbms_targets": r[3],
                "baseline": r[4],
                "created_at": str(r[5]),
            } for r in rows]
            self._respond_json({"ok": True, "favorites": favorites})
        except Exception as e:
            self._respond_json({"ok": False, "error": str(e)}, 500)

    def _handle_favorites_add(self):
        try:
            body = self._read_json()
        except Exception:
            self._respond_json({"ok": False, "error": "invalid JSON"}, 400)
            return

        user = self._get_user()
        eng_name = user.get("eng_name", "anonymous")
        sql_text = body.get("sql_text", "")
        title = body.get("title", "")
        dbms_targets = body.get("dbms_targets", "")
        baseline = body.get("baseline", "")

        try:
            _db_execute(
                "INSERT INTO sql_favorites (user_name, sql_text, title, dbms_targets, baseline) "
                "VALUES (%s, %s, %s, %s, %s)",
                (eng_name, sql_text, title, dbms_targets, baseline)
            )
            self._respond_json({"ok": True})
        except Exception as e:
            self._respond_json({"ok": False, "error": str(e)}, 500)

    def _handle_favorites_toggle(self):
        """Toggle favorite: add if not exists, remove if exists."""
        try:
            body = self._read_json()
        except Exception:
            self._respond_json({"ok": False, "error": "invalid JSON"}, 400)
            return

        user = self._get_user()
        eng_name = user.get("eng_name", "anonymous")
        sql_text = body.get("sql_text", "")

        try:
            cursor = _db_execute(
                "SELECT id FROM sql_favorites WHERE user_name = %s AND sql_text = %s",
                (eng_name, sql_text)
            )
            if cursor is None:
                self._respond_json({"ok": False, "error": "DB unavailable"}, 500)
                return

            existing = cursor.fetchone()
            if existing:
                _db_execute("DELETE FROM sql_favorites WHERE id = %s", (existing[0],))
                self._respond_json({"ok": True, "action": "removed"})
            else:
                _db_execute(
                    "INSERT INTO sql_favorites (user_name, sql_text, title, dbms_targets, baseline) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (eng_name, sql_text, body.get("title", ""),
                     body.get("dbms_targets", ""), body.get("baseline", ""))
                )
                self._respond_json({"ok": True, "action": "added"})
        except Exception as e:
            self._respond_json({"ok": False, "error": str(e)}, 500)

    def _handle_favorites_delete(self, fav_id: str):
        user = self._get_user()
        eng_name = user.get("eng_name", "anonymous")
        try:
            _db_execute(
                "DELETE FROM sql_favorites WHERE id = %s AND user_name = %s",
                (int(fav_id), eng_name)
            )
            self._respond_json({"ok": True})
        except Exception as e:
            self._respond_json({"ok": False, "error": str(e)}, 500)

    # ── API: Stop Execution ───────────────────────────────────────────

    def _handle_stop_api(self):
        self._cancel_event.set()
        self._cleanup_connections()
        log.info("Execution stop requested via /api/stop")
        self._respond_json({"ok": True, "message": "Execution cancelled"})

    # ── API: Execute (standard) ───────────────────────────────────────

    def _handle_execute_api(self):
        import concurrent.futures
        from .executor import DBConnection
        from .explain import is_explain_stmt, get_explain_variants

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
        configs_map = self._get_all_configs_map()

        if not requested_dbms:
            requested_dbms = list(configs_map.keys())

        targets = [configs_map[name] for name in requested_dbms if name in configs_map]
        if not targets:
            self._respond_json({"ok": False, "error": "no valid DBMS targets"}, 400)
            return

        database = self._database
        cancel = self._cancel_event

        from .mtr import MtrParser
        from .mtr.nodes import MtrCommandType
        _mtr_parser = MtrParser("<playground>")
        try:
            _mtr_test = _mtr_parser.parse_text(sql_text)
        except Exception as e:
            self._respond_json({"ok": False, "error": f"SQL parse error: {e}"}, 400)
            return
        stmts = [cmd.argument for cmd in _mtr_test.commands
                 if cmd.cmd_type in (MtrCommandType.SQL, MtrCommandType.EVAL,
                                     MtrCommandType.QUERY,
                                     MtrCommandType.QUERY_VERTICAL,
                                     MtrCommandType.QUERY_HORIZONTAL)]

        def _exec_on_dbms(config):
            import uuid
            result = {"name": config.name, "statements": [], "error": None, "cancelled": False}
            use_sandbox = sandbox and config.protocol != "oracle"
            temp_db = f"_rosetta_sandbox_{uuid.uuid4().hex[:8]}" if use_sandbox else None
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
                    stmt_result = {"sql": sql, "columns": None, "rows": None,
                                   "error": None, "affected_rows": 0, "elapsed_ms": 0}

                    if is_explain_stmt(sql):
                        variants = get_explain_variants(sql, config.protocol)
                        explain_results = []
                        total_elapsed = 0.0
                        for variant in variants:
                            if cancel.is_set():
                                result["cancelled"] = True
                                break
                            vr = {"format": variant["format"], "columns": None,
                                  "rows": None, "error": None, "elapsed_ms": 0}
                            try:
                                t0 = _time.monotonic()
                                db.cursor.execute(variant["sql"])
                                if db.cursor.description:
                                    vr["columns"] = [desc[0] for desc in db.cursor.description]
                                    rows = db.cursor.fetchall()
                                    vr["rows"] = [[_format_val(c) for c in row] for row in rows]
                                vr["elapsed_ms"] = round((_time.monotonic() - t0) * 1000, 3)
                            except Exception as e:
                                vr["elapsed_ms"] = round((_time.monotonic() - t0) * 1000, 3)
                                vr["error"] = str(e)
                            total_elapsed += vr["elapsed_ms"]
                            explain_results.append(vr)
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
                                stmt_result["columns"] = [desc[0] for desc in db.cursor.description]
                                rows = db.cursor.fetchall()
                                stmt_result["rows"] = [[_format_val(c) for c in row] for row in rows]
                            else:
                                stmt_result["affected_rows"] = db.cursor.rowcount or 0
                            stmt_result["elapsed_ms"] = round((_time.monotonic() - t0) * 1000, 3)
                        except Exception as e:
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
                            stmt_result["elapsed_ms"] = round((_time.monotonic() - t0) * 1000, 3)

                    result["statements"].append(stmt_result)
            finally:
                if use_sandbox and temp_db:
                    try:
                        db.cursor.execute(f"DROP DATABASE IF EXISTS `{temp_db}`")
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

        exec_timeout = max(30, len(stmts) * 30 + 15)
        watchdog_stop = threading.Event()

        def _watchdog():
            if watchdog_stop.wait(timeout=exec_timeout):
                return
            if not cancel.is_set():
                log.warning("Watchdog timeout (%ds), force killing", exec_timeout)
                cancel.set()
                self._cleanup_connections()

        wd_thread = threading.Thread(target=_watchdog, daemon=True)
        wd_thread.start()

        results = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(targets)) as pool:
                futures = {pool.submit(_exec_on_dbms, c): c for c in targets}
                for fut in concurrent.futures.as_completed(futures):
                    r = fut.result()
                    results[r["name"]] = r
        finally:
            watchdog_stop.set()
            self._cleanup_connections()

        cancelled = any(r.get("cancelled") for r in results.values())
        self._respond_json({"ok": True, "results": results, "cancelled": cancelled})

    # ── API: Execute (SSE stream) ─────────────────────────────────────

    def _handle_execute_stream_api(self):
        import concurrent.futures
        from .executor import DBConnection
        from .explain import is_explain_stmt, get_explain_variants

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
        configs_map = self._get_all_configs_map()

        if not requested_dbms:
            requested_dbms = list(configs_map.keys())

        targets = [configs_map[name] for name in requested_dbms if name in configs_map]
        if not targets:
            self._respond_json({"ok": False, "error": "no valid DBMS targets"}, 400)
            return

        database = self._database
        cancel = self._cancel_event

        from .mtr import MtrParser
        from .mtr.nodes import MtrCommandType
        _mtr_parser = MtrParser("<playground>")
        try:
            _mtr_test = _mtr_parser.parse_text(sql_text)
        except Exception as e:
            self._respond_json({"ok": False, "error": f"SQL parse error: {e}"}, 400)
            return
        stmts = [cmd.argument for cmd in _mtr_test.commands
                 if cmd.cmd_type in (MtrCommandType.SQL, MtrCommandType.EVAL,
                                     MtrCommandType.QUERY,
                                     MtrCommandType.QUERY_VERTICAL,
                                     MtrCommandType.QUERY_HORIZONTAL)]
        total = len(targets)

        try:
            raw_sock = self.connection
            raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            raw_sock = None

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self._send_cors_headers()
        self.end_headers()
        self.wfile.flush()

        try:
            heartbeat = b": heartbeat\n\n"
            if raw_sock:
                raw_sock.sendall(heartbeat)
            else:
                self.wfile.write(heartbeat)
                self.wfile.flush()
        except Exception:
            return

        sse_lock = threading.Lock()
        _client_gone = False

        def _send_sse(event: str, data: dict):
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
                    if not cancel.is_set():
                        cancel.set()
                        self._cleanup_connections()

        def _exec_on_dbms(config, index):
            import uuid
            result = {"name": config.name, "statements": [], "error": None, "cancelled": False}

            if cancel.is_set():
                result["cancelled"] = True
                _send_sse("progress", {"name": config.name, "index": index, "total": total, "result": result})
                return result

            use_sandbox = sandbox and config.protocol != "oracle"
            temp_db = f"_rosetta_sandbox_{uuid.uuid4().hex[:8]}" if use_sandbox else None
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
                    _send_sse("progress", {"name": config.name, "index": index, "total": total, "result": result})
                    return result
                result["error"] = f"Connection failed: {e}"
                _send_sse("progress", {"name": config.name, "index": index, "total": total, "result": result})
                return result

            try:
                total_stmts = len(stmts)
                for si, sql in enumerate(stmts):
                    if cancel.is_set():
                        result["cancelled"] = True
                        break
                    stmt_result = {"sql": sql, "columns": None, "rows": None,
                                   "error": None, "affected_rows": 0, "elapsed_ms": 0}

                    if is_explain_stmt(sql):
                        variants = get_explain_variants(sql, config.protocol)
                        explain_results = []
                        total_elapsed = 0.0
                        for variant in variants:
                            if cancel.is_set():
                                result["cancelled"] = True
                                break
                            vr = {"format": variant["format"], "columns": None,
                                  "rows": None, "error": None, "elapsed_ms": 0}
                            try:
                                t0 = _time.monotonic()
                                db.cursor.execute(variant["sql"])
                                if db.cursor.description:
                                    vr["columns"] = [desc[0] for desc in db.cursor.description]
                                    rows = db.cursor.fetchall()
                                    vr["rows"] = [[_format_val(c) for c in row] for row in rows]
                                vr["elapsed_ms"] = round((_time.monotonic() - t0) * 1000, 3)
                            except Exception as e:
                                vr["elapsed_ms"] = round((_time.monotonic() - t0) * 1000, 3)
                                vr["error"] = str(e)
                            total_elapsed += vr["elapsed_ms"]
                            explain_results.append(vr)
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
                                stmt_result["columns"] = [desc[0] for desc in db.cursor.description]
                                rows = db.cursor.fetchall()
                                stmt_result["rows"] = [[_format_val(c) for c in row] for row in rows]
                            else:
                                stmt_result["affected_rows"] = db.cursor.rowcount or 0
                            stmt_result["elapsed_ms"] = round((_time.monotonic() - t0) * 1000, 3)
                        except Exception as e:
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
                            stmt_result["elapsed_ms"] = round((_time.monotonic() - t0) * 1000, 3)

                    result["statements"].append(stmt_result)
                    _send_sse("stmt_progress", {
                        "name": config.name, "index": index, "total": total,
                        "stmt_index": si + 1, "stmt_total": total_stmts,
                    })
            finally:
                if use_sandbox and temp_db:
                    try:
                        db.cursor.execute(f"DROP DATABASE IF EXISTS `{temp_db}`")
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

            _send_sse("progress", {"name": config.name, "index": index, "total": total, "result": result})
            return result

        exec_timeout = max(30, len(stmts) * 30 + 15)
        watchdog_stop = threading.Event()

        def _watchdog():
            if watchdog_stop.wait(timeout=exec_timeout):
                return
            if not cancel.is_set():
                log.warning("Watchdog timeout (%ds), force killing", exec_timeout)
                cancel.set()
                self._cleanup_connections()
                _send_sse("cancelled", {"ok": False, "message": f"Execution timed out after {exec_timeout}s"})

        wd_thread = threading.Thread(target=_watchdog, daemon=True)
        wd_thread.start()

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(targets)) as pool:
                futures = {}
                for i, c in enumerate(targets):
                    futures[pool.submit(_exec_on_dbms, c, i + 1)] = c
                for fut in concurrent.futures.as_completed(futures):
                    fut.result()

            if cancel.is_set():
                _send_sse("cancelled", {"ok": False, "message": "Execution cancelled"})
            else:
                _send_sse("done", {"ok": True})
        except Exception as e:
            _send_sse("error", {"error": str(e)})
        finally:
            watchdog_stop.set()
            self._cleanup_connections()
            self.close_connection = True


# ── Value formatter ──────────────────────────────────────────────────────

def _format_val(v):
    """Format DB value for JSON serialization."""
    import datetime
    import decimal
    if v is None:
        return None
    if isinstance(v, datetime.datetime):
        return v.isoformat()
    if isinstance(v, datetime.date):
        return v.isoformat()
    if isinstance(v, datetime.timedelta):
        return str(v)
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except Exception:
            return v.hex()
    if isinstance(v, bytearray):
        return bytes(v).hex()
    return v


# ── Server launcher ──────────────────────────────────────────────────────

class PlaygroundServer:
    """Manages the Playground HTTP server."""

    def __init__(self, directory: str, port: int = 0,
                 configs=None, all_configs=None,
                 database: str = "", baseline: str = "", traceless: bool = True):
        self.directory = os.path.abspath(directory)
        self.port = port
        self.configs = configs or []
        self.all_configs = all_configs or self.configs
        self.database = database
        self.baseline = baseline
        self.traceless = traceless
        self._server = None
        self._thread = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def base_url(self) -> str:
        return f"http://localhost:{self.port}"

    def start(self) -> str:
        if self.running:
            return self.base_url
        if self.port == 0:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", 0))
                self.port = s.getsockname()[1]
        os.makedirs(self.directory, exist_ok=True)

        # Generate playground page — use With-enhanced version
        import shutil
        _pkg_dir = os.path.dirname(os.path.abspath(__file__))
        _src_html = os.path.join(_pkg_dir, "playground_with.html")
        _dst_html = os.path.join(self.directory, "playground.html")
        if os.path.exists(_src_html):
            shutil.copy(_src_html, _dst_html)
            log.info("Playground (With edition) page written: %s", _dst_html)
        else:
            from .reporter.history import generate_playground_html
            generate_playground_html(self.directory)

        # Always try to generate index as well
        try:
            from .reporter.history import generate_index_html
            generate_index_html(self.directory)
        except Exception:
            pass

        PlaygroundAPIHandler._configs = self.configs
        PlaygroundAPIHandler._all_configs = self.all_configs
        PlaygroundAPIHandler._database = self.database
        PlaygroundAPIHandler._baseline = self.baseline
        PlaygroundAPIHandler._traceless = self.traceless

        handler = lambda *a, **kw: PlaygroundAPIHandler(*a, directory=self.directory, **kw)

        from socketserver import ThreadingMixIn

        class _ThreadingHTTPServer(ThreadingMixIn, http.server.HTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        self._server = _ThreadingHTTPServer(("0.0.0.0", self.port), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        log.info("Playground server started on port %d", self.port)
        return self.base_url

    def stop(self):
        if self._server:
            PlaygroundAPIHandler._cleanup_connections()
            self._server.shutdown()
            self._server.server_close()
            self._thread = None
            self._server = None


def main():
    parser = argparse.ArgumentParser(description="Rosetta Playground Server for With Platform")
    parser.add_argument("-c", "--config",
                        default=os.environ.get("ROSETTA_CONFIG", "with_config.json"),
                        help="DBMS config file (env: ROSETTA_CONFIG)")
    parser.add_argument("-p", "--port", type=int, default=19527, help="HTTP port")
    parser.add_argument("-d", "--database", default="cross_dbms_test_db", help="Default database")
    parser.add_argument("-o", "--output-dir", default="results", help="Results directory")
    args = parser.parse_args()

    from .config import load_config
    all_configs = load_config(args.config)
    configs = [c for c in all_configs if c.enabled]

    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    server = PlaygroundServer(
        directory=output_dir, port=args.port,
        configs=configs, all_configs=all_configs,
        database=args.database,
    )
    url = server.start()
    print(f"Playground server started at {url}", flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    server.stop()


if __name__ == "__main__":
    main()
