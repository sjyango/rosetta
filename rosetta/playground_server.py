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
import subprocess
import sys
import threading
import time as _time
from typing import Optional
from urllib.parse import urlparse, parse_qs

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
    eng_name = headers.get("x-with-engname", "")
    chn_name = headers.get("x-with-chnname", "")
    dept_name = headers.get("x-with-deptname", "")
    position = headers.get("x-with-positionname", "")

    # Also try standard nginx/forwarded headers
    if not eng_name:
        eng_name = headers.get("x-forwarded-user", "")

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
    _config_path: str = ""
    _database: str = ""
    _baseline: str = ""
    _traceless: bool = True
    _cancel_event: threading.Event = threading.Event()
    _active_connections: list = []
    _active_connections_lock: threading.Lock = threading.Lock()

    # ── Health Monitor (class-level) ──
    _health_status: dict = {}          # name -> {"connected": bool, "host": str, "port": int, ...}
    _health_lock: threading.Lock = threading.Lock()
    _health_monitor_thread: Optional[threading.Thread] = None
    _health_monitor_stop: threading.Event = threading.Event()
    _health_monitor_config: dict = {}  # loaded from config
    _restart_in_progress: dict = {}    # name -> True if restart is ongoing
    _restart_lock: threading.Lock = threading.Lock()
    _restart_retry_count: dict = {}    # name -> retry count for current failure cycle
    _restart_script: str = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        "..", "scripts", "restart_dbms.sh")

    @classmethod
    def _reload_configs(cls):
        """Re-read config file and update class-level config caches."""
        if not cls._config_path:
            return
        if os.path.isfile(cls._config_path):
            from .config import load_config
            try:
                new_all = load_config(cls._config_path)
                cls._all_configs = new_all
                cls._configs = [c for c in new_all if c.enabled]
                log.debug("Configs reloaded from %s: %d total, %d enabled",
                          cls._config_path, len(new_all), len(cls._configs))
            except Exception as e:
                log.warning("Failed to reload configs: %s", e)

    @classmethod
    def _cleanup_connections(cls):
        with cls._active_connections_lock:
            for db in cls._active_connections:
                try:
                    db.close()
                except Exception:
                    pass
            cls._active_connections.clear()

    @classmethod
    def _get_all_configs_map(cls) -> dict:
        """Return a mapping of config name -> config object for all DBMS."""
        return {c.name: c for c in cls._all_configs}

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
        try:
            payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self._send_cors_headers()
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _get_user(self) -> dict:
        """Get user info, preferring With headers."""
        headers_lower = {k.lower(): v for k, v in self.headers.items()}
        return _get_with_user(headers_lower)

    # ── GET ───────────────────────────────────────────────────────────

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/":
            self.send_response(302)
            self.send_header("Location", "/playground.html")
            self.end_headers()
            return
        if path == "/api/dbms":
            self._handle_dbms_list()
            return
        if path == "/api/user":
            self._handle_user_info()
            return
        if path == "/api/history":
            self._handle_history_list()
            return
        if path == "/api/favorites":
            self._handle_favorites_list()
            return
        if path == "/api/health":
            self._handle_health_check()
            return
        if path == "/api/dbms/health":
            self._handle_per_dbms_health()
            return
        if path.startswith("/api/dbms/health/"):
            self._handle_single_dbms_health(path.split("/api/dbms/health/", 1)[-1])
            return
        if path == "/api/config/raw":
            self._handle_config_raw_get()
            return
        if path == "/playground.html":
            self._serve_playground_html()
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
        if self.path == "/api/dbms/restart":
            self._handle_dbms_restart()
            return
        if self.path == "/api/dbms/custom/save":
            self._handle_custom_dbms_save()
            return
        if self.path == "/api/config/raw":
            self._handle_config_raw_save()
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
        if self.path.startswith("/api/dbms/custom/"):
            cust_name = self.path.split("/")[-1]
            self._handle_custom_dbms_delete(cust_name)
            return
        self.send_error(404)

    # ── API: User Info ────────────────────────────────────────────────

    def _handle_user_info(self):
        user = self._get_user()
        self._respond_json({"ok": True, "user": user})

    # ── Serve Playground HTML (dynamic config injection) ───────────────

    def _serve_playground_html(self):
        """Serve playground.html with live config injected (not stale baked data).

        On every page load, re-reads the config file and injects the latest
        DBMS list into the HTML response, so refreshing the page always
        reflects the current with_config.json state.
        """
        import re
        cls = type(self)
        cls._reload_configs()

        _pkg_dir = os.path.dirname(os.path.abspath(__file__))
        _src_html = os.path.join(_pkg_dir, "playground_with.html")
        if not os.path.exists(_src_html):
            self.send_error(404)
            return

        try:
            with open(_src_html, "r", encoding="utf-8") as f:
                html = f.read()
        except Exception as e:
            self.send_error(500)
            return

        # Strip any stale EMBEDDED_xxx lines left over from previous bakes
        html = re.sub(
            r'\n?var EMBEDDED_DBMS=.*?;var EMBEDDED_DATABASE=.*?;'
            r'var EMBEDDED_BASELINE=.*?;var EMBEDDED_TRACELESS=.*?;'
            r'(?:var EMBEDDED_VERSION=.*?;)?\n?',
            '', html, count=1
        )

        # Build fresh config JSON
        active_names = {c.name for c in cls._configs}
        dbms_list = []
        for c in cls._all_configs:
            dbms_list.append({
                "name": c.name, "host": c.host, "port": c.port,
                "active": c.name in active_names, "enabled": c.enabled,
                "type": getattr(c, "protocol", "mysql"),
                "database": getattr(c, "service_name", ""),
                "source": "builtin",
            })
        dbms_json = json.dumps(dbms_list, ensure_ascii=False)

        from importlib.metadata import version as _get_version
        try:
            _version = _get_version("rosetta-sql")
        except Exception:
            _version = "1.5.1"

        embedded = (
            "var EMBEDDED_DBMS=" + dbms_json + ";"
            "var EMBEDDED_DATABASE=" + json.dumps(cls._database or "", ensure_ascii=False) + ";"
            "var EMBEDDED_BASELINE=" + json.dumps(cls._baseline or "", ensure_ascii=False) + ";"
            "var EMBEDDED_TRACELESS=" + json.dumps(cls._traceless) + ";"
            "var EMBEDDED_VERSION=" + json.dumps(_version, ensure_ascii=False) + ";\n"
        )
        # Inject after the first opening <script> tag
        html = html.replace("<script>\n", "<script>\n" + embedded, 1)

        payload = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self._send_cors_headers()
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    # ── API: DBMS List ────────────────────────────────────────────────

    @classmethod
    def _ensure_custom_dbms_table(cls):
        """Create the custom_dbms table if it doesn't exist."""
        try:
            db = _get_db()
            if db is None:
                return
            cursor = db.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS custom_dbms (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(128) NOT NULL,
                    protocol VARCHAR(32) DEFAULT 'mysql',
                    host VARCHAR(256) NOT NULL DEFAULT '127.0.0.1',
                    port INT NOT NULL DEFAULT 3306,
                    username VARCHAR(128) NOT NULL DEFAULT 'root',
                    password VARCHAR(256) NOT NULL DEFAULT '',
                    database_name VARCHAR(128) DEFAULT '',
                    owner VARCHAR(128) NOT NULL DEFAULT '',
                    enabled TINYINT(1) NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_owner_name (owner, name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cursor.close()
        except Exception as e:
            log.error("Failed to ensure custom_dbms table: %s", e)

    @classmethod
    def _ensure_history_table(cls):
        """Create the sql_history table if it doesn't exist."""
        try:
            db = _get_db()
            if db is None:
                return
            cursor = db.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sql_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_name VARCHAR(128) NOT NULL DEFAULT '',
                    sql_text TEXT NOT NULL,
                    dbms_targets VARCHAR(512) DEFAULT '',
                    baseline VARCHAR(128) DEFAULT '',
                    execution_time_ms INT DEFAULT 0,
                    result_summary JSON DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_created (user_name, created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cursor.close()
        except Exception as e:
            log.error("Failed to ensure sql_history table: %s", e)

    @classmethod
    def _ensure_favorites_table(cls):
        """Create the sql_favorites table if it doesn't exist."""
        try:
            db = _get_db()
            if db is None:
                return
            cursor = db.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sql_favorites (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_name VARCHAR(128) NOT NULL DEFAULT '',
                    sql_text TEXT NOT NULL,
                    title VARCHAR(256) DEFAULT '',
                    dbms_targets VARCHAR(512) DEFAULT '',
                    baseline VARCHAR(128) DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_user_sql (user_name, sql_text(255))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cursor.close()
        except Exception as e:
            log.error("Failed to ensure sql_favorites table: %s", e)

    @classmethod
    def _ensure_all_tables(cls):
        """Ensure all required tables exist."""
        cls._ensure_custom_dbms_table()
        cls._ensure_history_table()
        cls._ensure_favorites_table()

    def _load_custom_dbms(self, eng_name: str) -> list:
        """Load custom DBMS configs for a user from MySQL."""
        if not eng_name:
            return []
        cls = type(self)
        cls._ensure_custom_dbms_table()
        db = _get_db()
        if db is None:
            return []
        try:
            cursor = db.cursor()
            cursor.execute(
                "SELECT id, name, protocol, host, port, username, password, "
                "database_name, enabled FROM custom_dbms WHERE owner=%s ORDER BY name",
                (eng_name,)
            )
            rows = cursor.fetchall()
            cursor.close()
            result = []
            for r in rows:
                result.append({
                    "custom_id": r[0],
                    "name": r[1],
                    "protocol": r[2] or "mysql",
                    "host": r[3],
                    "port": r[4],
                    "user": r[5],
                    "password": r[6],
                    "database": r[7] or "",
                    "enabled": bool(r[8]),
                })
            return result
        except Exception as e:
            log.error("Failed to load custom DBMS: %s", e)
            return []

    def _handle_custom_dbms_save(self):
        """POST /api/dbms/custom/save — create or update a custom DBMS."""
        user = self._get_user()
        eng_name = user.get("eng_name", "") or "anonymous"

        body = self._read_json()
        name = (body.get("name") or "").strip()
        if not name:
            self._respond_json({"ok": False, "error": "DBMS name is required"}, 400)
            return

        protocol = body.get("protocol", "mysql")
        host = body.get("host", "127.0.0.1")
        port = int(body.get("port", 3306))
        username = body.get("user", "root")
        password = body.get("password", "")
        database_name = body.get("database", "")
        enabled = 1 if body.get("enabled", True) else 0

        type(self)._ensure_custom_dbms_table()
        db = _get_db()
        if db is None:
            self._respond_json({"ok": False, "error": "Database unavailable"}, 500)
            return

        try:
            cursor = db.cursor()
            cursor.execute(
                "SELECT id FROM custom_dbms WHERE owner=%s AND name=%s",
                (eng_name, name)
            )
            existing = cursor.fetchone()
            if existing:
                cursor.execute(
                    "UPDATE custom_dbms SET protocol=%s, host=%s, port=%s, "
                    "username=%s, password=%s, database_name=%s, enabled=%s "
                    "WHERE id=%s",
                    (protocol, host, port, username, password, database_name,
                     enabled, existing[0])
                )
                cursor.close()
                self._respond_json({"ok": True, "action": "updated",
                                    "name": name, "custom_id": existing[0]})
            else:
                cursor.execute(
                    "INSERT INTO custom_dbms (name, protocol, host, port, "
                    "username, password, database_name, owner, enabled) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (name, protocol, host, port, username, password,
                     database_name, eng_name, enabled)
                )
                new_id = cursor.lastrowid
                cursor.close()
                self._respond_json({"ok": True, "action": "created",
                                    "name": name, "custom_id": new_id})
        except Exception as e:
            log.error("Failed to save custom DBMS: %s", e)
            self._respond_json({"ok": False, "error": str(e)}, 500)

    def _handle_custom_dbms_delete(self, cust_name: str):
        """DELETE /api/dbms/custom/<name> — delete a custom DBMS."""
        user = self._get_user()
        eng_name = user.get("eng_name", "") or "anonymous"

        type(self)._ensure_custom_dbms_table()
        db = _get_db()
        if db is None:
            self._respond_json({"ok": False, "error": "Database unavailable"}, 500)
            return

        try:
            cursor = db.cursor()
            cursor.execute(
                "DELETE FROM custom_dbms WHERE owner=%s AND name=%s",
                (eng_name, cust_name)
            )
            affected = cursor.rowcount
            cursor.close()
            if affected > 0:
                self._respond_json({"ok": True, "action": "deleted",
                                    "name": cust_name})
            else:
                self._respond_json({"ok": False,
                                    "error": "Custom DBMS not found"}, 404)
        except Exception as e:
            log.error("Failed to delete custom DBMS: %s", e)
            self._respond_json({"ok": False, "error": str(e)}, 500)

    # ── API: Config Raw ───────────────────────────────────────────────

    def _handle_config_raw_get(self):
        """GET /api/config/raw — return the full config.json content."""
        config_path = self._config_path
        if not config_path or not os.path.isfile(config_path):
            self._respond_json({"ok": False, "error": "Config file not found"}, 404)
            return
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                content = f.read()
            self._respond_json({"ok": True, "content": content})
        except Exception as e:
            self._respond_json({"ok": False, "error": str(e)}, 500)

    def _handle_config_raw_save(self):
        """POST /api/config/raw — save config.json content."""
        config_path = self._config_path
        if not config_path:
            self._respond_json({"ok": False, "error": "Config path not configured"}, 500)
            return
        body = self._read_json()
        content = body.get("content", "")
        if not content.strip():
            self._respond_json({"ok": False, "error": "Content is empty"}, 400)
            return
        # Validate JSON
        try:
            parsed = json.loads(content)
            if "databases" not in parsed:
                self._respond_json({"ok": False,
                    "error": "Config must contain 'databases' key"}, 400)
                return
        except json.JSONDecodeError as e:
            self._respond_json({"ok": False, "error": f"Invalid JSON: {e}"}, 400)
            return
        # Backup before overwrite
        backup_path = config_path + ".bak"
        try:
            import shutil
            shutil.copy2(config_path, backup_path)
        except Exception:
            pass  # best-effort backup
        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(parsed, f, indent=2, ensure_ascii=False)
            # Reload configs
            from .config import load_config
            new_configs = load_config(config_path)
            PlaygroundAPIHandler._all_configs = new_configs
            PlaygroundAPIHandler._configs = [c for c in new_configs if c.enabled]
            self._respond_json({"ok": True, "action": "saved",
                                "databases": len(parsed.get("databases", []))})
        except Exception as e:
            self._respond_json({"ok": False, "error": f"Failed to save: {e}"}, 500)

    def _handle_dbms_list(self):
        """GET /api/dbms — list all DBMS (built-in + custom merged).

        Re-reads config file on each request to reflect live edits.
        """
        cls = type(self)
        cls._reload_configs()
        active_names = {c.name for c in self._configs}
        dbms_list = [{"name": c.name, "host": c.host, "port": c.port,
                      "active": c.name in active_names, "enabled": c.enabled,
                      "type": getattr(c, "protocol", "mysql"),
                      "database": getattr(c, "service_name", ""),
                      "source": "builtin"}
                     for c in self._all_configs]
        # Merge custom configs
        user = self._get_user()
        eng_name = user.get("eng_name", "") or "anonymous"
        custom_configs = self._load_custom_dbms(eng_name)
        for cc in custom_configs:
            dbms_list.append({
                "name": cc["name"], "host": cc["host"], "port": cc["port"],
                "active": cc["enabled"], "enabled": cc["enabled"],
                "type": cc.get("protocol", "mysql"),
                "database": cc.get("database", ""),
                "source": "custom", "custom_id": cc.get("custom_id"),
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
        # Find config (built-in first, then custom)
        config = None
        for c in self._all_configs:
            if c.name == name:
                config = (c.host, c.port, c.user, c.password)
                break
        if config is None:
            user = self._get_user()
            eng_name = user.get("eng_name", "anonymous")
            customs = self._load_custom_dbms(eng_name)
            for cc in customs:
                if cc["name"] == name:
                    config = (cc["host"], cc["port"], cc["user"], cc["password"])
                    break
        if config is None:
            self._respond_json({"ok": False, "error": f"Unknown DBMS: {name}"}, 404)
            return
        try:
            import pymysql
            conn = pymysql.connect(
                host=config[0],
                port=config[1],
                user=config[2],
                password=config[3],
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

    # ── API: Health ──────────────────────────────────────────────────

    def _handle_health_check(self):
        """GET /api/health — check if the system is functional.
        Tests connectivity to at least one enabled DBMS."""
        from importlib.metadata import version as _get_version
        try:
            _version = _get_version("rosetta-sql")
        except Exception:
            _version = "1.5.1"
        # Collect enabled active configs
        active_configs = [c for c in self._all_configs
                          if getattr(c, "enabled", True)]
        if not active_configs:
            self._respond_json({"ok": True, "status": "unhealthy",
                                "reason": "No enabled DBMS", "version": _version})
            return

        # Try to connect to at least one enabled DBMS
        import pymysql
        connected_count = 0
        for c in active_configs:
            try:
                conn = pymysql.connect(
                    host=c.host, port=c.port, user=c.user,
                    password=c.password,
                    charset="utf8mb4", connect_timeout=3,
                )
                conn.ping(reconnect=False)
                conn.close()
                connected_count += 1
            except Exception:
                pass

        if connected_count > 0:
            self._respond_json({"ok": True, "status": "healthy",
                                "connected": connected_count,
                                "total": len(active_configs),
                                "version": _version})
        else:
            self._respond_json({"ok": True, "status": "unhealthy",
                                "reason": "No DBMS reachable",
                                "total": len(active_configs),
                                "version": _version})

    # ── API: Per-DBMS Health ─────────────────────────────────────────

    def _handle_per_dbms_health(self):
        """GET /api/dbms/health — check health of all enabled DBMS individually.

        Returns per-DBMS status including: connected, host, port, version,
        latency, restart_available, restart_in_progress.
        """
        cls = type(self)
        cls._reload_configs()
        active_configs = [c for c in cls._all_configs if getattr(c, "enabled", True)]
        if not active_configs:
            self._respond_json({"ok": True, "dbms": [], "total": 0,
                                "healthy": 0, "unhealthy": 0})
            return

        import concurrent.futures
        dbms_list = []

        def _check_one(cfg):
            import pymysql
            start = _time.time()
            name = cfg.name
            host = cfg.host
            port = cfg.port
            restart_cfg = getattr(cfg, "restart", None) or {}
            restart_available = bool(restart_cfg.get("enabled") and restart_cfg.get("command"))
            with cls._restart_lock:
                restart_in_progress = cls._restart_in_progress.get(name, False)

            # Check port reachability quickly
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((host, port))
                s.close()
                port_ok = True
            except Exception:
                port_ok = False

            latency_ms = round((_time.time() - start) * 1000, 2)

            result = {
                "name": name, "host": host, "port": port,
                "port_reachable": port_ok, "connected": False,
                "version": None, "latency_ms": latency_ms,
                "restart_available": restart_available,
                "restart_in_progress": restart_in_progress,
                "error": None,
            }

            if port_ok:
                try:
                    conn = pymysql.connect(
                        host=host, port=port, user=cfg.user,
                        password=cfg.password,
                        charset="utf8mb4", connect_timeout=3,
                    )
                    cur = conn.cursor()
                    cur.execute("SELECT VERSION()")
                    row = cur.fetchone()
                    result["version"] = row[0] if row else "unknown"
                    result["connected"] = True
                    result["latency_ms"] = round((_time.time() - start) * 1000, 2)
                    cur.close()
                    conn.close()
                except Exception as e:
                    result["error"] = str(e)[:200]
            else:
                result["error"] = "Port unreachable"

            # Update class-level cache
            with cls._health_lock:
                cls._health_status[name] = result
            return result

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(active_configs)) as pool:
            futs = [pool.submit(_check_one, c) for c in active_configs]
            for fut in concurrent.futures.as_completed(futs):
                try:
                    dbms_list.append(fut.result())
                except Exception:
                    pass

        dbms_list.sort(key=lambda d: d["name"])
        healthy = sum(1 for d in dbms_list if d["connected"])
        hc = cls._health_monitor_config
        self._respond_json({
            "ok": True, "dbms": dbms_list, "total": len(dbms_list),
            "healthy": healthy, "unhealthy": len(dbms_list) - healthy,
            "_monitor_enabled": hc.get("enabled", False),
            "_monitor_interval": hc.get("interval_seconds", 30),
            "_monitor_auto_restart": hc.get("auto_restart", True),
        })

    def _handle_single_dbms_health(self, name: str):
        """GET /api/dbms/health/<name> — check health of a single DBMS."""
        from urllib.parse import unquote
        name = unquote(name)
        cls = type(self)
        cls._reload_configs()

        cfg = None
        for c in cls._all_configs:
            if c.name == name:
                cfg = c
                break
        if cfg is None:
            self._respond_json({"ok": False, "error": f"Unknown DBMS: {name}"}, 404)
            return

        # Quick check
        import pymysql
        start = _time.time()
        port_ok = False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((cfg.host, cfg.port))
            s.close()
            port_ok = True
        except Exception:
            pass

        result = {
            "name": name, "host": cfg.host, "port": cfg.port,
            "port_reachable": port_ok, "connected": False,
            "version": None, "latency_ms": round((_time.time() - start) * 1000, 2),
        }
        if port_ok:
            try:
                conn = pymysql.connect(
                    host=cfg.host, port=cfg.port, user=cfg.user,
                    password=cfg.password,
                    charset="utf8mb4", connect_timeout=3,
                )
                cur = conn.cursor()
                cur.execute("SELECT VERSION()")
                row = cur.fetchone()
                result["version"] = row[0] if row else "unknown"
                result["connected"] = True
                result["latency_ms"] = round((_time.time() - start) * 1000, 2)
                cur.close()
                conn.close()
            except Exception as e:
                result["error"] = str(e)[:200]
        else:
            result["error"] = "Port unreachable"

        with cls._health_lock:
            cls._health_status[name] = result
        self._respond_json({"ok": True, "dbms": result})

    # ── API: DBMS Restart ─────────────────────────────────────────────

    def _handle_dbms_restart(self):
        """POST /api/dbms/restart — restart a DBMS instance.

        Body: {"name": "mysql-9.6"}
        Returns progress via the restart flow.
        """
        body = self._read_json()
        name = body.get("name", "").strip()
        if not name:
            self._respond_json({"ok": False, "error": "Missing 'name'"}, 400)
            return

        cls = type(self)

        # Find config
        cfg = None
        for c in cls._all_configs:
            if c.name == name:
                cfg = c
                break

        if cfg is None:
            self._respond_json({"ok": False, "error": f"Unknown DBMS: {name}"}, 404)
            return

        restart_cfg = getattr(cfg, "restart", None) or {}
        if not restart_cfg.get("enabled"):
            self._respond_json({"ok": False,
                                "error": f"Restart not configured for {name}"}, 400)
            return

        # Check if restart already in progress
        with cls._restart_lock:
            if cls._restart_in_progress.get(name):
                self._respond_json({"ok": False,
                                    "error": f"Restart already in progress for {name}"}, 409)
                return
            cls._restart_in_progress[name] = True

        # Extract restart info
        cmd = restart_cfg.get("command", "")
        ssh_host = cls._health_monitor_config.get("ssh_host", "21.6.101.185")
        ssh_user = cls._health_monitor_config.get("ssh_user", "root")
        ssh_timeout = str(cls._health_monitor_config.get("ssh_timeout", 10))

        log.info("Restarting DBMS '%s' on %s via SSH: %s", name, ssh_host, cmd)

        # Build SSH command
        ssh_cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", f"ConnectTimeout={ssh_timeout}",
            "-o", "BatchMode=no",
            f"{ssh_user}@{ssh_host}",
            cmd,
        ]

        try:
            result = subprocess.run(
                ssh_cmd, capture_output=True, text=True,
                timeout=int(ssh_timeout) + 30,
            )
            success = result.returncode == 0
            output = (result.stdout + result.stderr).strip()[:500]
            log.info("Restart %s: exit=%d, output=%s", name, result.returncode, output[:200])

            # Wait a bit and verify
            _time.sleep(5)
            port_ok = False
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((cfg.host, cfg.port))
                s.close()
                port_ok = True
            except Exception:
                pass

            # Update health cache
            with cls._health_lock:
                cls._health_status[name] = {
                    "name": name, "host": cfg.host, "port": cfg.port,
                    "port_reachable": port_ok, "connected": port_ok,
                    "version": None, "latency_ms": 0,
                    "restart_available": True, "restart_in_progress": False,
                    "error": None if port_ok else "Port still unreachable after restart",
                }

            # Reset retry count on success
            if port_ok:
                cls._restart_retry_count[name] = 0

            self._respond_json({
                "ok": True, "success": port_ok,
                "name": name,
                "ssh_exit_code": result.returncode,
                "output": output,
                "port_reachable": port_ok,
                "message": f"Restart command sent (exit={result.returncode}), port {'reachable' if port_ok else 'still unreachable'}"
            })

        except subprocess.TimeoutExpired:
            log.error("Restart %s: SSH timeout", name)
            self._respond_json({
                "ok": False, "success": False,
                "error": f"SSH command timed out for {name}"
            }, 500)
        except FileNotFoundError:
            log.error("Restart %s: 'ssh' command not found", name)
            self._respond_json({
                "ok": False, "success": False,
                "error": "SSH command not available on this host"
            }, 500)
        except Exception as e:
            log.error("Restart %s: unexpected error: %s", name, e)
            self._respond_json({
                "ok": False, "success": False,
                "error": str(e)
            }, 500)
        finally:
            with cls._restart_lock:
                cls._restart_in_progress[name] = False

    # ── Health Monitor (class methods) ─────────────────────────────────

    @classmethod
    def _start_health_monitor(cls):
        """Start background health monitor thread if configured."""
        if cls._health_monitor_thread and cls._health_monitor_thread.is_alive():
            return

        hc = cls._health_monitor_config
        if not hc.get("enabled", False):
            log.info("Health monitor disabled by config")
            return

        interval = int(hc.get("interval_seconds", 30))
        auto_restart = hc.get("auto_restart", True)
        max_retries = int(hc.get("max_retries", 3))

        cls._health_monitor_stop.clear()

        def _monitor_loop():
            log.info("Health monitor started (interval=%ds, auto_restart=%s, max_retries=%d)",
                     interval, auto_restart, max_retries)
            while not cls._health_monitor_stop.is_set():
                cls._health_monitor_stop.wait(interval)
                if cls._health_monitor_stop.is_set():
                    break
                try:
                    cls._run_health_scan(auto_restart, max_retries)
                except Exception as e:
                    log.error("Health monitor scan error: %s", e)

        cls._health_monitor_thread = threading.Thread(
            target=_monitor_loop, daemon=True, name="health-monitor"
        )
        cls._health_monitor_thread.start()
        log.info("Health monitor thread started")

    @classmethod
    def _stop_health_monitor(cls):
        """Stop the background health monitor thread."""
        cls._health_monitor_stop.set()
        if cls._health_monitor_thread:
            cls._health_monitor_thread.join(timeout=5)
            cls._health_monitor_thread = None
            log.info("Health monitor stopped")

    @classmethod
    def _run_health_scan(cls, auto_restart: bool = True, max_retries: int = 3):
        """Run a single health scan of all enabled DBMS.

        If auto_restart is True, attempt to restart any unhealthy DBMS.
        """
        cls._reload_configs()
        active = [c for c in cls._all_configs if getattr(c, "enabled", True)]
        if not active:
            return

        import concurrent.futures

        def _check_one(cfg):
            import pymysql
            name = cfg.name
            connected = False
            version = None
            error = None
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((cfg.host, cfg.port))
                s.close()
                conn = pymysql.connect(
                    host=cfg.host, port=cfg.port, user=cfg.user,
                    password=cfg.password, charset="utf8mb4", connect_timeout=3,
                )
                cur = conn.cursor()
                cur.execute("SELECT VERSION()")
                row = cur.fetchone()
                version = row[0] if row else "unknown"
                cur.close()
                conn.close()
                connected = True
            except Exception as e:
                error = str(e)[:200]
            return name, connected, version, error

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(active)) as pool:
            futs = [pool.submit(_check_one, c) for c in active]
            for fut in concurrent.futures.as_completed(futs):
                try:
                    name, connected, version, error = fut.result()
                except Exception:
                    continue

                cfg = next((c for c in active if c.name == name), None)
                if cfg is None:
                    continue

                restart_cfg = getattr(cfg, "restart", None) or {}
                restart_avail = bool(restart_cfg.get("enabled") and restart_cfg.get("command"))

                with cls._health_lock:
                    cls._health_status[name] = {
                        "name": name, "host": cfg.host, "port": cfg.port,
                        "port_reachable": connected, "connected": connected,
                        "version": version, "latency_ms": 0,
                        "restart_available": restart_avail,
                        "restart_in_progress": cls._restart_in_progress.get(name, False),
                        "error": error,
                    }

                # Auto-restart if down
                if not connected and auto_restart and restart_avail:
                    with cls._restart_lock:
                        if cls._restart_in_progress.get(name):
                            continue
                        retries = cls._restart_retry_count.get(name, 0)
                        if retries >= max_retries:
                            log.warning("Health monitor: %s max retries (%d) reached, skipping auto-restart",
                                       name, max_retries)
                            continue
                        cls._restart_retry_count[name] = retries + 1
                        cls._restart_in_progress[name] = True

                    cmd = restart_cfg.get("command", "")
                    ssh_host = cls._health_monitor_config.get("ssh_host", "21.6.101.185")
                    ssh_user = cls._health_monitor_config.get("ssh_user", "root")
                    ssh_timeout = str(cls._health_monitor_config.get("ssh_timeout", 10))

                    log.warning("Health monitor: %s is DOWN, auto-restarting (attempt %d/%d)...",
                               name, retries + 1, max_retries)
                    try:
                        result = subprocess.run(
                            ["ssh", "-o", "StrictHostKeyChecking=no",
                             "-o", "UserKnownHostsFile=/dev/null",
                             "-o", f"ConnectTimeout={ssh_timeout}",
                             "-o", "BatchMode=no",
                             f"{ssh_user}@{ssh_host}", cmd],
                            capture_output=True, text=True,
                            timeout=int(ssh_timeout) + 30,
                        )
                        log.info("Auto-restart %s: exit=%d", name, result.returncode)
                    except Exception as e:
                        log.error("Auto-restart %s failed: %s", name, e)
                    finally:
                        with cls._restart_lock:
                            cls._restart_in_progress[name] = False

    @classmethod
    def _load_health_monitor_config(cls, config_path: str):
        """Load health_monitor settings from config JSON."""
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            cls._health_monitor_config = raw.get("health_monitor", {})
            log.info("Health monitor config loaded: %s", cls._health_monitor_config)
        except Exception as e:
            log.warning("Failed to load health_monitor config: %s", e)
            cls._health_monitor_config = {}

    # ── API: History ──────────────────────────────────────────────────

    def _handle_history_list(self):
        cls = type(self)
        cls._ensure_history_table()
        user = self._get_user()
        # Priority: query param > auth header > "anonymous"
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        query_user = qs.get("user_name", [None])[0]
        eng_name = query_user or user.get("eng_name", "") or "anonymous"
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
        cls = type(self)
        cls._ensure_history_table()
        try:
            body = self._read_json()
        except Exception:
            self._respond_json({"ok": False, "error": "invalid JSON"}, 400)
            return

        user = self._get_user()
        # Priority: With auth header > request body > "anonymous"
        eng_name = user.get("eng_name", "") or body.get("user_name", "") or "anonymous"
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
        eng_name = user.get("eng_name", "") or "anonymous"
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
        cls = type(self)
        cls._ensure_favorites_table()
        user = self._get_user()
        # Priority: query param > auth header > "anonymous"
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        query_user = qs.get("user_name", [None])[0]
        eng_name = query_user or user.get("eng_name", "") or "anonymous"
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
        eng_name = user.get("eng_name", "") or body.get("user_name", "") or "anonymous"
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
        eng_name = user.get("eng_name", "") or body.get("user_name", "") or "anonymous"
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
        eng_name = user.get("eng_name", "") or "anonymous"
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
            db = None
            try:
                db = DBConnection(config, target_db)
                with self._active_connections_lock:
                    self._active_connections.append(db)
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
        except Exception as e:
            log.error("Execute API internal error: %s", e, exc_info=True)
            try:
                self._respond_json({"ok": False, "error": f"Internal server error: {e}"}, 500)
            except Exception:
                pass

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
            db = None
            try:
                db = DBConnection(config, target_db)
                with self._active_connections_lock:
                    self._active_connections.append(db)
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
            # Embed DBMS config data so the page works even when API is unavailable
            try:
                with open(_dst_html, "r", encoding="utf-8") as f:
                    html = f.read()
                active_names = {c.name for c in self.configs}
                dbms_list = []
                for c in self.all_configs:
                    dbms_list.append({
                        "name": c.name, "host": c.host, "port": c.port,
                        "active": c.name in active_names, "enabled": c.enabled,
                        "type": getattr(c, "protocol", "mysql"),
                        "database": getattr(c, "service_name", ""),
                        "source": "builtin",
                    })
                dbms_json = json.dumps(dbms_list, ensure_ascii=False)
                from importlib.metadata import version as _get_version
                try:
                    _version = _get_version("rosetta-sql")
                except Exception:
                    _version = "1.5.1"
                embedded = (
                    "var EMBEDDED_DBMS=" + dbms_json + ";"
                    "var EMBEDDED_DATABASE=" + json.dumps(self.database or "", ensure_ascii=False) + ";"
                    "var EMBEDDED_BASELINE=" + json.dumps(self.baseline or "", ensure_ascii=False) + ";"
                    "var EMBEDDED_TRACELESS=" + json.dumps(self.traceless) + ";"
                    "var EMBEDDED_VERSION=" + json.dumps(_version, ensure_ascii=False) + ";\n"
                )
                html = html.replace("<script>\n", "<script>\n" + embedded, 1)
                with open(_dst_html, "w", encoding="utf-8") as f:
                    f.write(html)
                log.info("Embedded %d DBMS configs into playground page", len(dbms_list))
            except Exception as e:
                log.warning("Failed to embed DBMS data: %s", e)
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

        # Ensure database tables exist
        try:
            PlaygroundAPIHandler._ensure_all_tables()
        except Exception as e:
            log.warning("Failed to ensure DB tables: %s", e)

        handler = lambda *a, **kw: PlaygroundAPIHandler(*a, directory=self.directory, **kw)

        from socketserver import ThreadingMixIn

        class _ThreadingHTTPServer(ThreadingMixIn, http.server.HTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        self._server = _ThreadingHTTPServer(("0.0.0.0", self.port), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        log.info("Playground server started on port %d", self.port)

        # Start health monitor
        PlaygroundAPIHandler._load_health_monitor_config(
            PlaygroundAPIHandler._config_path or "with_config.json"
        )
        PlaygroundAPIHandler._start_health_monitor()

        return self.base_url

    def stop(self):
        if self._server:
            PlaygroundAPIHandler._stop_health_monitor()
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

    PlaygroundAPIHandler._config_path = os.path.abspath(args.config)

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
