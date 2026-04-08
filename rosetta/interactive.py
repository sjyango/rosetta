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
# HTTP server management
# ---------------------------------------------------------------------------

class _SilentHTTPServer(http.server.HTTPServer):
    """HTTPServer that silently handles connection errors."""

    def handle_error(self, request, client_address):
        """Silently ignore connection reset/broken pipe errors."""
        # These are normal when clients disconnect abruptly
        pass


class _APIHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler with whitelist/buglist API endpoints and suppressed logging."""

    # Class-level reference set by ReportServer before creating instances.
    _whitelist = None  # type: ignore
    _buglist = None    # type: ignore
    _configs: List[DBMSConfig] = []
    _all_configs: List[DBMSConfig] = []
    _database: str = ""

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
        if self.path.startswith("/api/whitelist/"):
            self._handle_whitelist_api()
        elif self.path.startswith("/api/buglist/"):
            self._handle_buglist_api()
        elif self.path == "/api/execute":
            self._handle_execute_api()
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

    def _handle_whitelist_api(self):
        action = self.path.split("/api/whitelist/", 1)[-1].strip("/")
        wl = self._whitelist
        if wl is None:
            self._respond_json({"ok": False, "error": "whitelist not loaded"},
                               500)
            return
        try:
            body = self._read_json()
        except Exception:
            body = {}

        if action == "add":
            fp = body.get("fingerprint", "")
            if not fp:
                self._respond_json({"ok": False,
                                    "error": "fingerprint required"}, 400)
                return
            entry = wl.add(
                fingerprint=fp,
                stmt=body.get("stmt", ""),
                dbms_a=body.get("dbms_a", ""),
                dbms_b=body.get("dbms_b", ""),
                block=body.get("block", 0),
                reason=body.get("reason", ""),
            )
            self._respond_json({"ok": True, "entry": entry})

        elif action == "remove":
            fp = body.get("fingerprint", "")
            removed = wl.remove(fp) if fp else False
            self._respond_json({"ok": removed})

        elif action == "clear":
            wl.clear()
            self._respond_json({"ok": True})

        elif action == "list":
            self._respond_json({"ok": True, "entries": wl.entries})

        else:
            self._respond_json({"ok": False, "error": "unknown action"}, 404)

    def _handle_buglist_api(self):
        action = self.path.split("/api/buglist/", 1)[-1].strip("/")
        bl = self._buglist
        if bl is None:
            self._respond_json({"ok": False, "error": "buglist not loaded"},
                               500)
            return
        try:
            body = self._read_json()
        except Exception:
            body = {}

        if action == "add":
            fp = body.get("fingerprint", "")
            if not fp:
                self._respond_json({"ok": False,
                                    "error": "fingerprint required"}, 400)
                return
            entry = bl.add(
                fingerprint=fp,
                stmt=body.get("stmt", ""),
                dbms_a=body.get("dbms_a", ""),
                dbms_b=body.get("dbms_b", ""),
                block=body.get("block", 0),
                reason=body.get("reason", ""),
            )
            self._respond_json({"ok": True, "entry": entry})

        elif action == "remove":
            fp = body.get("fingerprint", "")
            removed = bl.remove(fp) if fp else False
            self._respond_json({"ok": removed})

        elif action == "clear":
            bl.clear()
            self._respond_json({"ok": True})

        elif action == "list":
            self._respond_json({"ok": True, "entries": bl.entries})

        else:
            self._respond_json({"ok": False, "error": "unknown action"}, 404)

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
            "dbms": dbms_list,
        })

    def _handle_execute_api(self):
        """POST /api/execute — execute SQL on selected DBMS targets.

        Request body: {"sql": "...", "dbms": ["tdsql", "mysql"]}
        Response: {"ok": true, "results": {"tdsql": {...}, "mysql": {...}}}
        """
        import concurrent.futures

        from .executor import DBConnection, check_port

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

        # Reuse the full MTR parser to extract SQL statements,
        # filtering out all MTR directives (--echo, --error, etc.)
        from .parser import TestFileParser
        parsed = TestFileParser.parse_text(sql_text)
        stmts = [s.text for s in parsed]

        def _exec_on_dbms(config):
            """Execute all statements on one DBMS, return result dict."""
            result = {
                "name": config.name,
                "statements": [],
                "error": None,
            }

            if not check_port(config.host, config.port):
                result["error"] = (f"Cannot reach {config.host}:"
                                   f"{config.port}")
                return result

            db = DBConnection(config, database)
            try:
                db.connect()
            except Exception as e:
                result["error"] = f"Connection failed: {e}"
                return result

            try:
                for sql in stmts:
                    stmt_result = {"sql": sql, "columns": None,
                                   "rows": None, "error": None,
                                   "affected_rows": 0,
                                   "elapsed_ms": 0}
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
                        stmt_result["error"] = str(e)
                        stmt_result["elapsed_ms"] = round(
                            (t1 - t0) * 1000, 3)

                    result["statements"].append(stmt_result)
            finally:
                db.close()

            return result

        # Execute in parallel across all DBMS targets
        results = {}
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(targets)) as pool:
            futures = {pool.submit(_exec_on_dbms, c): c for c in targets}
            for fut in concurrent.futures.as_completed(futures):
                r = fut.result()
                results[r["name"]] = r

        self._respond_json({"ok": True, "results": results})


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

    def __init__(self, directory: str, port: int = 0, whitelist=None,
                 buglist=None, configs: Optional[List[DBMSConfig]] = None,
                 all_configs: Optional[List[DBMSConfig]] = None,
                 database: str = ""):
        self.directory = os.path.abspath(directory)
        self.port = port
        self.whitelist = whitelist
        self.buglist = buglist
        self.configs = configs or []
        self.all_configs = all_configs or self.configs
        self.database = database
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
        # Pre-generate index/whitelist/buglist pages so / redirects work
        from .reporter.history import (generate_buglist_html,
                                       generate_index_html,
                                       generate_playground_html,
                                       generate_whitelist_html)
        generate_index_html(self.directory)
        generate_whitelist_html(self.directory)
        generate_buglist_html(self.directory)
        generate_playground_html(self.directory)
        directory = self.directory
        wl = self.whitelist
        bl = self.buglist
        # Inject references into handler class
        _APIHandler._whitelist = wl
        _APIHandler._buglist = bl
        _APIHandler._configs = self.configs
        _APIHandler._all_configs = self.all_configs
        _APIHandler._database = self.database
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
                 skip_explain: bool = False,
                 skip_analyze: bool = False,
                 skip_show_create: bool = False,
                 output_format: str = "all",
                 serve: bool = False, port: int = 19527,
                 all_configs: Optional[List[DBMSConfig]] = None):
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
        self._report_server: Optional[ReportServer] = None
        # Whitelist — shared across all runs in this session
        from .whitelist import Whitelist
        self._whitelist = Whitelist(self.output_dir)
        # Buglist — shared across all runs in this session
        from .buglist import Buglist
        self._buglist = Buglist(self.output_dir)

    # -- server helpers -----------------------------------------------------

    def _ensure_server(self) -> Optional[ReportServer]:
        if not self.serve:
            return None
        if self._report_server and self._report_server.running:
            return self._report_server
        # Stop previous server if it exists but is no longer running
        if self._report_server:
            self._report_server.stop()
        self._report_server = ReportServer(self.output_dir, self.port,
                                           whitelist=self._whitelist,
                                           buglist=self._buglist,
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
        from .reporter.history import generate_buglist_html, generate_whitelist_html

        if not os.path.isfile(test_file):
            print_error(f"Test file not found: {test_file}")
            flush_all()
            return False

        run_stamp = _time.strftime("%Y%m%d_%H%M%S")
        test_name = Path(test_file).stem
        run_dir = os.path.join(self.output_dir, f"{test_name}_{run_stamp}")

        # Reload whitelist and buglist to pick up any changes from the web UI
        self._whitelist.load()
        self._buglist.load()

        print_info("DBMS targets:",
                   ", ".join(c.name for c in self.configs))

        runner = RosettaRunner(
            test_file=test_file, configs=self.configs,
            output_dir=run_dir, database=self.database,
            baseline=self.baseline, skip_explain=self.skip_explain,
            skip_analyze=self.skip_analyze,
            skip_show_create=self.skip_show_create,
            output_format=self.output_format,
            whitelist=self._whitelist,
            buglist=self._buglist)

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
        generate_whitelist_html(self.output_dir)
        generate_buglist_html(self.output_dir)

        # Print whitelist summary
        wl_count = sum(cmp.whitelisted for cmp in comparisons.values())
        if wl_count:
            console.print(
                f"  [yellow]⚡ {wl_count} diff(s) matched whitelist"
                f"[/yellow]")

        # Print bug summary
        bug_count = sum(cmp.bug_marked for cmp in comparisons.values())
        if bug_count:
            console.print(
                f"  [red]🐛 {bug_count} diff(s) marked as bug"
                f"[/red]")

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
            history=FileHistory(os.path.join(self.output_dir, ".rosetta_history")),
            completer=TestFileCompleter(),
            style=_PROMPT_STYLE,
            complete_while_typing=True,
            multiline=False,
        )

        _placeholder = HTML('<placeholder>Type a path, \'help\', \'back\', or \'quit\'</placeholder>')
        #   ║ + 55 chars content + ║
        #   ╚ + 55×═ + ╝
        border = "═" * 55
        title = "Rosetta Interactive Mode"
        hint = "Enter .test file paths to execute, or 'help'"
        # Center-pad content to 55 visible characters
        title_line = f"  {title}  ".center(55)
        hint_line = f"  {hint}  ".center(55)
        console.print(f"  [bold cyan]╔{border}╗[/bold cyan]")
        console.print(f"  [bold cyan]║[/bold cyan]"
                       f"[bold white]{title_line}[/bold white]"
                       f"[bold cyan]║[/bold cyan]")
        console.print(f"  [bold cyan]║[/bold cyan]"
                       f"[dim]{hint_line}[/dim]"
                       f"[bold cyan]║[/bold cyan]")
        console.print(f"  [bold cyan]╚{border}╝[/bold cyan]")

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
                f"[bold green]{srv.base_url}[/bold green]")
        console.print()

        run_count = 0
        exit_reason = "quit"

        while True:
            try:
                prompt_msg = HTML(
                    '<prompt>rosetta</prompt> <path>▶</path> ')
                user_input = session.prompt(
                    prompt_msg, placeholder=_placeholder).strip()
            except (EOFError, KeyboardInterrupt):
                break

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
                 bench_mode: str = "serial"):
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
        self._report_server: Optional[ReportServer] = None

    # -- server helpers -----------------------------------------------------

    def _ensure_server(self) -> Optional[ReportServer]:
        if not self.serve:
            return None
        if self._report_server and self._report_server.running:
            return self._report_server
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
            history=FileHistory(os.path.join(self.output_dir, ".rosetta_bench_history")),
            completer=BenchFileCompleter(),
            style=_PROMPT_STYLE,
            complete_while_typing=True,
            multiline=False,
        )

        _placeholder = HTML('<placeholder>Type a path, \'help\', \'back\', or \'quit\'</placeholder>')

        # Welcome banner
        border = "═" * 55
        title = "Rosetta Benchmark Interactive Mode"
        hint = "Enter bench file (.json/.sql) to execute, or 'help'"
        title_line = f"  {title}  ".center(55)
        hint_line = f"  {hint}  ".center(55)
        console.print(f"  [bold cyan]╔{border}╗[/bold cyan]")
        console.print(f"  [bold cyan]║[/bold cyan]"
                       f"[bold white]{title_line}[/bold white]"
                       f"[bold cyan]║[/bold cyan]")
        console.print(f"  [bold cyan]║[/bold cyan]"
                       f"[dim]{hint_line}[/dim]"
                       f"[bold cyan]║[/bold cyan]")
        console.print(f"  [bold cyan]╚{border}╝[/bold cyan]")

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
                    prompt_msg, placeholder=_placeholder).strip()
            except (EOFError, KeyboardInterrupt):
                break

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
