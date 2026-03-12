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
from prompt_toolkit.history import InMemoryHistory
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
})


# ---------------------------------------------------------------------------
# HTTP server management
# ---------------------------------------------------------------------------

class _APIHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler with whitelist/buglist API endpoints and suppressed logging."""

    # Class-level reference set by ReportServer before creating instances.
    _whitelist = None  # type: ignore
    _buglist = None    # type: ignore
    _configs: List[DBMSConfig] = []
    _database: str = ""

    def log_message(self, format, *args):  # noqa: A002
        pass  # Suppress all request logs

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

    # -- Playground API -----------------------------------------------------

    def _handle_dbms_list(self):
        """GET /api/dbms — return configured DBMS list and database name."""
        dbms_list = [{"name": c.name, "host": c.host, "port": c.port}
                     for c in self._configs]
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
        configs_map = {c.name: c for c in self._configs}

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

        # Split SQL into individual statements (by semicolons)
        stmts = [s.strip() for s in sql_text.split(";") if s.strip()]

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
                                   "affected_rows": 0}
                    try:
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
                    except Exception as e:
                        stmt_result["error"] = str(e)

                    result["statements"].append(stmt_result)
            finally:
                db.cleanup_database()
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
                 database: str = ""):
        self.directory = os.path.abspath(directory)
        self.port = port
        self.whitelist = whitelist
        self.buglist = buglist
        self.configs = configs or []
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
        _APIHandler._database = self.database
        handler = lambda *a, **kw: _APIHandler(
            *a, directory=directory, **kw)
        self._server = http.server.HTTPServer(("0.0.0.0", self.port), handler)
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        daemon=True)
        self._thread.start()
        return self.base_url

    def stop(self):
        if self._server:
            t = threading.Thread(target=self._server.shutdown, daemon=True)
            t.start()
            t.join(timeout=3)
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
        "quit":    "Exit (also: exit, q)",
    }

    def __init__(self, configs: List[DBMSConfig], output_dir: str,
                 database: str = DEFAULT_TEST_DB,
                 baseline: Optional[str] = None,
                 skip_explain: bool = False,
                 skip_analyze: bool = False,
                 skip_show_create: bool = False,
                 output_format: str = "all",
                 serve: bool = False, port: int = 19527):
        self.configs = configs
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
        self._report_server = ReportServer(self.output_dir, self.port,
                                           whitelist=self._whitelist,
                                           buglist=self._buglist,
                                           configs=self.configs,
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
        from .cli import RosettaRunner
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
        """Start the interactive REPL."""
        session: PromptSession = PromptSession(
            history=InMemoryHistory(),
            completer=TestFileCompleter(),
            style=_PROMPT_STYLE,
            complete_while_typing=True,
        )

        # Print welcome
        #   ╔ + 55×═ + ╗
        #   ║ + 55 chars content + ║
        #   ╚ + 55×═ + ╝
        border = "═" * 55
        title = "Rosetta Interactive Mode"
        hint = "Enter .test file paths to execute, or type 'help'"
        # Center-pad content to 55 visible characters
        title_line = f"  {title}  ".center(55)
        hint_line = f"  {hint}  ".center(55)
        console.print()
        console.print(f"  [bold cyan]╔{border}╗[/bold cyan]")
        console.print(f"  [bold cyan]║[/bold cyan]"
                       f"[bold white]{title_line}[/bold white]"
                       f"[bold cyan]║[/bold cyan]")
        console.print(f"  [bold cyan]║[/bold cyan]"
                       f"[dim]{hint_line}[/dim]"
                       f"[bold cyan]║[/bold cyan]")
        console.print(f"  [bold cyan]╚{border}╝[/bold cyan]")
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
                f"[bold green]{srv.base_url}[/bold green]")
        console.print()

        run_count = 0

        while True:
            try:
                prompt_msg = HTML(
                    '<prompt>rosetta</prompt> <path>▶</path> ')
                user_input = session.prompt(prompt_msg).strip()
            except (EOFError, KeyboardInterrupt):
                break

            if not user_input:
                continue

            cmd = user_input.lower()

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
                "Type a path or 'help' for commands.[/dim]\n")

        # Cleanup
        console.print()
        if self._run_history:
            console.print(
                f"  [dim]Session complete: "
                f"{len(self._run_history)} test(s) executed.[/dim]")
        if self._report_server:
            self._report_server.stop()
            console.print("  [dim]Report server stopped.[/dim]")
        console.print("  [bold cyan]Goodbye! 👋[/bold cyan]\n")
