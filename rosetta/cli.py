"""Command-line interface for Rosetta."""

import argparse
import concurrent.futures
import http.server
import logging
import os
import shutil
import socket
import subprocess
import sys
import threading
import time as _time
from pathlib import Path
from typing import Dict, List, Optional

from .comparator import compare_outputs
from .config import (DEFAULT_TEST_DB, filter_configs, generate_sample_config,
                     load_config)
from .executor import run_on_dbms
from .models import CompareResult, DBMSConfig, Statement, StmtType
from .parser import TestFileParser
from .reporter.html import write_html_report
from .reporter.history import generate_index_html
from .reporter.text import write_diff_file, write_text_report
from .ui import (ExecutionProgress, RichLogHandler, console, flush_all,
                 print_banner, print_error, print_info, print_phase,
                 print_report_file, print_server_info, print_success,
                 print_summary, print_warning)

log = logging.getLogger("rosetta")


class RosettaRunner:
    """Orchestrates parsing, execution, comparison, and reporting."""

    def __init__(self, test_file: str, configs: List[DBMSConfig],
                 output_dir: str, database: str = DEFAULT_TEST_DB,
                 baseline: Optional[str] = None,
                 skip_explain: bool = False,
                 skip_analyze: bool = False,
                 skip_show_create: bool = False,
                 output_format: str = "all",
                 whitelist=None,
                 buglist=None):
        self.test_file = test_file
        self.configs = configs
        self.output_dir = output_dir
        self.database = database
        self.baseline = baseline
        self.skip_explain_global = skip_explain
        self.skip_analyze_global = skip_analyze
        self.skip_show_create_global = skip_show_create
        self.output_format = output_format
        self.whitelist = whitelist
        self.buglist = buglist
        self.results: Dict[str, List[str]] = {}
        self.failed_connections: set = set()

    def _should_skip_stmt_global(self, stmt: Statement) -> bool:
        """Check if a statement should be skipped globally."""
        if stmt.stmt_type != StmtType.SQL:
            return False

        sql_upper = stmt.text.strip().upper()

        if self.skip_explain_global and sql_upper.startswith("EXPLAIN"):
            return True
        if self.skip_analyze_global and sql_upper.startswith("ANALYZE"):
            return True
        if (self.skip_show_create_global
                and sql_upper.startswith("SHOW CREATE")):
            return True

        for c in self.configs:
            if c.skip_explain and sql_upper.startswith("EXPLAIN"):
                return True
            if c.skip_analyze and sql_upper.startswith("ANALYZE"):
                return True
            if c.skip_show_create and sql_upper.startswith("SHOW CREATE"):
                return True

        return False

    def _order_configs(self) -> List[DBMSConfig]:
        """Order configs: baseline first, then 'mysql', then others."""
        baseline_cfg = []
        mysql_cfg = []
        others = []
        for c in self.configs:
            if self.baseline and c.name == self.baseline:
                baseline_cfg.append(c)
            elif c.name == "mysql":
                mysql_cfg.append(c)
            else:
                others.append(c)
        return baseline_cfg + mysql_cfg + others

    def _compare_all(self) -> Dict[str, CompareResult]:
        """Compare results across all DBMS pairs."""
        names = [n for n in self.results if n not in self.failed_connections]
        comparisons = {}

        if self.baseline and self.baseline in self.results:
            for name in names:
                if name == self.baseline:
                    continue
                key = f"{self.baseline}_vs_{name}"
                comparisons[key] = compare_outputs(
                    self.results[self.baseline],
                    self.results[name],
                    self.baseline, name,
                    baseline_name=self.baseline,
                    whitelist=self.whitelist,
                    buglist=self.buglist,
                )
        else:
            for i in range(len(names)):
                for j in range(i + 1, len(names)):
                    key = f"{names[i]}_vs_{names[j]}"
                    comparisons[key] = compare_outputs(
                        self.results[names[i]],
                        self.results[names[j]],
                        names[i], names[j],
                        whitelist=self.whitelist,
                        buglist=self.buglist,
                    )

        return comparisons

    def run(self) -> Dict[str, CompareResult]:
        """Execute the full pipeline: parse, execute, compare, report."""
        os.makedirs(self.output_dir, exist_ok=True)

        # Parse
        print_phase("Parse", self.test_file)
        parser = TestFileParser(self.test_file)
        statements = parser.parse()
        print_success(f"Parsed {len(statements)} statements")

        # Execute on each DBMS (in parallel)
        configs = self._order_configs()
        print_phase("Execute",
                    f"{len(configs)} DBMS targets (parallel)")

        def _run_single(config):
            """Execute on one DBMS; returns (config.name, output_lines | None)."""
            with ExecutionProgress(config.name,
                                   total=len(statements)) as prog:
                def _on_progress(error=False, _p=prog):
                    _p.advance(error=error)

                def _on_connect(name, ok, msg, _p=prog):
                    if ok:
                        _p.set_status(f"[green]{msg}[/green]")
                    else:
                        _p.set_status(f"[red]{msg}[/red]")

                def _on_done(name, executed, errors, _p=prog):
                    if errors:
                        _p.set_status(
                            f"[yellow]{executed} done, {errors} err[/yellow]")
                    else:
                        _p.set_status(f"[green]{executed} done[/green]")

                output = run_on_dbms(
                    config, statements, self.database,
                    should_skip_fn=self._should_skip_stmt_global,
                    on_connect=_on_connect,
                    on_progress=_on_progress,
                    on_done=_on_done,
                )

            # Progress bar is now gone (transient); write static line
            prog.write_summary_to_buffer()
            return config.name, output

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(configs)) as pool:
            futures = {pool.submit(_run_single, c): c for c in configs}
            for fut in concurrent.futures.as_completed(futures):
                name, output = fut.result()
                if output is None:
                    self.failed_connections.add(name)
                else:
                    self.results[name] = output

        # Write result files
        print_phase("Reports")
        test_name = Path(self.test_file).stem
        for name, lines in self.results.items():
            result_path = os.path.join(
                self.output_dir, f"{test_name}.{name}.result"
            )
            with open(result_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            print_report_file(result_path, label="result")

        # Compare
        comparisons = self._compare_all()

        # Generate reports
        self._generate_reports(test_name, comparisons)

        return comparisons

    def run_diff_only(self) -> Dict[str, CompareResult]:
        """Re-generate reports from existing .result files (no execution)."""
        os.makedirs(self.output_dir, exist_ok=True)
        test_name = Path(self.test_file).stem

        print_phase("Load Results", "(diff-only mode)")

        # Load existing .result files
        for config in self.configs:
            result_path = os.path.join(
                self.output_dir, f"{test_name}.{config.name}.result"
            )
            if os.path.isfile(result_path):
                with open(result_path, "r", encoding="utf-8") as f:
                    self.results[config.name] = [
                        line.rstrip("\n") for line in f
                    ]
                print_success(f"Loaded: {result_path}")
            else:
                print_warning(f"Not found: {result_path}")

        if len(self.results) < 2:
            print_error("Need at least 2 result files for comparison")
            return {}

        comparisons = self._compare_all()

        print_phase("Reports")
        self._generate_reports(test_name, comparisons)
        return comparisons

    def _generate_reports(self, test_name: str,
                          comparisons: Dict[str, CompareResult]):
        """Generate output reports based on format setting."""
        fmt = self.output_format

        if fmt in ("text", "all"):
            report_path = os.path.join(
                self.output_dir, f"{test_name}.report.txt"
            )
            write_text_report(report_path, self.test_file, comparisons)
            print_report_file(report_path, label="text")

            diff_path = os.path.join(
                self.output_dir, f"{test_name}.diff"
            )
            write_diff_file(diff_path, comparisons)
            print_report_file(diff_path, label="diff")

        if fmt in ("html", "all"):
            html_path = os.path.join(
                self.output_dir, f"{test_name}.html"
            )
            write_html_report(
                html_path, self.test_file, comparisons,
                baseline=self.baseline or "",
            )
            print_report_file(html_path, label="html")


def parse_args(argv=None):
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(
        prog="rosetta",
        description="Rosetta — Cross-DBMS SQL behavioral consistency "
                    "verification tool. Run MTR-style .test files "
                    "against multiple databases and compare results.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate sample config
  rosetta --gen-config dbms_config.json

  # Run test against specific DBMS
  rosetta --test path/to/test.test --dbms tdsql,mysql

  # Run with baseline comparison
  rosetta --test path/to/test.test --dbms tdsql,mysql,tidb --baseline tdsql

  # Re-generate reports from existing results (no execution)
  rosetta --test path/to/test.test --diff-only

  # Interactive mode: set params once, run tests repeatedly
  rosetta --interactive --dbms tdsql,mysql --serve

  # Parse only (debug)
  rosetta --test path/to/test.test --parse-only
        """,
    )

    p.add_argument("--test", "-t",
                   help="Path to .test file")
    p.add_argument("--config", "-c", default="dbms_config.json",
                   help="Path to DBMS config JSON file "
                        "(default: dbms_config.json)")
    p.add_argument("--dbms",
                   help="DBMS to compare, comma-separated "
                        "(e.g. tdsql,mysql,tidb). "
                        "If not set, uses enabled flag in config.")
    p.add_argument("--baseline", "-b", default="tdsql",
                   help="Baseline DBMS name (default: tdsql)")
    p.add_argument("--output-dir", "-o", default="results",
                   help="Output directory (default: results)")
    p.add_argument("--format", "-f", default="all",
                   choices=["text", "html", "all"],
                   help="Output format (default: all)")
    p.add_argument("--database", "-d", default=DEFAULT_TEST_DB,
                   help=f"Test database name (default: {DEFAULT_TEST_DB})")
    p.add_argument("--skip-explain", action="store_true", default=True,
                   help="Skip EXPLAIN statements (default: True)")
    p.add_argument("--skip-analyze", action="store_true",
                   help="Skip ANALYZE TABLE statements")
    p.add_argument("--skip-show-create", action="store_true",
                   help="Skip SHOW CREATE TABLE statements")
    p.add_argument("--parse-only", action="store_true",
                   help="Only parse .test file and print statements")
    p.add_argument("--diff-only", action="store_true",
                   help="Only re-generate reports from existing .result "
                        "files (no DB execution)")
    p.add_argument("--gen-config",
                   help="Generate sample config at given path and exit")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Enable verbose logging")
    p.add_argument("--serve", "-s", action="store_true",
                   help="Start a local HTTP server to view HTML report "
                        "after execution")
    p.add_argument("--port", "-p", type=int, default=19527,
                   help="Port for the HTTP server (default: 19527)")
    p.add_argument("--interactive", "-i", action="store_true",
                   help="Enter interactive mode: set base parameters "
                        "once, then submit test paths repeatedly")

    return p.parse_args(argv)


def main(argv=None):
    """Main entry point for the rosetta command."""
    # Configure logging to use rich handler
    rich_handler = RichLogHandler()
    rich_handler.setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.WARNING,
        handlers=[rich_handler],
    )

    args = parse_args(argv)

    if args.verbose:
        # In verbose mode, use standard logging for everything
        logging.root.handlers.clear()
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    print_banner()

    # Generate sample config
    if args.gen_config:
        generate_sample_config(args.gen_config)
        print_success(f"Config written: {args.gen_config}")
        flush_all()
        return 0

    # Interactive mode — does not require --test
    if args.interactive:
        return _enter_interactive(args)

    if not args.test:
        print_error("--test is required. Use --help for usage.")
        flush_all()
        return 1

    if not os.path.isfile(args.test):
        print_error(f"Test file not found: {args.test}")
        flush_all()
        return 1

    # Parse-only mode
    if args.parse_only:
        flush_all()
        parser = TestFileParser(args.test)
        stmts = parser.parse()
        for s in stmts:
            tag = s.stmt_type.name
            err = (f" [expect error: {s.expected_error}]"
                   if s.expected_error else "")
            sort = " [sorted]" if s.sort_result else ""
            print(f"L{s.line_no:4d} [{tag:5s}]{err}{sort}: "
                  f"{s.text[:100]}")
        print(f"\nTotal: {len(stmts)} statements")
        return 0

    if not os.path.isfile(args.config):
        print_error(f"Config file not found: {args.config}")
        flush_all()
        return 1

    # Load and filter configs
    all_configs = load_config(args.config)
    if not all_configs:
        print_error(f"No databases configured in {args.config}")
        flush_all()
        return 1

    try:
        configs = filter_configs(all_configs, args.dbms)
    except ValueError as e:
        print_error(str(e))
        flush_all()
        return 1

    if not configs:
        print_error("No databases selected for testing")
        flush_all()
        return 1

    # Resolve output_dir to absolute path early so it does not depend on cwd
    output_dir = os.path.abspath(args.output_dir)

    # Create a timestamped sub-directory for this run
    run_stamp = _time.strftime("%Y%m%d_%H%M%S")
    test_name = Path(args.test).stem
    run_dir = os.path.join(output_dir, f"{test_name}_{run_stamp}")

    print_info("DBMS targets:",
               ", ".join(c.name for c in configs))

    # Load whitelist from output directory
    from .whitelist import Whitelist
    whitelist = Whitelist(output_dir)

    # Load buglist from output directory
    from .buglist import Buglist
    buglist = Buglist(output_dir)

    # Run
    runner = RosettaRunner(
        test_file=args.test,
        configs=configs,
        output_dir=run_dir,
        database=args.database,
        baseline=args.baseline,
        skip_explain=args.skip_explain,
        skip_analyze=args.skip_analyze,
        skip_show_create=args.skip_show_create,
        output_format=args.format,
        whitelist=whitelist,
        buglist=buglist,
    )

    if args.diff_only:
        # Copy .result files from latest run into the new run_dir
        latest_link = os.path.join(output_dir, "latest")
        source_dir = (os.path.realpath(latest_link)
                      if os.path.islink(latest_link) else None)
        if source_dir and os.path.isdir(source_dir):
            os.makedirs(run_dir, exist_ok=True)
            for f in os.listdir(source_dir):
                if f.endswith(".result"):
                    shutil.copy2(
                        os.path.join(source_dir, f),
                        os.path.join(run_dir, f))
        comparisons = runner.run_diff_only()
    else:
        comparisons = runner.run()

    if not comparisons:
        flush_all()
        return 1

    # Update 'latest' symlink
    latest_link = os.path.join(output_dir, "latest")
    try:
        if os.path.islink(latest_link):
            os.remove(latest_link)
        os.symlink(os.path.basename(run_dir), latest_link)
    except OSError:
        pass

    # Generate history index page and whitelist/buglist pages
    generate_index_html(output_dir)
    from .reporter.history import generate_buglist_html, generate_whitelist_html
    generate_whitelist_html(output_dir)
    generate_buglist_html(output_dir)

    # Print rich summary table
    all_pass = print_summary(comparisons, runner.failed_connections)

    # Flush everything as one big panel
    flush_all()

    # Serve HTML report if requested
    if args.serve and args.format in ("html", "all"):
        html_file = f"{test_name}.html"
        html_path = os.path.join(run_dir, html_file)

        if os.path.isfile(html_path):
            # Serve from output_dir root so history is accessible
            relative_html = os.path.join(
                os.path.basename(run_dir), html_file)
            _serve_report(output_dir, relative_html, args.port,
                          whitelist=whitelist, buglist=buglist,
                          configs=configs, database=args.database)
        else:
            console.print(f"[yellow]HTML report not found: {html_path}[/yellow]")

    return 0 if (all_pass and not runner.failed_connections) else 1


def _enter_interactive(args) -> int:
    """Load config and launch the interactive session."""
    from .interactive import InteractiveSession

    if not os.path.isfile(args.config):
        print_error(f"Config file not found: {args.config}")
        flush_all()
        return 1

    all_configs = load_config(args.config)
    if not all_configs:
        print_error(f"No databases configured in {args.config}")
        flush_all()
        return 1

    try:
        configs = filter_configs(all_configs, args.dbms)
    except ValueError as e:
        print_error(str(e))
        flush_all()
        return 1

    if not configs:
        print_error("No databases selected for testing")
        flush_all()
        return 1

    output_dir = os.path.abspath(args.output_dir)

    session = InteractiveSession(
        configs=configs,
        output_dir=output_dir,
        database=args.database,
        baseline=args.baseline,
        skip_explain=args.skip_explain,
        skip_analyze=args.skip_analyze,
        skip_show_create=args.skip_show_create,
        output_format=args.format,
        serve=args.serve,
        port=args.port,
        all_configs=all_configs,
    )
    session.run()
    return 0


def _find_free_port() -> int:
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _serve_report(directory: str, html_file: str, port: int = 0,
                  whitelist=None, buglist=None, configs=None,
                  database: str = ""):
    """Start a local HTTP server and print the URL for the HTML report."""
    if port == 0:
        port = _find_free_port()

    abs_dir = os.path.abspath(directory)

    # Pre-generate playground page
    from .reporter.history import generate_playground_html
    generate_playground_html(abs_dir)

    # Use the API-capable handler from interactive module if whitelist given
    if whitelist is not None:
        from .interactive import _APIHandler
        _APIHandler._whitelist = whitelist
        _APIHandler._buglist = buglist
        _APIHandler._configs = configs or []
        _APIHandler._database = database
        handler = lambda *a, **kw: _APIHandler(
            *a, directory=abs_dir, **kw)
    else:
        handler = lambda *a, **kw: http.server.SimpleHTTPRequestHandler(
            *a, directory=abs_dir, **kw)

    try:
        server = http.server.HTTPServer(("0.0.0.0", port), handler)
    except OSError as e:
        print_error(f"Failed to start HTTP server on port {port}: {e}")
        return

    url = f"http://localhost:{port}/{html_file}"
    index_url = f"http://localhost:{port}/index.html"
    print_server_info(url, abs_dir, history_url=index_url)

    # Run server in a background thread so KeyboardInterrupt
    # can be caught without deadlocking serve_forever().
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    # Try to open the URL in the IDE's built-in Simple Browser.
    # Works in VS Code / CloudStudio / CodeBuddy environments.
    try:
        subprocess.Popen(
            ["code", "--open-url", url],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        pass  # 'code' CLI not available, skip

    try:
        # Block main thread until interrupted
        server_thread.join()
    except KeyboardInterrupt:
        pass
    finally:
        console.print("\n[dim]Shutting down server...[/dim]")
        # Run shutdown in a separate thread to avoid blocking forever.
        t = threading.Thread(target=server.shutdown, daemon=True)
        t.start()
        t.join(timeout=3)
        # server_thread is daemon=True, so it will be cleaned up on exit.
