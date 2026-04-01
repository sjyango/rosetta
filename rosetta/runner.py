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
from typing import Dict, List, Optional, Tuple

from .comparator import compare_outputs
from .config import (DEFAULT_TEST_DB, filter_configs, generate_sample_config,
                     load_config)
from .executor import run_on_dbms
from .models import CompareResult, DBMSConfig, Statement, StmtType, WorkloadMode
from .parser import TestFileParser
from .reporter.html import write_html_report
from .reporter.history import generate_index_html
from .reporter.text import write_diff_file, write_text_report
from .ui import (ExecutionProgress, RichLogHandler, console, flush_all,
                 print_banner, print_error, print_info, print_phase,
                 print_report_file, print_server_info, print_success,
                 print_summary, print_warning)

log = logging.getLogger("rosetta")


def _tty_write(data: str):
    """Write escape codes directly to /dev/tty.

    In environments where sys.stdout is a pipe (e.g. IDE terminals),
    prompt_toolkit writes to /dev/tty but sys.stdout does not reach the
    terminal.  This helper ensures escape sequences actually reach the
    terminal device.
    """
    try:
        fd = os.open("/dev/tty", os.O_WRONLY)
        try:
            os.write(fd, data.encode())
        finally:
            os.close(fd)
    except OSError:
        sys.stdout.write(data)
        sys.stdout.flush()


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
        description=(
            "Rosetta — Cross-DBMS SQL testing & benchmarking toolkit.\n"
            "\n"
            "Three operating modes:\n"
            "  MTR         Run .test files against multiple databases "
            "and diff results\n"
            "  Benchmark   Compare query performance across databases "
            "with latency/QPS reports\n"
            "  Playground  Launch an interactive SQL playground "
            "in the browser\n"
            "\n"
            "Use --interactive (-i) to enter a REPL that lets you "
            "switch between modes.\n"
            "Without -i, run a single MTR test (--test) or benchmark "
            "(--benchmark)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # ── Setup ──────────────────────────────────────────────────
  rosetta --gen-config dbms_config.json      Generate sample config

  # ── MTR (consistency test) ─────────────────────────────────
  rosetta -t path/to/test.test --dbms tdsql,mysql
  rosetta -t path/to/test.test --dbms tdsql,mysql,tidb -b tdsql
  rosetta -t path/to/test.test --diff-only   Re-diff without execution
  rosetta -t path/to/test.test --parse-only  Debug: show parsed stmts

  # ── Benchmark ──────────────────────────────────────────────
  rosetta --benchmark --bench-file bench.json --dbms tdsql,mysql
  rosetta --benchmark --template oltp_read_write --iterations 200
  rosetta --benchmark --bench-file bench.json --repeat 5
  rosetta --benchmark --bench-file bench.json --concurrency 16 --duration 60
  rosetta --benchmark --list-templates        Show built-in templates

  # ── Interactive / Playground ───────────────────────────────
  rosetta -i --dbms tdsql,mysql -s           Choose mode at startup
  rosetta -i --benchmark --dbms tdsql,mysql   Go straight to Benchmark
  rosetta -i --dbms tdsql,mysql --port 8080   Custom server port

  # ── Profiling ──────────────────────────────────────────────
  rosetta --benchmark --bench-file b.json --profile --perf-freq 199
  rosetta --benchmark --bench-file b.json --no-profile
""",
    )

    # -- Global options -------------------------------------------------------
    general = p.add_argument_group(
        "General", "Options shared across all modes")
    general.add_argument(
        "--config", "-c", default="dbms_config.json",
        help="Path to DBMS config JSON (default: dbms_config.json)")
    general.add_argument(
        "--dbms",
        help="DBMS targets, comma-separated (e.g. tdsql,mysql,tidb). "
             "Omit to use 'enabled' flag in config")
    general.add_argument(
        "--database", "-d", default=DEFAULT_TEST_DB,
        help=f"Test database name (default: {DEFAULT_TEST_DB})")
    general.add_argument(
        "--output-dir", "-o", default="results",
        help="Output directory for reports (default: results)")
    general.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose / debug logging")
    general.add_argument(
        "--gen-config",
        help="Generate sample config at the given path and exit")

    # -- Interactive / server -------------------------------------------------
    ui = p.add_argument_group(
        "Interactive & Server",
        "Enter a REPL or serve HTML reports in the browser")
    ui.add_argument(
        "--interactive", "-i", action="store_true",
        help="Enter interactive mode — choose MTR / Benchmark / "
             "Playground, then run tasks in a loop")
    ui.add_argument(
        "--serve", "-s", action="store_true",
        help="Start a local HTTP server to view HTML reports "
             "after execution")
    ui.add_argument(
        "--port", "-p", type=int, default=19527,
        help="HTTP server port (default: 19527)")

    # -- MTR options ----------------------------------------------------------
    mtr = p.add_argument_group(
        "MTR (Consistency Test)",
        "Run .test files and compare results across databases")
    mtr.add_argument(
        "--test", "-t",
        help="Path to .test file")
    mtr.add_argument(
        "--baseline", "-b", default="tdsql",
        help="Baseline DBMS name for diff (default: tdsql)")
    mtr.add_argument(
        "--format", "-f", default="all",
        choices=["text", "html", "all"],
        help="Output format (default: all)")
    mtr.add_argument(
        "--skip-explain", action="store_true", default=True,
        help="Skip EXPLAIN statements (default: on)")
    mtr.add_argument(
        "--skip-analyze", action="store_true",
        help="Skip ANALYZE TABLE statements")
    mtr.add_argument(
        "--skip-show-create", action="store_true",
        help="Skip SHOW CREATE TABLE statements")
    mtr.add_argument(
        "--parse-only", action="store_true",
        help="Only parse .test file and print statements (no execution)")
    mtr.add_argument(
        "--diff-only", action="store_true",
        help="Re-generate reports from existing .result files "
             "(no DB execution)")

    # -- Benchmark options ----------------------------------------------------
    bench = p.add_argument_group(
        "Benchmark",
        "Compare query performance across databases with "
        "latency / QPS reports")
    bench.add_argument(
        "--benchmark", action="store_true",
        help="Enable benchmark mode")
    bench.add_argument(
        "--bench-file",
        help="Benchmark definition file (.json or .sql)")
    bench.add_argument(
        "--template",
        help="Use a built-in template "
             "(e.g. oltp_read_write, oltp_read_only)")
    bench.add_argument(
        "--list-templates", action="store_true",
        help="List built-in benchmark templates and exit")
    bench.add_argument(
        "--iterations", type=int, default=100,
        help="Iterations per query — serial mode (default: 100)")
    bench.add_argument(
        "--warmup", type=int, default=5,
        help="Warmup iterations per query (default: 5)")
    bench.add_argument(
        "--concurrency", type=int, default=0,
        help="Concurrent threads; 0 = serial, >0 = concurrent "
             "(default: 0)")
    bench.add_argument(
        "--duration", type=float, default=30.0,
        help="Duration in seconds — concurrent mode (default: 30)")
    bench.add_argument(
        "--ramp-up", type=float, default=0.0,
        help="Ramp-up seconds — concurrent mode (default: 0)")
    bench.add_argument(
        "--query-timeout", type=int, default=5,
        help="Query timeout in seconds; slow queries will be logged as outliers "
             "(default: 5, 0 to disable)")
    bench.add_argument(
        "--flamegraph-min-ms", type=int, default=1000,
        help="Minimum total duration (ms) to show flamegraph in serial mode "
             "(default: 1000, 0 to always show)")
    bench.add_argument(
        "--bench-filter",
        help="Run only queries matching these names "
             "(comma-separated)")
    bench.add_argument(
        "--repeat", type=int, default=1,
        help="Number of benchmark rounds; each round produces "
             "a timestamped report (default: 1)")
    bench.add_argument(
        "--skip-setup", action="store_true", default=False,
        help="Skip setup phase (reuse existing tables from previous run)")
    bench.add_argument(
        "--skip-teardown", action="store_true", default=False,
        help="Skip teardown (keep tables for next run with --skip-setup)")
    bench.add_argument(
        "--no-parallel-dbms", dest="parallel_dbms",
        action="store_false",
        help="Run DBMS targets sequentially instead of in parallel")
    bench.set_defaults(parallel_dbms=True)

    # -- Profiling options ----------------------------------------------------
    prof = p.add_argument_group(
        "Profiling",
        "CPU flame-graph capture via perf (benchmark mode)")
    prof.add_argument(
        "--profile", action="store_true", dest="profile",
        default=True,
        help="Enable flame-graph capture (default: on)")
    prof.add_argument(
        "--no-profile", action="store_false", dest="profile",
        help="Disable flame-graph capture")
    prof.add_argument(
        "--perf-freq", type=int, default=99,
        help="perf sampling frequency in Hz (default: 99)")

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

    # List built-in benchmark templates
    if args.list_templates:
        from .benchmark import BenchmarkLoader
        templates = BenchmarkLoader.list_builtin_templates()
        console.print("[bold]Available built-in benchmark templates:[/bold]")
        for t in templates:
            console.print(f"  [cyan]•[/cyan] {t}")
        return 0

    # Benchmark mode (non-interactive)
    if args.benchmark and not args.interactive:
        return _run_benchmark(args)

    # Interactive mode — show mode selection (MTR / Benchmark)
    # If --benchmark is also set, skips selection and goes directly to bench mode
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


def _run_benchmark(args) -> int:
    """Execute the benchmark pipeline (supports --repeat N)."""
    from .benchmark import (BenchmarkLoader, run_benchmark,
                            BUILTIN_TEMPLATES)
    from .models import BenchmarkConfig, WorkloadMode
    from .reporter.bench_text import write_bench_text_report
    from .reporter.bench_html import write_bench_html_report
    from .ui import BenchProgress, print_bench_summary

    # Load DBMS configs
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
        print_error("No databases selected for benchmark")
        flush_all()
        return 1

    # Load workload
    json_extra_config = {}  # Extra config from JSON file
    try:
        if args.bench_file:
            workload = BenchmarkLoader.from_file(args.bench_file)
            # Read extra config fields from JSON file
            if args.bench_file.endswith('.json'):
                import json
                with open(args.bench_file, 'r') as f:
                    json_data = json.load(f)
                    json_extra_config = {
                        'database': json_data.get('database'),
                        'skip_setup': json_data.get('skip_setup'),
                        'skip_teardown': json_data.get('skip_teardown'),
                    }
        elif args.template:
            workload = BenchmarkLoader.from_builtin(args.template)
        else:
            # Default to oltp_read_write
            print_info("No --bench-file or --template specified, "
                       "using built-in", "oltp_read_write")
            workload = BenchmarkLoader.from_builtin("oltp_read_write")
    except (FileNotFoundError, ValueError) as e:
        print_error(str(e))
        flush_all()
        return 1

    # Build benchmark config
    if args.concurrency > 0:
        mode = WorkloadMode.CONCURRENT
    else:
        mode = WorkloadMode.SERIAL

    filter_queries = []
    if args.bench_filter:
        filter_queries = [
            n.strip() for n in args.bench_filter.split(",") if n.strip()
        ]

    # Determine skip_setup and skip_teardown: CLI args override JSON config
    json_skip_setup = json_extra_config.get('skip_setup')
    json_skip_teardown = json_extra_config.get('skip_teardown')
    
    # Use JSON value as default, CLI arg overrides if explicitly set
    # (CLI arg defaults to False, so only override if user explicitly passed it)
    cli_skip_setup = getattr(args, 'skip_setup', False)
    cli_skip_teardown = getattr(args, 'skip_teardown', False)
    
    # If JSON has the value and CLI didn't explicitly set it, use JSON value
    final_skip_setup = cli_skip_setup if cli_skip_setup else (json_skip_setup if json_skip_setup is not None else False)
    final_skip_teardown = cli_skip_teardown if cli_skip_teardown else (json_skip_teardown if json_skip_teardown is not None else False)
    
    bench_cfg = BenchmarkConfig(
        mode=mode,
        iterations=args.iterations,
        warmup=args.warmup,
        concurrency=args.concurrency if args.concurrency > 0 else 1,
        duration=args.duration,
        ramp_up=args.ramp_up,
        filter_queries=filter_queries,
        profile=getattr(args, 'profile', False),
        perf_freq=getattr(args, 'perf_freq', 99),
        query_timeout=args.query_timeout,
        flamegraph_min_ms=getattr(args, 'flamegraph_min_ms', 1000),
        skip_setup=final_skip_setup,
        skip_teardown=final_skip_teardown,
    )

    # Apply filter to workload for display
    display_workload = workload
    if filter_queries:
        try:
            display_workload = BenchmarkLoader.filter_queries(
                workload, filter_queries)
        except ValueError as e:
            print_error(str(e))
            flush_all()
            return 1

    # Display plan
    parallel_dbms = getattr(args, 'parallel_dbms', False)
    repeat = max(1, getattr(args, 'repeat', 1))
    output_dir = os.path.abspath(args.output_dir)
    fmt = args.format

    print_phase("Benchmark", workload.name)
    print_info("Mode:", mode.name)
    print_info("DBMS targets:",
               ", ".join(c.name for c in configs))
    if parallel_dbms and len(configs) > 1:
        print_info("DBMS execution:", "[bold green]parallel[/bold green]")
    elif not parallel_dbms and len(configs) > 1:
        print_info("DBMS execution:", "sequential")
    print_info("Queries:",
               ", ".join(q.name for q in display_workload.queries))
    if mode == WorkloadMode.SERIAL:
        print_info("Iterations:",
                    f"{bench_cfg.iterations}  Warmup: {bench_cfg.warmup}")
    else:
        print_info("Concurrency:",
                    f"{bench_cfg.concurrency}  Duration: {bench_cfg.duration}s")
    if filter_queries:
        print_info("Filter:", ", ".join(filter_queries))
    if repeat > 1:
        print_info("Repeat:", f"{repeat} rounds")
    if bench_cfg.profile:
        print_info("Profiling:",
                    f"[bold red]🔥 perf flame graph[/bold red] "
                    f"(freq: {bench_cfg.perf_freq} Hz)")
    if bench_cfg.skip_setup:
        print_info("Setup:", "[bold yellow]SKIPPED[/bold yellow] (reusing existing tables)")
    if bench_cfg.skip_teardown:
        print_info("Teardown:", "[bold yellow]SKIPPED[/bold yellow] (keeping tables)")

    # ------------------------------------------------------------------
    # Inner function: execute a single benchmark round
    # ------------------------------------------------------------------
    def _run_one_round(round_num: int) -> int:
        """Run one benchmark round. Returns 0 on success."""
        if repeat > 1:
            console.print(
                f"\n[bold cyan]{'━' * 60}[/bold cyan]")
            console.print(
                f"[bold cyan]  Round {round_num}/{repeat}[/bold cyan]")
            console.print(
                f"[bold cyan]{'━' * 60}[/bold cyan]\n")

        run_stamp = _time.strftime("%Y%m%d_%H%M%S")
        run_dir = os.path.join(
            output_dir,
            f"bench_{workload.name}_{run_stamp}")
        os.makedirs(run_dir, exist_ok=True)

        # Execute benchmark
        print_phase("Execute")

        # Progress tracking (fresh each round)
        progress_bars: Dict[str, BenchProgress] = {}
        _progress_lock = threading.Lock()

        n_queries = len(display_workload.queries)
        is_concurrent = (mode == WorkloadMode.CONCURRENT)
        if is_concurrent:
            duration = bench_cfg.duration if bench_cfg.duration > 0 else 30.0
            per_query = 100  # placeholder, not used for time-based
        else:
            duration = 0.0
            per_query = bench_cfg.iterations + bench_cfg.warmup

        # Create progress bars upfront (they will show "setup..." initially)
        if parallel_dbms and len(configs) > 1:
            for c in configs:
                bp = BenchProgress(
                    c.name, n_queries, per_query,
                    is_concurrent=is_concurrent, duration=duration)
                bp.__enter__()
                bp.set_status("[yellow]正在setup...[/yellow]")
                progress_bars[c.name] = bp

        def on_setup_start(dbms_name):
            with _progress_lock:
                if dbms_name not in progress_bars:
                    bp = BenchProgress(
                        dbms_name, n_queries, per_query,
                        is_concurrent=is_concurrent, duration=duration)
                    bp.__enter__()
                    bp.set_status("[yellow]正在setup...[/yellow]")
                    progress_bars[dbms_name] = bp

        def on_setup_done(dbms_name, success):
            bp = progress_bars.get(dbms_name)
            if bp:
                if success:
                    bp.set_status("[green]setup完毕[/green]")
                else:
                    bp.set_status("[red]setup失败[/red]")

        def on_dbms_start(dbms_name):
            with _progress_lock:
                if dbms_name not in progress_bars:
                    bp = BenchProgress(
                        dbms_name, n_queries, per_query,
                        is_concurrent=is_concurrent, duration=duration)
                    bp.__enter__()
                    progress_bars[dbms_name] = bp

        def on_progress(dbms_name, query_name, iteration, total,
                        is_warmup=False):
            bp = progress_bars.get(dbms_name)
            if bp and not is_concurrent:
                bp.advance(query_name=query_name, is_warmup=is_warmup)

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
                bp.set_status(f"[red]🔥 profiling {query_name}[/red]")

        def on_profile_done(dbms_name, query_name, sample_count):
            bp = progress_bars.get(dbms_name)
            if bp:
                bp.set_status(
                    f"[dim]🔥 {query_name}: {sample_count} samples[/dim]")

        # For concurrent mode, timer thread will be started after setup phase
        timer_stop_event = None
        timer_thread = None
        query_phase_started = threading.Event()
        timer_start_time = [None]  # Will be set in on_run_start

        if is_concurrent:
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
                    for dbms_name, bp in list(progress_bars.items()):
                        bp.update_time(status="")
                    _time.sleep(0.5)

            timer_thread = threading.Thread(target=_timer_update, daemon=True)
            timer_thread.start()

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

        # Determine database: JSON config takes precedence over default, CLI arg always wins
        json_database = json_extra_config.get('database')
        # If JSON specifies a database, use it; otherwise use CLI arg (which has default)
        final_database = json_database if json_database else args.database
        
        try:
            result = run_benchmark(
                configs=configs,
                workload=workload,
                bench_cfg=bench_cfg,
                database=final_database,
                on_progress=on_progress,
                on_dbms_start=on_dbms_start,
                on_dbms_done=on_dbms_done,
                on_profile_start=on_profile_start if bench_cfg.profile else None,
                on_profile_done=on_profile_done if bench_cfg.profile else None,
                on_run_start=on_run_start,
                on_setup_start=on_setup_start,
                on_setup_done=on_setup_done,
                parallel_dbms=parallel_dbms,
            )
        finally:
            # Stop timer thread
            if timer_stop_event is not None:
                timer_stop_event.set()
                if timer_thread is not None:
                    timer_thread.join(timeout=1.0)

        # Generate reports
        print_phase("Reports")

        if fmt in ("text", "all"):
            text_path = os.path.join(
                run_dir, f"bench_{workload.name}.report.txt")
            write_bench_text_report(text_path, result)
            print_report_file(text_path, label="text")

        if fmt in ("html", "all"):
            html_path = os.path.join(
                run_dir, f"bench_{workload.name}.html")
            write_bench_html_report(html_path, result)
            print_report_file(html_path, label="html")

        # Save raw JSON data
        json_path = os.path.join(run_dir, "bench_result.json")
        _save_bench_json(json_path, result)
        print_report_file(json_path, label="json")

        # Update 'latest' symlink
        latest_link = os.path.join(output_dir, "latest")
        try:
            if os.path.islink(latest_link):
                os.remove(latest_link)
            os.symlink(os.path.basename(run_dir), latest_link)
        except OSError:
            pass

        # Generate history index
        generate_index_html(output_dir)

        # Print rich summary
        print_bench_summary(result)
        flush_all()

        return run_dir

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    last_run_dir = None
    for rnd in range(1, repeat + 1):
        try:
            last_run_dir = _run_one_round(rnd)
        except KeyboardInterrupt:
            console.print(
                f"\n[yellow]Interrupted at round {rnd}/{repeat}. "
                f"Stopping.[/yellow]")
            flush_all()
            break
        # Small pause between rounds to avoid timestamp collision
        if rnd < repeat:
            _time.sleep(1)

    if repeat > 1:
        console.print(
            f"\n[bold green]All {repeat} rounds completed.[/bold green]")
        flush_all()

    # Serve if requested (use the latest run)
    if args.serve and fmt in ("html", "all") and last_run_dir:
        html_file = f"bench_{workload.name}.html"
        html_path = os.path.join(last_run_dir, html_file)
        if os.path.isfile(html_path):
            relative_html = os.path.join(
                os.path.basename(last_run_dir), html_file)
            _serve_report(output_dir, relative_html, args.port)

    return 0


def _save_bench_json(path: str, result):
    """Save benchmark result as JSON for later analysis."""
    import json
    data = {
        "workload": result.workload_name,
        "mode": result.mode.name,
        "timestamp": result.timestamp,
        "table_rows": result.table_rows,
        "table_rows_detail": result.table_rows_detail or {},
        "setup_sql": list(result.setup_sql) if result.setup_sql else [],
        "teardown_sql": list(result.teardown_sql) if result.teardown_sql else [],
        "queries_sql": list(result.queries_sql) if result.queries_sql else [],
        "config": {
            "iterations": result.config.iterations,
            "warmup": result.config.warmup,
            "concurrency": result.config.concurrency,
            "duration": result.config.duration,
            "filter_queries": result.config.filter_queries,
        },
        "dbms_results": [],
    }
    for dr in result.dbms_results:
        dbms_data = {
            "dbms_name": dr.dbms_name,
            "total_duration_s": round(dr.total_duration_s, 3),
            "total_queries": dr.total_queries,
            "total_errors": dr.total_errors,
            "overall_qps": round(dr.overall_qps, 2),
            "query_stats": [],
        }
        for qs in dr.query_stats:
            dbms_data["query_stats"].append({
                "query_name": qs.query_name,
                "total_executions": qs.total_executions,
                "total_errors": qs.total_errors,
                "min_ms": round(qs.min_ms, 3),
                "max_ms": round(qs.max_ms, 3),
                "avg_ms": round(qs.avg_ms, 3),
                "p50_ms": round(qs.p50_ms, 3),
                "p95_ms": round(qs.p95_ms, 3),
                "p99_ms": round(qs.p99_ms, 3),
                "qps": round(qs.qps, 2),
            })
        data["dbms_results"].append(dbms_data)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def run_benchmark_with_progress(
    configs: List[DBMSConfig],
    workload,
    bench_cfg,
    database: str,
    output_dir: str,
    output_format: str = "all",
    parallel_dbms: bool = True,
    json_extra_config: Optional[dict] = None,
    callbacks: Optional[dict] = None,
) -> Tuple[str, object]:
    """Core benchmark execution logic shared by CLI and Interactive modes.

    Args:
        configs: List of DBMS configurations
        workload: Benchmark workload (from BenchmarkLoader)
        bench_cfg: Benchmark configuration
        database: Database name
        output_dir: Output directory for reports
        output_format: Output format (text, html, all)
        parallel_dbms: Whether to run benchmarks in parallel
        json_extra_config: Extra config from JSON file (database, skip_setup, skip_teardown)
        callbacks: Optional callbacks for progress tracking:
            - on_progress(dbms_name, query_name, iteration, total, is_warmup)
            - on_dbms_start(dbms_name)
            - on_dbms_done(dbms_name, dbms_result)
            - on_profile_start(dbms_name, query_name)
            - on_profile_done(dbms_name, query_name, sample_count)
            - on_run_start()
            - on_setup_start(dbms_name)
            - on_setup_done(dbms_name, success)

    Returns:
        Tuple of (run_dir, result)
    """
    from .benchmark import run_benchmark
    from .reporter.bench_text import write_bench_text_report
    from .reporter.bench_html import write_bench_html_report
    from .reporter.history import generate_index_html

    callbacks = callbacks or {}

    # Create output directory
    run_stamp = _time.strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(
        output_dir,
        f"bench_{workload.name}_{run_stamp}"
    )
    os.makedirs(run_dir, exist_ok=True)

    # Determine database from JSON config if provided
    json_extra_config = json_extra_config or {}
    json_database = json_extra_config.get('database')
    final_database = json_database if json_database else database

    # Execute benchmark
    result = run_benchmark(
        configs=configs,
        workload=workload,
        bench_cfg=bench_cfg,
        database=final_database,
        on_progress=callbacks.get('on_progress'),
        on_dbms_start=callbacks.get('on_dbms_start'),
        on_dbms_done=callbacks.get('on_dbms_done'),
        on_profile_start=callbacks.get('on_profile_start'),
        on_profile_done=callbacks.get('on_profile_done'),
        on_run_start=callbacks.get('on_run_start'),
        on_setup_start=callbacks.get('on_setup_start'),
        on_setup_done=callbacks.get('on_setup_done'),
        parallel_dbms=parallel_dbms,
    )

    # Generate reports
    report_files = []

    if output_format in ("text", "all"):
        text_path = os.path.join(run_dir, f"bench_{workload.name}.report.txt")
        write_bench_text_report(text_path, result)
        report_files.append(text_path)

    if output_format in ("html", "all"):
        html_path = os.path.join(run_dir, f"bench_{workload.name}.html")
        write_bench_html_report(html_path, result)
        report_files.append(html_path)

    # Save JSON result
    json_path = os.path.join(run_dir, "bench_result.json")
    _save_bench_json(json_path, result)
    report_files.append(json_path)

    # Update latest symlink
    latest_link = os.path.join(output_dir, "latest")
    try:
        if os.path.islink(latest_link):
            os.remove(latest_link)
        os.symlink(os.path.basename(run_dir), latest_link)
    except OSError:
        pass

    # Generate history index
    generate_index_html(output_dir)

    return run_dir, result


def _select_bench_params(
    iterations: int = 100,
    warmup: int = 5,
    concurrency: int = 8,
    duration: float = 30.0,
    ramp_up: float = 0.0,
    profile: bool = True,
    skip_setup: bool = False,
    skip_teardown: bool = False,
) -> Optional[dict]:
    """Show an interactive benchmark parameter configuration panel.

    First, select mode (SERIAL or CONCURRENT), then configure parameters
    based on the selected mode.

    Returns a dict with mode-specific parameters, or ``None`` if cancelled.
    """
    # Step 1: Mode selection
    mode_result = _select_bench_mode()
    if mode_result is None:
        return None
    if mode_result.get("action") == "back":
        return {"action": "back"}

    mode = mode_result["mode"]  # "serial" or "concurrent"

    # Step 2: Parameter configuration based on mode
    if mode == "serial":
        return _select_serial_params(iterations, warmup, profile, skip_setup, skip_teardown)
    else:
        return _select_concurrent_params(concurrency, duration, ramp_up, profile, skip_setup, skip_teardown)


def _select_bench_mode() -> Optional[dict]:
    """Show mode selection dialog for benchmark.

    Returns dict with "mode" key ("serial" or "concurrent"),
    or None if cancelled.
    """
    from prompt_toolkit import Application
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import Layout
    from prompt_toolkit.layout.containers import HSplit, Window
    from prompt_toolkit.layout.controls import FormattedTextControl

    MODES = [
        ("serial", "SERIAL",
         "Sequential execution, fixed iterations per query"),
        ("concurrent", "CONCURRENT",
         "Multi-threaded stress test with duration-based execution"),
    ]

    selected = [0]
    result = [None]

    kb = KeyBindings()

    @kb.add("up")
    @kb.add("k")
    def _up(event):
        selected[0] = (selected[0] - 1) % len(MODES)

    @kb.add("down")
    @kb.add("j")
    def _down(event):
        selected[0] = (selected[0] + 1) % len(MODES)

    @kb.add("enter")
    def _confirm(event):
        result[0] = {"mode": MODES[selected[0]][0]}
        event.app.exit()

    @kb.add("c-c")
    @kb.add("escape")
    def _cancel(event):
        result[0] = None
        event.app.exit()

    @kb.add("b")
    def _back(event):
        result[0] = {"action": "back"}
        event.app.exit()

    def _get_text():
        lines = []
        border = "═" * 58
        title = "Select Benchmark Mode".center(58)
        hint = "↑↓ move · Enter select · B back · Esc cancel".center(58)

        lines.append(("bold cyan", f"  ╔{border}╗\n"))
        lines.append(("bold cyan", "  ║"))
        lines.append(("bold white", title))
        lines.append(("bold cyan", "║\n"))
        lines.append(("bold cyan", "  ║"))
        lines.append(("", hint))
        lines.append(("bold cyan", "║\n"))
        lines.append(("bold cyan", f"  ╚{border}╝\n"))
        lines.append(("", "\n"))

        for i, (mode_key, mode_name, mode_desc) in enumerate(MODES):
            is_sel = (i == selected[0])
            if is_sel:
                lines.append(("bold cyan", "  ❯ "))
                lines.append(("bold yellow", mode_name))
                lines.append(("", "\n"))
                lines.append(("", "      "))
                lines.append(("dim", mode_desc))
                lines.append(("", "\n"))
            else:
                lines.append(("", "    "))
                lines.append(("bold", mode_name))
                lines.append(("", "\n"))
                lines.append(("", "      "))
                lines.append(("dim", mode_desc))
                lines.append(("", "\n"))

        lines.append(("", "\n"))
        lines.append(("dim", "  ────────────────────────────────────────\n"))
        lines.append(("dim", "  SERIAL:      Each query runs N times sequentially\n"))
        lines.append(("dim", "  CONCURRENT:  Multiple threads, duration-based test\n"))

        return lines

    menu = Window(
        content=FormattedTextControl(_get_text),
        dont_extend_height=True,
    )

    app: Application = Application(
        layout=Layout(HSplit([menu])),
        key_bindings=kb,
        full_screen=False,
    )

    _tty_write("\033[s")
    app.run()
    _tty_write("\033[u\033[J")

    return result[0]


def _select_serial_params(
    iterations: int = 100,
    warmup: int = 5,
    profile: bool = True,
    skip_setup: bool = False,
    skip_teardown: bool = False,
) -> Optional[dict]:
    """Show parameter configuration for SERIAL mode."""
    from prompt_toolkit import Application
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import Layout
    from prompt_toolkit.layout.containers import HSplit, Window
    from prompt_toolkit.layout.controls import FormattedTextControl
    from prompt_toolkit.keys import Keys
    from prompt_toolkit.filters import Condition

    ITER_PRESETS = [10, 50, 100, 200, 500, 1000]
    WARMUP_PRESETS = [0, 5, 10, 20, 50]
    PROFILE_LABELS = {False: "Off", True: "On"}
    SKIP_LABELS = {False: "Off", True: "On"}

    custom_iter = [None]
    custom_warmup = [None]

    result = [None]
    sel = [0]
    it_idx = [ITER_PRESETS.index(iterations)
              if iterations in ITER_PRESETS else 2]
    wa_idx = [WARMUP_PRESETS.index(warmup)
              if warmup in WARMUP_PRESETS else 1]
    prof = [profile]
    s_setup = [skip_setup]
    s_teardown = [skip_teardown]

    FIELDS = [
        {"label": "Iterations", "type": "choice"},
        {"label": "Warmup", "type": "choice"},
        {"label": "Profile (flame graph)", "type": "toggle", "var": "prof"},
        {"label": "Skip Setup (reuse tables)", "type": "toggle", "var": "s_setup"},
        {"label": "Skip Teardown (keep tables)", "type": "toggle", "var": "s_teardown"},
        {"label": "OK", "type": "action"},
        {"label": "Back", "type": "action"},
        {"label": "Quit", "type": "action"},
    ]

    ACTION_OK = len(FIELDS) - 3
    ACTION_BACK = len(FIELDS) - 2
    ACTION_QUIT = len(FIELDS) - 1

    def _iter_val():
        if custom_iter[0] is not None:
            return custom_iter[0]
        return ITER_PRESETS[it_idx[0]]

    def _warmup_val():
        if custom_warmup[0] is not None:
            return custom_warmup[0]
        return WARMUP_PRESETS[wa_idx[0]]

    def _field_val(i):
        if i == 0:
            v = _iter_val()
            if custom_iter[0] is not None:
                return f"{v} (custom)"
            return str(v)
        elif i == 1:
            v = _warmup_val()
            if custom_warmup[0] is not None:
                return f"{v} (custom)"
            return str(v)
        elif i == 2:
            return PROFILE_LABELS[prof[0]]
        elif i == 3:
            return SKIP_LABELS[s_setup[0]]
        elif i == 4:
            return SKIP_LABELS[s_teardown[0]]
        return ""

    def _get_toggle_var(i):
        """Get the toggle variable list for field index."""
        if i == 2: return prof
        if i == 3: return s_setup
        if i == 4: return s_teardown
        return None

    def _toggle_right(i):
        if i == 0:
            if custom_iter[0] is not None:
                custom_iter[0] = None
            else:
                if it_idx[0] == len(ITER_PRESETS) - 1:
                    custom_iter[0] = _iter_val()
                else:
                    it_idx[0] += 1
        elif i == 1:
            if custom_warmup[0] is not None:
                custom_warmup[0] = None
            else:
                if wa_idx[0] == len(WARMUP_PRESETS) - 1:
                    custom_warmup[0] = _warmup_val()
                else:
                    wa_idx[0] += 1
        else:
            var = _get_toggle_var(i)
            if var is not None:
                var[0] = not var[0]

    def _toggle_left(i):
        if i == 0:
            if custom_iter[0] is not None:
                custom_iter[0] = None
            else:
                if it_idx[0] == 0:
                    custom_iter[0] = _iter_val()
                    it_idx[0] = 0
                else:
                    it_idx[0] -= 1
        elif i == 1:
            if custom_warmup[0] is not None:
                custom_warmup[0] = None
            else:
                if wa_idx[0] == 0:
                    custom_warmup[0] = _warmup_val()
                    wa_idx[0] = 0
                else:
                    wa_idx[0] -= 1
        else:
            var = _get_toggle_var(i)
            if var is not None:
                var[0] = not var[0]

    editing = [None]
    edit_buf = [""]

    kb = KeyBindings()

    @kb.add("up")
    @kb.add("k")
    def _up(event):
        if editing[0] is not None:
            return
        sel[0] = (sel[0] - 1) % len(FIELDS)

    @kb.add("down")
    @kb.add("j")
    def _down(event):
        if editing[0] is not None:
            return
        sel[0] = (sel[0] + 1) % len(FIELDS)

    @kb.add("left")
    @kb.add("h")
    def _left(event):
        if editing[0] is not None:
            return
        _toggle_left(sel[0])

    @kb.add("right")
    @kb.add("l")
    def _right(event):
        if editing[0] is not None:
            return
        _toggle_right(sel[0])

    @kb.add("backspace")
    def _backspace(event):
        if editing[0] is not None:
            edit_buf[0] = edit_buf[0][:-1]

    @kb.add(Keys.Any, filter=Condition(lambda: editing[0] is not None))
    def _type_char(event):
        ch = event.data
        if ch.isdigit():
            edit_buf[0] += ch

    @kb.add("enter")
    def _confirm(event):
        if editing[0] is not None:
            idx = editing[0]
            if edit_buf[0]:
                try:
                    n = int(edit_buf[0])
                    if n >= 0:
                        if idx == 0:
                            custom_iter[0] = n
                        else:
                            custom_warmup[0] = n
                except ValueError:
                    pass
            editing[0] = None
            edit_buf[0] = ""
            return

        if sel[0] == 0 and custom_iter[0] is not None:
            editing[0] = 0
            edit_buf[0] = str(custom_iter[0])
            return
        if sel[0] == 1 and custom_warmup[0] is not None:
            editing[0] = 1
            edit_buf[0] = str(custom_warmup[0])
            return

        if sel[0] == ACTION_OK:
            result[0] = {
                "mode": "serial",
                "iterations": _iter_val(),
                "warmup": _warmup_val(),
                "concurrency": 0,
                "duration": 0.0,
                "ramp_up": 0.0,
                "profile": prof[0],
                "skip_setup": s_setup[0],
                "skip_teardown": s_teardown[0],
            }
            event.app.exit()
            return
        if sel[0] == ACTION_BACK:
            result[0] = {"action": "back"}
            event.app.exit()
            return
        if sel[0] == ACTION_QUIT:
            result[0] = None
            event.app.exit()
            return

    @kb.add("c-c")
    @kb.add("escape")
    def _cancel(event):
        if editing[0] is not None:
            editing[0] = None
            edit_buf[0] = ""
            return
        result[0] = None
        event.app.exit()

    def _get_text():
        lines = []
        border = "═" * 55
        title = "SERIAL Mode Configuration".center(55)
        hint = ("←→ change · Enter confirm/custom · ↑↓ move"
                " · Esc cancel").center(55)
        lines.append(("bold cyan", f"  ╔{border}╗\n"))
        lines.append(("bold cyan", "  ║"))
        lines.append(("bold white", title))
        lines.append(("bold cyan", "║\n"))
        lines.append(("bold cyan", "  ║"))
        lines.append(("", hint))
        lines.append(("bold cyan", "║\n"))
        lines.append(("bold cyan", f"  ╚{border}╝\n"))
        lines.append(("", "\n"))

        if editing[0] is not None:
            idx = editing[0]
            label = FIELDS[idx]["label"]
            lines.append(("bold cyan", "  ❯ "))
            lines.append(("bold cyan", label))
            lines.append(("", "  "))
            lines.append(("bold white", f"[ {edit_buf[0]}▌ ]"))
            lines.append(("", "\n"))
            lines.append(("dim",
                         "     Type a number, Enter to confirm, "
                         "Esc to cancel\n"))
            return lines

        for i, field in enumerate(FIELDS):
            is_sel = (i == sel[0])

            if field["type"] == "action":
                if is_sel:
                    prefix = ("bold cyan", "  ❯ ")
                    label = ("bold cyan", field["label"])
                else:
                    prefix = ("", "    ")
                    label = ("dim", field["label"])
                lines.append(prefix)
                lines.append(label)
                lines.append(("", "\n"))
                continue

            prefix = ("bold cyan", "  ❯ ") if is_sel else ("", "    ")
            label = ("bold cyan" if is_sel else "bold",
                     field["label"])

            val_str = _field_val(i)
            if field["type"] == "choice":
                if is_sel:
                    val = ("bold yellow", f"◄ {val_str} ►")
                else:
                    val = ("dim", val_str)
            else:
                toggle_var = _get_toggle_var(i)
                toggle_on = toggle_var[0] if toggle_var else False
                if toggle_on:
                    val = ("bold green" if is_sel else "green",
                           f"● {val_str}")
                else:
                    val = ("dim", f"○ {val_str}")

            lines.append(prefix)
            lines.append(label)
            lines.append(("", "  "))
            lines.append(val)
            lines.append(("", "\n"))

        return lines

    menu = Window(
        content=FormattedTextControl(_get_text),
        dont_extend_height=True,
    )

    app: Application = Application(
        layout=Layout(HSplit([menu])),
        key_bindings=kb,
        full_screen=False,
    )

    _tty_write("\033[s")
    app.run()
    _tty_write("\033[u\033[J")

    return result[0]


def _select_concurrent_params(
    concurrency: int = 8,
    duration: float = 30.0,
    ramp_up: float = 0.0,
    profile: bool = True,
    skip_setup: bool = False,
    skip_teardown: bool = False,
) -> Optional[dict]:
    """Show parameter configuration for CONCURRENT mode."""
    from prompt_toolkit import Application
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import Layout
    from prompt_toolkit.layout.containers import HSplit, Window
    from prompt_toolkit.layout.controls import FormattedTextControl
    from prompt_toolkit.keys import Keys
    from prompt_toolkit.filters import Condition

    CONCURRENCY_PRESETS = [1, 2, 4, 8, 16, 32, 64]
    DURATION_PRESETS = [10.0, 30.0, 60.0, 120.0, 300.0]
    RAMPUP_PRESETS = [0.0, 1.0, 2.0, 5.0, 10.0]
    PROFILE_LABELS = {False: "Off", True: "On"}
    SKIP_LABELS = {False: "Off", True: "On"}

    custom_concurrency = [None]
    custom_duration = [None]
    custom_rampup = [None]

    result = [None]
    sel = [0]
    cc_idx = [CONCURRENCY_PRESETS.index(concurrency)
              if concurrency in CONCURRENCY_PRESETS else 3]
    dur_idx = [DURATION_PRESETS.index(duration)
               if duration in DURATION_PRESETS else 1]
    ramp_idx = [RAMPUP_PRESETS.index(ramp_up)
                if ramp_up in RAMPUP_PRESETS else 0]
    prof = [profile]
    s_setup = [skip_setup]
    s_teardown = [skip_teardown]

    FIELDS = [
        {"label": "Concurrency (threads)", "type": "choice"},
        {"label": "Duration (seconds)", "type": "choice"},
        {"label": "Ramp-up (seconds)", "type": "choice"},
        {"label": "Profile (flame graph)", "type": "toggle", "var": "prof"},
        {"label": "Skip Setup (reuse tables)", "type": "toggle", "var": "s_setup"},
        {"label": "Skip Teardown (keep tables)", "type": "toggle", "var": "s_teardown"},
        {"label": "OK", "type": "action"},
        {"label": "Back", "type": "action"},
        {"label": "Quit", "type": "action"},
    ]

    ACTION_OK = len(FIELDS) - 3
    ACTION_BACK = len(FIELDS) - 2
    ACTION_QUIT = len(FIELDS) - 1

    def _concurrency_val():
        if custom_concurrency[0] is not None:
            return custom_concurrency[0]
        return CONCURRENCY_PRESETS[cc_idx[0]]

    def _duration_val():
        if custom_duration[0] is not None:
            return custom_duration[0]
        return DURATION_PRESETS[dur_idx[0]]

    def _rampup_val():
        if custom_rampup[0] is not None:
            return custom_rampup[0]
        return RAMPUP_PRESETS[ramp_idx[0]]

    def _field_val(i):
        if i == 0:
            v = _concurrency_val()
            if custom_concurrency[0] is not None:
                return f"{v} (custom)"
            return str(v)
        elif i == 1:
            v = _duration_val()
            if custom_duration[0] is not None:
                return f"{v} (custom)"
            return str(v)
        elif i == 2:
            v = _rampup_val()
            if custom_rampup[0] is not None:
                return f"{v} (custom)"
            return str(v)
        elif i == 3:
            return PROFILE_LABELS[prof[0]]
        elif i == 4:
            return SKIP_LABELS[s_setup[0]]
        elif i == 5:
            return SKIP_LABELS[s_teardown[0]]
        return ""

    def _get_toggle_var(i):
        if i == 3: return prof
        if i == 4: return s_setup
        if i == 5: return s_teardown
        return None

    def _toggle_right(i):
        if i == 0:
            if custom_concurrency[0] is not None:
                custom_concurrency[0] = None
            else:
                if cc_idx[0] == len(CONCURRENCY_PRESETS) - 1:
                    custom_concurrency[0] = _concurrency_val()
                else:
                    cc_idx[0] += 1
        elif i == 1:
            if custom_duration[0] is not None:
                custom_duration[0] = None
            else:
                if dur_idx[0] == len(DURATION_PRESETS) - 1:
                    custom_duration[0] = _duration_val()
                else:
                    dur_idx[0] += 1
        elif i == 2:
            if custom_rampup[0] is not None:
                custom_rampup[0] = None
            else:
                if ramp_idx[0] == len(RAMPUP_PRESETS) - 1:
                    custom_rampup[0] = _rampup_val()
                else:
                    ramp_idx[0] += 1
        else:
            var = _get_toggle_var(i)
            if var is not None:
                var[0] = not var[0]

    def _toggle_left(i):
        if i == 0:
            if custom_concurrency[0] is not None:
                custom_concurrency[0] = None
            else:
                if cc_idx[0] == 0:
                    custom_concurrency[0] = _concurrency_val()
                    cc_idx[0] = 0
                else:
                    cc_idx[0] -= 1
        elif i == 1:
            if custom_duration[0] is not None:
                custom_duration[0] = None
            else:
                if dur_idx[0] == 0:
                    custom_duration[0] = _duration_val()
                    dur_idx[0] = 0
                else:
                    dur_idx[0] -= 1
        elif i == 2:
            if custom_rampup[0] is not None:
                custom_rampup[0] = None
            else:
                if ramp_idx[0] == 0:
                    custom_rampup[0] = _rampup_val()
                    ramp_idx[0] = 0
                else:
                    ramp_idx[0] -= 1
        else:
            var = _get_toggle_var(i)
            if var is not None:
                var[0] = not var[0]

    editing = [None]
    edit_buf = [""]

    kb = KeyBindings()

    @kb.add("up")
    @kb.add("k")
    def _up(event):
        if editing[0] is not None:
            return
        sel[0] = (sel[0] - 1) % len(FIELDS)

    @kb.add("down")
    @kb.add("j")
    def _down(event):
        if editing[0] is not None:
            return
        sel[0] = (sel[0] + 1) % len(FIELDS)

    @kb.add("left")
    @kb.add("h")
    def _left(event):
        if editing[0] is not None:
            return
        _toggle_left(sel[0])

    @kb.add("right")
    @kb.add("l")
    def _right(event):
        if editing[0] is not None:
            return
        _toggle_right(sel[0])

    @kb.add("backspace")
    def _backspace(event):
        if editing[0] is not None:
            edit_buf[0] = edit_buf[0][:-1]

    @kb.add(Keys.Any, filter=Condition(lambda: editing[0] is not None))
    def _type_char(event):
        ch = event.data
        if ch.isdigit() or ch == '.':
            edit_buf[0] += ch

    @kb.add("enter")
    def _confirm(event):
        if editing[0] is not None:
            idx = editing[0]
            if edit_buf[0]:
                try:
                    if idx == 0:
                        n = int(edit_buf[0])
                        if n >= 1:
                            custom_concurrency[0] = n
                    elif idx == 1:
                        n = float(edit_buf[0])
                        if n > 0:
                            custom_duration[0] = n
                    elif idx == 2:
                        n = float(edit_buf[0])
                        if n >= 0:
                            custom_rampup[0] = n
                except ValueError:
                    pass
            editing[0] = None
            edit_buf[0] = ""
            return

        if sel[0] == 0 and custom_concurrency[0] is not None:
            editing[0] = 0
            edit_buf[0] = str(custom_concurrency[0])
            return
        if sel[0] == 1 and custom_duration[0] is not None:
            editing[0] = 1
            edit_buf[0] = str(custom_duration[0])
            return
        if sel[0] == 2 and custom_rampup[0] is not None:
            editing[0] = 2
            edit_buf[0] = str(custom_rampup[0])
            return

        if sel[0] == ACTION_OK:
            result[0] = {
                "mode": "concurrent",
                "iterations": 100,
                "warmup": 5,
                "concurrency": _concurrency_val(),
                "duration": _duration_val(),
                "ramp_up": _rampup_val(),
                "profile": prof[0],
                "skip_setup": s_setup[0],
                "skip_teardown": s_teardown[0],
            }
            event.app.exit()
            return
        if sel[0] == ACTION_BACK:
            result[0] = {"action": "back"}
            event.app.exit()
            return
        if sel[0] == ACTION_QUIT:
            result[0] = None
            event.app.exit()
            return

    @kb.add("c-c")
    @kb.add("escape")
    def _cancel(event):
        if editing[0] is not None:
            editing[0] = None
            edit_buf[0] = ""
            return
        result[0] = None
        event.app.exit()

    def _get_text():
        lines = []
        border = "═" * 55
        title = "CONCURRENT Mode Configuration".center(55)
        hint = ("←→ change · Enter confirm/custom · ↑↓ move"
                " · Esc cancel").center(55)
        lines.append(("bold cyan", f"  ╔{border}╗\n"))
        lines.append(("bold cyan", "  ║"))
        lines.append(("bold white", title))
        lines.append(("bold cyan", "║\n"))
        lines.append(("bold cyan", "  ║"))
        lines.append(("", hint))
        lines.append(("bold cyan", "║\n"))
        lines.append(("bold cyan", f"  ╚{border}╝\n"))
        lines.append(("", "\n"))

        if editing[0] is not None:
            idx = editing[0]
            label = FIELDS[idx]["label"]
            lines.append(("bold cyan", "  ❯ "))
            lines.append(("bold cyan", label))
            lines.append(("", "  "))
            lines.append(("bold white", f"[ {edit_buf[0]}▌ ]"))
            lines.append(("", "\n"))
            lines.append(("dim",
                         "     Type a number, Enter to confirm, "
                         "Esc to cancel\n"))
            return lines

        for i, field in enumerate(FIELDS):
            is_sel = (i == sel[0])

            if field["type"] == "action":
                if is_sel:
                    prefix = ("bold cyan", "  ❯ ")
                    label = ("bold cyan", field["label"])
                else:
                    prefix = ("", "    ")
                    label = ("dim", field["label"])
                lines.append(prefix)
                lines.append(label)
                lines.append(("", "\n"))
                continue

            prefix = ("bold cyan", "  ❯ ") if is_sel else ("", "    ")
            label = ("bold cyan" if is_sel else "bold",
                     field["label"])

            val_str = _field_val(i)
            if field["type"] == "choice":
                if is_sel:
                    val = ("bold yellow", f"◄ {val_str} ►")
                else:
                    val = ("dim", val_str)
            else:
                toggle_var = _get_toggle_var(i)
                toggle_on = toggle_var[0] if toggle_var else False
                if toggle_on:
                    val = ("bold green" if is_sel else "green",
                           f"● {val_str}")
                else:
                    val = ("dim", f"○ {val_str}")

            lines.append(prefix)
            lines.append(label)
            lines.append(("", "  "))
            lines.append(val)
            lines.append(("", "\n"))

        return lines

    menu = Window(
        content=FormattedTextControl(_get_text),
        dont_extend_height=True,
    )

    app: Application = Application(
        layout=Layout(HSplit([menu])),
        key_bindings=kb,
        full_screen=False,
    )

    _tty_write("\033[s")
    app.run()
    _tty_write("\033[u\033[J")

    return result[0]


def _select_mode(configs, database: str) -> Optional[str]:
    """Show an arrow-key mode selector and return 'mtr' or 'bench'.

    Returns ``None`` if the user cancels (Ctrl-C / Esc).
    """
    import sys

    from prompt_toolkit import Application
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import Layout
    from prompt_toolkit.layout.containers import HSplit, Window
    from prompt_toolkit.layout.controls import FormattedTextControl

    MODES = [
        ("mtr",        "MTR mode",        "run .test compatibility tests"),
        ("playground", "Playground mode",  "run SQL Playground in browser"),
        ("bench",      "Benchmark mode",   "run .json/.sql performance benchmarks"),
        (None,         "Quit",             "exit"),
    ]

    QUIT_IDX = len(MODES) - 1

    selected = [0]       # mutable index
    result = [None]      # mutable result

    # -- key bindings -------------------------------------------------------
    kb = KeyBindings()

    @kb.add("up")
    @kb.add("k")
    def _up(event):
        selected[0] = (selected[0] - 1) % len(MODES)

    @kb.add("down")
    @kb.add("j")
    def _down(event):
        selected[0] = (selected[0] + 1) % len(MODES)

    @kb.add("enter")
    def _confirm(event):
        key = MODES[selected[0]][0]
        result[0] = key  # None for Quit, 'mtr' or 'bench' otherwise
        event.app.exit()

    @kb.add("c-c")
    @kb.add("escape")
    def _cancel(event):
        result[0] = None
        event.app.exit()

    # -- layout -------------------------------------------------------------
    def _get_menu_text():
        lines = []
        border = "═" * 55
        title = "Rosetta Interactive Mode".center(55)
        hint = "↑/↓ to move, Enter to select, Esc to quit".center(55)
        lines.append(("bold cyan", f"  ╔{border}╗\n"))
        lines.append(("bold cyan", "  ║"))
        lines.append(("bold white", title))
        lines.append(("bold cyan", "║\n"))
        lines.append(("bold cyan", "  ║"))
        lines.append(("", hint))
        lines.append(("bold cyan", "║\n"))
        lines.append(("bold cyan", f"  ╚{border}╝\n"))
        lines.append(("", "\n"))

        dbms_str = ", ".join(c.name for c in configs)
        lines.append(("gray", "  DBMS: "))
        lines.append(("bold", dbms_str))
        lines.append(("gray", "  Database: "))
        lines.append(("bold", database))
        lines.append(("", "\n\n"))

        for i, (key, label, desc) in enumerate(MODES):
            is_quit = (key is None)
            if i == selected[0]:
                if is_quit:
                    lines.append(("bold cyan", "  ❯ "))
                    lines.append(("bold cyan", label))
                else:
                    lines.append(("bold cyan", "  ❯ "))
                    lines.append(("bold cyan", f"{label:<18s}"))
                    lines.append(("cyan", f"— {desc}"))
            else:
                if is_quit:
                    lines.append(("", "    "))
                    lines.append(("dim", label))
                else:
                    lines.append(("", "    "))
                    lines.append(("", f"{label:<18s}"))
                    lines.append(("gray", f"— {desc}"))
            lines.append(("", "\n"))

        return lines

    menu = Window(
        content=FormattedTextControl(_get_menu_text),
        dont_extend_height=True,
    )

    app: Application = Application(
        layout=Layout(HSplit([menu])),
        key_bindings=kb,
        full_screen=False,
    )

    # Save cursor, run, then restore and clear via /dev/tty
    _tty_write("\033[s")
    app.run()
    _tty_write("\033[u\033[J")

    return result[0]


def _enter_interactive(args) -> int:
    """Load config and launch the interactive session.

    When --benchmark is not specified, prompts the user to choose between
    MTR mode and Benchmark mode before entering the corresponding REPL.
    """
    from .interactive import BenchInteractiveSession, InteractiveSession

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
        print_error("No databases selected")
        flush_all()
        return 1

    output_dir = os.path.abspath(args.output_dir)

    # Clear terminal before entering interactive mode
    console.clear()

    # ----- mode selection (skip if --benchmark already set) -----------------
    force_bench = getattr(args, "benchmark", False)

    if force_bench:
        mode = "bench"
    else:
        mode = _select_mode(configs, args.database)
        if mode is None:
            # User cancelled
            console.print("\n  [bold cyan]Goodbye! 👋[/bold cyan]\n")
            return 0

    # ----- benchmark parameter configuration (interactive) ----------------
    # Only in interactive benchmark mode — prompt for iterations/warmup/profile
    bench_iterations = args.iterations
    bench_warmup = args.warmup
    bench_profile = getattr(args, 'profile', True)

    # ----- launch selected session -----------------------------------------
    while True:
        if mode == "playground":
            # Start server and open Playground page in browser
            from .interactive import ReportServer, _APIHandler
            from .whitelist import Whitelist
            from .buglist import Buglist

            whitelist = Whitelist(output_dir)
            buglist = Buglist(output_dir)

            srv = ReportServer(
                output_dir, port=args.port,
                whitelist=whitelist,
                buglist=buglist,
                configs=configs,
                all_configs=all_configs,
                database=args.database,
            )
            try:
                srv.start()
            except OSError as e:
                print_error(f"Failed to start server: {e}")
                flush_all()
                return 1

            pg_url = f"{srv.base_url}/playground.html"
            console.print(
                f"\n  [green]●[/green] Playground: "
                f"[bold link={pg_url}]{pg_url}[/bold link]")
            # Open in IDE browser
            try:
                import subprocess as _sp
                _sp.Popen(["code", "--open-url", pg_url],
                          stdout=_sp.DEVNULL, stderr=_sp.DEVNULL)
            except FileNotFoundError:
                pass

            from prompt_toolkit import HTML as _HTML
            from prompt_toolkit.history import InMemoryHistory as _IMH
            from prompt_toolkit import PromptSession as _PS
            from .interactive import _PROMPT_STYLE

            _pg_placeholder = _HTML(
                "<placeholder>Type 'help', 'back', or 'quit'"
                "</placeholder>")
            _pg_prompt = _HTML(
                '<prompt>rosetta</prompt> <path>▶</path> ')
            _pg_session = _PS(
                history=_IMH(),
                style=_PROMPT_STYLE,
                multiline=False,
            )

            console.print()
            # Wait for user command
            while True:
                try:
                    user_input = _pg_session.prompt(
                        _pg_prompt,
                        placeholder=_pg_placeholder,
                    ).strip()
                except (EOFError, KeyboardInterrupt):
                    srv.stop()
                    console.print(
                        "\n  [bold cyan]Goodbye! 👋[/bold cyan]\n")
                    return 0

                if not user_input:
                    continue

                cmd = user_input.lower()

                if cmd in ("back", "b"):
                    break
                elif cmd in ("quit", "exit", "q"):
                    srv.stop()
                    console.print(
                        "\n  [bold cyan]Goodbye! 👋[/bold cyan]\n")
                    return 0
                elif cmd == "help":
                    console.print(
                        "\n  [bold]Playground commands:[/bold]")
                    console.print(
                        f"  [green]open[/green]    "
                        f"re-open playground in browser")
                    console.print(
                        f"  [green]back[/green]    "
                        f"return to mode selection")
                    console.print(
                        f"  [green]quit[/green]    "
                        f"exit rosetta\n")
                elif cmd == "open":
                    try:
                        _sp.Popen(["code", "--open-url", pg_url],
                                  stdout=_sp.DEVNULL,
                                  stderr=_sp.DEVNULL)
                        console.print(
                            f"  [green]Opened:[/green] {pg_url}")
                    except FileNotFoundError:
                        console.print(
                            f"  [dim]URL:[/dim] {pg_url}")
                elif cmd:
                    console.print(
                        f"  [yellow]Unknown command:[/yellow] {cmd}")
                    console.print(
                        f"  [dim]Type 'help', 'back', "
                        f"or 'quit'.[/dim]")

            srv.stop()
            console.clear()
            mode = _select_mode(configs, args.database)
            if mode is None:
                console.print("\n  [bold cyan]Goodbye! 👋[/bold cyan]\n")
                return 0
            continue

        elif mode == "mtr":
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
            reason = session.run()
            # Stop the report server before leaving this session
            # so the port is released for the next session.
            if session._report_server:
                session._report_server.stop()
            if reason != "back":
                break
            console.clear()
            mode = _select_mode(configs, args.database)
            if mode is None:
                console.print("\n  [bold cyan]Goodbye! 👋[/bold cyan]\n")
                return 0
            continue
        else:
            # --- benchmark: mode → params → repl (loop params ↔ repl) ---
            back_to_mode = False
            # Initialize bench params from CLI args
            bench_mode = "serial" if args.concurrency == 0 else "concurrent"
            bench_concurrency = args.concurrency if args.concurrency > 0 else 8
            bench_duration = args.duration
            bench_ramp_up = args.ramp_up
            while True:
                if not force_bench:
                    params = _select_bench_params(
                        iterations=bench_iterations,
                        warmup=bench_warmup,
                        concurrency=bench_concurrency,
                        duration=bench_duration,
                        ramp_up=bench_ramp_up,
                        profile=bench_profile,
                        skip_setup=getattr(args, 'skip_setup', False),
                        skip_teardown=getattr(args, 'skip_teardown', False),
                    )
                    if params is None:
                        console.print(
                            "\n  [bold cyan]Goodbye! 👋[/bold cyan]\n")
                        return 0
                    if params.get("action") == "back":
                        # Back to mode selection
                        console.clear()
                        mode = _select_mode(configs, args.database)
                        if mode is None:
                            console.print(
                                "\n  [bold cyan]Goodbye! 👋[/bold cyan]\n")
                            return 0
                        back_to_mode = True
                        break  # exit inner loop
                    bench_mode = params["mode"]
                    bench_iterations = params["iterations"]
                    bench_warmup = params["warmup"]
                    bench_concurrency = params["concurrency"]
                    bench_duration = params["duration"]
                    bench_ramp_up = params["ramp_up"]
                    bench_profile = params["profile"]
                    bench_skip_setup = params.get("skip_setup", False)
                    bench_skip_teardown = params.get("skip_teardown", False)
                else:
                    bench_skip_setup = getattr(args, 'skip_setup', False)
                    bench_skip_teardown = getattr(args, 'skip_teardown', False)

                session = BenchInteractiveSession(
                    configs=configs,
                    output_dir=output_dir,
                    database=args.database,
                    iterations=bench_iterations,
                    warmup=bench_warmup,
                    concurrency=bench_concurrency if bench_mode == "concurrent" else 0,
                    duration=bench_duration,
                    ramp_up=bench_ramp_up,
                    bench_filter=args.bench_filter,
                    repeat=getattr(args, 'repeat', 1),
                    parallel_dbms=getattr(args, 'parallel_dbms', True),
                    output_format=args.format,
                    serve=args.serve,
                    port=args.port,
                    profile=bench_profile,
                    perf_freq=getattr(args, 'perf_freq', 99),
                    flamegraph_min_ms=getattr(args, 'flamegraph_min_ms', 1000),
                    bench_mode=bench_mode,
                )
                session.skip_setup = bench_skip_setup
                session.skip_teardown = bench_skip_teardown
                reason = session.run()
                # Stop the report server before leaving this session
                # so the port is released for the next session.
                if session._report_server:
                    session._report_server.stop()
                if reason == "quit":
                    return 0
                if reason != "back":
                    break
                # Back to bench params
                console.clear()
                continue  # re-show _select_bench_params

            if back_to_mode:
                continue  # re-evaluate mode in outer loop
            break  # done

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
