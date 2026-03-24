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

  # Benchmark: run 5 rounds automatically
  rosetta --benchmark --bench-file bench.json --repeat 5

  # Interactive mode: choose MTR or Benchmark at startup
  rosetta --interactive --dbms tdsql,mysql --iterations 100 --serve

  # Interactive mode: skip selection, go directly to benchmark
  rosetta --benchmark --interactive --dbms tdsql,mysql --iterations 100 --serve

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

    # Benchmark arguments
    bench = p.add_argument_group("Benchmark",
                                 "Cross-DBMS performance comparison")
    bench.add_argument("--benchmark", action="store_true",
                       help="Run benchmark mode instead of "
                            "consistency test")
    bench.add_argument("--bench-file",
                       help="Path to benchmark definition file "
                            "(.json or .sql)")
    bench.add_argument("--template",
                       help="Use a built-in benchmark template "
                            "(e.g. oltp_read_write, oltp_read_only, "
                            "oltp_write_only)")
    bench.add_argument("--list-templates", action="store_true",
                       help="List available built-in benchmark "
                            "templates and exit")
    bench.add_argument("--iterations", type=int, default=100,
                       help="Number of iterations per query in "
                            "serial mode (default: 100)")
    bench.add_argument("--warmup", type=int, default=5,
                       help="Number of warmup iterations per query "
                            "(default: 5)")
    bench.add_argument("--concurrency", type=int, default=0,
                       help="Concurrent threads (0 = serial mode, "
                            ">0 = concurrent mode)")
    bench.add_argument("--duration", type=float, default=30.0,
                       help="Duration in seconds for concurrent "
                            "mode (default: 30)")
    bench.add_argument("--ramp-up", type=float, default=0.0,
                       help="Ramp-up time in seconds for concurrent "
                            "mode (default: 0)")
    bench.add_argument("--bench-filter",
                       help="Only run queries matching these names "
                            "(comma-separated)")
    bench.add_argument("--repeat", type=int, default=1,
                       help="Number of benchmark rounds to run "
                            "(default: 1). Each round produces its own "
                            "timestamped report.")
    bench.add_argument("--no-parallel-dbms", dest="parallel_dbms",
                       action="store_false",
                       help="Run benchmarks on DBMS targets sequentially "
                            "instead of in parallel")
    bench.set_defaults(parallel_dbms=True)
    bench.add_argument("--profile", action="store_true", dest="profile",
                       default=True,
                       help="Enable CPU flame graph capture via perf "
                            "for each query during benchmark execution "
                            "(default: on)")
    bench.add_argument("--no-profile", action="store_false", dest="profile",
                       help="Disable CPU flame graph capture")
    bench.add_argument("--perf-freq", type=int, default=99,
                       help="perf sampling frequency in Hz "
                            "(default: 99)")

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
    try:
        if args.bench_file:
            workload = BenchmarkLoader.from_file(args.bench_file)
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
        if mode == WorkloadMode.SERIAL:
            per_query = bench_cfg.iterations + bench_cfg.warmup
        else:
            per_query = 100

        if parallel_dbms and len(configs) > 1:
            for c in configs:
                bp = BenchProgress(c.name, n_queries, per_query)
                bp.__enter__()
                progress_bars[c.name] = bp

        def on_dbms_start(dbms_name):
            with _progress_lock:
                if dbms_name not in progress_bars:
                    bp = BenchProgress(dbms_name, n_queries, per_query)
                    bp.__enter__()
                    progress_bars[dbms_name] = bp

        def on_progress(dbms_name, query_name, iteration, total,
                        is_warmup=False):
            bp = progress_bars.get(dbms_name)
            if bp:
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

        result = run_benchmark(
            configs=configs,
            workload=workload,
            bench_cfg=bench_cfg,
            database=args.database,
            on_progress=on_progress,
            on_dbms_start=on_dbms_start,
            on_dbms_done=on_dbms_done,
            on_profile_start=on_profile_start if bench_cfg.profile else None,
            on_profile_done=on_profile_done if bench_cfg.profile else None,
            parallel_dbms=parallel_dbms,
        )

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


def _select_bench_params(
    iterations: int = 100,
    warmup: int = 5,
    profile: bool = True,
) -> Optional[dict]:
    """Show an interactive benchmark parameter configuration panel.

    Uses arrow-key navigation:  Up/Down to move between fields,
    Left/Right to change values.  Enter to confirm, Esc to cancel.

    Returns a dict with ``iterations``, ``warmup``, ``profile`` keys,
    or ``None`` if the user cancels.
    """
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

    # Custom value state: None means "use preset", otherwise an int
    custom_iter = [None]
    custom_warmup = [None]

    # Current state (mutable)
    result = [None]
    sel = [0]  # selected field index
    it_idx = [ITER_PRESETS.index(iterations)
              if iterations in ITER_PRESETS else 2]
    wa_idx = [WARMUP_PRESETS.index(warmup)
              if warmup in WARMUP_PRESETS else 1]
    prof = [profile]

    FIELDS = [
        {"label": "Iterations", "type": "choice"},
        {"label": "Warmup", "type": "choice"},
        {"label": "Profile (flame graph)", "type": "toggle"},
        {"label": "OK",    "type": "action"},
        {"label": "Back",  "type": "action"},
        {"label": "Quit",  "type": "action"},
    ]

    ACTION_OK = len(FIELDS) - 3
    ACTION_BACK = len(FIELDS) - 2  # index of "Back"
    ACTION_QUIT = len(FIELDS) - 1  # index of "Quit"

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
        else:
            return PROFILE_LABELS[prof[0]]

    def _toggle_right(i):
        if i == 0:
            if custom_iter[0] is not None:
                custom_iter[0] = None  # cycle out of custom → back to presets
            else:
                if it_idx[0] == len(ITER_PRESETS) - 1:
                    # At last preset, wrap to "Custom…"
                    custom_iter[0] = _iter_val()  # seed with current value
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
            prof[0] = not prof[0]

    def _toggle_left(i):
        if i == 0:
            if custom_iter[0] is not None:
                custom_iter[0] = None  # cycle out of custom → back to last preset
            else:
                if it_idx[0] == 0:
                    custom_iter[0] = _iter_val()
                    it_idx[0] = 0  # stay at first preset when returning
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
            prof[0] = not prof[0]

    # Inline editing state for custom values
    editing = [None]   # None or field index (0=Iterations, 1=Warmup)
    edit_buf = [""]     # current text being typed

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

    # Accept digit input only while in edit mode
    @kb.add(Keys.Any, filter=Condition(lambda: editing[0] is not None))
    def _type_char(event):
        ch = event.data
        if ch.isdigit():
            edit_buf[0] += ch

    @kb.add("enter")
    def _confirm(event):
        # --- currently in inline edit mode — confirm the value ---
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
                    pass  # keep old value on invalid input
            editing[0] = None
            edit_buf[0] = ""
            return

        # --- on a custom field → enter edit mode ---
        if sel[0] == 0 and custom_iter[0] is not None:
            editing[0] = 0
            edit_buf[0] = str(custom_iter[0])
            return
        if sel[0] == 1 and custom_warmup[0] is not None:
            editing[0] = 1
            edit_buf[0] = str(custom_warmup[0])
            return

        # --- handle OK / Back / Quit actions ---
        if sel[0] == ACTION_OK:
            result[0] = {
                "iterations": _iter_val(),
                "warmup": _warmup_val(),
                "profile": prof[0],
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
        title = "Benchmark Configuration".center(55)
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

        # If in edit mode, show only the editing field
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
                # Render as a simple highlighted label
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
            else:  # toggle
                if prof[0]:
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

    # Save cursor, run, then restore and clear via /dev/tty
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
        ("mtr",   "MTR mode",       "run .test compatibility tests"),
        ("bench", "Benchmark mode",  "run .json/.sql performance benchmarks"),
        (None,    "Quit",            "exit"),
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
        if mode == "mtr":
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
            while True:
                if not force_bench:
                    params = _select_bench_params(
                        iterations=bench_iterations,
                        warmup=bench_warmup,
                        profile=bench_profile,
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
                    bench_iterations = params["iterations"]
                    bench_warmup = params["warmup"]
                    bench_profile = params["profile"]

                session = BenchInteractiveSession(
                    configs=configs,
                    output_dir=output_dir,
                    database=args.database,
                    iterations=bench_iterations,
                    warmup=bench_warmup,
                    concurrency=args.concurrency,
                    duration=args.duration,
                    ramp_up=args.ramp_up,
                    bench_filter=args.bench_filter,
                    repeat=getattr(args, 'repeat', 1),
                    parallel_dbms=getattr(args, 'parallel_dbms', True),
                    output_format=args.format,
                    serve=args.serve,
                    port=args.port,
                    profile=bench_profile,
                    perf_freq=getattr(args, 'perf_freq', 99),
                )
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
