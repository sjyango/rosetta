"""
Main CLI entry point with subcommand structure.

This module provides a modern CLI architecture that is friendly to both
AI Agents (JSON output via -j/--json) and humans (default output).
"""

import argparse
import logging
import sys
from typing import List, Optional

from .. import __version__
from ..paths import CONFIG_FILE, RESULTS_DIR
from .output import OutputFormatter
from .result import CommandResult


def _add_global_options(parser: argparse.ArgumentParser) -> None:
    """Add global options (-j/--json, -c/--config, --verbose) to a parser.
    
    Called on every subcommand parser so that flags like ``--json`` can appear
    after the subcommand name (e.g. ``rosetta status --json``).
    """
    parser.add_argument(
        "-j", "--json",
        action="store_true",
        default=False,
        help="JSON output (AI Agent friendly)",
    )
    parser.add_argument(
        "--config", "-c",
        default=None,
        help=f"Path to DBMS config JSON (default: {CONFIG_FILE})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable verbose / debug logging",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        default=False,
        help="Show rosetta version and exit",
    )


# A lightweight parser that only knows the global flags.
# Used for a first pass so that ``rosetta -j status`` works
# (the main parser sees -j *before* the subcommand name).
_global_preparser = argparse.ArgumentParser(add_help=False)
_global_preparser.add_argument("-j", "--json", action="store_true", default=False)
_global_preparser.add_argument("--config", "-c", default=None)
_global_preparser.add_argument("--verbose", action="store_true", default=False)
_global_preparser.add_argument("-V", "--version", action="store_true", default=False)


def create_parser() -> argparse.ArgumentParser:
    """
    Create the main argument parser with subcommands.
    
    Returns:
        argparse.ArgumentParser: The configured parser
    """
    parser = argparse.ArgumentParser(
        prog="rosetta",
        allow_abbrev=False,
        description=(
            "Rosetta — Cross-DBMS SQL testing & benchmarking toolkit.\n\n"
            "Human-readable output by default.\n"
            "Use -j/--json for JSON output (AI Agent friendly)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # Inherit global flags so that ``rosetta -j status`` works
        parents=[_global_preparser],
    )
    
    # Create subparsers
    subparsers = parser.add_subparsers(
        dest="command",
        title="commands",
        description="Available subcommands",
    )
    
    # Add subcommands
    _add_init_subparser(subparsers)
    _add_test_subparser(subparsers)
    _add_mtr_subparser(subparsers)
    _add_bench_subparser(subparsers)
    _add_status_subparser(subparsers)
    _add_exec_subparser(subparsers)
    _add_config_subparser(subparsers)
    _add_result_subparser(subparsers)
    _add_interactive_subparser(subparsers)
    
    return parser


# ---------------------------------------------------------------------------
# Shared argument helpers
# ---------------------------------------------------------------------------

def _add_test_arguments(parser):
    """Add cross-DBMS consistency test arguments to a parser."""
    parser.add_argument(
        "-t", "--test",
        required=True,
        help="Path to .test file",
    )
    parser.add_argument(
        "--result",
        action="store_true",
        default=False,
        help="Use .result file instead of .test (MTR variables pre-expanded)",
    )
    parser.add_argument(
        "--dbms",
        required=True,
        help="DBMS targets, comma-separated (e.g. tdsql,mysql,tidb)",
    )
    parser.add_argument(
        "--database", "-d",
        default="rosetta_mtr_test",
        help="Test database name (default: rosetta_mtr_test)",
    )
    parser.add_argument(
        "--baseline", "-b",
        default="tdsql",
        help="Baseline DBMS name for diff (default: tdsql)",
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=RESULTS_DIR,
        help="Output directory for reports (default: ~/.rosetta/results)",
    )
    parser.add_argument(
        "--output-format", "-f",
        default="all",
        choices=["text", "html", "all"],
        help="Report format (default: all)",
    )
    parser.add_argument(
        "--skip-explain",
        action="store_true",
        default=True,
        help="Skip EXPLAIN statements (default: on)",
    )
    parser.add_argument(
        "--skip-analyze",
        action="store_true",
        help="Skip ANALYZE TABLE statements",
    )
    parser.add_argument(
        "--skip-show-create",
        action="store_true",
        help="Skip SHOW CREATE TABLE statements",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Only parse .test file and print statements (no execution)",
    )
    parser.add_argument(
        "--diff-only",
        action="store_true",
        help="Re-generate reports from existing .result files (no DB execution)",
    )
    parser.add_argument(
        "--serve", "-s",
        action="store_true",
        help="Start a local HTTP server to view HTML reports",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=19527,
        help="HTTP server port (default: 19527)",
    )


def _add_mtr_arguments(parser):
    """Add native MTR runner arguments to a parser."""
    parser.add_argument(
        "--test-dir",
        default=None,
        help="Path to MySQL test directory containing ./mtr (default: auto-detect)",
    )
    parser.add_argument(
        "--skip-list",
        default=None,
        help="Path to skip-test list file (default: auto-detect)",
    )
    parser.add_argument(
        "-t", "--total",
        action="store_true",
        default=False,
        help="Total MTR mode (use port-base=30000)",
    )
    parser.add_argument(
        "-o", "--optimistic",
        action="store_true",
        default=False,
        help="Enable optimistic transaction mode (--mysqld=--tdsql_trans_type=1)",
    )
    parser.add_argument(
        "-v", "--vector",
        action="store_true",
        default=False,
        help="Enable vector engine mode (--ve-protocol)",
    )
    parser.add_argument(
        "-pq", "--parallel-query",
        action="store_true",
        default=False,
        help="Enable parallel query mode (--parallel-query)",
    )
    parser.add_argument(
        "-m", "--mode",
        type=str,
        default=None,
        help="Run multiple MTR modes in parallel. "
             "Comma-separated list of: row (行存), col (列存/ve-protocol), pq (并行查询). "
             "Example: --mode row,col,pq. "
             "When specified, --vector and --parallel-query flags are ignored.",
    )
    parser.add_argument(
        "-r", "--record",
        action="store_true",
        default=False,
        help="Enable record mode (--record)",
    )
    parser.add_argument(
        "-s", "--suite",
        type=str,
        default=None,
        help="Test suite name (e.g. main, innodb)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=8,
        help="Number of parallel workers (default: 8)",
    )
    parser.add_argument(
        "--retry",
        type=int,
        default=3,
        help="Number of retries for failed tests (default: 3)",
    )
    parser.add_argument(
        "--retry-failure",
        type=int,
        default=3,
        help="Number of retries for failure (default: 3)",
    )
    parser.add_argument(
        "--max-test-fail",
        type=int,
        default=3000,
        help="Maximum test failures before stopping (default: 3000)",
    )
    parser.add_argument(
        "--testcase-timeout",
        type=int,
        default=1200,
        help="Test case timeout in seconds (default: 1200)",
    )
    parser.add_argument(
        "--suite-timeout",
        type=int,
        default=600,
        help="Suite timeout in seconds (default: 600)",
    )
    parser.add_argument(
        "cases",
        nargs="*",
        help="Specific test cases to run",
    )
    parser.add_argument(
        "--gcov",
        action="store_true",
        default=False,
        help="Enable gcov coverage collection (report after MTR run)",
    )
    parser.add_argument(
        "--gcov-clean",
        action="store_true",
        default=False,
        help="Clean gcov counters before running (reset to zero; default: accumulate)",
    )
    parser.add_argument(
        "--gcov-filter",
        type=str,
        default="auto",
        help="Source filter for coverage: 'auto' (default, only test-touched files), "
             "'all' (full project), or a glob pattern (e.g. '*/ha_rocksdb.cc')",
    )


def _add_bench_arguments(parser):
    """Add benchmark-specific arguments to a parser."""
    parser.add_argument(
        "--dbms",
        required=True,
        help="DBMS targets, comma-separated (e.g. tdsql,mysql)",
    )
    parser.add_argument(
        "--file",
        dest="bench_file",
        help="Benchmark definition file (.json or .sql)",
    )
    parser.add_argument(
        "--template",
        help="Use a built-in template (e.g. oltp_read_write, oltp_read_only)",
    )
    parser.add_argument(
        "--mode",
        choices=["SERIAL", "CONCURRENT"],
        default="SERIAL",
        help="Execution mode: SERIAL or CONCURRENT (default: SERIAL)",
    )
    parser.add_argument(
        "--database", "-d",
        default="rosetta_bench_test",
        help="Benchmark database name (default: rosetta_bench_test)",
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=RESULTS_DIR,
        help="Output directory for reports (default: ~/.rosetta/results)",
    )
    parser.add_argument(
        "--output-format", "-f",
        default="all",
        choices=["text", "html", "all"],
        help="Report format (default: all)",
    )
    # Serial mode options
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of iterations per query — serial mode (default: 1)",
    )
    # Concurrent mode options
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Number of concurrent threads — concurrent mode (default: 10)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Duration in seconds — concurrent mode (default: 30)",
    )
    # Shared options
    parser.add_argument(
        "--warmup",
        type=int,
        default=0,
        help="Warmup iterations (serial) or warmup duration in seconds (concurrent) (default: 0)",
    )
    parser.add_argument(
        "--ramp-up",
        type=float,
        default=0.0,
        help="Ramp-up seconds — concurrent mode (default: 0)",
    )
    parser.add_argument(
        "--query-timeout",
        type=int,
        default=5,
        help="Query timeout in seconds (default: 5, 0 to disable)",
    )
    parser.add_argument(
        "--bench-filter",
        help="Run only queries matching these names (comma-separated)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of benchmark rounds (default: 1)",
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        default=False,
        help="Skip setup phase (reuse existing tables)",
    )
    parser.add_argument(
        "--skip-teardown",
        action="store_true",
        default=False,
        help="Skip teardown (keep tables for next run)",
    )
    parser.add_argument(
        "--no-parallel-dbms",
        dest="parallel_dbms",
        action="store_false",
        help="Run DBMS targets sequentially instead of in parallel",
    )
    parser.set_defaults(parallel_dbms=True)
    parser.add_argument(
        "--profile",
        action="store_true",
        default=True,
        help="Enable flame-graph capture (default: on)",
    )
    parser.add_argument(
        "--no-profile",
        action="store_false",
        dest="profile",
        help="Disable flame-graph capture",
    )
    parser.add_argument(
        "--perf-freq",
        type=int,
        default=99,
        help="perf sampling frequency in Hz (default: 99)",
    )


# ---------------------------------------------------------------------------
# Subparser registration
# ---------------------------------------------------------------------------

def _add_test_subparser(subparsers):
    """Add the 'test' top-level subcommand (cross-DBMS consistency test)."""
    test_parser = subparsers.add_parser(
        "test",
        help="Run cross-DBMS consistency test",
        description="Execute .test files and compare SQL results across databases",
    )
    _add_global_options(test_parser)
    _add_test_arguments(test_parser)


def _add_mtr_subparser(subparsers):
    """Add the 'mtr' top-level subcommand (native MySQL MTR runner)."""
    mtr_parser = subparsers.add_parser(
        "mtr",
        help="Run native MySQL MTR test suite",
        description="Execute MySQL MTR test suites using the native ./mtr binary",
    )
    _add_global_options(mtr_parser)
    _add_mtr_arguments(mtr_parser)


def _add_bench_subparser(subparsers):
    """Add the 'bench' top-level subcommand."""
    bench_parser = subparsers.add_parser(
        "bench",
        help="Run performance benchmark",
        description="Compare query performance across databases with custom workloads",
    )
    _add_global_options(bench_parser)
    _add_bench_arguments(bench_parser)


def _add_list_subparser(subparsers):
    """Add the 'list' subcommand."""
    list_parser = subparsers.add_parser(
        "list",
        help="List resources (configs, history, templates)",
        description="List databases, execution history, or benchmark templates",
    )
    list_parser.add_argument(
        "resource",
        nargs="?",
        default="dbms",
        choices=["dbms", "history", "templates"],
        help="Resource to list: dbms (databases), history (runs), templates (benchmarks) (default: dbms)",
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of items to show (default: 20)",
    )


def _add_status_subparser(subparsers):
    """Add the 'status' subcommand."""
    status_parser = subparsers.add_parser(
        "status",
        help="Check DBMS connection status",
        description="Check connection status for all configured databases",
    )
    _add_global_options(status_parser)
    status_parser.add_argument(
        "--timeout",
        type=int,
        default=5,
        help="Connection timeout in seconds (default: 5)",
    )


def _add_exec_subparser(subparsers):
    """Add the 'exec' subcommand."""
    exec_parser = subparsers.add_parser(
        "exec",
        help="Execute SQL statements",
        description="Execute SQL statements on specified databases (CLI playground)",
    )
    _add_global_options(exec_parser)
    exec_parser.add_argument(
        "--sql",
        help="SQL statement to execute",
    )
    exec_parser.add_argument(
        "--file",
        help="File containing SQL statements",
    )
    exec_parser.add_argument(
        "--dbms",
        help="DBMS targets, comma-separated (default: all from config)",
    )
    exec_parser.add_argument(
        "--database", "-d",
        help="Database name (default: from config)",
    )


def _add_init_subparser(subparsers):
    """Add the 'init' top-level subcommand (shortcut for config init)."""
    init_parser = subparsers.add_parser(
        "init",
        help="Initialize ~/.rosetta directory and generate config",
        description="Create ~/.rosetta/ directory structure and generate a sample config.json",
    )
    _add_global_options(init_parser)
    init_parser.add_argument(
        "--output",
        help="Output file path (default: ~/.rosetta/config.json)",
    )


def _add_config_subparser(subparsers):
    """Add the 'config' subcommand."""
    config_parser = subparsers.add_parser(
        "config",
        help="Manage configurations",
        description="View, validate, or generate configuration files",
    )
    _add_global_options(config_parser)
    config_parser.add_argument(
        "action",
        choices=["show", "validate", "init"],
        help="Action: show (display config), validate (check config), init (generate sample)",
    )
    config_parser.add_argument(
        "--output",
        help="Output file path (for init action)",
    )


def _add_result_subparser(subparsers):
    """Add the 'result' subcommand with sub-actions via subparsers."""
    result_parser = subparsers.add_parser(
        "result",
        help="Manage execution results",
        description="Browse, view, and export historical execution results",
    )
    _add_global_options(result_parser)

    result_sub = result_parser.add_subparsers(dest="result_action")

    # result list  (also the default when no action given)
    list_p = result_sub.add_parser(
        "list", help="List historical runs",
        description="Show a table of past MTR / bench runs",
    )
    _add_global_options(list_p)
    list_p.add_argument(
        "-n", "--limit", type=int, default=20,
        help="Max rows per page (default: 20)",
    )
    list_p.add_argument(
        "-p", "--page", type=int, default=1,
        help="Page number (default: 1)",
    )
    list_p.add_argument(
        "--type", choices=["all", "mtr", "test", "bench"], default="all",
        help="Filter by run type (default: all)",
    )
    list_p.add_argument(
        "--output-dir", "-o", default=RESULTS_DIR,
        help="Results directory (default: ~/.rosetta/results)",
    )

    # result show <run_id>
    show_p = result_sub.add_parser(
        "show", help="Show details of a run",
        description="Display detailed information for a specific run",
    )
    _add_global_options(show_p)
    show_p.add_argument(
        "run_id", nargs="?", default=None,
        help="Run ID or path (default: latest)",
    )
    show_p.add_argument(
        "--output-dir", "-o", default=RESULTS_DIR,
        help="Results directory (default: ~/.rosetta/results)",
    )


def _add_interactive_subparser(subparsers):
    """Add the 'interactive' subcommand (with aliases 'repl' and 'i')."""
    for name in ["interactive", "repl", "i"]:
        interp_parser = subparsers.add_parser(
            name,
            help="Launch interactive REPL" + (" (alias)" if name != "interactive" else ""),
            description="Start an interactive session for repeated test execution",
        )
        _add_global_options(interp_parser)
        interp_parser.add_argument(
            "--dbms",
            help="DBMS targets, comma-separated (default: auto-detect reachable DBMS)",
        )
        interp_parser.add_argument(
            "--database", "-d",
            default="cross_dbms_test_db",
            help="Test database name (default: cross_dbms_test_db)",
        )
        interp_parser.add_argument(
            "--output-dir", "-o",
            default=RESULTS_DIR,
            help="Output directory for reports (default: ~/.rosetta/results)",
        )
        interp_parser.add_argument(
            "--serve", "-s",
            action="store_true",
            default=True,
            help="Start a local HTTP server to view HTML reports (default: on)",
        )
        interp_parser.add_argument(
            "--no-serve",
            action="store_false",
            dest="serve",
            help="Do not start HTTP server",
        )
        interp_parser.add_argument(
            "--port", "-p",
            type=int,
            default=19527,
            help="HTTP server port (default: 19527)",
        )


def main(argv: Optional[List[str]] = None) -> int:
    """
    Main entry point for the rosetta CLI.
    
    Args:
        argv: Command-line arguments (default: sys.argv[1:])
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    if argv is None:
        argv = sys.argv[1:]
    
    parser = create_parser()
    
    # Two-phase parse: first extract global flags that may appear before
    # the subcommand (e.g. ``rosetta -j status``), then let the full
    # parser handle everything.  The subcommand parsers also accept
    # the same flags, so ``rosetta status -j`` works too.
    pre_args, _ = _global_preparser.parse_known_args(argv)
    args = parser.parse_args(argv)
    
    # Merge: if the flag was set in *either* position, honour it.
    args.json = args.json or pre_args.json
    args.verbose = args.verbose or pre_args.verbose
    args.version = args.version or pre_args.version
    if args.config is None:
        args.config = pre_args.config if pre_args.config is not None else CONFIG_FILE

    # Derive output format from -j/--json flag
    fmt = "json" if args.json else "human"
    output = OutputFormatter(format=fmt)

    # Compatibility: keep `-v` as verbose for subcommands, but when used
    # alone at top level treat it as a convenient version shortcut.
    if not args.command and args.verbose and not args.version:
        args.version = True

    if args.version:
        if args.json:
            output.print(CommandResult.success(
                "version",
                {"name": "rosetta", "version": __version__},
            ))
        else:
            print(f"rosetta {__version__}")
        return 0
    
    # Configure logging - only show ERROR to console by default
    log_level = logging.DEBUG if args.verbose else logging.ERROR
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    # No command provided — default to interactive mode
    if not args.command:
        from .interactive_cmd import handle_interactive
        # Set default values for interactive mode
        args.dbms = getattr(args, 'dbms', None)
        args.database = getattr(args, 'database', 'cross_dbms_test_db')
        args.output_dir = getattr(args, 'output_dir', RESULTS_DIR)
        args.serve = getattr(args, 'serve', True)
        args.port = getattr(args, 'port', 19527)
        result = handle_interactive(args, output)
        output.print(result)
        return result.exit_code()
    
    # Dispatch to command handlers
    try:
        if args.command == "init":
            from .config_cmd import handle_config
            args.action = "init"  # Map to config init action
            result = handle_config(args, output)
        elif args.command == "test":
            from .run import handle_test
            result = handle_test(args, output)
        elif args.command == "mtr":
            from .mtr_cmd import handle_mtr
            result = handle_mtr(args, output)
        elif args.command == "bench":
            from .run import handle_bench
            result = handle_bench(args, output)
        elif args.command == "status":
            from .status import handle_status
            result = handle_status(args, output)
        elif args.command == "exec":
            from .exec import handle_exec
            result = handle_exec(args, output)
        elif args.command == "config":
            from .config_cmd import handle_config
            result = handle_config(args, output)
        elif args.command == "result":
            from .result_cmd import handle_result
            result = handle_result(args, output)
        elif args.command in ["interactive", "repl", "i"]:
            from .interactive_cmd import handle_interactive
            result = handle_interactive(args, output)
        else:
            result = CommandResult.failure(
                f"Unknown command: {args.command}",
            )
        
        # Print result
        output.print(result)
        return result.exit_code()
    
    except Exception as e:
        error_result = CommandResult.failure(
            f"Unexpected error: {str(e)}",
        )
        output.print(error_result)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    finally:
        # Always restore cursor visibility
        try:
            from rich.console import Console
            Console().show_cursor()
        except Exception:
            pass
