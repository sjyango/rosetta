"""
Handlers for 'mtr' and 'bench' commands.
"""

import os
import time as _time
from pathlib import Path
from typing import TYPE_CHECKING

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter


def handle_test(args, output: "OutputFormatter") -> CommandResult:
    """Handle the 'test' command — execute MTR consistency tests across databases."""
    return _handle_run_mtr(args, output)


def handle_bench(args, output: "OutputFormatter") -> CommandResult:
    """Handle the 'bench' command — execute performance benchmarks."""
    return _handle_run_bench(args, output)


def _handle_run_mtr(args, output: "OutputFormatter") -> CommandResult:
    """
    Handle 'run mtr' subcommand - execute MTR consistency tests.
    
    Reuses RosettaRunner from cli.py which has progress bars and logging.
    
    Args:
        args: Parsed arguments
        output: Output formatter
    
    Returns:
        CommandResult with test results
    """
    from ..config import load_config, filter_configs, DEFAULT_TEST_DB
    from ..runner import RosettaRunner
    import logging
    
    # Load config
    if not os.path.isfile(args.config):
        return CommandResult.failure(
            f"Config file not found: {args.config}\n"
            f"Run 'rosetta config init' to create a sample config, "
            f"or use '-c' to specify the config file path.",
        )
    
    all_configs = load_config(args.config)
    if not all_configs:
        return CommandResult.failure(
            f"No databases configured in {args.config}",
        )
    
    try:
        configs = filter_configs(all_configs, args.dbms)
    except ValueError as e:
        return CommandResult.failure(str(e))
    
    if not configs:
        return CommandResult.failure("No databases selected for testing")
    
    if len(configs) < 2:
        return CommandResult.failure(
            "At least 2 DBMS targets are required for cross-DBMS comparison. "
            f"Got: {', '.join(c.name for c in configs)}. "
            "Use --dbms to specify multiple targets (e.g. --dbms tdsql,mysql).",
        )
    
    # Check test file
    if not os.path.isfile(args.file):
        return CommandResult.failure(
            f"Test file not found: {args.file}",
        )
    
    # Parse-only mode
    if args.parse_only:
        try:
            from ..mtr import MtrParser
            mtr_parser = MtrParser(args.file)
            test = mtr_parser.parse()
            cmd_types = set(c.cmd_type.name for c in test.commands)
            return CommandResult.success(
                "mtr parse-only",
                {
                    "test_file": args.file,
                    "total_commands": len(test.commands),
                    "command_types": sorted(cmd_types),
                    "commands": [
                        {
                            "line_no": c.line_no,
                            "type": c.cmd_type.name,
                            "argument": (c.argument or "")[:200],
                        }
                        for c in test.commands[:50]  # Limit output
                    ],
                },
            )
        except Exception as e:
            return CommandResult.failure(f"Parse error: {str(e)}")
    
    # Create output directory
    output_dir = os.path.abspath(args.output_dir)
    run_stamp = _time.strftime("%Y%m%d_%H%M%S")
    test_name = Path(args.file).stem
    run_dir = os.path.join(output_dir, f"{test_name}_{run_stamp}")
    os.makedirs(run_dir, exist_ok=True)
    
    # Setup file logging before RosettaRunner
    log = logging.getLogger("rosetta")
    
    # Remove all existing handlers from rosetta logger
    for handler in log.handlers[:]:
        log.removeHandler(handler)
    
    # Also clear root logger's handlers (set by basicConfig in main.py)
    logging.root.handlers.clear()
    
    # Add file handler only
    file_handler = logging.FileHandler(
        os.path.join(run_dir, "rosetta.log"),
        mode="w",
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    log.addHandler(file_handler)
    log.setLevel(logging.DEBUG)  # Ensure all levels are captured
    
    # Use RosettaRunner for MTR execution (has progress bars)
    runner = RosettaRunner(
        test_file=args.file,
        configs=configs,
        output_dir=run_dir,
        database=args.database,
        baseline=args.baseline,
        skip_explain=args.skip_explain,
        skip_analyze=args.skip_analyze,
        skip_show_create=args.skip_show_create,
        output_format=args.output_format,
    )
    
    comparisons = runner.run()
    
    # Generate history index
    from ..reporter.history import generate_index_html
    generate_index_html(output_dir)
    
    # Update latest symlink
    latest_link = os.path.join(output_dir, "latest")
    try:
        if os.path.islink(latest_link):
            os.remove(latest_link)
        os.symlink(os.path.basename(run_dir), latest_link)
    except OSError:
        pass
    
    # Prepare result data
    comparison_data = {}
    for key, cmp in comparisons.items():
        comparison_data[key] = {
            "total_statements": cmp.total_stmts,
            "matched": cmp.matched,
            "mismatched": cmp.mismatched,
            "effective_mismatched": cmp.effective_mismatched,
            "pass_rate": round(cmp.pass_rate, 2),
        }
    
    # Get report files
    report_files = []
    for f in os.listdir(run_dir):
        if f.endswith(('.report.txt', '.html')):
            report_files.append(os.path.join(run_dir, f))
    
    # Handle --serve: start HTTP server and block
    if getattr(args, 'serve', False):
        from ..runner import _serve_report
        # Find the first HTML report file
        html_files = [f for f in report_files if f.endswith('.html')]
        if html_files:
            html_file = html_files[0]
        else:
            # Fallback: use test name
            test_name = Path(args.file).stem
            html_file = f"{test_name}.html"
        
        port = getattr(args, 'port', 19527)
        _serve_report(run_dir, html_file, port=port)
        # _serve_report blocks until KeyboardInterrupt, so we won't reach here
    
    return CommandResult.success(
        "mtr",
        {
            "test_file": args.file,
            "dbms_targets": [c.name for c in configs],
            "database": args.database,
            "baseline": args.baseline,
            "comparisons": comparison_data,
            "failed_connections": list(runner.failed_connections),
            "report_directory": run_dir,
            "report_files": sorted(report_files),
        },
    )


def _handle_run_bench(args, output: "OutputFormatter") -> CommandResult:
    """
    Handle 'bench' command - execute performance benchmarks with progress bars.

    Reuses the full progress-bar + reporting pipeline from runner._run_benchmark.

    Args:
        args: Parsed arguments
        output: Output formatter

    Returns:
        CommandResult with benchmark results
    """
    import json
    import logging
    import threading

    from rosetta.config import load_config, filter_configs
    from rosetta.benchmark import BenchmarkLoader, run_benchmark
    from rosetta.models import BenchmarkConfig, WorkloadMode
    from rosetta.reporter.bench_text import write_bench_text_report
    from rosetta.reporter.bench_html import write_bench_html_report
    from rosetta.reporter.history import generate_index_html
    from rosetta.runner import _save_bench_json
    from rosetta.ui import (BenchProgress, console, flush_all,
                            print_bench_summary, print_info, print_phase,
                            print_report_file)

    # Load config
    if not os.path.isfile(args.config):
        return CommandResult.failure(
            f"Config file not found: {args.config}\n"
            f"Run 'rosetta config init' to create a sample config, "
            f"or use '-c' to specify the config file path.",
        )

    all_configs = load_config(args.config)
    if not all_configs:
        return CommandResult.failure(
            f"No databases configured in {args.config}",
        )

    try:
        configs = filter_configs(all_configs, args.dbms)
    except ValueError as e:
        return CommandResult.failure(str(e))

    if not configs:
        return CommandResult.failure("No databases selected for benchmark")

    # Load workload & extra JSON config
    json_extra_config = {}
    if not args.bench_file:
        return CommandResult.failure(
            "Missing --file. Specify a benchmark definition file (.json or .sql).",
            command="bench",
        )
    try:
        workload = BenchmarkLoader.from_file(args.bench_file)
        if args.bench_file.endswith('.json'):
            with open(args.bench_file, 'r') as f:
                json_data = json.load(f)
                json_extra_config = {
                    'database': json_data.get('database'),
                    'skip_setup': json_data.get('skip_setup'),
                    'skip_teardown': json_data.get('skip_teardown'),
                }
    except (FileNotFoundError, ValueError) as e:
        return CommandResult.failure(str(e))

    # Determine mode from --mode argument
    bench_mode = getattr(args, "mode", "SERIAL")
    if bench_mode == "CONCURRENT":
        mode = WorkloadMode.CONCURRENT
        concurrency = args.concurrency if args.concurrency > 0 else 10
    else:
        mode = WorkloadMode.SERIAL
        concurrency = 0

    filter_queries = []
    if args.bench_filter:
        filter_queries = [
            n.strip() for n in args.bench_filter.split(",") if n.strip()
        ]

    # Determine skip_setup / skip_teardown: JSON overrides CLI defaults
    json_skip_setup = json_extra_config.get('skip_setup')
    json_skip_teardown = json_extra_config.get('skip_teardown')
    cli_skip_setup = getattr(args, 'skip_setup', False)
    cli_skip_teardown = getattr(args, 'skip_teardown', False)
    final_skip_setup = cli_skip_setup if cli_skip_setup else (json_skip_setup if json_skip_setup is not None else False)
    final_skip_teardown = cli_skip_teardown if cli_skip_teardown else (json_skip_teardown if json_skip_teardown is not None else False)

    bench_cfg = BenchmarkConfig(
        mode=mode,
        iterations=args.iterations,
        warmup=args.warmup,
        concurrency=concurrency if concurrency > 0 else 1,
        duration=args.duration,
        ramp_up=args.ramp_up,
        filter_queries=filter_queries,
        profile=getattr(args, "profile", True),
        perf_freq=getattr(args, "perf_freq", 99),
        query_timeout=args.query_timeout,
        flamegraph_min_ms=getattr(args, "flamegraph_min_ms", 1000),
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
            return CommandResult.failure(str(e))

    parallel_dbms = getattr(args, "parallel_dbms", True)
    output_dir = os.path.abspath(args.output_dir)
    fmt = args.output_format
    is_json = getattr(args, "json", False)

    # Determine database: JSON config overrides CLI default
    json_database = json_extra_config.get('database')
    final_database = json_database if json_database is not None else args.database

    # ------------------------------------------------------------------
    # Setup logging: redirect to file, suppress console noise
    # ------------------------------------------------------------------
    log = logging.getLogger("rosetta")
    for handler in log.handlers[:]:
        log.removeHandler(handler)
    logging.root.handlers.clear()

    # ------------------------------------------------------------------
    # Print plan (rich UI) — skip in JSON mode
    # ------------------------------------------------------------------
    if not is_json:
        print_phase("Benchmark", workload.name)
        print_info("Mode:", mode.name)
        print_info("DBMS targets:", ", ".join(c.name for c in configs))
        if parallel_dbms and len(configs) > 1:
            print_info("DBMS execution:", "[bold green]parallel[/bold green]")
        elif not parallel_dbms and len(configs) > 1:
            print_info("DBMS execution:", "sequential")
        print_info("Queries:", ", ".join(q.name for q in display_workload.queries))
        if mode == WorkloadMode.SERIAL:
            print_info("Iterations:",
                       f"{bench_cfg.iterations}  Warmup: {bench_cfg.warmup}")
        else:
            print_info("Concurrency:",
                       f"{bench_cfg.concurrency}  Duration: {bench_cfg.duration}s")
        if filter_queries:
            print_info("Filter:", ", ".join(filter_queries))
        if bench_cfg.profile:
            print_info("Profiling:",
                       f"[bold red]🔥 perf flame graph[/bold red] "
                       f"(freq: {bench_cfg.perf_freq} Hz)")
        if bench_cfg.skip_setup:
            print_info("Setup:", "[bold yellow]SKIPPED[/bold yellow] (reusing existing tables)")
        if bench_cfg.skip_teardown:
            print_info("Teardown:", "[bold yellow]SKIPPED[/bold yellow] (keeping tables)")

    # ------------------------------------------------------------------
    # Inner function: single benchmark round with progress bars
    # ------------------------------------------------------------------
    def _run_one_round():
        run_stamp = _time.strftime("%Y%m%d_%H%M%S")
        run_dir = os.path.join(output_dir, f"bench_{workload.name}_{run_stamp}")
        os.makedirs(run_dir, exist_ok=True)

        # File logging per round
        file_handler = logging.FileHandler(
            os.path.join(run_dir, "rosetta.log"), mode="w", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"))
        log.addHandler(file_handler)
        log.setLevel(logging.DEBUG)

        if not is_json:
            print_phase("Execute")

        # --- Live Table progress (same style as rosetta mtr/test) ---
        from rich import box
        from rich.console import Console as _Console
        from rich.live import Live as _Live
        from rich.table import Table as _Table
        from rich.text import Text as _Text

        live_console = _Console(stderr=True)

        n_queries = len(display_workload.queries)
        is_concurrent = (mode == WorkloadMode.CONCURRENT)
        if is_concurrent:
            duration = bench_cfg.duration if bench_cfg.duration > 0 else 30.0
            total_iters = int(duration)  # time-based
        else:
            duration = 0.0
            total_iters = n_queries * (bench_cfg.iterations + bench_cfg.warmup)

        # Track state per DBMS
        dbms_state = {
            c.name: {
                "status": "waiting",
                "progress": 0,
                "total": total_iters,
                "completed": 0,
                "elapsed": 0.0,
                "start_time": None,
                "last_status": "",
                "is_concurrent": is_concurrent,
                "duration": duration,
            }
            for c in configs
        }
        _state_lock = threading.Lock()

        def _build_bench_progress_table() -> _Table:
            table = _Table(
                show_header=True,
                header_style="bold cyan",
                expand=True,
                padding=(0, 1),
                box=box.ROUNDED,
            )
            table.add_column("DBMS", style="bold", min_width=12)
            table.add_column("Progress", min_width=14)
            table.add_column("Elapsed", justify="right", min_width=10)
            table.add_column("Status", ratio=1, overflow="ellipsis", no_wrap=True)

            for c in configs:
                st = dbms_state[c.name]
                # Elapsed time
                if st["status"] == "done" and st["elapsed"] > 0:
                    elapsed = st["elapsed"]
                elif st["start_time"] is not None:
                    elapsed = _time.monotonic() - st["start_time"]
                else:
                    elapsed = 0
                mins, secs = divmod(int(elapsed), 60)
                hours, mins = divmod(mins, 60)
                if hours > 0:
                    elapsed_str = f"{hours}h{mins:02d}m{secs:02d}s"
                else:
                    elapsed_str = f"{mins:02d}m{secs:02d}s"

                # Progress display
                pct = st.get("progress", 0)
                if st["status"] == "waiting":
                    progress = _Text("⏳ Waiting", style="dim")
                elif st["status"] == "running":
                    bar_filled = int(pct / 5)  # 20-char bar
                    bar_empty = 20 - bar_filled
                    if is_concurrent and duration > 0:
                        elapsed_int = int(elapsed)
                        bar_str = (f"[yellow]{'█' * bar_filled}{'░' * bar_empty}"
                                   f"[/yellow] {elapsed_int}s/{int(duration)}s")
                    else:
                        bar_str = (f"[yellow]{'█' * bar_filled}{'░' * bar_empty}"
                                   f"[/yellow] {pct}%")
                    progress = _Text.from_markup(bar_str)
                elif st["status"] == "done":
                    if st.get("failed"):
                        progress = _Text("❌ Failed", style="red bold")
                    else:
                        progress = _Text("✅ Done", style="green bold")
                else:
                    progress = _Text(st["status"])

                # Status text
                status_text = st.get("last_status", "")

                table.add_row(c.name, progress, elapsed_str, status_text)

            return table

        def on_setup_start(dbms_name):
            with _state_lock:
                st = dbms_state[dbms_name]
                st["status"] = "running"
                st["start_time"] = _time.monotonic()
                st["last_status"] = "setup..."

        def on_setup_done(dbms_name, success):
            with _state_lock:
                st = dbms_state[dbms_name]
                if success:
                    st["last_status"] = "setup done"
                else:
                    st["status"] = "done"
                    st["elapsed"] = _time.monotonic() - (st["start_time"] or _time.monotonic())
                    st["failed"] = True
                    st["last_status"] = "setup failed"

        def on_dbms_start(dbms_name):
            with _state_lock:
                st = dbms_state[dbms_name]
                if st["status"] != "done":  # not failed during setup
                    st["status"] = "running"
                    if st["start_time"] is None:
                        st["start_time"] = _time.monotonic()

        def on_progress(dbms_name, query_name, iteration, total,
                        is_warmup=False):
            with _state_lock:
                st = dbms_state[dbms_name]
                st["completed"] += 1
                if st["total"] > 0 and not is_concurrent:
                    st["progress"] = int(st["completed"] / st["total"] * 100)
                status_prefix = "warmup" if is_warmup else ""
                if is_concurrent and duration > 0:
                    elapsed = _time.monotonic() - (st["start_time"] or _time.monotonic())
                    st["progress"] = min(int(elapsed / duration * 100), 100)
                    st["last_status"] = f"{status_prefix}{query_name}" if is_warmup else query_name
                else:
                    st["last_status"] = f"{status_prefix}{query_name} {iteration}/{total}" if is_warmup else f"{query_name} {iteration}/{total}"

        def on_dbms_done(dbms_name, dbms_result):
            with _state_lock:
                st = dbms_state[dbms_name]
                st["status"] = "done"
                st["progress"] = 100
                st["elapsed"] = _time.monotonic() - (st["start_time"] or _time.monotonic())
                st["last_status"] = (
                    f"{dbms_result.total_queries} queries, "
                    f"{dbms_result.overall_qps:.1f} QPS"
                )

        def on_profile_start(dbms_name, query_name):
            with _state_lock:
                st = dbms_state[dbms_name]
                st["last_status"] = f"🔥 profiling {query_name}"

        def on_profile_done(dbms_name, query_name, sample_count):
            with _state_lock:
                st = dbms_state[dbms_name]
                st["last_status"] = f"🔥 {query_name}: {sample_count} samples"

        def on_run_start():
            with _state_lock:
                for c in configs:
                    st = dbms_state[c.name]
                    if st["status"] != "done":
                        st["start_time"] = _time.monotonic()

        # Timer thread for concurrent mode (updates progress periodically)
        timer_stop_event = None
        timer_thread = None

        if is_concurrent:
            timer_stop_event = threading.Event()

            def _timer_update():
                while not timer_stop_event.is_set():
                    with _state_lock:
                        for c in configs:
                            st = dbms_state[c.name]
                            if st["status"] == "running" and st["start_time"] is not None:
                                elapsed = _time.monotonic() - st["start_time"]
                                if duration > 0:
                                    st["progress"] = min(int(elapsed / duration * 100), 100)
                    _time.sleep(0.5)

            timer_thread = threading.Thread(target=_timer_update, daemon=True)
            timer_thread.start()

        try:
            with _Live(
                _build_bench_progress_table(),
                console=live_console,
                refresh_per_second=2,
                transient=is_json,
            ) as live:
                # Background thread to refresh the Live table periodically
                # (callbacks update dbms_state but don't call live.update)
                _live_stop_event = threading.Event()

                def _live_refresher():
                    while not _live_stop_event.is_set():
                        live.update(_build_bench_progress_table())
                        _live_stop_event.wait(0.5)

                _refresher_thread = threading.Thread(target=_live_refresher, daemon=True)
                _refresher_thread.start()

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
                    # Set run_id for the result
                    result.run_id = os.path.basename(run_dir)
                finally:
                    # Stop the live refresher and do one final update
                    _live_stop_event.set()
                    _refresher_thread.join(timeout=1.0)
                    live.update(_build_bench_progress_table())
        finally:
            if timer_stop_event is not None:
                timer_stop_event.set()
                if timer_thread is not None:
                    timer_thread.join(timeout=1.0)

        # Generate reports
        if not is_json:
            print_phase("Reports")

        if fmt in ("text", "all"):
            text_path = os.path.join(run_dir, f"bench_{workload.name}.report.txt")
            write_bench_text_report(text_path, result)
            if not is_json:
                print_report_file(text_path, label="text")

        if fmt in ("html", "all"):
            html_path = os.path.join(run_dir, f"bench_{workload.name}.html")
            write_bench_html_report(html_path, result)
            if not is_json:
                print_report_file(html_path, label="html")

        json_path = os.path.join(run_dir, "bench_result.json")
        _save_bench_json(json_path, result, bench_file=args.bench_file or "", database=final_database)
        if not is_json:
            print_report_file(json_path, label="json")

        # Update latest symlink
        latest_link = os.path.join(output_dir, "latest")
        try:
            if os.path.islink(latest_link):
                os.remove(latest_link)
            os.symlink(os.path.basename(run_dir), latest_link)
        except OSError:
            pass

        generate_index_html(output_dir)
        if not is_json:
            print_bench_summary(result)
            flush_all()

        # Remove file handler after round
        log.removeHandler(file_handler)

        return run_dir, result

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    last_run_dir = None
    last_result = None
    try:
        last_run_dir, last_result = _run_one_round()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted. Stopping.[/yellow]")
        flush_all()

    if last_result is None:
        return CommandResult.failure("Benchmark execution was interrupted")

    # Build CommandResult
    total_queries = sum(dr.total_queries for dr in last_result.dbms_results)

    report_files = []
    if last_run_dir:
        for f in os.listdir(last_run_dir):
            if f.endswith(('.report.txt', '.html', '.json')):
                report_files.append(os.path.join(last_run_dir, f))

    return CommandResult.success(
        "bench",
        {
            "workload": workload.name,
            "mode": mode.name,
            "dbms_targets": [c.name for c in configs],
            "database": final_database,
            "iterations": args.iterations,
            "warmup": args.warmup,
            "concurrency": concurrency,
            "duration": args.duration,
            "total_queries": total_queries,
            "dbms_results": [
                {
                    "dbms_name": dr.dbms_name,
                    "total_queries": dr.total_queries,
                    "total_errors": dr.total_errors,
                    "overall_qps": round(dr.overall_qps, 2),
                    "total_duration_s": round(dr.total_duration_s, 3),
                }
                for dr in last_result.dbms_results
            ],
            "report_directory": last_run_dir,
            "report_files": sorted(report_files),
        },
    )
