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
    from ..whitelist import Whitelist
    from ..buglist import Buglist
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
    
    # Check test file
    if not os.path.isfile(args.test):
        return CommandResult.failure(
            f"Test file not found: {args.test}",
        )
    
    # Parse-only mode
    if args.parse_only:
        try:
            from ..mtr import MtrParser
            mtr_parser = MtrParser(args.test)
            test = mtr_parser.parse()
            cmd_types = set(c.cmd_type.name for c in test.commands)
            return CommandResult.success(
                "mtr parse-only",
                {
                    "test_file": args.test,
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
    test_name = Path(args.test).stem
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
    
    # Load whitelist and buglist
    whitelist = Whitelist(output_dir)
    buglist = Buglist(output_dir)
    
    # Use RosettaRunner for MTR execution (has progress bars)
    runner = RosettaRunner(
        test_file=args.test,
        configs=configs,
        output_dir=run_dir,
        database=args.database,
        baseline=args.baseline,
        skip_explain=args.skip_explain,
        skip_analyze=args.skip_analyze,
        skip_show_create=args.skip_show_create,
        output_format=args.output_format,
        whitelist=whitelist,
        buglist=buglist,
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
            "whitelisted": cmp.whitelisted,
            "sql_whitelisted": cmp.sql_whitelisted,
            "bug_marked": cmp.bug_marked,
            "pass_rate": round(cmp.pass_rate, 2),
        }
    
    # Get report files
    report_files = []
    for f in os.listdir(run_dir):
        if f.endswith(('.report.txt', '.diff', '.html')):
            report_files.append(f)
    
    # Handle --serve: start HTTP server and block
    if getattr(args, 'serve', False):
        from ..runner import _serve_report
        # Find the first HTML report file
        html_files = [f for f in report_files if f.endswith('.html')]
        if html_files:
            html_file = html_files[0]
        else:
            # Fallback: use test name
            test_name = Path(args.test).stem
            html_file = f"{test_name}.html"
        
        port = getattr(args, 'port', 19527)
        _serve_report(run_dir, html_file, port=port)
        # _serve_report blocks until KeyboardInterrupt, so we won't reach here
    
    return CommandResult.success(
        "mtr",
        {
            "test_file": args.test,
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
    try:
        if args.bench_file:
            workload = BenchmarkLoader.from_file(args.bench_file)
            if args.bench_file.endswith('.json'):
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
            workload = BenchmarkLoader.from_builtin("oltp_read_write")
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
    repeat = max(1, getattr(args, 'repeat', 1))
    output_dir = os.path.abspath(args.output_dir)
    fmt = args.output_format

    # Determine database: JSON config overrides CLI default
    json_database = json_extra_config.get('database')
    final_database = json_database if json_database else args.database

    # ------------------------------------------------------------------
    # Setup logging: redirect to file, suppress console noise
    # ------------------------------------------------------------------
    log = logging.getLogger("rosetta")
    for handler in log.handlers[:]:
        log.removeHandler(handler)
    logging.root.handlers.clear()

    # ------------------------------------------------------------------
    # Print plan (rich UI)
    # ------------------------------------------------------------------
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
    # Inner function: single benchmark round with progress bars
    # ------------------------------------------------------------------
    def _run_one_round(round_num: int):
        if repeat > 1:
            console.print(f"\n[bold cyan]{'━' * 60}[/bold cyan]")
            console.print(f"[bold cyan]  Round {round_num}/{repeat}[/bold cyan]")
            console.print(f"[bold cyan]{'━' * 60}[/bold cyan]\n")

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

        print_phase("Execute")

        # Progress tracking
        progress_bars = {}
        _progress_lock = threading.Lock()

        n_queries = len(display_workload.queries)
        is_concurrent = (mode == WorkloadMode.CONCURRENT)
        if is_concurrent:
            duration = bench_cfg.duration if bench_cfg.duration > 0 else 30.0
            per_query = 100
        else:
            duration = 0.0
            per_query = bench_cfg.iterations + bench_cfg.warmup

        # Pre-create progress bars for parallel mode
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
                    bp.set_status("[red]setup失败 — 跳过该DBMS[/red]")
                    # Close progress bar for failed DBMS
                    bp.__exit__(None, None, None)
                    bp.write_summary_to_buffer()

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

        # Timer thread for concurrent mode
        timer_stop_event = None
        timer_thread = None
        query_phase_started = threading.Event()
        timer_start_time = [None]

        if is_concurrent:
            timer_stop_event = threading.Event()

            def _timer_update():
                query_phase_started.wait()
                while not timer_stop_event.is_set():
                    if timer_start_time[0] is not None:
                        elapsed = _time.monotonic() - timer_start_time[0]
                        if elapsed >= duration:
                            break
                    for _, bp in list(progress_bars.items()):
                        bp.update_time(status="")
                    _time.sleep(0.5)

            timer_thread = threading.Thread(target=_timer_update, daemon=True)
            timer_thread.start()

        def on_run_start():
            with _progress_lock:
                for bp in progress_bars.values():
                    bp.reset_timer()
            timer_start_time[0] = _time.monotonic()
            query_phase_started.set()

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
            if timer_stop_event is not None:
                timer_stop_event.set()
                if timer_thread is not None:
                    timer_thread.join(timeout=1.0)

        # Generate reports
        print_phase("Reports")

        if fmt in ("text", "all"):
            text_path = os.path.join(run_dir, f"bench_{workload.name}.report.txt")
            write_bench_text_report(text_path, result)
            print_report_file(text_path, label="text")

        if fmt in ("html", "all"):
            html_path = os.path.join(run_dir, f"bench_{workload.name}.html")
            write_bench_html_report(html_path, result)
            print_report_file(html_path, label="html")

        json_path = os.path.join(run_dir, "bench_result.json")
        _save_bench_json(json_path, result, bench_file=args.bench_file or "", database=final_database)
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
    for rnd in range(1, repeat + 1):
        try:
            last_run_dir, last_result = _run_one_round(rnd)
        except KeyboardInterrupt:
            console.print(
                f"\n[yellow]Interrupted at round {rnd}/{repeat}. "
                f"Stopping.[/yellow]")
            flush_all()
            break
        if rnd < repeat:
            _time.sleep(1)

    if repeat > 1:
        console.print(
            f"\n[bold green]All {repeat} rounds completed.[/bold green]")
        flush_all()

    if last_result is None:
        return CommandResult.failure("Benchmark execution was interrupted")

    # Build CommandResult
    total_queries = sum(dr.total_queries for dr in last_result.dbms_results)

    report_files = []
    if last_run_dir:
        for f in os.listdir(last_run_dir):
            if f.endswith(('.report.txt', '.html', '.json')):
                report_files.append(f)

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
