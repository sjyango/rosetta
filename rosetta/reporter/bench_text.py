"""Plain-text benchmark report generator for Rosetta."""

import logging
import time
from typing import List

from ..models import BenchmarkResult, DBMSBenchResult, QueryLatencyStats

log = logging.getLogger("rosetta")


def _fmt_ms(v: float) -> str:
    """Format a millisecond value nicely."""
    if v < 1:
        return f"{v:.3f}"
    if v < 100:
        return f"{v:.2f}"
    return f"{v:.1f}"


def write_bench_text_report(path: str, result: BenchmarkResult):
    """Write a plain-text benchmark report.

    Sections:
    1. Header (workload, mode, config)
    2. Per-DBMS results with per-query latency stats
    3. Cross-DBMS comparison table
    """
    with open(path, "w", encoding="utf-8") as f:
        # Header
        f.write("=" * 78 + "\n")
        f.write("Rosetta Benchmark Report\n")
        f.write(f"Time: {result.timestamp or time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Workload: {result.workload_name}\n")
        f.write(f"Mode: {result.mode.name}\n")
        cfg = result.config
        if result.mode.name == "SERIAL":
            f.write(f"Iterations: {cfg.iterations}  Warmup: {cfg.warmup}\n")
        else:
            f.write(f"Concurrency: {cfg.concurrency}  "
                    f"Duration: {cfg.duration}s\n")
        if cfg.filter_queries:
            f.write(f"Filter: {', '.join(cfg.filter_queries)}\n")
        f.write("=" * 78 + "\n\n")

        # Per-DBMS detail
        for dr in result.dbms_results:
            f.write("-" * 78 + "\n")
            f.write(f"DBMS: {dr.dbms_name}\n")
            f.write(f"Total duration: {dr.total_duration_s:.2f}s  "
                    f"Total queries: {dr.total_queries}  "
                    f"Errors: {dr.total_errors}  "
                    f"Overall QPS: {dr.overall_qps:.1f}\n")
            f.write("-" * 78 + "\n")

            # Per-query table
            hdr = (f"  {'Query':<20s} {'Exec':>6s} {'Err':>5s} "
                   f"{'Avg':>8s} {'P50':>8s} {'P95':>8s} {'P99':>8s} "
                   f"{'Min':>8s} {'Max':>8s} {'QPS':>8s}\n")
            f.write(hdr)
            f.write("  " + "-" * 74 + "\n")

            for qs in dr.query_stats:
                row = (
                    f"  {qs.query_name:<20s} "
                    f"{qs.total_executions - qs.total_errors:>6d} "
                    f"{qs.total_errors:>5d} "
                    f"{_fmt_ms(qs.avg_ms):>8s} "
                    f"{_fmt_ms(qs.p50_ms):>8s} "
                    f"{_fmt_ms(qs.p95_ms):>8s} "
                    f"{_fmt_ms(qs.p99_ms):>8s} "
                    f"{_fmt_ms(qs.min_ms):>8s} "
                    f"{_fmt_ms(qs.max_ms):>8s} "
                    f"{qs.qps:>8.1f}\n"
                )
                f.write(row)

            f.write("\n")

        # Cross-DBMS comparison table
        if len(result.dbms_results) >= 2:
            f.write("=" * 78 + "\n")
            f.write("CROSS-DBMS COMPARISON\n")
            f.write("=" * 78 + "\n")

            # Collect all query names
            all_queries = []
            for dr in result.dbms_results:
                for qs in dr.query_stats:
                    if qs.query_name not in all_queries:
                        all_queries.append(qs.query_name)

            dbms_names = [dr.dbms_name for dr in result.dbms_results]

            for qname in all_queries:
                f.write(f"\n  Query: {qname}\n")
                hdr = f"  {'DBMS':<16s} {'Avg(ms)':>10s} {'P95(ms)':>10s} {'QPS':>10s}\n"
                f.write(hdr)
                f.write("  " + "-" * 48 + "\n")

                for dr in result.dbms_results:
                    qs = _find_query_stats(dr, qname)
                    if qs:
                        f.write(
                            f"  {dr.dbms_name:<16s} "
                            f"{_fmt_ms(qs.avg_ms):>10s} "
                            f"{_fmt_ms(qs.p95_ms):>10s} "
                            f"{qs.qps:>10.1f}\n"
                        )

            # Overall QPS comparison
            f.write(f"\n  Overall QPS:\n")
            f.write(f"  {'DBMS':<16s} {'QPS':>10s} {'Duration':>10s}\n")
            f.write("  " + "-" * 38 + "\n")
            for dr in result.dbms_results:
                f.write(
                    f"  {dr.dbms_name:<16s} "
                    f"{dr.overall_qps:>10.1f} "
                    f"{dr.total_duration_s:>9.2f}s\n"
                )

        f.write("\n" + "=" * 78 + "\n")

    log.info("Benchmark text report written: %s", path)


def _find_query_stats(
    dr: DBMSBenchResult, query_name: str
) -> QueryLatencyStats:
    """Find stats for a query in a DBMS result."""
    for qs in dr.query_stats:
        if qs.query_name == query_name:
            return qs
    return None
