"""Benchmark engine for Rosetta cross-DBMS performance comparison."""

import concurrent.futures
import json
import logging
import math
import random
import re
import string
import threading
import time as _time
from pathlib import Path
from typing import Callable, Dict, List, Optional

from .executor import DBConnection, ensure_service
from .models import (
    BenchmarkConfig, BenchmarkResult, BenchQuery, BenchWorkload,
    DBMSBenchResult, DBMSConfig, QueryLatencyStats, WorkloadMode,
)

log = logging.getLogger("rosetta")


# ---------------------------------------------------------------------------
# Template variable engine
# ---------------------------------------------------------------------------

class TemplateEngine:
    """Render template variables in SQL strings.

    Supported placeholders:
        {{rand_int(min,max)}}       — random integer in [min, max]
        {{rand_str(len)}}           — random alphanumeric string of given length
        {{rand_choice(a,b,c,...)}}  — random pick from comma-separated values
        {{seq_int()}}               — monotonically increasing integer (per engine)
    """

    # Match {{func_name(args)}} or {{func_name()}}
    _PATTERN = re.compile(r"\{\{\s*(\w+)\s*\(([^)]*)\)\s*\}\}")

    def __init__(self, seed: Optional[int] = None):
        self._rng = random.Random(seed)
        self._seq_counter = 0
        self._seq_lock = threading.Lock()

    def render(self, sql: str) -> str:
        """Replace all template placeholders in *sql* with concrete values."""
        return self._PATTERN.sub(self._replace_match, sql)

    def _replace_match(self, match: re.Match) -> str:
        func_name = match.group(1).lower()
        raw_args = match.group(2).strip()

        handler = {
            "rand_int": self._rand_int,
            "rand_str": self._rand_str,
            "rand_choice": self._rand_choice,
            "seq_int": self._seq_int,
        }.get(func_name)

        if handler is None:
            log.warning("Unknown template function: %s", func_name)
            return match.group(0)  # leave unchanged

        return handler(raw_args)

    # -- handler implementations ---------------------------------------------

    def _rand_int(self, args: str) -> str:
        parts = [p.strip() for p in args.split(",")]
        if len(parts) != 2:
            log.warning("rand_int expects 2 args, got: %s", args)
            return "0"
        try:
            lo, hi = int(parts[0]), int(parts[1])
        except ValueError:
            log.warning("rand_int args must be integers: %s", args)
            return "0"
        return str(self._rng.randint(lo, hi))

    def _rand_str(self, args: str) -> str:
        args = args.strip()
        try:
            length = int(args)
        except ValueError:
            log.warning("rand_str expects an integer length: %s", args)
            length = 8
        chars = string.ascii_letters + string.digits
        return "".join(self._rng.choice(chars) for _ in range(length))

    def _rand_choice(self, args: str) -> str:
        choices = [c.strip() for c in args.split(",") if c.strip()]
        if not choices:
            log.warning("rand_choice received empty choices")
            return ""
        return self._rng.choice(choices)

    def _seq_int(self, _args: str) -> str:
        with self._seq_lock:
            self._seq_counter += 1
            return str(self._seq_counter)

    def reset_seq(self):
        """Reset the sequential counter (useful between runs)."""
        with self._seq_lock:
            self._seq_counter = 0


# ---------------------------------------------------------------------------
# Benchmark loader
# ---------------------------------------------------------------------------

class BenchmarkLoader:
    """Load benchmark workload definitions from various sources.

    Supported sources:
        1. Built-in template name  — e.g. "oltp_read_write"
        2. Plain .sql file         — each non-empty/non-comment line is a query
        3. JSON definition file    — full control (setup, queries, teardown)
    """

    @staticmethod
    def from_builtin(template_name: str) -> BenchWorkload:
        """Load a built-in benchmark template by name."""
        template_name = template_name.lower()
        if template_name not in BUILTIN_TEMPLATES:
            available = ", ".join(sorted(BUILTIN_TEMPLATES.keys()))
            raise ValueError(
                f"Unknown built-in template: '{template_name}'. "
                f"Available: {available}"
            )
        return BUILTIN_TEMPLATES[template_name]()

    @staticmethod
    def from_sql_file(path: str) -> BenchWorkload:
        """Load a benchmark workload from a plain .sql file.

        Each non-empty, non-comment line becomes a query.
        Multi-line statements are NOT supported in plain mode;
        use JSON definition for complex workloads.
        """
        filepath = Path(path)
        if not filepath.exists():
            raise FileNotFoundError(f"SQL file not found: {path}")

        text = filepath.read_text(encoding="utf-8")
        queries: List[BenchQuery] = []
        idx = 0

        for line in text.splitlines():
            stripped = line.strip()
            # skip empty lines and comments
            if not stripped or stripped.startswith("--") or stripped.startswith("#"):
                continue
            # remove trailing semicolon for consistency
            if stripped.endswith(";"):
                stripped = stripped[:-1].rstrip()
            if not stripped:
                continue
            idx += 1
            # use a readable default name
            name = f"query_{idx}"
            queries.append(BenchQuery(name=name, sql=stripped, weight=1))

        if not queries:
            raise ValueError(f"No valid queries found in: {path}")

        return BenchWorkload(
            name=filepath.stem,
            queries=queries,
        )

    @staticmethod
    def from_json_file(path: str) -> BenchWorkload:
        """Load a benchmark workload from a JSON definition file.

        Expected JSON schema:
        {
            "name": "my_workload",            // optional
            "setup": ["CREATE TABLE ..."],     // optional
            "queries": [
                {
                    "name": "point_select",    // optional, auto-generated
                    "sql": "SELECT ...",       // required
                    "weight": 5               // optional, default 1
                }
            ],
            "teardown": ["DROP TABLE ..."]     // optional
        }
        """
        filepath = Path(path)
        if not filepath.exists():
            raise FileNotFoundError(f"JSON file not found: {path}")

        data = json.loads(filepath.read_text(encoding="utf-8"))

        if not isinstance(data, dict):
            raise ValueError("JSON benchmark file must be a JSON object")

        raw_queries = data.get("queries", [])
        if not raw_queries:
            raise ValueError("JSON benchmark file must contain 'queries'")

        queries: List[BenchQuery] = []
        for i, q in enumerate(raw_queries):
            if isinstance(q, str):
                # shorthand: just a SQL string
                queries.append(BenchQuery(
                    name=f"query_{i + 1}",
                    sql=q.rstrip(";").strip(),
                    weight=1,
                ))
            elif isinstance(q, dict):
                sql = q.get("sql", "").rstrip(";").strip()
                if not sql:
                    raise ValueError(
                        f"Query at index {i} is missing 'sql' field"
                    )
                queries.append(BenchQuery(
                    name=q.get("name", f"query_{i + 1}"),
                    sql=sql,
                    weight=max(1, int(q.get("weight", 1))),
                ))
            else:
                raise ValueError(
                    f"Query at index {i}: expected string or object, "
                    f"got {type(q).__name__}"
                )

        setup = data.get("setup", [])
        if isinstance(setup, str):
            setup = [setup]

        teardown = data.get("teardown", [])
        if isinstance(teardown, str):
            teardown = [teardown]

        return BenchWorkload(
            name=data.get("name", filepath.stem),
            setup=setup,
            queries=queries,
            teardown=teardown,
        )

    @staticmethod
    def from_file(path: str) -> BenchWorkload:
        """Auto-detect file type and load accordingly."""
        lower = path.lower()
        if lower.endswith(".json"):
            return BenchmarkLoader.from_json_file(path)
        elif lower.endswith(".sql"):
            return BenchmarkLoader.from_sql_file(path)
        else:
            raise ValueError(
                f"Unsupported benchmark file extension: {path}. "
                "Use .json or .sql"
            )

    @staticmethod
    def list_builtin_templates() -> List[str]:
        """Return names of all available built-in templates."""
        return sorted(BUILTIN_TEMPLATES.keys())

    @staticmethod
    def filter_queries(
        workload: BenchWorkload, names: List[str]
    ) -> BenchWorkload:
        """Return a new workload containing only queries whose names match.

        Matching is case-insensitive and supports substring matching.
        """
        if not names:
            return workload

        lower_names = [n.lower() for n in names]
        filtered = [
            q for q in workload.queries
            if any(n in q.name.lower() for n in lower_names)
        ]

        if not filtered:
            available = ", ".join(q.name for q in workload.queries)
            raise ValueError(
                f"No queries match filter {names}. "
                f"Available queries: {available}"
            )

        return BenchWorkload(
            name=workload.name,
            setup=workload.setup,
            queries=filtered,
            teardown=workload.teardown,
        )


# ---------------------------------------------------------------------------
# Built-in benchmark templates
# ---------------------------------------------------------------------------

def _template_oltp_read_write() -> BenchWorkload:
    """OLTP Read-Write mixed workload.

    Requires a pre-created table. Setup creates 'bench_accounts' with
    10 000 rows; teardown drops it.
    """
    return BenchWorkload(
        name="oltp_read_write",
        setup=[
            "CREATE TABLE IF NOT EXISTS bench_accounts ("
            "  id INT PRIMARY KEY AUTO_INCREMENT,"
            "  name VARCHAR(100) NOT NULL,"
            "  balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,"
            "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ")",
            # Seed 10 000 rows via a quick INSERT-SELECT trick.
            # We insert in small batches to avoid overlong SQL.
            "INSERT INTO bench_accounts (name, balance) "
            "SELECT CONCAT('user_', seq), ROUND(RAND() * 10000, 2) "
            "FROM (SELECT @rownum := @rownum + 1 AS seq "
            "      FROM information_schema.columns a, "
            "           information_schema.columns b, "
            "           (SELECT @rownum := 0) r "
            "      LIMIT 10000) t",
        ],
        queries=[
            BenchQuery(
                name="point_select",
                sql="SELECT * FROM bench_accounts WHERE id = {{rand_int(1,10000)}}",
                weight=5,
            ),
            BenchQuery(
                name="range_select",
                sql="SELECT * FROM bench_accounts WHERE id BETWEEN "
                    "{{rand_int(1,5000)}} AND {{rand_int(5001,10000)}} "
                    "ORDER BY id LIMIT 100",
                weight=3,
            ),
            BenchQuery(
                name="update_balance",
                sql="UPDATE bench_accounts SET balance = balance + "
                    "{{rand_int(1,100)}} WHERE id = {{rand_int(1,10000)}}",
                weight=3,
            ),
            BenchQuery(
                name="insert_row",
                sql="INSERT INTO bench_accounts (name, balance) VALUES "
                    "('{{rand_str(10)}}', {{rand_int(100,9999)}})",
                weight=2,
            ),
            BenchQuery(
                name="delete_row",
                sql="DELETE FROM bench_accounts ORDER BY RAND() LIMIT 1",
                weight=1,
            ),
            BenchQuery(
                name="aggregate_sum",
                sql="SELECT COUNT(*), SUM(balance), AVG(balance) "
                    "FROM bench_accounts",
                weight=1,
            ),
        ],
        teardown=[
            "DROP TABLE IF EXISTS bench_accounts",
        ],
    )


def _template_oltp_read_only() -> BenchWorkload:
    """OLTP Read-Only workload — no writes after setup."""
    return BenchWorkload(
        name="oltp_read_only",
        setup=[
            "CREATE TABLE IF NOT EXISTS bench_accounts ("
            "  id INT PRIMARY KEY AUTO_INCREMENT,"
            "  name VARCHAR(100) NOT NULL,"
            "  balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,"
            "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ")",
            "INSERT INTO bench_accounts (name, balance) "
            "SELECT CONCAT('user_', seq), ROUND(RAND() * 10000, 2) "
            "FROM (SELECT @rownum := @rownum + 1 AS seq "
            "      FROM information_schema.columns a, "
            "           information_schema.columns b, "
            "           (SELECT @rownum := 0) r "
            "      LIMIT 10000) t",
        ],
        queries=[
            BenchQuery(
                name="point_select",
                sql="SELECT * FROM bench_accounts WHERE id = {{rand_int(1,10000)}}",
                weight=5,
            ),
            BenchQuery(
                name="range_select",
                sql="SELECT * FROM bench_accounts WHERE id BETWEEN "
                    "{{rand_int(1,5000)}} AND {{rand_int(5001,10000)}} "
                    "ORDER BY id LIMIT 100",
                weight=3,
            ),
            BenchQuery(
                name="aggregate_sum",
                sql="SELECT COUNT(*), SUM(balance), AVG(balance) "
                    "FROM bench_accounts",
                weight=2,
            ),
            BenchQuery(
                name="order_by_limit",
                sql="SELECT * FROM bench_accounts ORDER BY balance DESC "
                    "LIMIT {{rand_int(10,50)}}",
                weight=2,
            ),
            BenchQuery(
                name="like_search",
                sql="SELECT * FROM bench_accounts WHERE name LIKE "
                    "'user_{{rand_int(1,999)}}%' LIMIT 20",
                weight=1,
            ),
        ],
        teardown=[
            "DROP TABLE IF EXISTS bench_accounts",
        ],
    )


def _template_oltp_write_only() -> BenchWorkload:
    """OLTP Write-Only workload — inserts, updates, deletes."""
    return BenchWorkload(
        name="oltp_write_only",
        setup=[
            "CREATE TABLE IF NOT EXISTS bench_accounts ("
            "  id INT PRIMARY KEY AUTO_INCREMENT,"
            "  name VARCHAR(100) NOT NULL,"
            "  balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,"
            "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            ")",
            "INSERT INTO bench_accounts (name, balance) "
            "SELECT CONCAT('user_', seq), ROUND(RAND() * 10000, 2) "
            "FROM (SELECT @rownum := @rownum + 1 AS seq "
            "      FROM information_schema.columns a, "
            "           information_schema.columns b, "
            "           (SELECT @rownum := 0) r "
            "      LIMIT 10000) t",
        ],
        queries=[
            BenchQuery(
                name="insert_row",
                sql="INSERT INTO bench_accounts (name, balance) VALUES "
                    "('{{rand_str(10)}}', {{rand_int(100,9999)}})",
                weight=4,
            ),
            BenchQuery(
                name="update_balance",
                sql="UPDATE bench_accounts SET balance = balance + "
                    "{{rand_int(1,100)}} WHERE id = {{rand_int(1,10000)}}",
                weight=4,
            ),
            BenchQuery(
                name="update_name",
                sql="UPDATE bench_accounts SET name = '{{rand_str(10)}}' "
                    "WHERE id = {{rand_int(1,10000)}}",
                weight=2,
            ),
            BenchQuery(
                name="delete_row",
                sql="DELETE FROM bench_accounts WHERE id = {{rand_int(1,10000)}}",
                weight=2,
            ),
            BenchQuery(
                name="replace_row",
                sql="REPLACE INTO bench_accounts (id, name, balance) VALUES "
                    "({{rand_int(1,10000)}}, '{{rand_str(8)}}', {{rand_int(100,9999)}})",
                weight=1,
            ),
        ],
        teardown=[
            "DROP TABLE IF EXISTS bench_accounts",
        ],
    )


# Registry of built-in templates (name → factory function)
BUILTIN_TEMPLATES: Dict[str, callable] = {
    "oltp_read_write": _template_oltp_read_write,
    "oltp_read_only": _template_oltp_read_only,
    "oltp_write_only": _template_oltp_write_only,
}


# ---------------------------------------------------------------------------
# Statistics computation (pure Python, no numpy)
# ---------------------------------------------------------------------------

def _percentile(sorted_data: List[float], pct: float) -> float:
    """Compute the *pct*-th percentile from pre-sorted data."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_data[int(k)]
    d0 = sorted_data[int(f)] * (c - k)
    d1 = sorted_data[int(c)] * (k - f)
    return d0 + d1


def compute_stats(
    latencies: List[float], total_errors: int, elapsed_s: float,
    query_name: str, sql_template: str = "",
) -> QueryLatencyStats:
    """Compute latency statistics from a list of execution times (ms)."""
    stats = QueryLatencyStats(query_name=query_name, sql_template=sql_template)
    stats.total_executions = len(latencies) + total_errors
    stats.total_errors = total_errors

    if not latencies:
        return stats

    sorted_lat = sorted(latencies)
    stats.latencies_ms = latencies  # keep raw data for reports
    stats.min_ms = sorted_lat[0]
    stats.max_ms = sorted_lat[-1]
    stats.avg_ms = sum(sorted_lat) / len(sorted_lat)
    stats.p50_ms = _percentile(sorted_lat, 50)
    stats.p95_ms = _percentile(sorted_lat, 95)
    stats.p99_ms = _percentile(sorted_lat, 99)

    if elapsed_s > 0:
        stats.qps = len(latencies) / elapsed_s

    return stats


# ---------------------------------------------------------------------------
# Serial benchmark runner
# ---------------------------------------------------------------------------

class SerialBenchmarkRunner:
    """Execute each query N times sequentially, one after another."""

    def __init__(
        self, config: DBMSConfig, workload: BenchWorkload,
        bench_cfg: BenchmarkConfig, template_engine: TemplateEngine,
        database: str = "rosetta_bench",
        on_progress: Optional[Callable] = None,
        on_profile_start: Optional[Callable] = None,
        on_profile_done: Optional[Callable] = None,
    ):
        self.config = config
        self.workload = workload
        self.bench_cfg = bench_cfg
        self.engine = template_engine
        self.database = database
        self.on_progress = on_progress  # callback(query_name, iteration, total)
        self.on_profile_start = on_profile_start
        self.on_profile_done = on_profile_done
        self._mysqld_pid: Optional[int] = None

    def _resolve_mysqld_pid(self) -> Optional[int]:
        """Resolve the mysqld PID for perf profiling (cached)."""
        if self._mysqld_pid is not None:
            return self._mysqld_pid
        from .flamegraph import find_mysqld_pid
        pid = find_mysqld_pid(port=self.config.port)
        if pid:
            self._mysqld_pid = pid
            log.info("[%s] Resolved mysqld PID: %d (port %d)",
                     self.config.name, pid, self.config.port)
        else:
            log.warning("[%s] Could not find mysqld PID for port %d",
                        self.config.name, self.config.port)
        return self._mysqld_pid

    def run(self) -> DBMSBenchResult:
        """Run the serial benchmark and return results."""
        result = DBMSBenchResult(dbms_name=self.config.name)

        if not ensure_service(self.config):
            log.error("[%s] Service unavailable, skipping benchmark",
                      self.config.name)
            return result

        db = DBConnection(self.config, self.database)
        try:
            db.connect()
        except Exception as e:
            log.error("[%s] Connection failed: %s", self.config.name, e)
            return result

        # Resolve mysqld PID if profiling is enabled.
        # Profiling is only supported for tdsql – skip for other DBMS.
        profiling = self.bench_cfg.profile
        if profiling and self.config.name.lower() != "tdsql":
            log.info("[%s] Profiling skipped (only tdsql is profiled)",
                     self.config.name)
            profiling = False
        if profiling:
            from .flamegraph import PerfProfiler, check_perf_available
            ok, msg = check_perf_available()
            if not ok:
                log.warning("[%s] Profiling disabled: %s",
                            self.config.name, msg)
                profiling = False
            else:
                mysqld_pid = self._resolve_mysqld_pid()
                if not mysqld_pid:
                    log.warning("[%s] Profiling disabled: mysqld PID not found",
                                self.config.name)
                    profiling = False

        try:
            # Run setup
            for sql in self.workload.setup:
                try:
                    db.cursor.execute(sql)
                except Exception as e:
                    log.warning("[%s] Setup failed: %s — %s",
                                self.config.name, sql[:80], e)

            overall_start = None  # set at first query's iteration start

            for query in self.workload.queries:
                latencies: List[float] = []
                errors = 0

                # --- Phase 1: Warmup (no profiling, no latency recording) ---
                for i in range(self.bench_cfg.warmup):
                    rendered_sql = self.engine.render(query.sql)
                    try:
                        db.cursor.execute(rendered_sql)
                        if db.cursor.description:
                            db.cursor.fetchall()
                    except Exception as e:
                        log.debug("[%s] Warmup error: %s — %s",
                                  self.config.name, query.name, e)
                        if db._is_connection_lost(e):
                            if db.reconnect():
                                try:
                                    db.cursor.execute(
                                        f"USE `{self.database}`")
                                except Exception:
                                    pass
                        continue

                    if self.on_progress:
                        self.on_progress(
                            query.name, i + 1,
                            self.bench_cfg.iterations,
                            is_warmup=True,
                        )

                # --- Phase 1.5: Capture EXPLAIN plan (once, after warmup) ---
                explain_text = ""
                explain_tree_text = ""
                try:
                    rendered_sql = self.engine.render(query.sql)
                    db.cursor.execute("EXPLAIN " + rendered_sql)
                    if db.cursor.description:
                        cols = [desc[0] for desc in db.cursor.description]
                        rows = db.cursor.fetchall()
                        # Format as aligned text table
                        col_widths = [len(c) for c in cols]
                        for row in rows:
                            for j, cell in enumerate(row):
                                col_widths[j] = max(
                                    col_widths[j], len(str(cell)))
                        header = " | ".join(
                            c.ljust(col_widths[j])
                            for j, c in enumerate(cols))
                        sep = "-+-".join(
                            "-" * col_widths[j]
                            for j in range(len(cols)))
                        lines = [header, sep]
                        for row in rows:
                            lines.append(" | ".join(
                                str(cell).ljust(col_widths[j])
                                for j, cell in enumerate(row)))
                        explain_text = "\n".join(lines)
                except Exception as e:
                    log.debug("[%s] EXPLAIN failed for %s: %s",
                              self.config.name, query.name, e)

                # --- Phase 1.6: Capture EXPLAIN FORMAT=TREE (tdsql only) ---
                if self.config.name.lower() == "tdsql":
                    try:
                        rendered_sql = self.engine.render(query.sql)
                        db.cursor.execute(
                            "EXPLAIN FORMAT=TREE " + rendered_sql)
                        tree_rows = db.cursor.fetchall()
                        if tree_rows:
                            explain_tree_text = "\n".join(
                                str(row[0]) if row else ""
                                for row in tree_rows)
                    except Exception as e:
                        log.debug("[%s] EXPLAIN FORMAT=TREE failed for %s: %s",
                                  self.config.name, query.name, e)

                # --- Phase 2: Start perf, then execute all iterations ---
                # Begin timing: only actual SQL execution counts
                q_start = _time.monotonic()
                if overall_start is None:
                    overall_start = q_start

                profiler = None
                if profiling:
                    if self.on_profile_start:
                        self.on_profile_start(query.name)
                    profiler = PerfProfiler(
                        mysqld_pid=self._mysqld_pid,
                        perf_freq=self.bench_cfg.perf_freq,
                    )
                    profiler.start()

                for i in range(self.bench_cfg.iterations):
                    rendered_sql = self.engine.render(query.sql)

                    t0 = _time.monotonic()
                    try:
                        db.cursor.execute(rendered_sql)
                        # Consume result set to measure full round-trip
                        if db.cursor.description:
                            db.cursor.fetchall()
                    except Exception as e:
                        errors += 1
                        log.debug("[%s] Query error: %s — %s",
                                  self.config.name, query.name, e)
                        # Try reconnect on connection loss
                        if db._is_connection_lost(e):
                            if db.reconnect():
                                try:
                                    db.cursor.execute(
                                        f"USE `{self.database}`")
                                except Exception:
                                    pass
                        continue
                    t1 = _time.monotonic()

                    latencies.append((t1 - t0) * 1000.0)  # ms

                    if self.on_progress:
                        self.on_progress(
                            query.name, i + 1,
                            self.bench_cfg.iterations,
                            is_warmup=False,
                        )

                # --- Phase 3: Stop perf immediately after iterations ---
                fg_svg = ""
                if profiler is not None:
                    fg_data = profiler.stop(query_name=query.name)
                    if fg_data.svg_content:
                        fg_svg = fg_data.svg_content
                    elif fg_data.error:
                        log.warning("[%s] Flame graph for %s: %s",
                                    self.config.name, query.name,
                                    fg_data.error)
                    profiler.cleanup()
                    if self.on_profile_done:
                        self.on_profile_done(
                            query.name, fg_data.sample_count)

                q_elapsed = _time.monotonic() - q_start
                stats = compute_stats(
                    latencies, errors, q_elapsed, query.name,
                    sql_template=query.sql)
                stats.flamegraph_svg = fg_svg
                stats.explain_plan = explain_text
                stats.explain_tree = explain_tree_text
                result.query_stats.append(stats)
                result.total_queries += len(latencies) + errors
                result.total_errors += errors

            result.total_duration_s = (
                (_time.monotonic() - overall_start)
                if overall_start is not None else 0.0
            )
            if result.total_duration_s > 0:
                result.overall_qps = (
                    result.total_queries / result.total_duration_s
                )

        finally:
            # Run teardown
            for sql in self.workload.teardown:
                try:
                    db.cursor.execute(sql)
                except Exception as e:
                    log.warning("[%s] Teardown failed: %s — %s",
                                self.config.name, sql[:80], e)
            db.cleanup_database()
            db.close()

        return result


# ---------------------------------------------------------------------------
# Concurrent benchmark runner
# ---------------------------------------------------------------------------

class ConcurrentBenchmarkRunner:
    """Multi-threaded stress test with weight-based query selection."""

    def __init__(
        self, config: DBMSConfig, workload: BenchWorkload,
        bench_cfg: BenchmarkConfig, template_engine: TemplateEngine,
        database: str = "rosetta_bench",
        on_progress: Optional[Callable] = None,
        on_profile_start: Optional[Callable] = None,
        on_profile_done: Optional[Callable] = None,
    ):
        self.config = config
        self.workload = workload
        self.bench_cfg = bench_cfg
        self.engine = template_engine
        self.database = database
        self.on_progress = on_progress
        self.on_profile_start = on_profile_start
        self.on_profile_done = on_profile_done
        self._mysqld_pid: Optional[int] = None

    def _resolve_mysqld_pid(self) -> Optional[int]:
        """Resolve the mysqld PID for perf profiling (cached)."""
        if self._mysqld_pid is not None:
            return self._mysqld_pid
        from .flamegraph import find_mysqld_pid
        pid = find_mysqld_pid(port=self.config.port)
        if pid:
            self._mysqld_pid = pid
        return self._mysqld_pid

    def _build_weighted_pool(self) -> List[BenchQuery]:
        """Build a flat list based on query weights for random selection."""
        pool: List[BenchQuery] = []
        for q in self.workload.queries:
            pool.extend([q] * q.weight)
        return pool

    def run(self) -> DBMSBenchResult:
        """Run the concurrent benchmark and return results."""
        result = DBMSBenchResult(dbms_name=self.config.name)

        if not ensure_service(self.config):
            log.error("[%s] Service unavailable, skipping benchmark",
                      self.config.name)
            return result

        # Setup phase (single connection)
        setup_db = DBConnection(self.config, self.database)
        try:
            setup_db.connect()
            for sql in self.workload.setup:
                try:
                    setup_db.cursor.execute(sql)
                except Exception as e:
                    log.warning("[%s] Setup failed: %s — %s",
                                self.config.name, sql[:80], e)
        except Exception as e:
            log.error("[%s] Connection failed for setup: %s",
                      self.config.name, e)
            return result
        finally:
            setup_db.close()

        weighted_pool = self._build_weighted_pool()
        if not weighted_pool:
            log.error("[%s] No queries in workload", self.config.name)
            return result

        # Capture EXPLAIN plans before the concurrent run (single connection)
        explain_plans: Dict[str, str] = {}
        explain_tree_plans: Dict[str, str] = {}
        try:
            explain_db = DBConnection(self.config, self.database)
            explain_db.connect()
            for query in self.workload.queries:
                try:
                    rendered_sql = self.engine.render(query.sql)
                    explain_db.cursor.execute("EXPLAIN " + rendered_sql)
                    if explain_db.cursor.description:
                        cols = [desc[0]
                                for desc in explain_db.cursor.description]
                        rows = explain_db.cursor.fetchall()
                        col_widths = [len(c) for c in cols]
                        for row in rows:
                            for j, cell in enumerate(row):
                                col_widths[j] = max(
                                    col_widths[j], len(str(cell)))
                        header = " | ".join(
                            c.ljust(col_widths[j])
                            for j, c in enumerate(cols))
                        sep = "-+-".join(
                            "-" * col_widths[j]
                            for j in range(len(cols)))
                        lines = [header, sep]
                        for row in rows:
                            lines.append(" | ".join(
                                str(cell).ljust(col_widths[j])
                                for j, cell in enumerate(row)))
                        explain_plans[query.name] = "\n".join(lines)
                except Exception as e:
                    log.debug("[%s] EXPLAIN failed for %s: %s",
                              self.config.name, query.name, e)

                # EXPLAIN FORMAT=TREE (tdsql only)
                if self.config.name.lower() == "tdsql":
                    try:
                        rendered_sql = self.engine.render(query.sql)
                        explain_db.cursor.execute(
                            "EXPLAIN FORMAT=TREE " + rendered_sql)
                        tree_rows = explain_db.cursor.fetchall()
                        if tree_rows:
                            explain_tree_plans[query.name] = "\n".join(
                                str(row[0]) if row else ""
                                for row in tree_rows)
                    except Exception as e:
                        log.debug(
                            "[%s] EXPLAIN FORMAT=TREE failed for %s: %s",
                            self.config.name, query.name, e)
            explain_db.close()
        except Exception as e:
            log.debug("[%s] EXPLAIN connection failed: %s",
                      self.config.name, e)

        # Determine run duration
        duration = self.bench_cfg.duration
        if duration <= 0:
            # Estimate from iterations: run enough to execute ~iterations per query
            duration = 30.0  # default 30s

        concurrency = max(1, self.bench_cfg.concurrency)
        ramp_up = self.bench_cfg.ramp_up

        # Per-query latencies collected across all threads
        latency_lock = threading.Lock()
        per_query_latencies: Dict[str, List[float]] = {
            q.name: [] for q in self.workload.queries
        }
        per_query_errors: Dict[str, int] = {
            q.name: 0 for q in self.workload.queries
        }
        stop_event = threading.Event()
        total_executed = [0]  # mutable counter

        # Profiling setup — in concurrent mode, capture a single mixed
        # flame graph for the entire run duration.
        # Profiling is only supported for tdsql – skip for other DBMS.
        profiling = self.bench_cfg.profile
        profiler = None
        if profiling and self.config.name.lower() != "tdsql":
            log.info("[%s] Profiling skipped (only tdsql is profiled)",
                     self.config.name)
            profiling = False
        if profiling:
            from .flamegraph import PerfProfiler, check_perf_available
            ok, msg = check_perf_available()
            if not ok:
                log.warning("[%s] Profiling disabled: %s",
                            self.config.name, msg)
                profiling = False
            else:
                mysqld_pid = self._resolve_mysqld_pid()
                if not mysqld_pid:
                    log.warning(
                        "[%s] Profiling disabled: mysqld PID not found",
                        self.config.name)
                    profiling = False
                else:
                    if self.on_profile_start:
                        self.on_profile_start("concurrent_mix")
                    profiler = PerfProfiler(
                        mysqld_pid=mysqld_pid,
                        perf_freq=self.bench_cfg.perf_freq,
                    )

        def worker(thread_id: int, start_delay: float):
            """Worker thread that executes queries until stop_event."""
            if start_delay > 0:
                _time.sleep(start_delay)

            rng = random.Random()
            eng = TemplateEngine()  # thread-local engine

            db = DBConnection(self.config, self.database)
            try:
                db.connect()
                db.cursor.execute(f"USE `{self.database}`")
            except Exception as e:
                log.warning("[%s] Worker %d connect failed: %s",
                            self.config.name, thread_id, e)
                return

            try:
                while not stop_event.is_set():
                    query = rng.choice(weighted_pool)
                    rendered_sql = eng.render(query.sql)

                    t0 = _time.monotonic()
                    try:
                        db.cursor.execute(rendered_sql)
                        if db.cursor.description:
                            db.cursor.fetchall()
                    except Exception as e:
                        with latency_lock:
                            per_query_errors[query.name] += 1
                        if db._is_connection_lost(e):
                            if not db.reconnect():
                                break
                            try:
                                db.cursor.execute(
                                    f"USE `{self.database}`")
                            except Exception:
                                pass
                        continue
                    t1 = _time.monotonic()

                    lat_ms = (t1 - t0) * 1000.0
                    with latency_lock:
                        per_query_latencies[query.name].append(lat_ms)
                        total_executed[0] += 1

                    if self.on_progress:
                        self.on_progress(
                            query.name, total_executed[0], 0,
                            is_warmup=False,
                        )
            finally:
                db.close()

        # Launch threads with ramp-up
        exec_start = None  # set after ramp-up, before steady-state timing
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=concurrency) as pool:
            futures = []
            for i in range(concurrency):
                delay = (ramp_up / concurrency) * i if ramp_up > 0 else 0
                futures.append(pool.submit(worker, i, delay))

            # Wait for ramp-up to complete before starting profiler
            # so it only captures steady-state load
            if ramp_up > 0:
                _time.sleep(ramp_up)

            # Begin timing: only steady-state execution counts
            exec_start = _time.monotonic()

            # Start profiler after ramp-up, when all workers are active
            if profiler is not None:
                profiler.start()

            # Run for the remaining duration (subtract ramp-up already elapsed)
            remaining = max(0, duration - ramp_up) if ramp_up > 0 else duration
            _time.sleep(remaining)
            stop_event.set()

            # Wait for all threads to finish
            for f in futures:
                try:
                    f.result(timeout=10)
                except Exception as e:
                    log.warning("[%s] Worker error: %s",
                                self.config.name, e)

        overall_elapsed = _time.monotonic() - exec_start

        # Stop profiler immediately after all workers finish
        concurrent_fg_svg = ""
        if profiler is not None:
            fg_data = profiler.stop(query_name="concurrent_mix")
            if fg_data.svg_content:
                concurrent_fg_svg = fg_data.svg_content
            elif fg_data.error:
                log.warning("[%s] Flame graph: %s",
                            self.config.name, fg_data.error)
            profiler.cleanup()
            if self.on_profile_done:
                self.on_profile_done("concurrent_mix", fg_data.sample_count)

        # Compute stats per query
        for query in self.workload.queries:
            lats = per_query_latencies[query.name]
            errs = per_query_errors[query.name]
            stats = compute_stats(lats, errs, overall_elapsed, query.name,
                                  sql_template=query.sql)
            # In concurrent mode, all queries share the same flame graph
            stats.flamegraph_svg = concurrent_fg_svg
            stats.explain_plan = explain_plans.get(query.name, "")
            stats.explain_tree = explain_tree_plans.get(query.name, "")
            result.query_stats.append(stats)
            result.total_queries += len(lats) + errs
            result.total_errors += errs

        result.total_duration_s = overall_elapsed
        if overall_elapsed > 0:
            result.overall_qps = result.total_queries / overall_elapsed

        # Teardown (single connection)
        teardown_db = DBConnection(self.config, self.database)
        try:
            teardown_db.connect()
            for sql in self.workload.teardown:
                try:
                    teardown_db.cursor.execute(sql)
                except Exception as e:
                    log.warning("[%s] Teardown failed: %s — %s",
                                self.config.name, sql[:80], e)
            teardown_db.cleanup_database()
        except Exception:
            pass
        finally:
            teardown_db.close()

        return result


# ---------------------------------------------------------------------------
# Top-level benchmark runner
# ---------------------------------------------------------------------------

def run_benchmark(
    configs: List[DBMSConfig],
    workload: BenchWorkload,
    bench_cfg: BenchmarkConfig,
    database: str = "rosetta_bench",
    on_progress: Optional[Callable] = None,
    on_dbms_start: Optional[Callable] = None,
    on_dbms_done: Optional[Callable] = None,
    on_profile_start: Optional[Callable] = None,
    on_profile_done: Optional[Callable] = None,
    parallel_dbms: bool = False,
) -> BenchmarkResult:
    """Run benchmark on all DBMS targets and return aggregated results.

    Args:
        configs: List of DBMS connection configs.
        workload: The workload definition.
        bench_cfg: Benchmark runtime configuration.
        database: Database name for the benchmark.
        on_progress: Optional callback(dbms_name, query_name, iteration, total).
        on_dbms_start: Optional callback(dbms_name).
        on_dbms_done: Optional callback(dbms_name, dbms_result).
        on_profile_start: Optional callback(dbms_name, query_name).
        on_profile_done: Optional callback(dbms_name, query_name, sample_count).
        parallel_dbms: If True, run benchmarks on all DBMS targets in
            parallel (each DBMS gets its own thread and TemplateEngine).

    Returns:
        BenchmarkResult with results from all DBMS instances.
    """
    result = BenchmarkResult(
        workload_name=workload.name,
        mode=bench_cfg.mode,
        config=bench_cfg,
        timestamp=_time.strftime("%Y-%m-%d %H:%M:%S"),
    )

    # Apply query filter
    if bench_cfg.filter_queries:
        workload = BenchmarkLoader.filter_queries(
            workload, bench_cfg.filter_queries)

    def _run_single(config: DBMSConfig) -> DBMSBenchResult:
        """Run benchmark on a single DBMS target."""
        if on_dbms_start:
            on_dbms_start(config.name)

        # Each DBMS gets its own TemplateEngine for thread safety
        engine = TemplateEngine()

        def _progress_cb(qname, it, total, is_warmup=False,
                         _dbms=config.name):
            if on_progress:
                on_progress(_dbms, qname, it, total, is_warmup)

        def _profile_start_cb(qname, _dbms=config.name):
            if on_profile_start:
                on_profile_start(_dbms, qname)

        def _profile_done_cb(qname, samples, _dbms=config.name):
            if on_profile_done:
                on_profile_done(_dbms, qname, samples)

        if bench_cfg.mode == WorkloadMode.SERIAL:
            runner = SerialBenchmarkRunner(
                config, workload, bench_cfg, engine, database,
                on_progress=_progress_cb,
                on_profile_start=_profile_start_cb,
                on_profile_done=_profile_done_cb,
            )
        else:
            runner = ConcurrentBenchmarkRunner(
                config, workload, bench_cfg, engine, database,
                on_progress=_progress_cb,
                on_profile_start=_profile_start_cb,
                on_profile_done=_profile_done_cb,
            )

        dbms_result = runner.run()

        if on_dbms_done:
            on_dbms_done(config.name, dbms_result)

        return dbms_result

    if parallel_dbms and len(configs) > 1:
        # Run all DBMS targets in parallel
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(configs)) as pool:
            futures = {
                pool.submit(_run_single, c): c for c in configs
            }
            for fut in concurrent.futures.as_completed(futures):
                try:
                    dbms_result = fut.result()
                    result.dbms_results.append(dbms_result)
                except Exception as e:
                    config = futures[fut]
                    log.error("[%s] Benchmark failed: %s", config.name, e)
    else:
        # Sequential execution (original behavior)
        for config in configs:
            dbms_result = _run_single(config)
            result.dbms_results.append(dbms_result)

    # Ensure results are in the same order as configs for consistent reports
    name_order = {c.name: i for i, c in enumerate(configs)}
    result.dbms_results.sort(
        key=lambda r: name_order.get(r.dbms_name, 999))

    return result
