"""Data models for Rosetta cross-DBMS testing tool."""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple


class StmtType(Enum):
    """Type of a parsed statement from a .test file."""
    SQL = auto()
    ECHO = auto()
    ERROR = auto()
    SORTED_RESULT = auto()
    SKIP = auto()


@dataclass
class Statement:
    """A parsed statement from a .test file."""
    stmt_type: StmtType
    text: str
    line_no: int
    expected_error: Optional[str] = None
    sort_result: bool = False


@dataclass
class StmtResult:
    """Result of executing a single statement."""
    stmt: Statement
    columns: Optional[List[str]] = None
    rows: Optional[List[Tuple]] = None
    error: Optional[str] = None
    warnings: Optional[List[str]] = None
    affected_rows: int = 0
    output_text: str = ""


@dataclass
class DBMSConfig:
    """Configuration for a single DBMS connection."""
    name: str
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    driver: str = "pymysql"
    skip_patterns: List[str] = field(default_factory=list)
    init_sql: List[str] = field(default_factory=list)
    skip_explain: bool = False
    skip_analyze: bool = False
    skip_show_create: bool = False
    enabled: bool = True
    restart_cmd: str = ""


@dataclass
class CompareResult:
    """Result of comparing two DBMS outputs."""
    dbms_a: str
    dbms_b: str
    total_stmts: int = 0
    matched: int = 0
    mismatched: int = 0
    skipped: int = 0
    diffs: List[Dict] = field(default_factory=list)

    @property
    def whitelisted(self) -> int:
        """Count of diffs that are whitelisted."""
        return sum(1 for d in self.diffs if d.get("whitelisted"))

    @property
    def bug_marked(self) -> int:
        """Count of diffs that are marked as bugs."""
        return sum(1 for d in self.diffs if d.get("bug_marked"))

    @property
    def effective_mismatched(self) -> int:
        """Mismatches excluding whitelisted diffs."""
        return self.mismatched - self.whitelisted

    @property
    def pass_rate(self) -> float:
        effective = self.total_stmts - self.skipped
        if effective == 0:
            return 100.0
        return ((self.matched + self.whitelisted) / effective) * 100.0


# ---------------------------------------------------------------------------
# Benchmark data models
# ---------------------------------------------------------------------------

class WorkloadMode(Enum):
    """Benchmark workload execution mode."""
    SERIAL = auto()
    CONCURRENT = auto()


@dataclass
class BenchQuery:
    """A single query in a benchmark workload."""
    name: str
    sql: str
    weight: int = 1


@dataclass
class BenchWorkload:
    """A complete benchmark workload definition."""
    name: str = "custom"
    setup: List[str] = field(default_factory=list)
    queries: List[BenchQuery] = field(default_factory=list)
    teardown: List[str] = field(default_factory=list)


@dataclass
class BenchmarkConfig:
    """Runtime configuration for a benchmark run."""
    mode: WorkloadMode = WorkloadMode.SERIAL
    iterations: int = 100
    warmup: int = 5
    concurrency: int = 1
    duration: float = 0.0  # seconds; 0 means use iterations
    ramp_up: float = 0.0   # seconds to ramp up threads
    filter_queries: List[str] = field(default_factory=list)  # --bench-filter
    profile: bool = True    # --profile: enable perf flame graph capture
    perf_freq: int = 99     # perf sampling frequency (Hz)


@dataclass
class QueryLatencyStats:
    """Latency statistics for a single query."""
    query_name: str
    sql_template: str = ""
    total_executions: int = 0
    total_errors: int = 0
    latencies_ms: List[float] = field(default_factory=list)
    min_ms: float = 0.0
    max_ms: float = 0.0
    avg_ms: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    qps: float = 0.0
    flamegraph_svg: str = ""  # SVG flame graph content (if profiling enabled)
    explain_plan: str = ""    # EXPLAIN output (text format)
    explain_tree: str = ""    # EXPLAIN FORMAT=TREE output (tree format)


@dataclass
class DBMSBenchResult:
    """Benchmark results for a single DBMS."""
    dbms_name: str
    query_stats: List[QueryLatencyStats] = field(default_factory=list)
    total_duration_s: float = 0.0
    total_queries: int = 0
    total_errors: int = 0
    overall_qps: float = 0.0


@dataclass
class BenchmarkResult:
    """Complete benchmark result across all DBMS instances."""
    workload_name: str
    mode: WorkloadMode
    config: BenchmarkConfig
    dbms_results: List[DBMSBenchResult] = field(default_factory=list)
    timestamp: str = ""
