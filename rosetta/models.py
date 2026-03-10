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
    def pass_rate(self) -> float:
        effective = self.total_stmts - self.skipped
        if effective == 0:
            return 100.0
        return (self.matched / effective) * 100.0
