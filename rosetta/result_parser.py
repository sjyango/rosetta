"""MTR .result file parser for Rosetta.

Parses a MySQL MTR .result file to extract the SQL statements that
were actually executed (with all MTR variables expanded and all
if/while branches resolved).  This provides a "ground truth" SQL
sequence that can be replayed on other DBMS without dealing with
MTR-specific syntax.

The parser uses heuristics to distinguish SQL statements from query
output.  Lines starting with known SQL keywords followed by appropriate
syntax are treated as SQL; everything else is output.
"""

import logging
import os
import re
from typing import List, Optional

from .models import Statement, StmtType

log = logging.getLogger("rosetta")

# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

# Lines that start with a known SQL keyword (case-insensitive).
# These are the *executable* statements we want to extract.
_RE_SQL_START = re.compile(
    r"^(CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|SELECT|REPLACE|TRUNCATE|"
    r"SET|SHOW|EXPLAIN|ANALYZE|BEGIN|START|COMMIT|ROLLBACK|SAVEPOINT|"
    r"GRANT|REVOKE|FLUSH|RENAME|LOCK|UNLOCK|USE|DESCRIBE|DESC|CALL|"
    r"LOAD|OPTIMIZE|REPAIR|CACHE|CHECK|BACKUP|RESTORE|HANDLER|"
    r"PREPARE|EXECUTE|DEALLOCATE|INSTALL|UNINSTALL|SHUTDOWN|KILL|"
    r"CHANGE|RESET|PURGE|BINLOG|MASTER|SLAVE|START|STOP|HELP)"
    r"\b",
    re.IGNORECASE,
)

# Lines that are clearly *output*, not SQL.
_RE_OUTPUT_LINE = re.compile(
    r"^("
    r"Table\s+Create Table|"     # SHOW CREATE TABLE header
    r"Variable_name\s+Value|"    # SHOW VARIABLES header
    r"Database|"                 # SHOW DATABASES header
    r"Warning\s+\d+|"           # Warning lines
    r"Warnings:|"               # Warnings header
    r"ERROR\b|"                 # Error output
    r"Empty set|"               # Empty result
    r"Query OK|"                # DML result
    r"Rows matched|"            # UPDATE result
    r"Schema changed|"          # DDL result
    r"^pk\b|^id\b|^a\b|^b\b|^c\b|^c1\b|^c2\b|^c3\b"  # Common column names
    r")",
    re.IGNORECASE,
)

# SQL patterns to normalize (strip engine-specific clauses before execution)
# .result files have MTR variables already expanded, so only constant values
# need to be stripped here.
_NORMALIZE_PATTERNS = [
    (re.compile(r"\s*ENGINE\s*=\s*\w+", re.IGNORECASE), ""),
    (re.compile(r"\s*DEFAULT\s+CHARSET\s*=\s*\w+(\s+COLLATE\s*=\s*\w+)?",
                re.IGNORECASE), ""),
    (re.compile(r"\s*AUTO_INCREMENT\s*=\s*\d+", re.IGNORECASE), ""),
    (re.compile(r"\s*ROW_FORMAT\s*=\s*\w+", re.IGNORECASE), ""),
    (re.compile(r"\s*STATS_PERSISTENT\s*=\s*\d+", re.IGNORECASE), ""),
]

# SQL patterns to skip entirely (truly non-functional across all DBMS)
# NOTE: DBMS-specific SET statements (e.g. SET tdsql_*) should NOT be skipped
# here — they are valid SQL that affects behavior on specific DBMS.  They are
# handled by the executor's skip_patterns mechanism instead (configured per DBMS
# in ~/.rosetta/config.json).
_SKIP_SQL_PATTERNS = [
    re.compile(r"^\s*SET\s+(default_)?storage_engine\s*=", re.IGNORECASE),
    re.compile(r"^\s*SET\s+max_parallel_degree\s*=", re.IGNORECASE),
]


def _normalize_sql(sql: str) -> str:
    """Normalize SQL by removing engine-specific clauses."""
    s = sql
    for pattern, replacement in _NORMALIZE_PATTERNS:
        s = pattern.sub(replacement, s)
    # Clean up trailing commas before )
    s = re.compile(r",\s*\)", re.IGNORECASE).sub(")", s)
    # Collapse multiple spaces
    s = re.compile(r"  +").sub(" ", s)
    return s.strip()


def _should_skip_sql(sql: str) -> bool:
    """Check if this SQL should be skipped."""
    for pat in _SKIP_SQL_PATTERNS:
        if pat.match(sql):
            return True
    return False


class ResultFileParser:
    """Parses an MTR .result file and extracts SQL statements.

    The .result file contains both SQL statements and their output.
    This parser uses heuristics to distinguish between them:

    1. Lines starting with known SQL keywords are candidates for SQL.
    2. Lines that look like table output (column headers, data rows,
       Warning/ERROR lines) are marked as output.
    3. Multi-line SQL is detected by checking if a SQL-like line
       doesn't end with a semicolon.
    """

    # Context state for distinguishing SQL from output in tricky cases
    # After a SQL statement, the next non-empty lines are output until
    # we see another SQL-starting line.

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.statements: List[Statement] = []

    def parse(self) -> List[Statement]:
        """Parse the result file and return list of SQL Statement objects."""
        with open(self.filepath, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return self._parse_lines(lines)

    @classmethod
    def parse_text(cls, text: str) -> List[Statement]:
        """Parse raw result text and return SQL statements."""
        parser = cls.__new__(cls)
        parser.filepath = "<text>"
        parser.statements = []
        lines = [ln + "\n" for ln in text.splitlines()]
        return parser._parse_lines(lines)

    def _parse_lines(self, lines: list) -> List[Statement]:
        """Core parsing logic."""
        idx = 0
        state = "SEEK_SQL"  # SEEK_SQL -> IN_SQL -> SEEK_SQL

        sql_lines: List[str] = []
        sql_start_line = 0

        while idx < len(lines):
            line = lines[idx].rstrip("\n\r")
            stripped = line.strip()
            idx += 1

            if not stripped or stripped.startswith("#"):
                # Comment or blank line — if we're in a multi-line SQL,
                # blank line may terminate it
                if state == "IN_SQL" and not stripped:
                    # Blank line inside SQL — likely output separator
                    self._flush_sql(sql_lines, sql_start_line)
                    sql_lines = []
                    state = "SEEK_SQL"
                continue

            if state == "SEEK_SQL":
                # Look for the start of a SQL statement
                if self._is_sql_start(stripped):
                    sql_lines = [stripped]
                    sql_start_line = idx
                    if stripped.endswith(";"):
                        self._flush_sql(sql_lines, sql_start_line)
                        sql_lines = []
                    else:
                        state = "IN_SQL"

            elif state == "IN_SQL":
                # Collecting multi-line SQL
                # Check if this line looks like it's output instead
                if self._is_output_line(stripped):
                    # The previous SQL was likely terminated without ;
                    self._flush_sql(sql_lines, sql_start_line)
                    sql_lines = []
                    state = "SEEK_SQL"
                    # Re-check this line as potential SQL start
                    if self._is_sql_start(stripped):
                        sql_lines = [stripped]
                        sql_start_line = idx
                        if stripped.endswith(";"):
                            self._flush_sql(sql_lines, sql_start_line)
                            sql_lines = []
                        else:
                            state = "IN_SQL"
                else:
                    sql_lines.append(stripped)
                    if stripped.endswith(";"):
                        self._flush_sql(sql_lines, sql_start_line)
                        sql_lines = []
                        state = "SEEK_SQL"

        # Flush any remaining SQL
        if sql_lines:
            self._flush_sql(sql_lines, sql_start_line)

        return self.statements

    def _is_sql_start(self, line: str) -> bool:
        """Check if a line starts with a SQL keyword."""
        # Quick reject: lines starting with special chars are not SQL
        if line.startswith(('"', "'", "(", "-", "*", "+", ">", "<", "|")):
            return False
        # Lines that are just echo output (quoted strings)
        if line.startswith('"') and line.endswith('"'):
            return False
        return bool(_RE_SQL_START.match(line))

    def _is_output_line(self, line: str) -> bool:
        """Check if a line is query output rather than SQL."""
        # Common output patterns
        if _RE_OUTPUT_LINE.match(line):
            return True
        # Lines that contain tab characters are typically table output
        if "\t" in line and not line.lower().startswith(("select", "insert", "create")):
            return True
        # Lines that look like column definitions in SHOW CREATE TABLE output
        if line.startswith("  `"):
            return True
        # Lines that are just a table name followed by tab + CREATE TABLE
        if re.match(r"^\w+\tCREATE TABLE", line):
            return True
        return False

    def _flush_sql(self, sql_lines: List[str], line_no: int) -> None:
        """Process collected SQL lines and add to statements if valid."""
        if not sql_lines:
            return

        sql_text = " ".join(sql_lines)
        # Remove trailing semicolon
        if sql_text.endswith(";"):
            sql_text = sql_text[:-1].strip()

        if not sql_text:
            return

        # Normalize (remove ENGINE=, ALGORITHM=, etc.)
        sql_text = _normalize_sql(sql_text)

        if not sql_text:
            return

        # Mark non-functional SQL as SKIP (visible in reports but not executed)
        if _should_skip_sql(sql_text):
            self.statements.append(Statement(
                stmt_type=StmtType.SKIP,
                text=sql_text,
                line_no=line_no,
            ))
            return

        self.statements.append(Statement(
            stmt_type=StmtType.SQL,
            text=sql_text,
            line_no=line_no,
        ))
