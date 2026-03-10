"""MTR .test file parser for Rosetta."""

import logging
import os
import re
from typing import List, Optional

from .models import Statement, StmtType

log = logging.getLogger("rosetta")

# MTR directives that we skip (they control mysqltest behaviour, not SQL)
SKIP_DIRECTIVES = frozenset([
    "source", "skip", "skip_if_hypergraph", "require",
    "disable_warnings", "enable_warnings", "disable_query_log",
    "enable_query_log", "disable_result_log", "enable_result_log",
    "let", "inc", "dec", "if", "while", "end",
    "die", "exit", "sleep", "real_sleep",
    "replace_column", "replace_regex",
    "connect", "disconnect", "connection",
    "remove_file", "write_file", "append_file",
    "copy_file", "move_file", "file_exists",
    "mkdir", "rmdir", "list_files",
    "exec", "system",
    "perl", "end",
    "result_format",
])


class TestFileParser:
    """Parses an MTR-style .test file into a list of Statement objects."""

    # SQL patterns to skip (non-functional for cross-DBMS comparison)
    _SKIP_SQL_PATTERNS = [
        re.compile(r"^\s*SET\s+(default_)?storage_engine\s*=", re.IGNORECASE),
        re.compile(r"^\s*SET\s+max_parallel_degree\s*=", re.IGNORECASE),
    ]

    def __init__(self, filepath: str, mysql_test_dir: Optional[str] = None):
        self.filepath = filepath
        self.statements: List[Statement] = []
        self._delimiter = ";"
        if mysql_test_dir:
            self.mysql_test_dir = mysql_test_dir
        else:
            path = os.path.abspath(filepath)
            idx_mt = path.find("mysql-test")
            if idx_mt >= 0:
                self.mysql_test_dir = path[:idx_mt + len("mysql-test")]
            else:
                self.mysql_test_dir = None
        self._included: set = set()

    def _resolve_source_path(self, arg: str) -> Optional[str]:
        """Resolve a --source argument to an absolute file path."""
        if not self.mysql_test_dir:
            return None
        if arg.startswith("include/"):
            return None
        resolved = os.path.join(self.mysql_test_dir, arg)
        if os.path.isfile(resolved):
            return resolved
        resolved = os.path.join(os.path.dirname(self.filepath), arg)
        if os.path.isfile(resolved):
            return resolved
        return None

    def _should_skip_sql(self, sql: str) -> bool:
        """Check if this SQL should be skipped."""
        for pat in self._SKIP_SQL_PATTERNS:
            if pat.match(sql):
                return True
        return False

    @staticmethod
    def _strip_line_comment(line: str) -> str:
        """Strip trailing # comment from a line, respecting quoted strings."""
        in_single = False
        in_double = False
        i = 0
        while i < len(line):
            ch = line[i]
            if ch == '\\' and i + 1 < len(line):
                i += 2
                continue
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            elif ch == '#' and not in_single and not in_double:
                return line[:i].rstrip()
            i += 1
        return line

    def _ends_with_delimiter(self, stripped: str) -> bool:
        """Check if a stripped line ends with the current delimiter."""
        return stripped.endswith(self._delimiter)

    def _strip_delimiter(self, sql_text: str) -> str:
        """Remove the trailing delimiter from collected SQL text."""
        if self._delimiter and sql_text.endswith(self._delimiter):
            return sql_text[:-len(self._delimiter)].strip()
        return sql_text

    def _parse_delimiter(self, arg: str) -> None:
        """Parse a delimiter directive and update self._delimiter."""
        old_delim = self._delimiter
        if arg.endswith(old_delim):
            new_delim = arg[:-len(old_delim)].strip()
        else:
            new_delim = arg.rstrip(";").strip()
        if new_delim:
            self._delimiter = new_delim
            log.debug("Delimiter changed: %r -> %r", old_delim, new_delim)

    @staticmethod
    def _skip_brace_block(lines: list, idx: int,
                          current_stripped: str) -> int:
        """Skip a brace-delimited block: if/while { ... }.

        Returns the updated line index past the closing brace.
        """
        depth = current_stripped.count("{") - current_stripped.count("}")
        if depth <= 0 and "{" not in current_stripped:
            while idx < len(lines):
                ln = lines[idx].rstrip("\n\r").strip()
                idx += 1
                if not ln or ln.startswith("#"):
                    continue
                depth += ln.count("{") - ln.count("}")
                if depth <= 0 and "{" in ln:
                    return idx
                if depth > 0:
                    break
            else:
                return idx

        while depth > 0 and idx < len(lines):
            ln = lines[idx].rstrip("\n\r").strip()
            idx += 1
            depth += ln.count("{") - ln.count("}")

        return idx

    def parse(self) -> List[Statement]:
        """Parse the test file and return list of statements."""
        self._parse_file(self.filepath)
        return self.statements

    def _parse_file(self, filepath: str):
        """Parse a single file, handling --source recursion."""
        abs_path = os.path.abspath(filepath)
        if abs_path in self._included:
            return
        self._included.add(abs_path)

        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()

        idx = 0
        pending_error: Optional[str] = None
        pending_sorted: bool = False

        while idx < len(lines):
            line = lines[idx].rstrip("\n\r")
            line_no = idx + 1
            stripped = line.strip()
            idx += 1

            if not stripped or stripped.startswith("#"):
                continue

            if stripped == "}" or stripped == "};":
                continue

            # Handle MTR directives (--xxx ...)
            if stripped.startswith("--"):
                directive_line = stripped[2:].strip()
                m = re.match(r"(\w+)\s*(.*)", directive_line)
                if not m:
                    continue
                directive = m.group(1).lower()
                arg = m.group(2).strip()

                if directive == "echo":
                    self.statements.append(Statement(
                        stmt_type=StmtType.ECHO,
                        text=arg,
                        line_no=line_no,
                    ))
                    continue

                if directive == "error":
                    pending_error = arg
                    continue

                if directive == "sorted_result":
                    pending_sorted = True
                    continue

                if directive == "source":
                    inc_path = self._resolve_source_path(arg)
                    if inc_path:
                        log.info("Including source: %s", arg)
                        self._parse_file(inc_path)
                    continue

                if directive == "delimiter":
                    self._parse_delimiter(arg)
                    continue

                if directive in SKIP_DIRECTIVES:
                    if directive in ("if", "while"):
                        idx = self._skip_brace_block(lines, idx, stripped)
                    continue

                continue

            # Not a directive — treat as SQL or MTR-only command
            first_word = (stripped.split()[0].lower().rstrip(";")
                          if stripped else "")
            if first_word in ("let", "eval", "if", "while", "end",
                              "inc", "dec", "source",
                              "perl", "die", "exit",
                              "connect", "connection", "disconnect",
                              "disable_warnings", "enable_warnings",
                              "disable_query_log", "enable_query_log",
                              "disable_result_log", "enable_result_log",
                              "replace_column", "replace_regex",
                              "remove_file", "write_file", "append_file",
                              "copy_file", "move_file", "file_exists",
                              "mkdir", "rmdir", "list_files",
                              "exec", "system", "result_format",
                              "skip", "require", "sleep", "real_sleep"):
                if first_word in ("if", "while"):
                    idx = self._skip_brace_block(lines, idx, stripped)
                else:
                    clean = self._strip_line_comment(stripped)
                    while (not self._ends_with_delimiter(clean)
                           and idx < len(lines)):
                        stripped = lines[idx].rstrip("\n\r").strip()
                        idx += 1
                        clean = self._strip_line_comment(stripped)
                pending_error = None
                pending_sorted = False
                continue

            # Handle bare 'delimiter' without '--' prefix
            if first_word == "delimiter":
                rest = stripped[len("delimiter"):].strip()
                self._parse_delimiter(rest)
                continue

            # Collect multi-line SQL (terminated by current delimiter)
            sql_lines = [self._strip_line_comment(line)]
            clean = self._strip_line_comment(stripped)
            while (not self._ends_with_delimiter(clean)
                   and idx < len(lines)):
                next_line = lines[idx].rstrip("\n\r")
                idx += 1
                sql_lines.append(self._strip_line_comment(next_line))
                clean = self._strip_line_comment(next_line.strip())

            sql_text = "\n".join(sql_lines).strip()
            sql_text = self._strip_delimiter(sql_text)

            if not sql_text:
                continue

            if self._should_skip_sql(sql_text):
                pending_error = None
                pending_sorted = False
                continue

            stmt = Statement(
                stmt_type=StmtType.SQL,
                text=sql_text,
                line_no=line_no,
                expected_error=pending_error,
                sort_result=pending_sorted,
            )
            self.statements.append(stmt)

            pending_error = None
            pending_sorted = False

        return self.statements
