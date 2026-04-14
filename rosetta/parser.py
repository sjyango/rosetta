"""MTR .test file parser for Rosetta."""

import logging
import os
import re
from typing import List, Optional

from .models import Statement, StmtType
from .result_parser import ResultFileParser

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
    # Note: "eval" is NOT skipped — it's handled separately to extract SQL
])


class TestFileParser:
    """Parses an MTR-style .test file into a list of Statement objects."""

    # SQL patterns to skip (non-functional for cross-DBMS comparison)
    _SKIP_SQL_PATTERNS = [
        re.compile(r"^\s*SET\s+(default_)?storage_engine\s*=", re.IGNORECASE),
        re.compile(r"^\s*SET\s+max_parallel_degree\s*=", re.IGNORECASE),
    ]

    # SQL patterns to normalize (strip engine-specific clauses before execution)
    # NOTE: Only constant values are stripped here. Clauses with MTR variables
    # (e.g. ALGORITHM=$ALGORITHM_TYPE) cannot be resolved from .test files and
    # should be skipped entirely — the resolved SQL is available via .result files.
    _NORMALIZE_SQL_PATTERNS = [
        # Remove ENGINE=<name> from CREATE TABLE / ALTER TABLE statements
        (re.compile(r"\s*ENGINE\s*=\s*\w+", re.IGNORECASE), ""),
        # Remove DEFAULT CHARSET=<name> [COLLATE=<name>]
        (re.compile(r"\s*DEFAULT\s+CHARSET\s*=\s*\w+(\s+COLLATE\s*=\s*\w+)?", re.IGNORECASE), ""),
        # Remove AUTO_INCREMENT=<n>
        (re.compile(r"\s*AUTO_INCREMENT\s*=\s*\d+", re.IGNORECASE), ""),
        # Remove ROW_FORMAT=<name>
        (re.compile(r"\s*ROW_FORMAT\s*=\s*\w+", re.IGNORECASE), ""),
        # Remove STATS_PERSISTENT=<n>
        (re.compile(r"\s*STATS_PERSISTENT\s*=\s*\d+", re.IGNORECASE), ""),
        # Remove COMMENT=<string> on table level
        (re.compile(r"\s*COMMENT\s*=?\s*'[^']*'", re.IGNORECASE), ""),
        # Remove ALGORITHM=<name> from ALTER TABLE (constant values only)
        (re.compile(r"\s*,?\s*ALGORITHM\s*=\s*\w+", re.IGNORECASE), ""),
        # Remove LOCK=<name> from ALTER TABLE (constant values only)
        (re.compile(r"\s*,?\s*LOCK\s*=\s*\w+", re.IGNORECASE), ""),
    ]

    # SQL patterns that contain MTR variables — these cannot be resolved from
    # .test files and should be skipped (use .result files instead)
    _SKIP_VARIABLE_PATTERNS = [
        re.compile(r"\$\w+", re.IGNORECASE),  # Any $VARIABLE reference
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

    def _has_mtr_variable(self, sql: str) -> bool:
        """Check if SQL contains MTR variable references ($VAR).

        SQL with MTR variables cannot be resolved from .test files —
        the actual values are only available in .result files.
        """
        for pat in self._SKIP_VARIABLE_PATTERNS:
            if pat.search(sql):
                return True
        return False

    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL by removing engine-specific clauses.

        Strips ENGINE=, CHARSET=, ALGORITHM=, LOCK=, etc. so that
        the same SQL can run across different DBMS implementations.
        Also removes trailing commas before closing parentheses and
        collapses multiple spaces.
        """
        s = sql
        for pattern, replacement in self._NORMALIZE_SQL_PATTERNS:
            s = pattern.sub(replacement, s)
        # Clean up trailing commas before ) in CREATE TABLE
        s = re.compile(r",\s*\)", re.IGNORECASE).sub(")", s)
        # Collapse multiple spaces
        s = re.compile(r"  +").sub(" ", s)
        return s.strip()

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

    def _collect_brace_block(self, lines: list, idx: int,
                             current_stripped: str):
        """Collect SQL statements inside a brace-delimited block.

        Instead of skipping if/while blocks entirely, we extract the
        SQL statements within them so they get executed.  Nested
        if/while blocks are flattened (all branches are collected).

        Returns (updated_idx, inner_lines) where inner_lines is a list
        of raw line strings for the content inside the braces.
        The opening if/while line and braces themselves are excluded.
        """
        # Find the opening brace position in current_stripped
        brace_pos = current_stripped.find("{")
        if brace_pos >= 0:
            # There may be content after { on the same line
            after_brace = current_stripped[brace_pos + 1:].strip()
            depth = current_stripped.count("{") - current_stripped.count("}")
            inner_lines = []
            if after_brace and after_brace != "}":
                inner_lines.append(after_brace)
            # Check if block closes on same line
            if depth <= 0:
                return idx, inner_lines
        else:
            # Brace is on a subsequent line, skip until we find it
            inner_lines = []
            depth = 0
            found_brace = False
            while idx < len(lines):
                ln = lines[idx].rstrip("\n\r").strip()
                idx += 1
                if not ln or ln.startswith("#"):
                    continue
                bp = ln.find("{")
                if bp >= 0:
                    found_brace = True
                    after = ln[bp + 1:].strip()
                    depth = ln.count("{") - ln.count("}")
                    if after and after != "}":
                        inner_lines.append(after)
                    if depth <= 0:
                        return idx, inner_lines
                    break
                # Lines before the brace are not SQL (e.g., else/elseif)
            if not found_brace:
                return idx, inner_lines

        # Collect lines until closing brace
        while depth > 0 and idx < len(lines):
            ln = lines[idx].rstrip("\n\r")
            stripped_ln = ln.strip()
            idx += 1
            depth += stripped_ln.count("{") - stripped_ln.count("}")
            if depth <= 0:
                # The closing } is on this line — include content before it
                close_pos = stripped_ln.rfind("}")
                before = stripped_ln[:close_pos].strip()
                if before:
                    inner_lines.append(before)
            else:
                inner_lines.append(ln)

        return idx, inner_lines

    @staticmethod
    def find_result_file(test_path: str) -> Optional[str]:
        """Find the .result file corresponding to a .test file.

        MTR stores results in an 'r/' subdirectory with the same stem.
        For example:
            suite/tdsql/t/foo.test  ->  suite/tdsql/r/foo.result

        If the test file is not in a 't/' directory, look for the
        .result file alongside the .test file.
        """
        if not test_path or not os.path.isfile(test_path):
            return None

        test_dir = os.path.dirname(test_path)
        test_stem = os.path.splitext(os.path.basename(test_path))[0]

        # Strategy 1: replace /t/ with /r/ in path
        if os.sep + "t" + os.sep in test_dir:
            result_dir = test_dir.replace(os.sep + "t" + os.sep,
                                          os.sep + "r" + os.sep)
            result_path = os.path.join(result_dir, test_stem + ".result")
            if os.path.isfile(result_path):
                return result_path

        # Strategy 2: look in r/ subdirectory relative to parent
        parent_dir = os.path.dirname(test_dir)
        result_path = os.path.join(parent_dir, "r", test_stem + ".result")
        if os.path.isfile(result_path):
            return result_path

        # Strategy 3: look alongside the .test file
        result_path = os.path.join(test_dir, test_stem + ".result")
        if os.path.isfile(result_path):
            return result_path

        return None

    def parse(self, prefer_result: bool = True) -> List[Statement]:
        """Parse the test file and return list of statements.

        If *prefer_result* is True (default), and a corresponding .result
        file exists, the .result file is parsed instead.  This provides
        SQL with all MTR variables expanded and if/while branches resolved,
        which is much more reliable for cross-DBMS execution.

        Falls back to .test parsing if no .result file is found.
        """
        if prefer_result:
            result_path = self.find_result_file(self.filepath)
            if result_path:
                log.info("Using .result file instead of .test: %s",
                         result_path)
                parser = ResultFileParser(result_path)
                self.statements = parser.parse()
                return self.statements

        self._parse_file(self.filepath)
        return self.statements

    @classmethod
    def parse_text(cls, text: str) -> List[Statement]:
        """Parse raw MTR/SQL text (not from a file) and return SQL statements.

        This is useful for the Playground where input comes from the UI
        rather than a .test file.  It reuses the full MTR directive
        filtering and multi-line SQL collection logic.
        """
        parser = cls.__new__(cls)
        parser.filepath = "<text>"
        parser.statements = []
        parser._delimiter = ";"
        parser.mysql_test_dir = None
        parser._included = set()
        lines = [ln + "\n" for ln in text.splitlines()]
        parser._parse_lines(lines)
        return [s for s in parser.statements if s.stmt_type == StmtType.SQL]

    def _parse_file(self, filepath: str):
        """Parse a single file, handling --source recursion."""
        abs_path = os.path.abspath(filepath)
        if abs_path in self._included:
            return
        self._included.add(abs_path)

        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()
        self._parse_lines(lines)

    def _parse_lines(self, lines: list):
        """Core parsing logic shared by file-based and text-based parsing."""
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

                if directive == "eval":
                    # --eval means "execute the following SQL"
                    clean = self._strip_line_comment(stripped)
                    while (not self._ends_with_delimiter(clean)
                           and idx < len(lines)):
                        next_line = lines[idx].rstrip("\n\r")
                        idx += 1
                        clean = self._strip_line_comment(next_line.strip())
                    sql_after_eval = arg.strip()
                    if not self._ends_with_delimiter(sql_after_eval):
                        # arg might not include the full SQL, re-collect
                        sql_after_eval = clean[2:].strip()  # strip --
                        m = re.match(r"eval\s+(.*)", sql_after_eval, re.IGNORECASE)
                        if m:
                            sql_after_eval = m.group(1).strip()
                    sql_after_eval = self._strip_delimiter(sql_after_eval)
                    sql_after_eval = self._normalize_sql(sql_after_eval)
                    if sql_after_eval:
                        if self._should_skip_sql(sql_after_eval):
                            self.statements.append(Statement(
                                stmt_type=StmtType.SKIP,
                                text=sql_after_eval,
                                line_no=line_no,
                            ))
                        elif self._has_mtr_variable(sql_after_eval):
                            self.statements.append(Statement(
                                stmt_type=StmtType.SKIP,
                                text=sql_after_eval,
                                line_no=line_no,
                            ))
                            log.debug("Skipping --eval SQL with MTR variable: %s",
                                      sql_after_eval[:80])
                        else:
                            self.statements.append(Statement(
                                stmt_type=StmtType.SQL,
                                text=sql_after_eval,
                                line_no=line_no,
                                expected_error=pending_error,
                                sort_result=pending_sorted,
                            ))
                    pending_error = None
                    pending_sorted = False
                    continue

                if directive in SKIP_DIRECTIVES:
                    if directive in ("if", "while"):
                        idx, inner_lines = self._collect_brace_block(
                            lines, idx, stripped)
                        # Recursively parse inner lines to extract SQL
                        if inner_lines:
                            self._parse_lines(
                                [ln + "\n" for ln in inner_lines])
                    continue

                continue

            # Not a directive — treat as SQL or MTR-only command
            first_word = (stripped.split()[0].lower().rstrip(";")
                          if stripped else "")

            # Also detect if(...) and while(...) patterns
            is_if_or_while = False
            if re.match(r"if\s*\(", stripped, re.IGNORECASE):
                is_if_or_while = True
                first_word = "if"
            elif re.match(r"while\s*\(", stripped, re.IGNORECASE):
                is_if_or_while = True
                first_word = "while"

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
                    idx, inner_lines = self._collect_brace_block(
                        lines, idx, stripped)
                    # Recursively parse inner lines to extract SQL
                    if inner_lines:
                        self._parse_lines(
                            [ln + "\n" for ln in inner_lines])
                elif first_word == "eval":
                    # eval means "execute the following SQL"
                    # Collect the full eval statement (may span lines)
                    eval_sql_lines = [self._strip_line_comment(stripped[len("eval"):].strip())]
                    clean = self._strip_line_comment(stripped)
                    while (not self._ends_with_delimiter(clean)
                           and idx < len(lines)):
                        next_line = lines[idx].rstrip("\n\r")
                        idx += 1
                        eval_sql_lines.append(self._strip_line_comment(next_line))
                        clean = self._strip_line_comment(next_line.strip())
                    sql_after_eval = "\n".join(eval_sql_lines).strip()
                    sql_after_eval = self._strip_delimiter(sql_after_eval)
                    sql_after_eval = self._normalize_sql(sql_after_eval)
                    if sql_after_eval:
                        if self._should_skip_sql(sql_after_eval):
                            self.statements.append(Statement(
                                stmt_type=StmtType.SKIP,
                                text=sql_after_eval,
                                line_no=line_no,
                            ))
                        elif self._has_mtr_variable(sql_after_eval):
                            self.statements.append(Statement(
                                stmt_type=StmtType.SKIP,
                                text=sql_after_eval,
                                line_no=line_no,
                            ))
                            log.debug("Skipping eval SQL with MTR variable: %s",
                                      sql_after_eval[:80])
                        else:
                            self.statements.append(Statement(
                                stmt_type=StmtType.SQL,
                                text=sql_after_eval,
                                line_no=line_no,
                                expected_error=pending_error,
                                sort_result=pending_sorted,
                            ))
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
            sql_text = self._normalize_sql(sql_text)

            if not sql_text:
                continue

            if self._should_skip_sql(sql_text):
                self.statements.append(Statement(
                    stmt_type=StmtType.SKIP,
                    text=sql_text,
                    line_no=line_no,
                ))
                pending_error = None
                pending_sorted = False
                continue

            if self._has_mtr_variable(sql_text):
                self.statements.append(Statement(
                    stmt_type=StmtType.SKIP,
                    text=sql_text,
                    line_no=line_no,
                ))
                log.debug("Skipping SQL with MTR variable (use .result instead): %s",
                          sql_text[:80])
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
