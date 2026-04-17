"""Result processing for MTR test execution.

Implements result formatting and transformation directives:
  - --sorted_result / --partially_sorted_result
  - --replace_column
  - --replace_result
  - --replace_regex
  - --replace_numeric_round
  - --vertical_results / --horizontal_results
  - --lowercase_result
  - --disable_result_log / --enable_result_log
  - --disable_query_log / --enable_query_log
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .nodes import (
    ReplaceColumnSpec,
    ReplaceRegexSpec,
    ReplaceResultSpec,
)


@dataclass
class QueryResult:
    """Result of executing a SQL query."""
    columns: List[str] = field(default_factory=list)
    rows: List[Tuple] = field(default_factory=list)
    affected_rows: int = 0
    error_code: int = 0
    sqlstate: str = ""
    error_message: str = ""
    warnings: List[str] = field(default_factory=list)
    output_text: str = ""  # Raw text output (for non-tabular results)
    has_result_set: bool = False
    is_error: bool = False


class ResultProcessor:
    """Process and format query results according to MTR directives.

    This implements the result processing pipeline from mysqltest.cc:
    1. Format the result (vertical/horizontal)
    2. Apply replace_result substitutions
    3. Apply replace_column substitutions
    4. Apply replace_regex substitutions
    5. Apply replace_numeric_round
    6. Apply sorted_result
    7. Apply lowercase_result
    """

    def __init__(self):
        # Formatting state
        self.display_vertical: bool = False
        self.display_sorted: bool = False
        self.sort_start_column: int = 0
        self.display_lowercase: bool = False

        # Logging state
        self.disable_query_log: bool = False
        self.disable_result_log: bool = False
        self.disable_warnings: bool = False
        self.disable_info: bool = False
        self.disable_connect_log: bool = False
        self.display_metadata: bool = False
        self.display_info: bool = False

        # Replace state (one-shot, cleared after each query)
        self.replace_columns: List[ReplaceColumnSpec] = []
        self.replace_results: List[ReplaceResultSpec] = []
        self.replace_regexes: List[ReplaceRegexSpec] = []
        self.numeric_round_precision: int = -1

        # Result format version
        self.result_format_version: int = 2

    def format_result(self, result: QueryResult) -> str:
        """Format a QueryResult into the text representation expected by .result files.

        This is the Python equivalent of the display_result logic in mysqltest.cc.

        Args:
            result: The query result to format.

        Returns:
            Formatted string matching MTR .result file format.
        """
        if result.is_error:
            return self._format_error(result)

        if not result.has_result_set:
            # DML/DDL with affected rows
            parts = []
            if not self.disable_result_log:
                if result.affected_rows >= 0:
                    parts.append(f"affected rows: {result.affected_rows}")
            return "\n".join(parts)

        # Format the result set
        if self.display_vertical:
            text = self._format_vertical(result)
        else:
            text = self._format_horizontal(result)

        # Apply transformations
        text = self._apply_replace_result(text)
        text = self._apply_replace_column(text, result)
        text = self._apply_replace_regex(text)
        text = self._apply_numeric_round(text)
        text = self._apply_lowercase(text)

        return text

    def format_query_log(self, sql: str, line_no: int = 0) -> str:
        """Format a SQL statement for the result file (query logging).

        In MTR, the SQL statement is printed before its result,
        unless query logging is disabled.

        When line_no > 0, a [Lnnn] tag is prepended for cross-DBMS
        block alignment in the comparator.
        """
        if self.disable_query_log:
            return ""
        if line_no > 0:
            return f"[L{line_no}] {sql};\n"
        return sql + ";\n"

    def reset_one_shot(self) -> None:
        """Reset one-shot directives after a query has been processed.

        This is called after each query execution, matching mysqltest.cc's
        behavior where sorted_result, replace_column, replace_regex, etc.
        are only applied to the next query.
        """
        self.display_sorted = False
        self.sort_start_column = 0
        self.display_lowercase = False
        self.replace_columns = []
        self.replace_results = []
        self.replace_regexes = []
        self.numeric_round_precision = -1

    # -----------------------------------------------------------------------
    # Formatting methods
    # -----------------------------------------------------------------------

    def _format_horizontal(self, result: QueryResult) -> str:
        """Format result in horizontal (tabular) format.

        This matches the default MTR output format:
        ```
        column1   column2   column3
        value1    value2    value3
        ```
        """
        if not result.columns:
            return result.output_text

        lines = []

        # Header line
        header = "\t".join(str(c) for c in result.columns)
        lines.append(header)

        # Sort rows if requested
        rows = result.rows
        if self.display_sorted:
            rows = self._sort_rows(rows, self.sort_start_column)

        # Data rows
        for row in rows:
            line = "\t".join(str(v) if v is not None else "NULL" for v in row)
            lines.append(line)

        return "\n".join(lines) + "\n"

    def _format_vertical(self, result: QueryResult) -> str:
        """Format result in vertical format.

        This matches MTR's --vertical_results output:
        ```
        column1: value1
        column2: value2
        column3: value3

        column1: value4
        ...
        ```
        """
        if not result.columns:
            return result.output_text

        lines = []

        # Sort rows if requested
        rows = result.rows
        if self.display_sorted:
            rows = self._sort_rows(rows, self.sort_start_column)

        for i, row in enumerate(rows):
            if i > 0:
                lines.append("")  # Blank line between rows
            for j, col in enumerate(result.columns):
                val = str(row[j]) if row[j] is not None else "NULL"
                lines.append(f"{col}: {val}")

        return "\n".join(lines) + "\n"

    def _format_error(self, result: QueryResult) -> str:
        """Format an error result.

        Matches mysqltest.cc's error output format:
        ```
        ERROR <error_code> (<sqlstate>): <error_message>
        ```
        """
        if result.sqlstate:
            return f"ERROR {result.error_code} ({result.sqlstate}): {result.error_message}"
        return f"ERROR {result.error_code}: {result.error_message}"

    # -----------------------------------------------------------------------
    # Sort methods
    # -----------------------------------------------------------------------

    def _sort_rows(self, rows: List[Tuple],
                   start_column: int = 0) -> List[Tuple]:
        """Sort result rows for --sorted_result.

        Rows are sorted lexicographically starting from the given column.
        """
        def sort_key(row):
            # Sort starting from start_column
            key_parts = []
            for i in range(start_column, len(row)):
                val = row[i]
                if val is None:
                    key_parts.append((0, ""))
                else:
                    s = str(val)
                    try:
                        # Try numeric sort for numbers
                        f = float(s)
                        key_parts.append((1, f))
                    except ValueError:
                        key_parts.append((2, s))
            return key_parts

        return sorted(rows, key=sort_key)

    # -----------------------------------------------------------------------
    # Replace methods
    # -----------------------------------------------------------------------

    def _apply_replace_result(self, text: str) -> str:
        """Apply --replace_result substitutions.

        --replace_result from_str to_str [from_str to_str ...]
        Replaces all occurrences of from_str with to_str in the result.
        """
        for spec in self.replace_results:
            text = text.replace(spec.from_str, spec.to_str)
        return text

    def _apply_replace_column(self, text: str,
                               result: QueryResult) -> str:
        """Apply --replace_column substitutions.

        --replace_column col_num replacement [col_num replacement ...]
        Replaces the value in the specified column with the replacement string.
        Column numbers are 1-based.
        """
        if not self.replace_columns or not result.columns:
            return text

        lines = text.split("\n")
        new_lines = []

        for line in lines:
            if not line:
                new_lines.append(line)
                continue

            # Split by tab (horizontal format)
            parts = line.split("\t")

            # Check if this is a data line (same number of fields as columns)
            if len(parts) == len(result.columns):
                for spec in self.replace_columns:
                    col_idx = spec.column_number - 1  # Convert to 0-based
                    if 0 <= col_idx < len(parts):
                        parts[col_idx] = spec.replacement
                new_lines.append("\t".join(parts))
            else:
                # Header or other line - don't modify
                new_lines.append(line)

        return "\n".join(new_lines)

    def _apply_replace_regex(self, text: str) -> str:
        """Apply --replace_regex substitutions.

        --replace_regex pattern replacement [pattern replacement ...]
        Applies regex pattern matching and replacement.
        """
        for spec in self.replace_regexes:
            try:
                text = re.sub(spec.pattern, spec.replacement, text)
            except re.error as e:
                # Log but don't crash on invalid regex
                pass
        return text

    def _apply_numeric_round(self, text: str) -> str:
        """Apply --replace_numeric_round substitutions.

        Rounds numeric values in the result to the specified precision.
        """
        if self.numeric_round_precision < 0:
            return text

        precision = self.numeric_round_precision

        def round_match(m):
            try:
                val = float(m.group(0))
                return f"{val:.{precision}f}"
            except ValueError:
                return m.group(0)

        # Match decimal numbers
        text = re.sub(
            r'-?\d+\.\d+',
            round_match,
            text
        )
        return text

    def _apply_lowercase(self, text: str) -> str:
        """Apply --lowercase_result.

        Lowercases the entire result output.
        """
        if self.display_lowercase:
            return text.lower()
        return text

    # -----------------------------------------------------------------------
    # Setters for directives
    # -----------------------------------------------------------------------

    def set_sorted_result(self, start_column: int = 0) -> None:
        """Set --sorted_result for the next query."""
        self.display_sorted = True
        self.sort_start_column = start_column

    def set_lowercase(self) -> None:
        """Set --lowercase_result for the next query."""
        self.display_lowercase = True

    def set_vertical(self, vertical: bool) -> None:
        """Set vertical/horizontal result display."""
        self.display_vertical = vertical

    def set_replace_column(self, specs: List[ReplaceColumnSpec]) -> None:
        """Set --replace_column for the next query."""
        self.replace_columns = specs

    def set_replace_result(self, specs: List[ReplaceResultSpec]) -> None:
        """Set --replace_result for the next query."""
        self.replace_results = specs

    def set_replace_regex(self, specs: List[ReplaceRegexSpec]) -> None:
        """Set --replace_regex for the next query."""
        self.replace_regexes = specs

    def set_numeric_round(self, precision: int) -> None:
        """Set --replace_numeric_round for the next query."""
        self.numeric_round_precision = precision
