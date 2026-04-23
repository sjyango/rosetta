"""MTR .test file parser.

A complete Python reimplementation of the mysqltest parser from
/data/workspace/SQLEngine/client/mysqltest.cc, supporting ALL MTR
directives and their full syntax.

Architecture:
  1. Tokenizer: reads lines, identifies directives vs SQL, handles
     multi-line collection, delimiter tracking
  2. Parser: processes tokens into MtrCommand AST nodes
  3. Source inclusion: --source / .inc file recursion
  4. Block parsing: if/while/else/end with proper nesting
"""

from __future__ import annotations

import logging
import os
import re
from typing import Dict, List, Optional, Set, Tuple

from .nodes import (
    BlockOp,
    ConditionExpr,
    ConnectSpec,
    ErrorSpec,
    MtrBlock,
    MtrCommand,
    MtrCommandType,
    MtrIfBlock,
    MtrTestFile,
    MtrWhileBlock,
    ReplaceColumnSpec,
    ReplaceRegexSpec,
    ReplaceResultSpec,
)

log = logging.getLogger("rosetta.mtr")

# ---------------------------------------------------------------------------
# Directive name -> MtrCommandType mapping
# Mirrors command_names[] in mysqltest.cc lines 541-572
# ---------------------------------------------------------------------------
COMMAND_MAP: Dict[str, MtrCommandType] = {
    "connection": MtrCommandType.CONNECTION,
    "query": MtrCommandType.QUERY,
    "connect": MtrCommandType.CONNECT,
    "sleep": MtrCommandType.SLEEP,
    "inc": MtrCommandType.INC,
    "dec": MtrCommandType.DEC,
    "source": MtrCommandType.SOURCE,
    "disconnect": MtrCommandType.DISCONNECT,
    "let": MtrCommandType.LET,
    "echo": MtrCommandType.ECHO,
    "expr": MtrCommandType.EXPR,
    "while": MtrCommandType.WHILE,
    "end": MtrCommandType.END,
    "save_master_pos": MtrCommandType.SAVE_MASTER_POS,
    "sync_with_master": MtrCommandType.SYNC_WITH_MASTER,
    "sync_slave_with_master": MtrCommandType.SYNC_SLAVE_WITH_MASTER,
    "error": MtrCommandType.ERROR,
    "send": MtrCommandType.SEND,
    "reap": MtrCommandType.REAP,
    "dirty_close": MtrCommandType.DIRTY_CLOSE,
    "replace_result": MtrCommandType.REPLACE_RESULT,
    "replace_column": MtrCommandType.REPLACE_COLUMN,
    "ping": MtrCommandType.PING,
    "eval": MtrCommandType.EVAL,
    "enable_query_log": MtrCommandType.ENABLE_QUERY_LOG,
    "disable_query_log": MtrCommandType.DISABLE_QUERY_LOG,
    "enable_result_log": MtrCommandType.ENABLE_RESULT_LOG,
    "disable_result_log": MtrCommandType.DISABLE_RESULT_LOG,
    "enable_connect_log": MtrCommandType.ENABLE_CONNECT_LOG,
    "disable_connect_log": MtrCommandType.DISABLE_CONNECT_LOG,
    "wait_for_slave_to_stop": MtrCommandType.WAIT_FOR_SLAVE_TO_STOP,
    "enable_warnings": MtrCommandType.ENABLE_WARNINGS,
    "disable_warnings": MtrCommandType.DISABLE_WARNINGS,
    "enable_info": MtrCommandType.ENABLE_INFO,
    "disable_info": MtrCommandType.DISABLE_INFO,
    "enable_session_track_info": MtrCommandType.ENABLE_SESSION_TRACK_INFO,
    "disable_session_track_info": MtrCommandType.DISABLE_SESSION_TRACK_INFO,
    "enable_metadata": MtrCommandType.ENABLE_METADATA,
    "disable_metadata": MtrCommandType.DISABLE_METADATA,
    "enable_async_client": MtrCommandType.ENABLE_ASYNC_CLIENT,
    "disable_async_client": MtrCommandType.DISABLE_ASYNC_CLIENT,
    "exec": MtrCommandType.EXEC,
    "execw": MtrCommandType.EXECW,
    "exec_in_background": MtrCommandType.EXEC_BACKGROUND,
    "delimiter": MtrCommandType.DELIMITER,
    "disable_abort_on_error": MtrCommandType.DISABLE_ABORT_ON_ERROR,
    "enable_abort_on_error": MtrCommandType.ENABLE_ABORT_ON_ERROR,
    "vertical_results": MtrCommandType.VERTICAL_RESULTS,
    "horizontal_results": MtrCommandType.HORIZONTAL_RESULTS,
    "query_vertical": MtrCommandType.QUERY_VERTICAL,
    "query_horizontal": MtrCommandType.QUERY_HORIZONTAL,
    "sorted_result": MtrCommandType.SORTED_RESULT,
    "partially_sorted_result": MtrCommandType.PARTIALLY_SORTED_RESULT,
    "lowercase_result": MtrCommandType.LOWERCASE,
    "skip_if_hypergraph": MtrCommandType.SKIP_IF_HYPERGRAPH,
    "run_with_if_pq": MtrCommandType.RUN_WITH_IF_PQ,
    "start_timer": MtrCommandType.START_TIMER,
    "end_timer": MtrCommandType.END_TIMER,
    "character_set": MtrCommandType.CHARACTER_SET,
    "disable_ps_protocol": MtrCommandType.DISABLE_PS_PROTOCOL,
    "enable_ps_protocol": MtrCommandType.ENABLE_PS_PROTOCOL,
    "disable_reconnect": MtrCommandType.DISABLE_RECONNECT,
    "enable_reconnect": MtrCommandType.ENABLE_RECONNECT,
    "if": MtrCommandType.IF,
    "disable_testcase": MtrCommandType.DISABLE_TESTCASE,
    "enable_testcase": MtrCommandType.ENABLE_TESTCASE,
    "replace_regex": MtrCommandType.REPLACE_REGEX,
    "replace_numeric_round": MtrCommandType.REPLACE_NUMERIC_ROUND,
    "remove_file": MtrCommandType.REMOVE_FILE,
    "file_exists": MtrCommandType.FILE_EXISTS,
    "file_exist": MtrCommandType.FILE_EXISTS,  # alias
    "write_file": MtrCommandType.WRITE_FILE,
    "copy_file": MtrCommandType.COPY_FILE,
    "perl": MtrCommandType.PERL,
    "die": MtrCommandType.DIE,
    "exit": MtrCommandType.EXIT,
    "skip": MtrCommandType.SKIP,
    "chmod": MtrCommandType.CHMOD,
    "append_file": MtrCommandType.APPEND_FILE,
    "cat_file": MtrCommandType.CAT_FILE,
    "diff_files": MtrCommandType.DIFF_FILES,
    "send_quit": MtrCommandType.SEND_QUIT,
    "change_user": MtrCommandType.CHANGE_USER,
    "mkdir": MtrCommandType.MKDIR,
    "rmdir": MtrCommandType.RMDIR,
    "force-rmdir": MtrCommandType.FORCE_RMDIR,
    "force-cpdir": MtrCommandType.FORCE_CPDIR,
    "list_files": MtrCommandType.LIST_FILES,
    "list_files_write_file": MtrCommandType.LIST_FILES_WRITE_FILE,
    "list_files_append_file": MtrCommandType.LIST_FILES_APPEND_FILE,
    "send_shutdown": MtrCommandType.SEND_SHUTDOWN,
    "shutdown_server": MtrCommandType.SHUTDOWN_SERVER,
    "result_format": MtrCommandType.RESULT_FORMAT,
    "move_file": MtrCommandType.MOVE_FILE,
    "remove_files_wildcard": MtrCommandType.REMOVE_FILES_WILDCARD,
    "copy_files_wildcard": MtrCommandType.COPY_FILES_WILDCARD,
    "send_eval": MtrCommandType.SEND_EVAL,
    "output": MtrCommandType.OUTPUT,
    "reset_connection": MtrCommandType.RESET_CONNECTION,
    "query_attributes": MtrCommandType.QUERY_ATTRIBUTES,
    "system": MtrCommandType.EXEC,  # alias
    "real_sleep": MtrCommandType.SLEEP,  # alias
    "assert": MtrCommandType.ASSERT,
}

# Directives that take content until a delimiter (like --write_file ... EOF)
_CONTENT_DELIMITED_COMMANDS = {
    MtrCommandType.WRITE_FILE,
    MtrCommandType.APPEND_FILE,
    MtrCommandType.PERL,
    MtrCommandType.DISABLE_TESTCASE,
}

# Directives that are simple property toggles (no args needed)
_PROPERTY_COMMANDS = {
    MtrCommandType.ENABLE_QUERY_LOG,
    MtrCommandType.DISABLE_QUERY_LOG,
    MtrCommandType.ENABLE_RESULT_LOG,
    MtrCommandType.DISABLE_RESULT_LOG,
    MtrCommandType.ENABLE_CONNECT_LOG,
    MtrCommandType.DISABLE_CONNECT_LOG,
    MtrCommandType.ENABLE_WARNINGS,
    MtrCommandType.DISABLE_WARNINGS,
    MtrCommandType.ENABLE_INFO,
    MtrCommandType.DISABLE_INFO,
    MtrCommandType.ENABLE_SESSION_TRACK_INFO,
    MtrCommandType.DISABLE_SESSION_TRACK_INFO,
    MtrCommandType.ENABLE_METADATA,
    MtrCommandType.DISABLE_METADATA,
    MtrCommandType.ENABLE_ABORT_ON_ERROR,
    MtrCommandType.DISABLE_ABORT_ON_ERROR,
    MtrCommandType.ENABLE_PS_PROTOCOL,
    MtrCommandType.DISABLE_PS_PROTOCOL,
    MtrCommandType.ENABLE_RECONNECT,
    MtrCommandType.DISABLE_RECONNECT,
    MtrCommandType.ENABLE_ASYNC_CLIENT,
    MtrCommandType.DISABLE_ASYNC_CLIENT,
    MtrCommandType.VERTICAL_RESULTS,
    MtrCommandType.HORIZONTAL_RESULTS,
    MtrCommandType.SORTED_RESULT,
    MtrCommandType.LOWERCASE,
    MtrCommandType.REAP,
    MtrCommandType.PING,
    MtrCommandType.RESET_CONNECTION,
    MtrCommandType.SEND_SHUTDOWN,
    MtrCommandType.START_TIMER,
    MtrCommandType.END_TIMER,
    MtrCommandType.SKIP_IF_HYPERGRAPH,
    MtrCommandType.ENABLE_TESTCASE,
}

# Commands that should not clear the expected error list
_NO_CLEAR_ERROR_COMMANDS = {
    MtrCommandType.ERROR,
    MtrCommandType.COMMENT,
    MtrCommandType.IF,
    MtrCommandType.END,
}


class ParseError(Exception):
    """Error during .test file parsing."""
    pass


class MtrParser:
    """Complete MTR .test file parser.

    Parses .test files into a list of MtrCommand AST nodes,
    supporting all MTR directives from mysqltest.cc.

    Usage:
        parser = MtrParser("/path/to/test.test", mysql_test_dir="/path/to/mysql-test")
        test_file = parser.parse()
        for cmd in test_file.commands:
            ...
    """

    def __init__(self, filepath: str, mysql_test_dir: Optional[str] = None):
        """Initialize parser.

        Args:
            filepath: Path to the .test file.
            mysql_test_dir: Root mysql-test directory for resolving --source paths.
        """
        self.filepath = filepath
        self.mysql_test_dir = mysql_test_dir
        self._delimiter = ";"
        self._included: Set[str] = set()
        self._commands: List[MtrCommand] = []

        if not self.mysql_test_dir:
            abs_path = os.path.abspath(filepath)
            idx = abs_path.find("mysql-test")
            if idx >= 0:
                self.mysql_test_dir = abs_path[:idx + len("mysql-test")]

    def parse(self) -> MtrTestFile:
        """Parse the .test file and return an MtrTestFile AST.

        Returns:
            MtrTestFile with all commands parsed.
        """
        self._commands = []
        self._delimiter = ";"
        self._parse_file(self.filepath)
        return MtrTestFile(
            file_path=self.filepath,
            commands=self._commands,
        )

    def parse_text(self, text: str) -> MtrTestFile:
        """Parse raw MTR text (not from a file).

        Useful for playground or inline test definitions.

        Args:
            text: Raw MTR/SQL text.

        Returns:
            MtrTestFile with all commands parsed.
        """
        self._commands = []
        self._delimiter = ";"
        lines = text.splitlines(keepends=True)
        self._parse_lines(lines, "<text>")
        return MtrTestFile(
            file_path="<text>",
            commands=self._commands,
        )

    # -----------------------------------------------------------------------
    # Internal parsing methods
    # -----------------------------------------------------------------------

    def _parse_file(self, filepath: str) -> None:
        """Parse a single file, handling --source recursion.

        The same file may be sourced multiple times (e.g. running an .inc
        with different variable settings).  We use a recursion depth counter
        instead of a path set to prevent infinite recursion while allowing
        legitimate re-includes.
        """
        abs_path = os.path.abspath(filepath)

        # Guard against infinite recursion (max depth 64)
        depth = getattr(self, '_source_depth', 0)
        if depth > 64:
            log.warning("Source recursion depth exceeded for: %s", filepath)
            return
        self._source_depth = depth + 1

        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except FileNotFoundError:
            raise ParseError(f"File not found: {filepath}")

        self._parse_lines(lines, filepath)
        self._source_depth = depth  # restore depth after returning

    def _parse_lines(self, lines: List[str], filepath: str) -> None:
        """Core line-by-line parsing logic."""
        idx = 0
        line_no = 0

        while idx < len(lines):
            line = lines[idx].rstrip("\n\r")
            line_no = idx + 1
            idx += 1

            stripped = line.strip()

            # Skip empty lines
            if not stripped:
                continue

            # Skip comment lines (single # at start)
            if stripped.startswith("#"):
                # Double ## comments may be output to result file
                continue

            # Handle } and }; (end of block)
            if stripped in ("}", "};"):
                continue

            # Handle --directive lines
            if stripped.startswith("--"):
                cmd = self._parse_directive(stripped, lines, idx, line_no, filepath)
                if cmd is not None:
                    # For content-delimited commands, consume content lines
                    if cmd.cmd_type in _CONTENT_DELIMITED_COMMANDS:
                        idx = self._consume_content(cmd, lines, idx, filepath)
                    elif cmd.cmd_type == MtrCommandType.SOURCE:
                        self._handle_source(cmd)
                    self._commands.append(cmd)
                    # Update idx if multi-line directive consumed lines
                    if cmd.cmd_type in (MtrCommandType.LET, MtrCommandType.EVAL,
                                        MtrCommandType.QUERY,
                                        MtrCommandType.QUERY_VERTICAL,
                                        MtrCommandType.QUERY_HORIZONTAL,
                                        MtrCommandType.SEND, MtrCommandType.SEND_EVAL):
                        # These may consume additional lines
                        pass
                continue

            # Handle bare-word directives (without -- prefix)
            # e.g., "let $var = value;" or "source include/foo.inc;"
            bare_cmd = self._try_parse_bare_directive(stripped, lines, idx,
                                                       line_no, filepath)
            if bare_cmd is not None:
                if bare_cmd.cmd_type == MtrCommandType.SOURCE:
                    self._handle_source(bare_cmd)
                self._commands.append(bare_cmd)
                continue

            # Handle if(...) and while(...) without -- prefix
            if re.match(r'if\s*\(', stripped, re.IGNORECASE):
                cmd = self._parse_if_while(stripped, MtrCommandType.IF,
                                           line_no, filepath)
                self._commands.append(cmd)
                continue

            if re.match(r'while\s*\(', stripped, re.IGNORECASE):
                cmd = self._parse_if_while(stripped, MtrCommandType.WHILE,
                                           line_no, filepath)
                self._commands.append(cmd)
                continue

            # Handle SQL statements (collected until delimiter)
            sql_text, new_idx = self._collect_sql(stripped, lines, idx)
            idx = new_idx
            if sql_text:
                sql_text = self._strip_delimiter(sql_text)
                cmd = MtrCommand(
                    cmd_type=MtrCommandType.SQL,
                    raw_text=sql_text,
                    line_no=line_no,
                    file_path=filepath,
                    argument=sql_text,
                )
                self._commands.append(cmd)

    def _parse_directive(self, stripped: str, lines: List[str],
                         idx: int, line_no: int, filepath: str) -> Optional[MtrCommand]:
        """Parse a --directive line.

        Args:
            stripped: The stripped line starting with --.
            lines: All lines (for multi-line lookahead).
            idx: Current index into lines (next line to read).
            line_no: Line number of this directive.
            filepath: Current file path.

        Returns:
            MtrCommand or None if the directive should be skipped.
        """
        # Strip the -- prefix and get directive name + arguments
        after_dashes = stripped[2:].strip()
        m = re.match(r'(\w+)\s*(.*)', after_dashes, re.DOTALL)
        if not m:
            return None

        directive_name = m.group(1).lower()
        argument = m.group(2).strip()

        # Look up the command type
        cmd_type = COMMAND_MAP.get(directive_name)
        if cmd_type is None:
            log.debug("Unknown MTR directive: --%s (line %d)", directive_name, line_no)
            return None

        # Build the base command
        cmd = MtrCommand(
            cmd_type=cmd_type,
            raw_text=stripped,
            line_no=line_no,
            file_path=filepath,
            argument=argument,
        )

        # Parse directive-specific arguments
        self._parse_command_args(cmd, argument, lines, idx, line_no, filepath)

        return cmd

    def _parse_command_args(self, cmd: MtrCommand, argument: str,
                            lines: List[str], idx: int,
                            line_no: int, filepath: str) -> None:
        """Parse command-specific arguments into the MtrCommand fields."""

        if cmd.cmd_type == MtrCommandType.ECHO:
            cmd.argument = argument

        elif cmd.cmd_type == MtrCommandType.ERROR:
            from .error_handler import ErrorHandler
            handler = ErrorHandler()
            cmd.error_specs = handler.parse_error_specs(argument)

        elif cmd.cmd_type == MtrCommandType.SORTED_RESULT:
            cmd.sort_start_column = 0

        elif cmd.cmd_type == MtrCommandType.PARTIALLY_SORTED_RESULT:
            try:
                cmd.sort_start_column = int(argument.strip())
            except ValueError:
                cmd.sort_start_column = 0

        elif cmd.cmd_type in (MtrCommandType.LET,):
            self._parse_let(cmd, argument)

        elif cmd.cmd_type in (MtrCommandType.INC, MtrCommandType.DEC):
            cmd.var_name = argument.strip()

        elif cmd.cmd_type == MtrCommandType.EXPR:
            self._parse_expr(cmd, argument)

        elif cmd.cmd_type in (MtrCommandType.IF, MtrCommandType.WHILE,
                               MtrCommandType.ASSERT):
            self._parse_if_while_args(cmd, argument)

        elif cmd.cmd_type == MtrCommandType.SOURCE:
            cmd.source_path = argument.strip().rstrip(';').strip()

        elif cmd.cmd_type == MtrCommandType.CONNECT:
            self._parse_connect(cmd, argument)

        elif cmd.cmd_type in (MtrCommandType.CONNECTION,
                               MtrCommandType.DISCONNECT,
                               MtrCommandType.DIRTY_CLOSE,
                               MtrCommandType.SEND_QUIT):
            cmd.connection_name = argument.strip()

        elif cmd.cmd_type == MtrCommandType.CHANGE_USER:
            self._parse_change_user(cmd, argument)

        elif cmd.cmd_type == MtrCommandType.REPLACE_COLUMN:
            cmd.replace_columns = self._parse_replace_column(argument)

        elif cmd.cmd_type == MtrCommandType.REPLACE_RESULT:
            cmd.replace_results = self._parse_replace_result(argument)

        elif cmd.cmd_type == MtrCommandType.REPLACE_REGEX:
            cmd.replace_regexes = self._parse_replace_regex(argument)

        elif cmd.cmd_type == MtrCommandType.REPLACE_NUMERIC_ROUND:
            try:
                cmd.numeric_round_precision = int(argument.strip())
            except ValueError:
                cmd.numeric_round_precision = -1

        elif cmd.cmd_type == MtrCommandType.DELIMITER:
            # MTR syntax: DELIMITER <new_delim><old_delim>
            # e.g. "DELIMITER //;" sets new_delim to "//" (old_delim is ";")
            #      "DELIMITER ;//" sets new_delim to ";" (old_delim is "//")
            raw = argument.strip()
            # Strip the current (old) delimiter from the end
            if self._delimiter and raw.endswith(self._delimiter):
                raw = raw[:-len(self._delimiter)].strip()
            cmd.new_delimiter = raw
            if cmd.new_delimiter:
                self._delimiter = cmd.new_delimiter

        elif cmd.cmd_type == MtrCommandType.SLEEP:
            try:
                cmd.sleep_seconds = float(argument.strip())
            except ValueError:
                cmd.sleep_seconds = 0.0

        elif cmd.cmd_type in (MtrCommandType.EVAL, MtrCommandType.QUERY,
                               MtrCommandType.QUERY_VERTICAL,
                               MtrCommandType.QUERY_HORIZONTAL):
            cmd.argument = argument

        elif cmd.cmd_type in (MtrCommandType.SEND, MtrCommandType.SEND_EVAL):
            cmd.argument = argument

        elif cmd.cmd_type in (MtrCommandType.WRITE_FILE,
                               MtrCommandType.APPEND_FILE):
            self._parse_write_file_args(cmd, argument)

        elif cmd.cmd_type == MtrCommandType.PERL:
            # Optional delimiter argument
            parts = argument.strip().split(None, 1)
            if parts:
                cmd.file_delimiter = parts[0] if parts else "EOF"

        elif cmd.cmd_type in (MtrCommandType.COPY_FILE, MtrCommandType.MOVE_FILE,
                               MtrCommandType.DIFF_FILES):
            parts = self._split_args(argument, 2)
            if len(parts) >= 2:
                cmd.from_file = parts[0]
                cmd.to_file = parts[1]

        elif cmd.cmd_type == MtrCommandType.REMOVE_FILE:
            parts = self._split_args(argument, 2)
            cmd.target_file = parts[0] if parts else ""
            cmd.retry = int(parts[1]) if len(parts) > 1 else 0

        elif cmd.cmd_type == MtrCommandType.FILE_EXISTS:
            parts = self._split_args(argument, 2)
            cmd.target_file = parts[0] if parts else ""
            cmd.retry = int(parts[1]) if len(parts) > 1 else 0

        elif cmd.cmd_type == MtrCommandType.CAT_FILE:
            cmd.target_file = argument.strip()

        elif cmd.cmd_type == MtrCommandType.CHMOD:
            parts = self._split_args(argument, 2)
            if len(parts) >= 2:
                cmd.chmod_mode = parts[0]
                cmd.chmod_file = parts[1]

        elif cmd.cmd_type == MtrCommandType.MKDIR:
            cmd.dir_path = argument.strip()

        elif cmd.cmd_type in (MtrCommandType.RMDIR, MtrCommandType.FORCE_RMDIR):
            cmd.dir_path = argument.strip()

        elif cmd.cmd_type == MtrCommandType.FORCE_CPDIR:
            parts = self._split_args(argument, 2)
            if len(parts) >= 2:
                cmd.from_file = parts[0]
                cmd.to_file = parts[1]

        elif cmd.cmd_type == MtrCommandType.LIST_FILES:
            parts = self._split_args(argument, 2)
            cmd.dir_path = parts[0] if parts else ""
            cmd.wildcard = parts[1] if len(parts) > 1 else ""

        elif cmd.cmd_type in (MtrCommandType.LIST_FILES_WRITE_FILE,
                               MtrCommandType.LIST_FILES_APPEND_FILE):
            parts = self._split_args(argument, 3)
            if len(parts) >= 2:
                cmd.file_path_arg = parts[0]
                cmd.dir_path = parts[1]
                cmd.wildcard = parts[2] if len(parts) > 2 else ""

        elif cmd.cmd_type == MtrCommandType.REMOVE_FILES_WILDCARD:
            parts = self._split_args(argument, 3)
            if len(parts) >= 2:
                cmd.dir_path = parts[0]
                cmd.wildcard = parts[1]
                cmd.retry = int(parts[2]) if len(parts) > 2 else 0

        elif cmd.cmd_type == MtrCommandType.COPY_FILES_WILDCARD:
            parts = self._split_args(argument, 4)
            if len(parts) >= 3:
                cmd.from_file = parts[0]
                cmd.to_file = parts[1]
                cmd.wildcard = parts[2]
                cmd.retry = int(parts[3]) if len(parts) > 3 else 0

        elif cmd.cmd_type in (MtrCommandType.EXEC, MtrCommandType.EXECW,
                               MtrCommandType.EXEC_BACKGROUND):
            cmd.exec_command = argument.strip()

        elif cmd.cmd_type == MtrCommandType.QUERY_ATTRIBUTES:
            cmd.query_attrs = self._parse_query_attributes(argument)

        elif cmd.cmd_type == MtrCommandType.CHARACTER_SET:
            cmd.charset_name = argument.strip()

        elif cmd.cmd_type == MtrCommandType.RESULT_FORMAT:
            try:
                cmd.result_format_version = int(argument.strip())
            except ValueError:
                pass

        elif cmd.cmd_type == MtrCommandType.OUTPUT:
            cmd.output_file = argument.strip()

        elif cmd.cmd_type == MtrCommandType.SKIP:
            cmd.skip_message = argument.strip()

        elif cmd.cmd_type == MtrCommandType.DIE:
            cmd.die_message = argument.strip()

        elif cmd.cmd_type == MtrCommandType.DISABLE_TESTCASE:
            cmd.bug_number = argument.strip()

        elif cmd.cmd_type == MtrCommandType.SYNC_WITH_MASTER:
            try:
                cmd.sync_offset = int(argument.strip()) if argument.strip() else 0
            except ValueError:
                cmd.sync_offset = 0

        elif cmd.cmd_type == MtrCommandType.SHUTDOWN_SERVER:
            parts = self._split_args(argument, 2)
            try:
                cmd.shutdown_timeout = int(parts[0]) if parts else 600
            except ValueError:
                cmd.shutdown_timeout = 600
            if len(parts) > 1:
                cmd.shutdown_pid_file = parts[1]

        elif cmd.cmd_type in (MtrCommandType.ENABLE_WARNINGS,
                               MtrCommandType.DISABLE_WARNINGS):
            # May include warning list and ONCE keyword
            self._parse_warnings_args(cmd, argument)

        elif cmd.cmd_type == MtrCommandType.RESET_CONNECTION:
            pass  # No arguments

        elif cmd.cmd_type == MtrCommandType.ASSERT:
            self._parse_if_while_args(cmd, argument)

    # -----------------------------------------------------------------------
    # Specific argument parsers
    # -----------------------------------------------------------------------

    def _parse_let(self, cmd: MtrCommand, argument: str) -> None:
        """Parse --let $var = value."""
        m = re.match(r'\$(\w+)\s*=\s*(.*)', argument.strip(), re.DOTALL)
        if m:
            cmd.var_name = m.group(1)
            cmd.var_value = m.group(2).strip().rstrip(';').strip()
        else:
            # Try without $ prefix
            m = re.match(r'(\w+)\s*=\s*(.*)', argument.strip(), re.DOTALL)
            if m:
                cmd.var_name = m.group(1)
                cmd.var_value = m.group(2).strip().rstrip(';').strip()
            else:
                cmd.argument = argument

    def _parse_expr(self, cmd: MtrCommand, argument: str) -> None:
        """Parse --expr $var = $op1 <operator> $op2."""
        m = re.match(
            r'\$(\w+)\s*=\s*(\$\w+|\d+)\s*([+\-*/%])\s*(\$\w+|\d+)',
            argument.strip())
        if m:
            cmd.var_name = m.group(1)
            cmd.expr_operand1 = m.group(2)
            cmd.expr_operator = m.group(3)
            cmd.expr_operand2 = m.group(4)
        else:
            cmd.argument = argument

    def _parse_if_while_args(self, cmd: MtrCommand, argument: str) -> None:
        """Parse if/while/assert condition expression."""
        cond = self._parse_condition(argument)
        cmd.condition = cond

    def _parse_condition(self, expr_str: str) -> ConditionExpr:
        """Parse a condition expression like ($var == value) or (!$var)."""
        # Strip outer parentheses
        s = expr_str.strip()
        if s.startswith('(') and s.endswith(')'):
            s = s[1:-1].strip()
        # Also handle trailing { and content after )
        brace_idx = s.find('{')
        if brace_idx >= 0:
            s = s[:brace_idx].strip()
            # Re-check for closing paren
            if s.endswith(')'):
                s = s[:-1].strip()

        negated = False
        if s.startswith('!'):
            negated = True
            s = s[1:].strip()

        # Check for comparison operators
        for op_str, op_enum in [('==', BlockOp.EQ), ('!=', BlockOp.NE),
                                 ('<=', BlockOp.LE), ('>=', BlockOp.GE),
                                 ('<', BlockOp.LT), ('>', BlockOp.GT)]:
            idx = s.find(op_str)
            if idx >= 0 and s.startswith('$'):
                var_part = s[:idx].strip()
                right_part = s[idx + len(op_str):].strip()
                # Strip trailing ) if present
                if right_part.endswith(')'):
                    right_part = right_part[:-1].strip()
                # Strip quotes
                if (right_part.startswith("'") and right_part.endswith("'")) or \
                   (right_part.startswith('"') and right_part.endswith('"')):
                    right_part = right_part[1:-1]

                var_name = var_part.lstrip('$')
                return ConditionExpr(
                    var_name=var_name,
                    negated=negated,
                    operator=op_enum,
                    right_operand=right_part,
                )

        # No operator - simple truthy check
        var_name = s.lstrip('$').strip()
        # Strip trailing ) if present
        if var_name.endswith(')'):
            var_name = var_name[:-1].strip()

        return ConditionExpr(
            var_name=var_name,
            negated=negated,
        )

    def _parse_connect(self, cmd: MtrCommand, argument: str) -> None:
        """Parse --connect arguments.

        Format: name,host,user,password,database,port,socket,options,
                default_auth,compression_algorithm,zstd_compression_level
        """
        # Strip parentheses if present
        arg = argument.strip()
        if arg.startswith('(') and arg.endswith(')'):
            arg = arg[1:-1]

        parts = [p.strip() for p in arg.split(',')]
        spec = ConnectSpec()
        if len(parts) > 0:
            spec.connection_name = parts[0]
        if len(parts) > 1:
            spec.host = parts[1]
        if len(parts) > 2:
            spec.user = parts[2]
        if len(parts) > 3:
            spec.password = parts[3]
        if len(parts) > 4:
            spec.database = parts[4]
        if len(parts) > 5:
            try:
                spec.port = int(parts[5])
            except ValueError:
                spec.port = 0
        if len(parts) > 6:
            spec.socket = parts[6]
        if len(parts) > 7:
            spec.options = parts[7]
        if len(parts) > 8:
            spec.default_auth = parts[8]
        if len(parts) > 9:
            spec.compression_algorithm = parts[9]
        if len(parts) > 10:
            spec.zstd_compression_level = parts[10]

        cmd.connect_spec = spec
        cmd.connection_name = spec.connection_name

    def _parse_change_user(self, cmd: MtrCommand, argument: str) -> None:
        """Parse --change_user user,password,database,reconnect."""
        parts = [p.strip() for p in argument.split(',')]
        if len(parts) > 0:
            cmd.change_user_name = parts[0]
        if len(parts) > 1:
            cmd.change_user_password = parts[1]
        if len(parts) > 2:
            cmd.change_user_database = parts[2]
        if len(parts) > 3:
            cmd.change_user_reconnect = parts[3]

    def _parse_replace_column(self, argument: str) -> List[ReplaceColumnSpec]:
        """Parse --replace_column col_num replacement [col_num replacement ...]"""
        specs = []
        # Parse pairs: column_number replacement_string
        parts = self._split_quoted_args(argument)
        i = 0
        while i + 1 < len(parts):
            try:
                col_num = int(parts[i])
                replacement = parts[i + 1]
                specs.append(ReplaceColumnSpec(
                    column_number=col_num,
                    replacement=replacement,
                ))
                i += 2
            except ValueError:
                i += 1
        return specs

    def _parse_replace_result(self, argument: str) -> List[ReplaceResultSpec]:
        """Parse --replace_result from_str to_str [from_str to_str ...]"""
        specs = []
        parts = self._split_quoted_args(argument)
        i = 0
        while i + 1 < len(parts):
            specs.append(ReplaceResultSpec(
                from_str=parts[i],
                to_str=parts[i + 1],
            ))
            i += 2
        return specs

    def _parse_replace_regex(self, argument: str) -> List[ReplaceRegexSpec]:
        """Parse --replace_regex pattern replacement [pattern replacement ...]

        The argument may be a $variable reference containing the full spec.
        """
        specs = []
        # If argument starts with $, it's a variable reference - store as-is
        if argument.strip().startswith('$'):
            return specs  # Will be resolved at execution time

        parts = self._split_quoted_args(argument)
        i = 0
        while i + 1 < len(parts):
            specs.append(ReplaceRegexSpec(
                pattern=parts[i],
                replacement=parts[i + 1],
            ))
            i += 2
        return specs

    def _parse_query_attributes(self, argument: str) -> List[tuple]:
        """Parse --query_attributes name1 value1 name2 value2 ..."""
        attrs = []
        parts = self._split_quoted_args(argument)
        i = 0
        while i + 1 < len(parts):
            attrs.append((parts[i], parts[i + 1]))
            i += 2
        return attrs

    def _parse_write_file_args(self, cmd: MtrCommand, argument: str) -> None:
        """Parse --write_file / --append_file arguments."""
        parts = self._split_args(argument, 2)
        if parts:
            cmd.file_path_arg = parts[0].strip()
        if len(parts) > 1:
            cmd.file_delimiter = parts[1].strip()
        else:
            cmd.file_delimiter = "EOF"

    def _parse_warnings_args(self, cmd: MtrCommand, argument: str) -> None:
        """Parse --enable_warnings / --disable_warnings arguments.

        May include: [warning_list] [ONCE]
        """
        arg = argument.strip()
        if not arg:
            return

        parts = arg.split()
        for part in parts:
            if part.upper() == 'ONCE':
                cmd.once = True
            else:
                cmd.warning_list.append(part)

    # -----------------------------------------------------------------------
    # Content consumption for write_file, perl, etc.
    # -----------------------------------------------------------------------

    def _consume_content(self, cmd: MtrCommand, lines: List[str],
                         start_idx: int, filepath: str) -> int:
        """Consume lines until the content delimiter for write_file/perl/etc.

        Returns the new index after consuming content.
        """
        delimiter = cmd.file_delimiter or "EOF"
        content_lines = []
        idx = start_idx

        while idx < len(lines):
            line = lines[idx].rstrip("\n\r")
            idx += 1
            if line.strip() == delimiter:
                break
            content_lines.append(line)

        cmd.file_content = "\n".join(content_lines)
        return idx

    # -----------------------------------------------------------------------
    # SQL collection
    # -----------------------------------------------------------------------

    def _collect_sql(self, first_line: str, lines: List[str],
                     start_idx: int) -> Tuple[str, int]:
        """Collect a multi-line SQL statement terminated by the current delimiter.

        Returns (sql_text, new_idx).
        """
        sql_lines = [self._strip_line_comment(first_line)]

        if self._ends_with_delimiter(first_line.strip()):
            return first_line, start_idx

        idx = start_idx
        while idx < len(lines):
            line = lines[idx].rstrip("\n\r")
            idx += 1
            stripped = line.strip()
            sql_lines.append(self._strip_line_comment(line))
            if self._ends_with_delimiter(stripped):
                break

        return "\n".join(sql_lines), idx

    # -----------------------------------------------------------------------
    # Bare-word directive handling
    # -----------------------------------------------------------------------

    def _try_parse_bare_directive(self, stripped: str, lines: List[str],
                                   idx: int, line_no: int,
                                   filepath: str) -> Optional[MtrCommand]:
        """Try to parse a bare-word directive (without -- prefix).

        Many MTR directives can appear without the -- prefix:
          let $var = value;
          source include/foo.inc;
          eval SELECT * FROM t1;
          if (...) {
          while (...) {
        """
        first_word = stripped.split()[0].lower().rstrip(';') if stripped else ""
        if not first_word:
            return None

        # Map of bare-word directives
        bare_words = {
            "let", "eval", "source", "if", "while", "end",
            "inc", "dec", "perl", "die", "exit", "skip",
            "connect", "connection", "disconnect",
            "enable_warnings", "disable_warnings",
            "enable_query_log", "disable_query_log",
            "enable_result_log", "disable_result_log",
            "replace_column", "replace_regex", "replace_result",
            "remove_file", "write_file", "append_file",
            "copy_file", "move_file", "file_exists",
            "mkdir", "rmdir", "list_files",
            "exec", "system", "result_format",
            "sleep", "real_sleep", "echo",
            "delimiter", "error", "require", "assert",
        }

        if first_word not in bare_words:
            return None

        # Get the rest of the line after the first word
        rest = stripped[len(first_word):].strip()

        cmd_type = COMMAND_MAP.get(first_word)
        if cmd_type is None:
            return None

        cmd = MtrCommand(
            cmd_type=cmd_type,
            raw_text=stripped,
            line_no=line_no,
            file_path=filepath,
            argument=rest,
        )

        self._parse_command_args(cmd, rest, lines, idx, line_no, filepath)

        # For bare 'source', handle immediately
        if cmd.cmd_type == MtrCommandType.SOURCE:
            cmd.source_path = rest.strip().rstrip(';').strip()
            self._handle_source(cmd)

        return cmd

    # -----------------------------------------------------------------------
    # Source file handling
    # -----------------------------------------------------------------------

    def _handle_source(self, cmd: MtrCommand) -> None:
        """Handle --source directive by recursively parsing the included file."""
        source_path = cmd.source_path
        if not source_path:
            return

        resolved = self._resolve_source_path(source_path)
        if resolved:
            self._parse_file(resolved)

    def _resolve_source_path(self, arg: str) -> Optional[str]:
        """Resolve a --source argument to an absolute file path."""
        if not self.mysql_test_dir:
            return None

        # Try relative to mysql-test dir
        resolved = os.path.join(self.mysql_test_dir, arg)
        if os.path.isfile(resolved):
            return resolved

        # Try relative to current file's directory
        resolved = os.path.join(os.path.dirname(self.filepath), arg)
        if os.path.isfile(resolved):
            return resolved

        return None

    # -----------------------------------------------------------------------
    # Utility methods
    # -----------------------------------------------------------------------

    def _ends_with_delimiter(self, stripped: str) -> bool:
        """Check if a stripped line ends with the current delimiter."""
        return stripped.endswith(self._delimiter)

    def _strip_delimiter(self, sql_text: str) -> str:
        """Remove the trailing delimiter from SQL text."""
        if self._delimiter and sql_text.endswith(self._delimiter):
            return sql_text[:-len(self._delimiter)].strip()
        return sql_text

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

    @staticmethod
    def _split_args(argument: str, max_parts: int = 0) -> List[str]:
        """Split arguments by whitespace, respecting quoted strings.

        Args:
            argument: The argument string.
            max_parts: Maximum number of parts (0 = unlimited).

        Returns:
            List of argument parts.
        """
        parts = []
        current = []
        in_quote = None
        i = 0
        while i < len(argument):
            ch = argument[i]
            if ch in ('"', "'") and in_quote is None:
                in_quote = ch
                current.append(ch)
            elif ch == in_quote:
                in_quote = None
                current.append(ch)
            elif ch in (' ', '\t') and in_quote is None:
                if current:
                    parts.append(''.join(current))
                    current = []
                    if max_parts and len(parts) >= max_parts:
                        # Include the rest as one part
                        rest = argument[i:].strip()
                        if rest:
                            parts.append(rest)
                        return parts
            else:
                current.append(ch)
            i += 1

        if current:
            parts.append(''.join(current))

        return parts

    @staticmethod
    def _split_quoted_args(argument: str) -> List[str]:
        """Split arguments respecting quoted strings and escape chars."""
        parts = []
        current = []
        in_quote = None
        i = 0
        while i < len(argument):
            ch = argument[i]
            if ch == '\\' and i + 1 < len(argument) and in_quote:
                current.append(argument[i + 1])
                i += 2
                continue
            if ch in ('"', "'") and in_quote is None:
                in_quote = ch
            elif ch == in_quote:
                in_quote = None
            elif ch in (' ', '\t') and in_quote is None:
                if current:
                    parts.append(''.join(current))
                    current = []
            else:
                current.append(ch)
            i += 1

        if current:
            parts.append(''.join(current))

        return parts

    def _parse_if_while(self, stripped: str,
                         cmd_type: MtrCommandType,
                         line_no: int, filepath: str) -> MtrCommand:
        """Parse if(...) or while(...) without -- prefix."""
        # Extract the condition part
        m = re.match(r'(if|while)\s*\((.+?)\)\s*\{?', stripped, re.IGNORECASE)
        if m:
            cond_str = m.group(2)
            condition = self._parse_condition(cond_str)
            return MtrCommand(
                cmd_type=cmd_type,
                raw_text=stripped,
                line_no=line_no,
                file_path=filepath,
                argument=cond_str,
                condition=condition,
            )
        return MtrCommand(
            cmd_type=cmd_type,
            raw_text=stripped,
            line_no=line_no,
            file_path=filepath,
        )
