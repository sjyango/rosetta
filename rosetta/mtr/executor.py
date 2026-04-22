"""MTR test executor - the runtime engine.

Orchestrates the execution of parsed MTR commands against a DBMS,
handling variables, conditions, connections, result processing,
file operations, and error matching.

This is the Python equivalent of the main execution loop in
mysqltest.cc (the switch(command->type) in the run_test loop).
"""

from __future__ import annotations

import glob
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .connection import Connection, ConnectionManager, DBConnector
from .error_handler import (
    ErrorHandler,
    ErrorType,
    ExpectedError,
    MtrError,
    MtrTestDied,
    MtrTestExit,
    MtrTestFailed,
    MtrTestSkipped,
)
from .nodes import (
    BlockOp,
    ConditionExpr,
    ConnectSpec,
    MtrBlock,
    MtrCommand,
    MtrCommandType,
    MtrIfBlock,
    MtrTestFile,
    MtrWhileBlock,
)
from .result_processor import QueryResult, ResultProcessor
from .variable import VariableError, VariableStore

log = logging.getLogger("rosetta.mtr")


@dataclass
class ExecutionState:
    """Runtime state for MTR test execution."""
    # Execution control
    abort_on_error: bool = True
    testcase_disabled: bool = False
    skip_remaining: bool = False

    # Properties that can be set with ONCE
    once_property: bool = False

    # Timer
    timer_start: float = 0.0
    timer_file: str = ""

    # Result format
    result_format_version: int = 2

    # Character set
    charset_name: str = "utf8mb4"

    # PS protocol
    ps_protocol_enabled: bool = False

    # Reconnect
    reconnect_enabled: bool = True

    # Async client
    async_client_enabled: bool = False


@dataclass
class ExecutionResult:
    """Result of executing an MTR test file."""
    test_file: str
    output_lines: List[str] = field(default_factory=list)
    commands_executed: int = 0
    errors: List[str] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""
    died: bool = False
    die_reason: str = ""
    elapsed_time: float = 0.0


class MtrExecutor:
    """Execute parsed MTR test commands against a DBMS.

    This is the main runtime engine that processes MtrCommand AST nodes
    produced by MtrParser, executing them in sequence against a target
    database via the provided DBConnector.

    Usage:
        parser = MtrParser("test.test")
        test = parser.parse()

        connector = MyDBConnector()
        executor = MtrExecutor(connector)
        result = executor.execute(test)
    """

    def __init__(self, connector: Optional[DBConnector] = None,
                 mysql_test_dir: Optional[str] = None,
                 vardir: Optional[str] = None,
                 abort_on_error: Optional[bool] = None,
                 on_progress: Optional[Callable] = None):
        """Initialize the executor.

        Args:
            connector: DBConnector implementation for database operations.
            mysql_test_dir: Root mysql-test directory for file operations.
            vardir: Variable directory for temp files (MYSQLTEST_VARDIR).
            abort_on_error: Whether to abort on unexpected SQL errors.
                None (default): abort on error (standard MTR behavior).
                False: log errors and continue (cross-DBMS compare mode).
            on_progress: Optional callback invoked after each command.
                Signature: on_progress(commands_executed: int, has_error: bool)
        """
        self._connector = connector
        self._mysql_test_dir = mysql_test_dir
        self._vardir = vardir or tempfile.mkdtemp(prefix="mtr_")

        # Runtime components
        self._variables = VariableStore()
        self._connections = ConnectionManager(connector)
        self._error_handler = ErrorHandler()
        self._result_processor = ResultProcessor()
        self._state = ExecutionState()
        if abort_on_error is not None:
            self._state.abort_on_error = abort_on_error

        # Progress callback
        self._on_progress = on_progress

        # Output buffer
        self._output: List[str] = []

        # Block stack for if/while
        self._block_stack: List[Dict] = []
        self._max_block_depth: int = 32

        # Command counter
        self._commands_executed: int = 0

    @property
    def variables(self) -> VariableStore:
        """Access the variable store."""
        return self._variables

    @property
    def connections(self) -> ConnectionManager:
        """Access the connection manager."""
        return self._connections

    @property
    def result_processor(self) -> ResultProcessor:
        """Access the result processor."""
        return self._result_processor

    def execute(self, test_file: MtrTestFile) -> ExecutionResult:
        """Execute a complete parsed test file.

        Args:
            test_file: The parsed MtrTestFile from MtrParser.

        Returns:
            ExecutionResult with output and status.
        """
        start_time = time.time()
        self._output = []
        self._commands_executed = 0
        self._sql_seq = 0  # Global sequence for unique [#nnn] tags

        try:
            self._execute_commands(test_file.commands)
        except MtrTestSkipped as e:
            return ExecutionResult(
                test_file=test_file.file_path,
                output_lines=self._output,
                commands_executed=self._commands_executed,
                skipped=True,
                skip_reason=str(e),
                elapsed_time=time.time() - start_time,
            )
        except MtrTestDied as e:
            return ExecutionResult(
                test_file=test_file.file_path,
                output_lines=self._output,
                commands_executed=self._commands_executed,
                died=True,
                die_reason=str(e),
                elapsed_time=time.time() - start_time,
            )
        except MtrTestExit:
            pass
        except Exception as e:
            self._output.append(f"FATAL ERROR: {e}")

        return ExecutionResult(
            test_file=test_file.file_path,
            output_lines=self._output,
            commands_executed=self._commands_executed,
            elapsed_time=time.time() - start_time,
        )

    def _execute_commands(self, commands: List[MtrCommand]) -> None:
        """Execute a list of commands sequentially."""
        for cmd in commands:
            if self._state.skip_remaining:
                break
            self._execute_command(cmd)

    def _execute_command(self, cmd: MtrCommand) -> None:
        """Execute a single MTR command.

        This is the Python equivalent of the main switch() in mysqltest.cc's
        run_test loop (lines 9829-10301).
        """
        # Check if we're in a disabled block
        if self._state.testcase_disabled:
            if cmd.cmd_type != MtrCommandType.ENABLE_TESTCASE:
                return

        self._commands_executed += 1
        had_error = False

        try:
            handler = self._get_handler(cmd.cmd_type)
            if handler:
                handler(cmd)
            else:
                log.debug("No handler for command type: %s", cmd.cmd_type.name)
        except MtrTestSkipped:
            raise
        except MtrTestDied:
            raise
        except MtrTestExit:
            raise
        except MtrTestFailed:
            raise
        except Exception as e:
            had_error = True
            if self._state.abort_on_error:
                raise MtrError(f"Error executing {cmd.cmd_type.name} "
                               f"at line {cmd.line_no}: {e}")
            else:
                log.warning("Error at line %d: %s", cmd.line_no, e)

        # Notify progress callback
        if self._on_progress:
            try:
                self._on_progress(self._commands_executed, had_error)
            except Exception:
                pass

        # Reset one-shot directives after certain commands
        if cmd.cmd_type not in {
            MtrCommandType.ERROR,
            MtrCommandType.COMMENT,
            MtrCommandType.IF,
            MtrCommandType.END,
        }:
            self._error_handler.clear_expected()

        if cmd.cmd_type not in {
            MtrCommandType.ERROR,
        }:
            self._result_processor.reset_one_shot()

    def _get_handler(self, cmd_type: MtrCommandType) -> Optional[Callable]:
        """Get the handler function for a command type."""
        handlers = {
            MtrCommandType.SQL: self._handle_sql,
            MtrCommandType.EVAL: self._handle_eval,
            MtrCommandType.QUERY: self._handle_query,
            MtrCommandType.QUERY_VERTICAL: self._handle_query_vertical,
            MtrCommandType.QUERY_HORIZONTAL: self._handle_query_horizontal,
            MtrCommandType.ECHO: self._handle_echo,
            MtrCommandType.ERROR: self._handle_error,
            MtrCommandType.LET: self._handle_let,
            MtrCommandType.INC: self._handle_inc,
            MtrCommandType.DEC: self._handle_dec,
            MtrCommandType.EXPR: self._handle_expr,
            MtrCommandType.IF: self._handle_if,
            MtrCommandType.WHILE: self._handle_while,
            MtrCommandType.END: self._handle_end,
            MtrCommandType.ASSERT: self._handle_assert,
            MtrCommandType.SORTED_RESULT: self._handle_sorted_result,
            MtrCommandType.PARTIALLY_SORTED_RESULT: self._handle_partially_sorted_result,
            MtrCommandType.LOWERCASE: self._handle_lowercase,
            MtrCommandType.VERTICAL_RESULTS: self._handle_vertical_results,
            MtrCommandType.HORIZONTAL_RESULTS: self._handle_horizontal_results,
            MtrCommandType.REPLACE_COLUMN: self._handle_replace_column,
            MtrCommandType.REPLACE_RESULT: self._handle_replace_result,
            MtrCommandType.REPLACE_REGEX: self._handle_replace_regex,
            MtrCommandType.REPLACE_NUMERIC_ROUND: self._handle_replace_numeric_round,
            MtrCommandType.CONNECT: self._handle_connect,
            MtrCommandType.DISCONNECT: self._handle_disconnect,
            MtrCommandType.CONNECTION: self._handle_connection,
            MtrCommandType.DIRTY_CLOSE: self._handle_dirty_close,
            MtrCommandType.CHANGE_USER: self._handle_change_user,
            MtrCommandType.SEND_QUIT: self._handle_send_quit,
            MtrCommandType.RESET_CONNECTION: self._handle_reset_connection,
            MtrCommandType.PING: self._handle_ping,
            MtrCommandType.SEND: self._handle_send,
            MtrCommandType.SEND_EVAL: self._handle_send_eval,
            MtrCommandType.REAP: self._handle_reap,
            MtrCommandType.QUERY_ATTRIBUTES: self._handle_query_attributes,
            MtrCommandType.SOURCE: self._handle_source,
            MtrCommandType.SLEEP: self._handle_sleep,
            MtrCommandType.DELIMITER: self._handle_delimiter,
            MtrCommandType.EXIT: self._handle_exit,
            MtrCommandType.DIE: self._handle_die,
            MtrCommandType.SKIP: self._handle_skip,
            MtrCommandType.EXEC: self._handle_exec,
            MtrCommandType.EXECW: self._handle_execw,
            MtrCommandType.EXEC_BACKGROUND: self._handle_exec_background,
            MtrCommandType.CHARACTER_SET: self._handle_character_set,
            MtrCommandType.RESULT_FORMAT: self._handle_result_format,
            MtrCommandType.OUTPUT: self._handle_output,
            MtrCommandType.WRITE_FILE: self._handle_write_file,
            MtrCommandType.APPEND_FILE: self._handle_append_file,
            MtrCommandType.CAT_FILE: self._handle_cat_file,
            MtrCommandType.COPY_FILE: self._handle_copy_file,
            MtrCommandType.MOVE_FILE: self._handle_move_file,
            MtrCommandType.REMOVE_FILE: self._handle_remove_file,
            MtrCommandType.FILE_EXISTS: self._handle_file_exists,
            MtrCommandType.MKDIR: self._handle_mkdir,
            MtrCommandType.RMDIR: self._handle_rmdir,
            MtrCommandType.FORCE_RMDIR: self._handle_force_rmdir,
            MtrCommandType.FORCE_CPDIR: self._handle_force_cpdir,
            MtrCommandType.LIST_FILES: self._handle_list_files,
            MtrCommandType.LIST_FILES_WRITE_FILE: self._handle_list_files_write_file,
            MtrCommandType.LIST_FILES_APPEND_FILE: self._handle_list_files_append_file,
            MtrCommandType.DIFF_FILES: self._handle_diff_files,
            MtrCommandType.CHMOD: self._handle_chmod,
            MtrCommandType.REMOVE_FILES_WILDCARD: self._handle_remove_files_wildcard,
            MtrCommandType.COPY_FILES_WILDCARD: self._handle_copy_files_wildcard,
            MtrCommandType.PERL: self._handle_perl,
            MtrCommandType.ENABLE_QUERY_LOG: self._handle_enable_query_log,
            MtrCommandType.DISABLE_QUERY_LOG: self._handle_disable_query_log,
            MtrCommandType.ENABLE_RESULT_LOG: self._handle_enable_result_log,
            MtrCommandType.DISABLE_RESULT_LOG: self._handle_disable_result_log,
            MtrCommandType.ENABLE_WARNINGS: self._handle_enable_warnings,
            MtrCommandType.DISABLE_WARNINGS: self._handle_disable_warnings,
            MtrCommandType.ENABLE_INFO: self._handle_enable_info,
            MtrCommandType.DISABLE_INFO: self._handle_disable_info,
            MtrCommandType.ENABLE_METADATA: self._handle_enable_metadata,
            MtrCommandType.DISABLE_METADATA: self._handle_disable_metadata,
            MtrCommandType.ENABLE_ABORT_ON_ERROR: self._handle_enable_abort_on_error,
            MtrCommandType.DISABLE_ABORT_ON_ERROR: self._handle_disable_abort_on_error,
            MtrCommandType.ENABLE_PS_PROTOCOL: self._handle_enable_ps_protocol,
            MtrCommandType.DISABLE_PS_PROTOCOL: self._handle_disable_ps_protocol,
            MtrCommandType.ENABLE_RECONNECT: self._handle_enable_reconnect,
            MtrCommandType.DISABLE_RECONNECT: self._handle_disable_reconnect,
            MtrCommandType.ENABLE_ASYNC_CLIENT: self._handle_enable_async_client,
            MtrCommandType.DISABLE_ASYNC_CLIENT: self._handle_disable_async_client,
            MtrCommandType.ENABLE_TESTCASE: self._handle_enable_testcase,
            MtrCommandType.DISABLE_TESTCASE: self._handle_disable_testcase,
            MtrCommandType.ENABLE_CONNECT_LOG: self._handle_enable_connect_log,
            MtrCommandType.DISABLE_CONNECT_LOG: self._handle_disable_connect_log,
            MtrCommandType.ENABLE_SESSION_TRACK_INFO: self._handle_enable_session_track_info,
            MtrCommandType.DISABLE_SESSION_TRACK_INFO: self._handle_disable_session_track_info,
            MtrCommandType.SAVE_MASTER_POS: self._handle_save_master_pos,
            MtrCommandType.SYNC_WITH_MASTER: self._handle_sync_with_master,
            MtrCommandType.SYNC_SLAVE_WITH_MASTER: self._handle_sync_slave_with_master,
            MtrCommandType.WAIT_FOR_SLAVE_TO_STOP: self._handle_wait_for_slave_to_stop,
            MtrCommandType.SEND_SHUTDOWN: self._handle_send_shutdown,
            MtrCommandType.SHUTDOWN_SERVER: self._handle_shutdown_server,
            MtrCommandType.START_TIMER: self._handle_start_timer,
            MtrCommandType.END_TIMER: self._handle_end_timer,
            MtrCommandType.SKIP_IF_HYPERGRAPH: self._handle_skip_if_hypergraph,
            MtrCommandType.RUN_WITH_IF_PQ: self._handle_run_with_if_pq,
        }
        return handlers.get(cmd_type)

    # -----------------------------------------------------------------------
    # SQL execution handlers
    # -----------------------------------------------------------------------

    def _execute_sql(self, sql: str, flags: int = 3) -> QueryResult:
        """Execute a SQL statement and return the result.

        Args:
            sql: The SQL statement to execute.
            flags: Bit flags (1=SEND, 2=REAP, 3=SEND+REAP).

        Returns:
            QueryResult with the execution result.
        """
        if not self._connector:
            return QueryResult(output_text=f"-- would execute: {sql}")

        conn = self._connections.current
        if not conn or not conn.connector:
            return QueryResult(
                is_error=True,
                error_code=-1,
                error_message="No current database connection"
            )

        result = QueryResult()
        try:
            cursor = self._connector.execute(conn.connector, sql)
            if cursor is not None:
                # Try to fetch result set
                try:
                    rows_data = self._connector.fetch_result(cursor)
                    if rows_data is not None:
                        if hasattr(cursor, 'description') and cursor.description:
                            result.columns = [desc[0] for desc in cursor.description]
                        if isinstance(rows_data, (list, tuple)):
                            result.rows = list(rows_data)
                            result.has_result_set = True
                        elif isinstance(rows_data, str):
                            result.output_text = rows_data
                except Exception:
                    pass

                # Get affected rows
                if hasattr(cursor, 'rowcount'):
                    result.affected_rows = cursor.rowcount or 0

                # Close the cursor (each execution creates a new one
                # via RosettaDBConnector, so it's safe to close)
                try:
                    cursor.close()
                except Exception:
                    pass
        except Exception as e:
            result.is_error = True
            error_code, sqlstate, error_message = 0, "", str(e)

            if self._connector:
                try:
                    error_code, sqlstate, error_message = \
                        self._connector.get_error_info(conn.connector)
                except Exception:
                    pass

            result.error_code = error_code
            result.sqlstate = sqlstate
            result.error_message = error_message

            # Check if error was expected
            if self._error_handler.is_error_expected(
                    error_code, sqlstate, self._variables):
                log.debug("Expected error: %d (%s): %s",
                          error_code, sqlstate, error_message)
            elif self._state.abort_on_error:
                raise
            else:
                # Non-abort mode: log the error and continue
                log.warning("SQL error (continuing): %d (%s): %s",
                            error_code, sqlstate, error_message)

        return result

    def _output_result(self, sql: str, result: QueryResult,
                       line_no: int = 0) -> None:
        """Output the formatted result of a SQL query."""
        # Use global sequence for unique [#nnn] tag
        self._sql_seq += 1
        seq = self._sql_seq

        query_log = self._result_processor.format_query_log(sql, seq)
        if query_log:
            self._output.append(query_log)

        # Output the result (if result log enabled)
        if not self._result_processor.disable_result_log:
            formatted = self._result_processor.format_result(result)
            if formatted:
                self._output.append(formatted)

    # -----------------------------------------------------------------------
    # Command handlers
    # -----------------------------------------------------------------------

    def _handle_sql(self, cmd: MtrCommand) -> None:
        """Handle a raw SQL statement."""
        sql = cmd.argument
        if not sql:
            return
        result = self._execute_sql(sql)
        self._output_result(sql, result, line_no=cmd.line_no)

    def _handle_eval(self, cmd: MtrCommand) -> None:
        """Handle --eval: execute SQL with variable substitution."""
        sql = self._variables.substitute(cmd.argument)
        result = self._execute_sql(sql)
        self._output_result(sql, result, line_no=cmd.line_no)

    def _handle_query(self, cmd: MtrCommand) -> None:
        """Handle --query: execute SQL query."""
        sql = cmd.argument
        result = self._execute_sql(sql)
        self._output_result(sql, result, line_no=cmd.line_no)

    def _handle_query_vertical(self, cmd: MtrCommand) -> None:
        """Handle --query_vertical: query with vertical output."""
        old_vertical = self._result_processor.display_vertical
        self._result_processor.display_vertical = True
        sql = cmd.argument
        result = self._execute_sql(sql)
        self._output_result(sql, result, line_no=cmd.line_no)
        self._result_processor.display_vertical = old_vertical

    def _handle_query_horizontal(self, cmd: MtrCommand) -> None:
        """Handle --query_horizontal: query with horizontal output."""
        old_vertical = self._result_processor.display_vertical
        self._result_processor.display_vertical = False
        sql = cmd.argument
        result = self._execute_sql(sql)
        self._output_result(sql, result, line_no=cmd.line_no)
        self._result_processor.display_vertical = old_vertical

    def _handle_echo(self, cmd: MtrCommand) -> None:
        """Handle --echo: print text to result file."""
        text = self._variables.substitute(cmd.argument)
        self._sql_seq += 1
        self._output.append(f"[#{self._sql_seq}] {text}")

    def _handle_error(self, cmd: MtrCommand) -> None:
        """Handle --error: set expected error for next statement."""
        self._error_handler.set_expected(cmd.error_specs)

    # Variable handlers

    def _handle_let(self, cmd: MtrCommand) -> None:
        """Handle --let: set a variable."""
        var_name = cmd.var_name
        value = self._variables.substitute(cmd.var_value)

        # Check for query assignment: --let $var = `SELECT ...`
        m = re.match(r'^`(.*)`$', value, re.DOTALL)
        if m:
            query_sql = m.group(1)
            result = self._execute_sql(query_sql)
            if result.has_result_set and result.rows:
                # Take the first column of the first row
                val = result.rows[0][0]
                value = str(val) if val is not None else "NULL"
            else:
                value = ""

        self._variables.set(var_name, value)
        log.debug("let $%s = %s", var_name, value[:80])

    def _handle_inc(self, cmd: MtrCommand) -> None:
        """Handle --inc: increment a variable."""
        self._variables.inc(cmd.var_name)

    def _handle_dec(self, cmd: MtrCommand) -> None:
        """Handle --dec: decrement a variable."""
        self._variables.dec(cmd.var_name)

    def _handle_expr(self, cmd: MtrCommand) -> None:
        """Handle --expr: math expression."""
        result = self._variables.evaluate_expr(
            cmd.expr_operand1, cmd.expr_operator, cmd.expr_operand2)
        self._variables.set_int(cmd.var_name, result)

    # Conditional handlers

    def _handle_if(self, cmd: MtrCommand) -> None:
        """Handle --if: conditional execution.

        Note: The actual branching logic depends on the block structure
        parsed by the parser. This handler evaluates the condition
        and records the result for block processing.
        """
        cond = cmd.condition
        if cond:
            result = self._evaluate_condition(cond)
            self._block_stack.append({
                'type': 'if',
                'condition': result,
                'depth': len(self._block_stack),
            })

    def _handle_while(self, cmd: MtrCommand) -> None:
        """Handle --while: loop execution."""
        cond = cmd.condition
        if cond:
            result = self._evaluate_condition(cond)
            self._block_stack.append({
                'type': 'while',
                'condition': result,
                'start_cmd_idx': self._commands_executed - 1,
                'depth': len(self._block_stack),
            })

    def _handle_end(self, cmd: MtrCommand) -> None:
        """Handle --end: end of if/while block."""
        if self._block_stack:
            self._block_stack.pop()

    def _handle_assert(self, cmd: MtrCommand) -> None:
        """Handle --assert: assertion."""
        cond = cmd.condition
        if cond:
            result = self._evaluate_condition(cond)
            if not result:
                raise MtrTestFailed(
                    f"Assertion failed: {cmd.raw_text}")

    def _evaluate_condition(self, cond: ConditionExpr) -> bool:
        """Evaluate a condition expression."""
        return self._variables.evaluate_condition(
            var_name=cond.var_name,
            negated=cond.negated,
            operator=cond.operator.value if cond.operator else None,
            right_operand=cond.right_operand,
        )

    # Result formatting handlers

    def _handle_sorted_result(self, cmd: MtrCommand) -> None:
        self._result_processor.set_sorted_result(0)

    def _handle_partially_sorted_result(self, cmd: MtrCommand) -> None:
        self._result_processor.set_sorted_result(cmd.sort_start_column)

    def _handle_lowercase(self, cmd: MtrCommand) -> None:
        self._result_processor.set_lowercase()

    def _handle_vertical_results(self, cmd: MtrCommand) -> None:
        self._result_processor.set_vertical(True)

    def _handle_horizontal_results(self, cmd: MtrCommand) -> None:
        self._result_processor.set_vertical(False)

    def _handle_replace_column(self, cmd: MtrCommand) -> None:
        self._result_processor.set_replace_column(cmd.replace_columns)

    def _handle_replace_result(self, cmd: MtrCommand) -> None:
        self._result_processor.set_replace_result(cmd.replace_results)

    def _handle_replace_regex(self, cmd: MtrCommand) -> None:
        self._result_processor.set_replace_regex(cmd.replace_regexes)

    def _handle_replace_numeric_round(self, cmd: MtrCommand) -> None:
        self._result_processor.set_numeric_round(cmd.numeric_round_precision)

    # Connection handlers

    def _handle_connect(self, cmd: MtrCommand) -> None:
        spec = cmd.connect_spec
        if spec:
            self._connections.connect(
                name=spec.connection_name,
                host=spec.host or "localhost",
                port=spec.port or 3306,
                user=spec.user or "root",
                password=spec.password,
                database=spec.database,
                connector=self._connector,
            )

    def _handle_disconnect(self, cmd: MtrCommand) -> None:
        self._connections.disconnect(cmd.connection_name)

    def _handle_connection(self, cmd: MtrCommand) -> None:
        self._connections.select(cmd.connection_name)

    def _handle_dirty_close(self, cmd: MtrCommand) -> None:
        self._connections.disconnect(cmd.connection_name, dirty=True)

    def _handle_change_user(self, cmd: MtrCommand) -> None:
        self._connections.change_user(
            user=cmd.change_user_name,
            password=cmd.change_user_password,
            database=cmd.change_user_database,
        )

    def _handle_send_quit(self, cmd: MtrCommand) -> None:
        self._connections.send_quit(cmd.connection_name)

    def _handle_reset_connection(self, cmd: MtrCommand) -> None:
        self._connections.reset_connection()

    def _handle_ping(self, cmd: MtrCommand) -> None:
        self._connections.ping()

    def _handle_send(self, cmd: MtrCommand) -> None:
        if cmd.argument:
            sql = self._variables.substitute(cmd.argument)
            self._connections.mark_send(sql)
            self._execute_sql(sql, flags=1)  # SEND only
        else:
            # Mark next query as send-only
            pass

    def _handle_send_eval(self, cmd: MtrCommand) -> None:
        sql = self._variables.substitute(cmd.argument)
        self._connections.mark_send(sql)
        self._execute_sql(sql, flags=1)  # SEND only

    def _handle_reap(self, cmd: MtrCommand) -> None:
        sql = self._connections.reap()
        if sql:
            result = self._execute_sql(sql, flags=2)  # REAP only
            self._output_result(sql, result, line_no=cmd.line_no)

    def _handle_query_attributes(self, cmd: MtrCommand) -> None:
        # Store for next query - implementation depends on connector
        log.debug("Query attributes: %s", cmd.query_attrs)

    # Source handler (already expanded by parser)

    def _handle_source(self, cmd: MtrCommand) -> None:
        # Source inclusion is handled by the parser during parse time.
        # This handler is a no-op at execution time.
        pass

    # Sleep handler

    def _handle_sleep(self, cmd: MtrCommand) -> None:
        if cmd.sleep_seconds > 0:
            time.sleep(cmd.sleep_seconds)

    # Delimiter handler

    def _handle_delimiter(self, cmd: MtrCommand) -> None:
        # Delimiter is tracked by the parser; this is a no-op at execution
        pass

    # Flow control handlers

    def _handle_exit(self, cmd: MtrCommand) -> None:
        raise MtrTestExit()

    def _handle_die(self, cmd: MtrCommand) -> None:
        msg = self._variables.substitute(cmd.die_message)
        raise MtrTestDied(msg)

    def _handle_skip(self, cmd: MtrCommand) -> None:
        msg = self._variables.substitute(cmd.skip_message)
        raise MtrTestSkipped(msg)

    # External command handlers

    def _handle_exec(self, cmd: MtrCommand) -> None:
        command = self._variables.substitute(cmd.exec_command, True)
        try:
            result = subprocess.run(
                command, shell=True, capture_output=True, text=True, timeout=60)
            if result.stdout:
                self._output.append(result.stdout)
        except subprocess.TimeoutExpired:
            self._output.append("ERROR: exec command timed out")
        except Exception as e:
            if self._state.abort_on_error:
                raise MtrError(f"exec failed: {e}")

    def _handle_execw(self, cmd: MtrCommand) -> None:
        # execw is for wide character commands on Windows
        self._handle_exec(cmd)

    def _handle_exec_background(self, cmd: MtrCommand) -> None:
        command = self._variables.substitute(cmd.exec_command, True)
        try:
            subprocess.Popen(command, shell=True,
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        except Exception as e:
            if self._state.abort_on_error:
                raise MtrError(f"exec_in_background failed: {e}")

    # Character set handler

    def _handle_character_set(self, cmd: MtrCommand) -> None:
        self._state.charset_name = cmd.charset_name

    # Result format handler

    def _handle_result_format(self, cmd: MtrCommand) -> None:
        self._state.result_format_version = cmd.result_format_version
        self._result_processor.result_format_version = cmd.result_format_version

    # Output redirect handler

    def _handle_output(self, cmd: MtrCommand) -> None:
        # Redirect output to file - implementation depends on use case
        pass

    # File operation handlers

    def _resolve_path(self, path: str) -> str:
        """Resolve a file path relative to vardir or mysql_test_dir."""
        if os.path.isabs(path):
            return path
        if self._vardir and not path.startswith('/'):
            return os.path.join(self._vardir, path)
        return path

    def _handle_write_file(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.file_path_arg)
        content = self._variables.substitute(cmd.file_content)
        with open(path, 'w') as f:
            f.write(content)

    def _handle_append_file(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.file_path_arg)
        content = self._variables.substitute(cmd.file_content)
        with open(path, 'a') as f:
            f.write(content)

    def _handle_cat_file(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.target_file)
        try:
            with open(path, 'r') as f:
                self._output.append(f.read())
        except FileNotFoundError:
            if self._state.abort_on_error:
                raise MtrError(f"File not found: {path}")

    def _handle_copy_file(self, cmd: MtrCommand) -> None:
        src = self._resolve_path(cmd.from_file)
        dst = self._resolve_path(cmd.to_file)
        shutil.copy2(src, dst)

    def _handle_move_file(self, cmd: MtrCommand) -> None:
        src = self._resolve_path(cmd.from_file)
        dst = self._resolve_path(cmd.to_file)
        shutil.move(src, dst)

    def _handle_remove_file(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.target_file)
        for attempt in range(cmd.retry + 1):
            try:
                os.remove(path)
                return
            except FileNotFoundError:
                return
            except Exception:
                if attempt < cmd.retry:
                    time.sleep(1)
                elif self._state.abort_on_error:
                    raise

    def _handle_file_exists(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.target_file)
        for attempt in range(cmd.retry + 1):
            if os.path.exists(path):
                return
            if attempt < cmd.retry:
                time.sleep(1)
        if not os.path.exists(path) and self._state.abort_on_error:
            raise MtrError(f"File does not exist: {path}")

    def _handle_mkdir(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.dir_path)
        os.makedirs(path, exist_ok=True)

    def _handle_rmdir(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.dir_path)
        os.rmdir(path)

    def _handle_force_rmdir(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.dir_path)
        shutil.rmtree(path, ignore_errors=True)

    def _handle_force_cpdir(self, cmd: MtrCommand) -> None:
        src = self._resolve_path(cmd.from_file)
        dst = self._resolve_path(cmd.to_file)
        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst)

    def _handle_list_files(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.dir_path)
        pattern = cmd.wildcard or "*"
        files = sorted(glob.glob(os.path.join(path, pattern)))
        self._output.append("\n".join(os.path.basename(f) for f in files))

    def _handle_list_files_write_file(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.dir_path)
        out_path = self._resolve_path(cmd.file_path_arg)
        pattern = cmd.wildcard or "*"
        files = sorted(glob.glob(os.path.join(path, pattern)))
        content = "\n".join(os.path.basename(f) for f in files)
        with open(out_path, 'w') as f:
            f.write(content)

    def _handle_list_files_append_file(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.dir_path)
        out_path = self._resolve_path(cmd.file_path_arg)
        pattern = cmd.wildcard or "*"
        files = sorted(glob.glob(os.path.join(path, pattern)))
        content = "\n".join(os.path.basename(f) for f in files)
        with open(out_path, 'a') as f:
            f.write(content)

    def _handle_diff_files(self, cmd: MtrCommand) -> None:
        file1 = self._resolve_path(cmd.from_file)
        file2 = self._resolve_path(cmd.to_file)
        import difflib
        with open(file1) as f1, open(file2) as f2:
            diff = difflib.unified_diff(f1.readlines(), f2.readlines())
            diff_text = ''.join(diff)
            if diff_text:
                self._output.append(diff_text)

    def _handle_chmod(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.chmod_file)
        mode = int(cmd.chmod_mode, 8)
        os.chmod(path, mode)

    def _handle_remove_files_wildcard(self, cmd: MtrCommand) -> None:
        path = self._resolve_path(cmd.dir_path)
        pattern = cmd.wildcard or "*"
        for f in glob.glob(os.path.join(path, pattern)):
            try:
                os.remove(f)
            except Exception:
                pass

    def _handle_copy_files_wildcard(self, cmd: MtrCommand) -> None:
        src_dir = self._resolve_path(cmd.from_file)
        dst_dir = self._resolve_path(cmd.to_file)
        pattern = cmd.wildcard or "*"
        for f in glob.glob(os.path.join(src_dir, pattern)):
            shutil.copy2(f, dst_dir)

    def _handle_perl(self, cmd: MtrCommand) -> None:
        """Execute perl script content."""
        script = cmd.file_content
        if not script:
            return
        # Write to temp file and execute
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pl',
                                          delete=False) as f:
            f.write(script)
            temp_path = f.name
        try:
            result = subprocess.run(
                ['perl', temp_path], capture_output=True, text=True, timeout=60)
            if result.stdout:
                self._output.append(result.stdout)
        except Exception as e:
            if self._state.abort_on_error:
                raise MtrError(f"perl execution failed: {e}")
        finally:
            os.unlink(temp_path)

    # Property toggle handlers

    def _handle_enable_query_log(self, cmd: MtrCommand) -> None:
        self._result_processor.disable_query_log = False

    def _handle_disable_query_log(self, cmd: MtrCommand) -> None:
        self._result_processor.disable_query_log = True

    def _handle_enable_result_log(self, cmd: MtrCommand) -> None:
        self._result_processor.disable_result_log = False

    def _handle_disable_result_log(self, cmd: MtrCommand) -> None:
        self._result_processor.disable_result_log = True

    def _handle_enable_warnings(self, cmd: MtrCommand) -> None:
        self._result_processor.disable_warnings = False

    def _handle_disable_warnings(self, cmd: MtrCommand) -> None:
        self._result_processor.disable_warnings = True

    def _handle_enable_info(self, cmd: MtrCommand) -> None:
        self._result_processor.display_info = True

    def _handle_disable_info(self, cmd: MtrCommand) -> None:
        self._result_processor.display_info = False

    def _handle_enable_metadata(self, cmd: MtrCommand) -> None:
        self._result_processor.display_metadata = True

    def _handle_disable_metadata(self, cmd: MtrCommand) -> None:
        self._result_processor.display_metadata = False

    def _handle_enable_abort_on_error(self, cmd: MtrCommand) -> None:
        self._state.abort_on_error = True

    def _handle_disable_abort_on_error(self, cmd: MtrCommand) -> None:
        self._state.abort_on_error = False

    def _handle_enable_ps_protocol(self, cmd: MtrCommand) -> None:
        self._state.ps_protocol_enabled = True

    def _handle_disable_ps_protocol(self, cmd: MtrCommand) -> None:
        self._state.ps_protocol_enabled = False

    def _handle_enable_reconnect(self, cmd: MtrCommand) -> None:
        self._state.reconnect_enabled = True

    def _handle_disable_reconnect(self, cmd: MtrCommand) -> None:
        self._state.reconnect_enabled = False

    def _handle_enable_async_client(self, cmd: MtrCommand) -> None:
        self._state.async_client_enabled = True

    def _handle_disable_async_client(self, cmd: MtrCommand) -> None:
        self._state.async_client_enabled = False

    def _handle_enable_testcase(self, cmd: MtrCommand) -> None:
        self._state.testcase_disabled = False

    def _handle_disable_testcase(self, cmd: MtrCommand) -> None:
        self._state.testcase_disabled = True

    def _handle_enable_connect_log(self, cmd: MtrCommand) -> None:
        self._result_processor.disable_connect_log = False

    def _handle_disable_connect_log(self, cmd: MtrCommand) -> None:
        self._result_processor.disable_connect_log = True

    def _handle_enable_session_track_info(self, cmd: MtrCommand) -> None:
        pass

    def _handle_disable_session_track_info(self, cmd: MtrCommand) -> None:
        pass

    # Replication handlers (stubs for non-MySQL DBMS)

    def _handle_save_master_pos(self, cmd: MtrCommand) -> None:
        log.debug("save_master_pos: stub (replication not supported)")

    def _handle_sync_with_master(self, cmd: MtrCommand) -> None:
        log.debug("sync_with_master: stub (replication not supported)")

    def _handle_sync_slave_with_master(self, cmd: MtrCommand) -> None:
        log.debug("sync_slave_with_master: stub (replication not supported)")

    def _handle_wait_for_slave_to_stop(self, cmd: MtrCommand) -> None:
        log.debug("wait_for_slave_to_stop: stub (replication not supported)")

    def _handle_send_shutdown(self, cmd: MtrCommand) -> None:
        log.debug("send_shutdown: stub (server control not supported)")

    def _handle_shutdown_server(self, cmd: MtrCommand) -> None:
        log.debug("shutdown_server: stub (server control not supported)")

    # Timer handlers

    def _handle_start_timer(self, cmd: MtrCommand) -> None:
        self._state.timer_start = time.time()

    def _handle_end_timer(self, cmd: MtrCommand) -> None:
        elapsed = time.time() - self._state.timer_start
        self._output.append(f"Timer: {elapsed:.3f}s")

    # Other handlers

    def _handle_skip_if_hypergraph(self, cmd: MtrCommand) -> None:
        # MySQL-specific, ignore for cross-DBMS
        pass

    def _handle_run_with_if_pq(self, cmd: MtrCommand) -> None:
        # MySQL-specific parallel query testing
        pass
