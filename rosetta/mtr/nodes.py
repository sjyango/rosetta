"""AST node definitions for MTR .test file parsing.

Based on the complete set of MTR directives from mysqltest.cc
(enum_commands + command_names arrays, lines 434-572).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Union


class MtrCommandType(Enum):
    """All supported MTR command types.

    Mirrors the enum_commands in mysqltest.cc (lines 434-539).
    """
    # Connection management
    CONNECTION = auto()       # --connection <name>
    CONNECT = auto()          # --connect <name>,<host>,...
    DISCONNECT = auto()       # --disconnect <name>
    DIRTY_CLOSE = auto()      # --dirty_close <name>
    CHANGE_USER = auto()      # --change_user <user>,<passwd>,<db>
    SEND_QUIT = auto()        # --send_quit <name>
    RESET_CONNECTION = auto() # --reset_connection
    PING = auto()             # --ping

    # Query execution
    QUERY = auto()            # --query <sql>  (implicit SQL)
    EVAL = auto()             # --eval <sql_with_vars>
    QUERY_VERTICAL = auto()   # --query_vertical <sql>
    QUERY_HORIZONTAL = auto() # --query_horizontal <sql>
    SEND = auto()             # --send [sql]
    SEND_EVAL = auto()        # --send_eval <sql>
    REAP = auto()             # --reap
    QUERY_ATTRIBUTES = auto() # --query_attributes <name1> <val1> ...

    # Error handling
    ERROR = auto()            # --error <err_spec>[,<err_spec>...]

    # Variable system
    LET = auto()              # --let $var = <value>
    INC = auto()              # --inc $var
    DEC = auto()              # --dec $var
    EXPR = auto()             # --expr $var = $op1 <operator> $op2

    # Conditionals & loops
    IF = auto()               # --if (<expr>) {
    WHILE = auto()            # --while (<expr>) {
    END = auto()              # --end
    ASSERT = auto()           # --assert (<expr>)

    # Result formatting
    SORTED_RESULT = auto()                # --sorted_result
    PARTIALLY_SORTED_RESULT = auto()      # --partially_sorted_result <col>
    VERTICAL_RESULTS = auto()             # --vertical_results
    HORIZONTAL_RESULTS = auto()           # --horizontal_results
    LOWERCASE = auto()                    # --lowercase_result

    # Replace directives
    REPLACE_RESULT = auto()    # --replace_result <from> <to> ...
    REPLACE_COLUMN = auto()    # --replace_column <col> <val> ...
    REPLACE_REGEX = auto()     # --replace_regex <pattern> <replacement> ...
    REPLACE_NUMERIC_ROUND = auto()  # --replace_numeric_round <precision>

    # Logging control
    ENABLE_QUERY_LOG = auto()
    DISABLE_QUERY_LOG = auto()
    ENABLE_RESULT_LOG = auto()
    DISABLE_RESULT_LOG = auto()
    ENABLE_CONNECT_LOG = auto()
    DISABLE_CONNECT_LOG = auto()
    ENABLE_WARNINGS = auto()
    DISABLE_WARNINGS = auto()
    ENABLE_INFO = auto()
    DISABLE_INFO = auto()
    ENABLE_SESSION_TRACK_INFO = auto()
    DISABLE_SESSION_TRACK_INFO = auto()
    ENABLE_METADATA = auto()
    DISABLE_METADATA = auto()
    ENABLE_ABORT_ON_ERROR = auto()
    DISABLE_ABORT_ON_ERROR = auto()
    ENABLE_PS_PROTOCOL = auto()
    DISABLE_PS_PROTOCOL = auto()
    ENABLE_RECONNECT = auto()
    DISABLE_RECONNECT = auto()
    ENABLE_ASYNC_CLIENT = auto()
    DISABLE_ASYNC_CLIENT = auto()
    ENABLE_TESTCASE = auto()
    DISABLE_TESTCASE = auto()

    # File operations
    REMOVE_FILE = auto()
    REMOVE_FILES_WILDCARD = auto()
    WRITE_FILE = auto()
    APPEND_FILE = auto()
    CAT_FILE = auto()
    COPY_FILE = auto()
    COPY_FILES_WILDCARD = auto()
    MOVE_FILE = auto()
    CHMOD = auto()
    MKDIR = auto()
    RMDIR = auto()
    FORCE_RMDIR = auto()
    FORCE_CPDIR = auto()
    LIST_FILES = auto()
    LIST_FILES_WRITE_FILE = auto()
    LIST_FILES_APPEND_FILE = auto()
    FILE_EXISTS = auto()       # --file_exists (mapped from file_exist)
    DIFF_FILES = auto()

    # External commands
    EXEC = auto()
    EXECW = auto()
    EXEC_BACKGROUND = auto()
    PERL = auto()
    SYSTEM = auto()            # alias for exec

    # Flow control
    EXIT = auto()
    DIE = auto()
    SKIP = auto()
    SOURCE = auto()
    SLEEP = auto()

    # Delimiter
    DELIMITER = auto()

    # Replication
    SAVE_MASTER_POS = auto()
    SYNC_WITH_MASTER = auto()
    SYNC_SLAVE_WITH_MASTER = auto()
    WAIT_FOR_SLAVE_TO_STOP = auto()

    # Server control
    SEND_SHUTDOWN = auto()
    SHUTDOWN_SERVER = auto()

    # Other
    ECHO = auto()
    CHARACTER_SET = auto()
    RESULT_FORMAT = auto()
    OUTPUT = auto()
    SKIP_IF_HYPERGRAPH = auto()
    RUN_WITH_IF_PQ = auto()
    START_TIMER = auto()
    END_TIMER = auto()

    # Internal types
    SQL = auto()               # Raw SQL statement
    COMMENT = auto()           # # comment line
    EMPTY_LINE = auto()
    UNKNOWN = auto()


class BlockOp(Enum):
    """Comparison operators for if/while/assert conditions."""
    EQ = "=="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="


@dataclass
class ConditionExpr:
    """A condition expression for if/while/assert blocks.

    Supports:
      - $var  (truthy check)
      - !$var (negated truthy check)
      - $var == value
      - $var != value
      - $var < N, $var <= N, $var > N, $var >= N
    """
    var_name: str
    negated: bool = False
    operator: Optional[BlockOp] = None
    right_operand: Optional[str] = None


@dataclass
class ErrorSpec:
    """An expected error specification for --error directive.

    Supports:
      - Numeric error code: 1045
      - SQLSTATE: S07000
      - Error name: ER_ACCESS_DENIED_ERROR, CR_SERVER_LOST
      - Variable reference: $mysql_errno
    """
    raw: str
    error_code: Optional[int] = None
    sqlstate: Optional[str] = None
    error_name: Optional[str] = None
    variable_ref: Optional[str] = None


@dataclass
class ReplaceColumnSpec:
    """Specification for --replace_column directive."""
    column_number: int
    replacement: str


@dataclass
class ReplaceResultSpec:
    """Specification for --replace_result directive."""
    from_str: str
    to_str: str


@dataclass
class ReplaceRegexSpec:
    """Specification for --replace_regex directive."""
    pattern: str
    replacement: str


@dataclass
class ConnectSpec:
    """Arguments for --connect directive."""
    connection_name: str = ""
    host: str = "localhost"
    user: str = ""
    password: str = ""
    database: str = ""
    port: int = 0
    socket: str = ""
    options: str = ""  # SSL, COMPRESS, PIPE, SOCKET, SHM, TCP
    default_auth: str = ""
    compression_algorithm: str = ""
    zstd_compression_level: str = ""


@dataclass
class MtrCommand:
    """A single MTR command (directive) parsed from a .test file.

    This is the unified AST node for all MTR directives.
    """
    cmd_type: MtrCommandType
    raw_text: str
    line_no: int
    file_path: str = ""

    # Common fields
    argument: str = ""  # The first argument (after directive name)

    # --error specs
    error_specs: List[ErrorSpec] = field(default_factory=list)

    # --let / --inc / --dec / --expr
    var_name: str = ""
    var_value: str = ""
    expr_operator: str = ""
    expr_operand1: str = ""
    expr_operand2: str = ""

    # --if / --while / --assert condition
    condition: Optional[ConditionExpr] = None

    # --source
    source_path: str = ""

    # --connect
    connect_spec: Optional[ConnectSpec] = None

    # --connection / --disconnect / --dirty_close / --send_quit
    connection_name: str = ""

    # --replace_column
    replace_columns: List[ReplaceColumnSpec] = field(default_factory=list)

    # --replace_result
    replace_results: List[ReplaceResultSpec] = field(default_factory=list)

    # --replace_regex
    replace_regexes: List[ReplaceRegexSpec] = field(default_factory=list)

    # --replace_numeric_round
    numeric_round_precision: int = -1

    # --sorted_result / --partially_sorted_result
    sort_start_column: int = 0

    # --write_file / --append_file / --perl
    file_path_arg: str = ""
    file_delimiter: str = "EOF"
    file_content: str = ""

    # --copy_file / --move_file / --diff_files
    from_file: str = ""
    to_file: str = ""

    # --chmod
    chmod_mode: str = ""
    chmod_file: str = ""

    # --mkdir / --rmdir / --force_rmdir / --force_cpdir / --list_files*
    dir_path: str = ""
    wildcard: str = ""
    retry: int = 0

    # --remove_file / --file_exists / --cat_file
    target_file: str = ""

    # --exec / --execw / --exec_background
    exec_command: str = ""

    # --sleep
    sleep_seconds: float = 0.0

    # --query_attributes
    query_attrs: List[tuple] = field(default_factory=list)  # [(name, value), ...]

    # --change_user
    change_user_name: str = ""
    change_user_password: str = ""
    change_user_database: str = ""
    change_user_reconnect: str = ""

    # --delimiter
    new_delimiter: str = ""

    # --character_set
    charset_name: str = ""

    # --result_format
    result_format_version: int = 1

    # --output
    output_file: str = ""

    # --skip
    skip_message: str = ""

    # --die
    die_message: str = ""

    # --disable_testcase / --enable_testcase
    bug_number: str = ""

    # --sync_with_master
    sync_offset: int = 0

    # --shutdown_server
    shutdown_timeout: int = 600
    shutdown_pid_file: str = ""

    # --enable_warnings / --disable_warnings
    warning_list: List[str] = field(default_factory=list)
    once: bool = False


@dataclass
class MtrBlock:
    """A block of commands (body of if/while)."""
    commands: List[MtrCommand] = field(default_factory=list)


@dataclass
class MtrIfBlock:
    """An if block with condition, body, and optional else branches."""
    condition: ConditionExpr
    body: MtrBlock
    else_body: Optional[MtrBlock] = None


@dataclass
class MtrWhileBlock:
    """A while block with condition and body."""
    condition: ConditionExpr
    body: MtrBlock


@dataclass
class MtrTestFile:
    """A complete parsed .test file."""
    file_path: str
    commands: List[MtrCommand] = field(default_factory=list)
