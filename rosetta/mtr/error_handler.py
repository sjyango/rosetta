"""Error handling for MTR test execution.

Implements the --error directive semantics from mysqltest.cc,
supporting numeric error codes, SQLSTATE codes, error name codes,
and variable references.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Set

log = logging.getLogger("rosetta.mtr")


class ErrorType(Enum):
    """Type of error specification."""
    ERRNO = auto()       # Numeric error code
    SQLSTATE = auto()    # SQLSTATE (5-char, S prefix)
    ERROR_NAME = auto()  # Symbolic error name (E/C prefix)
    VARIABLE = auto()    # Variable reference ($var)


@dataclass
class ExpectedError:
    """A single expected error specification.

    From mysqltest.cc do_error() (line 6293+):
      - S07000     -> SQLSTATE
      - ER_XXX     -> Error name (E/C prefix)
      - 1045       -> Numeric errno
      - $var       -> Variable reference
    """
    raw: str
    error_type: ErrorType
    value: str  # The resolved value (error code string, SQLSTATE, name)
    error_code: Optional[int] = None  # Resolved numeric code


class MtrError(Exception):
    """Base exception for MTR test errors."""
    pass


class MtrTestSkipped(MtrError):
    """Test was skipped via --skip."""
    pass


class MtrTestDied(MtrError):
    """Test was aborted via --die."""
    pass


class MtrTestFailed(MtrError):
    """Test assertion failed or unexpected result."""
    pass


class MtrTestExit(MtrError):
    """Test exited early via --exit."""
    pass


class ErrorHandler:
    """Handles expected error specifications and error matching.

    Manages the --error directive state, which declares the set of
    expected errors for the next SQL statement. If the statement
    produces one of these errors, it's considered expected rather
    than a failure.
    """

    # Common MySQL error codes mapping (subset for cross-DBMS use)
    _COMMON_ERROR_NAMES: dict = {
        "ER_NO_SUCH_TABLE": 1146,
        "ER_TABLE_EXISTS_ERROR": 1050,
        "ER_DUP_ENTRY": 1062,
        "ER_DUP_KEY": 1022,
        "ER_BAD_FIELD_ERROR": 1054,
        "ER_PARSE_ERROR": 1064,
        "ER_ACCESS_DENIED_ERROR": 1045,
        "ER_DBACCESS_DENIED_ERROR": 1044,
        "ER_WRONG_VALUE_COUNT": 1058,
        "ER_BAD_NULL_ERROR": 1048,
        "ER_NON_UNIQ_ERROR": 1052,
        "ER_UNKNOWN_TABLE": 1109,
        "ER_SYNTAX_ERROR": 1149,
        "ER_NORMAL_SHUTDOWN": 1001,
        "ER_KEY_NOT_FOUND": 1032,
        "ER_NOT_SUPPORTED_YET": 1235,
        "ER_SP_DOES_NOT_EXIST": 1305,
        "ER_VIEW_INVALID": 1356,
        "ER_WRONG_OBJECT": 1347,
        "ER_NO_SUCH_INDEX": 1176,
        "ER_TRUNCATED_WRONG_VALUE": 1292,
        "ER_DATA_TOO_LONG": 1406,
        "ER_WARN_DATA_OUT_OF_RANGE": 1264,
        "ER_DIVISION_BY_ZERO": 1365,
        "ER_DATA_OVERFLOW": 1268,
        "ER_WRONG_VALUE_FOR_VAR": 1231,
        "ER_UNKNOWN_SYSTEM_VARIABLE": 1193,
        "ER_NOT_SUPPORTED_AUTH_MODE": 1251,
        "ER_UNKNOWN_ERROR": 1105,

        # Client errors (CR_ prefix)
        "CR_SERVER_LOST": 2013,
        "CR_SERVER_GONE_ERROR": 2006,
        "CR_CONN_HOST_ERROR": 2003,
        "CR_CONNECTION_ERROR": 2002,

        # Extended error names used in MTR tests
        "ER_DB_CREATE_EXISTS": 1007,
        "ER_DB_DROP_EXISTS": 1008,
        "ER_DB_DROP_RMDIR": 1010,
        "ER_DISK_FULL": 1021,
        "ER_DUP_UNIQUE": 1020,
        "ER_CHECKREAD": 1023,
        "ER_OUTOFMEMORY": 1037,
        "ER_CON_COUNT_ERROR": 1040,
        "ER_OUT_OF_RESOURCES": 1041,
        "ER_HOST_IS_BLOCKED": 1129,
        "ER_HOST_NOT_PRIVILEGED": 1130,
        "ER_ABORTING_CONNECTION": 1152,
        "ER_NET_PACKET_TOO_LARGE": 1153,
        "ER_NET_READ_ERROR_FROM_PIPE": 1154,
        "ER_NET_FCNTL_ERROR": 1155,
        "ER_NET_PACKETS_OUT_OF_ORDER": 1156,
        "ER_NET_UNCOMPRESS_ERROR": 1157,
        "ER_NET_READ_ERROR": 1158,
        "ER_NET_READ_INTERRUPTED": 1159,
        "ER_NET_ERROR_ON_WRITE": 1160,
        "ER_NET_WRITE_INTERRUPTED": 1161,
        "ER_TOO_LONG_STRING": 1163,
        "ER_TABLE_CANT_HANDLE_BLOB": 1171,
        "ER_TABLE_CANT_HANDLE_AUTO_INCREMENT": 1172,
        "ER_WRONG_COLUMN_NAME": 1166,
        "ER_WRONG_KEY_COLUMN": 1167,
        "ER_WRONG_MRG_TABLE": 1168,
        "ER_DUP_KEYNAME": 1173,
        "ER_DUP_FIELDNAME": 1060,
        "ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT": 1222,
        "ER_CANT_AGGREGATE_2COLLATIONS": 1267,
        "ER_CANT_AGGREGATE_3COLLATIONS": 1270,
        "ER_TOO_BIG_ROWSIZE": 1393,
        "ER_REQUIRES_PRIMARY_KEY": 1173,
    }

    def __init__(self):
        self._expected: List[ExpectedError] = []
        self._error_name_map: dict = dict(self._COMMON_ERROR_NAMES)

    def register_error_name(self, name: str, code: int) -> None:
        """Register a custom error name to code mapping."""
        self._error_name_map[name.upper()] = code

    def parse_error_specs(self, raw_args: str) -> List[ExpectedError]:
        """Parse --error arguments into a list of ExpectedError.

        Syntax: --error <spec>[,<spec>...]
        Where <spec> can be:
          - Numeric: 1045
          - SQLSTATE: S07000
          - Error name: ER_ACCESS_DENIED_ERROR or CR_SERVER_LOST
          - Variable: $mysql_errno

        Args:
            raw_args: The comma-separated error specifications.

        Returns:
            List of parsed ExpectedError objects.
        """
        specs = []
        # Remove trailing comments
        args = raw_args.split('#')[0].strip()
        if not args:
            return specs

        for part in args.split(','):
            part = part.strip()
            if not part:
                continue

            if part.startswith('$'):
                # Variable reference
                specs.append(ExpectedError(
                    raw=part,
                    error_type=ErrorType.VARIABLE,
                    value=part,
                ))
            elif part.startswith('S') and len(part) == 6:
                # SQLSTATE: S followed by 5 uppercase alphanumeric chars
                sqlstate = part[1:]
                if re.match(r'^[0-9A-Z]{5}$', sqlstate):
                    specs.append(ExpectedError(
                        raw=part,
                        error_type=ErrorType.SQLSTATE,
                        value=sqlstate,
                    ))
                else:
                    raise MtrError(
                        f"Invalid SQLSTATE: {part} "
                        f"(must be S followed by 5 uppercase alphanumeric)")
            elif part[0] in ('E', 'C') and len(part) > 1:
                # Error name (must start with uppercase E or C)
                name = part
                code = self._error_name_map.get(name.upper(), -1)
                if code == -1:
                    # Unknown error name - store without code, will be
                    # resolved at execution time or matched by name
                    log.warning("Unknown MTR error name: %s", name)
                    specs.append(ExpectedError(
                        raw=part,
                        error_type=ErrorType.ERROR_NAME,
                        value=name,
                        error_code=None,
                    ))
                else:
                    specs.append(ExpectedError(
                        raw=part,
                        error_type=ErrorType.ERROR_NAME,
                        value=name,
                        error_code=code,
                    ))
            elif part[0] in ('e', 'c'):
                raise MtrError(
                    f"Error name must start with uppercase E or C, got: {part}")
            elif part[0] == 's':
                raise MtrError(
                    f"SQLSTATE must start with uppercase S, got: {part}")
            else:
                # Numeric error code
                try:
                    code = int(part)
                    specs.append(ExpectedError(
                        raw=part,
                        error_type=ErrorType.ERRNO,
                        value=part,
                        error_code=code,
                    ))
                except ValueError:
                    raise MtrError(
                        f"Invalid error specification: {part!r}")

        return specs

    def set_expected(self, specs: List[ExpectedError]) -> None:
        """Set the expected errors for the next statement."""
        self._expected = specs

    def clear_expected(self) -> None:
        """Clear expected errors (after a non-error, non-comment, non-if command)."""
        self._expected = []

    @property
    def expected(self) -> List[ExpectedError]:
        """Current expected error list."""
        return self._expected

    def is_error_expected(self, error_code: int,
                          sqlstate: str = "",
                          variable_store=None) -> bool:
        """Check if the given error matches any expected error spec.

        Args:
            error_code: The numeric error code from the DBMS.
            sqlstate: The SQLSTATE code (5-char).
            variable_store: VariableStore for resolving $var references.

        Returns:
            True if the error is expected.
        """
        if not self._expected:
            return False

        for spec in self._expected:
            if spec.error_type == ErrorType.ERRNO:
                if spec.error_code == error_code:
                    return True
            elif spec.error_type == ErrorType.SQLSTATE:
                if sqlstate and spec.value == sqlstate:
                    return True
            elif spec.error_type == ErrorType.ERROR_NAME:
                if spec.error_code == error_code:
                    return True
            elif spec.error_type == ErrorType.VARIABLE:
                if variable_store:
                    try:
                        var_val = variable_store.get(spec.value)
                        if var_val:
                            try:
                                if int(var_val) == error_code:
                                    return True
                            except ValueError:
                                pass
                    except Exception:
                        pass

        return False

    def get_expected_codes(self, variable_store=None) -> Set[int]:
        """Get the set of all expected error codes (for display/logging)."""
        codes = set()
        for spec in self._expected:
            if spec.error_code is not None:
                codes.add(spec.error_code)
            elif spec.error_type == ErrorType.VARIABLE and variable_store:
                try:
                    val = variable_store.get(spec.value)
                    codes.add(int(val))
                except (ValueError, TypeError, Exception):
                    pass
        return codes
