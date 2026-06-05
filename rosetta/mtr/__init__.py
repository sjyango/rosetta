"""Rosetta MTR (MySQL Test Run) framework.

A complete Python reimplementation of MySQL's mysqltest program,
enabling non-MySQL DBMS systems to leverage existing MTR .test files
for integration testing.
"""

from .nodes import (
    MtrCommand,
    MtrCommandType,
    MtrBlock,
    MtrIfBlock,
    MtrWhileBlock,
    MtrTestFile,
)
from .parser import MtrParser
from .executor import MtrExecutor
from .connection import Connection, ConnectionManager
from .variable import VariableStore
from .result_processor import ResultProcessor
from .error_handler import ErrorHandler, MtrError
from .adapter import RosettaDBConnector, run_mtr_test, parse_mtr_to_statements

__all__ = [
    "MtrCommand",
    "MtrCommandType",
    "MtrBlock",
    "MtrIfBlock",
    "MtrWhileBlock",
    "MtrTestFile",
    "MtrParser",
    "MtrExecutor",
    "Connection",
    "ConnectionManager",
    "VariableStore",
    "ResultProcessor",
    "ErrorHandler",
    "MtrError",
    "RosettaDBConnector",
    "run_mtr_test",
    "parse_mtr_to_statements",
]
