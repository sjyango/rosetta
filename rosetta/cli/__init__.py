"""
Rosetta CLI - Modern command-line interface for AI Agents and humans.

Human-readable output by default; use -j/--json for JSON output.
"""

from .main import main
from .result import CommandResult
from ..runner import _enter_interactive, parse_args

__all__ = ["main", "CommandResult", "_enter_interactive", "parse_args"]
