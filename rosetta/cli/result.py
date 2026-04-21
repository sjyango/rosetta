"""
Unified command result structure for all CLI commands.
"""

from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Any, Dict, Optional
import json


@dataclass
class CommandResult:
    """
    Unified result structure for all CLI commands.
    
    This ensures consistent output format for AI Agent consumption.
    
    Attributes:
        ok: Whether the command itself executed successfully
        command: The command that was executed (e.g., "run mtr")
        timestamp: ISO format timestamp of execution
        data: Command-specific result data
        error: Error message if command failed
        status: Execution status — "success", "partial", or "failure"
            - "success": Command completed fully, all results positive
            - "partial": Command completed, but some sub-results have failures
                         (e.g. some test cases failed). ok=True in this case.
            - "failure": Command failed to execute (config error, connection
                         error, etc.). ok=False in this case.
    """
    ok: bool
    command: str
    timestamp: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status: str = "success"
    
    @classmethod
    def success(cls, command: str, data: Optional[Dict[str, Any]] = None) -> "CommandResult":
        """Create a successful result."""
        return cls(
            ok=True,
            command=command,
            timestamp=datetime.now().isoformat(),
            data=data,
            status="success",
        )
    
    @classmethod
    def partial(cls, command: str, data: Optional[Dict[str, Any]] = None,
                warning: str = "") -> "CommandResult":
        """Create a partial-success result (command succeeded but some sub-items failed).
        
        Use this when the tool execution itself was successful, but some of the
        results contain failures (e.g. some MTR test cases failed).  The ``ok``
        field is True because the command did execute correctly; the ``status``
        field is "partial" to indicate mixed results; and ``warning`` carries a
        human-readable summary of what partially failed.
        """
        return cls(
            ok=True,
            command=command,
            timestamp=datetime.now().isoformat(),
            data=data,
            error=warning or None,
            status="partial",
        )
    
    @classmethod
    def failure(cls, error: str, command: str = "unknown",
                data: Optional[Dict[str, Any]] = None) -> "CommandResult":
        """Create a failed result."""
        return cls(
            ok=False,
            command=command,
            timestamp=datetime.now().isoformat(),
            data=data,
            error=error,
            status="failure",
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)
    
    def exit_code(self) -> int:
        """Return appropriate exit code.
        
        - 0: success (all results positive)
        - 1: failure (command could not execute)
        - 2: partial (command executed, but some sub-results have failures)
        """
        if self.status == "partial":
            return 2
        if self.ok:
            return 0
        return 1
