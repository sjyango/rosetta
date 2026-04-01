"""
Unified command result structure for all CLI commands.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, Optional
import json


@dataclass
class CommandResult:
    """
    Unified result structure for all CLI commands.
    
    This ensures consistent output format for AI Agent consumption.
    
    Attributes:
        ok: Whether the command succeeded
        command: The command that was executed (e.g., "run mtr")
        timestamp: ISO format timestamp of execution
        data: Command-specific result data
        error: Error message if command failed
    """
    ok: bool
    command: str
    timestamp: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    
    @classmethod
    def success(cls, command: str, data: Optional[Dict[str, Any]] = None) -> "CommandResult":
        """Create a successful result."""
        return cls(
            ok=True,
            command=command,
            timestamp=datetime.now().isoformat(),
            data=data
        )
    
    @classmethod
    def failure(cls, error: str, command: str = "unknown") -> "CommandResult":
        """Create a failed result."""
        return cls(
            ok=False,
            command=command,
            timestamp=datetime.now().isoformat(),
            error=error
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)
    
    def exit_code(self) -> int:
        """Return appropriate exit code."""
        return 0 if self.ok else 1
