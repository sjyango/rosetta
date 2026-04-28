"""Handler for 'rosetta update' — self-update to latest PyPI version."""

import json
import subprocess
import sys
import urllib.request
from importlib.metadata import version

from .result import CommandResult

_PYPI_JSON_URL = "https://pypi.org/pypi/rosetta-sql/json"
_PACKAGE_NAME = "rosetta-sql"


def handle_update(args, output) -> CommandResult:
    """Check for updates and upgrade rosetta to the latest PyPI release.

    Steps:
      1. Query PyPI JSON API for latest version
      2. Compare with locally installed version
      3. If behind → pip install --upgrade rosetta-sql
      4. If up-to-date → inform user
    """
    is_json = getattr(args, "json", False)

    # Step 1: Get local version
    local_ver = _get_local_version()

    # Step 2: Get remote (PyPI) latest version
    if not is_json:
        from rich.console import Console
        Console().print("  [dim]Checking for updates...[/dim]")
    pypi_info = _fetch_pypi_info()
    if pypi_info is None:
        return CommandResult.failure(
            "Failed to fetch version info from PyPI. Check your network.",
        )
    remote_ver = pypi_info["info"]["version"]

    # Step 3: Compare
    if _version_tuple(local_ver) >= _version_tuple(remote_ver):
        if is_json:
            return CommandResult.success(
                "update",
                {"status": "up_to_date", "version": local_ver,
                 "message": f"Already at latest version ({local_ver})"},
            )
        else:
            from rich.console import Console
            Console().print(
                f"\n  [green bold]✓[/green bold] Already up to date: "
                f"rosetta {local_ver}\n",
            )
            return CommandResult.success("update")

    # Step 4: Upgrade
    if not is_json:
        from rich.console import Console
        console = Console()
        console.print(f"\n  [yellow]Update available:[/yellow]")
        console.print(f"     Current : {local_ver}")
        console.print(f"     Latest  : {remote_ver}")
        console.print()
        Console().print("  [dim]Upgrading...[/dim]")

    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--upgrade",
         _PACKAGE_NAME, "--quiet"],
        capture_output=True,
        timeout=120,
    )
    if result.returncode != 0:
        return CommandResult.failure(
            f"pip install failed:\n{result.stderr.decode(errors='replace')}",
        )

    final_ver = _get_local_version()
    if is_json:
        return CommandResult.success(
            "update",
            {
                "status": "updated",
                "old_version": local_ver,
                "new_version": final_ver,
            },
        )
    else:
        from rich.console import Console
        Console().print(
            f"\n  [green bold]✓[/green bold] Updated to "
            f"[bold cyan]{final_ver}[/bold cyan]\n",
        )
        return CommandResult.success("update")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_local_version() -> str:
    try:
        return version(_PACKAGE_NAME)
    except Exception:
        return "unknown"


def _fetch_pypi_info():
    """Query PyPI JSON API. Returns parsed dict or None on failure."""
    try:
        req = urllib.request.Request(_PYPI_JSON_URL, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _version_tuple(v: str) -> tuple:
    """Parse '1.2.3' into comparable tuple of ints."""
    parts = []
    for part in v.split("."):
        # Handle suffixes like .dev1, .a1, .rc1 — treat as 0
        num = ""
        for ch in part:
            if ch.isdigit():
                num += ch
            else:
                break
        parts.append(int(num) if num else 0)
    return tuple(parts) if parts else (0,)
