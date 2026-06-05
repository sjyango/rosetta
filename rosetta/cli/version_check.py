"""Auto version check on startup — notify user if a newer release is available."""

import os
import subprocess
import sys
import threading

from .update_cmd import _fetch_pypi_info, _get_local_version, _version_tuple, _PACKAGE_NAME

_SUPPRESS_FILE = os.path.expanduser("~/.rosetta/.update_suppress")


def check_update_on_startup():
    """Check for newer rosetta version and prompt user to update.

    Called at CLI startup (human mode only). Skips silently if:
    - Suppress file exists (user chose "don't ask again")
    - Network is unreachable
    - Already at latest version
    """
    # Check suppress file
    if os.path.isfile(_SUPPRESS_FILE):
        return

    # Get local version
    local_ver = _get_local_version()
    if local_ver == "unknown":
        return

    # Query PyPI with spinner (shown only if check takes > 0.5s)
    from rich.console import Console
    console = Console(stderr=True)

    pypi_info = [None]
    done = threading.Event()

    def _fetch():
        try:
            pypi_info[0] = _fetch_pypi_info()
        except Exception:
            pass
        done.set()

    t = threading.Thread(target=_fetch, daemon=True)
    t.start()

    # Wait briefly — if fast, no spinner needed
    if not done.wait(timeout=0.5):
        # Still waiting, show spinner
        with console.status(
            "  [dim]Checking for updates...[/dim]",
            spinner="dots",
        ):
            done.wait(timeout=10)

    if pypi_info[0] is None:
        return

    remote_ver = pypi_info[0]["info"]["version"]

    # Compare versions
    if _version_tuple(local_ver) >= _version_tuple(remote_ver):
        return  # Already up-to-date

    # New version available — prompt user
    console.print(
        f"\n  [yellow bold]Update available:[/yellow bold] "
        f"[dim]{local_ver}[/dim] [yellow]→[/yellow] "
        f"[bold cyan]{remote_ver}[/bold cyan]")
    console.print(
        f"  [dim]Update now? "
        f"[bold]Y[/bold]=update  [bold]n[/bold]=skip  "
        f"[bold]s[/bold]=don't ask again[/dim]")

    try:
        choice = input("  > ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        console.print()
        return

    if choice in ("y", "yes", ""):
        # Update
        console.print("  [dim]Upgrading...[/dim]")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--upgrade",
             _PACKAGE_NAME, "--quiet"],
            capture_output=True,
            timeout=120,
        )
        if result.returncode == 0:
            final_ver = _get_local_version()
            console.print(
                f"  [green bold]✓[/green bold] Updated to "
                f"[bold cyan]{final_ver}[/bold cyan]\n")
        else:
            console.print(
                f"  [red bold]✗[/red bold] Update failed. "
                f"Run [bold]rosetta update[/bold] manually.\n")
    elif choice in ("s", "suppress"):
        # Suppress future checks
        try:
            os.makedirs(os.path.dirname(_SUPPRESS_FILE), exist_ok=True)
            with open(_SUPPRESS_FILE, "w") as f:
                f.write(remote_ver + "\n")
        except Exception:
            pass
        console.print("  [dim]Update prompts suppressed. "
                      "Run [bold]rosetta update[/bold] to check manually.[/dim]\n")
    else:
        # Skip (N or anything else)
        console.print()
