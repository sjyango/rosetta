"""Rich terminal UI for Rosetta."""

import logging
import threading
import time
from typing import Dict, List, Optional

from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import (BarColumn, MofNCompleteColumn, Progress,
                           SpinnerColumn, TextColumn, TimeElapsedColumn,
                           TimeRemainingColumn)
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from .models import CompareResult

# The real stderr console for final output and live progress.
console = Console(stderr=True)

_log = logging.getLogger("rosetta")
_BOX = "cyan"

# Collects rich renderables for final Panel output.
_renderables: List = []


def _add(renderable):
    """Add a renderable to the output buffer."""
    _renderables.append(renderable)


def flush_all(title: str = "Rosetta"):
    """Flush all buffered renderables as a single Panel."""
    if not _renderables:
        return

    group = Group(*_renderables)
    console.print(Panel(
        group,
        title=f"[bold]{title}[/bold]",
        border_style=_BOX,
        expand=True,
        padding=(0, 1),
    ))
    _renderables.clear()


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

BANNER_TEXT = (
    "[bold cyan]"
    r"  ____                _   _" "\n"
    r" |  _ \ ___  ___  ___| |_| |_ __ _" "\n"
    r" | |_) / _ \/ __|/ _ \ __| __/ _` |" "\n"
    r" |  _ < (_) \__ \  __/ |_| || (_| |" "\n"
    r" |_| \_\___/|___/\___|\__|\__\__,_|"
    "[/bold cyan]\n"
    "[dim]Cross-DBMS SQL Behavioral Consistency Verification[/dim]\n"
)


def print_banner():
    """Buffer the Rosetta banner."""
    _add(Text.from_markup(BANNER_TEXT))


# ---------------------------------------------------------------------------
# Phase headers
# ---------------------------------------------------------------------------

def print_phase(title: str, detail: str = ""):
    """Buffer a phase header."""
    text = f"[bold white]{title}[/bold white]"
    if detail:
        text += f"  [dim]{detail}[/dim]"
    _add(Text(""))
    _add(Rule(Text.from_markup(text), style=_BOX))


# ---------------------------------------------------------------------------
# Info messages
# ---------------------------------------------------------------------------

def print_info(msg: str, highlight: str = ""):
    """Buffer an informational line."""
    if highlight:
        _add(Text.from_markup(f"  [cyan]>[/cyan] {msg} [bold]{highlight}[/bold]"))
    else:
        _add(Text.from_markup(f"  [cyan]>[/cyan] {msg}"))


def print_success(msg: str):
    """Buffer a success message."""
    _add(Text.from_markup(f"  [green]✓[/green] {msg}"))


def print_warning(msg: str):
    """Buffer a warning message."""
    _add(Text.from_markup(f"  [yellow]⚠[/yellow] {msg}"))


def print_error(msg: str):
    """Buffer an error message."""
    _add(Text.from_markup(f"  [red]✗[/red] {msg}"))


# ---------------------------------------------------------------------------
# Execution progress bar
# ---------------------------------------------------------------------------

class ExecutionProgress:
    """Context manager for a DBMS execution progress bar.

    Multiple instances share a single rich Progress bar so that parallel
    DBMS executions are displayed simultaneously.  The shared Progress is
    created on the first ``__enter__`` and stopped when the last instance
    exits.
    """

    _lock = threading.Lock()
    _shared_progress: Optional[Progress] = None
    _ref_count = 0

    def __init__(self, dbms_name: str, total: int):
        self.dbms_name = dbms_name
        self.total = total
        self._task_id = None
        self._errors = 0
        self._executed = 0
        self._elapsed = 0.0
        self._start_time = 0.0

    # -- shared Progress lifecycle ------------------------------------------

    @classmethod
    def _acquire(cls) -> Progress:
        with cls._lock:
            if cls._shared_progress is None:
                cls._shared_progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[bold blue]{task.fields[dbms]}[/bold blue]"),
                    BarColumn(bar_width=40),
                    MofNCompleteColumn(),
                    TextColumn("[dim]|[/dim]"),
                    TimeElapsedColumn(),
                    TextColumn("[dim]|[/dim]"),
                    TimeRemainingColumn(),
                    TextColumn("{task.fields[status]}"),
                    console=console,
                    transient=True,
                )
                cls._shared_progress.start()
            cls._ref_count += 1
            return cls._shared_progress

    @classmethod
    def _release(cls):
        with cls._lock:
            cls._ref_count -= 1
            if cls._ref_count <= 0:
                if cls._shared_progress is not None:
                    cls._shared_progress.stop()
                    cls._shared_progress = None
                cls._ref_count = 0

    # -- context manager ----------------------------------------------------

    def __enter__(self):
        self._start_time = time.monotonic()
        progress = self._acquire()
        self._task_id = progress.add_task(
            "exec", total=self.total,
            dbms=self.dbms_name, status="",
        )
        return self

    def __exit__(self, *args):
        self._elapsed = time.monotonic() - self._start_time
        self._release()

    def advance(self, error: bool = False):
        """Advance progress by 1."""
        if error:
            self._errors += 1
        self._executed += 1
        status = (f"[red]{self._errors} err[/red]"
                  if self._errors else "[green]ok[/green]")
        prog = self.__class__._shared_progress
        if prog is not None:
            prog.update(self._task_id, advance=1, status=status)

    def set_status(self, text: str):
        """Set a custom status text."""
        prog = self.__class__._shared_progress
        if prog is not None:
            prog.update(self._task_id, status=text)

    def write_summary_to_buffer(self):
        """Write a static one-line summary into the buffer (call after exit)."""
        elapsed = f"{self._elapsed:.1f}s"
        if self._errors:
            status = f"[yellow]{self._executed} done, {self._errors} err[/yellow]"
        else:
            status = f"[green]{self._executed} done[/green]"
        _add(Text.from_markup(
            f"  [bold blue]{self.dbms_name}[/bold blue]  "
            f"{self._executed}/{self.total}  "
            f"[dim]{elapsed}[/dim]  {status}"
        ))


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

def print_summary(comparisons: Dict[str, CompareResult],
                  failed_connections: set = None):
    """Buffer a rich summary table of comparison results."""
    _add(Text(""))
    _add(Rule(Text.from_markup("[bold white]Summary[/bold white]"), style=_BOX))
    _add(Text(""))

    # Detect whether any comparison has whitelisted diffs
    has_wl = any(cmp.whitelisted > 0 for cmp in comparisons.values())
    # Detect whether any comparison has bug-marked diffs
    has_bug = any(cmp.bug_marked > 0 for cmp in comparisons.values())

    table = Table(
        header_style="bold",
        show_lines=False,
        padding=(0, 1),
        expand=True,
        show_edge=False,
    )

    table.add_column("Comparison", style="white", ratio=3)
    table.add_column("Status", justify="center", min_width=6)
    table.add_column("Match", justify="right", style="green")
    table.add_column("Mismatch", justify="right")
    if has_wl:
        table.add_column("Whitelist", justify="right")
    if has_bug:
        table.add_column("Bug", justify="right")
    table.add_column("Skip", justify="right", style="dim")
    table.add_column("Total", justify="right")
    table.add_column("Rate", justify="right", min_width=14)

    if failed_connections:
        for name in failed_connections:
            cols = [name, "[yellow]SKIP[/yellow]",
                    "-", "-"]
            if has_wl:
                cols.append("-")
            if has_bug:
                cols.append("-")
            cols += ["-", "-", "[dim]conn failed[/dim]"]
            table.add_row(*cols)

    all_pass = True
    for key, cmp in comparisons.items():
        effective_mismatch = cmp.effective_mismatched
        is_pass = effective_mismatch <= 0
        if not is_pass:
            all_pass = False

        status = ("[bold green]PASS[/bold green]" if is_pass
                  else "[bold red]FAIL[/bold red]")
        mismatch_style = "red bold" if effective_mismatch > 0 else "dim"
        rate = cmp.pass_rate
        rate_color = ("green" if rate >= 100
                      else "yellow" if rate >= 90
                      else "red")

        bar_len = 8
        filled = int(rate / 100 * bar_len)
        bar = (f"[{rate_color}]{'█' * filled}{'░' * (bar_len - filled)}"
               f"[/{rate_color}] {rate:.1f}%")

        cols = [
            key, status,
            str(cmp.matched),
            Text(str(effective_mismatch if effective_mismatch > 0 else 0),
                 style=mismatch_style),
        ]
        if has_wl:
            wl_text = (Text(str(cmp.whitelisted), style="yellow")
                       if cmp.whitelisted > 0
                       else Text("0", style="dim"))
            cols.append(wl_text)
        if has_bug:
            bug_text = (Text(str(cmp.bug_marked), style="red")
                        if cmp.bug_marked > 0
                        else Text("0", style="dim"))
            cols.append(bug_text)
        cols += [
            str(cmp.skipped),
            str(cmp.total_stmts),
            bar,
        ]
        table.add_row(*cols)

    _add(table)

    # Overall verdict
    _add(Text(""))
    if all_pass and not (failed_connections):
        _add(Text.from_markup("[bold green]  ★  OVERALL: ALL PASSED[/bold green]"))
    elif all_pass:
        _add(Text.from_markup(
            "[bold yellow]  ★  OVERALL: ALL COMPARED PASSED[/bold yellow]"
            "  [dim](some connections failed)[/dim]"))
    else:
        _add(Text.from_markup(
            "[bold red]  ★  OVERALL: DIFFERENCES FOUND[/bold red]"))

    return all_pass


# ---------------------------------------------------------------------------
# Report output
# ---------------------------------------------------------------------------

def print_report_file(path: str, label: str = ""):
    """Buffer a generated report file path."""
    label_text = f"[dim]{label}[/dim]  " if label else ""
    _add(Text.from_markup(f"  [green]✓[/green] {label_text}[bold]{path}[/bold]"))


# ---------------------------------------------------------------------------
# HTTP server panel
# ---------------------------------------------------------------------------

def print_server_info(url: str, directory: str, *,
                      history_url: str = ""):
    """Print the HTTP server panel (standalone, after main panel)."""
    lines = [
        f"[bold cyan]URL:[/bold cyan]      {url}",
        f"[dim]Dir:[/dim]      {directory}",
    ]
    if history_url:
        lines.append(f"[dim]History:[/dim]  {history_url}")
    lines.append("")
    lines.append("Press [bold]Ctrl+C[/bold] to stop")
    console.print()
    console.print(Panel("\n".join(lines),
                        title="[bold]HTML Report Server[/bold]",
                        border_style=_BOX, expand=False))


# ---------------------------------------------------------------------------
# Benchmark progress & summary
# ---------------------------------------------------------------------------

class BenchProgress:
    """Context manager for benchmark execution progress.

    Shows a live progress bar per DBMS during benchmark execution.
    Reuses the same shared Progress approach as ExecutionProgress.

    For SERIAL mode: shows iteration count (N/M)
    For CONCURRENT mode: shows time progress (20s/30s)
    """

    _lock = threading.Lock()
    _shared_progress: Optional[Progress] = None
    _ref_count = 0

    def __init__(self, dbms_name: str, total_queries: int, iterations: int,
                 is_concurrent: bool = False, duration: float = 0.0):
        self.dbms_name = dbms_name
        self.is_concurrent = is_concurrent
        if is_concurrent and duration > 0:
            self.total = int(duration)  # seconds for time-based progress
        else:
            self.total = total_queries * iterations
        self.duration = duration
        self._task_id = None
        self._completed = 0
        self._start_time = 0.0
        self._elapsed = 0.0

    @classmethod
    def _acquire(cls, is_concurrent: bool = False) -> Progress:
        with cls._lock:
            if cls._shared_progress is None:
                if is_concurrent:
                    # Time-based progress for concurrent mode
                    cls._shared_progress = Progress(
                        SpinnerColumn(),
                        TextColumn(
                            "[bold blue]{task.fields[dbms]}[/bold blue]"),
                        BarColumn(bar_width=40),
                        TextColumn("[cyan]{task.fields[elapsed_s]}s[/cyan]"),
                        TextColumn("[dim]/[/dim]"),
                        TextColumn("[cyan]{task.fields[total_s]}s[/cyan]"),
                        TextColumn("[dim]|[/dim]"),
                        TextColumn("{task.fields[status]}"),
                        console=console,
                        transient=True,
                    )
                else:
                    # Iteration-based progress for serial mode
                    cls._shared_progress = Progress(
                        SpinnerColumn(),
                        TextColumn(
                            "[bold blue]{task.fields[dbms]}[/bold blue]"),
                        BarColumn(bar_width=40),
                        MofNCompleteColumn(),
                        TextColumn("[dim]|[/dim]"),
                        TimeElapsedColumn(),
                        TextColumn("[dim]|[/dim]"),
                        TextColumn("{task.fields[status]}"),
                        console=console,
                        transient=True,
                    )
                cls._shared_progress.start()
            cls._ref_count += 1
            return cls._shared_progress

    @classmethod
    def _release(cls):
        with cls._lock:
            cls._ref_count -= 1
            if cls._ref_count <= 0:
                if cls._shared_progress is not None:
                    cls._shared_progress.stop()
                    cls._shared_progress = None
                cls._ref_count = 0

    def __enter__(self):
        self._start_time = time.monotonic()
        progress = self._acquire(is_concurrent=self.is_concurrent)
        if self.is_concurrent and self.duration > 0:
            self._task_id = progress.add_task(
                "bench", total=self.total,
                dbms=self.dbms_name, status="[dim]setup...[/dim]",
                elapsed_s=0, total_s=int(self.duration),
            )
        else:
            self._task_id = progress.add_task(
                "bench", total=self.total,
                dbms=self.dbms_name, status="[dim]warmup[/dim]",
            )
        return self

    def reset_timer(self):
        """Reset the start time for concurrent mode (call after setup)."""
        self._start_time = time.monotonic()

    def __exit__(self, *args):
        self._elapsed = time.monotonic() - self._start_time
        self._release()

    def advance(self, query_name: str = "", iteration: int = 0,
                total: int = 0, is_warmup: bool = False):
        """Advance overall progress by 1 (for serial mode).

        The progress bar tracks the overall test case count (warmup + iterations
        across all queries).  The status text shows which query is running and
        its per-query iteration count.

        Args:
            query_name: Current query name
            iteration: Current iteration for this query (1-indexed)
            total: Total iterations for this query
            is_warmup: Whether this is a warmup iteration
        """
        self._completed += 1
        if is_warmup:
            status = "[dim]warmup[/dim]"
        else:
            status = f"{query_name}"
        prog = self.__class__._shared_progress
        if prog is not None:
            prog.update(self._task_id, advance=1, status=status)

    def update_time(self, status: str = ""):
        """Update progress based on elapsed time (for concurrent mode).
        
        Args:
            status: Optional status text to display.
        """
        elapsed = time.monotonic() - self._start_time
        elapsed_int = int(elapsed)
        prog = self.__class__._shared_progress
        if prog is not None:
            prog.update(
                self._task_id,
                completed=elapsed_int,
                elapsed_s=elapsed_int,
                status=status,
            )

    def set_status(self, text: str):
        """Set custom status.
        
        Args:
            text: Status text to display.
        """
        prog = self.__class__._shared_progress
        if prog is not None:
            if self.is_concurrent and self.duration > 0:
                elapsed = time.monotonic() - self._start_time
                elapsed_int = int(elapsed)
                prog.update(
                    self._task_id,
                    status=text,
                    elapsed_s=elapsed_int,
                    completed=elapsed_int,
                )
            else:
                prog.update(self._task_id, status=text)

    def write_summary_to_buffer(self):
        """Write a one-line summary into the buffer."""
        elapsed = f"{self._elapsed:.1f}s"
        _add(Text.from_markup(
            f"  [bold blue]{self.dbms_name}[/bold blue]  "
            f"{self._completed} queries  "
            f"[dim]{elapsed}[/dim]  [green]done[/green]"
        ))


def print_bench_summary(result):
    """Buffer a rich benchmark summary table.

    Args:
        result: BenchmarkResult instance.
    """
    from .models import BenchmarkResult, WorkloadMode  # avoid circular at module level

    _add(Text(""))
    _add(Rule(Text.from_markup(
        "[bold white]Benchmark Summary[/bold white]"), style=_BOX))
    _add(Text(""))

    # Config info
    cfg = result.config
    mode_str = result.mode.name

    # Build config details based on mode
    if result.mode == WorkloadMode.CONCURRENT:
        config_parts = [
            f"Mode: [cyan]{mode_str}[/cyan]",
            f"Concurrency: [cyan]{cfg.concurrency}[/cyan]",
            f"Duration: [cyan]{cfg.duration}s[/cyan]",
        ]
        if cfg.ramp_up > 0:
            config_parts.append(f"Ramp-up: [cyan]{cfg.ramp_up}s[/cyan]")
        if cfg.warmup > 0:
            config_parts.append(f"Warmup: [cyan]{cfg.warmup}[/cyan]")
    else:
        config_parts = [
            f"Mode: [cyan]{mode_str}[/cyan]",
            f"Iterations: [cyan]{cfg.iterations}[/cyan]",
            f"Warmup: [cyan]{cfg.warmup}[/cyan]",
        ]

    _add(Text.from_markup(
        f"  Workload: [bold]{result.workload_name}[/bold]  "
        + "  ".join(config_parts) +
        f"  Timestamp: [dim]{result.timestamp}[/dim]"
    ))

    # Show profiling status (always visible)
    if getattr(cfg, 'profile', False):
        # Count flame graphs collected
        fg_count = sum(
            1 for dr in result.dbms_results
            for qs in dr.query_stats
            if qs.flamegraph_svg
        )
        _add(Text.from_markup(
            f"  Profiling: [bold red]🔥 ON[/bold red]  "
            f"[dim]{fg_count} flame graph(s) captured[/dim]"
        ))
    else:
        _add(Text.from_markup(
            f"  Profiling: [dim]OFF[/dim]"
        ))

    _add(Text(""))

    # Per-DBMS summary table
    table = Table(
        header_style="bold",
        show_lines=False,
        padding=(0, 1),
        expand=True,
        show_edge=False,
    )

    table.add_column("DBMS", style="bold blue", ratio=2)
    table.add_column("Queries", justify="right")
    table.add_column("Errors", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("QPS", justify="right", style="green")

    for dr in result.dbms_results:
        err_style = "red bold" if dr.total_errors > 0 else "dim"
        table.add_row(
            dr.dbms_name,
            str(dr.total_queries),
            Text(str(dr.total_errors), style=err_style),
            f"{dr.total_duration_s:.2f}s",
            f"{dr.overall_qps:.1f}",
        )

    _add(table)
    _add(Text(""))

    # Per-query comparison (if multiple DBMS)
    if len(result.dbms_results) >= 2:
        _add(Rule(Text.from_markup(
            "[bold white]Per-Query Comparison[/bold white]"), style=_BOX))
        _add(Text(""))

        # Collect all query names
        all_queries = []
        for dr in result.dbms_results:
            for qs in dr.query_stats:
                if qs.query_name not in all_queries:
                    all_queries.append(qs.query_name)

        for qname in all_queries:
            qtable = Table(
                title=f"[cyan]{qname}[/cyan]",
                header_style="bold",
                show_lines=False,
                padding=(0, 1),
                expand=True,
                show_edge=False,
            )
            qtable.add_column("DBMS", style="blue", ratio=2)
            qtable.add_column("Avg(ms)", justify="right")
            qtable.add_column("P50", justify="right")
            qtable.add_column("P95", justify="right")
            qtable.add_column("P99", justify="right")
            qtable.add_column("QPS", justify="right", style="green")

            for dr in result.dbms_results:
                qs = next(
                    (s for s in dr.query_stats
                     if s.query_name == qname), None)
                if qs:
                    qtable.add_row(
                        dr.dbms_name,
                        f"{qs.avg_ms:.2f}",
                        f"{qs.p50_ms:.2f}",
                        f"{qs.p95_ms:.2f}",
                        f"{qs.p99_ms:.2f}",
                        f"{qs.qps:.1f}",
                    )

            _add(qtable)
            _add(Text(""))


# ---------------------------------------------------------------------------
# Logging handler that uses rich
# ---------------------------------------------------------------------------

class RichLogHandler(logging.Handler):
    """Redirect log records to rich console with minimal formatting."""

    def emit(self, record):
        try:
            msg = self.format(record)
            if record.levelno >= logging.ERROR:
                print_error(msg)
            elif record.levelno >= logging.WARNING:
                print_warning(msg)
        except Exception:
            self.handleError(record)
