"""
Output formatter for CLI commands.

Provides JSON output by default (AI Agent friendly) and human-readable output
as an option.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .result import CommandResult


class OutputFormatter:
    """
    Format command results for output.
    
    Supports two output formats:
    - json: Machine-readable JSON (default, AI Agent friendly)
    - human: Human-readable format with colors and tables
    """
    
    def __init__(self, format: str = "json"):
        """
        Initialize output formatter.
        
        Args:
            format: Output format, either "json" or "human"
        """
        self.format = format
    
    def print(self, result: "CommandResult") -> None:
        """
        Print the command result.
        
        Args:
            result: CommandResult to print
        """
        if self.format == "json":
            self._print_json(result)
        else:
            self._print_human(result)
    
    def _print_json(self, result: "CommandResult") -> None:
        """Print result as JSON."""
        print(result.to_json())
    
    def _print_human(self, result: "CommandResult") -> None:
        """Print result in human-readable format."""
        try:
            from rich.console import Console
            from rich.table import Table
            
            console = Console()
            
            if result.ok:
                if result.data:
                    self._print_data_human(console, result.data)
                # Show warning for partial results (e.g. some test cases failed)
                if result.status == "partial" and result.error:
                    console.print(f"[yellow]Warning:[/yellow] {result.error}")
            else:
                if result.error:
                    console.print(f"[red]Error:[/red] {result.error}")
        except ImportError:
            # Fallback to plain text if rich is not available
            if result.ok:
                if result.data:
                    print(result.data)
                if result.status == "partial" and result.error:
                    print(f"Warning: {result.error}")
            else:
                if result.error:
                    print(f"Error: {result.error}")
    
    def _print_data_human(self, console, data: dict) -> None:
        """
        Print data in human-readable format with smart formatting.
        
        Args:
            console: Rich console instance
            data: Data dictionary to print
        """
        from rich.table import Table
        from rich.panel import Panel
        
        # Detect command type and format accordingly
        if "run_id" in data and "report_files" in data:
            # result show (must be checked before generic "dbms" list match)
            self._print_result_show(console, data)
        elif "dbms" in data and isinstance(data.get("dbms"), list):
            # status dbms or list dbms
            if "connected" in data:  # status dbms
                self._print_dbms_status(console, data)
            else:  # list dbms
                self._print_dbms_list(console, data)
        elif "runs" in data and isinstance(data.get("runs"), list):
            # result list / history
            self._print_history(console, data)
        elif "dbms_results" in data and isinstance(data.get("dbms_results"), list):
            # run bench result
            self._print_bench_result(console, data)
        elif "comparisons" in data and isinstance(data.get("comparisons"), dict):
            # run mtr result
            self._print_mtr_result(console, data)
        elif "results" in data and isinstance(data.get("results"), dict):
            # exec result
            self._print_exec_result(console, data)
        elif "databases" in data and isinstance(data.get("databases"), list):
            # config show
            self._print_config_show(console, data)
        elif all(not isinstance(v, (dict, list)) for v in data.values()):
            # Simple key-value data
            from rich import box
            table = Table(show_header=False, expand=True, box=box.ROUNDED)
            table.add_column("Key", style="cyan")
            table.add_column("Value")
            for k, v in data.items():
                table.add_row(str(k), str(v))
            console.print(table)
        else:
            # Fallback: print as formatted dict
            import json
            console.print(Panel(
                json.dumps(data, indent=2, ensure_ascii=False),
                title="[bold cyan]Result Data[/bold cyan]",
                title_align="left",
                padding=(0, 1)
            ))

    def _print_dbms_status(self, console, data: dict) -> None:
        """Print DBMS connection status."""
        from rich import box
        from rich.table import Table
        
        # Print DBMS table with summary in title
        dbms_list = data.get("dbms", [])
        if dbms_list:
            total = data.get('total', 0)
            connected = data.get('connected', 0)
            disconnected = data.get('disconnected', 0)
            table = Table(
                title=f"Total: {total}  Connected: {connected}  Disconnected: {disconnected}",
                expand=True,
                box=box.ROUNDED,
            )
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Host", no_wrap=True)
            table.add_column("Port", justify="right")
            table.add_column("Driver", no_wrap=True)
            table.add_column("Status", justify="center")
            table.add_column("Version")
            table.add_column("Latency", justify="right")
            
            for db in dbms_list:
                # Status with color
                if db.get("connected"):
                    status = "[green]✓ Connected[/green]"
                    version = db.get("version", "")
                elif db.get("port_reachable"):
                    status = "[red]✗ Auth Failed[/red]"
                    version = ""
                else:
                    status = "[red]✗ Unreachable[/red]"
                    version = ""
                
                # Latency
                latency = db.get("latency_ms")
                latency_str = f"{latency:.2f}ms" if latency else "-"
                
                table.add_row(
                    db.get("name", ""),
                    db.get("host", ""),
                    str(db.get("port", "")),
                    db.get("driver", ""),
                    status,
                    version,
                    latency_str
                )
            
            console.print(table)

    def _print_dbms_list(self, console, data: dict) -> None:
        """Print configured DBMS list."""
        from rich import box
        from rich.table import Table
        
        total = data.get('total', 0)
        dbms_list = data.get("dbms", [])
        
        if dbms_list:
            table = Table(
                title=f"Total: {total}  Configured DBMS",
                expand=True,
                box=box.ROUNDED,
            )
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Host", no_wrap=True)
            table.add_column("Port", justify="right")
            table.add_column("Driver", no_wrap=True)
            table.add_column("Version")
            table.add_column("Enabled", justify="center")
            
            for db in dbms_list:
                enabled = "[green]✓[/green]" if db.get("enabled") else "[red]✗[/red]"
                version = db.get("version", "") if db.get("enabled") else ""
                # Truncate version if too long
                if version and len(version) > 20:
                    version = version[:17] + "..."
                table.add_row(
                    db.get("name", ""),
                    db.get("host", ""),
                    str(db.get("port", "")),
                    db.get("driver", ""),
                    version,
                    enabled
                )

            console.print(table)

    def _print_config_show(self, console, data: dict) -> None:
        """Print configuration details."""
        from rich import box
        from rich.table import Table

        console.print(f"[cyan]Config Path:[/cyan] {data.get('config_path', '')}")
        console.print(f"[cyan]Total DBMS:[/cyan] {data.get('total_dbms', 0)}")
        console.print(f"[cyan]Enabled DBMS:[/cyan] {data.get('enabled_dbms', 0)}")
        console.print()

        databases = data.get("databases", [])
        if databases:
            table = Table(
                title="Database Configurations",
                expand=True,
                box=box.ROUNDED,
            )
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Host", no_wrap=True)
            table.add_column("Port", justify="right")
            table.add_column("User", no_wrap=True)
            table.add_column("Enabled", justify="center")
            table.add_column("Init SQL", justify="center")
            table.add_column("Skip Patterns", justify="right")
            
            for db in databases:
                enabled = "[green]✓[/green]" if db.get("enabled") else "[red]✗[/red]"
                has_init = "[green]✓[/green]" if db.get("has_init_sql") else "-"
                
                table.add_row(
                    db.get("name", ""),
                    db.get("host", ""),
                    str(db.get("port", "")),
                    db.get("user", ""),
                    enabled,
                    has_init,
                    str(db.get("skip_patterns_count", 0))
                )
            
            console.print(table)

    def _print_history(self, console, data: dict) -> None:
        """Print execution history (result list) with pagination."""
        from rich import box
        from rich.table import Table

        total = data.get("total", 0)
        page = data.get("page", 1)
        total_pages = data.get("total_pages", 1)
        per_page = data.get("per_page", 20)

        runs = data.get("runs", [])
        if not runs:
            console.print("[dim]No runs found.[/dim]")
            return

        title = f"History  (page {page}/{total_pages}, {total} total)"
        table = Table(
            title=title,
            show_header=True,
            header_style="bold cyan",
            pad_edge=True,
            expand=True,
            box=box.ROUNDED,
        )
        table.add_column("#", style="dim", justify="right", no_wrap=True)
        table.add_column("Run ID", style="cyan")
        table.add_column("Type", no_wrap=True)
        table.add_column("DBMS")
        table.add_column("Timestamp", no_wrap=True)

        for run in runs:
            rtype = run.get("type", "")
            if rtype == "bench":
                type_badge = "[orange1]bench[/orange1]"
            elif rtype == "mtr":
                type_badge = "[green]mtr[/green]"
            elif rtype == "test":
                type_badge = "[cyan]test[/cyan]"
            else:
                type_badge = f"[dim]{rtype or 'unknown'}[/dim]"

            table.add_row(
                str(run.get("idx", "")),
                run.get("id", ""),
                type_badge,
                run.get("dbms", ""),
                run.get("timestamp", ""),
            )

        console.print(table)
        if total_pages > 1:
            hints = []
            if page < total_pages:
                hints.append(f"-p {page + 1}")
            if page > 1:
                hints.append(f"-p {page - 1}")
            console.print(
                f"[dim]Page {page}/{total_pages}. "
                f"Use {' / '.join(hints)} to navigate.[/dim]"
            )

    def _print_result_show(self, console, data: dict) -> None:
        """Print result show details."""
        import os
        from rich import box
        from rich.table import Table
        from rich.panel import Panel

        run_path = data.get('path', '')
        abs_path = os.path.abspath(run_path) if run_path else ''

        # Header info
        info_lines = []
        info_lines.append(f"[bold]Run ID[/bold]     {data.get('run_id', '')}")
        info_lines.append(f"[bold]Type[/bold]       {data.get('type', '')}")
        info_lines.append(f"[bold]Workload[/bold]   {data.get('workload', '')}")
        info_lines.append(f"[bold]Timestamp[/bold]  {data.get('timestamp', '')}")
        dbms_list = data.get("dbms", [])
        if dbms_list:
            info_lines.append(f"[bold]DBMS[/bold]       {', '.join(dbms_list)}")
        if data.get("mode"):
            info_lines.append(f"[bold]Mode[/bold]       {data.get('mode', '')}")
        info_lines.append(f"[bold]Path[/bold]       {abs_path}")

        console.print(Panel(
            "\n".join(info_lines),
            title="[bold cyan]Run Details[/bold cyan]",
            title_align="left",
            padding=(0, 1),
        ))

        # Bench summary
        bench_summary = data.get("bench_summary", [])
        if bench_summary:
            console.print()
            table = Table(
                title="[bold]Performance Summary[/bold]",
                title_style="",
                show_header=True, header_style="bold cyan",
                pad_edge=True,
                expand=True,
                box=box.ROUNDED,
            )
            table.add_column("DBMS", style="bold", no_wrap=True)
            table.add_column("QPS", justify="right")
            table.add_column("Duration", justify="right")
            table.add_column("Queries", justify="right")
            table.add_column("Errors", justify="right")

            for s in bench_summary:
                errors_str = str(s.get("errors", 0))
                if s.get("errors", 0) > 0:
                    errors_str = f"[red]{errors_str}[/red]"
                table.add_row(
                    s.get("dbms", ""),
                    f"{s.get('qps', 0):.2f}",
                    f"{s.get('duration_s', 0):.2f}s",
                    str(s.get("queries", 0)),
                    errors_str,
                )
            console.print(table)

        # Report files (already absolute paths from data)
        report_files = data.get("report_files", [])
        if report_files:
            console.print()
            console.print("[bold]Reports:[/bold]")
            for f in report_files:
                console.print(f"  [dim]•[/dim] {f}")

    def _print_bench_result(self, console, data: dict) -> None:
        """Print benchmark result summary."""
        from rich import box
        from rich.table import Table
        
        dbms_results = data.get("dbms_results", [])
        if dbms_results:
            table = Table(expand=True, box=box.ROUNDED)
            table.add_column("DBMS", style="cyan", no_wrap=True)
            table.add_column("QPS", justify="right", no_wrap=True)
            table.add_column("Duration", justify="right", no_wrap=True)
            table.add_column("Queries", justify="right")
            table.add_column("Errors", justify="right")
            
            for dr in dbms_results:
                table.add_row(
                    dr.get("dbms_name", ""),
                    f"{dr.get('overall_qps', 0):.2f}",
                    f"{dr.get('total_duration_s', 0):.2f}s",
                    str(dr.get("total_queries", 0)),
                    str(dr.get("total_errors", 0))
                )
            
            console.print(table)
        
        console.print()
        console.print(f"[dim]Report directory:[/dim] {data.get('report_directory', '')}")

    def _print_mtr_result(self, console, data: dict) -> None:
        """Print MTR test result summary — styled like rosetta mtr output."""
        from rich import box
        from rich.table import Table
        from rich.text import Text
        from rich.panel import Panel

        test_file = data.get('test_file', 'unknown')
        dbms_targets = ', '.join(data.get('dbms_targets', []))
        database = data.get('database', '')
        baseline = data.get('baseline', '')

        # --- Configuration panel (like rosetta mtr) ---
        info_lines = []
        info_lines.append(f"[bold]Test File[/bold]  : {test_file}")
        info_lines.append(f"[bold]DBMS[/bold]       : {dbms_targets}")
        if database:
            info_lines.append(f"[bold]Database[/bold]   : {database}")
        if baseline:
            info_lines.append(f"[bold]Baseline[/bold]   : {baseline}")
        report_dir = data.get('report_directory', '')
        if report_dir:
            info_lines.append(f"[bold]Report Dir[/bold] : {report_dir}")
        console.print(Panel(
            "\n".join(info_lines),
            title="[bold cyan]Configuration[/bold cyan]",
            title_align="left",
            padding=(0, 1),
        ))

        # --- Summary table (like rosetta mtr) ---
        comparisons = data.get("comparisons", {})
        if comparisons:
            table = Table(
                show_header=True,
                header_style="bold cyan",
                padding=(0, 1),
                box=box.ROUNDED,
                expand=True,
            )
            table.add_column("Comparison", style="bold", min_width=18)
            table.add_column("Result", min_width=10)
            table.add_column("Total", justify="center")
            table.add_column("Matched", justify="center")
            table.add_column("Mismatch", justify="center")
            table.add_column("Pass Rate", justify="center")

            any_error = False
            for key, cmp in comparisons.items():
                mismatched = cmp.get("mismatched", 0)
                pass_rate = cmp.get('pass_rate', 0)
                total = cmp.get("total_statements", 0)
                matched = cmp.get("matched", 0)
                if mismatched > 0:
                    any_error = True

                # Result column
                if pass_rate >= 100:
                    result_text = "[green bold]PASSED[/green bold]"
                elif mismatched > 0:
                    result_text = "[red bold]FAILED[/red bold]"
                else:
                    result_text = "[green bold]PASSED[/green bold]"

                # Matched column
                matched_text = f"[green]{matched}[/green]"

                # Mismatch
                if mismatched > 0:
                    mismatch_text = f"[red bold]{mismatched}[/red bold]"
                else:
                    mismatch_text = "[green]0[/green]"
                    err_text = "[green]0[/green]"

                # Pass Rate
                if pass_rate >= 100:
                    rate_text = f"[green bold]{pass_rate:.1f}%[/green bold]"
                elif pass_rate >= 90:
                    rate_text = f"[yellow bold]{pass_rate:.1f}%[/yellow bold]"
                else:
                    rate_text = f"[red bold]{pass_rate:.1f}%[/red bold]"

                table.add_row(
                    key,
                    result_text,
                    str(total),
                    matched_text,
                    mismatch_text,
                    rate_text,
                )

            console.print(table)

            # Show details for failed/diff comparisons (like rosetta mtr failed cases panel)
            for key, cmp in comparisons.items():
                mismatched = cmp.get("mismatched", 0)
                if mismatched > 0:
                    detail_lines = []
                    if mismatched > 0:
                        detail_lines.append(
                            f"  [red]•[/red] [bold]Mismatches[/bold]: {mismatched} "
                            f"(behavioral difference, needs investigation)"
                        )
                    # Add hint for report files
                    report_files = data.get('report_files', [])
                    if report_files:
                        txt_reports = [f for f in report_files if f.endswith('.report.txt')]
                        html_reports = [f for f in report_files if f.endswith('.html')]
                        hint_parts = []
                        if txt_reports:
                            hint_parts.append(
                                f"  [dim]📄 Text:[/dim] {txt_reports[0]}"
                            )
                        if html_reports:
                            hint_parts.append(
                                f"  [dim]🌐 HTML:[/dim] {html_reports[0]}"
                            )
                        if hint_parts:
                            detail_lines.append("")
                            detail_lines.append(
                                f"  [cyan]View diff details:[/cyan]"
                            )
                            detail_lines.extend(hint_parts)
                    console.print(Panel(
                        "\n".join(detail_lines),
                        title=f"[bold red]{key} — Issues ({effective + dbms_diff})[/bold red]",
                        title_align="left",
                        border_style="red" if effective > 0 else "yellow",
                        padding=(0, 1),
                    ))

        if data.get("failed_connections"):
            console.print()
            console.print(f"[red]Failed Connections:[/red] {', '.join(data['failed_connections'])}")

    def _print_exec_result(self, console, data: dict) -> None:
        """Print SQL execution result — one column per DBMS."""
        from rich import box
        from rich.table import Table

        results = data.get("results", {})
        dbms_names = list(results.keys())

        # Print connection-level errors first
        has_conn_err = False
        for name in dbms_names:
            r = results[name]
            if r.get("error"):
                console.print(f"[red]✗ {name}:[/red] {r['error']}")
                has_conn_err = True
        if has_conn_err:
            console.print()

        ok_dbms = [n for n in dbms_names if not results[n].get("error")]
        if not ok_dbms:
            return

        n_stmts = max(len(results[n].get("statements", [])) for n in ok_dbms)
        if n_stmts == 0:
            return

        # Build table: # | SQL | dbms1 (time) | dbms2 (time) | ...
        table = Table(
            show_header=True,
            header_style="bold cyan",
            pad_edge=True,
            expand=True,
            box=box.ROUNDED,
        )
        table.add_column("#", style="bold cyan", no_wrap=True, justify="right", width=3)
        table.add_column("SQL", style="dim", no_wrap=True, max_width=40)
        for name in ok_dbms:
            table.add_column(name, no_wrap=False)

        for si in range(n_stmts):
            # Get SQL text
            sql_text = ""
            for n in ok_dbms:
                stmts = results[n].get("statements", [])
                if si < len(stmts):
                    sql_text = stmts[si].get("sql", "")
                    break
            sql_display = sql_text if len(sql_text) <= 40 else sql_text[:37] + "..."

            # Collect result string for each DBMS
            cells = []
            for name in ok_dbms:
                stmts = results[name].get("statements", [])
                sd = stmts[si] if si < len(stmts) else {}
                elapsed = f"{sd.get('elapsed_ms', 0):.2f}ms"

                if sd.get("error"):
                    cells.append(f"[red]ERROR: {sd['error']}[/red]\n[dim]{elapsed}[/dim]")
                elif sd.get("columns"):
                    rows = sd.get("rows", [])
                    cols = sd["columns"]
                    if rows:
                        lines = []
                        for row in rows[:5]:
                            if len(cols) == 1:
                                lines.append(str(row[0]))
                            else:
                                lines.append(", ".join(
                                    f"{cols[ci]}={row[ci]}" for ci in range(len(cols))
                                ))
                        if len(rows) > 5:
                            lines.append(f"[dim]... +{len(rows) - 5} rows[/dim]")
                        lines.append(f"[dim]{elapsed}[/dim]")
                        cells.append("\n".join(lines))
                    else:
                        cells.append(f"[dim]Empty  {elapsed}[/dim]")
                else:
                    affected = sd.get("affected_rows", 0)
                    cells.append(f"[dim]OK, {affected} rows  {elapsed}[/dim]")

            if si > 0:
                table.add_section()
            table.add_row(str(si + 1), sql_display, *cells)

        console.print(table)
