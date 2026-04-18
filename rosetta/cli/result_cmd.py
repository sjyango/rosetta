"""
Handler for the 'result' subcommand — list / show / export historical runs.
"""

import json
import os
import re
from typing import TYPE_CHECKING, List, Dict, Any, Optional

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter

# ---------------------------------------------------------------------------
# Pattern for run directory names produced by rosetta
#   bench_json_mv_ddl_20260326_141650
#   array_index_20260313_144633
# ---------------------------------------------------------------------------
_TIMESTAMP_RE = re.compile(r"(\d{8}_\d{6})$")


def handle_result(args, output: "OutputFormatter") -> CommandResult:
    """Dispatch result sub-actions."""
    action = getattr(args, "result_action", None)

    # ``rosetta result`` with no sub-action → default to list
    if not action:
        action = "list"

    if action == "list":
        return _handle_list(args, output)
    elif action == "show":
        return _handle_show(args, output)
    else:
        return CommandResult.failure(f"Unknown result action: {action}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scan_runs(output_dir: str) -> List[Dict[str, Any]]:
    """Scan the output directory and return a list of run metadata dicts,
    sorted newest-first."""
    runs: List[Dict[str, Any]] = []
    if not os.path.isdir(output_dir):
        return runs

    for name in os.listdir(output_dir):
        full = os.path.join(output_dir, name)
        if not os.path.isdir(full) or name in ("latest", "__pycache__"):
            continue

        run: Dict[str, Any] = {
            "id": name,
            "path": full,
            "type": "unknown",
            "timestamp": "",
            "workload": "",
        }

        # Extract timestamp from directory name
        m = _TIMESTAMP_RE.search(name)
        if m:
            raw = m.group(1)  # e.g. 20260326_141650
            run["timestamp"] = (
                f"{raw[:4]}-{raw[4:6]}-{raw[6:8]} "
                f"{raw[9:11]}:{raw[11:13]}:{raw[13:15]}"
            )
            # Workload = everything before the timestamp part
            prefix = name[: m.start()].rstrip("_")
            run["workload"] = prefix

        # Detect run type
        if os.path.isfile(os.path.join(full, "bench_result.json")):
            run["type"] = "bench"
        else:
            result_files = [f for f in os.listdir(full) if f.endswith(".result")]
            if result_files:
                run["type"] = "mtr"

        # Extra metadata for bench
        if run["type"] == "bench":
            try:
                with open(os.path.join(full, "bench_result.json"), "r") as f:
                    bdata = json.load(f)
                run["mode"] = bdata.get("mode", "")
                run["dbms_targets"] = [
                    d.get("dbms_name", "") for d in bdata.get("dbms_results", [])
                ]
            except Exception:
                pass

        # For MTR, list result files
        if run["type"] == "mtr":
            run["result_files"] = sorted(
                f for f in os.listdir(full) if f.endswith(".result")
            )
            # Infer dbms targets from .result filenames  (e.g. test.mysql.result)
            targets = []
            for rf in run.get("result_files", []):
                parts = rf.rsplit(".", 2)
                if len(parts) == 3:
                    targets.append(parts[1])
            run["dbms_targets"] = targets

        # Count report files
        report_files = [
            f for f in os.listdir(full)
            if f.endswith((".html", ".report.txt", ".json", ".diff"))
        ]
        run["report_files"] = sorted(report_files)

        runs.append(run)

    # Sort newest first
    runs.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
    return runs


def _resolve_run(run_id: Optional[str], output_dir: str) -> Optional[Dict[str, Any]]:
    """Resolve a run_id (exact, prefix, or 'latest') to a run metadata dict."""
    runs = _scan_runs(output_dir)
    if not runs:
        return None

    if not run_id:
        # Default: latest
        return runs[0] if runs else None

    # Exact match
    for r in runs:
        if r["id"] == run_id:
            return r

    # Direct path
    if os.path.isdir(run_id):
        return {"id": os.path.basename(run_id), "path": run_id, "type": "unknown"}

    # Prefix match
    candidates = [r for r in runs if r["id"].startswith(run_id)]
    if len(candidates) == 1:
        return candidates[0]

    return None


# ---------------------------------------------------------------------------
# Sub-actions
# ---------------------------------------------------------------------------

def _handle_list(args, output: "OutputFormatter") -> CommandResult:
    """List historical runs with pagination."""
    from ..paths import RESULTS_DIR as _DEFAULT_RESULTS
    output_dir = getattr(args, "output_dir", _DEFAULT_RESULTS)
    limit = getattr(args, "limit", 20)
    page = max(1, getattr(args, "page", 1))
    type_filter = getattr(args, "type", "all")

    runs = _scan_runs(output_dir)

    if type_filter != "all":
        runs = [r for r in runs if r["type"] == type_filter]

    total = len(runs)
    total_pages = max(1, (total + limit - 1) // limit)
    page = min(page, total_pages)

    start = (page - 1) * limit
    display_runs = runs[start:start + limit]

    # Slim down for output
    rows = []
    for i, r in enumerate(display_runs, start + 1):
        rows.append({
            "idx": i,
            "id": r["id"],
            "type": r["type"],
            "workload": r.get("workload", ""),
            "timestamp": r.get("timestamp", ""),
            "dbms": ", ".join(r.get("dbms_targets", [])),
        })

    return CommandResult.success(
        "result list",
        {
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "per_page": limit,
            "showing": len(rows),
            "output_dir": output_dir,
            "runs": rows,
        },
    )


def _handle_show(args, output: "OutputFormatter") -> CommandResult:
    """Show details of a specific run."""
    from ..paths import RESULTS_DIR as _DEFAULT_RESULTS
    output_dir = getattr(args, "output_dir", _DEFAULT_RESULTS)
    run_id = getattr(args, "run_id", None)

    run = _resolve_run(run_id, output_dir)
    if not run:
        if run_id:
            return CommandResult.failure(f"Run not found: {run_id}")
        return CommandResult.failure("No runs found in results directory")

    abs_path = os.path.abspath(run.get("path", ""))

    data: Dict[str, Any] = {
        "run_id": run["id"],
        "type": run.get("type", "unknown"),
        "timestamp": run.get("timestamp", ""),
        "workload": run.get("workload", ""),
        "path": abs_path,
        "dbms": run.get("dbms_targets", []),
        "report_files": [
            os.path.join(abs_path, f) for f in run.get("report_files", [])
        ],
    }

    # Bench: include summary stats
    if run.get("type") == "bench":
        bench_json = os.path.join(run["path"], "bench_result.json")
        if os.path.isfile(bench_json):
            try:
                with open(bench_json, "r", encoding="utf-8") as f:
                    bdata = json.load(f)
                data["mode"] = bdata.get("mode", "")
                data["bench_summary"] = []
                for dr in bdata.get("dbms_results", []):
                    data["bench_summary"].append({
                        "dbms": dr.get("dbms_name", ""),
                        "qps": round(dr.get("overall_qps", 0), 2),
                        "duration_s": round(dr.get("total_duration_s", 0), 2),
                        "queries": dr.get("total_queries", 0),
                        "errors": dr.get("total_errors", 0),
                    })
            except Exception:
                pass

    # MTR: list result files
    if run.get("type") == "mtr":
        data["result_files"] = run.get("result_files", [])

    return CommandResult.success("result show", data)
