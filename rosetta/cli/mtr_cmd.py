"""
Handler for the 'mtr' command — run native MySQL MTR test suites.

This wraps the ./mtr binary in the MySQL test directory, supporting
common options like suite selection, record mode, optimistic transactions,
vector engine, parallel query, etc.

Supports running multiple modes (row/column/pq) in parallel via --mode.

Configuration is read from the same rosetta_config.json file under the
"mtr" top-level key.  CLI flags override config values.
"""

import concurrent.futures
import glob as _glob
import json
import os
import re
import subprocess
import sys
import threading
import time as _time
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from .result import CommandResult

if TYPE_CHECKING:
    from .output import OutputFormatter


# -----------------------------------------------------------------------
# Mode definitions
# -----------------------------------------------------------------------

# Canonical mode names and their display labels
MTR_MODES = {
    "row":    {"label": "行存 (Row)",    "vector": False, "parallel_query": False},
    "col":    {"label": "列存 (Column)", "vector": True,  "parallel_query": False},
    "pq":     {"label": "PQ (Parallel)", "vector": False, "parallel_query": True},
}

# Aliases for convenience (column -> col)
_MODE_ALIASES = {"column": "col"}

# Port offset per mode (to avoid port conflicts when running in parallel)
# Each MTR worker uses ~30 ports, with --parallel=8 that's ~240 ports.
# Use 1000 offset per mode to be safe.
_MODE_PORT_OFFSETS = {"row": 0, "col": 1000, "pq": 2000}


# -----------------------------------------------------------------------
# Config loading
# -----------------------------------------------------------------------

def _load_mtr_config(config_path: str) -> dict:
    """
    Load the ``mtr`` section from the shared rosetta_config.json.

    Returns a dict (possibly empty) with whatever keys the user has set.
    Required keys must all be present or the handler will report an error.
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("mtr", {})
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


# -----------------------------------------------------------------------
# Command builder
# -----------------------------------------------------------------------

def _build_mysqld_opts(mysqld_opts_list: List[str]) -> str:
    """Convert a list of mysqld options to CLI flags.

    Each item can be either ``key=value`` (auto-prefixed with ``--``)
    or ``--key=value`` (used as-is).
    """
    parts = []
    for opt in mysqld_opts_list:
        if opt.startswith("--"):
            parts.append(f"--mysqld={opt}")
        else:
            parts.append(f"--mysqld=--{opt}")
    return " ".join(parts)


def _build_command(cfg: dict) -> str:
    """Build the full ./mtr command string from resolved config dict."""
    parts = ["./mtr"]
    parts.append(f"--port-base={cfg['port_base']}")
    parts.append(f"--skip-test-list={cfg['skip_list']}")
    parts.append(f"--parallel={cfg['parallel']}")
    parts.append(f"--retry={cfg['retry']}")
    parts.append(f"--retry-failure={cfg['retry_failure']}")
    parts.append(f"--max-test-fail={cfg['max_test_fail']}")
    parts.append("--force")
    parts.append("--big-test")
    parts.append("--nounit-tests")
    parts.append("--nowarnings")
    parts.append(f"--testcase-timeout={cfg['testcase_timeout']}")
    parts.append(f"--suite-timeout={cfg['suite_timeout']}")
    parts.append("--report-unstable-tests")

    # Isolated var/tmp directories for parallel mode execution
    if cfg.get("vardir"):
        parts.append(f"--vardir={cfg['vardir']}")
    if cfg.get("tmpdir"):
        parts.append(f"--tmpdir={cfg['tmpdir']}")

    if cfg.get("mysqld_opts"):
        parts.append(cfg["mysqld_opts"])

    # Feature flags
    if cfg.get("optimistic"):
        parts.append("--mysqld=--tdsql_trans_type=1")
    if cfg.get("record"):
        parts.append("--record")
    if cfg.get("vector"):
        parts.append("--ve-protocol")
    if cfg.get("parallel_query"):
        parts.append("--parallel-query")
    if cfg.get("suite"):
        parts.append(f"--suite={cfg['suite']}")
    if cfg.get("cases"):
        parts.append(" ".join(cfg["cases"]))

    return " ".join(parts)


# -----------------------------------------------------------------------
# Output filtering
# -----------------------------------------------------------------------

# Patterns for noisy lines that should be suppressed from mtr output.
_SUPPRESSED_PATTERNS = [
    # mysqld daemon internal logs  e.g. [2026-04-16 23:38:28 ...] [WARN/INFO/ERROR] ...
    re.compile(r"^\[\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"),
    # AsyncFileWriteLogger rotate messages
    re.compile(r"^AsyncFileWriteLogger"),
    # MySQL server thread exit error
    re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s+\d+\s+\[ERROR\].*my_thread_global_end"),
    # mysql-test-run "Could not parse variable list line" warnings (JSON config noise)
    re.compile(r"^mysql-test-run:\s+WARNING:\s+Could not parse variable list line"),
    # TDStoreServiceImpl noise
    re.compile(r"TDStoreServiceImpl"),
    # brpc init noise
    re.compile(r"bthread/task_control\.cpp"),
    # var directory cleanup noise (chmod/delete failures on stale files)
    re.compile(r"^couldn't chmod\("),
    re.compile(r"^Couldn't delete file "),
    # SSL library warning (harmless)
    re.compile(r"\[Warning\].*CRYPTO_set_mem_functions failed"),
    # mysqld timestamp logs (e.g. 2026-04-18T15:04:09.835941+08:00 ...)
    re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[Z+-]"),
]


def _should_suppress(line: str) -> bool:
    """Return True if the line matches any suppressed pattern."""
    for pat in _SUPPRESSED_PATTERNS:
        if pat.search(line):
            return True
    return False


def _filter_output(proc, verbose: bool = False) -> int:
    """Read proc stdout line by line, printing only non-suppressed lines."""
    interrupted = False
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n")
            if verbose or not _should_suppress(line):
                print(line)
                sys.stdout.flush()
    except KeyboardInterrupt:
        interrupted = True
        proc.terminate()
    proc.wait()
    if interrupted:
        return -1
    return proc.returncode


def _parse_mtr_log_stats(log_path: str) -> dict:
    """Parse MTR log tail to extract test statistics.

    Looks for lines like:
      Total cases: 88
      Pass cases: 82
      Fail cases: 6
      Pass ratio: 93.18%
      Failing test(s): case1 case2 ...
    """
    stats: dict = {}
    if not log_path or not os.path.isfile(log_path):
        return stats

    try:
        # Read only last 2KB for efficiency
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - 4096))
            tail = f.read()
    except Exception:
        return stats

    for line in tail.splitlines():
        line = line.strip()
        if line.startswith("Total cases:"):
            try:
                stats["total"] = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("Pass cases:"):
            try:
                stats["pass"] = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("Fail cases:"):
            try:
                stats["fail"] = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.startswith("Pass ratio:"):
            stats["pass_ratio"] = line.split(":", 1)[1].strip()
        elif line.startswith("Failing test(s):"):
            cases_str = line.split(":", 1)[1].strip()
            if cases_str:
                stats["failing_tests"] = cases_str.split()

    return stats


# -----------------------------------------------------------------------
# gcov coverage support
# -----------------------------------------------------------------------

def _infer_build_dir(test_dir: str) -> str:
    """Infer the build directory from the MTR test directory.

    test_dir is typically <build_dir>/mysql-test, so build_dir is its parent.
    """
    return os.path.dirname(os.path.abspath(test_dir))


def _check_gcov_build(build_dir: str) -> Tuple[bool, str, bool]:
    """Check if the build has gcov instrumentation enabled.

    Detection strategy (ordered by reliability):
      1. Check CMakeCache.txt for ENABLE_GCOV=ON
      2. Check for .gcno files in the build directory
      3. Check Makefile for fastcov targets

    Returns:
        (is_gcov_build, detail_message, is_clang_build)
    """
    is_clang = False

    # Strategy 1: CMakeCache.txt
    cmake_cache = os.path.join(build_dir, "CMakeCache.txt")
    if os.path.isfile(cmake_cache):
        try:
            gcov_found = False
            with open(cmake_cache, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    # Detect compiler
                    if "CMAKE_C_COMPILER:" in line and "clang" in line.lower():
                        is_clang = True
                    if "CMAKE_CXX_COMPILER:" in line and "clang" in line.lower():
                        is_clang = True
                    if "ENABLE_GCOV" in line and "ON" in line.upper():
                        gcov_found = True
                    if "fprofile-arcs" in line:
                        gcov_found = True
                    if "fcoverage-mapping" in line:
                        gcov_found = True
                        is_clang = True
            if gcov_found:
                compiler = "Clang" if is_clang else "GCC"
                return True, f"CMakeCache.txt: gcov ON ({compiler})", is_clang
        except Exception:
            pass

    # Strategy 2: .gcno files exist
    for sub in ["sql", "storage", "plugin", "."]:
        search_dir = os.path.join(build_dir, sub)
        if os.path.isdir(search_dir):
            gcno_files = _glob.glob(os.path.join(search_dir, "**", "*.gcno"),
                                    recursive=True)
            if gcno_files:
                return True, f"Found {len(gcno_files)} .gcno files in {sub}/", is_clang

    # Strategy 3: Makefile has fastcov targets
    makefile = os.path.join(build_dir, "Makefile")
    if os.path.isfile(makefile):
        try:
            with open(makefile, "r", encoding="utf-8", errors="replace") as f:
                content = f.read(100_000)
                if "fastcov" in content:
                    return True, "Makefile: fastcov targets found", is_clang
        except Exception:
            pass

    return False, "", is_clang


def _check_gcov_tools(is_clang: bool = False) -> Tuple[List[str], str, bool]:
    """Check for required gcov tools, auto-selecting based on compiler.

    Args:
        is_clang: Whether the build was compiled with Clang.

    Returns:
        (missing_required_tools, gcov_tool_name, has_fastcov)
    """
    missing = []
    gcov_tool = ""
    has_fastcov = False

    if is_clang:
        # Clang build: must use llvm-cov gcov (GCC gcov is incompatible)
        try:
            subprocess.run(["llvm-cov", "gcov", "--version"],
                           capture_output=True, timeout=5)
            gcov_tool = "llvm-cov gcov"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            missing.append("llvm-cov (Clang build requires llvm-cov, "
                           "yum install llvm / apt install llvm)")
    else:
        # GCC build: prefer gcov, fallback to llvm-cov gcov
        try:
            subprocess.run(["gcov", "--version"], capture_output=True, timeout=5)
            gcov_tool = "gcov"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            try:
                subprocess.run(["llvm-cov", "gcov", "--version"],
                               capture_output=True, timeout=5)
                gcov_tool = "llvm-cov gcov"
            except (FileNotFoundError, subprocess.TimeoutExpired):
                missing.append("gcov (yum install gcc / apt install gcc)")

    # lcov is required for report generation
    try:
        subprocess.run(["lcov", "--version"], capture_output=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        missing.append("lcov (yum install lcov / apt install lcov)")

    # fastcov is optional (speeds up report generation)
    try:
        subprocess.run(["fastcov", "--version"], capture_output=True, timeout=5)
        has_fastcov = True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    return missing, gcov_tool, has_fastcov


def _find_changed_sources(build_dir: str) -> List[str]:
    """Find C/C++ source files changed on current branch vs origin/master.

    Uses `git diff --name-only origin/master...HEAD` to find files that
    the user has modified. Only returns .cc/.cpp/.c/.h files.

    Returns a list of relative paths like
    ['storage/rocksdb/ha_rocksdb.cc', 'sql/load_data.cc'].
    """
    source_root = os.path.dirname(os.path.abspath(build_dir))

    for remote_branch in ["origin/master", "origin/main"]:
        try:
            result = subprocess.run(
                ["git", "diff", "--name-only", f"{remote_branch}...HEAD"],
                cwd=source_root,
                capture_output=True, text=True, timeout=30)
            if result.returncode == 0 and result.stdout.strip():
                changed = []
                for line in result.stdout.strip().splitlines():
                    name = line.strip()
                    if name.endswith((".cc", ".cpp", ".c", ".h", ".hpp")):
                        changed.append(name)
                return sorted(set(changed))
        except Exception:
            pass

    return []


def _snapshot_gcda(build_dir: str) -> Dict[str, float]:
    """Record mtime of all .gcda files before MTR run."""
    snapshot = {}
    for root, _dirs, files in os.walk(build_dir):
        for f in files:
            if f.endswith(".gcda"):
                path = os.path.join(root, f)
                try:
                    snapshot[path] = os.path.getmtime(path)
                except Exception:
                    pass
    return snapshot


def _find_touched_sources(build_dir: str,
                          before: Dict[str, float]) -> List[str]:
    """Find source files whose .gcda was created/updated since snapshot.

    Maps .gcda paths back to source file paths by replacing the build
    dir structure markers (CMakeFiles/xxx.dir/) and .gcda -> .cc/.cpp.
    Returns a sorted list of unique source file glob patterns.
    """
    import re as _re

    touched = set()
    for root, _dirs, files in os.walk(build_dir):
        for f in files:
            if not f.endswith(".gcda"):
                continue
            path = os.path.join(root, f)
            try:
                mtime = os.path.getmtime(path)
            except Exception:
                continue
            old_mtime = before.get(path, 0)
            if mtime > old_mtime:
                # Convert .gcda path to source file name
                # e.g. storage/rocksdb/CMakeFiles/rocksdb.dir/ha_rocksdb.cc.gcda
                #   -> ha_rocksdb.cc
                source_name = f.replace(".gcda", "").replace(".gcno", "")
                # Only include .cc/.cpp/.c files
                if source_name.endswith((".cc", ".cpp", ".c", ".h")):
                    touched.add(source_name)
    # Convert to glob patterns: */filename
    patterns = sorted(f"*/{name}" for name in touched)
    return patterns


def _gcov_clean(build_dir: str) -> Tuple[bool, str]:
    """Clean gcov counters before running tests.

    Tries `make fastcov-clean` first, falls back to deleting .gcda files.
    """
    # Try make fastcov-clean
    try:
        result = subprocess.run(
            ["make", "fastcov-clean"],
            cwd=build_dir,
            capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            return True, "make fastcov-clean succeeded"
    except Exception:
        pass

    # Fallback: delete .gcda files manually
    count = 0
    for root, dirs, files in os.walk(build_dir):
        for f in files:
            if f.endswith(".gcda"):
                try:
                    os.remove(os.path.join(root, f))
                    count += 1
                except Exception:
                    pass
    return True, f"Deleted {count} .gcda files"


def _gcov_report(build_dir: str, gcov_tool: str = "",
                 source_filter: str = "",
                 has_fastcov: bool = False) -> Tuple[bool, str]:
    """Generate gcov coverage report after tests.

    Priority: fastcov (fast, parallel) > lcov (slower, sequential).
    If source_filter is provided, extracts only matching files.

    Returns (success, report_info_path_or_error_message).
    """
    report_path = os.path.join(build_dir, "report.info")

    # Prepare llvm-cov gcov wrapper (needed by both fastcov and lcov for Clang)
    wrapper = ""
    if gcov_tool == "llvm-cov gcov":
        wrapper = os.path.join(os.path.abspath(build_dir), ".gcov-wrapper.sh")
        try:
            with open(wrapper, "w") as f:
                f.write("#!/bin/sh\nexec llvm-cov gcov \"$@\"\n")
            os.chmod(wrapper, 0o755)
        except Exception:
            wrapper = ""

    # --- Strategy 1: fastcov (10-50x faster than lcov) ---
    if has_fastcov:
        fastcov_cmd = ["fastcov", "--directory", build_dir,
                       "--output", report_path, "--lcov"]
        if wrapper:
            fastcov_cmd.extend(["--gcov", wrapper])
        elif gcov_tool == "gcov":
            fastcov_cmd.extend(["--gcov", "gcov"])
        # fastcov supports --source-files filter for targeted capture
        if source_filter:
            # Convert glob pattern to regex-ish for fastcov --include
            # fastcov uses fnmatch-style include, similar to lcov patterns
            fastcov_cmd.extend(["--include", source_filter])
        try:
            result = subprocess.run(
                fastcov_cmd, cwd=build_dir,
                capture_output=True, text=True, timeout=600)
            if result.returncode == 0 and os.path.isfile(report_path):
                return True, report_path
        except Exception:
            pass  # Fall through to lcov

    # --- Strategy 2: lcov (slower but more reliable) ---
    lcov_cmd = ["lcov", "--capture", "--directory", build_dir,
                "--output-file", report_path, "--quiet",
                "--no-external"]
    if wrapper:
        lcov_cmd.extend(["--gcov-tool", wrapper])

    try:
        result = subprocess.run(
            lcov_cmd, cwd=build_dir,
            capture_output=True, text=True, timeout=1200)
        if result.returncode == 0 and os.path.isfile(report_path):
            if source_filter:
                filtered_path = os.path.join(build_dir, "coverage_filtered.info")
                ext_result = subprocess.run(
                    ["lcov", "--extract", report_path, source_filter,
                     "--output-file", filtered_path, "--quiet"],
                    capture_output=True, text=True, timeout=120)
                if ext_result.returncode == 0 and os.path.isfile(filtered_path):
                    return True, filtered_path
            return True, report_path
        err = (result.stderr or result.stdout or "").strip()
        return False, f"lcov exit code {result.returncode}: {err[-500:]}"
    except subprocess.TimeoutExpired:
        return False, "lcov timed out (>1200s)"
    except Exception as e:
        return False, str(e)


def _gcov_extract(report_path: str, filter_pattern: str,
                  build_dir: str) -> Tuple[bool, str]:
    """Extract coverage for specific source files from report.info.

    Returns (success, extracted_info_path).
    """
    extracted = os.path.join(build_dir, "coverage_filtered.info")
    try:
        result = subprocess.run(
            ["lcov", "--extract", report_path, filter_pattern,
             "--output-file", extracted, "--quiet"],
            capture_output=True, text=True, timeout=120)
        if result.returncode == 0 and os.path.isfile(extracted):
            return True, extracted
    except Exception:
        pass
    return False, ""


def _parse_lcov_info(info_path: str) -> Dict:
    """Parse an lcov .info file into a structured coverage summary.

    Returns a dict with per-file coverage data suitable for AI consumption.
    """
    files = {}
    current_file = None
    current_data = None

    try:
        with open(info_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line.startswith("SF:"):
                    current_file = line[3:]
                    current_data = {
                        "file": current_file,
                        "lines_total": 0,
                        "lines_hit": 0,
                        "branches_total": 0,
                        "branches_hit": 0,
                        "functions_total": 0,
                        "functions_hit": 0,
                        "uncovered_lines": [],
                        "uncovered_functions": [],
                    }
                elif line.startswith("DA:"):
                    # DA:line_number,execution_count
                    parts = line[3:].split(",")
                    if len(parts) >= 2 and current_data:
                        current_data["lines_total"] += 1
                        count = int(parts[1])
                        if count > 0:
                            current_data["lines_hit"] += 1
                        else:
                            current_data["uncovered_lines"].append(
                                int(parts[0]))
                elif line.startswith("BRDA:"):
                    # BRDA:line_number,block_number,branch_number,taken
                    parts = line[5:].split(",")
                    if len(parts) >= 4 and current_data:
                        current_data["branches_total"] += 1
                        if parts[3] != "-" and int(parts[3]) > 0:
                            current_data["branches_hit"] += 1
                elif line.startswith("FN:"):
                    if current_data:
                        current_data["functions_total"] += 1
                elif line.startswith("FNDA:"):
                    # FNDA:execution_count,function_name
                    parts = line[5:].split(",", 1)
                    if len(parts) >= 2 and current_data:
                        count = int(parts[0])
                        if count > 0:
                            current_data["functions_hit"] += 1
                        else:
                            current_data["uncovered_functions"].append(
                                parts[1])
                elif line == "end_of_record":
                    if current_file and current_data:
                        # Compute percentages
                        lt = current_data["lines_total"]
                        lh = current_data["lines_hit"]
                        current_data["line_coverage_pct"] = (
                            round(lh / lt * 100, 1) if lt > 0 else 0.0)
                        bt = current_data["branches_total"]
                        bh = current_data["branches_hit"]
                        current_data["branch_coverage_pct"] = (
                            round(bh / bt * 100, 1) if bt > 0 else 0.0)
                        # Compact uncovered lines into ranges
                        current_data["uncovered_line_ranges"] = (
                            _compact_ranges(current_data["uncovered_lines"]))
                        files[current_file] = current_data
                    current_file = None
                    current_data = None
    except Exception:
        pass

    # Build summary (exclude system headers)
    _SYSTEM_PREFIXES = ("/usr/include/", "/usr/lib/", "/usr/local/include/",
                        "/usr/local/lib/", "/opt/rh/")

    filtered_files = {
        path: data for path, data in files.items()
        if not any(path.startswith(p) for p in _SYSTEM_PREFIXES)
    }

    total_lines = sum(f["lines_total"] for f in filtered_files.values())
    hit_lines = sum(f["lines_hit"] for f in filtered_files.values())
    total_branches = sum(f["branches_total"] for f in filtered_files.values())
    hit_branches = sum(f["branches_hit"] for f in filtered_files.values())

    return {
        "summary": {
            "files": len(filtered_files),
            "lines_total": total_lines,
            "lines_hit": hit_lines,
            "line_coverage_pct": (
                round(hit_lines / total_lines * 100, 1)
                if total_lines > 0 else 0.0),
            "branches_total": total_branches,
            "branches_hit": hit_branches,
            "branch_coverage_pct": (
                round(hit_branches / total_branches * 100, 1)
                if total_branches > 0 else 0.0),
        },
        "files": {
            path: {
                "line_coverage": f"{d['lines_hit']}/{d['lines_total']}"
                                 f" ({d['line_coverage_pct']}%)",
                "branch_coverage": f"{d['branches_hit']}/{d['branches_total']}"
                                   f" ({d['branch_coverage_pct']}%)",
                "uncovered_line_ranges": d["uncovered_line_ranges"],
                "uncovered_functions": d["uncovered_functions"],
            }
            for path, d in sorted(filtered_files.items())
        },
    }


def _compact_ranges(numbers: List[int]) -> List[str]:
    """Compact a sorted list of integers into range strings.

    e.g. [1,2,3,5,7,8,9] -> ["1-3", "5", "7-9"]
    """
    if not numbers:
        return []
    numbers = sorted(set(numbers))
    ranges = []
    start = prev = numbers[0]
    for n in numbers[1:]:
        if n == prev + 1:
            prev = n
        else:
            ranges.append(f"{start}-{prev}" if start != prev else str(start))
            start = prev = n
    ranges.append(f"{start}-{prev}" if start != prev else str(start))
    return ranges


def _recalc_summary(files_dict: Dict) -> Dict:
    """Recalculate summary stats from a filtered files dict."""
    total_lines = 0
    hit_lines = 0
    total_branches = 0
    hit_branches = 0
    for data in files_dict.values():
        # Parse "123/456 (78.9%)" format back to numbers
        lc = data.get("line_coverage", "0/0 (0.0%)")
        bc = data.get("branch_coverage", "0/0 (0.0%)")
        try:
            lparts = lc.split(" ")[0].split("/")
            hit_lines += int(lparts[0])
            total_lines += int(lparts[1])
        except (IndexError, ValueError):
            pass
        try:
            bparts = bc.split(" ")[0].split("/")
            hit_branches += int(bparts[0])
            total_branches += int(bparts[1])
        except (IndexError, ValueError):
            pass
    return {
        "files": len(files_dict),
        "lines_total": total_lines,
        "lines_hit": hit_lines,
        "line_coverage_pct": round(hit_lines / total_lines * 100, 1) if total_lines > 0 else 0.0,
        "branches_total": total_branches,
        "branches_hit": hit_branches,
        "branch_coverage_pct": round(hit_branches / total_branches * 100, 1) if total_branches > 0 else 0.0,
    }


def _print_coverage_table(coverage: Dict):
    """Print a formatted coverage table to terminal."""
    s = coverage["summary"]
    files = coverage.get("files", {})

    print()
    print("  " + "=" * 72)
    print("  COVERAGE REPORT")
    print("  " + "=" * 72)
    print(f"  Files: {s['files']}  |  "
          f"Lines: {s['lines_hit']}/{s['lines_total']} "
          f"({s['line_coverage_pct']}%)  |  "
          f"Branches: {s['branches_hit']}/{s['branches_total']} "
          f"({s['branch_coverage_pct']}%)")
    print("  " + "-" * 72)

    if not files:
        print("  (no files)")
        print("  " + "=" * 72)
        return

    # Column widths
    name_w = 40
    line_w = 18
    branch_w = 18

    # Header
    print(f"  {'File':<{name_w}} {'Lines':<{line_w}} {'Branches':<{branch_w}}")
    print("  " + "-" * 72)

    for path, data in sorted(files.items()):
        # Short name: last 2 path components
        parts = path.split("/")
        short = "/".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        if len(short) > name_w:
            short = "..." + short[-(name_w - 3):]

        lc = data.get("line_coverage", "0/0")
        bc = data.get("branch_coverage", "0/0")
        print(f"  {short:<{name_w}} {lc:<{line_w}} {bc:<{branch_w}}")

        # Show uncovered details compactly
        uf = data.get("uncovered_functions", [])
        ur = data.get("uncovered_line_ranges", [])
        if uf:
            funcs_str = ", ".join(uf[:5])
            if len(uf) > 5:
                funcs_str += f" (+{len(uf)-5} more)"
            print(f"  {'':>{name_w}}   uncov funcs: {funcs_str}")
        if ur:
            lines_str = ", ".join(ur[:15])
            if len(ur) > 15:
                lines_str += f" (+{len(ur)-15} more)"
            print(f"  {'':>{name_w}}   uncov lines: {lines_str}")

    print("  " + "=" * 72)
    print()


def _write_coverage_report(coverage: Dict, build_dir: str) -> str:
    """Write coverage summary to a JSON file in the build directory.

    Returns the absolute path to the written file.
    """
    import time as _time

    report_dir = os.path.join(build_dir, "coverage_report")
    os.makedirs(report_dir, exist_ok=True)
    timestamp = _time.strftime("%Y%m%d_%H%M%S")
    json_path = os.path.join(report_dir, f"coverage_{timestamp}.json")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(coverage, f, indent=2, ensure_ascii=False)

    return os.path.abspath(json_path)


# -----------------------------------------------------------------------
# Handler
# -----------------------------------------------------------------------

def handle_mtr(args, output: "OutputFormatter") -> CommandResult:
    """Handle the 'mtr' command — run native MySQL MTR test suites.

    When ``--mode`` is given (e.g. ``--mode row,col,pq``), the handler
    launches each mode in parallel, with per-mode progress bars and log
    files.  Terminal output is kept minimal (progress bars + final table).
    """
    modes_str = getattr(args, "mode", None)
    if modes_str:
        # Multi-mode parallel execution
        requested = [m.strip().lower() for m in modes_str.split(",") if m.strip()]
        # Apply aliases (e.g. column -> col)
        requested = [_MODE_ALIASES.get(m, m) for m in requested]
        invalid = [m for m in requested if m not in MTR_MODES]
        if invalid:
            return CommandResult.failure(
                f"Unknown MTR mode(s): {', '.join(invalid)}. "
                f"Valid modes: {', '.join(MTR_MODES.keys())}",
                command="mtr",
            )
        if len(requested) < 2:
            # Single mode via --mode, just treat as normal
            mode_def = MTR_MODES[requested[0]]
            args.vector = mode_def["vector"]
            args.parallel_query = mode_def["parallel_query"]
            return _run_native_mtr(args, output)
        return _run_parallel_modes(args, output, requested)
    else:
        # Legacy single-mode execution
        return _run_native_mtr(args, output)


def _parse_mtr_mode_name(args) -> str:
    """Determine the human-readable mode name from args flags."""
    if getattr(args, "vector", False):
        return "col"
    elif getattr(args, "parallel_query", False):
        return "pq"
    return "row"


# -----------------------------------------------------------------------
# Multi-mode parallel runner
# -----------------------------------------------------------------------

def _run_parallel_modes(
    args, output: "OutputFormatter", modes: List[str]
) -> CommandResult:
    """Run multiple MTR modes in parallel with Rich progress UI.

    Each mode gets its own subprocess and log file.  The terminal shows
    a live progress panel with one row per mode, and after all modes
    finish, a summary table is printed.
    """
    from rich import box
    from rich.console import Console
    from rich.live import Live
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text

    console = Console(stderr=True)
    is_json = getattr(args, "json", False)

    # --- 1. Resolve shared config (validates once) ---
    config_path = getattr(args, "config", "rosetta_config.json")
    file_cfg = _load_mtr_config(config_path)

    required_keys = [
        "test_dir", "skip_list", "base_port", "total_port",
        "parallel", "retry", "retry_failure", "max_test_fail",
        "testcase_timeout", "suite_timeout", "mysqld_opts",
    ]
    missing = [k for k in required_keys if k not in file_cfg]
    if missing:
        return CommandResult.failure(
            f"Missing required mtr config in {os.path.abspath(config_path)}: "
            f"{', '.join(missing)}\n"
            f"Please add them under the 'mtr' section. "
            f"Run 'rosetta config --sample' for a template.",
            command="mtr",
        )

    test_dir = getattr(args, "test_dir", None) or file_cfg["test_dir"]
    if not os.path.isdir(test_dir):
        return CommandResult.failure(
            f"MySQL test directory not found: {test_dir}\n"
            f"Set 'mtr.test_dir' in {config_path}, or use --test-dir.",
            command="mtr",
        )
    mtr_bin = os.path.join(test_dir, "mtr")
    if not os.path.isfile(mtr_bin) and not os.path.isfile(mtr_bin + ".py"):
        return CommandResult.failure(
            f"mtr binary not found in {test_dir}",
            command="mtr",
        )

    total_mode = getattr(args, "total", False)
    base_port = file_cfg["total_port"] if total_mode else file_cfg["base_port"]

    # --- 2. Create log directory ---
    log_dir = os.path.join(
        os.path.dirname(os.path.abspath(config_path)),
        "mtr_logs",
        _time.strftime("%Y%m%d_%H%M%S"),
    )
    os.makedirs(log_dir, exist_ok=True)

    # --- 3. Build per-mode configs ---
    mode_cfgs = {}
    for mode_name in modes:
        mode_def = MTR_MODES[mode_name]
        cfg = {
            "test_dir": test_dir,
            "skip_list": getattr(args, "skip_list", None) or file_cfg["skip_list"],
            "parallel": getattr(args, "parallel", None) or file_cfg["parallel"],
            "retry": getattr(args, "retry", None) or file_cfg["retry"],
            "retry_failure": getattr(args, "retry_failure", None) or file_cfg["retry_failure"],
            "max_test_fail": getattr(args, "max_test_fail", None) or file_cfg["max_test_fail"],
            "testcase_timeout": getattr(args, "testcase_timeout", None) or file_cfg["testcase_timeout"],
            "suite_timeout": getattr(args, "suite_timeout", None) or file_cfg["suite_timeout"],
            "port_base": base_port + _MODE_PORT_OFFSETS[mode_name],
            "optimistic": getattr(args, "optimistic", False),
            "record": getattr(args, "record", False),
            "vector": mode_def["vector"],
            "parallel_query": mode_def["parallel_query"],
            "suite": getattr(args, "suite", None),
            "cases": getattr(args, "cases", []),
            # Isolated var/tmp directories per mode to prevent conflicts
            "vardir": os.path.join(test_dir, f"var_{mode_name}"),
            "tmpdir": os.path.join(test_dir, f"tmp_{mode_name}"),
        }
        opts = file_cfg["mysqld_opts"]
        if isinstance(opts, list):
            cfg["mysqld_opts"] = _build_mysqld_opts(opts)
        elif isinstance(opts, str):
            cfg["mysqld_opts"] = opts
        else:
            cfg["mysqld_opts"] = ""
        mode_cfgs[mode_name] = cfg

    # --- 4. Print plan ---
    if not is_json:
        console.print()
        plan_table = Table(
            title="MTR Parallel Execution Plan",
            show_header=True,
            header_style="bold cyan",
            border_style="dim",
            title_style="bold white",
            expand=True,
            box=box.ROUNDED,
        )
        plan_table.add_column("Mode", style="bold", min_width=16)
        plan_table.add_column("Port Base", justify="right")
        plan_table.add_column("Vardir")
        plan_table.add_column("Flags")
        plan_table.add_column("Log File")

        for mode_name in modes:
            mode_def = MTR_MODES[mode_name]
            cfg = mode_cfgs[mode_name]
            flags = []
            if cfg["vector"]:
                flags.append("--ve-protocol")
            if cfg["parallel_query"]:
                flags.append("--parallel-query")
            if cfg["optimistic"]:
                flags.append("optimistic")
            if cfg["record"]:
                flags.append("--record")
            log_file = os.path.join(log_dir, f"{mode_name}.log")
            plan_table.add_row(
                mode_def["label"],
                str(cfg["port_base"]),
                os.path.basename(cfg["vardir"]),
                " ".join(flags) if flags else "(default)",
                os.path.abspath(log_file),
            )

        console.print(plan_table)

        # Config info panel
        info_lines = []
        info_lines.append(f"[bold]Config[/bold]   : {os.path.abspath(config_path)}")
        info_lines.append(f"[bold]Test dir[/bold]  : {test_dir}")
        info_lines.append(f"[bold]Log dir[/bold]   : {log_dir}")
        if getattr(args, "suite", None):
            info_lines.append(f"[bold]Suite[/bold]     : {args.suite}")
        if getattr(args, "cases", []):
            info_lines.append(f"[bold]Cases[/bold]     : {' '.join(args.cases)}")
        console.print(Panel(
            "\n".join(info_lines),
            title="[bold cyan]Configuration[/bold cyan]",
            title_align="left",
            border_style="dim",
            padding=(0, 1),
        ))

        # Print actual MTR commands per mode
        for mode_name in modes:
            cfg = mode_cfgs[mode_name]
            cmd = _build_command(cfg)
            label = MTR_MODES[mode_name]["label"]
            console.print(Panel(
                f"[dim]{cmd}[/dim]",
                title=f"[bold cyan]{label}[/bold cyan]",
                title_align="left",
                border_style="dim",
                padding=(0, 1),
            ))

    # --- 5. Execute modes in parallel with live progress ---
    results_lock = threading.Lock()
    mode_results: Dict[str, dict] = {}
    # Track state for progress display
    mode_state: Dict[str, dict] = {
        m: {"status": "waiting", "elapsed": 0.0, "exit_code": None,
            "last_line": "", "start_time": None}
        for m in modes
    }

    def _run_single_mode(mode_name: str) -> dict:
        """Execute a single MTR mode, writing output to a log file."""
        cfg = mode_cfgs[mode_name]
        cmd = _build_command(cfg)
        log_path = os.path.join(log_dir, f"{mode_name}.log")

        with results_lock:
            mode_state[mode_name]["status"] = "running"
            mode_state[mode_name]["start_time"] = _time.monotonic()

        exit_code = -1
        try:
            proc = subprocess.Popen(
                cmd, shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True, bufsize=1,
                cwd=test_dir,
            )
            with open(log_path, "w", encoding="utf-8") as log_f:
                try:
                    for raw_line in proc.stdout:
                        line = raw_line.rstrip("\n")
                        stripped = line.strip()
                        # Filter noisy lines from log file
                        if _should_suppress(stripped):
                            continue
                        log_f.write(line + "\n")
                        log_f.flush()
                        # Update last meaningful line for progress display
                        if stripped:
                            with results_lock:
                                mode_state[mode_name]["last_line"] = stripped[-80:]
                except KeyboardInterrupt:
                    proc.terminate()
                proc.wait()
                exit_code = proc.returncode
        except Exception as e:
            with open(log_path, "a", encoding="utf-8") as log_f:
                log_f.write(f"\n[ERROR] {e}\n")

        elapsed = _time.monotonic() - (mode_state[mode_name]["start_time"] or _time.monotonic())
        with results_lock:
            mode_state[mode_name]["status"] = "done"
            mode_state[mode_name]["exit_code"] = exit_code
            mode_state[mode_name]["elapsed"] = elapsed

        return {
            "mode": mode_name,
            "label": MTR_MODES[mode_name]["label"],
            "exit_code": exit_code,
            "elapsed": elapsed,
            "log_file": log_path,
            "port_base": cfg["port_base"],
        }

    def _build_progress_table() -> Table:
        """Build the live progress table."""
        table = Table(
            show_header=True,
            header_style="bold cyan",
            border_style="dim",
            expand=True,
            padding=(0, 1),
            box=box.ROUNDED,
        )
        table.add_column("Mode", style="bold", min_width=16)
        table.add_column("Status", min_width=12)
        table.add_column("Elapsed", justify="right", min_width=10)
        table.add_column("Latest Output", ratio=1, overflow="ellipsis", no_wrap=True)

        for m in modes:
            st = mode_state[m]
            label = MTR_MODES[m]["label"]
            elapsed_str = ""
            if st["status"] == "done" and st["elapsed"] > 0:
                # Use frozen elapsed time for completed modes
                elapsed = st["elapsed"]
            elif st["start_time"] is not None:
                # Live counting for running modes
                elapsed = _time.monotonic() - st["start_time"]
            else:
                elapsed = 0
            if elapsed > 0:
                mins, secs = divmod(int(elapsed), 60)
                hours, mins = divmod(mins, 60)
                if hours > 0:
                    elapsed_str = f"{hours}h{mins:02d}m{secs:02d}s"
                else:
                    elapsed_str = f"{mins:02d}m{secs:02d}s"

            if st["status"] == "waiting":
                status = Text("⏳ Waiting", style="dim")
            elif st["status"] == "running":
                status = Text("🔄 Running", style="yellow bold")
            elif st["status"] == "done":
                if st["exit_code"] == 0:
                    status = Text("✅ Passed", style="green bold")
                else:
                    status = Text(f"❌ Failed({st['exit_code']})", style="red bold")
            else:
                status = Text(st["status"])

            table.add_row(label, status, elapsed_str, st.get("last_line", ""))

        return table

    interrupted = False
    if not is_json:
        with Live(
            _build_progress_table(),
            console=console,
            refresh_per_second=2,
            transient=False,
        ) as live:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(modes)
            ) as pool:
                futures = {
                    pool.submit(_run_single_mode, m): m for m in modes
                }

                # Update progress while waiting
                while True:
                    done_futures = {
                        f for f in futures if f.done()
                    }
                    live.update(_build_progress_table())

                    if len(done_futures) == len(futures):
                        break
                    _time.sleep(0.5)

                # Collect results
                for fut in futures:
                    try:
                        result = fut.result()
                        mode_results[result["mode"]] = result
                    except KeyboardInterrupt:
                        interrupted = True
                    except Exception as e:
                        m = futures[fut]
                        mode_results[m] = {
                            "mode": m,
                            "label": MTR_MODES[m]["label"],
                            "exit_code": -1,
                            "elapsed": 0,
                            "log_file": os.path.join(log_dir, f"{m}.log"),
                            "error": str(e),
                        }
    else:
        # JSON mode: no live display
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(modes)
        ) as pool:
            futures = {pool.submit(_run_single_mode, m): m for m in modes}
            for fut in concurrent.futures.as_completed(futures):
                try:
                    result = fut.result()
                    mode_results[result["mode"]] = result
                except Exception as e:
                    m = futures[fut]
                    mode_results[m] = {
                        "mode": m, "exit_code": -1, "error": str(e)
                    }

    if interrupted:
        if not is_json:
            console.print("\n[yellow bold]Interrupted by user.[/yellow bold]")
        return CommandResult.failure("MTR execution interrupted by user", command="mtr")

    # --- 6. Print final summary ---
    if not is_json:
        console.print()
        summary = Table(
            title="MTR Execution Summary",
            show_header=True,
            header_style="bold white on dark_blue",
            border_style="blue",
            title_style="bold white",
            padding=(0, 1),
            box=box.ROUNDED,
            expand=True,
        )
        summary.add_column("Mode", style="bold", min_width=16)
        summary.add_column("Result", min_width=10)
        summary.add_column("Total", justify="center")
        summary.add_column("Pass", justify="center")
        summary.add_column("Fail", justify="center")
        summary.add_column("Pass Rate", justify="center")
        summary.add_column("Elapsed", justify="right", min_width=10)
        summary.add_column("Log File")

        all_passed = True
        mode_stats: Dict[str, dict] = {}
        for m in modes:
            r = mode_results.get(m, {})
            label = MTR_MODES[m]["label"]
            ec = r.get("exit_code", -1)
            elapsed = r.get("elapsed", 0)
            log_file = r.get("log_file", "")

            # Parse stats from log file
            stats = _parse_mtr_log_stats(log_file)
            mode_stats[m] = stats

            # Format elapsed
            mins, secs = divmod(int(elapsed), 60)
            hours, mins = divmod(mins, 60)
            if hours > 0:
                elapsed_str = f"{hours}h{mins:02d}m{secs:02d}s"
            else:
                elapsed_str = f"{mins:02d}m{secs:02d}s"

            if ec == 0:
                result_text = "[green bold]PASSED[/green bold]"
            else:
                result_text = "[red bold]FAILED[/red bold]"
                all_passed = False

            summary.add_row(
                label,
                result_text,
                str(stats.get("total", "-")),
                f"[green]{stats.get('pass', '-')}[/green]",
                f"[red]{stats.get('fail', '-')}[/red]" if stats.get("fail", 0) > 0 else str(stats.get("fail", "-")),
                stats.get("pass_ratio", "-"),
                elapsed_str,
                os.path.abspath(log_file) if log_file else "",
            )

        console.print(summary)

        # Show failed cases per mode
        has_failures = False
        for m in modes:
            stats = mode_stats.get(m, {})
            failing = stats.get("failing_tests", [])
            if failing:
                if not has_failures:
                    console.print()
                    has_failures = True
                label = MTR_MODES[m]["label"]
                cases_str = "\n".join(f"  [red]•[/red] {c}" for c in failing)
                console.print(Panel(
                    cases_str,
                    title=f"[bold red]{label} — Failed Cases ({len(failing)})[/bold red]",
                    title_align="left",
                    border_style="red",
                    padding=(0, 1),
                ))

        console.print(f"\n  Log directory: {log_dir}")

        if all_passed:
            console.print("  [green bold]All modes passed! ✅[/green bold]\n")
        else:
            failed_modes = [
                MTR_MODES[m]["label"]
                for m in modes
                if mode_results.get(m, {}).get("exit_code", -1) != 0
            ]
            console.print(
                f"  [red bold]Failed modes: {', '.join(failed_modes)} ❌[/red bold]"
            )
            console.print(
                "  [dim]Check log files for details.[/dim]\n"
            )

    # --- 7. Build result ---
    any_failed = any(
        r.get("exit_code", -1) != 0 for r in mode_results.values()
    )
    # Parse stats for JSON output (reuse if already parsed)
    if not is_json:
        # Already parsed above
        all_mode_stats = mode_stats
    else:
        all_mode_stats = {}
        for m in modes:
            r = mode_results.get(m, {})
            all_mode_stats[m] = _parse_mtr_log_stats(r.get("log_file", ""))

    result_data = {
        "test_dir": test_dir,
        "modes": modes,
        "port_mode": "total" if total_mode else "base",
        "suite": getattr(args, "suite", None),
        "cases": getattr(args, "cases", []),
        "record": getattr(args, "record", False),
        "optimistic": getattr(args, "optimistic", False),
        "log_dir": log_dir,
        "mode_results": {
            m: {
                "label": MTR_MODES[m]["label"],
                "exit_code": r.get("exit_code", -1),
                "elapsed_seconds": round(r.get("elapsed", 0), 1),
                "log_file": r.get("log_file", ""),
                "total_cases": all_mode_stats.get(m, {}).get("total"),
                "pass_cases": all_mode_stats.get(m, {}).get("pass"),
                "fail_cases": all_mode_stats.get(m, {}).get("fail"),
                "pass_ratio": all_mode_stats.get(m, {}).get("pass_ratio"),
                "failing_tests": all_mode_stats.get(m, {}).get("failing_tests", []),
            }
            for m, r in mode_results.items()
        },
    }

    if any_failed:
        failed_names = [
            MTR_MODES[m]["label"]
            for m in modes
            if mode_results.get(m, {}).get("exit_code", -1) != 0
        ]
        return CommandResult.failure(
            f"MTR failed for mode(s): {', '.join(failed_names)}",
            command="mtr",
            data=result_data,
        )
    return CommandResult.success("mtr", result_data)


def _run_native_mtr(args, output: "OutputFormatter") -> CommandResult:
    """
    Build and execute a native ./mtr command.

    Config resolution order (later wins):
      1. rosetta_config.json ``mtr`` section (required)
      2. CLI flags

    All required settings must be present in the config file; otherwise
    an error message is returned guiding the user to configure them.

    Returns:
        CommandResult with execution status
    """
    # --- 1. Load config file ---
    config_path = getattr(args, "config", "rosetta_config.json")
    file_cfg = _load_mtr_config(config_path)

    # Required config keys — must be set in rosetta_config.json
    required_keys = [
        "test_dir", "skip_list", "base_port", "total_port",
        "parallel", "retry", "retry_failure", "max_test_fail",
        "testcase_timeout", "suite_timeout", "mysqld_opts",
    ]
    missing = [k for k in required_keys if k not in file_cfg]
    if missing:
        return CommandResult.failure(
            f"Missing required mtr config in {os.path.abspath(config_path)}: "
            f"{', '.join(missing)}\n"
            f"Please add them under the 'mtr' section. "
            f"Run 'rosetta config --sample' for a template.",
            command="mtr",
        )

    # Build resolved config from file
    cfg = {
        "test_dir": file_cfg["test_dir"],
        "skip_list": file_cfg["skip_list"],
        "base_port": file_cfg["base_port"],
        "total_port": file_cfg["total_port"],
        "parallel": file_cfg["parallel"],
        "retry": file_cfg["retry"],
        "retry_failure": file_cfg["retry_failure"],
        "max_test_fail": file_cfg["max_test_fail"],
        "testcase_timeout": file_cfg["testcase_timeout"],
        "suite_timeout": file_cfg["suite_timeout"],
        "optimistic": False,
        "record": False,
        "vector": False,
        "parallel_query": False,
        "suite": None,
        "cases": [],
    }

    # mysqld_opts: list → joined CLI flags
    opts = file_cfg["mysqld_opts"]
    if isinstance(opts, list):
        cfg["mysqld_opts"] = _build_mysqld_opts(opts)
    elif isinstance(opts, str):
        cfg["mysqld_opts"] = opts
    else:
        return CommandResult.failure(
            f"Invalid 'mysqld_opts' type in config: expected list or str, got {type(opts).__name__}",
            command="mtr",
        )

    # --- 2. CLI overrides ---
    if getattr(args, "test_dir", None):
        cfg["test_dir"] = args.test_dir
    if getattr(args, "skip_list", None):
        cfg["skip_list"] = args.skip_list
    if getattr(args, "parallel", None):
        cfg["parallel"] = args.parallel
    if getattr(args, "retry", None):
        cfg["retry"] = args.retry
    if getattr(args, "retry_failure", None):
        cfg["retry_failure"] = args.retry_failure
    if getattr(args, "max_test_fail", None):
        cfg["max_test_fail"] = args.max_test_fail
    if getattr(args, "testcase_timeout", None):
        cfg["testcase_timeout"] = args.testcase_timeout
    if getattr(args, "suite_timeout", None):
        cfg["suite_timeout"] = args.suite_timeout

    total_mode = getattr(args, "total", False)
    port_base = cfg["total_port"] if total_mode else cfg["base_port"]
    cfg["port_base"] = port_base

    cfg["optimistic"] = getattr(args, "optimistic", False)
    cfg["record"] = getattr(args, "record", False)
    cfg["vector"] = getattr(args, "vector", False)
    cfg["parallel_query"] = getattr(args, "parallel_query", False)
    cfg["suite"] = getattr(args, "suite", None)
    cfg["cases"] = getattr(args, "cases", [])

    test_dir = cfg["test_dir"]

    # Validate test directory
    if not os.path.isdir(test_dir):
        return CommandResult.failure(
            f"MySQL test directory not found: {test_dir}\n"
            f"Set 'mtr.test_dir' in {config_path}, or use --test-dir.",
            command="mtr",
        )

    # Validate mtr binary
    mtr_bin = os.path.join(test_dir, "mtr")
    if not os.path.isfile(mtr_bin) and not os.path.isfile(mtr_bin + ".py"):
        return CommandResult.failure(
            f"mtr binary not found in {test_dir}\n"
            f"Expected: {mtr_bin} or {mtr_bin}.py",
            command="mtr",
        )

    # Build command
    cmd = _build_command(cfg)

    # --- 3. gcov setup ---
    gcov_enabled = getattr(args, "gcov", False)
    gcov_filter = getattr(args, "gcov_filter", "auto")
    # Normalize: None or empty -> "auto"
    if not gcov_filter:
        gcov_filter = "auto"
    build_dir = _infer_build_dir(test_dir)
    is_json = getattr(args, "json", False)

    if gcov_enabled:
        # Check if build has gcov instrumentation
        is_gcov_build, gcov_detail, is_clang = _check_gcov_build(build_dir)
        if not is_gcov_build:
            return CommandResult.failure(
                f"gcov instrumentation not detected in build directory: "
                f"{build_dir}\n\n"
                f"To enable gcov, recompile with:\n"
                f"  ./make.sh -G 1 -d 1 -m 1\n\n"
                f"Parameters:\n"
                f"  -G 1  Enable gcov coverage instrumentation\n"
                f"  -d 1  Debug mode (recommended for gcov)\n"
                f"  -m 1  Build MTR test binaries\n\n"
                f"The build directory was inferred as: {build_dir}\n"
                f"(parent of test_dir: {test_dir})",
                command="mtr",
            )

        # Check for required tools (auto-selects gcov vs llvm-cov based on compiler)
        missing_tools, gcov_tool, has_fastcov = _check_gcov_tools(is_clang)
        if missing_tools:
            return CommandResult.failure(
                "Required gcov tools not found:\n  "
                + "\n  ".join(missing_tools),
                command="mtr",
            )

        # Clean counters and snapshot .gcda state
        if not is_json:
            print(f"gcov           : ON ({gcov_detail})")
            print(f"gcov tool      : {gcov_tool}")
            print(f"Report tool    : {'fastcov (fast)' if has_fastcov else 'lcov (slow, consider: pip install fastcov)'}")
            print(f"Build directory: {build_dir}")
            if gcov_filter == "auto":
                print(f"gcov filter    : auto (git diff vs origin/master)")
            elif gcov_filter == "all":
                print(f"gcov filter    : all (full project)")
            else:
                print(f"gcov filter    : {gcov_filter}")

        gcov_clean = getattr(args, "gcov_clean", False)
        if gcov_clean:
            if not is_json:
                print(f"Cleaning gcov counters...")
            clean_ok, clean_msg = _gcov_clean(build_dir)
            if not is_json:
                print(f"  {clean_msg}")
        else:
            if not is_json:
                print(f"gcov mode      : accumulate (use --gcov-clean to reset)")

    # Print plan
    if not is_json:
        print(f"Config file    : {os.path.abspath(config_path)}")
        print(f"Test directory : {test_dir}")
        print(f"Mode           : {'total' if total_mode else 'base'}")
        print(f"Port base      : {port_base}")
        print(f"Skip list      : {cfg['skip_list']}")
        if cfg["suite"]:
            print(f"Suite          : {cfg['suite']}")
        if cfg["cases"]:
            print(f"Cases          : {' '.join(cfg['cases'])}")
        if cfg["record"]:
            print(f"Record mode    : ON")
        if cfg["optimistic"]:
            print(f"Optimistic     : ON")
        if cfg["vector"]:
            print(f"Vector engine  : ON")
        if cfg["parallel_query"]:
            print(f"Parallel query : ON")
        print()

    # --- 4. Execute MTR ---
    original_dir = os.getcwd()
    try:
        os.chdir(test_dir)
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        verbose = getattr(args, "verbose", False)
        exit_code = _filter_output(proc, verbose=verbose)
    except Exception as e:
        return CommandResult.failure(f"Failed to execute mtr: {str(e)}", command="mtr")
    finally:
        os.chdir(original_dir)

    if exit_code == -1:
        if not is_json:
            print("\nInterrupted by user.")
        return CommandResult.failure("MTR execution interrupted by user", command="mtr")

    # --- 5. gcov report (after MTR, regardless of exit code) ---
    coverage_data = None
    if gcov_enabled:
        # Resolve filter mode
        if gcov_filter == "auto":
            changed = _find_changed_sources(build_dir)
            if changed:
                if not is_json:
                    print(f"\n  Git diff vs origin/master: "
                          f"{len(changed)} changed source files:")
                    for c in changed:
                        print(f"    {c}")
                _auto_touched = set(changed)
                effective_filter = ""  # Capture all, filter post-parse
            else:
                if not is_json:
                    print("\n  Auto-filter: no changed files found "
                          "vs origin/master, capturing full report")
                _auto_touched = None
                effective_filter = ""
        elif gcov_filter == "all":
            _auto_touched = None
            effective_filter = ""
        else:
            # Explicit glob pattern
            _auto_touched = None
            effective_filter = gcov_filter

        if not is_json:
            print("\nGenerating gcov coverage report...")
            if effective_filter:
                print(f"  Filter: {effective_filter}")

        report_ok, report_result = _gcov_report(
            build_dir, gcov_tool,
            source_filter=effective_filter,
            has_fastcov=has_fastcov)
        if not report_ok:
            if not is_json:
                print(f"  WARNING: Failed to generate coverage report")
                print(f"  Detail: {report_result}")
        else:
            if not is_json:
                print(f"  Info file: {report_result}")

            # Parse, write to file, print summary path
            coverage_data = _parse_lcov_info(report_result)

            # Auto-filter: keep only files changed in git diff
            if _auto_touched and coverage_data.get("files"):
                # _auto_touched contains relative paths like
                # "storage/rocksdb/ha_rocksdb.cc"
                # lcov paths are absolute like
                # "/data/workspace/SQLEngine/storage/rocksdb/ha_rocksdb.cc"
                # Match by checking if the absolute path ends with the relative path
                filtered = {}
                for path, data in coverage_data["files"].items():
                    norm = path.replace("\\", "/")
                    if any(norm.endswith("/" + rel) or norm == rel
                           for rel in _auto_touched):
                        filtered[path] = data
                coverage_data["files"] = filtered
                coverage_data["summary"] = _recalc_summary(filtered)

            cov_file = _write_coverage_report(coverage_data, build_dir)
            if not is_json:
                _print_coverage_table(coverage_data)
                print(f"  Report: {cov_file}")
            coverage_data["report_file"] = cov_file

    # --- 6. Return result ---
    result_data = {
        "test_dir": test_dir,
        "mode": "total" if total_mode else "base",
        "suite": cfg["suite"],
        "cases": cfg["cases"],
        "record": cfg["record"],
        "optimistic": cfg["optimistic"],
        "vector": cfg["vector"],
        "parallel_query": cfg["parallel_query"],
        "exit_code": exit_code,
    }
    if coverage_data:
        # Only include summary + file path in CommandResult (not full file details)
        result_data["coverage"] = {
            "summary": coverage_data.get("summary", {}),
            "report_file": coverage_data.get("report_file", ""),
        }

    if exit_code == 0:
        return CommandResult.success("mtr", result_data)
    else:
        # Still return coverage data even on failure
        return CommandResult.failure(
            f"MTR execution failed with exit code {exit_code}",
            command="mtr",
            data=result_data,
        )
