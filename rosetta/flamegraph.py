"""Flame graph capture and SVG generation for Rosetta benchmark profiling.

This module provides:
  1. ``PerfProfiler`` – captures CPU stack traces via ``perf record`` / ``perf script``
     for a running mysqld process while a SQL query is being executed.
  2. ``collapse_stacks()`` – folds raw ``perf script`` output into the
     "folded stacks" format (``func1;func2;func3 count``).
  3. ``flamegraph_svg()`` – renders folded stacks into an interactive SVG
     flame graph (pure Python, no external dependencies).

Requirements:
  - ``perf`` installed on the server (``linux-tools-*`` or ``perf-tools``).
  - The mysqld process accessible locally (same machine).
  - Sufficient privileges to run ``perf record -g -p <pid>``.
"""

import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("rosetta")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FlameGraphData:
    """Holds the profiling result for a single capture session."""
    query_name: str = ""
    svg_content: str = ""
    folded_stacks: str = ""
    sample_count: int = 0
    duration_ms: float = 0.0
    error: str = ""


# ---------------------------------------------------------------------------
# perf availability check
# ---------------------------------------------------------------------------

def check_perf_available() -> Tuple[bool, str]:
    """Check if ``perf`` is installed and usable.

    Returns:
        (ok, message) tuple.
    """
    perf_path = shutil.which("perf")
    if not perf_path:
        return False, "perf not found in PATH. Install linux-tools or perf-tools."

    try:
        proc = subprocess.run(
            ["perf", "--version"],
            capture_output=True, text=True, timeout=5,
        )
        if proc.returncode == 0:
            return True, proc.stdout.strip()
        return False, f"perf returned exit code {proc.returncode}: {proc.stderr.strip()}"
    except Exception as e:
        return False, f"perf check failed: {e}"


def find_mysqld_pid(port: int = 3306) -> Optional[int]:
    """Find the PID of the mysqld process listening on *port*.

    Strategy:
      1. Try ``ss -tlnp`` to find the process bound to *port*.
      2. Fall back to ``pidof mysqld``.
    """
    # Strategy 1: ss -tlnp
    try:
        proc = subprocess.run(
            ["ss", "-tlnp"],
            capture_output=True, text=True, timeout=5,
        )
        if proc.returncode == 0:
            for line in proc.stdout.splitlines():
                # Match lines containing the target port
                if f":{port}" in line:
                    # Extract pid from "pid=12345,"
                    m = re.search(r'pid=(\d+)', line)
                    if m:
                        return int(m.group(1))
    except Exception:
        pass

    # Strategy 2: pidof mysqld
    try:
        proc = subprocess.run(
            ["pidof", "mysqld"],
            capture_output=True, text=True, timeout=5,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            # pidof may return multiple PIDs; take the first
            pids = proc.stdout.strip().split()
            return int(pids[0])
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# PerfProfiler – capture CPU stacks around SQL execution
# ---------------------------------------------------------------------------

class PerfProfiler:
    """Profile a mysqld process using ``perf record`` during query execution.

    Usage::

        profiler = PerfProfiler(mysqld_pid=12345, perf_freq=99)

        profiler.start()          # starts perf record in background
        cursor.execute(sql)       # run the query
        fg_data = profiler.stop() # stops perf, generates SVG
    """

    def __init__(
        self,
        mysqld_pid: int,
        perf_freq: int = 99,
        tmp_dir: Optional[str] = None,
    ):
        self.mysqld_pid = mysqld_pid
        self.perf_freq = perf_freq
        self._tmp_dir = tmp_dir or tempfile.mkdtemp(prefix="rosetta_perf_")
        self._perf_data_path = os.path.join(self._tmp_dir, "perf.data")
        self._perf_proc: Optional[subprocess.Popen] = None
        self._start_time: float = 0.0

    def start(self):
        """Start ``perf record`` in the background."""
        cmd = [
            "perf", "record",
            "-g",                          # call-graph (DWARF or fp)
            "-F", str(self.perf_freq),     # sampling frequency
            "-p", str(self.mysqld_pid),    # attach to mysqld
            "-o", self._perf_data_path,    # output file
            "--",
        ]

        try:
            self._perf_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            self._start_time = time.monotonic()
        except Exception as e:
            log.warning("Failed to start perf record: %s", e)
            self._perf_proc = None

    def stop(self, query_name: str = "") -> FlameGraphData:
        """Stop ``perf record``, process output, and return flame graph data."""
        result = FlameGraphData(query_name=query_name)

        if self._perf_proc is None:
            result.error = "perf record was not started"
            return result

        duration = time.monotonic() - self._start_time
        result.duration_ms = duration * 1000.0

        # Send SIGINT to perf to stop recording gracefully
        try:
            self._perf_proc.send_signal(2)  # SIGINT
            self._perf_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            log.warning("perf record did not stop after SIGINT, killing")
            self._perf_proc.kill()
            try:
                self._perf_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log.warning("perf record could not be killed")
                result.error = "perf record could not be stopped"
                return result
        except Exception as e:
            log.warning("Error stopping perf: %s", e)
            result.error = f"Failed to stop perf: {e}"
            return result

        # Check if perf.data was created
        if not os.path.isfile(self._perf_data_path):
            stderr_out = ""
            try:
                stderr_out = self._perf_proc.stderr.read().decode(
                    "utf-8", errors="replace")
            except Exception:
                pass
            result.error = f"perf.data not created. stderr: {stderr_out}"
            return result

        # Check perf.data size — skip processing if too large (>200MB)
        # to avoid perf script hanging for minutes
        try:
            data_size = os.path.getsize(self._perf_data_path)
            if data_size > 200 * 1024 * 1024:
                result.error = (
                    f"perf.data too large ({data_size // (1024*1024)}MB), "
                    f"skipping flamegraph generation")
                log.warning("[%s] %s", query_name, result.error)
                return result
        except OSError:
            pass

        # Run perf script to get human-readable stack traces
        # Use a generous but bounded timeout to avoid hanging indefinitely
        perf_script_timeout = max(60, min(300, int(duration * 3)))
        try:
            script_proc = subprocess.run(
                ["perf", "script", "-i", self._perf_data_path],
                capture_output=True, text=True, timeout=perf_script_timeout,
            )
            if script_proc.returncode != 0:
                result.error = (
                    f"perf script failed (rc={script_proc.returncode}): "
                    f"{script_proc.stderr[:500]}")
                return result

            raw_script = script_proc.stdout
        except subprocess.TimeoutExpired:
            result.error = (
                f"perf script timed out after {perf_script_timeout}s "
                f"(perf.data may be too large)")
            log.warning("[%s] %s", query_name, result.error)
            return result
        except Exception as e:
            result.error = f"perf script error: {e}"
            return result

        # Collapse stacks
        folded = collapse_stacks(raw_script)
        result.folded_stacks = folded

        # Count total samples
        total_samples = 0
        for line in folded.splitlines():
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    total_samples += int(parts[1])
                except ValueError:
                    pass
        result.sample_count = total_samples

        # Generate SVG
        if total_samples > 0:
            result.svg_content = flamegraph_svg(
                folded, title=query_name or "Flame Graph")
        else:
            result.error = "No samples captured (query may have been too fast)"

        return result

    def cleanup(self):
        """Remove temporary perf data files."""
        try:
            if os.path.isdir(self._tmp_dir):
                shutil.rmtree(self._tmp_dir, ignore_errors=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Stack collapsing (equivalent to stackcollapse-perf.pl)
# ---------------------------------------------------------------------------

def collapse_stacks(perf_script_output: str) -> str:
    """Collapse ``perf script`` output into folded stack format.

    Each output line: ``func1;func2;func3 count``

    This is a pure-Python equivalent of Brendan Gregg's
    ``stackcollapse-perf.pl`` script.
    """
    stacks: Dict[str, int] = defaultdict(int)
    current_stack: List[str] = []
    in_stack = False

    for line in perf_script_output.splitlines():
        line = line.rstrip()

        # Empty line = end of a stack trace
        if not line:
            if current_stack:
                # Reverse to get caller→callee order (bottom-up)
                key = ";".join(reversed(current_stack))
                stacks[key] += 1
                current_stack = []
                in_stack = False
            continue

        # Stack frame lines start with whitespace and a hex address
        # Example: "    ffffffff81234567 do_something+0x12 ([kernel.kallsyms])"
        if line.startswith(("\t", " ")) and in_stack:
            # Extract function name
            stripped = line.strip()
            # Format: <addr> <func+offset> (<module>)
            parts = stripped.split(None, 2)
            if len(parts) >= 2:
                func = parts[1]
                # Remove offset suffix: "func+0x1a" → "func"
                plus_idx = func.find("+0x")
                if plus_idx > 0:
                    func = func[:plus_idx]
                current_stack.append(func)
            continue

        # Header line (process info): "mysqld 12345 [001] 12345.678901: ..."
        # Indicates start of a new sample
        if not line.startswith(("\t", " ")):
            if current_stack:
                key = ";".join(reversed(current_stack))
                stacks[key] += 1
                current_stack = []
            in_stack = True
            continue

    # Handle last stack if file doesn't end with blank line
    if current_stack:
        key = ";".join(reversed(current_stack))
        stacks[key] += 1

    # Sort by count (descending) for consistent output
    lines = []
    for stack, count in sorted(stacks.items(), key=lambda x: -x[1]):
        lines.append(f"{stack} {count}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SVG Flame Graph renderer (pure Python)
# ---------------------------------------------------------------------------

# Color palette is now generated algorithmically in _flame_color().
# No static list needed.


def flamegraph_svg(
    folded_stacks: str,
    title: str = "Flame Graph",
    width: int = 1200,
    frame_height: int = 16,
    font_size: float = 12.0,
    min_width_px: float = 0.1,
) -> str:
    """Generate an interactive SVG flame graph from folded stack data.

    Args:
        folded_stacks: Folded stacks (``func1;func2 count`` per line).
        title: Title shown at the top of the SVG.
        width: Total SVG width in pixels.
        frame_height: Height of each stack frame in pixels.
        font_size: Font size for labels.
        min_width_px: Minimum frame width to render (in pixels).

    Returns:
        SVG content as a string.
    """
    # Parse folded stacks
    stack_counts: Dict[str, int] = {}
    total_samples = 0
    for line in folded_stacks.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue
        stack, count_str = parts
        try:
            count = int(count_str)
        except ValueError:
            continue
        stack_counts[stack] = stack_counts.get(stack, 0) + count
        total_samples += count

    if total_samples == 0:
        return ""

    # Build a tree from the folded stacks
    # Each node: {name, value, children: {name: node}}
    root = {"name": "root", "value": 0, "children": {}}

    for stack, count in stack_counts.items():
        funcs = stack.split(";")
        node = root
        for func in funcs:
            if func not in node["children"]:
                node["children"][func] = {
                    "name": func, "value": 0, "children": {},
                }
            node = node["children"][func]
        node["value"] += count

    # Propagate values upward (each node's total = own + children)
    def _total_value(node):
        total = node["value"]
        for child in node["children"].values():
            total += _total_value(child)
        return total

    root_total = _total_value(root)

    # Compute the maximum depth for SVG height
    def _max_depth(node, depth=0):
        if not node["children"]:
            return depth
        return max(_max_depth(c, depth + 1) for c in node["children"].values())

    max_depth = _max_depth(root)
    y_pad_top = 50   # space for title and info
    y_pad_bottom = 40  # space for bottom text
    svg_height = y_pad_top + (max_depth + 2) * frame_height + y_pad_bottom
    x_pad = 10
    chart_width = width - 2 * x_pad

    # Flatten tree into rectangles
    # Each rect: (x, y, w, h, name, self_count, total_count)
    rects: List[Tuple[float, float, float, float, str, int, int]] = []

    def _layout(node, x_start, depth):
        total = _total_value(node)
        w = (total / root_total) * chart_width

        if w < min_width_px:
            return

        # y position: flame graph is bottom-up, so deeper = lower
        # We'll use an inverted layout: root at bottom
        y = svg_height - y_pad_bottom - (depth + 1) * frame_height

        rects.append((
            x_start + x_pad, y, w, frame_height - 1,
            node["name"], node["value"], total,
        ))

        # Layout children left-to-right
        child_x = x_start
        for child in sorted(node["children"].values(),
                            key=lambda c: _total_value(c), reverse=True):
            child_total = _total_value(child)
            child_w = (child_total / root_total) * chart_width
            if child_w >= min_width_px:
                _layout(child, child_x, depth + 1)
            child_x += child_w

    # Layout from root's children (skip the "root" pseudo-node itself)
    child_x = 0.0
    for child in sorted(root["children"].values(),
                        key=lambda c: _total_value(c), reverse=True):
        _layout(child, child_x, 0)
        child_total = _total_value(child)
        child_x += (child_total / root_total) * chart_width

    # Generate SVG
    svg_parts = []
    svg_parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {width} {svg_height}" '
        f'width="{width}" height="{svg_height}" '
        f'style="background:#0d1117" '
        f'data-chart-width="{chart_width}" data-x-pad="{x_pad}" '
        f'data-total-samples="{total_samples}">'
    )

    # Embedded styles and interactivity
    svg_parts.append("""
<defs>
  <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
    <stop offset="0%" style="stop-color:#161b22"/>
    <stop offset="100%" style="stop-color:#0d1117"/>
  </linearGradient>
  <filter id="outline" x="-2%" y="-2%" width="104%" height="104%">
    <feMorphology in="SourceAlpha" result="dilated" operator="dilate" radius="1"/>
    <feFlood flood-color="#000000" flood-opacity="0.9" result="black"/>
    <feComposite in="black" in2="dilated" operator="in" result="shadow"/>
    <feMerge>
      <feMergeNode in="shadow"/>
      <feMergeNode in="SourceGraphic"/>
    </feMerge>
  </filter>
</defs>
<style>
  .fg-frame { stroke: #0d1117; stroke-width: 0.5; cursor: pointer; }
  .fg-frame:hover { stroke: #f0e68c; stroke-width: 1.5; }
  .fg-label { font-family: 'SF Mono', Consolas, monospace; fill: #ffffff;
    filter: url(#outline); pointer-events: none; }
  .fg-title { font-family: -apple-system, sans-serif; fill: #e6edf3;
    font-weight: 700; }
  .fg-info { font-family: 'SF Mono', Consolas, monospace; fill: #8b949e; }
</style>
""")

    # Background
    svg_parts.append(
        f'<rect x="0" y="0" width="{width}" height="{svg_height}" '
        f'fill="url(#bg)" />'
    )

    # Title
    svg_parts.append(
        f'<text x="{width / 2}" y="24" text-anchor="middle" '
        f'class="fg-title" font-size="16">{_svg_escape(title)}</text>'
    )

    # Info line
    svg_parts.append(
        f'<text x="{width / 2}" y="40" text-anchor="middle" '
        f'class="fg-info" font-size="11">'
        f'samples: {total_samples} | '
        f'Hover for details, click to zoom</text>'
    )

    # Tooltip / details container
    svg_parts.append(
        f'<text class="fg-details" x="{x_pad}" y="{svg_height - 12}" '
        f'font-size="11"> </text>'
    )

    # Render frames – store original geometry as data attributes so the
    # HTML-side JS can implement zoom (scale / translate) without needing
    # an embedded <script> (which does not execute when SVG is injected
    # via innerHTML).
    #
    # Each frame's text is clipped via a <clipPath> that matches the
    # rectangle bounds, so labels never overflow even if truncation
    # estimates are slightly off.  A trailing "…" is appended when the
    # label is truncated to signal that the full name is available on
    # hover / in the details bar.
    char_w = font_size * 0.65  # approximate width of a single monospace char
    min_text_w = 20  # show text in frames wider than this (px)

    for i, (x, y, w, h, name, self_count, total_count) in enumerate(rects):
        color = _flame_color(name, i)
        pct = (total_count / total_samples) * 100

        # Truncate label to fit – leave 8 px padding (4 left + 4 right)
        avail_w = w - 8
        max_chars = int(avail_w / char_w) if avail_w > 0 else 0

        if max_chars >= len(name):
            label = name  # fits completely
        elif max_chars > 3:
            label = name[: max_chars - 1] + "\u2026"  # truncate + ellipsis
        elif max_chars > 0:
            label = name[: max_chars]  # very narrow – no room for ellipsis
        else:
            label = ""

        clip_id = f"clip-{i}"

        svg_parts.append(
            f'<g class="fg-frame" '
            f'data-name="{_svg_escape(name)}" '
            f'data-samples="{total_count}" '
            f'data-pct="{pct:.2f}" '
            f'data-x="{x:.1f}" data-y="{y:.1f}" '
            f'data-w="{w:.1f}" data-h="{h}">'
        )
        # clipPath keeps text inside the rectangle
        svg_parts.append(
            f'<clipPath id="{clip_id}">'
            f'<rect x="{x:.1f}" y="{y:.1f}" '
            f'width="{w:.1f}" height="{h}" />'
            f'</clipPath>'
        )
        svg_parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" '
            f'width="{w:.1f}" height="{h}" '
            f'fill="{color}" rx="2" />'
        )
        if label and w > min_text_w:
            svg_parts.append(
                f'<text x="{x + 4:.1f}" y="{y + h - 3:.1f}" '
                f'clip-path="url(#{clip_id})" '
                f'class="fg-label" font-size="{font_size}">'
                f'{_svg_escape(label)}</text>'
            )
        svg_parts.append('</g>')

    svg_parts.append('</svg>')

    return "\n".join(svg_parts)


def _svg_escape(text: str) -> str:
    """Escape text for safe SVG/XML inclusion."""
    return (text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&#39;"))


def _flame_color(name: str, index: int) -> str:
    """Pick a warm flame color for a flame graph frame.

    Uses a hash-based approach to generate classic flame graph colors
    (red → orange → yellow gradient).  The same function name always
    gets the same color for easy cross-run comparison.
    """
    # Kernel frames / unknown addresses → muted grey
    if name.startswith(("[", "0x")):
        return "#626262"

    # Hash the name for deterministic color assignment
    h = hash(name) & 0xFFFFFFFF

    # Classic flame graph palette:
    #   Red channel:  200–240   (warm base)
    #   Green channel: 50–200   (controls red→orange→yellow shift)
    #   Blue channel:  30–55    (subtle warmth)
    r = 200 + (h % 41)          # 200–240
    g = 50 + ((h >> 8) % 151)   # 50–200
    b = 30 + ((h >> 16) % 26)   # 30–55

    return f"#{r:02x}{g:02x}{b:02x}"
