"""Result comparison engine for Rosetta."""

import difflib
import re
from typing import Dict, List, Optional

from .models import CompareResult

# ---------------------------------------------------------------------------
# Normalization regex patterns (compiled once at module load)
# ---------------------------------------------------------------------------
_RE_ENGINE = re.compile(r"ENGINE\s*=\s*\w+")
_RE_CHARSET_COLLATE = re.compile(
    r"DEFAULT CHARSET=\w+(\s+COLLATE=\w+)?"
)
_RE_ERROR_LINE = re.compile(r"^ERROR\b[^(]*\((\d+),")
_RE_AUTO_INCREMENT = re.compile(r"\s*AUTO_INCREMENT=\d+")
_RE_ROW_FORMAT = re.compile(r"\s*ROW_FORMAT=\w+")
_RE_STATS_PERSISTENT = re.compile(r"\s*STATS_PERSISTENT=\d+")
_RE_TDSQL_TAIL = re.compile(
    r"\.\s*txid:\s*\S+\.\s*sql-node:\s*\S+\.\s*error-store-node:\s*\S+\s*$"
)
_RE_DEFINER = re.compile(r"DEFINER=`[^`]*`@`[^`]*`")
_RE_WARNING_LINE = re.compile(r"^Warning\s+\d+\s+")

# SQL statement start pattern for block splitting.
# Lines may optionally carry a "[Lnnn] " prefix emitted by the executor.
_RE_SQL_START = re.compile(
    r"^(\[L\d+\]\s+)?"
    r"(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|SHOW|EXPLAIN|"
    r"ANALYZE|TRUNCATE|SET|BEGIN|COMMIT|ROLLBACK|CALL|GRANT|REVOKE|"
    r"FLUSH|RENAME|LOCK|UNLOCK|USE|DESCRIBE|DESC)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Line normalization
# ---------------------------------------------------------------------------
def normalize_line(line: str) -> str:
    """Normalize a single output line to ignore known non-functional diffs.

    Handled cases:
      - ERROR lines: only keep error code
      - TDSQL tail (txid, sql-node, error-store-node)
      - ENGINE=, CHARSET=, AUTO_INCREMENT=, ROW_FORMAT=, DEFINER=, etc.
    """
    s = line
    m = _RE_ERROR_LINE.match(s)
    if m:
        return f"ERROR: ({m.group(1)})"
    if s.startswith("ERROR"):
        return "ERROR: (unknown)"
    s = _RE_TDSQL_TAIL.sub("", s)
    s = _RE_ENGINE.sub("ENGINE=<NORMALIZED>", s)
    s = _RE_CHARSET_COLLATE.sub("DEFAULT CHARSET=<NORMALIZED>", s)
    s = _RE_AUTO_INCREMENT.sub("", s)
    s = _RE_ROW_FORMAT.sub("", s)
    s = _RE_STATS_PERSISTENT.sub("", s)
    s = _RE_DEFINER.sub("DEFINER=<NORMALIZED>", s)
    return s


def normalize_block(block: List[str]) -> List[str]:
    """Normalize all lines in a block for comparison.

    Filters out Warning lines and the "Warnings:" header.
    """
    lines = [normalize_line(l) for l in block]
    return [l for l in lines
            if l.strip() != "Warnings:"
            and not _RE_WARNING_LINE.match(l.strip())]


def filter_warnings(block: List[str]) -> List[str]:
    """Remove warning lines from a block (for cleaner diff output)."""
    return [l for l in block
            if l.strip() != "Warnings:"
            and not _RE_WARNING_LINE.match(l.strip())]


# ---------------------------------------------------------------------------
# Block splitting
# ---------------------------------------------------------------------------
def split_into_blocks(lines: List[str]) -> List[List[str]]:
    """Split output lines into logical blocks.

    A new block starts at SQL statements or echo comment lines (# ...).
    """
    blocks: List[List[str]] = []
    current: List[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if _RE_SQL_START.match(stripped) or stripped.startswith("#"):
            if current:
                blocks.append(current)
            current = [line]
        else:
            current.append(line)

    if current:
        blocks.append(current)

    return blocks


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------
def block_has_unexpected_error(block: List[str]) -> bool:
    """Check if a block contains an unexpected error line."""
    return any(l.strip().startswith("ERROR (unexpected):")
               for l in block)


def compare_outputs(lines_a: List[str], lines_b: List[str],
                    name_a: str, name_b: str,
                    baseline_name: Optional[str] = None) -> CompareResult:
    """Compare two result outputs block-by-block.

    If baseline_name is set, blocks where the baseline has an unexpected
    error are skipped.
    """
    result = CompareResult(dbms_a=name_a, dbms_b=name_b)

    blocks_a = split_into_blocks(lines_a)
    blocks_b = split_into_blocks(lines_b)

    max_blocks = max(len(blocks_a), len(blocks_b))
    result.total_stmts = max_blocks

    for i in range(max_blocks):
        ba = blocks_a[i] if i < len(blocks_a) else []
        bb = blocks_b[i] if i < len(blocks_b) else []

        if baseline_name:
            baseline_block = ba if name_a == baseline_name else bb
            if block_has_unexpected_error(baseline_block):
                result.skipped += 1
                continue

        na = normalize_block(ba)
        nb = normalize_block(bb)

        if na == nb:
            result.matched += 1
        else:
            result.mismatched += 1
            diff = list(difflib.unified_diff(
                filter_warnings(ba), filter_warnings(bb),
                fromfile=f"{name_a} (block {i + 1})",
                tofile=f"{name_b} (block {i + 1})",
                lineterm="",
            ))

            # Collect nearby block headers as context (2 before, 2 after)
            # so reports can show surrounding SQL for quick orientation.
            ctx_before = []
            for ci in range(max(0, i - 2), i):
                blk = blocks_a[ci] if ci < len(blocks_a) else []
                if blk:
                    ctx_before.append({"block": ci + 1,
                                       "stmt": blk[0][:120]})
            ctx_after = []
            for ci in range(i + 1, min(i + 3, max_blocks)):
                blk = blocks_a[ci] if ci < len(blocks_a) else []
                if blk:
                    ctx_after.append({"block": ci + 1,
                                      "stmt": blk[0][:120]})

            result.diffs.append({
                "block": i + 1,
                "stmt": ba[0] if ba else (bb[0] if bb else "???"),
                "lines_a": filter_warnings(ba),
                "lines_b": filter_warnings(bb),
                "diff": diff,
                "context_before": ctx_before,
                "context_after": ctx_after,
            })

    return result
