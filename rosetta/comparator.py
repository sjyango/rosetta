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

    A new block starts when a line:
      - has a [Lnnn] tag prefix (SQL executed by rosetta executor), or
      - starts with '#' (echo comment line).

    Lines that start with SQL keywords but lack [Lnnn] tags are treated
    as output content of the previous block (e.g. EXPLAIN tree output
    that starts with "EXPLAIN -> Filter: ...").
    """
    # Flatten: each element may contain multiple lines separated by \n
    flat = []
    for line in lines:
        flat.extend(line.split("\n"))

    blocks: List[List[str]] = []
    current: List[str] = []

    for line in flat:
        stripped = line.strip()
        if not stripped:
            continue
        # Check if line has [Lnnn] tag → always starts a new block
        has_tag = bool(_RE_LINE_TAG.match(stripped))
        if has_tag or stripped.startswith("#"):
            if current:
                blocks.append(current)
            current = [line]
        else:
            current.append(line)

    if current:
        blocks.append(current)

    return blocks


# ---------------------------------------------------------------------------
# Block alignment helpers
# ---------------------------------------------------------------------------
_RE_LINE_TAG = re.compile(r"^\[#(\d+)\]\s+")


def _block_line_tag(block: List[str]) -> Optional[int]:
    """Extract the [Lnnn] line-number tag from the first line of a block."""
    if not block:
        return None
    m = _RE_LINE_TAG.match(block[0].strip())
    return int(m.group(1)) if m else None


def _align_blocks(blocks_a: List[List[str]],
                  blocks_b: List[List[str]]):
    """Align two block lists by [Lnnn] line tags.

    Returns a list of (block_a_or_empty, block_b_or_empty) pairs.
    Blocks sharing the same [Lxxx] tag are paired together; blocks
    present only on one side are paired with an empty list.
    """
    # Build tag -> block mappings while preserving order.
    def _build(blocks):
        tag_map = {}
        order = []
        untagged_id = -1
        for blk in blocks:
            tag = _block_line_tag(blk)
            if tag is not None:
                tag_map[tag] = blk
                order.append(tag)
            else:
                tag_map[untagged_id] = blk
                order.append(untagged_id)
                untagged_id -= 1
        return tag_map, order

    map_a, order_a = _build(blocks_a)
    map_b, order_b = _build(blocks_b)

    # Merge order: walk through both sequences, preserving relative order
    seen = set()
    merged = []
    ia = ib = 0
    while ia < len(order_a) or ib < len(order_b):
        if ia < len(order_a) and order_a[ia] not in seen:
            key = order_a[ia]
            merged.append(key)
            seen.add(key)
            ia += 1
            if ib < len(order_b) and order_b[ib] == key:
                ib += 1
        elif ib < len(order_b) and order_b[ib] not in seen:
            key = order_b[ib]
            merged.append(key)
            seen.add(key)
            ib += 1
        else:
            if ia < len(order_a):
                ia += 1
            if ib < len(order_b):
                ib += 1

    pairs = []
    for key in merged:
        ba = map_a.get(key, [])
        bb = map_b.get(key, [])
        pairs.append((ba, bb))
    return pairs


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------
def block_has_unexpected_error(block: List[str]) -> bool:
    """Check if a block contains an unexpected error line."""
    return any(l.strip().startswith("ERROR (unexpected):")
               for l in block)


def compare_outputs(lines_a: List[str], lines_b: List[str],
                    name_a: str, name_b: str,
                    baseline_name: Optional[str] = None,
                    skip_sql_types: Optional[List[str]] = None,
                    ) -> CompareResult:
    """Compare two result outputs block-by-block.

    Blocks are aligned by their ``[Lnnn]`` line-number tag so that
    statements skipped on one DBMS (via skip_patterns) do not cause
    all subsequent blocks to mis-align.

    If baseline_name is set, blocks where the baseline has an unexpected
    error are skipped.

    If skip_sql_types is set, blocks whose SQL starts with any of the
    given prefixes (e.g. ["EXPLAIN", "ANALYZE"]) are counted as skipped
    rather than mismatched when they differ.
    """
    result = CompareResult(dbms_a=name_a, dbms_b=name_b)

    blocks_a = split_into_blocks(lines_a)
    blocks_b = split_into_blocks(lines_b)

    # Check whether blocks carry [Lxxx] tags — if both sides have tags
    # we use tag-based alignment; otherwise fall back to positional.
    has_tags_a = any(_block_line_tag(b) is not None for b in blocks_a)
    has_tags_b = any(_block_line_tag(b) is not None for b in blocks_b)

    if has_tags_a and has_tags_b:
        pairs = _align_blocks(blocks_a, blocks_b)
    else:
        # Fallback: positional alignment (legacy behaviour)
        max_blocks = max(len(blocks_a), len(blocks_b))
        pairs = [
            (blocks_a[i] if i < len(blocks_a) else [],
             blocks_b[i] if i < len(blocks_b) else [])
            for i in range(max_blocks)
        ]

    result.total_stmts = len(pairs)

    for idx, (ba, bb) in enumerate(pairs):
        # Blocks that only exist on one side — the other DBMS
        # skipped this statement (e.g. DBMS-specific variable).
        # Record them as skipped but still show in reports.
        if not ba or not bb:
            result.skipped += 1
            stmt = ba[0] if ba else (bb[0] if bb else "???")
            result.diffs.append({
                "block": idx + 1,
                "stmt": stmt,
                "lines_a": filter_warnings(ba) if ba else [],
                "lines_b": filter_warnings(bb) if bb else [],
                "diff": [],
                "context_before": [],
                "context_after": [],
                "skipped": True,
                "skip_reason": (
                    f"Only in {name_a}" if not bb
                    else f"Only in {name_b}" if not ba
                    else ""
                ),
            })
            continue

        if baseline_name:
            baseline_block = ba if name_a == baseline_name else bb
            if block_has_unexpected_error(baseline_block):
                result.skipped += 1
                stmt = ba[0] if ba else (bb[0] if bb else "???")
                result.diffs.append({
                    "block": idx + 1,
                    "stmt": stmt,
                    "lines_a": filter_warnings(ba),
                    "lines_b": filter_warnings(bb),
                    "diff": [],
                    "context_before": [],
                    "context_after": [],
                    "skipped": True,
                    "skip_reason": "Baseline has unexpected error",
                })
                continue

        na = normalize_block(ba)
        nb = normalize_block(bb)

        # Check if this block's SQL type should be skipped from diff
        if skip_sql_types and na != nb:
            first_line = ba[0] if ba else ""
            m = _RE_LINE_TAG.match(first_line.strip())
            sql_text = first_line.strip()[m.end():].strip().upper() if m else first_line.strip().upper()
            if any(sql_text.startswith(prefix.upper()) for prefix in skip_sql_types):
                result.skipped += 1
                stmt = ba[0] if ba else (bb[0] if bb else "???")
                result.diffs.append({
                    "block": idx + 1,
                    "stmt": stmt,
                    "lines_a": filter_warnings(ba),
                    "lines_b": filter_warnings(bb),
                    "diff": [],
                    "context_before": [],
                    "context_after": [],
                    "skipped": True,
                    "skip_reason": "SQL type skipped from comparison",
                })
                continue

        if na == nb:
            result.matched += 1
        else:
            result.mismatched += 1
            diff = list(difflib.unified_diff(
                filter_warnings(ba), filter_warnings(bb),
                fromfile=f"{name_a} (block {idx + 1})",
                tofile=f"{name_b} (block {idx + 1})",
                lineterm="",
            ))

            # Collect nearby block headers as context (2 before, 2 after)
            ctx_before = []
            for ci in range(max(0, idx - 2), idx):
                blk_a, _ = pairs[ci]
                if blk_a:
                    ctx_before.append({"block": ci + 1,
                                       "stmt": blk_a[0][:120]})
            ctx_after = []
            for ci in range(idx + 1, min(idx + 3, len(pairs))):
                blk_a, _ = pairs[ci]
                if blk_a:
                    ctx_after.append({"block": ci + 1,
                                      "stmt": blk_a[0][:120]})

            stmt = ba[0] if ba else (bb[0] if bb else "???")
            fa = filter_warnings(ba)
            fb = filter_warnings(bb)

            result.diffs.append({
                "block": idx + 1,
                "stmt": stmt,
                "lines_a": fa,
                "lines_b": fb,
                "diff": diff,
                "context_before": ctx_before,
                "context_after": ctx_after,
                "skipped": False,
            })

    return result
