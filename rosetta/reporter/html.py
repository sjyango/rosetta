"""HTML report generator for Rosetta.

Generates a single self-contained HTML file with:
- Summary table with per-comparison pass rates
- Per-statement multi-DBMS side-by-side panels (Playground style)
- Block context and SQL line numbers
- Filter bar (All/Match/Diff/Skip)
"""

import html
import json
import logging
import re
import time
from typing import Dict, List, Optional

from ..models import CompareResult, Statement, StmtType

log = logging.getLogger("rosetta")

# regex: lines may carry "[#nnn] " prefix from executor (global sequence tag)
_RE_LINE_TAG = re.compile(r"^\[#(\d+)\]\s+")
_RE_SQL_START = re.compile(
    r"^(SELECT|INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|DROP|SHOW|EXPLAIN|"
    r"ANALYZE|TRUNCATE|SET|BEGIN|COMMIT|ROLLBACK|CALL|GRANT|REVOKE|"
    r"FLUSH|RENAME|LOCK|UNLOCK|USE|DESCRIBE|DESC|LOAD|PREPARE|EXECUTE|"
    r"DEALLOCATE|DO|HANDLER|WITH|CHECK|OPTIMIZE|REPAIR|CHECKSUM|RESET|"
    r"INSTALL|UNINSTALL|XA|SAVEPOINT|RELEASE|HELP|SIGNAL|RESIGNAL|GET|"
    r"START|STOP|CHANGE|PURGE|BINLOG|CACHE|KILL|SHUTDOWN)\b",
    re.IGNORECASE,
)


def _escape(text: str) -> str:
    """HTML-escape a string."""
    return html.escape(text, quote=True)


def _safe_json(obj):
    """JSON-encode for embedding in <script> tags."""
    s = json.dumps(obj, ensure_ascii=False)
    s = s.replace("<", "\\u003c")
    return s


# ---------------------------------------------------------------------------
# Data builders
# ---------------------------------------------------------------------------

def _build_summary_data(comparisons: Dict[str, CompareResult]) -> List[dict]:
    """Build summary data for the template."""
    rows = []
    for key, cmp in comparisons.items():
        rows.append({
            "key": key,
            "dbms_a": cmp.dbms_a,
            "dbms_b": cmp.dbms_b,
            "matched": cmp.matched,
            "mismatched": cmp.mismatched,
            "effective_mismatched": cmp.effective_mismatched,
            "skipped": cmp.skipped,
            "total": cmp.total_stmts,
            "pass_rate": round(cmp.pass_rate, 1),
        })
    return rows


def _build_sql_list_data(sql_list: Optional[List[Statement]]) -> List[dict]:
    """Build SQL list data for the template.

    Only includes statements that are actually executed (StmtType.SQL),
    excluding directives like ECHO, ERROR, SORTED_RESULT, SKIP, DDL_WAIT.
    """
    if not sql_list:
        return []
    return [
        {
            "idx": i + 1,
            "sql": s.text,
            "line_no": s.line_no,
            "skipped": s.stmt_type == StmtType.SKIP,
        }
        for i, s in enumerate(sql_list)
        if s.text.strip() and s.stmt_type == StmtType.SQL
    ]


def _split_into_blocks(lines: List[str]) -> List[List[str]]:
    """Split output lines into logical blocks (same logic as comparator).

    A new block starts when a line has a [Lnnn] tag or starts with '#'.
    Lines starting with SQL keywords but without [Lnnn] are treated as
    output content (e.g. EXPLAIN tree output).
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


def _block_line_tag(block: List[str]) -> Optional[int]:
    """Extract [Lnnn] line number tag from the first line of a block."""
    if not block:
        return None
    m = _RE_LINE_TAG.match(block[0].strip())
    return int(m.group(1)) if m else None


def _block_sql(block: List[str]) -> str:
    """Extract the full SQL statement from a block, joined into one line.

    Multi-line SQL (e.g. CREATE TABLE) is concatenated until the line
    ending with ';'. The [Lnnn] tag is stripped from the first line.
    """
    if not block:
        return ""
    parts = []
    for i, raw in enumerate(block):
        line = raw.strip()
        if i == 0:
            m = _RE_LINE_TAG.match(line)
            if m:
                line = line[m.end():].strip()
        parts.append(line)
        if line.endswith(";"):
            break
    return " ".join(parts)


def _align_all_blocks(results: Dict[str, List[str]]) -> dict:
    """Align blocks from all DBMS by [Lnnn] tags.

    Returns:
        {
            "block_keys": [line_tag_or_index, ...],  # ordered
            "blocks": {
                dbms_name: {line_tag: block_lines, ...},
                ...
            }
        }
    """
    all_blocks = {}
    for name, lines in results.items():
        blocks = _split_into_blocks(lines)
        tag_map = {}
        untagged = -1
        order = []
        for blk in blocks:
            tag = _block_line_tag(blk)
            if tag is not None:
                tag_map[tag] = blk
                order.append(tag)
            else:
                tag_map[untagged] = blk
                order.append(untagged)
                untagged -= 1
        all_blocks[name] = (tag_map, order)

    # Merge all key orderings
    seen = set()
    merged = []
    orders = [all_blocks[n][1] for n in results]
    pointers = [0] * len(orders)

    changed = True
    while changed:
        changed = False
        for oi, order in enumerate(orders):
            while pointers[oi] < len(order):
                key = order[pointers[oi]]
                if key in seen:
                    pointers[oi] += 1
                    changed = True
                    continue
                merged.append(key)
                seen.add(key)
                pointers[oi] += 1
                changed = True
                break

    return {
        "block_keys": merged,
        "blocks": {n: all_blocks[n][0] for n in results},
    }


def _build_block_statuses(
        comparisons: Dict[str, CompareResult],
        total_blocks: int,
        line_tags: List[Optional[int]],
        sql_texts: List[str]) -> list:
    """Build per-block status from pairwise comparison results.

    Uses [Lnnn] line tags to match blocks across different comparison pairs,
    since block_idx may differ between pairs (different totals).

    For untagged blocks (e.g. # echo lines), falls back to matching by
    the SQL/statement text.

    Args:
        comparisons: pairwise comparison results
        total_blocks: total number of blocks in the multi-DBMS view
        line_tags: list of line_tag for each block (same order as STMTS)
        sql_texts: list of SQL text for each block (same order as STMTS)

    Returns:
        List of status strings ("match"|"diff"|"skip"), one per block.
    """
    # Build a mapping: line_tag -> block_idx (1-based) in our view
    tag_to_idx = {}
    for i, tag in enumerate(line_tags):
        if tag is not None:
            tag_to_idx[tag] = i + 1

    # Build a mapping: sql_text -> block_idx for untagged blocks
    text_to_idx = {}
    for i, (tag, text) in enumerate(zip(line_tags, sql_texts)):
        if tag is None and text.strip():
            text_to_idx[text.strip()] = i + 1

    # Initialize all blocks as match (will be overridden by comparison data)
    statuses = ["match"] * total_blocks

    # Map comparison diffs to our block ordering
    for key, cmp in comparisons.items():
        for d in cmp.diffs:
            # Extract line tag from the statement line
            stmt_line = d.get("stmt", "")
            m = _RE_LINE_TAG.match(stmt_line.strip())
            diff_tag = int(m.group(1)) if m else None

            # Find our block index by line tag
            idx = None
            if diff_tag is not None and diff_tag in tag_to_idx:
                idx = tag_to_idx[diff_tag]
            else:
                # Fallback for untagged blocks: match by stmt text
                clean = _block_sql([stmt_line]) if stmt_line else ""
                if clean.strip() and clean.strip() in text_to_idx:
                    idx = text_to_idx[clean.strip()]

            if idx is not None and 1 <= idx <= total_blocks:
                is_skip = d.get("skipped", False)
                if is_skip:
                    if statuses[idx - 1] == "match":
                        statuses[idx - 1] = "skip"
                else:
                    # diff overrides skip
                    statuses[idx - 1] = "diff"

    return statuses


def _build_stmt_data(results: Dict[str, List[str]],
                     baseline: str,
                     comparisons: Optional[Dict[str, CompareResult]] = None
                     ) -> List[dict]:
    """Build per-statement data for the multi-DBMS grid view.

    Each entry:
    {
        "block_idx": 1,
        "line_tag": 42 or null,
        "sql": "SELECT ...",
        "status": "match"|"diff"|"skip",   # from comparator
        "dbms_results": {
            "dbms_name": {
                "lines": ["col1\\tcol2", "val1\\tval2", ...],
                "present": true
            },
            ...
        }
    }
    """
    if not results:
        return []

    aligned = _align_all_blocks(results)
    block_keys = aligned["block_keys"]
    blocks = aligned["blocks"]
    dbms_names = list(results.keys())

    # Pre-extract sql_text for each block (needed for status matching)
    sql_texts = []
    for key in block_keys:
        sql_text = ""
        for name in dbms_names:
            blk = blocks[name].get(key)
            if blk:
                sql_text = _block_sql(blk)
                break
        sql_texts.append(sql_text)

    # Build block statuses from comparisons if available
    block_statuses = []
    if comparisons:
        total = len(block_keys)
        line_tags = [key if isinstance(key, int) and key >= 0 else None
                     for key in block_keys]
        block_statuses = _build_block_statuses(
            comparisons, total, line_tags, sql_texts)

    stmts = []
    block_num = 0
    for idx, key in enumerate(block_keys):
        sql_text = sql_texts[idx]

        # Skip non-SQL blocks: only keep blocks starting with SQL keywords
        if not sql_text or not _RE_SQL_START.match(sql_text):
            continue

        block_num += 1

        dbms_results = {}
        for name in dbms_names:
            blk = blocks[name].get(key)
            if blk:
                # Find where SQL statement ends (line ending with ;)
                # and result output begins. The first line is always SQL
                # (has [Lnnn] tag). Multi-line SQL (e.g. CREATE TABLE)
                # continues until a line ends with ';'.
                result_start = 1  # default: skip only the first line
                for li in range(len(blk)):
                    if blk[li].rstrip().endswith(';'):
                        result_start = li + 1
                        break
                output_lines = blk[result_start:] if result_start < len(blk) else []
                dbms_results[name] = {
                    "lines": output_lines,
                    "present": True,
                    "first_line": blk[0] if blk else "",
                }
            else:
                dbms_results[name] = {
                    "lines": [],
                    "present": False,
                    "first_line": "",
                }

        stmt = {
            "block_idx": block_num,
            "line_tag": key if isinstance(key, int) and key >= 0 else None,
            "sql": sql_text,
            "dbms_results": dbms_results,
        }

        # Use comparator status if available, otherwise compute from presence
        if block_statuses and idx < len(block_statuses):
            stmt["status"] = block_statuses[idx]
        else:
            # Fallback: determine from presence data
            if any(not dbms_results[n]["present"] for n in dbms_names):
                stmt["status"] = "skip"
            else:
                stmt["status"] = "match"

        stmts.append(stmt)

    return stmts


# ---------------------------------------------------------------------------
# HTML Template
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rosetta Report — {{TEST_NAME}}</title>
<style>
:root {
  --bg: #0d1117; --bg2: #161b22; --bg3: #21262d;
  --fg: #c9d1d9; --fg2: #8b949e;
  --green: #3fb950; --green-bg: #12261e;
  --red: #f85149; --red-bg: #2d1315;
  --blue: #58a6ff; --yellow: #d29922;
  --orange: #db8b0b; --orange-bg: #2d2009;
  --border: #30363d; --accent: #1f6feb;
  --diff-add: #1a4721; --diff-del: #5b2125;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--fg); line-height: 1.5; padding: 20px; }
.container { max-width: 1800px; margin: 0 auto; }

/* Scrollbar */
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-track { background: var(--bg); border-radius: 6px; }
::-webkit-scrollbar-thumb { background: var(--bg3); border-radius: 6px; border: 2px solid var(--bg); }
::-webkit-scrollbar-thumb:hover { background: var(--fg2); }
::-webkit-scrollbar-corner { background: var(--bg); }
* { scrollbar-width: thin; scrollbar-color: var(--bg3) var(--bg); }

/* Header */
h1 { margin-bottom: 4px; font-size: 24px; display: flex; align-items: center; gap: 8px; }
h1 .brand { background: linear-gradient(135deg, #1f6feb 0%, #58a6ff 50%, #79c0ff 100%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  background-clip: text; font-weight: 700; }
h1 .title-rest { background: linear-gradient(135deg, #1f6feb 0%, #58a6ff 50%, #79c0ff 100%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  background-clip: text; font-weight: 600; }
.btn-nav { color: var(--fg2); font-size: 13px; border: 1px solid var(--border);
  border-radius: 6px; padding: 5px 14px; background: var(--bg3); text-decoration: none;
  transition: all 0.15s; display: inline-block; }
.btn-nav:hover { border-color: var(--blue); color: var(--blue); background: var(--bg2); }
.meta { color: var(--fg2); font-size: 14px; margin-bottom: 24px; }
.meta span { margin-right: 16px; }

/* Summary table */
.summary-card { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; padding: 20px; margin-bottom: 24px; }
.summary-card h2 { font-size: 18px; margin-bottom: 12px; }
table.summary-table { width: 100%; border-collapse: collapse; font-size: 14px; }
.summary-table th { text-align: left; padding: 8px 12px; border-bottom: 2px solid var(--border);
  color: var(--fg2); font-weight: 600; }
.summary-table td { padding: 8px 12px; border-bottom: 1px solid var(--border); }
.summary-table tr:hover { background: var(--bg3); }
.bar-bg { display: inline-block; width: 120px; height: 8px; border-radius: 4px;
  background: var(--bg3); vertical-align: middle; position: relative; overflow: hidden; }
.bar-fill { height: 100%; border-radius: 4px; position: absolute; left: 0; top: 0; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: 600; }
.badge-pass { background: var(--green-bg); color: var(--green); }
.badge-fail { background: var(--red-bg); color: var(--red); }
.num-mismatch { color: var(--red); font-weight: 600; }
.num-match { color: var(--green); }

/* SQL list */
.sql-toggle { display: flex; align-items: center; gap: 10px;
  margin-bottom: 16px; cursor: pointer; user-select: none;
  padding: 10px 16px; background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; }
.sql-toggle:hover { background: var(--bg3); }
.sql-toggle .arrow { transition: transform 0.2s; color: var(--fg2); }
.sql-toggle.open .arrow { transform: rotate(90deg); }
.sql-toggle .label { font-size: 14px; font-weight: 600; color: var(--fg); }
.sql-toggle .count { font-size: 13px; color: var(--fg2); }
.sql-list { display: none; margin-bottom: 24px; }
.sql-list.open { display: block; }
.sql-list-card { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; overflow: hidden; }
.sql-item { display: flex; align-items: flex-start; padding: 4px 16px;
  border-bottom: 1px solid var(--border); font-size: 13px; }
.sql-item:last-child { border-bottom: none; }
.sql-item:hover { background: var(--bg3); }
.sql-idx { color: var(--fg2); min-width: 50px; padding-top: 2px;
  font-family: 'SF Mono', Consolas, monospace; font-size: 12px; }
.sql-text { font-family: 'SF Mono', Consolas, monospace; font-size: 13px;
  color: var(--blue); white-space: pre-wrap; word-break: break-all; line-height: 1.5; }
.sql-item.skipped { opacity: 0.5; }
.sql-item.skipped .sql-text { color: var(--fg2); text-decoration: line-through; }

/* Results summary bar (from Playground) */
.results-summary { display: flex; gap: 10px; align-items: center; padding: 10px 20px;
  background: var(--bg2); border: 1px solid var(--border); border-radius: 8px;
  margin-bottom: 16px; font-size: 13px; font-weight: 600; }
.summary-btn { display: inline-flex; align-items: center; gap: 4px;
  background: none; border: 1px solid transparent; border-radius: 6px;
  padding: 4px 10px; cursor: pointer; font-size: 13px; font-weight: 600;
  transition: all 0.15s; }
.summary-btn:hover { background: var(--bg3); border-color: var(--border); }
.summary-btn.active { background: var(--bg3); border-color: var(--accent);
  box-shadow: 0 0 0 1px var(--accent); }
.summary-match { color: var(--green); }
.summary-match.active { background: var(--green-bg); border-color: var(--green); box-shadow: none; }
.summary-diff { color: var(--red); }
.summary-diff.active { background: var(--red-bg); border-color: var(--red); box-shadow: none; }
.summary-diff-zero { color: var(--fg2); opacity: 0.6; cursor: default; background: none;
  border: none; padding: 4px 10px; }
.summary-skip { color: var(--yellow); }
.summary-skip.active { background: #2a2518; border-color: var(--yellow); box-shadow: none; }
.summary-total { color: var(--fg2); margin-left: auto; font-weight: 400; }
.summary-total.active { background: var(--bg3); border-color: var(--fg2); box-shadow: none; }

/* Per-statement block (from Playground) */
.stmt-block { margin-bottom: 28px; }
.stmt-sql { font-family: 'SF Mono', Consolas, monospace; font-size: 13px;
  color: var(--blue); background: var(--bg2); border: 1px solid var(--border);
  border-radius: 8px; padding: 10px 16px; margin-bottom: 12px;
  word-break: break-all; border-left: 3px solid var(--accent); }
.stmt-label { font-size: 12px; color: var(--fg2); margin-bottom: 4px; font-weight: 600; }
.line-tag { font-size: 13px; color: var(--fg2); background: var(--bg3); border-radius: 4px;
  padding: 2px 8px; margin-right: 6px; font-family: 'SF Mono', Consolas, monospace; font-weight: 600; }

/* Context bar */
.context-bar { padding: 6px 16px; font-size: 12px; color: var(--fg2);
  background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
  margin-bottom: 8px; font-family: 'SF Mono', Consolas, monospace; line-height: 1.6; }
.context-bar .ctx-item { display: block; padding: 1px 0; }
.context-bar .ctx-block { color: var(--yellow); margin-right: 4px; }
.context-bar .ctx-sql { color: var(--fg2); }
.context-bar .ctx-current { color: var(--red); font-weight: 600; }

/* Grid of DBMS result panels (from Playground) */
.results-grid { display: flex; gap: 14px; overflow-x: auto; }
.results-grid > .result-panel { flex: 1 1 0; min-width: 280px; }
.result-panel { background: var(--bg2); border: 1px solid var(--border);
  border-radius: 10px; overflow: hidden; transition: border-color 0.2s; }
.result-panel:hover { border-color: var(--fg2); }
.result-panel.has-diff { border-color: var(--red); box-shadow: 0 0 0 1px var(--red-bg); }
.result-panel.not-present { opacity: 0.5; }
.result-panel-header { padding: 10px 16px; border-bottom: 1px solid var(--border);
  display: flex; align-items: center; justify-content: space-between;
  background: var(--bg3); }
.result-panel-name { font-weight: 600; font-size: 14px;
  display: flex; align-items: center; gap: 8px; }
.result-panel-meta { font-size: 12px; color: var(--fg2); }
.result-panel-body { padding: 0; overflow-x: auto; }

/* Diff badges */
.diff-badge { display: inline-block; padding: 2px 10px; border-radius: 12px;
  font-size: 11px; font-weight: 600; }
.diff-badge-match { background: var(--green-bg); color: var(--green); }
.diff-badge-diff { background: var(--red-bg); color: var(--red); }
.diff-badge-skip { background: #2a2518; color: var(--yellow); }
.diff-badge-baseline { background: #12261e; color: var(--green); }
.diff-badge-absent { background: var(--bg3); color: var(--fg2); }

/* Data table (from Playground) */
.data-table { width: 100%; border-collapse: collapse; font-size: 13px;
  font-family: 'SF Mono', Consolas, monospace; }
.data-table th { background: var(--bg3); padding: 8px 14px; text-align: left;
  font-weight: 600; color: var(--fg2); border-bottom: 1px solid var(--border);
  white-space: nowrap; }
.data-table td { padding: 6px 14px; border-bottom: 1px solid var(--border);
  white-space: nowrap; }
.data-table tr:last-child td { border-bottom: none; }
.data-table tr:hover td { background: rgba(255,255,255,0.02); }
.cell-diff { background: var(--diff-del); border-radius: 3px; padding: 1px 5px; }

/* Error / info states */
.result-error { color: var(--red); padding: 14px 16px; font-size: 13px;
  font-family: 'SF Mono', Consolas, monospace; white-space: pre-wrap; word-break: break-all; }
.result-error-match { color: var(--orange); padding: 14px 16px; font-size: 13px;
  font-family: 'SF Mono', Consolas, monospace; white-space: pre-wrap; word-break: break-all; }
.result-ok { color: var(--green); padding: 14px 16px; font-size: 13px; }
.result-empty { color: var(--fg2); padding: 14px 16px; font-size: 13px; font-style: italic; }
.result-output { padding: 0; font-size: 13px; font-family: 'SF Mono', Consolas, monospace;
  color: var(--fg); line-height: 1.5; }
.result-output .output-line { padding: 4px 14px; border-bottom: 1px solid var(--border);
  white-space: pre-wrap; word-break: break-all; }
.result-output .output-line:last-child { border-bottom: none; }

/* diff lines inside panel */
.diff-line { font-family: 'SF Mono', Consolas, monospace; font-size: 13px;
  padding: 4px 14px; white-space: pre-wrap; word-break: break-all;
  min-height: 22px; line-height: 22px; border-bottom: 1px solid var(--border); }
.diff-line:last-child { border-bottom: none; }
.diff-line.added { color: var(--green); }
.diff-line.removed { color: var(--red); }
.diff-line.context { }

/* Empty / no diff */
.no-diff { padding: 40px; text-align: center; color: var(--fg2); font-size: 16px; }

/* Toast */
.toast { position: fixed; bottom: 24px; right: 24px; padding: 12px 20px;
  border-radius: 8px; font-size: 14px; color: #fff; z-index: 9999;
  transition: opacity 0.3s; pointer-events: none; }
.toast-success { background: var(--green); }
.toast-error { background: var(--red); }
</style>
</head>
<body>
<div class="container">
  <div style="display:flex;align-items:center;gap:16px;margin-bottom:4px">
    <h1><svg width="28" height="28" viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M6 3C6 1.9 6.9 1 8 1H24C25.1 1 26 1.9 26 3V28C26 29.1 25.1 30 24 30H8C6.9 30 6 29.1 6 28V3Z" stroke="#58a6ff" stroke-width="2" fill="none"/><line x1="16" y1="5" x2="16" y2="27" stroke="#30363d" stroke-width="1" stroke-dasharray="2 2"/><line x1="9" y1="8" x2="14" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="8" x2="23" y2="8" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="12" x2="14" y2="12" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="12" x2="23" y2="12" stroke="#f85149" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="16" x2="14" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="16" x2="23" y2="16" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="20" x2="14" y2="20" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="20" x2="23" y2="23" stroke="#d29922" stroke-width="2" stroke-linecap="round"/><line x1="9" y1="24" x2="14" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/><line x1="18" y1="24" x2="23" y2="24" stroke="#3fb950" stroke-width="2" stroke-linecap="round"/></svg> <span class="brand">Rosetta</span> <span class="title-rest">Report</span></h1>
    <a href="../index.html" class="btn-nav">&#9664; History</a>
    <a href="../playground.html" class="btn-nav">&#9654; Playground</a>
  </div>
  <div class="meta">
    <span>Test: <strong>{{TEST_NAME}}</strong></span>
    <span>Time: {{TIME}}</span>
    <span>Baseline: <strong>{{BASELINE}}</strong></span>
  </div>

  <div class="sql-toggle" id="sql-list-toggle" style="display:none">
    <span class="arrow">&#9654;</span>
    <span class="label">Executed SQL</span>
    <span class="count" id="sql-list-count"></span>
  </div>
  <div class="sql-list" id="sql-list-section">
    <div class="sql-list-card" id="sql-list-body"></div>
  </div>

  <div class="summary-card">
    <h2>Summary</h2>
    <table class="summary-table">
      <thead>
        <tr>
          <th>Comparison</th><th>Status</th><th>Match</th>
          <th>Mismatch</th><th>Skip</th><th>Total</th><th>Pass Rate</th>
        </tr>
      </thead>
      <tbody id="summary-body"></tbody>
    </table>
  </div>

  <div id="results-area"></div>
</div>

<div id="toast" class="toast" style="opacity:0"></div>

<script>
const SUMMARY = {{SUMMARY_JSON}};
const STMTS = {{STMTS_JSON}};
const SQL_LIST = {{SQL_LIST_JSON}};
const DBMS_NAMES = {{DBMS_NAMES_JSON}};
const BASELINE = {{BASELINE_JSON}};

// --- Utility ---
function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#39;').replace(/`/g,'&#96;')
    .replace(/\$\{/g,'&#36;{');
}

function showToast(msg, type) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast toast-' + (type || 'success');
  t.style.opacity = '1';
  setTimeout(() => { t.style.opacity = '0'; }, 2500);
}

// --- SQL List ---
(function() {
  if (!SQL_LIST || SQL_LIST.length === 0) return;
  const toggle = document.getElementById('sql-list-toggle');
  const section = document.getElementById('sql-list-section');
  const body = document.getElementById('sql-list-body');
  const countEl = document.getElementById('sql-list-count');
  toggle.style.display = 'flex';
  countEl.textContent = '(' + SQL_LIST.length + ' statements)';
  const frag = document.createDocumentFragment();
  SQL_LIST.forEach(s => {
    const item = document.createElement('div');
    item.className = 'sql-item' + (s.skipped ? ' skipped' : '');
    const lineLabel = '#' + s.idx;
    item.innerHTML = '<span class="sql-idx">' + lineLabel + '</span><span class="sql-text">' + esc(s.sql) + '</span>';
    frag.appendChild(item);
  });
  body.appendChild(frag);
  toggle.onclick = () => {
    toggle.classList.toggle('open');
    section.classList.toggle('open');
  };
})();

// --- Summary table ---
const tbody = document.getElementById('summary-body');
SUMMARY.forEach(r => {
  const effectiveMismatch = r.effective_mismatched || 0;
  const status = effectiveMismatch <= 0;
  const pct = r.pass_rate;
  const row = document.createElement('tr');
  row.innerHTML =
    '<td>' + esc(r.key) + '</td>' +
    '<td><span class="badge ' + (status ? 'badge-pass' : 'badge-fail') + '">' + (status ? 'PASS' : 'FAIL') + '</span></td>' +
    '<td class="num-match">' + r.matched + '</td>' +
    '<td class="' + (effectiveMismatch > 0 ? 'num-mismatch' : '') + '">' + (effectiveMismatch > 0 ? effectiveMismatch : 0) + '</td>' +
    '<td>' + r.skipped + '</td>' +
    '<td>' + r.total + '</td>' +
    '<td><span class="bar-bg"><span class="bar-fill" style="width:' + pct + '%;background:' + (pct>=100?'var(--green)':pct>=90?'var(--yellow)':'var(--red)') + '"></span></span> ' + pct + '%</td>';
  tbody.appendChild(row);
});

// --- Diff logic (from Playground) ---
// Normalize a result output line for comparison (strip [Lnnn] tags, ENGINE=, etc.)
const RE_ERROR_LINE = /^ERROR\b[^(]*\((\d+),/;
const RE_ENGINE = /ENGINE\s*=\s*\w+/g;
const RE_CHARSET = /DEFAULT CHARSET=\w+(\s+COLLATE=\w+)?/g;
const RE_AUTO_INC = /\s*AUTO_INCREMENT=\d+/g;
const RE_ROW_FORMAT = /\s*ROW_FORMAT=\w+/g;
const RE_STATS_PERSISTENT = /\s*STATS_PERSISTENT=\d+/g;
const RE_TDSQL_TAIL = /\.\s*txid:\s*\S+\.\s*sql-node:\s*\S+\.\s*error-store-node:\s*\S+\s*$/;
const RE_DEFINER = /DEFINER=`[^`]*`@`[^`]*`/g;
const RE_WARNING = /^Warning\s+\d+\s+/;

function normalizeLine(line) {
  let s = line;
  const em = RE_ERROR_LINE.exec(s);
  if (em) return 'ERROR: (' + em[1] + ')';
  if (s.startsWith('ERROR')) return 'ERROR: (unknown)';
  s = s.replace(RE_TDSQL_TAIL, '');
  s = s.replace(RE_ENGINE, 'ENGINE=<N>');
  s = s.replace(RE_CHARSET, 'DEFAULT CHARSET=<N>');
  s = s.replace(RE_AUTO_INC, '');
  s = s.replace(RE_ROW_FORMAT, '');
  s = s.replace(RE_STATS_PERSISTENT, '');
  s = s.replace(RE_DEFINER, 'DEFINER=<N>');
  return s;
}

function normalizeLines(lines) {
  return lines.map(normalizeLine)
    .filter(l => l.trim() !== 'Warnings:' && !RE_WARNING.test(l.trim()));
}

function filterWarnings(lines) {
  return lines.filter(l => l.trim() !== 'Warnings:' && !RE_WARNING.test(l.trim()));
}

// SQL types that skip diff validation (used for visual hint only, not for status)
// Status is entirely driven by Python comparator results.

function linesHaveDiff(linesA, linesB) {
  const na = normalizeLines(linesA);
  const nb = normalizeLines(linesB);
  if (na.length !== nb.length) return true;
  for (let i = 0; i < na.length; i++) {
    if (na[i] !== nb[i]) return true;
  }
  return false;
}

// --- Build and render the per-statement grid ---
function renderStatements() {
  const area = document.getElementById('results-area');
  area.innerHTML = '';

  if (!STMTS || STMTS.length === 0 || DBMS_NAMES.length === 0) {
    area.innerHTML = '<div class="no-diff">No statement data available.</div>';
    return;
  }

  // Determine baseline
  const refName = BASELINE && DBMS_NAMES.includes(BASELINE) ? BASELINE : DBMS_NAMES[0];
  const nonRefNames = DBMS_NAMES.filter(n => n !== refName);

  // Count match/diff/skip for summary bar using comparator-provided statuses
  let matchCount = 0, diffCount = 0, skipCount = 0;
  const stmtStatuses = STMTS.map(stmt => stmt.status || 'match');

  stmtStatuses.forEach(status => {
    if (status === 'diff') diffCount++;
    else if (status === 'skip') skipCount++;
    else matchCount++;
  });

  // Render summary filter bar
  const totalStmts = STMTS.length;
  if (totalStmts > 0 && DBMS_NAMES.length > 1) {
    const summaryBar = document.createElement('div');
    summaryBar.className = 'results-summary';
    summaryBar.id = 'stmt-summary-bar';
    summaryBar.innerHTML =
      '<button class="summary-btn summary-match" data-filter="match" title="Show matched">&#10003; Match: ' + matchCount + '</button>' +
      (diffCount > 0
        ? '<button class="summary-btn summary-diff" data-filter="diff" title="Show diffs">&#10007; Diff: ' + diffCount + '</button>'
        : '<span class="summary-btn summary-diff-zero">Diff: 0</span>') +
      (skipCount > 0
        ? '<button class="summary-btn summary-skip" data-filter="skip" title="Show skipped">&#9888; Skip: ' + skipCount + '</button>'
        : '') +
      '<button class="summary-btn summary-total" data-filter="" title="Show all">Total: ' + totalStmts + '</button>';
    area.appendChild(summaryBar);

    summaryBar.querySelectorAll('.summary-btn[data-filter]').forEach(btn => {
      btn.addEventListener('click', () => {
        const filter = btn.getAttribute('data-filter');
        summaryBar.querySelectorAll('.summary-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        area.querySelectorAll('.stmt-block').forEach((block, i) => {
          if (!filter) {
            block.style.display = '';
          } else {
            block.style.display = stmtStatuses[i] === filter ? '' : 'none';
          }
        });
      });
    });
  }

  // Render each statement block
  STMTS.forEach((stmt, si) => {
    const block = document.createElement('div');
    block.className = 'stmt-block';
    block.setAttribute('data-status', stmtStatuses[si] || 'match');

    // SQL header with block number and line tag
    const sqlDiv = document.createElement('div');
    sqlDiv.className = 'stmt-sql';
    let prefix = '<span class="stmt-label">Block ' + stmt.block_idx + '/' + totalStmts + '</span> ';
    if (stmt.line_tag !== null && stmt.line_tag !== undefined) {
      prefix += '<span class="line-tag">#' + stmt.line_tag + '</span>';
    }
    sqlDiv.innerHTML = prefix + esc(stmt.sql);
    block.appendChild(sqlDiv);

    // Context bar: show for all diff blocks
    if (stmtStatuses[si] === 'diff') {
      const ctxItems = [];
      const fmtCtx = (s, isCurrent) => {
        const tag = (s.line_tag !== null && s.line_tag !== undefined) ? ' #' + s.line_tag : '';
        const blk = '<span class="ctx-block">Block ' + s.block_idx + tag + '</span>';
        const sql = isCurrent
          ? '<span class="ctx-current">&#9654; ' + esc(s.sql.substring(0, 120)) + '</span>'
          : '<span class="ctx-sql">' + esc(s.sql.substring(0, 120)) + '</span>';
        return '<span class="ctx-item">' + blk + sql + '</span>';
      };
      for (let ci = Math.max(0, si - 2); ci < si; ci++) {
        ctxItems.push(fmtCtx(STMTS[ci], false));
      }
      ctxItems.push(fmtCtx(stmt, true));
      for (let ci = si + 1; ci < Math.min(si + 3, STMTS.length); ci++) {
        ctxItems.push(fmtCtx(STMTS[ci], false));
      }
      const ctxBar = document.createElement('div');
      ctxBar.className = 'context-bar';
      ctxBar.innerHTML = ctxItems.join('');
      block.appendChild(ctxBar);
    }

    // Multi-DBMS results grid
    const grid = document.createElement('div');
    grid.className = 'results-grid';

    const stmtStatus = stmtStatuses[si];
    const refResult = stmt.dbms_results[refName];
    const refLines = (refResult && refResult.present) ? filterWarnings(refResult.lines) : [];

    DBMS_NAMES.forEach(name => {
      const sr = stmt.dbms_results[name];
      const panel = document.createElement('div');
      panel.className = 'result-panel';

      const present = sr && sr.present;
      if (!present) panel.classList.add('not-present');

      // Determine per-panel diff state: use comparator status as authority,
      // then check actual line diff for visual highlighting
      let panelDiff = false;
      if (stmtStatus === 'diff' && present && name !== refName && refResult && refResult.present) {
        panelDiff = linesHaveDiff(refLines, filterWarnings(sr.lines));
        if (panelDiff) panel.classList.add('has-diff');
      }

      // Header badge: purely driven by comparator status
      const header = document.createElement('div');
      header.className = 'result-panel-header';
      let badge = '';
      if (!present) {
        badge = ' <span class="diff-badge diff-badge-absent">ABSENT</span>';
      } else if (name === refName) {
        if (stmtStatus === 'skip') {
          badge = ' <span class="diff-badge diff-badge-skip">SKIP</span>';
        } else {
          badge = ' <span class="diff-badge diff-badge-baseline">BASELINE</span>';
        }
      } else if (stmtStatus === 'skip') {
        badge = ' <span class="diff-badge diff-badge-skip">SKIP</span>';
      } else if (stmtStatus === 'diff') {
        // Per-panel: only mark DIFF if this DBMS actually differs from baseline
        badge = panelDiff
          ? ' <span class="diff-badge diff-badge-diff">DIFF</span>'
          : ' <span class="diff-badge diff-badge-match">MATCH</span>';
      } else {
        badge = ' <span class="diff-badge diff-badge-match">MATCH</span>';
      }
      const nLines = present ? filterWarnings(sr.lines).length : 0;
      header.innerHTML = '<span class="result-panel-name">' + esc(name) + badge + '</span>' +
        '<span class="result-panel-meta">' + nLines + ' line(s)</span>';
      panel.appendChild(header);

      // Body
      const body = document.createElement('div');
      body.className = 'result-panel-body';

      if (!present) {
        body.innerHTML = '<div class="result-empty">Statement not present on this DBMS</div>';
      } else {
        const lines = filterWarnings(sr.lines);
        if (lines.length === 0) {
          body.innerHTML = '<div class="result-ok">OK — no output</div>';
        } else {
          // Check if output contains ERROR lines
          const hasError = lines.some(l => /^ERROR\b/.test(l.trim()));

          // Check if output is an "OK" result (affected rows, no actual data)
          const isOkResult = lines.length <= 2 && lines.every(l =>
            /^affected rows/i.test(l.trim()) || /^insert.id/i.test(l.trim()) ||
            /^OK\b/i.test(l.trim()) || l.trim() === '');

          if (hasError) {
            const errClass = (stmtStatus === 'diff') ? 'result-error' : 'result-error-match';
            const pre = document.createElement('div');
            pre.className = errClass;
            pre.textContent = lines.join('\n');
            body.appendChild(pre);
          } else if (isOkResult) {
            body.innerHTML = '<div class="result-ok">' + esc(lines.join('\n')) + '</div>';
          } else {
            // Render as table (playground style) with optional diff highlighting
            const refLinesForDiff = (stmtStatus === 'diff' && name !== refName && refResult && refResult.present) ? refLines : null;
            body.appendChild(buildTableFromLines(lines, refLinesForDiff));
          }
        }
      }

      panel.appendChild(body);
      grid.appendChild(panel);
    });

    block.appendChild(grid);
    area.appendChild(block);
  });
}

// Build a data-table from tab-separated output lines (playground style).
// If refLines is provided, cells that differ from baseline are highlighted.
function buildTableFromLines(lines, refLines) {
  // Determine if output is tab-separated tabular data (like SELECT results).
  // SELECT output from MTR: first line is tab-separated column names like
  // "id\tname\tprice", where each field is a simple identifier.
  // DDL/INSERT output may contain incidental tabs but first line will have
  // SQL syntax characters like (, ), =, ;, etc.
  const RE_COL_NAME = /^[a-zA-Z_@#][\w@#$.()]*$/;
  let isTabular = false;
  if (lines.length >= 1) {
    const cols0 = lines[0].split('\t');
    if (cols0.length >= 2) {
      // Every field in the first line must look like a column name
      const allColNames = cols0.every(c => RE_COL_NAME.test(c.trim()));
      isTabular = allColNames;
    }
  }

  if (!isTabular) {
    // Non-tabular output (e.g. EXPLAIN tree, SHOW output) — render as line list
    const container = document.createElement('div');
    container.className = 'result-output';
    const normRef = refLines ? normalizeLines(refLines) : null;
    const normTgt = normalizeLines(lines);
    lines.forEach((line, i) => {
      const div = document.createElement('div');
      div.className = 'output-line';
      if (normRef && normRef[i] !== normTgt[i]) {
        div.innerHTML = '<span class="cell-diff">' + esc(line) + '</span>';
      } else {
        div.textContent = line;
      }
      container.appendChild(div);
    });
    return container;
  }

  // Tab-separated tabular data — render as <table>
  const rows = lines.map(l => l.split('\t'));
  const refRows = refLines ? refLines.map(l => l.split('\t')) : null;

  const table = document.createElement('table');
  table.className = 'data-table';

  // First row is the header (column names)
  const thead = document.createElement('thead');
  const hRow = document.createElement('tr');
  const refHdr = refRows ? refRows[0] : null;
  rows[0].forEach((col, ci) => {
    const th = document.createElement('th');
    if (refHdr && ci < refHdr.length && col !== refHdr[ci]) {
      th.innerHTML = '<span class="cell-diff">' + esc(col) + '</span>';
    } else {
      th.textContent = col;
    }
    hRow.appendChild(th);
  });
  thead.appendChild(hRow);
  table.appendChild(thead);

  // Data rows
  const tbody = document.createElement('tbody');
  if (rows.length <= 1) {
    // Only header, no data rows — show empty set hint
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = rows[0].length;
    td.style.cssText = 'text-align:center;color:var(--fg2);font-style:italic;padding:12px';
    td.textContent = 'Empty set';
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    for (let ri = 1; ri < rows.length; ri++) {
      const tr = document.createElement('tr');
      const refRow = refRows && ri < refRows.length ? refRows[ri] : null;
      rows[ri].forEach((cell, ci) => {
        const td = document.createElement('td');
        const refCell = refRow ? (refRow[ci] != null ? refRow[ci] : '') : cell;
        if (refRows && String(cell) !== String(refCell)) {
          td.innerHTML = '<span class="cell-diff">' + esc(cell) + '</span>';
        } else {
          td.textContent = cell;
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    }
  }
  table.appendChild(tbody);
  return table;
}

// --- Init ---
renderStatements();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def write_html_report(path: str, test_file: str,
                      comparisons: Dict[str, CompareResult],
                      baseline: str = "",
                      sql_list: Optional[List[Statement]] = None,
                      raw_results: Optional[Dict[str, List[str]]] = None):
    """Generate a self-contained HTML report file.

    Args:
        path: Output file path.
        test_file: Test file name.
        comparisons: Pairwise comparison results (for Summary table).
        baseline: Baseline DBMS name.
        sql_list: Parsed statement list (for SQL list panel).
        raw_results: Dict[dbms_name -> output_lines] for multi-DBMS view.
            If not provided, falls back to extracting data from comparisons.
    """
    summary = _build_summary_data(comparisons)
    sql_data = _build_sql_list_data(sql_list)
    test_name = test_file.rsplit("/", 1)[-1] if "/" in test_file else test_file

    # Build per-statement multi-DBMS data
    if raw_results:
        # Preferred: use raw results for full multi-DBMS view
        stmt_data = _build_stmt_data(raw_results, baseline, comparisons)
        dbms_names = list(raw_results.keys())
    else:
        # Fallback: reconstruct from comparisons (limited to pairwise data)
        stmt_data, dbms_names = _build_stmt_data_from_comparisons(
            comparisons, baseline)

    # Ensure baseline first in ordering
    if baseline and baseline in dbms_names:
        dbms_names = [baseline] + [n for n in dbms_names if n != baseline]

    page = _HTML_TEMPLATE
    page = page.replace("{{TEST_NAME}}", _escape(test_name))
    page = page.replace("{{TIME}}",
                         _escape(time.strftime("%Y-%m-%d %H:%M:%S")))
    page = page.replace("{{BASELINE}}", _escape(baseline or "N/A"))
    page = page.replace("{{SUMMARY_JSON}}", _safe_json(summary))
    page = page.replace("{{STMTS_JSON}}", _safe_json(stmt_data))
    page = page.replace("{{SQL_LIST_JSON}}", _safe_json(sql_data))
    page = page.replace("{{DBMS_NAMES_JSON}}", _safe_json(dbms_names))
    page = page.replace("{{BASELINE_JSON}}", _safe_json(baseline or ""))

    with open(path, "w", encoding="utf-8") as f:
        f.write(page)

    log.info("HTML report written: %s", path)


def _build_stmt_data_from_comparisons(
        comparisons: Dict[str, CompareResult],
        baseline: str) -> tuple:
    """Fallback: reconstruct per-statement data from pairwise comparisons.

    Returns (stmt_list, dbms_names).
    """
    if not comparisons:
        return [], []

    # Collect all DBMS names from comparisons
    dbms_set = set()
    for cmp in comparisons.values():
        dbms_set.add(cmp.dbms_a)
        dbms_set.add(cmp.dbms_b)
    dbms_names = sorted(dbms_set)

    # Use the first comparison to get block structure
    first_cmp = next(iter(comparisons.values()))
    total_blocks = first_cmp.total_stmts

    # Build per-block data from diffs
    # This is approximate — we can only show diff blocks, not matched ones
    stmts = []
    diff_blocks = {}
    for cmp in comparisons.values():
        for d in cmp.diffs:
            block_idx = d["block"]
            if block_idx not in diff_blocks:
                diff_blocks[block_idx] = {
                    "block_idx": block_idx,
                    "line_tag": None,
                    "sql": d["stmt"][:200],
                    "dbms_results": {},
                    "context_before": d.get("context_before", []),
                    "context_after": d.get("context_after", []),
                    "skipped": d.get("skipped", False),
                }
            entry = diff_blocks[block_idx]
            entry["dbms_results"][cmp.dbms_a] = {
                "lines": d.get("lines_a", []),
                "present": True,
                "first_line": "",
            }
            entry["dbms_results"][cmp.dbms_b] = {
                "lines": d.get("lines_b", []),
                "present": True,
                "first_line": "",
            }

    for idx in sorted(diff_blocks.keys()):
        stmts.append(diff_blocks[idx])

    return stmts, dbms_names
