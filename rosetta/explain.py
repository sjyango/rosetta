"""EXPLAIN statement detection and multi-format variant generation.

Detects EXPLAIN / EXPLAIN ANALYZE statements and generates format-specific
SQL variants for each DBMS type (MySQL/TDSQL, TiDB, OceanBase).
"""

import re
from typing import Dict, List

# ---------------------------------------------------------------------------
# EXPLAIN statement detection
# ---------------------------------------------------------------------------

_RE_EXPLAIN_PREFIX = re.compile(r'^\s*EXPLAIN\s', re.IGNORECASE)

# Regex to decompose:  EXPLAIN [ANALYZE] [FORMAT=xxx] <inner_query>
# Groups: (1) ANALYZE keyword, (2) format value, (3) inner query
_RE_EXPLAIN_PARTS = re.compile(
    r'^\s*EXPLAIN\s+'
    r'(?:(ANALYZE)\s+)?'
    r'(?:FORMAT\s*=\s*["\']?(\w+)["\']?\s+)?'
    r'(.+)',
    re.IGNORECASE | re.DOTALL,
)

# OceanBase style: EXPLAIN EXTENDED / EXPLAIN BASIC / EXPLAIN OUTLINE etc.
_RE_EXPLAIN_OB_KEYWORD = re.compile(
    r'^\s*EXPLAIN\s+'
    r'(?:(ANALYZE)\s+)?'
    r'(?:(EXTENDED|EXTENDED_NOADDR|BASIC|OUTLINE|PARTITIONS|PRETTY|PRETTY_COLOR)\s+)?'
    r'(.+)',
    re.IGNORECASE | re.DOTALL,
)


def is_explain_stmt(sql: str) -> bool:
    """Return True if *sql* is an EXPLAIN or EXPLAIN ANALYZE statement."""
    return bool(_RE_EXPLAIN_PREFIX.match(sql))


def _extract_inner_query(sql: str) -> tuple:
    """Extract (is_analyze, inner_query) from an EXPLAIN statement.

    Strips the EXPLAIN prefix, ANALYZE keyword, FORMAT clause, and
    OceanBase-style keywords, returning just the inner SQL query.
    """
    # Try OceanBase-style keywords first (EXTENDED, BASIC, etc.)
    m = _RE_EXPLAIN_OB_KEYWORD.match(sql)
    if m:
        is_analyze = bool(m.group(1))
        inner_query = m.group(3).strip()
        # If there's a FORMAT= in the inner query, strip it too
        m2 = re.match(
            r'FORMAT\s*=\s*["\']?\w+["\']?\s+(.+)',
            inner_query,
            re.IGNORECASE | re.DOTALL,
        )
        if m2:
            inner_query = m2.group(1).strip()
        return is_analyze, inner_query

    # Standard style: EXPLAIN [ANALYZE] [FORMAT=xxx] <query>
    m = _RE_EXPLAIN_PARTS.match(sql)
    if m:
        is_analyze = bool(m.group(1))
        inner_query = m.group(3).strip()
        return is_analyze, inner_query

    # Fallback: strip "EXPLAIN" prefix
    stripped = re.sub(r'^\s*EXPLAIN\s+', '', sql, count=1, flags=re.IGNORECASE)
    return False, stripped.strip()


def get_explain_variants(sql: str, protocol: str) -> List[Dict[str, str]]:
    """Generate EXPLAIN format variants for a given DBMS protocol.

    Args:
        sql: The original EXPLAIN SQL statement.
        protocol: One of "mysql", "tdsql", "tidb", "oceanbase", "oracle".

    Returns:
        List of dicts: [{"format": "Traditional", "sql": "EXPLAIN ..."}]
        Each dict has a human-readable format name and the SQL to execute.
    """
    is_analyze, inner_query = _extract_inner_query(sql)

    if protocol == "tidb":
        variants = [
            {"format": "Default", "sql": f"EXPLAIN {inner_query}"},
            {"format": "Verbose", "sql": f'EXPLAIN FORMAT="verbose" {inner_query}'},
            {"format": "TiDB JSON", "sql": f'EXPLAIN FORMAT="tidb_json" {inner_query}'},
        ]
        if is_analyze:
            variants.insert(0, {
                "format": "Analyze",
                "sql": f"EXPLAIN ANALYZE {inner_query}",
            })

    elif protocol == "oceanbase":
        variants = [
            {"format": "Standard", "sql": f"EXPLAIN {inner_query}"},
            {"format": "Extended", "sql": f"EXPLAIN EXTENDED {inner_query}"},
            {"format": "JSON", "sql": f"EXPLAIN FORMAT=JSON {inner_query}"},
        ]
        if is_analyze:
            variants.insert(0, {
                "format": "Analyze",
                "sql": f"EXPLAIN ANALYZE {inner_query}",
            })

    else:
        # MySQL / TDSQL (default)
        variants = [
            {"format": "Traditional", "sql": f"EXPLAIN {inner_query}"},
            {"format": "JSON", "sql": f"EXPLAIN FORMAT=JSON {inner_query}"},
            {"format": "Tree", "sql": f"EXPLAIN FORMAT=TREE {inner_query}"},
        ]
        if is_analyze:
            variants.insert(0, {
                "format": "Analyze",
                "sql": f"EXPLAIN ANALYZE {inner_query}",
            })

    return variants
