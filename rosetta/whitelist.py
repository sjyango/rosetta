"""Whitelist management for Rosetta.

A *whitelist entry* records the fingerprint of a known-acceptable diff so that
subsequent test runs can skip it automatically.  The whitelist is persisted as
a single JSON file (``whitelist.json``) in the output directory.

Fingerprint strategy
--------------------
Each diff block already carries:
  - ``stmt``  – the leading SQL statement (first line of the block)
  - ``lines_a`` / ``lines_b`` – full output from the two DBMS

We compute an MD5 hash over the normalised SQL statement plus the sorted
(lines_a, lines_b) content.  This means: if exactly the same diff re-appears
for the same statement, it will be matched.  Trivial whitespace or ordering
changes still produce a hit; genuinely different output will *not* match.
"""

import hashlib
import json
import logging
import os
import time as _time
from typing import Dict, List, Optional

log = logging.getLogger("rosetta")

_WHITELIST_FILE = "whitelist.json"


# ---------------------------------------------------------------------------
# Fingerprint helpers
# ---------------------------------------------------------------------------

def _normalise_for_hash(lines: List[str]) -> str:
    """Collapse whitespace and sort lines for a stable fingerprint."""
    return "\n".join(sorted(l.strip() for l in lines if l.strip()))


def diff_fingerprint(stmt: str, lines_a: List[str],
                     lines_b: List[str]) -> str:
    """Return an MD5 hex digest that identifies *this specific diff*."""
    parts = [
        stmt.strip(),
        _normalise_for_hash(lines_a),
        _normalise_for_hash(lines_b),
    ]
    blob = "\x00".join(parts).encode("utf-8", errors="replace")
    return hashlib.md5(blob).hexdigest()


# ---------------------------------------------------------------------------
# Whitelist store
# ---------------------------------------------------------------------------

class Whitelist:
    """In-memory whitelist backed by a JSON file.

    Structure of ``whitelist.json``::

        {
            "<fingerprint>": {
                "stmt": "SELECT ...",
                "dbms_a": "tdsql",
                "dbms_b": "mysql",
                "block": 42,
                "reason": "TDSQL-specific feature",
                "added_at": "2026-03-10 17:00:00"
            },
            ...
        }
    """

    def __init__(self, output_dir: str):
        self._path = os.path.join(output_dir, _WHITELIST_FILE)
        self._data: Dict[str, dict] = {}
        self.load()

    # -- persistence --------------------------------------------------------

    def load(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                log.warning("Failed to load whitelist: %s", e)
                self._data = {}
        else:
            self._data = {}

    def save(self):
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

    # -- query / mutate -----------------------------------------------------

    @property
    def entries(self) -> Dict[str, dict]:
        return dict(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def contains(self, fingerprint: str) -> bool:
        return fingerprint in self._data

    def add(self, fingerprint: str, stmt: str, dbms_a: str, dbms_b: str,
            block: int = 0, reason: str = "") -> dict:
        """Add an entry and persist.  Returns the stored dict."""
        entry = {
            "stmt": stmt[:300],
            "dbms_a": dbms_a,
            "dbms_b": dbms_b,
            "block": block,
            "reason": reason,
            "added_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self._data[fingerprint] = entry
        self.save()
        return entry

    def remove(self, fingerprint: str) -> bool:
        """Remove an entry.  Returns True if it existed."""
        if fingerprint in self._data:
            del self._data[fingerprint]
            self.save()
            return True
        return False

    def clear(self):
        """Remove all entries."""
        self._data.clear()
        self.save()

    # -- diff-level helpers -------------------------------------------------

    def check_diff(self, diff_dict: dict) -> Optional[str]:
        """Check whether a diff dict is whitelisted.

        Returns the fingerprint if matched, else ``None``.
        """
        fp = diff_fingerprint(
            diff_dict.get("stmt", ""),
            diff_dict.get("lines_a", []),
            diff_dict.get("lines_b", []),
        )
        return fp if self.contains(fp) else None

    def annotate_diffs(self, diffs: List[dict]) -> List[dict]:
        """Add ``whitelisted`` boolean and ``fingerprint`` to each diff."""
        for d in diffs:
            fp = diff_fingerprint(
                d.get("stmt", ""),
                d.get("lines_a", []),
                d.get("lines_b", []),
            )
            d["fingerprint"] = fp
            d["whitelisted"] = self.contains(fp)
        return diffs
