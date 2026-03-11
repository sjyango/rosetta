"""Bug-mark management for Rosetta.

A *bug entry* records the fingerprint of a diff that has been identified as a
genuine bug.  Unlike whitelisted diffs, bug-marked diffs **still count toward
the failure rate** — the mark is purely informational so that users can track
known bugs across test runs.

The bug list is persisted as a single JSON file (``buglist.json``) in the
output directory.  The fingerprint algorithm is identical to the whitelist
(MD5 over normalised SQL + output), so the same ``diff_fingerprint`` helper
is reused.
"""

import json
import logging
import os
import time as _time
from typing import Dict, Optional

log = logging.getLogger("rosetta")

_BUGLIST_FILE = "buglist.json"


# ---------------------------------------------------------------------------
# Buglist store
# ---------------------------------------------------------------------------

class Buglist:
    """In-memory bug list backed by a JSON file.

    Structure of ``buglist.json``::

        {
            "<fingerprint>": {
                "stmt": "SELECT ...",
                "dbms_a": "tdsql",
                "dbms_b": "mysql",
                "block": 42,
                "reason": "Known bug #123",
                "added_at": "2026-03-10 18:00:00"
            },
            ...
        }
    """

    def __init__(self, output_dir: str):
        self._path = os.path.join(output_dir, _BUGLIST_FILE)
        self._data: Dict[str, dict] = {}
        self.load()

    # -- persistence --------------------------------------------------------

    def load(self):
        if os.path.isfile(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                log.warning("Failed to load buglist: %s", e)
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
