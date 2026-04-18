"""Centralized path management for Rosetta.

All Rosetta data lives under ``~/.rosetta/`` (ROSETTA_HOME).

Directory layout::

    ~/.rosetta/
    ├── config.json          # Main configuration (databases + mtr settings)
    ├── results/             # rosetta test / bench output
    │   └── latest -> ...
    ├── mtr_logs/            # rosetta mtr --mode parallel logs
    │   └── 20260418_155656/
    └── bench/               # reserved for future use

Environment variable ``ROSETTA_HOME`` can override the default location.
"""

import os

# ---------------------------------------------------------------------------
# Core paths
# ---------------------------------------------------------------------------

ROSETTA_HOME = os.environ.get(
    "ROSETTA_HOME",
    os.path.join(os.path.expanduser("~"), ".rosetta"),
)

CONFIG_FILE = os.path.join(ROSETTA_HOME, "config.json")
RESULTS_DIR = os.path.join(ROSETTA_HOME, "results")
MTR_LOGS_DIR = os.path.join(ROSETTA_HOME, "mtr_logs")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_home() -> str:
    """Create ``~/.rosetta`` if it doesn't exist. Return the path."""
    os.makedirs(ROSETTA_HOME, exist_ok=True)
    return ROSETTA_HOME


def ensure_results_dir() -> str:
    """Create the results directory if needed. Return the path."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    return RESULTS_DIR


def ensure_mtr_logs_dir() -> str:
    """Create the mtr_logs directory if needed. Return the path."""
    os.makedirs(MTR_LOGS_DIR, exist_ok=True)
    return MTR_LOGS_DIR
