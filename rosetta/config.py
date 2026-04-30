"""Configuration loading and validation for Rosetta."""

import json
import logging
import shutil
from typing import List

from .models import DBMSConfig
from .paths import SAMPLE_CONFIG_FILE

log = logging.getLogger("rosetta")

DEFAULT_TEST_DB = "cross_dbms_test_db"


def load_config(config_path: str) -> List[DBMSConfig]:
    """Load DBMS configurations from a JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    configs = []
    for entry in data.get("databases", []):
        configs.append(DBMSConfig(
            name=entry.get("name", "unknown"),
            host=entry.get("host", "127.0.0.1"),
            port=entry.get("port", 3306),
            user=entry.get("user", "root"),
            password=entry.get("password", ""),
            driver=entry.get("driver", "pymysql"),
            skip_patterns=entry.get("skip_patterns", []),
            init_sql=entry.get("init_sql", []),
            enabled=entry.get("enabled", True),
            restart_cmd=entry.get("restart_cmd", ""),
        ))

    return configs


def filter_configs(configs: List[DBMSConfig],
                   dbms_names: str = None) -> List[DBMSConfig]:
    """Filter configs by --dbms argument or enabled flag.

    Args:
        configs: All loaded DBMS configs.
        dbms_names: Comma-separated DBMS names from --dbms argument,
                    or None to use the enabled flag.

    Returns:
        Filtered list of DBMSConfig.

    Raises:
        ValueError: If a requested DBMS name is not found in configs.
    """
    if dbms_names:
        requested = [n.strip() for n in dbms_names.split(",")]
        # "all" is a special keyword meaning all configured DBMS
        if "all" in requested:
            return list(configs)
        available = {c.name: c for c in configs}
        result = []
        for name in requested:
            if name not in available:
                raise ValueError(
                    f"DBMS '{name}' not found in config. "
                    f"Available: {', '.join(available.keys())}"
                )
            result.append(available[name])
        return result

    # Fall back to enabled flag
    enabled = [c for c in configs if c.enabled]
    disabled = [c.name for c in configs if not c.enabled]
    if disabled:
        log.info("Skipping disabled DBMS: %s", ", ".join(disabled))
    return enabled


def generate_sample_config(path: str):
    """Copy the bundled sample config to *path*."""
    shutil.copy2(SAMPLE_CONFIG_FILE, path)
    log.info("Sample config written to: %s", path)
