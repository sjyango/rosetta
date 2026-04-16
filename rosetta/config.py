"""Configuration loading and validation for Rosetta."""

import json
import logging
from typing import List

from .models import DBMSConfig

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
            skip_explain=entry.get("skip_explain", False),
            skip_analyze=entry.get("skip_analyze", False),
            skip_show_create=entry.get("skip_show_create", False),
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
    """Generate a sample configuration file."""
    sample = {
        "mtr": {
            "test_dir": "/data/workspace/SQLEngine/bld/mysql-test",
            "skip_list": "/data/workspace/SQLEngine/mysql-test/collections/disabled.def",
            "base_port": 13000,
            "total_port": 30000,
            "parallel": 8,
            "retry": 3,
            "retry_failure": 3,
            "max_test_fail": 3000,
            "testcase_timeout": 1200,
            "suite_timeout": 600,
            "mysqld_opts": [
                "tdsql_debug_table_scan_rows=10000",
                "tdsql_auto_increment_batch_size=1",
                "tdsql_enable_proxy_for_system_views=off",
                "log_timestamps=SYSTEM",
                "tdsql_log_autoinc_result=false",
                "tdstore_mod_log_flags=SPECIAL_FLAG=off",
                "tdstore_delete_job_ctx_delay_s=3",
                "tdstore_safely_destroy_region_delay_time_s=3",
                "tdsql_check_task_status_retry_interval_ms=1",
            ],
        },
        "databases": [
            {
                "name": "mysql",
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "password": "",
                "driver": "pymysql",
                "skip_patterns": ["tdsql_", "ddl_recovery"],
                "init_sql": [
                    "SET sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'"
                ],
                "skip_explain": False,
                "skip_analyze": False,
                "skip_show_create": False,
                "restart_cmd": "mysqld_safe &",
            },
            {
                "name": "tdsql",
                "host": "127.0.0.1",
                "port": 4000,
                "user": "root",
                "password": "",
                "driver": "pymysql",
                "skip_patterns": [],
                "init_sql": [],
                "skip_explain": False,
                "skip_analyze": False,
                "skip_show_create": False,
            },
            {
                "name": "tidb",
                "host": "127.0.0.1",
                "port": 4001,
                "user": "root",
                "password": "",
                "driver": "pymysql",
                "skip_patterns": ["tdsql_", "ddl_recovery"],
                "init_sql": [],
                "skip_explain": True,
                "skip_analyze": True,
                "skip_show_create": True,
            },
            {
                "name": "oceanbase",
                "host": "127.0.0.1",
                "port": 2881,
                "user": "root@mysql",
                "password": "",
                "driver": "pymysql",
                "skip_patterns": ["tdsql_", "ddl_recovery"],
                "init_sql": [],
                "skip_explain": True,
                "skip_analyze": True,
                "skip_show_create": True,
            },
        ]
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=2, ensure_ascii=False)
    log.info("Sample config written to: %s", path)
