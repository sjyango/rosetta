"""Rosetta — Cross-DBMS SQL behavioral consistency verification tool."""

from importlib.metadata import version as _get_version

try:
    __version__ = _get_version("rosetta-sql")
except Exception:
    # Fallback for editable install / development mode
    import tomllib
    with open(__file__ + "/../pyproject.toml", "rb") as _f:
        __version__ = tomllib.load(_f)["project"]["version"]
