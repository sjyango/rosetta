#!/usr/bin/env python3
"""Resolve and run Rosetta from local install, cache, or GitHub Release."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable, Optional, Tuple

DEFAULT_REPO = "sjyango/rosetta"
DEFAULT_VERSION = os.environ.get("ROSETTA_VERSION", "v1.0.0")
DEFAULT_CACHE_DIR = Path.home() / ".cache" / "rosetta-runtime"
USER_AGENT = "rosetta-github-release-skill/1.0"


class ReleaseError(RuntimeError):
    """Raised when GitHub Release runtime resolution fails."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve and run Rosetta from environment, PATH, cache, or GitHub Release.",
    )
    parser.add_argument("--repo", default=DEFAULT_REPO, help="GitHub repo in owner/name form")
    parser.add_argument("--version", default=DEFAULT_VERSION, help="Release tag, e.g. v1.0.0")
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help="Cache directory for downloaded runtimes",
    )
    parser.add_argument(
        "--asset-name",
        default=None,
        help="Explicit release asset name. If omitted, auto-discover the .pyz asset.",
    )
    parser.add_argument(
        "--ensure-only",
        action="store_true",
        help="Ensure the runtime exists locally, then print its path and exit.",
    )
    parser.add_argument(
        "--print-command",
        action="store_true",
        help="Print the resolved execution command instead of running it.",
    )
    parser.add_argument(
        "args",
        nargs=argparse.REMAINDER,
        help="Arguments passed to Rosetta after '--'.",
    )
    return parser.parse_args()


def normalize_passthrough_args(args: Iterable[str]) -> list[str]:
    args = list(args)
    if args and args[0] == "--":
        return args[1:]
    return args


def env_rosetta_bin() -> Optional[Tuple[str, list[str]]]:
    value = os.environ.get("ROSETTA_BIN")
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.exists():
        raise ReleaseError(f"ROSETTA_BIN points to a missing path: {path}")
    return command_for_path(path)


def path_rosetta_bin() -> Optional[Tuple[str, list[str]]]:
    binary = shutil.which("rosetta")
    if not binary:
        return None
    return (binary, [binary])


def command_for_path(path: Path) -> Tuple[str, list[str]]:
    if path.suffix == ".pyz":
        return (str(path), [sys.executable, str(path)])
    return (str(path), [str(path)])


def request_json(url: str) -> dict:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/vnd.github+json"})
    try:
        with urllib.request.urlopen(request) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise ReleaseError(f"Failed to fetch release metadata: HTTP {exc.code} for {url}") from exc
    except urllib.error.URLError as exc:
        raise ReleaseError(f"Failed to fetch release metadata: {exc.reason}") from exc


def download_file(url: str, destination: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir=str(destination.parent)) as tmp:
        temp_path = Path(tmp.name)
    try:
        with urllib.request.urlopen(request) as response, temp_path.open("wb") as output:
            shutil.copyfileobj(response, output)
        temp_path.replace(destination)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_sha256_file(path: Path, fallback_asset_name: str) -> str:
    line = path.read_text(encoding="utf-8").strip().splitlines()[0]
    parts = line.split()
    if not parts:
        raise ReleaseError(f"Invalid checksum file: {path}")
    checksum = parts[0]
    if len(parts) >= 2:
        filename = parts[-1].lstrip("*")
        if filename != fallback_asset_name:
            raise ReleaseError(
                f"Checksum file {path.name} targets {filename}, expected {fallback_asset_name}",
            )
    return checksum


def discover_release_assets(repo: str, version: str) -> tuple[dict, dict | None]:
    metadata = request_json(f"https://api.github.com/repos/{repo}/releases/tags/{version}")
    assets = metadata.get("assets", [])
    pyz_asset = None
    sha_asset = None
    for asset in assets:
        name = asset.get("name", "")
        if pyz_asset is None and name.endswith(".pyz"):
            pyz_asset = asset
        elif sha_asset is None and name.endswith(".sha256"):
            sha_asset = asset
    if pyz_asset is None:
        raise ReleaseError(f"No .pyz asset found in release {version} for {repo}")
    return pyz_asset, sha_asset


def ensure_release_runtime(repo: str, version: str, cache_dir: Path, asset_name: Optional[str]) -> Tuple[str, list[str]]:
    version_dir = cache_dir / version
    runtime_path = version_dir / "rosetta.pyz"
    checksum_path = version_dir / "rosetta.sha256"

    if runtime_path.is_file():
        return command_for_path(runtime_path)

    pyz_asset, sha_asset = discover_release_assets(repo, version)
    if asset_name is not None and pyz_asset.get("name") != asset_name:
        raise ReleaseError(
            f"Requested asset {asset_name} but release provides {pyz_asset.get('name')}",
        )

    download_file(pyz_asset["browser_download_url"], runtime_path)
    runtime_path.chmod(runtime_path.stat().st_mode | stat.S_IXUSR)

    if sha_asset is not None:
        download_file(sha_asset["browser_download_url"], checksum_path)
        expected = parse_sha256_file(checksum_path, pyz_asset["name"])
        actual = sha256_file(runtime_path)
        if actual != expected:
            runtime_path.unlink(missing_ok=True)
            raise ReleaseError(
                f"Checksum mismatch for {runtime_path.name}: expected {expected}, got {actual}",
            )

    return command_for_path(runtime_path)


def resolve_command(repo: str, version: str, cache_dir: Path, asset_name: Optional[str]) -> Tuple[str, list[str]]:
    env_command = env_rosetta_bin()
    if env_command is not None:
        return env_command

    path_command = path_rosetta_bin()
    if path_command is not None:
        return path_command

    return ensure_release_runtime(repo, version, cache_dir, asset_name)


def main() -> int:
    args = parse_args()
    passthrough_args = normalize_passthrough_args(args.args)
    cache_dir = Path(args.cache_dir).expanduser().resolve()

    try:
        runtime_path, command = resolve_command(args.repo, args.version, cache_dir, args.asset_name)
    except ReleaseError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.ensure_only:
        print(runtime_path)
        return 0

    full_command = command + passthrough_args

    if args.print_command:
        print(" ".join(full_command))
        return 0

    completed = subprocess.run(full_command)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
