#!/usr/bin/env python3
"""Build versioned Rosetta release artifacts for GitHub Releases."""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build and prepare versioned Rosetta release artifacts.",
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Rosetta repository root (default: current directory)",
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Release version tag, e.g. v1.0.0",
    )
    parser.add_argument(
        "--output-dir",
        default="dist",
        help="Output directory relative to repo root (default: dist)",
    )
    parser.add_argument(
        "--build-script",
        default="build.sh",
        help="Build script relative to repo root (default: build.sh)",
    )
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    build_script = repo_root / args.build_script
    output_dir = (repo_root / args.output_dir).resolve()

    if not build_script.is_file():
        print(f"ERROR: build script not found: {build_script}", file=sys.stderr)
        return 1

    subprocess.run([str(build_script)], cwd=repo_root, check=True)

    built_asset = output_dir / "rosetta.pyz"
    if not built_asset.is_file():
        print(f"ERROR: built artifact not found: {built_asset}", file=sys.stderr)
        return 1

    version = args.version
    versioned_asset = output_dir / f"rosetta-{version}.pyz"
    checksum_file = output_dir / f"rosetta-{version}.sha256"

    shutil.copy2(built_asset, versioned_asset)
    checksum = sha256_file(versioned_asset)
    checksum_file.write_text(f"{checksum}  {versioned_asset.name}\n", encoding="utf-8")

    print(f"Prepared release asset: {versioned_asset}")
    print(f"Prepared checksum: {checksum_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
