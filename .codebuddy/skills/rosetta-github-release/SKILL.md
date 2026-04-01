---
name: rosetta-github-release
description: This skill should be used when users want to publish Rosetta as a GitHub Release runtime, run Rosetta on machines without a preinstalled `rosetta` executable, or maintain the release-backed Rosetta Skill workflow.
---

# Rosetta GitHub Release

## Overview

Provide a release-backed workflow for Rosetta so that CodeBuddy can run Rosetta without relying on a local source checkout or a preinstalled `rosetta` binary. Use the bundled scripts to prepare versioned release assets, resolve a runtime from GitHub Releases, and launch Rosetta commands in a deterministic way.

## Use This Skill When

Trigger this skill in any of these scenarios:

- Publish a Rosetta release to GitHub and attach runtime artifacts.
- Run Rosetta on a machine that does not already have `rosetta` installed.
- Package or update the project-level Rosetta Skill that depends on a GitHub Release runtime.
- Diagnose why Rosetta cannot be found from `ROSETTA_BIN`, `PATH`, cache, or GitHub Release.

## Core Workflow

### 1. Prepare a release branch

Use `release-<version>` for release work instead of `main`. For `v1.0.0`, use `release-1.0.0`.

### 2. Build versioned release artifacts

Run `scripts/prepare_release_assets.py` to build and rename the runtime artifact to a versioned file, then generate a SHA256 checksum.

Expected outputs for `v1.0.0`:

- `dist/rosetta-v1.0.0.pyz`
- `dist/rosetta-v1.0.0.sha256`

### 3. Publish a GitHub Release

Create tag `v1.0.0` from the release branch and upload the two files above to the GitHub Release. Do not depend on `latest`; always pin an explicit version.

For the detailed publishing sequence, load `references/release-process.md`.

### 4. Resolve or launch Rosetta from the release runtime

Use `scripts/run_rosetta.py` whenever Rosetta must be executed in an environment that may not have a local install.

Resolution order:

1. `ROSETTA_BIN`
2. `rosetta` from `PATH`
3. cached release asset under `~/.cache/rosetta-runtime/`
4. download from GitHub Release

Example invocations:

```bash
python .codebuddy/skills/rosetta-github-release/scripts/run_rosetta.py --version v1.0.0 -- --version
python .codebuddy/skills/rosetta-github-release/scripts/run_rosetta.py --version v1.0.0 -- status --config dbms_config.json
python .codebuddy/skills/rosetta-github-release/scripts/run_rosetta.py --ensure-only --version v1.0.0
```

## Bundled Resources

### `scripts/prepare_release_assets.py`

Build the local `.pyz` artifact, rename it to the versioned release filename, and generate a checksum file suitable for GitHub Release upload.

### `scripts/run_rosetta.py`

Resolve a Rosetta runtime from environment override, system `PATH`, local cache, or GitHub Release, then launch Rosetta with forwarded arguments.

### `references/release-process.md`

Document the branch, tag, asset naming, and upload sequence for release-backed Skill publishing.

## Operating Rules

Pin an explicit release version such as `v1.0.0`; never default to `latest` in automation.

Prefer the bundled launcher over direct `rosetta` calls when runtime availability is uncertain.

Fail fast with a clear error when the GitHub Release does not contain a `.pyz` asset for the requested version.

Verify checksum when a matching `.sha256` asset exists.
