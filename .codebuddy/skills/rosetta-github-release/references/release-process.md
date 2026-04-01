# Rosetta GitHub Release Process

## Goal

Publish a version-pinned Rosetta runtime that the project Skill can download deterministically from GitHub Releases.

## Branch and tag conventions

- Release branch: `release-1.0.0`
- Git tag: `v1.0.0`
- Runtime asset: `rosetta-v1.0.0.pyz`
- Checksum asset: `rosetta-v1.0.0.sha256`

Keep branch name and tag logically aligned:

- branch uses the plain semantic version without leading `v`
- tag and release assets use the `v`-prefixed version string

## Local preparation

From the repository root:

```bash
git checkout -b release-1.0.0
python .codebuddy/skills/rosetta-github-release/scripts/prepare_release_assets.py --version v1.0.0
```

That produces:

- `dist/rosetta.pyz` from `build.sh`
- `dist/rosetta-v1.0.0.pyz`
- `dist/rosetta-v1.0.0.sha256`

## Publish branch and tag

```bash
git push -u origin release-1.0.0
git tag -a v1.0.0 -m "Rosetta v1.0.0"
git push origin v1.0.0
```

## Create GitHub Release

Either use the GitHub web UI or `gh`:

```bash
gh release create v1.0.0 \
  dist/rosetta-v1.0.0.pyz \
  dist/rosetta-v1.0.0.sha256 \
  --title "Rosetta v1.0.0" \
  --notes "Rosetta v1.0.0 runtime release"
```

## Skill-side runtime contract

The Skill launcher expects the target GitHub Release to contain at least one `.pyz` asset.

Recommended release assets:

- `rosetta-v1.0.0.pyz`
- `rosetta-v1.0.0.sha256`

The launcher verifies checksum when the `.sha256` file is present.

## Runtime resolution order

The Skill launcher resolves Rosetta in this order:

1. `ROSETTA_BIN`
2. `rosetta` from `PATH`
3. cached runtime under `~/.cache/rosetta-runtime/<version>/rosetta.pyz`
4. GitHub Release download

## Recommendations

- Pin an explicit version such as `v1.0.0`; do not use `latest`.
- Keep Skill changes on `release-1.0.0` until the release is published.
- Upload both the runtime and checksum together.
- After the release is live, validate the launcher with:

```bash
python .codebuddy/skills/rosetta-github-release/scripts/run_rosetta.py --version v1.0.0 -- --version
```
