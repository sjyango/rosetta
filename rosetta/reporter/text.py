"""Plain-text report generator for Rosetta."""

import logging
import time
from typing import Dict

from ..models import CompareResult

log = logging.getLogger("rosetta")


def write_text_report(path: str, test_file: str,
                      comparisons: Dict[str, CompareResult]) -> bool:
    """Write a plain-text comparison report.

    Returns True if all comparisons passed.
    """
    all_pass = True

    with open(path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("Rosetta Cross-DBMS Test Comparison Report\n")
        f.write(f"Test file: {test_file}\n")
        f.write(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 70 + "\n\n")

        f.write("SUMMARY\n")
        f.write("-" * 70 + "\n")
        f.write(f"{'Comparison':<35} {'Match':>6} {'Mismatch':>9} "
                f"{'Skip':>6} {'Total':>6} {'Pass%':>7}\n")
        f.write("-" * 70 + "\n")

        for key, cmp in comparisons.items():
            f.write(f"{key:<35} {cmp.matched:>6} "
                    f"{cmp.mismatched:>9} {cmp.skipped:>6} "
                    f"{cmp.total_stmts:>6} "
                    f"{cmp.pass_rate:>6.1f}%\n")
            if cmp.mismatched > 0:
                all_pass = False

        f.write("-" * 70 + "\n")
        if all_pass:
            f.write("RESULT: ALL PASSED\n\n")
        else:
            f.write("RESULT: DIFFERENCES FOUND\n\n")

        for key, cmp in comparisons.items():
            if not cmp.diffs:
                continue
            f.write("=" * 70 + "\n")
            f.write(f"DIFFS: {key}\n")
            f.write("=" * 70 + "\n")
            for d in cmp.diffs:
                f.write(f"\n--- Block {d['block']}: "
                        f"{d['stmt'][:80]}\n")
                # Show surrounding context for quick orientation
                ctx_before = d.get("context_before", [])
                ctx_after = d.get("context_after", [])
                if ctx_before or ctx_after:
                    f.write("    Context:\n")
                    for c in ctx_before:
                        f.write(f"      Block {c['block']:>4}: "
                                f"{c['stmt'][:70]}\n")
                    f.write(f"    ▶ Block {d['block']:>4}: "
                            f"{d['stmt'][:70]}\n")
                    for c in ctx_after:
                        f.write(f"      Block {c['block']:>4}: "
                                f"{c['stmt'][:70]}\n")
                for dl in d["diff"]:
                    f.write(dl + "\n")
            f.write("\n")

    log.info("Text report written: %s", path)
    return all_pass


def write_diff_file(path: str,
                    comparisons: Dict[str, CompareResult]):
    """Write a unified diff file."""
    diff_lines = []
    for key, cmp in comparisons.items():
        if not cmp.diffs:
            continue
        diff_lines.append("=" * 70)
        diff_lines.append(f"DIFFS: {key}")
        diff_lines.append("=" * 70)
        for d in cmp.diffs:
            diff_lines.append(
                f"\n--- Block {d['block']}: {d['stmt'][:100]}"
            )
            ctx_before = d.get("context_before", [])
            ctx_after = d.get("context_after", [])
            if ctx_before or ctx_after:
                diff_lines.append("    Context:")
                for c in ctx_before:
                    diff_lines.append(
                        f"      Block {c['block']:>4}: {c['stmt'][:80]}")
                diff_lines.append(
                    f"    ▶ Block {d['block']:>4}: {d['stmt'][:80]}")
                for c in ctx_after:
                    diff_lines.append(
                        f"      Block {c['block']:>4}: {c['stmt'][:80]}")
            for dl in d["diff"]:
                diff_lines.append(dl)
        diff_lines.append("")

    if diff_lines:
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(diff_lines) + "\n")
        log.info("Diff file written: %s", path)
