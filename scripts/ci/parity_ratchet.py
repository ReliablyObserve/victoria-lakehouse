#!/usr/bin/env python3
"""Ratchet gate for the hot-vs-cold parity suite.

Reads `go test -json` output from the parity suite and compares it against
a checked-in allowlist of known failures (``tests/parity/known_failures.txt``).

The job fails when any of the following is true:

1. A test failed and is not covered by the allowlist — a new divergence or a
   harness defect regressed.
2. An allowlisted test now passes, is skipped, or no longer exists — the entry
   is stale and must be deleted so the allowlist only ever shrinks.
3. The number of passing tests dropped below the allowlist's recorded
   ``min-pass`` expectation — coverage went backwards even though nothing
   turned red (for example a test started skipping on missing data).

A parent test that fails only because an allowlisted subtest failed is
accepted without needing its own allowlist entry.

Usage::

    python scripts/ci/parity_ratchet.py \\
        --results parity-results.json \\
        --allowlist tests/parity/known_failures.txt \\
        [--summary-file "$GITHUB_STEP_SUMMARY"]
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from typing import Iterable

# Terminal actions emitted by `go test -json` for a single test.
TERMINAL_ACTIONS = ("pass", "fail", "skip")

MIN_PASS_DIRECTIVE = "min-pass:"


@dataclass
class Allowlist:
    """Parsed contents of ``known_failures.txt``."""

    entries: dict[str, str] = field(default_factory=dict)
    min_pass: int | None = None


def parse_allowlist(text: str) -> Allowlist:
    """Parse the allowlist file.

    Each non-comment line is ``<test path>  # <reason>``. The reason is
    mandatory so every entry points at a tracked divergence. A
    ``# min-pass: N`` comment records the expected number of passing tests.
    """
    result = Allowlist()
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            directive = line.lstrip("#").strip()
            if directive.startswith(MIN_PASS_DIRECTIVE):
                value = directive[len(MIN_PASS_DIRECTIVE) :].strip()
                try:
                    result.min_pass = int(value)
                except ValueError as exc:
                    raise ValueError(
                        f"line {lineno}: invalid min-pass value {value!r}"
                    ) from exc
            continue
        name, sep, reason = line.partition("#")
        name = name.strip()
        reason = reason.strip()
        if not name:
            continue
        if not sep or not reason:
            raise ValueError(
                f"line {lineno}: allowlist entry {name!r} has no '# reason' "
                "comment — every entry must reference a divergence id from "
                "docs/parity-and-gaps.md"
            )
        if name in result.entries:
            raise ValueError(f"line {lineno}: duplicate allowlist entry {name!r}")
        result.entries[name] = reason
    return result


def parse_go_test_json(lines: Iterable[str]) -> dict[str, str]:
    """Return ``{test name: terminal action}`` from `go test -json` output.

    Lines that are not JSON (a panic trace, a `docker compose` banner) are
    ignored so the gate still works on a tee'd log.
    """
    results: dict[str, str] = {}
    for line in lines:
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        test = event.get("Test")
        action = event.get("Action")
        if not test or action not in TERMINAL_ACTIONS:
            continue
        results[test] = action
    return results


def _is_ancestor_of(name: str, other: str) -> bool:
    return other.startswith(name + "/")


def unexpected_failures(
    results: dict[str, str], allowed: set[str]
) -> list[str]:
    """Failing tests that neither are allowlisted nor only carry a failing child."""
    failing = {name for name, action in results.items() if action == "fail"}
    # Deepest tests first: a parent is only excused once its children are.
    ordered = sorted(failing, key=lambda n: (-n.count("/"), n))
    accepted: set[str] = set()
    unexpected: list[str] = []
    for name in ordered:
        if name in allowed:
            accepted.add(name)
            continue
        children = [f for f in failing if _is_ancestor_of(name, f)]
        if children and all(c in accepted for c in children):
            accepted.add(name)
            continue
        unexpected.append(name)
    return sorted(unexpected)


def stale_entries(
    results: dict[str, str], allowlist: Allowlist
) -> list[tuple[str, str]]:
    """Allowlist entries that are no longer failing, with the reason why."""
    stale: list[tuple[str, str]] = []
    for name in sorted(allowlist.entries):
        action = results.get(name)
        if action == "fail":
            continue
        if action is None:
            stale.append((name, "did not run"))
        else:
            stale.append((name, f"now {action}es" if action == "pass" else f"now {action}s"))
    return stale


def count_by_action(results: dict[str, str]) -> dict[str, int]:
    counts = {action: 0 for action in TERMINAL_ACTIONS}
    for action in results.values():
        counts[action] += 1
    return counts


def render_summary(
    results: dict[str, str],
    allowlist: Allowlist,
    unexpected: list[str],
    stale: list[tuple[str, str]],
    pass_regression: str | None,
) -> str:
    counts = count_by_action(results)
    lines = ["## Parity Test Results", ""]
    lines.append("| Result | Count |")
    lines.append("| --- | --- |")
    lines.append(f"| Passed | {counts['pass']} |")
    lines.append(f"| Failed | {counts['fail']} |")
    lines.append(f"| Skipped | {counts['skip']} |")
    lines.append(f"| Known failures allowlisted | {len(allowlist.entries)} |")
    if allowlist.min_pass is not None:
        lines.append(f"| Minimum expected passes | {allowlist.min_pass} |")
    lines.append("")

    if unexpected:
        lines.append("### Unexpected failures")
        lines.append("")
        lines.append(
            "These are not in `tests/parity/known_failures.txt`. Either fix them "
            "or add an entry naming the divergence from `docs/parity-and-gaps.md`."
        )
        lines.append("")
        for name in unexpected:
            lines.append(f"- `{name}`")
        lines.append("")

    if stale:
        lines.append("### Stale allowlist entries")
        lines.append("")
        lines.append(
            "These no longer fail. Delete them from "
            "`tests/parity/known_failures.txt` so the allowlist only shrinks."
        )
        lines.append("")
        for name, why in stale:
            lines.append(f"- `{name}` — {why}")
        lines.append("")

    if pass_regression:
        lines.append("### Pass-count regression")
        lines.append("")
        lines.append(pass_regression)
        lines.append("")

    if not unexpected and not stale and not pass_regression:
        lines.append("All failures are known and every allowlist entry is still live.")
        lines.append("")
        if allowlist.entries:
            lines.append("<details><summary>Known failures</summary>")
            lines.append("")
            for name, reason in sorted(allowlist.entries.items()):
                lines.append(f"- `{name}` — {reason}")
            lines.append("")
            lines.append("</details>")
            lines.append("")

    return "\n".join(lines)


def evaluate(
    results: dict[str, str], allowlist: Allowlist
) -> tuple[list[str], list[tuple[str, str]], str | None]:
    unexpected = unexpected_failures(results, set(allowlist.entries))
    stale = stale_entries(results, allowlist)
    pass_regression = None
    passes = count_by_action(results)["pass"]
    if allowlist.min_pass is not None and passes < allowlist.min_pass:
        pass_regression = (
            f"Only {passes} tests passed; the allowlist records a minimum of "
            f"{allowlist.min_pass}. Coverage went backwards — most likely a test "
            "started skipping because its data or query stopped producing rows."
        )
    return unexpected, stale, pass_regression


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results",
        required=True,
        help="file containing `go test -json` output ('-' for stdin)",
    )
    parser.add_argument(
        "--allowlist",
        default="tests/parity/known_failures.txt",
        help="path to the known-failure allowlist",
    )
    parser.add_argument(
        "--summary-file",
        default=None,
        help="append the markdown summary here (e.g. $GITHUB_STEP_SUMMARY)",
    )
    args = parser.parse_args(argv)

    if args.results == "-":
        results = parse_go_test_json(sys.stdin)
    else:
        with open(args.results, encoding="utf-8", errors="replace") as fh:
            results = parse_go_test_json(fh)

    with open(args.allowlist, encoding="utf-8") as fh:
        allowlist = parse_allowlist(fh.read())

    if not results:
        print(
            "parity_ratchet: no test results parsed — the suite did not run "
            "or its output was not captured with `go test -json`",
            file=sys.stderr,
        )
        return 1

    unexpected, stale, pass_regression = evaluate(results, allowlist)
    summary = render_summary(results, allowlist, unexpected, stale, pass_regression)
    print(summary)
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as fh:
            fh.write(summary)
            fh.write("\n")

    return 1 if (unexpected or stale or pass_regression) else 0


if __name__ == "__main__":
    sys.exit(main())
