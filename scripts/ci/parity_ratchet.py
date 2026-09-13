#!/usr/bin/env python3
"""Ratchet gate for the hot-vs-cold parity suite.

Reads `go test -json` output from the parity suite and compares it against
a checked-in allowlist of known failures (``tests/parity/known_failures.txt``).

The job fails when any of the following is true:

1. A test failed and is not covered by the allowlist — a new divergence or a
   harness defect regressed.
2. A test started and never finished. `go test -json` emits ``run`` for it and
   then no ``pass``, ``fail`` or ``skip`` — which is exactly what the
   ``-timeout`` alarm, a panic outside the test goroutine or an ``os.Exit``
   leaves behind. The only other trace of the crash is a package-level
   ``fail``, so without this check a suite that died half-way through, after
   its earlier tests had all matched the allowlist, would pass the gate.
3. The test binary panicked (a ``panic:`` line at the start of the output),
   even if every test that did report a result is accounted for — a known
   failure that starts crashing the binary takes every later test with it.
4. A package failed without any failing or aborted test: a crash after the
   last test finished, a ``TestMain`` exit, or a build failure.
5. An allowlisted test now passes, is skipped, or no longer exists — the entry
   is stale and must be deleted so the allowlist only ever shrinks.
6. The number of passing tests dropped below the allowlist's recorded
   ``min-pass`` expectation — coverage went backwards even though nothing
   turned red (for example a test started skipping on missing data).

Results are keyed by ``(package, test)``, so the same test name in two
packages is two results; an allowlist entry names a test path and covers that
path in every package.

A parent test that fails only because an allowlisted subtest failed is
accepted without needing its own allowlist entry. That rule has a blind spot
which cannot be closed from outside the test binary: `go test -json` reports
a parent as a single ``fail`` whether it failed only through its children or
ALSO in its own body (an assertion after its ``t.Run`` calls), so a parent is
excused whenever all of its failing children are listed, even if its own body
failed too. Keep assertions out of parent bodies whose children are
allowlisted.

Usage::

    python scripts/ci/parity_ratchet.py \\
        --results parity-results.json \\
        --allowlist tests/parity/known_failures.txt \\
        [--summary-file "$GITHUB_STEP_SUMMARY"]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from typing import Iterable

# Terminal actions emitted by `go test -json` for a single test.
TERMINAL_ACTIONS = ("pass", "fail", "skip")

MIN_PASS_DIRECTIVE = "min-pass:"

# The allowlist separates a test path from its reason with a `#` that has
# whitespace on its left and whitespace (or the end of the line) on its right.
# Go never puts whitespace in a test path — t.Run rewrites spaces to `_` — so
# the first such `#` is always the separator, while a `#` inside a path (the
# `#01` suffix go test appends to a duplicate subtest name) is not.
REASON_SEPARATOR = re.compile(r"\s+#(?:\s+|$)")

# The Go runtime writes this at the start of a line when a panic terminates
# the test binary; the `-timeout` alarm is reported the same way.
PANIC_PREFIX = "panic: "

# (package, test path)
ResultKey = tuple[str, str]


@dataclass
class Allowlist:
    """Parsed contents of ``known_failures.txt``."""

    entries: dict[str, str] = field(default_factory=dict)
    min_pass: int | None = None


def parse_allowlist(text: str) -> Allowlist:
    """Parse the allowlist file.

    Each non-comment line is ``<test path>  # <reason>``: the path, then a
    ``#`` with whitespace on both sides, then the reason. The reason is
    mandatory so every entry points at a tracked divergence. A single
    ``# min-pass: N`` comment records the expected number of passing tests.
    """
    result = Allowlist()
    min_pass_line: int | None = None
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            directive = line.lstrip("#").strip()
            if directive.startswith(MIN_PASS_DIRECTIVE):
                if min_pass_line is not None:
                    raise ValueError(
                        f"line {lineno}: duplicate min-pass directive (the first "
                        f"is on line {min_pass_line}) — keep exactly one"
                    )
                value = directive[len(MIN_PASS_DIRECTIVE) :].strip()
                try:
                    min_pass = int(value)
                except ValueError as exc:
                    raise ValueError(
                        f"line {lineno}: invalid min-pass value {value!r}"
                    ) from exc
                if min_pass < 0:
                    raise ValueError(
                        f"line {lineno}: min-pass must not be negative, got {min_pass}"
                    )
                result.min_pass = min_pass
                min_pass_line = lineno
            continue
        parts = REASON_SEPARATOR.split(line, maxsplit=1)
        name = parts[0].strip()
        reason = parts[1].strip() if len(parts) == 2 else ""
        if not reason:
            raise ValueError(
                f"line {lineno}: allowlist entry {name!r} has no ' # reason' — "
                "separate the test path from its reason with whitespace, '#' "
                "and whitespace; every entry must reference a divergence id "
                "from docs/parity-and-gaps.md"
            )
        if any(ch.isspace() for ch in name):
            raise ValueError(
                f"line {lineno}: test path {name!r} contains whitespace — Go "
                "test paths never do, so the ' # ' separator is missing"
            )
        if name in result.entries:
            raise ValueError(f"line {lineno}: duplicate allowlist entry {name!r}")
        result.entries[name] = reason
    return result


@dataclass
class GoTestRun:
    """What one `go test -json` stream reported."""

    # Terminal action of every test that reported one.
    results: dict[ResultKey, str] = field(default_factory=dict)
    # Every test that emitted a `run` event.
    started: set[ResultKey] = field(default_factory=set)
    # Packages that reported a package-level `fail`.
    failed_packages: set[str] = field(default_factory=set)
    # First panic line per package, with the test whose output carried it
    # ("" when the package itself printed it).
    panics: dict[str, tuple[str, str]] = field(default_factory=dict)

    def aborted(self) -> list[ResultKey]:
        """Tests that started and never reported pass, fail or skip."""
        return sorted(self.started - self.results.keys())

    def packages(self) -> set[str]:
        return {pkg for pkg, _ in self.started | self.results.keys()}


def parse_go_test_json(lines: Iterable[str]) -> GoTestRun:
    """Collect test results, started tests, package failures and panics.

    Lines that are not JSON (a `docker compose` banner, a truncated line) are
    ignored so the gate still works on a tee'd log.
    """
    run = GoTestRun()
    for line in lines:
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        package = event.get("Package") or ""
        test = event.get("Test") or ""
        action = event.get("Action")
        if action == "output":
            output = event.get("Output") or ""
            if output.startswith(PANIC_PREFIX) and package not in run.panics:
                run.panics[package] = (test, output.strip())
            continue
        if not test:
            if action == "fail" and package:
                run.failed_packages.add(package)
            continue
        key = (package, test)
        if action == "run":
            run.started.add(key)
        elif action in TERMINAL_ACTIONS:
            run.results[key] = action
    return run


def _is_ancestor_of(name: str, other: str) -> bool:
    return other.startswith(name + "/")


def unexpected_failures(
    results: dict[ResultKey, str], allowed: set[str]
) -> list[ResultKey]:
    """Failing tests that neither are allowlisted nor only carry failing children."""
    failing = {key for key, action in results.items() if action == "fail"}
    # Deepest tests first: a parent is only excused once its children are.
    ordered = sorted(failing, key=lambda k: (-k[1].count("/"), k))
    accepted: set[ResultKey] = set()
    unexpected: list[ResultKey] = []
    for key in ordered:
        package, name = key
        if name in allowed:
            accepted.add(key)
            continue
        children = [
            f for f in failing if f[0] == package and _is_ancestor_of(name, f[1])
        ]
        if children and all(c in accepted for c in children):
            accepted.add(key)
            continue
        unexpected.append(key)
    return sorted(unexpected)


def stale_entries(
    results: dict[ResultKey, str],
    allowlist: Allowlist,
    aborted: Iterable[ResultKey] = (),
) -> list[tuple[str, str]]:
    """Allowlist entries that are no longer failing, with the reason why.

    An entry whose test aborted is left to the aborted-test report: calling it
    "did not run" would hide that it crashed.
    """
    aborted_names = {name for _, name in aborted}
    actions_by_name: dict[str, set[str]] = {}
    for (_, name), action in results.items():
        actions_by_name.setdefault(name, set()).add(action)
    stale: list[tuple[str, str]] = []
    for name in sorted(allowlist.entries):
        actions = actions_by_name.get(name, set())
        if "fail" in actions or name in aborted_names:
            continue
        if not actions:
            stale.append((name, "did not run"))
        elif actions == {"pass"}:
            stale.append((name, "now passes"))
        elif actions == {"skip"}:
            stale.append((name, "now skips"))
        else:
            stale.append((name, "now " + " and ".join(sorted(actions))))
    return stale


def unexplained_package_failures(run: GoTestRun) -> list[str]:
    """Packages that failed although none of their tests failed or aborted."""
    explained = {pkg for (pkg, _), action in run.results.items() if action == "fail"}
    explained |= {pkg for pkg, _ in run.aborted()}
    return sorted(run.failed_packages - explained)


def count_by_action(results: dict[ResultKey, str]) -> dict[str, int]:
    counts = {action: 0 for action in TERMINAL_ACTIONS}
    for action in results.values():
        counts[action] += 1
    return counts


@dataclass
class Verdict:
    unexpected: list[ResultKey] = field(default_factory=list)
    aborted: list[ResultKey] = field(default_factory=list)
    panics: dict[str, tuple[str, str]] = field(default_factory=dict)
    crashed_packages: list[str] = field(default_factory=list)
    stale: list[tuple[str, str]] = field(default_factory=list)
    pass_regression: str | None = None

    @property
    def failed(self) -> bool:
        return bool(
            self.unexpected
            or self.aborted
            or self.panics
            or self.crashed_packages
            or self.stale
            or self.pass_regression
        )


def evaluate(run: GoTestRun, allowlist: Allowlist) -> Verdict:
    verdict = Verdict()
    verdict.unexpected = unexpected_failures(run.results, set(allowlist.entries))
    verdict.aborted = run.aborted()
    verdict.panics = dict(run.panics)
    verdict.crashed_packages = unexplained_package_failures(run)
    verdict.stale = stale_entries(run.results, allowlist, verdict.aborted)
    passes = count_by_action(run.results)["pass"]
    if allowlist.min_pass is not None and passes < allowlist.min_pass:
        verdict.pass_regression = (
            f"Only {passes} tests passed; the allowlist records a minimum of "
            f"{allowlist.min_pass}. Coverage went backwards — most likely a test "
            "started skipping because its data or query stopped producing rows."
        )
    return verdict


def _label(key: ResultKey, qualify: bool) -> str:
    package, name = key
    return f"{package}: {name}" if qualify and package else name


def render_summary(run: GoTestRun, allowlist: Allowlist, verdict: Verdict) -> str:
    qualify = len(run.packages()) > 1
    counts = count_by_action(run.results)
    lines = ["## Parity Test Results", ""]
    lines.append("| Result | Count |")
    lines.append("| --- | --- |")
    lines.append(f"| Passed | {counts['pass']} |")
    lines.append(f"| Failed | {counts['fail']} |")
    lines.append(f"| Skipped | {counts['skip']} |")
    lines.append(f"| Aborted (started, never finished) | {len(verdict.aborted)} |")
    lines.append(f"| Known failures allowlisted | {len(allowlist.entries)} |")
    if allowlist.min_pass is not None:
        lines.append(f"| Minimum expected passes | {allowlist.min_pass} |")
    lines.append("")

    if verdict.aborted or verdict.panics or verdict.crashed_packages:
        lines.append("### Crashed or aborted")
        lines.append("")
        lines.append(
            "The test binary did not run to completion, so every result after "
            "this point is missing rather than known. An allowlist entry never "
            "covers this."
        )
        lines.append("")
        for key in verdict.aborted:
            lines.append(
                f"- `{_label(key, qualify)}` — aborted (crash or timeout): "
                "started and never reported pass, fail or skip"
            )
        for package, (test, message) in sorted(verdict.panics.items()):
            where = f" while `{test}` was running" if test else ""
            lines.append(f"- package `{package}` panicked{where}: `{message}`")
        for package in verdict.crashed_packages:
            lines.append(
                f"- package `{package}` failed with no failing or aborted test "
                "(crash after the last test, TestMain exit, or build failure)"
            )
        lines.append("")

    if verdict.unexpected:
        lines.append("### Unexpected failures")
        lines.append("")
        lines.append(
            "These are not in `tests/parity/known_failures.txt`. Either fix them "
            "or add an entry naming the divergence from `docs/parity-and-gaps.md`."
        )
        lines.append("")
        for key in verdict.unexpected:
            lines.append(f"- `{_label(key, qualify)}`")
        lines.append("")

    if verdict.stale:
        lines.append("### Stale allowlist entries")
        lines.append("")
        lines.append(
            "These no longer fail. Delete them from "
            "`tests/parity/known_failures.txt` so the allowlist only shrinks."
        )
        lines.append("")
        for name, why in verdict.stale:
            lines.append(f"- `{name}` — {why}")
        lines.append("")

    if verdict.pass_regression:
        lines.append("### Pass-count regression")
        lines.append("")
        lines.append(verdict.pass_regression)
        lines.append("")

    if not verdict.failed:
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
        run = parse_go_test_json(sys.stdin)
    else:
        with open(args.results, encoding="utf-8", errors="replace") as fh:
            run = parse_go_test_json(fh)

    with open(args.allowlist, encoding="utf-8") as fh:
        allowlist = parse_allowlist(fh.read())

    if not run.results and not run.started and not run.failed_packages:
        print(
            "parity_ratchet: no test results parsed — the suite did not run "
            "or its output was not captured with `go test -json`",
            file=sys.stderr,
        )
        return 1

    verdict = evaluate(run, allowlist)
    summary = render_summary(run, allowlist, verdict)
    print(summary)
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as fh:
            fh.write(summary)
            fh.write("\n")

    return 1 if verdict.failed else 0


if __name__ == "__main__":
    sys.exit(main())
