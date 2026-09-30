#!/usr/bin/env python3
"""Read `go test -json` on stdin, print a readable log, and hold each package
to a share of its `-timeout`.

Why this exists: a package's test time crept up release after release
(465s, 494s, 551s, then a 600s timeout kill) and nothing said so until CI
failed at random. `go test` kills the whole package binary at -timeout, so
the useful question is not "did the tests pass" but "how close to the
timeout did this package run". This script answers it on every run:

  * >= --warn  of the budget: a ::warning:: annotation (default 70%).
  * >= --fail  of the budget: a ::error:: annotation and exit 1 (default 90%),
    so the creep is fixed while the run is still green, not after a timeout.
  * always: the slowest packages and tests as a Markdown table on the job
    summary ($GITHUB_STEP_SUMMARY), so the next slow test is visible in review.

It also keeps the log short. `go test -json` is verbose by nature, so only
package results, failing tests' output and anything emitted outside a test
(panics, timeouts, build errors) are printed; the full stream is kept by the
caller (`tee`) for the artifact.

usage:
  go test ./... -json -timeout=10m | tee raw.json | \
      python3 scripts/ci/gotest_report.py --budget-seconds 600 --title "logs"

Pipe with `set -o pipefail` so a `go test` failure still fails the step.
"""
import argparse
import json
import os
import re
import sys

MAX_TEST_OUTPUT_LINES = 400
# `BenchmarkX-4   1000   1234 ns/op ...`: one finished benchmark.
BENCH_RESULT = re.compile(r"^(Benchmark\S+)\s+(\d+)\s+([\d.]+) ns/op")
# Lines benchstat needs around the results.
BENCH_HEADER = re.compile(r"^(goos|goarch|pkg|cpu): ")


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--budget-seconds", type=float, required=True,
                   help="the -timeout each package binary runs under, in seconds")
    p.add_argument("--warn", type=float, default=0.70,
                   help="warn at this share of the budget (default 0.70)")
    p.add_argument("--fail", type=float, default=0.90,
                   help="fail at this share of the budget (default 0.90)")
    p.add_argument("--top", type=int, default=15, help="slowest tests to list (default 15)")
    p.add_argument("--top-packages", type=int, default=8, help="slowest packages to list (default 8)")
    p.add_argument("--bench-out", default="",
                   help="write the benchmark result lines (benchstat input) to this file and list the "
                        "slowest benchmarks in the summary; for `go test -bench` runs")
    p.add_argument("--title", default="go test", help="heading for the job summary")
    p.add_argument("--summary", default=os.environ.get("GITHUB_STEP_SUMMARY", ""),
                   help="file to append the Markdown summary to (default $GITHUB_STEP_SUMMARY)")
    return p.parse_args(argv)


class Collector:
    def __init__(self):
        self.pkg_elapsed = {}      # package -> seconds
        self.pkg_status = {}       # package -> pass|fail|skip
        self.tests = []            # (elapsed, package, test)
        self.benchmarks = []       # (seconds spent, package, name, ns/op)
        self.bench_lines = []      # raw result/header lines, in order
        self.failed_tests = []     # (package, test)
        self._out = {}             # (package, test) -> [lines]

    def feed(self, ev, emit):
        action = ev.get("Action")
        pkg = ev.get("Package", "")
        test = ev.get("Test", "")
        if action == "output":
            text = ev.get("Output", "")
            m = BENCH_RESULT.match(text)
            if m:
                # A benchmark runs N iterations of ns/op each; N * ns/op is
                # the time it kept the runner busy.
                self.benchmarks.append((int(m.group(2)) * float(m.group(3)) / 1e9, pkg, m.group(1), float(m.group(3))))
                self.bench_lines.append(text)
                emit(text)
                return
            if BENCH_HEADER.match(text):
                self.bench_lines.append(text)
            if test:
                buf = self._out.setdefault((pkg, test), [])
                if len(buf) < MAX_TEST_OUTPUT_LINES:
                    buf.append(text)
            else:
                emit(text)
            return
        if action in ("pass", "fail", "skip"):
            if test:
                if action == "pass":
                    self.tests.append((ev.get("Elapsed", 0.0), pkg, test))
                elif action == "fail":
                    self.failed_tests.append((pkg, test))
                    # Only the leaf failure prints its output; a parent test
                    # repeats its children's lines.
                    emit("".join(self._out.get((pkg, test), [])))
                self._out.pop((pkg, test), None)
            else:
                self.pkg_elapsed[pkg] = ev.get("Elapsed", 0.0)
                self.pkg_status[pkg] = action


def short(pkg):
    return pkg.split("/internal/", 1)[-1] if "/internal/" in pkg else pkg.rsplit("/", 1)[-1]


def build_summary(c, args, verdicts):
    lines = [f"## Test time: {args.title}", ""]
    lines.append(f"Budget per package: `-timeout` = {args.budget_seconds:.0f}s "
                 f"(warn at {args.warn:.0%}, fail at {args.fail:.0%}).")
    lines.append("")
    lines.append("| package | seconds | of budget |")
    lines.append("|---|---:|---:|")
    ranked = sorted(c.pkg_elapsed.items(), key=lambda kv: -kv[1])[: args.top_packages]
    for pkg, el in ranked:
        lines.append(f"| `{short(pkg)}` | {el:.1f} | {el / args.budget_seconds:.0%} |")
    lines.append("")
    lines.append(f"Slowest {args.top} tests (subtests included):")
    lines.append("")
    lines.append("| seconds | package | test |")
    lines.append("|---:|---|---|")
    for el, pkg, test in sorted(c.tests, reverse=True)[: args.top]:
        lines.append(f"| {el:.1f} | `{short(pkg)}` | `{test}` |")
    if c.benchmarks:
        lines.append("")
        lines.append(f"Slowest {args.top} benchmarks (iterations x ns/op, {len(c.benchmarks)} ran):")
        lines.append("")
        lines.append("| seconds | package | benchmark | ns/op |")
        lines.append("|---:|---|---|---:|")
        for sec, pkg, name, nsop in sorted(c.benchmarks, reverse=True)[: args.top]:
            lines.append(f"| {sec:.1f} | `{short(pkg)}` | `{name}` | {nsop:,.0f} |")
    lines.append("")
    lines.append(f"Sum of package times: {sum(c.pkg_elapsed.values()):.0f}s over {len(c.pkg_elapsed)} packages.")
    if verdicts:
        lines.append("")
        lines.extend(verdicts)
    lines.append("")
    return "\n".join(lines) + "\n"


def main(argv=None, stdin=None, stdout=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    stdin = stdin if stdin is not None else sys.stdin
    stdout = stdout if stdout is not None else sys.stdout
    c = Collector()

    def emit(text):
        if text:
            stdout.write(text if text.endswith("\n") else text + "\n")

    for raw in stdin:
        raw = raw.rstrip("\n")
        if not raw:
            continue
        try:
            ev = json.loads(raw)
        except ValueError:
            emit(raw)  # not a test event (toolchain noise): show it
            continue
        if not isinstance(ev, dict):
            emit(raw)
            continue
        c.feed(ev, emit)

    verdicts = []
    exit_code = 0
    for pkg, el in sorted(c.pkg_elapsed.items(), key=lambda kv: -kv[1]):
        share = el / args.budget_seconds
        name = short(pkg)
        if share >= args.fail:
            msg = (f"{name} took {el:.0f}s = {share:.0%} of its {args.budget_seconds:.0f}s timeout "
                   f"(fail threshold {args.fail:.0%}). Make the slowest tests faster or split the package "
                   "before it starts timing out at random; see the table in the job summary.")
            stdout.write(f"::error title=Test time budget::{msg}\n")
            verdicts.append(f"**FAIL** {msg}")
            exit_code = 1
        elif share >= args.warn:
            msg = (f"{name} took {el:.0f}s = {share:.0%} of its {args.budget_seconds:.0f}s timeout "
                   f"(warn threshold {args.warn:.0%}).")
            stdout.write(f"::warning title=Test time budget::{msg}\n")
            verdicts.append(f"**WARN** {msg}")
    if c.failed_tests:
        exit_code = 1
    if any(s == "fail" for s in c.pkg_status.values()):
        exit_code = 1
    if not c.pkg_elapsed:
        stdout.write("::error title=Test time budget::no package results in the go test -json stream\n")
        exit_code = 1

    for pkg, el in sorted(c.pkg_elapsed.items(), key=lambda kv: -kv[1])[: args.top_packages]:
        stdout.write(f"{c.pkg_status.get(pkg, '?'):5s} {el:8.1f}s {el / args.budget_seconds:5.0%}  {short(pkg)}\n")

    if args.bench_out:
        with open(args.bench_out, "w", encoding="utf-8") as f:
            f.writelines(c.bench_lines)
    if args.summary:
        try:
            with open(args.summary, "a", encoding="utf-8") as f:
                f.write(build_summary(c, args, verdicts))
        except OSError as e:
            stdout.write(f"could not write job summary: {e}\n")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
