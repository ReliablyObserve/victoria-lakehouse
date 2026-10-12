#!/usr/bin/env python3
"""The data behind the `lh-pr-quality` comment: tests, coverage, registry, parity, fuzz targets and lint, base vs PR.

  quality.py collect --tree DIR --out snapshot.json [--cover root=cover.out --cover lakehouse-traces=cover.out]
                     [--parity-results go-test.json] [--lint golangci.json]
  quality.py report  --base base.json --pr pr.json [--out pr-quality.md] [--verdict verdict.json]

`collect` computes the cheap parts itself, from the tree alone: the number of Go tests and fuzz targets per module
(a static count of `func Test*` / `func Fuzz*`), and the conformance registry (rows per `expect`, features per status,
known differences and how many cite a GitHub issue). The heavy parts are read from files the CI jobs already produce
and are left out of the snapshot when not given: Go coverage profiles (`-coverprofile`, per module), the parity suite's
`go test -json` stream (parsed with scripts/ci/parity_ratchet.py: pass/fail counts and the known-failure allowlist with
its reasons) and a golangci-lint JSON report. `report` joins two snapshots and renders the sticky comment.

Gates (the check fails on): a module's coverage below the floor or down by more than 0.2 points, fewer Go tests than
the base, more registry rows `differ`/`unsupported` than the base without more rows overall, a parity failure outside
the allowlist, more lint findings than the base. A number that was not collected is shown as "not collected", never zero.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402
import known as knownmod  # noqa: E402

MARKER = "<!-- lh-pr-quality -->"
MODULES = {"root": "", "lakehouse-traces": "lakehouse-traces"}
COVER_FLOOR = 90.0
COVER_DROP = 0.2
SKIP_DIRS = {"deps", "node_modules", "vendor", "testdata"}  # and every dot-directory (.git, .claude, ...)
TEST_RE = re.compile(r"^func (Test\w+)\(", re.M)
FUZZ_RE = re.compile(r"^func (Fuzz\w+)\(", re.M)


def go_counts(tree: str) -> dict:
    out = {}
    for name, sub in MODULES.items():
        base = os.path.join(tree, sub) if sub else tree
        tests = fuzz = files = 0
        targets = []
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".") and not (not sub and d == "lakehouse-traces" and dirpath == base)]
            for fn in filenames:
                if not fn.endswith("_test.go"):
                    continue
                files += 1
                with open(os.path.join(dirpath, fn), encoding="utf-8", errors="replace") as f:
                    text = f.read()
                tests += len(TEST_RE.findall(text))
                f_hits = FUZZ_RE.findall(text)
                fuzz += len(f_hits)
                targets += [f"{os.path.relpath(dirpath, base)}:{x}" for x in f_hits]
        out[name] = {"tests": tests, "fuzz": fuzz, "test_files": files, "fuzz_targets": sorted(targets)}
    return out


def cover_percent(profile: str) -> float | None:
    """Statement coverage of a Go cover profile, in percent."""
    total = covered = 0
    try:
        with open(profile, encoding="utf-8") as f:
            for line in f:
                if line.startswith("mode:"):
                    continue
                m = re.match(r"^.+:\d+\.\d+,\d+\.\d+ (\d+) (\d+)$", line.strip())
                if m:
                    total += int(m.group(1))
                    if int(m.group(2)) > 0:
                        covered += int(m.group(1))
    except OSError:
        return None
    return round(covered / total * 100, 2) if total else None


def registry_counts(tree: str) -> dict:
    root = os.path.join(tree, "tests", "conformance", "registry")
    rows, feats = abreg.load_rows(root), abreg.load_features(root)
    by_expect: dict[str, int] = {}
    for r in rows:
        by_expect[r["expect"]] = by_expect.get(r["expect"], 0) + 1
    by_status: dict[str, int] = {}
    for f in feats:
        by_status[f["status"]] = by_status.get(f["status"], 0) + 1
    gaps = abreg.gaps_doc_issues(tree)
    stats = knownmod.registry_gap_stats(rows, gaps)
    stats.pop("without_issue_ids", None)
    return {"rows": len(rows), "by_expect": by_expect, "features": len(feats), "feature_status": by_status, "pending_rows": sum(1 for r in rows if r.get("pending")),
            "known_differences": stats}


def parity_summary(results_path: str, tree: str) -> dict:
    """Pass/fail counts of the parity suite and its known failures, via scripts/ci/parity_ratchet.py."""
    sys.path.insert(0, abreg.ROOT)
    from scripts.ci import parity_ratchet as pr  # noqa: PLC0415
    with open(results_path, encoding="utf-8") as f:
        run = pr.parse_go_test_json(f.read().splitlines())
    allow_path = os.path.join(tree, "tests", "parity", "known_failures.txt")
    allow = pr.Allowlist()
    if os.path.exists(allow_path):
        with open(allow_path, encoding="utf-8") as f:
            allow = pr.parse_allowlist(f.read())
    unexpected = pr.unexpected_failures(run.results, set(allow.entries))
    return {"counts": pr.count_by_action(run.results), "known_failures": len(allow.entries),
            "known": [{"test": t, "reason": r[:140]} for t, r in sorted(allow.entries.items())][:60],
            "unexpected": [f"{p}:{t}" for p, t in unexpected][:20], "aborted": len(run.aborted()), "min_pass": allow.min_pass}


def lint_count(path: str) -> int | None:
    try:
        with open(path, encoding="utf-8") as f:
            return len(json.load(f).get("Issues") or [])
    except (OSError, ValueError):
        return None


def collect(tree: str, cover: dict[str, str] | None = None, parity: str = "", lint: str = "") -> dict:
    snap = {"go": go_counts(tree), "registry": registry_counts(tree), "coverage": {}, "parity": None, "lint": None}
    for module, profile in (cover or {}).items():
        snap["coverage"][module] = cover_percent(profile)
    if parity:
        snap["parity"] = parity_summary(parity, tree)
    if lint:
        snap["lint"] = lint_count(lint)
    return snap


# ------------------------------------------------------------------------------------------------ report
def delta(b, p, digits=0) -> str:
    if b is None or p is None:
        return "not collected"
    d = p - b
    f = f"{{:+.{digits}f}}"
    return f.format(d) if d else "0"


def gates(base: dict, pr: dict) -> list[str]:
    fails = []
    for m in MODULES:
        cb, cp = (base["coverage"] or {}).get(m), (pr["coverage"] or {}).get(m)
        if cp is not None and cp < COVER_FLOOR:
            fails.append(f"coverage of {m} is {cp}%, below the {COVER_FLOOR:g}% floor")
        elif cb is not None and cp is not None and cp < cb - COVER_DROP:
            fails.append(f"coverage of {m} fell from {cb}% to {cp}%")
        if pr["go"][m]["tests"] < base["go"][m]["tests"]:
            fails.append(f"{m} has {base['go'][m]['tests'] - pr['go'][m]['tests']} fewer Go tests than the base")
    be, pe = base["registry"]["by_expect"], pr["registry"]["by_expect"]
    weakened = sum(pe.get(k, 0) - be.get(k, 0) for k in ("differ", "unsupported", "absent"))
    if weakened > 0 and pr["registry"]["rows"] - base["registry"]["rows"] < weakened:
        fails.append(f"{weakened} more registry rows record a difference (differ, unsupported, absent) without new rows: a weakened row needs owner review")
    if pr["parity"] and pr["parity"]["unexpected"]:
        fails.append(f"{len(pr['parity']['unexpected'])} parity failure(s) outside the allowlist")
    if base["lint"] is not None and pr["lint"] is not None and pr["lint"] > base["lint"]:
        fails.append(f"{pr['lint'] - base['lint']} new lint finding(s)")
    return fails


def render(base: dict, pr: dict, perf_pointer: str = "") -> tuple[str, dict]:
    fails = gates(base, pr)
    lines = [MARKER, f"### PR quality report: {'RED, ' + str(len(fails)) + ' gate(s) failed' if fails else 'GREEN, no gate failed'}", ""]
    lines += [f"- **{f}**" for f in fails] + ([""] if fails else [])
    lines += ["| module | Go tests | coverage | fuzz targets |", "|---|--:|--:|--:|"]
    for m in MODULES:
        cb, cp = (base["coverage"] or {}).get(m), (pr["coverage"] or {}).get(m)
        cov = "not collected" if cp is None else f"{cp}% ({delta(cb, cp, 2)})"
        lines.append(f"| {m} | {pr['go'][m]['tests']} ({delta(base['go'][m]['tests'], pr['go'][m]['tests'])}) | {cov} | "
                     f"{pr['go'][m]['fuzz']} ({delta(base['go'][m]['fuzz'], pr['go'][m]['fuzz'])}) |")
    rb, rp = base["registry"], pr["registry"]
    lines += ["", "**Conformance registry**", "", "| | base | PR | change |", "|---|--:|--:|--:|", f"| rows | {rb['rows']} | {rp['rows']} | {delta(rb['rows'], rp['rows'])} |"]
    for k in sorted(set(rb["by_expect"]) | set(rp["by_expect"])):
        lines.append(f"| rows `expect: {k}` | {rb['by_expect'].get(k, 0)} | {rp['by_expect'].get(k, 0)} | {delta(rb['by_expect'].get(k, 0), rp['by_expect'].get(k, 0))} |")
    for k in sorted(set(rb["feature_status"]) | set(rp["feature_status"])):
        lines.append(f"| features `{k}` | {rb['feature_status'].get(k, 0)} | {rp['feature_status'].get(k, 0)} | "
                     f"{delta(rb['feature_status'].get(k, 0), rp['feature_status'].get(k, 0))} |")
    kd = rp["known_differences"]
    lines += ["", f"Known differences recorded: {kd['rows']} rows, {kd['with_issue']} cite a GitHub issue, {kd['without_issue']} do not "
              f"(base: {rb['known_differences']['rows']} / {rb['known_differences']['with_issue']} / {rb['known_differences']['without_issue']})."]
    if pr["parity"]:
        pp, bp = pr["parity"], base["parity"]
        c = pp["counts"]
        lines += ["", f"**Parity suite**: {', '.join(f'{n} {k}' for k, n in sorted(c.items()))}; {pp['known_failures']} known failure(s) allowlisted"
                  + (f" (base {bp['known_failures']})" if bp else "") + "."]
        for e in pp["known"][:10]:
            lines.append(f"- known: `{e['test']}` {e['reason']}".rstrip())
    else:
        lines += ["", "**Parity suite**: not collected in this run."]
    lines += ["", f"**Lint**: {'not collected' if pr['lint'] is None else str(pr['lint']) + ' finding(s)' + (' (' + delta(base['lint'], pr['lint']) + ')' if base['lint'] is not None else '')}."]
    if perf_pointer:
        lines += ["", perf_pointer]
    lines += ["", "<sub>Static counts come from the tree; coverage, parity and lint come from the CI jobs' own output. Reproduce: `docs/pr-perf-smoke.md`.</sub>"]
    return "\n".join(lines) + "\n", {"failed": bool(fails), "exit": 1 if fails else 0, "gates": fails}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--tree", default=abreg.ROOT)
    c.add_argument("--out", required=True)
    c.add_argument("--cover", action="append", default=[], metavar="MODULE=PROFILE")
    c.add_argument("--parity-results", default="")
    c.add_argument("--lint", default="")
    r = sub.add_parser("report")
    r.add_argument("--base", required=True)
    r.add_argument("--pr", required=True)
    r.add_argument("--out", default="")
    r.add_argument("--verdict", default="")
    r.add_argument("--perf-pointer", default="")
    a = ap.parse_args(argv)
    if a.cmd == "collect":
        snap = collect(a.tree, dict(x.split("=", 1) for x in a.cover), a.parity_results, a.lint)
        abreg.dump_json(a.out, snap)
        print(json.dumps({k: snap[k] for k in ("coverage", "lint")} | {"go": {m: {"tests": v["tests"], "fuzz": v["fuzz"]} for m, v in snap["go"].items()}}))
        return 0
    text, verdict = render(abreg.load_json(a.base), abreg.load_json(a.pr), a.perf_pointer)
    if a.out:
        abreg.write_text(a.out, text)
    if a.verdict:
        abreg.dump_json(a.verdict, verdict)
    print(text)
    return verdict["exit"]


if __name__ == "__main__":
    sys.exit(main())
