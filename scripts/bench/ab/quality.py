#!/usr/bin/env python3
"""The data behind the `lh-pr-quality` comment and the `pr-quality` artifact: tests, coverage, registry, parity, fuzz targets, lint, flaky
tests and test time, base vs PR.

  quality.py packages --base-ref SHA [--head-ref HEAD] --module root|lakehouse-traces [--files-from FILE]   # the Go packages to measure
  quality.py collect  --tree DIR --out snapshot.json [--gotest MODULE=go-test.json --module-path MODULE=IMPORT/PATH]...
                      [--parity-results go-test.json] [--lint MODULE=golangci.json]...
  quality.py flaky    --repo OWNER/NAME --sha SHA --out flaky.json              # needs `gh` and a token with actions: read
  quality.py bundle   --base base.json --pr pr.json --out-dir DIR --pr-number N --head-sha S --base-sha S --merge-base-sha S
                      --run-id ID --run-attempt N [--changed changed.json] [--flaky flaky.json] [--note TEXT]...
  quality.py verify   DIR                                                   # validate a downloaded pr-quality artifact
  quality.py report   --base base.json --pr pr.json [--out pr-quality.md] [--verdict verdict.json]

`collect` computes the cheap parts from the tree alone: the number of Go tests and fuzz targets per module (a static count of
`func Test*` / `func Fuzz*`), and the conformance registry (rows per `expect`, features per status, pending rows, known differences and how
many cite a GitHub issue). The measured parts come from files the CI job produced and are left out of the snapshot when not given: the
`go test -cover -json` stream of the changed packages (coverage and test time per package, see quality_inputs.py), the parity suite's
`go test -json` stream (parsed with scripts/ci/parity_ratchet.py; without it the parity section shows the allowlist of the tree only)
and a golangci-lint JSON report of the findings new since the merge base. `bundle` joins two snapshots into the artifact (quality_bundle.py).

Gates (the check fails on): a measured package under internal/ below the 90 % floor (the same exclusions as the CI coverage step) or down by
more than 0.2 points, fewer Go tests than the base, more registry rows `differ`/`unsupported`/`absent` than the base without more rows overall,
a parity failure outside the allowlist, a new lint finding. A number that was not collected is shown as "not collected", never zero.
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402
import known as knownmod  # noqa: E402
import quality_bundle as qb  # noqa: E402
import quality_inputs as qi  # noqa: E402

MARKER = "<!-- lh-pr-quality -->"
MODULES = {"root": "", "lakehouse-traces": "lakehouse-traces"}
COVER_FLOOR = 90.0
COVER_DROP = 0.2
# Packages below internal/ that the CI coverage step does not hold to the floor (the `-x` arguments of scripts/ci/check_coverage.sh in
# .github/workflows/ci.yaml, per module). test_quality.py fails when this table and ci.yaml differ.
COVER_EXCLUDE = {"root": ("testutil", "testutil/*"), "lakehouse-traces": ("testutil", "testutil/*", "vtstorage_adapter", "selectapi")}
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


def floor_applies(module: str, rel: str) -> bool:
    """check_coverage.sh holds a package below internal/ to the floor, except the excluded ones (matched as shell patterns)."""
    if not rel.startswith("internal/"):
        return False
    below = rel[len("internal/"):]
    return not any(fnmatch.fnmatchcase(below, pat) for pat in COVER_EXCLUDE[module])


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


def _allowlist(tree: str):
    sys.path.insert(0, abreg.ROOT)
    from scripts.ci import parity_ratchet as pr  # noqa: PLC0415
    allow_path = os.path.join(tree, "tests", "parity", "known_failures.txt")
    allow = pr.Allowlist()
    if os.path.exists(allow_path):
        with open(allow_path, encoding="utf-8") as f:
            allow = pr.parse_allowlist(f.read())
    return pr, allow


def _known(allow) -> list[dict]:
    return [{"test": t[:300], "reason": r[:300], "issues": abreg.issue_refs(r)[:10]} for t, r in sorted(allow.entries.items())][:60]


def parity_summary(results_path: str, tree: str) -> dict:
    """Pass/fail counts of the parity suite and its known failures, via scripts/ci/parity_ratchet.py."""
    pr, allow = _allowlist(tree)
    with open(results_path, encoding="utf-8") as f:
        run = pr.parse_go_test_json(f.read().splitlines())
    unexpected = pr.unexpected_failures(run.results, set(allow.entries))
    by_signal = {(sig or "unattributed"): c for sig, c in pr.counts_by_signal(run.results).items()}
    return {"source": "run", "counts": pr.count_by_action(run.results), "by_signal": by_signal, "known_failures": len(allow.entries),
            "known": _known(allow), "unexpected": [f"{p}:{t}"[:400] for p, t in unexpected][:20], "aborted": len(run.aborted()), "min_pass": allow.min_pass}


def parity_static(tree: str) -> dict:
    """The parity allowlist of a tree (known failures with their issue links and the pass floor), when no run of the suite was available."""
    _pr, allow = _allowlist(tree)
    return {"source": "static", "counts": {}, "by_signal": {}, "known_failures": len(allow.entries), "known": _known(allow), "unexpected": [],
            "aborted": 0, "min_pass": allow.min_pass}


def collect(tree: str, gotest: dict[str, str] | None = None, module_paths: dict[str, str] | None = None, parity: str = "",
            lint: dict[str, str] | None = None) -> dict:
    snap = {"go": go_counts(tree), "registry": registry_counts(tree), "coverage": {}, "timing": {}, "tests_failed": {},
            "parity": parity_summary(parity, tree) if parity else parity_static(tree), "lint": None}
    for module, path in (gotest or {}).items():
        with open(path, encoding="utf-8", errors="replace") as f:
            got = qi.parse_gotest(f, (module_paths or {}).get(module, ""))
        snap["coverage"][module] = got["coverage"]
        snap["timing"][module] = {p: {"seconds": s, "status": got["status"].get(p, "pass")} for p, s in got["seconds"].items()}
        if got["failed_tests"]:
            snap["tests_failed"][module] = got["failed_tests"]
    if lint:
        snap["lint"] = {}
        for module, path in lint.items():
            with open(path, encoding="utf-8") as f:
                snap["lint"][module] = qi.parse_lint(f.read())
    return snap


# ------------------------------------------------------------------------------------------------ report
def delta(b, p, digits=0) -> str:
    if b is None or p is None:
        return "not collected"
    d = p - b
    f = f"{{:+.{digits}f}}"
    return f.format(d) if d else "0"


def lint_total(snap: dict) -> int | None:
    return None if snap.get("lint") is None else sum(v["new"] for v in snap["lint"].values())


def gates(base: dict, pr: dict) -> list[str]:
    fails = []
    for m in MODULES:
        cb, cp = (base["coverage"] or {}).get(m, {}), (pr["coverage"] or {}).get(m, {})
        for rel, p in sorted(cp.items()):
            b = cb.get(rel)
            if floor_applies(m, rel) and p < COVER_FLOOR:
                fails.append(f"coverage of {m}/{rel} is {p}%, below the {COVER_FLOOR:g}% floor")
            elif b is not None and p < b - COVER_DROP:
                fails.append(f"coverage of {m}/{rel} fell from {b}% to {p}%")
        if pr["go"][m]["tests"] < base["go"][m]["tests"]:
            fails.append(f"{m} has {base['go'][m]['tests'] - pr['go'][m]['tests']} fewer Go tests than the base")
    be, pe = base["registry"]["by_expect"], pr["registry"]["by_expect"]
    weakened = sum(pe.get(k, 0) - be.get(k, 0) for k in ("differ", "unsupported", "absent"))
    if weakened > 0 and pr["registry"]["rows"] - base["registry"]["rows"] < weakened:
        fails.append(f"{weakened} more registry rows record a difference (differ, unsupported, absent) without new rows: a weakened row needs owner review")
    if pr["parity"] and pr["parity"]["unexpected"]:
        fails.append(f"{len(pr['parity']['unexpected'])} parity failure(s) outside the allowlist")
    n = lint_total(pr)
    if n:
        fails.append(f"{n} new lint findings since the merge base")
    return fails


def issue_link(ref: str) -> str:
    """`repo#123` as a link to that issue of the organisation (the schema pattern guarantees the shape; anything else is shown as text)."""
    m = re.fullmatch(r"([A-Za-z0-9_.-]+)#([0-9]+)", ref)
    return f"[{ref}](https://github.com/{abreg.ORG}/{m.group(1)}/issues/{m.group(2)})" if m else qb.md_code(ref)


def _cover_rows(base: dict, pr: dict) -> tuple[list[str], int]:
    """(table rows that changed, regressed or fall below the floor, count of the unchanged ones)."""
    rows, same = [], 0
    for m in MODULES:
        cb, cp = (base["coverage"] or {}).get(m, {}), (pr["coverage"] or {}).get(m, {})
        for rel, p in sorted(cp.items(), key=lambda kv: (kv[1] - (cb.get(kv[0]) if cb.get(kv[0]) is not None else kv[1]), kv[0])):
            b = cb.get(rel)
            low = floor_applies(m, rel) and p < COVER_FLOOR
            if b is not None and abs(p - b) < 0.05 and not low:
                same += 1
                continue
            flag = " **below the floor**" if low else (" **dropped**" if b is not None and p < b - COVER_DROP else (" new package" if b is None else ""))
            rows.append(f"| {qb.md_code(m + '/' + rel, 120)} | {'-' if b is None else str(b) + '%'} | {p}% | {delta(b, p, 2) if b is not None else '-'}{flag} |")
    return rows, same


def _time_rows(pr: dict, base: dict) -> list[str]:
    rows = []
    for m in MODULES:
        tp, tb = (pr["timing"] or {}).get(m, {}), (base["timing"] or {}).get(m, {})
        for rel, v in sorted(tp.items(), key=lambda kv: -kv[1]["seconds"])[:8]:
            b = tb.get(rel)
            share = v["seconds"] / qi.CI_BUDGET_SECONDS
            warn = " **WARN**" if share >= qi.BUDGET_FAIL else (" warn" if share >= qi.BUDGET_WARN else "")
            rows.append(f"| {qb.md_code(m + '/' + rel, 120)} | {'-' if not b else format(b['seconds'], '.1f')} | {v['seconds']:.1f} | {share:.0%}{warn} |")
    return rows


def render(base: dict, pr: dict, changed: dict | None = None, flaky: dict | None = None, notes: list[str] | None = None,
           perf_pointer: str = "") -> tuple[str, dict]:
    fails = gates(base, pr)
    S, C = qb.md_safe, qb.md_code
    lines = [MARKER, f"### PR quality: {'RED, ' + str(len(fails)) + ' gate(s) failed' if fails else 'GREEN, no gate failed'}", ""]
    lines += [f"- **{S(f, 300)}**" for f in fails] + ([""] if fails else [])
    lines += ["| module | Go tests | fuzz targets | new lint findings |", "|---|--:|--:|--:|"]
    for m in MODULES:
        ln = (pr["lint"] or {}).get(m) if pr["lint"] is not None else None
        lines.append(f"| {m} | {pr['go'][m]['tests']} ({delta(base['go'][m]['tests'], pr['go'][m]['tests'])}) | "
                     f"{pr['go'][m]['fuzz']} ({delta(base['go'][m]['fuzz'], pr['go'][m]['fuzz'])}) | {'not collected' if ln is None else ln['new']} |")
    # coverage: only what changed, regressed or sits below the floor
    measured = {m: (changed or {}).get(m, {}).get("measured", []) for m in MODULES}
    left = {m: (changed or {}).get(m, {}).get("left_out", []) for m in MODULES}
    if not any((pr["coverage"] or {}).get(m) for m in MODULES):
        lines += ["", "**Coverage**: not collected" + (" (no Go package changed)." if changed is not None and not any(measured.values()) else ".")]
    else:
        crows, same = _cover_rows(base, pr)
        lines += ["", f"**Coverage** of the {sum(len(v) for v in measured.values())} changed package(s) ({COVER_FLOOR:g}% floor, the CI step's exclusions): "
                  f"{len(crows)} changed, {same} unchanged."]
        if crows:
            lines += ["", "| package | base | PR | change |", "|---|--:|--:|--:|"] + crows
    for m in MODULES:
        if left[m]:
            lines += ["", f"Not measured in {m} (over the cap of {qi.MAX_PACKAGES} packages): " + ", ".join(C(x, 80) for x in left[m][:15]) + (" ..." if len(left[m]) > 15 else "")]
        if (pr["tests_failed"] or {}).get(m):
            lines += ["", f"Failing tests in the coverage run of {m} (the CI test jobs are the gate; coverage of that package is unreliable): "
                      + ", ".join(C(x, 100) for x in pr["tests_failed"][m][:8])]
    # registry
    rb, rp = base["registry"], pr["registry"]
    keys_e = sorted(set(rb["by_expect"]) | set(rp["by_expect"]))
    moved = [("rows", rb["rows"], rp["rows"]), ("pending rows", rb["pending_rows"], rp["pending_rows"]), ("features", rb["features"], rp["features"])]
    moved += [(f"rows `expect: {k}`", rb["by_expect"].get(k, 0), rp["by_expect"].get(k, 0)) for k in keys_e]
    moved += [(f"features `{k}`", rb["feature_status"].get(k, 0), rp["feature_status"].get(k, 0)) for k in sorted(set(rb["feature_status"]) | set(rp["feature_status"]))]
    kb, kp = rb["known_differences"], rp["known_differences"]
    moved += [(f"known differences: {k.replace('_', ' ')}", kb.get(k, 0), kp.get(k, 0)) for k in sorted(set(kb) | set(kp))]
    diff = [x for x in moved if x[1] != x[2]]
    lines += ["", f"**Conformance registry**: {rp['rows']} rows ({rp['pending_rows']} pending; " + ", ".join(f"{k} {rp['by_expect'].get(k, 0)}" for k in keys_e)
              + f"), {rp['features']} features, {kp.get('rows', 0)} known-difference rows of which {kp.get('with_issue', 0)} cite an issue; "
              + ("unchanged." if not diff else f"{len(diff)} figure(s) changed.")]
    if diff:
        lines += ["", "| | base | PR | change |", "|---|--:|--:|--:|"] + [f"| {n} | {b} | {p} | {delta(b, p)} |" for n, b, p in diff]
    # parity
    pp, bp = pr["parity"], base["parity"]
    if pp and pp["source"] == "run":
        c = pp["counts"]
        lines += ["", f"**Parity suite**: {', '.join(f'{n} {k}' for k, n in sorted(c.items()))}; {pp['known_failures']} known failure(s) allowlisted"
                  + (f" (base {bp['known_failures']})" if bp else "") + "."]
        if pp.get("by_signal"):
            lines += ["", "| Parity results | pass | fail | skip |", "| --- | --- | --- | --- |"]
            for sig in ("logs", "traces", "unattributed"):
                c = pp["by_signal"].get(sig, {})
                lines.append(f"| {sig} | {c.get('pass', 0)} | {c.get('fail', 0)} | {c.get('skip', 0)} |")
    elif pp:
        lines += ["", f"**Parity suite**: pass/fail counts not collected (the parity run had not finished when this report was computed); allowlist of the tree: "
                  f"{pp['known_failures']} known failure(s)" + (f" (base {bp['known_failures']})" if bp else "") + (f", pass floor {pp['min_pass']}" if pp["min_pass"] is not None else "") + "."]
    else:
        lines += ["", "**Parity suite**: not collected."]
    if pp:
        for e in pp["known"][:10]:
            lines.append(f"- known: {C(e['test'], 100)} {S(e['reason'], 140)} " + " ".join(issue_link(i) for i in e["issues"]))
    # lint
    if pr["lint"] is not None:
        findings = [(m, f) for m in MODULES for f in (pr["lint"].get(m) or {}).get("findings", [])]
        if findings:
            lines += ["", "**New lint findings** (since the merge base):", ""] + [f"- {C(m + '/' + f['file'] + ':' + str(f['line']), 140)} {C(f['linter'], 40)} {S(f['text'], 160)}" for m, f in findings[:15]]
            if lint_total(pr) > 15:
                lines.append(f"- ... {lint_total(pr) - 15} more")
        else:
            lines += ["", "**Lint**: no new finding since the merge base (changed packages only)."]
    else:
        lines += ["", "**Lint**: not collected."]
    # flaky-test watch
    if flaky is None:
        lines += ["", "**Flaky-test watch**: not collected."]
    elif not flaky["items"]:
        lines += ["", f"**Flaky-test watch**: none ({flaky['runs_seen']} workflow run(s) of this head seen, {flaky['reran_runs']} re-run)."
                  + (" A job can show up here only after a re-run of its workflow." if not flaky["reran_runs"] else "")]
    else:
        lines += ["", "**Flaky-test watch**: failed in one attempt, passed in a later one (a finding, not noise):", ""]
        for it in flaky["items"][:15]:
            names = (": " + ", ".join(C(t, 80) for t in it["tests"][:5])) if it["tests"] else ""
            lines.append(f"- {S(it['workflow'], 60)} / {C(it['job'], 80)} attempt {it['failed_attempt']} failed, attempt {it['passed_attempt']} passed{names}")
    # time budget, collapsed
    trows = _time_rows(pr, base)
    if trows:
        lines += ["", "<details><summary>Test time of the changed packages (no -race: a lower bound of the CI time)</summary>", "",
                  f"Share of the {qi.CI_BUDGET_SECONDS:.0f}s `-timeout` per package that the CI test jobs hold (warn at {qi.BUDGET_WARN:.0%}).", "",
                  "| package | base s | PR s | of budget |", "|---|--:|--:|--:|"] + trows + ["", "</details>"]
    for n in notes or []:
        lines += ["", f"_{S(n, 300)}_"]
    if perf_pointer:
        lines += ["", perf_pointer]
    lines += ["", "<sub>Static counts come from the trees; coverage, test time and lint come from the changed packages of the PR vs its merge base. Reproduce: `docs/pr-perf-smoke.md`.</sub>"]
    return qb.cap_body("\n".join(lines) + "\n"), {"failed": bool(fails), "exit": 1 if fails else 0, "gates": fails}


def build_doc(base: dict, pr: dict, changed: dict | None = None, flaky: dict | None = None, notes: list[str] | None = None) -> dict:
    _text, verdict = render(base, pr, changed, flaky, notes)
    mods = {m: {"measured": [], "left_out": []} for m in MODULES}
    for m, v in (changed or {}).items():
        mods[m] = {"measured": list(v.get("measured", [])), "left_out": list(v.get("left_out", []))}
    doc = {"schema_version": qb.SCHEMA_VERSION, "base": base, "pr": pr, "changed": mods, "flaky": flaky, "verdict": verdict}
    if notes:
        doc["notes"] = list(notes)
    return doc


def render_doc(doc: dict) -> tuple[str, dict]:
    """The comment body of a (validated) quality.json: what the trusted consumer calls."""
    return render(doc["base"], doc["pr"], doc["changed"], doc["flaky"], doc.get("notes"))


def git_changed_files(base_ref: str, head_ref: str) -> list[str]:
    out = subprocess.run(["git", "diff", "--no-renames", "--name-only", "-z", base_ref, head_ref], cwd=abreg.ROOT, check=True, capture_output=True).stdout
    return [f for f in out.decode("utf-8", "replace").split("\0") if f]


def run_gh(path: str, jq: str | None = None) -> str:
    cmd = ["gh", "api", path] + (["--paginate", "--jq", jq] if jq else [])
    return subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120).stdout


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    k = sub.add_parser("packages")
    k.add_argument("--base-ref", default="", help="the merge base (required unless --files-from)")
    k.add_argument("--head-ref", default="HEAD")
    k.add_argument("--module", required=True, choices=sorted(MODULES))
    k.add_argument("--files-from", default="", help="read the changed files (one per line) instead of asking git")
    k.add_argument("--exists-in", default="", help="keep only packages that exist as Go packages in this tree")
    k.add_argument("--group", choices=("short", "full"), default="", help="print only the packages whose coverage run is -short (or is not): the flags of the CI coverage step")
    k.add_argument("--json", action="store_true")
    c = sub.add_parser("collect")
    c.add_argument("--tree", default=abreg.ROOT)
    c.add_argument("--out", required=True)
    c.add_argument("--gotest", action="append", default=[], metavar="MODULE=GOTEST_JSON")
    c.add_argument("--module-path", action="append", default=[], metavar="MODULE=IMPORT_PATH")
    c.add_argument("--parity-results", default="")
    c.add_argument("--lint", action="append", default=[], metavar="MODULE=GOLANGCI_JSON")
    f = sub.add_parser("flaky")
    f.add_argument("--repo", required=True)
    f.add_argument("--sha", required=True)
    f.add_argument("--out", required=True)
    b = sub.add_parser("bundle")
    b.add_argument("--base", required=True)
    b.add_argument("--pr", required=True)
    b.add_argument("--out-dir", required=True)
    b.add_argument("--pr-number", type=int, required=True)
    b.add_argument("--head-sha", required=True)
    b.add_argument("--base-sha", required=True)
    b.add_argument("--merge-base-sha", required=True)
    b.add_argument("--run-id", required=True)
    b.add_argument("--run-attempt", type=int, required=True)
    b.add_argument("--changed", default="")
    b.add_argument("--flaky", default="")
    b.add_argument("--note", action="append", default=[])
    v = sub.add_parser("verify")
    v.add_argument("dir")
    r = sub.add_parser("report")
    r.add_argument("--base", required=True)
    r.add_argument("--pr", required=True)
    r.add_argument("--out", default="")
    r.add_argument("--verdict", default="")
    r.add_argument("--perf-pointer", default="")
    a = ap.parse_args(argv)
    if a.cmd == "packages":
        if a.files_from:
            files = [x for x in open(a.files_from, encoding="utf-8").read().splitlines() if x]
        elif a.base_ref:
            files = git_changed_files(a.base_ref, a.head_ref)
        else:
            ap.error("packages needs --base-ref or --files-from")
        measured, left = qi.changed_packages(files, a.module)
        if a.exists_in:
            measured = [p for p in measured if qi.has_go_files(a.exists_in, a.module, p)]
        gomod = [x for x in files if x.endswith(("go.mod", "go.sum")) and (x.startswith("lakehouse-traces/") == (a.module == "lakehouse-traces"))]
        out = {"measured": measured, "left_out": left, "dependency_change": bool(gomod)}
        if a.group:
            print("".join(p + "\n" for p in measured if (qi.go_test_flags(p) == ["-short"]) == (a.group == "short")), end="")
        elif a.json:
            print(json.dumps(out))
        else:
            print("".join(p + "\n" for p in measured), end="")
        return 0
    if a.cmd == "collect":
        snap = collect(a.tree, dict(x.split("=", 1) for x in a.gotest), dict(x.split("=", 1) for x in a.module_path), a.parity_results,
                       dict(x.split("=", 1) for x in a.lint))
        abreg.dump_json(a.out, snap)
        print(json.dumps({"coverage": {m: len(v) for m, v in snap["coverage"].items()}, "lint": lint_total(snap)}
                         | {"go": {m: {"tests": v["tests"], "fuzz": v["fuzz"]} for m, v in snap["go"].items()}}))
        return 0
    if a.cmd == "flaky":
        try:
            res = qi.collect_flaky(a.repo, a.sha, run_gh)
        except Exception as e:  # noqa: BLE001 - the watch is best effort: no token scope, API error
            print(f"flaky-test watch not collected: {e}", file=sys.stderr)
            res = None
        abreg.dump_json(a.out, res)
        return 0
    if a.cmd == "bundle":
        base, pr = abreg.load_json(a.base), abreg.load_json(a.pr)
        changed = abreg.load_json(a.changed) if a.changed else {}
        notes = list(a.note) + [f"go.mod or go.sum of {m} changed: every package of it is affected, coverage is measured only for the packages with changed files."
                                for m, v in sorted(changed.items()) if m in MODULES and v.get("dependency_change")]
        flaky = abreg.load_json(a.flaky) if a.flaky and os.path.exists(a.flaky) else None
        doc = build_doc(base, pr, {m: v for m, v in changed.items() if m in MODULES}, flaky, notes)
        errs = qb.validate_quality(doc)
        if errs:
            print("quality.json does not match its schema:\n  " + "\n  ".join(errs[:20]), file=sys.stderr)
            return 2
        text, _v = render_doc(doc)
        abreg.dump_json(os.path.join(a.out_dir, qb.QUALITY_FILE), doc)
        abreg.write_text(os.path.join(a.out_dir, qb.REPORT_FILE), text)
        manifest = qb.manifest_for(a.out_dir, a.pr_number, a.head_sha, a.base_sha, a.merge_base_sha, a.run_id, a.run_attempt, doc["verdict"])
        errs = qb.validate_manifest(manifest)
        if errs:
            print("manifest does not match its schema:\n  " + "\n  ".join(errs), file=sys.stderr)
            return 2
        abreg.dump_json(os.path.join(a.out_dir, qb.MANIFEST_FILE), manifest)
        problems = qb.verify_bundle(a.out_dir)
        if problems:
            print("the artifact does not verify:\n  " + "\n  ".join(problems), file=sys.stderr)
            return 2
        print(text)
        return doc["verdict"]["exit"]
    if a.cmd == "verify":
        problems = qb.verify_bundle(a.dir)
        print("\n".join(problems) if problems else "ok")
        return 1 if problems else 0
    base, pr = abreg.load_json(a.base), abreg.load_json(a.pr)
    text, verdict = render(base, pr, None, None, None, a.perf_pointer)
    if a.out:
        abreg.write_text(a.out, text)
    if a.verdict:
        abreg.dump_json(a.verdict, verdict)
    print(text)
    return verdict["exit"]


if __name__ == "__main__":
    sys.exit(main())
