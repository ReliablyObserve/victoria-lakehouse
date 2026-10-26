"""Tool output that feeds the `lh-pr-quality` report: `go test -json`, golangci-lint JSON, the packages a PR changes and the
flaky-test watch over the workflow attempts of the PR's head.

Everything here is a pure parser over text (or takes an injectable `gh` callable), so it is unit-tested on fixtures.

  * `parse_gotest`: coverage per package and test time per package, from the `go test -cover -json` stream. The coverage figure
    is the one `go test -cover` prints, the same line scripts/ci/check_coverage.sh reads in the CI test jobs; package time and
    failures come from scripts/ci/gotest_report.py's Collector (not re-implemented).
  * `changed_packages`: the Go packages a diff touches, per module, capped so the informational job stays cheap.
  * `parse_lint`: new findings of `golangci-lint run --new-from-rev=<merge base> --output.json.path=...`.
  * `collect_flaky`: a job that failed in one attempt of a workflow run and passed in a later attempt of the same run.
"""
from __future__ import annotations

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from scripts.ci import gotest_report as gtr  # noqa: E402  (Collector: package time, failed tests)

COVER_RE = re.compile(r"coverage: ([0-9][0-9.]*)% of statements")
# A package whose tests need more than this are not run twice by an informational job: the CI test jobs measure it.
MAX_PACKAGES = 12
# The per-package `-timeout` of the CI test jobs (ci.yaml); the share of it a package used is what gotest_report.py warns on.
CI_BUDGET_SECONDS = 600.0
BUDGET_WARN = 0.70
BUDGET_FAIL = 0.90
# Packages whose CI coverage run is not -short (ci.yaml: SHARD_SHORT=0 for compaction); every other package runs -short.
NOT_SHORT = ("internal/compaction",)
# module -> (prefix of its files in the repository, directories whose packages are measured)
MODULE_DIRS = {"root": ("", ("internal/", "cmd/")), "lakehouse-traces": ("lakehouse-traces/", ("internal/",))}
LINT_MAX_FINDINGS = 40


def rel_pkg(pkg: str, module_path: str) -> str:
    """Import path -> directory relative to its module (`.` for the module's root package)."""
    if module_path and pkg == module_path:
        return "."
    if module_path and pkg.startswith(module_path + "/"):
        return pkg[len(module_path) + 1:]
    return pkg


def parse_gotest(lines, module_path: str = "") -> dict:
    """Coverage %, seconds and status per package from a `go test -json` stream (events that are not JSON are skipped)."""
    col = gtr.Collector()
    cover: dict[str, float] = {}
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            ev = json.loads(raw)
        except ValueError:
            continue
        if not isinstance(ev, dict):
            continue
        col.feed(ev, lambda _t: None)
        if ev.get("Action") == "output" and not ev.get("Test") and ev.get("Package"):
            m = COVER_RE.search(ev.get("Output", ""))
            if m:
                cover[rel_pkg(ev["Package"], module_path)] = float(m.group(1))
    return {
        "coverage": dict(sorted(cover.items())),
        "seconds": {rel_pkg(p, module_path): round(s, 2) for p, s in sorted(col.pkg_elapsed.items())},
        "status": {rel_pkg(p, module_path): s for p, s in sorted(col.pkg_status.items())},
        "failed_tests": sorted({f"{rel_pkg(p, module_path)}:{t}" for p, t in col.failed_tests})[:20],
    }


def changed_packages(files: list[str], module: str, cap: int = MAX_PACKAGES) -> tuple[list[str], list[str]]:
    """(measured, left out) package directories of `module` that the changed `files` belong to.

    Only Go files count (a `_test.go` change too), `testdata` and vendored trees do not. The busiest packages are measured first; what is left
    out because of the cap is listed in the report, never silently dropped.
    """
    prefix, roots = MODULE_DIRS[module]
    count: dict[str, int] = {}
    for f in files:
        if not f.endswith(".go") or "/testdata/" in "/" + f or "/deps/" in "/" + f:
            continue
        if prefix and not f.startswith(prefix):
            continue
        sub = f[len(prefix):]
        d = os.path.dirname(sub) or "."
        if d == "." and module == "root":
            continue
        if d != "." and not sub.startswith(roots):
            continue
        count[d] = count.get(d, 0) + 1
    ranked = sorted(count, key=lambda d: (-count[d], d))
    return sorted(ranked[:cap]), sorted(ranked[cap:])


def has_go_files(tree: str, module: str, rel: str) -> bool:
    prefix = MODULE_DIRS[module][0]
    d = os.path.join(tree, prefix, rel)
    return os.path.isdir(d) and any(n.endswith(".go") for n in os.listdir(d))


def go_test_flags(rel: str) -> list[str]:
    """The flags of the coverage run of one package, as the CI coverage step runs it."""
    return [] if rel in NOT_SHORT else ["-short"]


def parse_lint(text: str) -> dict:
    """New findings of a golangci-lint JSON report (it was run with --new-from-rev, so every issue is new)."""
    doc = json.loads(text) if text.strip() else {}
    issues = doc.get("Issues") or []
    out = []
    for i in issues[:LINT_MAX_FINDINGS]:
        pos = i.get("Pos") or {}
        out.append({"file": str(pos.get("Filename", "")), "line": int(pos.get("Line", 0) or 0),
                    "linter": str(i.get("FromLinter", "")), "text": str(i.get("Text", ""))[:200]})
    return {"new": len(issues), "findings": out}


# ------------------------------------------------------------------------------------------------ flaky-test watch
FAIL_RE = re.compile(r"^\s*--- FAIL: (\S+)", re.M)
MAX_LOG_BYTES = 30_000_000
MAX_LOG_JOBS = 8
MAX_FLAKY = 30
BAD = ("failure", "timed_out")


def failed_test_names(log: str) -> list[str]:
    return sorted(set(FAIL_RE.findall(log[:MAX_LOG_BYTES])))[:10]


def collect_flaky(repo: str, sha: str, gh) -> dict:
    """Jobs of the PR head's workflow runs that failed in one attempt and passed in a later attempt of the same run.

    `gh(path, jq=None)` returns the text of `gh api <path>` (a callable so the tests need no network). A run that was never re-run has
    one attempt and cannot show a flake, so it costs nothing beyond the run list. The watch therefore fills in after a re-run of
    a CI job, or when this report is itself computed after a re-run; with one attempt it says so.
    """
    runs = [json.loads(x) for x in gh(f"repos/{repo}/actions/runs?head_sha={sha}&per_page=100",
                                      '.workflow_runs[] | {id,name,run_attempt,event}').splitlines() if x.strip()]
    items: list[dict] = []
    reran = [r for r in runs if int(r.get("run_attempt") or 1) > 1]
    log_jobs = 0
    for r in sorted(reran, key=lambda r: str(r.get("name"))):
        attempts: dict[int, dict[str, dict]] = {}
        for n in range(1, int(r["run_attempt"]) + 1):
            text = gh(f"repos/{repo}/actions/runs/{r['id']}/attempts/{n}/jobs?per_page=100", '.jobs[] | {id,name,conclusion}')
            attempts[n] = {j["name"]: j for j in (json.loads(x) for x in text.splitlines() if x.strip())}
        for n in sorted(attempts):
            for name, job in sorted(attempts[n].items()):
                if job.get("conclusion") not in BAD:
                    continue
                later = [m for m in sorted(attempts) if m > n and (attempts[m].get(name) or {}).get("conclusion") == "success"]
                if not later:
                    continue
                tests: list[str] = []
                if log_jobs < MAX_LOG_JOBS:
                    log_jobs += 1
                    try:
                        tests = failed_test_names(gh(f"repos/{repo}/actions/jobs/{job['id']}/logs"))
                    except Exception:  # noqa: BLE001 - logs expire or are unreadable: the job-level finding stands
                        tests = []
                items.append({"workflow": str(r.get("name", "")), "job": name, "failed_attempt": n, "passed_attempt": later[0], "tests": tests})
    return {"runs_seen": len(runs), "reran_runs": len(reran), "items": items[:MAX_FLAKY]}
