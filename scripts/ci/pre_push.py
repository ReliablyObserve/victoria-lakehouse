#!/usr/bin/env python3
"""`make pre-push`: a preview of the fast, always-run tier of CI, run locally before a push.

It calls the same tools and scripts the CI jobs call (scripts/ci/pr_classify.py, gate_local.sh,
check_changelog_pr.py, changelog_fragments.py, `make conformance-check`, actionlint, zizmor, the
scripts/ci unit tests, pytest of the proof harness, helm, golangci-lint, gofmt, go vet, go build, go
test) and re-implements none of them. It is NOT the same as CI: it picks the Go test packages from the
diff, runs without -race unless asked, does not shard, and cannot run the Docker-stack jobs, so every
row says what it covered and the summary lists what it did not verify (CONTRIBUTING.md, "make pre-push",
has the per-step table with the workflow file:line and the list of what it cannot catch).

    make pre-push                       what the diff against origin/main touches
    make pre-push PREPUSH_SCOPE=all     every step, whatever the diff
    make pre-push RACE=1                adds -race to the go test

Scope comes from the committed diff `merge-base(origin/main, HEAD)..HEAD` (what a push publishes).
Uncommitted edits are NOT judged by the gates (they read HEAD); the lint and test steps read the working
tree, so a dirty tree (tracked or untracked files) is reported.

Result per step: PASS, FAIL, or SKIPPED(reason). Exit 0 when nothing FAILed, 1 when a step FAILed,
2 on a usage error (not a git repository, base not resolvable); `make` turns any non-zero exit into 2.
The headline is `PASS (N not verified)` when rows were skipped because a tool or input is missing or
ran only in part; the summary then names those rows and every always-run CI job that was not run here.
The full output of every step that printed any is kept in <git dir>/pre-push-logs/, overwritten by each run.

Stdlib only.
"""
from __future__ import annotations

import argparse
import dataclasses
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional

PASS, FAIL, SKIPPED = "PASS", "FAIL", "SKIPPED"

# The 11 parts of the PR evidence pack (skill section 1a). Printed as a reminder; the PR body is not parsed.
EVIDENCE_PARTS = (
    "Problem + reproduction on current origin/main (command and real output / failing assertion, issue or gap row)",
    "Expected behaviour = pinned upstream VL/VT (file:line, exact status and message)",
    "Spec and registry compliance (rows verbatim, MAIN RULES one line each, Direction section even if empty)",
    "Fail-before / pass-after (test names, command, result on main vs branch)",
    "VL/VT non-regression proof, logs and traces each (upstream-exact rows, differential run, all dimensions)",
    "Real-system proof beyond mocks, or 'not run locally, CI job X covers it'",
    "Regression guards (lock/fuzz/differential, mutants killed + survivors, multi-instance test, other-signal test)",
    "Blast radius: every changed file REQUIRED + reason (generated files named)",
    "Cost and perf (measured S3 GETs/CPU/latency, or the argument the success path is unchanged)",
    "Honest limits and follow-up issues (every number measured / sourced / assumed)",
    "Independent review verdict and resolutions",
)
EVIDENCE_EXTRA = (
    "Architectural change: Mermaid before/after in the PR body and the doc page (else one line: no architectural change)",
    "Before handover: all GHA checks green on the head, zero unresolved review threads, zero open code-scanning alerts",
)


# ------------------------------------------------------------------------------------------ scope tables
TRACES_DIR = "lakehouse-traces"
# The cheap static tests of workflows, tool pins, image digests and Dependabot coverage (about 30 s together,
# measured). They read .github/, the shell scripts of scripts/ci, the Makefile, Dockerfiles, compose files and
# manifests, so they run when the diff touches any of those (STATIC_*). The full `unittest discover` of
# scripts/ci/tests that the changelog-tests job runs on every PR is slow (about 400 s here, measured), so it
# runs for PREPUSH_SCOPE=all only; a changed scripts/ci file runs its own tests (tests_for_ci_file) plus the static set.
STATIC_TESTS = (
    "test_workflow_permissions.py",
    "test_workflow_action_pins.py",
    "test_workflow_concurrency.py",
    "test_ci_scope_workflows.py",
    "test_gate_workflows_static.py",
    "test_ci_tool_pins.py",
    "test_image_digest_pins.py",
    "test_dependabot_coverage.py",
    "test_proof_comment.py",
)
# conformance.yaml (job conformance-inventory) runs these next to `make conformance-check`. The first two clone the
# real repository (changelog.d, registry and features included) and take about 8 minutes together (measured), so they
# run for PREPUSH_SCOPE=all only and are reported NOT VERIFIED when the diff touches what they clone.
GATE_SELFTESTS = ("scripts/ci/tests/test_check_registry_touch.sh", "scripts/ci/tests/test_gate_workflow_emulation.sh")
GATE_SELFTEST_PY = "scripts/ci/tests/test_pr_classify_toolchain.py"
GATE_SELFTEST_INPUTS = ("changelog.d/", "tests/conformance/registry/", "docs/features.md")
# lint-logs (ci.yaml) runs these shell self-tests, and check_patches_equal.sh against a pristine logs-pin clone.
LINT_LOGS_SELFTESTS = (
    "scripts/ci/tests/check_patches_equal_test.sh",
    "scripts/ci/tests/check_coverage_test.sh",
    "scripts/ci/tests/upstream_sync_probe_test.sh",
    "scripts/ci/tests/upstream_sync_publish_test.sh",
    "scripts/ci/tests/loki_vl_proxy_bump_test.sh",
)
STATIC_PREFIXES = (".github/", "scripts/ci/", "deployment/", "charts/")
STATIC_NAMES = re.compile(r"(^|/)(Makefile|go\.mod|go\.sum|Dockerfile[^/]*|requirements[^/]*\.(txt|in)|docker-compose[^/]*\.ya?ml)$")
# proof-metrics.yaml path filter, plus the ab harness tests (pr-proof.yaml:457).
HARNESS_PREFIXES = (
    "scripts/bench/ab/", "scripts/proof/", "deployment/docker/proof/", "deployment/docker/docker-compose-proof.yml",
    "tests/parity/testdata/tiecut_golden.json",
)
# Inputs that shape a whole Go module: no package is "touched", yet CI runs everything for them.
WIDE_MODULE_FILES = {
    "go.mod": ("logs",), "go.sum": ("logs",),
    "lakehouse-traces/go.mod": ("traces",), "lakehouse-traces/go.sum": ("traces",),
    ".golangci.yml": ("logs", "traces"),
}
# patches/<set>/...: the set names the module it patches; a file directly under patches/ concerns both.
PATCH_SET_MODULES = {"vl-logs": ("logs",), "vl-traces": ("traces",), "vt-traces": ("traces",)}
# Files whose logic the CI scope and gate steps run from the BASE branch, not from this branch.
BASE_SIDE_FILES = ("scripts/ci/pr_classify.py", ".github/actions/ci-scope/")

# Non-Go paths that a Go test of a CI-tested package reads (the test_pre_push drift test greps every Go file of
# those packages for the top-level repository names and fails when this table misses one):
#   path prefix -> ((module, package dir, -run regex or ""), ...)
# A non-Go file under a Go package directory needs no row: it belongs to that package (embedded assets, testdata).
# tests/conformance reads many of these too, but `make conformance-check` always runs it.
GO_TEST_READERS = {
    "docs/": (("logs", "internal/config", ""), ("logs", "internal/delete", "")),
    "README.md": (("logs", "internal/config", ""),),
    "deployment/": (("logs", "internal/config", ""),),
    "alerts/": (("logs", "internal/metrics", ""),),
    "dashboards/": (("logs", "internal/metrics", ""),),
    "charts/": (("logs", "internal/compaction", "^TestCompaction_PDB_NoSimultaneousEviction$"),),
    "benchmarks/": (("logs", "cmd/bench", ""),),
}
# Paths no Go test of a CI-tested package reads (proved by the same drift test). They can still change what other
# jobs report; the rows for those jobs say so.
GO_INERT = (
    ".github/", "bench-results/", "results/", "website/", "scripts/", "tests/", "changelog.d/",
    "CHANGELOG.md", "UPSTREAM_COVERAGE.md", "CONTRIBUTING.md", "SECURITY.md", "LICENSE", "Makefile",
    "Dockerfile", ".gitignore", ".dockerignore", ".gitleaks.toml", "go.work", "go.work.sum", ".claude/", "AGENTS.md",
)
# Go packages CI tests (`go test` / `go vet`): everything else a diff touches is linted or built but not selected.
CI_TESTED_LOGS = ("internal/", "cmd/", "scripts/ci/parquet-readback/")

# ------------------------------------------------------------------------------------------ CI job map
# One row per job of the workflows this tool mirrors: whether the job always runs (no `if:`), the pre-push steps
# that mirror it, and for mirrored jobs one entry per workflow step saying what covers it. test_pre_push parses the
# workflow files and fails when a job or a step of a mirrored job is missing here, so a new CI step is noticed.
# Step keys are the step `name`, or "golangci-lint <args>" for the unnamed lint action steps.
NC = "NOT COVERED: "
CI_JOBS = {
    "ci.yaml:scope": {"always": True, "local": ("scope",), "steps": None},
    "ci.yaml:deps": {"always": True, "local": ("deps",), "steps": None},
    "ci.yaml:helm-config-drift": {"always": True, "local": (), "steps": None},
    "ci.yaml:bench-selftest": {"always": True, "local": (), "steps": None},
    "ci.yaml:field-metadata-perf": {"always": False, "local": (), "steps": None},
    "ci.yaml:parquet-readback": {"always": False, "local": (), "steps": None},
    "ci.yaml:test-logs": {"always": True, "local": ("go vet (logs)", "go test (logs)"), "steps": {
        "Check gofmt -s (logs)": "go vet (logs)",
        "Test logs module": "go test (logs): packages picked from the diff, -race only with RACE=1, no sharding",
        "Coverage check (logs)": NC + "the 90% coverage gate",
        "Vet logs module": "go vet (logs)",
    }},
    "ci.yaml:test-traces": {"always": True, "local": ("go vet (traces)", "go test (traces)"), "steps": {
        "Check gofmt -s (traces)": "go vet (traces)",
        "Test traces module": "go test (traces): packages picked from the diff, -race only with RACE=1, no sharding",
        "Coverage check (traces)": NC + "the 90% coverage gate",
        "Vet traces module": "go vet (traces)",
    }},
    "ci.yaml:test-parquets3-heavy": {"always": False, "local": (), "steps": None},
    "ci.yaml:storage-health-compaction": {"always": False, "local": (), "steps": None},
    "ci.yaml:parity-unit": {"always": False, "local": (), "steps": None},
    "ci.yaml:lint-logs": {"always": True, "local": ("golangci-lint (logs)", "lint-logs-selftests"), "steps": {
        "Clone the logs-pin VictoriaLogs tree": "lint-logs-selftests",
        "Patch-set consistency guard self-test": "lint-logs-selftests",
        "Patch-set consistency (vl-logs vs vl-traces)": "lint-logs-selftests",
        "Coverage gate self-test": "lint-logs-selftests",
        "Upstream sync probe self-test": "lint-logs-selftests",
        "Upstream sync publish self-test": "lint-logs-selftests",
        "loki-vl-proxy bump self-test": "lint-logs-selftests",
        "golangci-lint ./internal/... ./cmd/...": "golangci-lint (logs)",
        "golangci-lint --build-tags e2e,chaos ./tests/ingestmatrix/... ./tests/e2e/... ./tests/conformance/...": "golangci-lint (logs)",
        "golangci-lint --build-tags parity,chaos ./tests/parity/...": "golangci-lint (logs)",
    }},
    "ci.yaml:lint-traces": {"always": True, "local": ("golangci-lint (traces)",), "steps": {
        "golangci-lint": "golangci-lint (traces)",
        "golangci-lint --build-tags parity,chaos,e2e ./...": "golangci-lint (traces)",
    }},
    "ci.yaml:build-logs": {"always": True, "local": ("go build (logs)",), "steps": {
        "Build lakehouse-logs": "go build (logs): the host platform only, no linux/darwin matrix",
    }},
    "ci.yaml:build-traces": {"always": True, "local": ("go build (traces)",), "steps": {
        "Build lakehouse-traces": "go build (traces): the host platform only, no linux/darwin matrix",
    }},
    "ci.yaml:docker-logs": {"always": True, "local": (), "steps": None},
    "ci.yaml:docker-traces": {"always": True, "local": (), "steps": None},
    "ci.yaml:security-logs": {"always": False, "local": (), "steps": None},
    "ci.yaml:security-traces": {"always": False, "local": (), "steps": None},
    "ci.yaml:helm": {"always": True, "local": ("chart-tests",), "steps": {
        "Lint Helm chart": "chart-tests",
        "Verify Helm images and templates": "chart-tests",
    }},
    "ci.yaml:benchmarks-logs": {"always": False, "local": (), "steps": None},
    "ci.yaml:benchmarks-traces": {"always": False, "local": (), "steps": None},
    "conformance.yaml:conformance-inventory": {"always": True, "local": ("deps", "conformance-check", "registry-gate-selftests"), "steps": {
        "Prepare deps (no-op when cache hit)": "deps",
        "Conformance drift and generated files check": "conformance-check",
        "Registry gate self-tests": "registry-gate-selftests",
    }},
    # proof-metrics.yaml has a workflow-level path filter, so it is not an always-run job
    "proof-metrics.yaml:proof-metrics": {"always": False, "local": ("harness-tests",), "steps": {
        "Install test dependencies": "harness-tests (the venv of PRE_PUSH_PYTHON)",
        "Lint": "harness-tests",
        "Unit tests with coverage floor": "harness-tests",
        "Proof stack compose file is valid": NC + "needs Docker (`docker compose config`)",
        "Fixtures match their recorded expectations": "harness-tests",
        "Fixture generator output is committed": "harness-tests (run in a copy of HEAD, never in the worktree)",
    }},
    "changelog-pr.yaml:changelog-check": {"always": True, "local": ("changelog-gate",), "steps": {
        "Check changelog": "changelog-gate",
    }},
    "changelog-pr.yaml:changelog-tests": {"always": True, "local": ("ci-script-tests",), "steps": {
        "Install test dependencies": "ci-script-tests (the venv of PRE_PUSH_PYTHON)",
        "Run CI script tests": "ci-script-tests: the full discovery only with PREPUSH_SCOPE=all, else the tests of the changed files plus the static set",
    }},
    "workflow-lint.yaml:workflow-lint": {"always": True, "local": ("actionlint", "zizmor"), "steps": {
        "actionlint": "actionlint",
        "zizmor": "zizmor (online audits need GH_TOKEN)",
    }},
}
# Steps with no pr-proof job equivalent here but named in the CONTRIBUTING table: pr-proof.yaml:457 runs the ab tests.
VENV_HINT = (
    "python3 -m venv ~/.cache/lakehouse-pre-push-venv && ~/.cache/lakehouse-pre-push-venv/bin/pip install --require-hashes -r "
    "scripts/ci/requirements.txt -r scripts/proof/requirements.txt; "
    "then PRE_PUSH_PYTHON=~/.cache/lakehouse-pre-push-venv/bin/python make pre-push"
)

# AI / co-author attribution that the repository forbids in commits (owner rule: none anywhere).
TRAILER_RE = re.compile(r"(?im)^[ \t]*co-authored-by[ \t]*:")
ATTRIBUTION_RE = re.compile(
    r"(?i)claude-session[ \t]*:|claude\.ai/code|generated[ \t]+with[ \t]+\[?claude|noreply@anthropic\.com|\U0001F916"
)
IDENT_RE = re.compile(r"(?i)claude|anthropic")
# The line of a failing command's output that says what failed (actionlint ends with a "| ^" marker, go test with
# a bare "FAIL"): the first line that looks like an error, else the last line.
ERROR_LINE_RE = re.compile(r"(?i)(\berror\b|\bfail(ed|ure)?\b|panic:|:\d+:\d+:|undefined|cannot |not found|required on|mismatch)")


# ------------------------------------------------------------------------------------------ data types
@dataclasses.dataclass
class Result:
    status: str
    detail: str = ""
    log: str = ""
    unverified: bool = False  # SKIPPED because a tool or input is missing: CI will still run the check
    partial: str = ""  # not SKIPPED, but part of what CI runs for this row was not run here (the reason)


@dataclasses.dataclass
class Outcome:
    name: str
    status: str
    detail: str
    seconds: float
    log: str = ""
    unverified: bool = False
    partial: str = ""


@dataclasses.dataclass
class Step:
    name: str
    # None: run it. A string: SKIPPED with that reason (not in scope).
    applies: Callable[["Ctx"], Optional[str]]
    run: Callable[["Ctx"], Result]


class Sys:
    """The only door to the outside world: commands and tool lookup. Tests replace it."""

    def __init__(self, root: Path, environ: Optional[dict] = None):
        self.root = root
        self.environ = dict(os.environ if environ is None else environ)

    def run(self, cmd, cwd=None, env=None, timeout=1800):
        e = dict(self.environ)
        e.update(env or {})
        try:
            p = subprocess.run(
                cmd, cwd=str(cwd or self.root), env=e, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace", timeout=timeout,
            )
            return p.returncode, p.stdout
        except FileNotFoundError as ex:
            return 127, f"command not found: {ex}"
        except subprocess.TimeoutExpired as ex:
            out = ex.stdout if isinstance(ex.stdout, str) else ""
            return 124, out + f"\ntimeout after {timeout}s"

    def which(self, name):
        return shutil.which(name, path=self.environ.get("PATH"))

    def exists(self, rel):
        return (self.root / rel).exists()

    def listdir(self, rel):
        d = self.root / rel
        return sorted(p.name for p in d.iterdir()) if d.is_dir() else []

    def has_go(self, rel):
        d = self.root / rel
        return d.is_dir() and any(d.glob("*.go"))


@dataclasses.dataclass
class Ctx:
    sys: Sys
    base: str = "origin/main"
    scope_all: bool = False
    race: bool = False
    offline: bool = False
    py: str = sys.executable or "python3"
    files: list = dataclasses.field(default_factory=list)
    classify: dict = dataclasses.field(default_factory=dict)
    classify_ok: bool = False
    merge_base: str = ""
    cache: dict = dataclasses.field(default_factory=dict)

    @property
    def root(self) -> Path:
        return self.sys.root

    def git(self, *args):
        return self.sys.run(["git", *args])

    def go_env(self) -> dict:
        return {"GOWORK": "off", "GOMEMLIMIT": self.sys.environ.get("GOMEMLIMIT") or "4GiB"}

    def lint_base(self) -> str:
        """What CI's only-new-issues compares against: the merge base of the pull request."""
        return self.merge_base or self.base


# ------------------------------------------------------------------------------------------ pure scope logic
def under(files, *prefixes) -> bool:
    return any(f.startswith(p) for f in files for p in prefixes)


def is_deps_path(f: str) -> bool:
    return f.startswith("deps/") or "/deps/" in f or "/vendor/" in f


def module_of(f: str) -> str:
    return "traces" if f.startswith(TRACES_DIR + "/") else "logs"


def go_pkg_dir(f: str) -> str:
    """The directory of the Go package a changed path belongs to (testdata belongs to its parent)."""
    if "/testdata/" in f:
        return f[: f.index("/testdata/")]
    return os.path.dirname(f) or "."


def nearest_go_dir(sys_: Sys, f: str) -> Optional[str]:
    """The nearest ancestor directory of a non-Go file that holds Go files on disk (embedded assets, testdata)."""
    d = os.path.dirname(f)
    while d:
        if sys_.has_go(d):
            return d
        d = os.path.dirname(d)
    return None


def rel_to_module(module: str, d: str) -> str:
    if module == "traces":
        return d[len(TRACES_DIR) + 1:] if d != TRACES_DIR else "."
    return d


def pattern(d: str) -> str:
    return "./" + d if d != "." else "."


def wide_modules(files) -> set:
    """Go modules whose whole build changes with the diff (go.mod, go.sum, patches, the linter config)."""
    out: set = set()
    for f in files:
        if f in WIDE_MODULE_FILES:
            out.update(WIDE_MODULE_FILES[f])
        elif f.startswith("patches/"):
            parts = f.split("/")
            out.update(PATCH_SET_MODULES.get(parts[1], ("logs", "traces")) if len(parts) > 2 else ("logs", "traces"))
    return out


def reader_rows(f: str):
    """The GO_TEST_READERS rows of the longest prefix that matches a path (empty when none does)."""
    best = ""
    for prefix in GO_TEST_READERS:
        if (f == prefix or f.startswith(prefix)) and len(prefix) > len(best):
            best = prefix
    return GO_TEST_READERS.get(best, ())


def is_inert(f: str) -> bool:
    return any(f == p or (p.endswith("/") and f.startswith(p)) for p in GO_INERT) or f.startswith("Dockerfile")


def go_plan(ctx: "Ctx") -> dict:
    """What the diff means for the Go jobs, per module: `compile` (vet, lint and build must run: a Go file, an
    embedded asset or a module-wide input changed), `dirs` (changed package directories), `readers` ({dir: -run
    regex or ""} for packages whose tests read a changed non-Go file), `unmapped` (changed paths that no row
    explains) and `wide`. A change to the logs module compiles into the traces module too (replace ../)."""
    if "go" in ctx.cache:
        return ctx.cache["go"]
    plan = {m: {"compile": False, "dirs": set(), "readers": {}, "unmapped": [], "wide": False} for m in ("logs", "traces")}
    wide = wide_modules(ctx.files)
    for m in wide:
        plan[m]["compile"] = plan[m]["wide"] = True
    for f in ctx.files:
        if is_deps_path(f) or f in WIDE_MODULE_FILES or f.startswith("patches/"):
            continue  # deps/ is not a diff; go.mod, go.sum and patches/ are module-wide (wide_modules)
        m = module_of(f)
        if f.endswith(".go"):
            plan[m]["dirs"].add(rel_to_module(m, go_pkg_dir(f)))
            if "/testdata/" not in f:
                plan[m]["compile"] = True
            continue
        d = nearest_go_dir(ctx.sys, f)
        if d is not None:
            plan[m]["dirs"].add(rel_to_module(m, d))
            if "/testdata/" not in f and not f.endswith((".md", ".txt")):
                plan[m]["compile"] = True
            continue
        rows = reader_rows(f)
        if rows:
            for rm, rdir, run in rows:
                cur = plan[rm]["readers"]
                if rdir in cur and cur[rdir] != run:
                    cur[rdir] = ""  # one reader needs the whole package
                else:
                    cur.setdefault(rdir, run)
            continue
        if is_inert(f):
            continue
        plan[m]["unmapped"].append(f)
    if plan["logs"]["compile"]:
        plan["traces"]["compile"] = True
    for m in plan:
        plan[m]["dirs"] = sorted(plan[m]["dirs"])
    ctx.cache["go"] = plan
    return plan


def vet_test_dirs(module: str, dirs) -> list:
    """Directories that get go test: tests/ holds build-tagged suites that need Docker (CI jobs e2e, parity and
    conformance run them), so they are linted but not run here."""
    return [d for d in dirs if not (module == "logs" and (d == "tests" or d.startswith("tests/")))]


def lint_invocations(module: str) -> list:
    """[(build tags, [patterns], only_new)] mirroring the lint steps of .github/workflows/ci.yaml. The plain run
    has no only-new-issues in CI, so it lints whole packages; the tagged tests/ runs are only-new-issues there."""
    if module == "traces":
        return [("", ["./..."], False), ("parity,chaos,e2e", ["./..."], True)]
    return [
        ("", ["./internal/...", "./cmd/..."], False),
        ("e2e,chaos", ["./tests/ingestmatrix/...", "./tests/e2e/...", "./tests/conformance/..."], True),
        ("parity,chaos", ["./tests/parity/..."], True),
    ]


def vet_patterns(module: str) -> list:
    return ["./..."] if module == "traces" else ["./internal/...", "./cmd/..."]


def static_trigger(files) -> bool:
    """True when the diff touches something the static scripts/ci tests read."""
    return any(f.startswith(STATIC_PREFIXES) or STATIC_NAMES.search(f) for f in files)


def tests_for_ci_file(f: str, test_names) -> list:
    """The scripts/ci/tests files that belong to a changed scripts/ci path (by name, as the repository names them)."""
    if not f.startswith("scripts/ci/") or f.startswith("scripts/ci/tests/testdata/"):
        return []
    base = os.path.basename(f)
    if f.startswith("scripts/ci/tests/"):
        is_test = base.startswith("test_") or base.endswith("_test.sh")
        return [base] if is_test and base in test_names and base.endswith((".py", ".sh")) else []
    stem = base.rsplit(".", 1)[0]
    return [n for n in test_names if (n.startswith(f"test_{stem}") and n.endswith((".py", ".sh"))) or n == f"{stem}_test.sh"]


def parse_keyvals(text: str) -> dict:
    out: dict = {}
    for line in text.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            if re.fullmatch(r"[a-z_]+", k):
                out[k] = v
    return out


def commit_signature_problems(log_text: str) -> list:
    """Lines `<sha>\\x1f<%G?>\\x1f<subject>`: every commit must show G (good signature)."""
    bad = []
    for line in log_text.splitlines():
        parts = line.split("\x1f")
        if len(parts) >= 2 and parts[1] != "G":
            bad.append(f"{parts[0][:8]} [{parts[1] or '?'}] {parts[2] if len(parts) > 2 else ''}".rstrip())
    return bad


def attribution_problems(log_text: str) -> list:
    """Records `<sha>\\x1f<an>\\x1f<ae>\\x1f<cn>\\x1f<ce>\\x1f<body>\\x1e`: no co-author trailer, no AI attribution
    line, no AI identity as author or committer."""
    bad = []
    for rec in log_text.split("\x1e"):
        rec = rec.strip("\n")
        if not rec.strip():
            continue
        parts = rec.split("\x1f", 5)
        if len(parts) < 6:
            bad.append(f"unparsable commit record: {rec[:40]!r}")
            continue
        sha, ident, body = parts[0], " ".join(parts[1:5]), parts[5]
        why = []
        if TRAILER_RE.search(body):
            why.append("Co-Authored-By trailer")
        if ATTRIBUTION_RE.search(body):
            why.append("AI attribution text")
        if IDENT_RE.search(ident):
            why.append("AI identity as author/committer")
        if why:
            bad.append(f"{sha[:8]}: {', '.join(why)}")
    return bad


def one_line(text: str, limit: int = 100) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def tail(text: str, n: int = 12) -> str:
    return "\n".join(text.rstrip().splitlines()[-n:])


def failure_line(out: str, rc: int) -> str:
    """The first line of the output that looks like the error, else the last line (actionlint ends with `| ^`)."""
    lines = [ln for ln in out.strip().splitlines() if ln.strip()]
    for ln in lines:
        if ERROR_LINE_RE.search(ln):
            return ln.strip()
    return lines[-1].strip() if lines else f"exit {rc}"


# ------------------------------------------------------------------------------------------ step helpers
def scoped(ctx: Ctx, present: bool, what: str) -> Optional[str]:
    """None when the step should run: PREPUSH_SCOPE=all, or the diff touches `what`."""
    if ctx.scope_all or present:
        return None
    return f"diff touches no {what}"


def run_cmds(ctx: Ctx, cmds, ok_detail: str = "") -> Result:
    """Run [(label, cmd, kwargs)] in order; stop at the first failure. The full output goes to the log."""
    log = []
    for label, cmd, kw in cmds:
        rc, out = ctx.sys.run(cmd, **kw)
        log.append(f"$ {' '.join(cmd)}\n{out}")
        if rc != 0:
            return Result(FAIL, f"{label} exit {rc}: {one_line(failure_line(out, rc), 70)}", "\n".join(log))
    return Result(PASS, ok_detail, "\n".join(log))


def py_has(ctx: Ctx, module: str) -> bool:
    rc, _ = ctx.sys.run([ctx.py, "-c", f"import {module}"])
    return rc == 0


# ------------------------------------------------------------------------------------------ steps
def step_fetch(ctx: Ctx) -> Result:
    if not ctx.base.startswith("origin/"):
        return Result(SKIPPED, f"base {ctx.base} is not origin/*: nothing to fetch")
    if ctx.offline:
        return Result(SKIPPED, "offline: verdicts are against the local origin/main, which may be stale", unverified=True)
    branch = ctx.base.split("/", 1)[1]
    rc, out = ctx.git("fetch", "-q", "origin", branch)
    if rc != 0:
        return Result(FAIL, f"cannot fetch origin {branch} (a verdict on a stale base is worthless; PRE_PUSH_OFFLINE=1 to accept)", out)
    return Result(PASS, f"origin/{branch} is current")


def tree_note(porcelain: str) -> str:
    """The note on uncommitted work: tracked edits and untracked files, which the gates (HEAD) do not see."""
    lines = [ln for ln in porcelain.splitlines() if ln.strip()]
    untracked = sum(1 for ln in lines if ln.startswith("??"))
    tracked = len(lines) - untracked
    parts = []
    if tracked:
        parts.append(f"{tracked} uncommitted tracked change(s)")
    if untracked:
        parts.append(f"{untracked} untracked file(s)")
    if not parts:
        return ""
    return "; " + " and ".join(parts) + " NOT seen by the gates (go vet, lint and test do read the working tree)"


def step_scope(ctx: Ctx) -> Result:
    rc, mb = ctx.git("merge-base", ctx.base, "HEAD")
    if rc != 0:
        return Result(FAIL, f"no merge base with {ctx.base}", mb)
    ctx.merge_base = mb.strip()
    rc, out = ctx.git("diff", "--no-renames", "--name-only", "-z", ctx.merge_base, "HEAD")
    if rc != 0:
        return Result(FAIL, "git diff failed", out)
    ctx.files = [f for f in out.split("\0") if f]
    rc, out = ctx.sys.run([
        ctx.py, "scripts/ci/pr_classify.py", "--base", ctx.merge_base, "--head", "HEAD",
        "--ci-scope", "--event", "pull_request",
    ])
    if rc != 0:
        # fail open, like the classifier itself: an unknown scope means the Go group is in scope
        ctx.classify, ctx.classify_ok = {}, False
        return Result(FAIL, f"pr_classify.py failed (scope treated as full): {one_line(tail(out, 2), 70)}", out)
    ctx.classify, ctx.classify_ok = parse_keyvals(out), True
    c = ctx.classify
    note = tree_note(ctx.git("status", "--porcelain", "--untracked-files=normal")[1])
    mode = "PREPUSH_SCOPE=all" if ctx.scope_all else "diff"
    partial = ""
    if any(f.startswith(BASE_SIDE_FILES) for f in ctx.files):
        partial = "the diff changes the classifier or the ci-scope action: CI scopes the run with the copy on the base branch, this ran this branch's copy"
    return Result(
        PASS,
        f"{mode}: {len(ctx.files)} file(s), ci_class={c.get('ci_class', '?')} skip_go={c.get('skip_go', '?')} "
        f"(informational: no Go job is skipped on it){note}" + (f"; {partial}" if partial else ""),
        out,
        partial=partial,
    )


def step_contains_main(ctx: Ctx) -> Result:
    rc, tip = ctx.git("rev-parse", "--verify", ctx.base + "^{commit}")
    if rc != 0:
        return Result(FAIL, f"{ctx.base} does not resolve", tip)
    if ctx.merge_base == tip.strip():
        return Result(PASS, f"merge-base == {ctx.base}")
    rc, n = ctx.git("rev-list", "--count", f"HEAD..{ctx.base}")
    behind = n.strip() if rc == 0 else "?"
    return Result(FAIL, f"branch is {behind} commit(s) behind {ctx.base}: merge it in, regenerate, re-gate (checks on a stale base mean nothing)")


def step_signed(ctx: Ctx) -> Result:
    rc, out = ctx.git("log", "--format=%H%x1f%G?%x1f%s", f"{ctx.base}..HEAD")
    if rc != 0:
        return Result(FAIL, "git log failed", out)
    if not out.strip():
        return Result(SKIPPED, f"no commits ahead of {ctx.base}")
    bad = commit_signature_problems(out)
    if bad:
        return Result(FAIL, f"{len(bad)} commit(s) not signed (G): {bad[0]}", "\n".join(bad))
    return Result(PASS, f"{len(out.strip().splitlines())} commit(s), all signed")


def step_attribution(ctx: Ctx) -> Result:
    rc, out = ctx.git("log", "--format=%H%x1f%an%x1f%ae%x1f%cn%x1f%ce%x1f%B%x1e", f"{ctx.base}..HEAD")
    if rc != 0:
        return Result(FAIL, "git log failed", out)
    if not out.strip():
        return Result(SKIPPED, f"no commits ahead of {ctx.base}")
    bad = attribution_problems(out)
    if bad:
        return Result(FAIL, f"{len(bad)} commit(s) carry attribution: {bad[0]}", "\n".join(bad))
    return Result(PASS, "no AI attribution or co-author trailer")


def gate_detail(log: str) -> tuple:
    """(verdict, kind, first failing rule) from the gate report that gate_local.sh prints."""
    verdict = kind = rule = ""
    for ln in log.splitlines():
        m = re.match(r"## Registry gate: (.*)", ln)
        if m:
            verdict = m.group(1).strip()
        elif ln.startswith("PR kind:"):
            kind = ln[len("PR kind:"):].strip()
        elif not rule and re.match(r"\|[^|]*\|\s*FAIL\s*\|", ln):
            cells = [c.strip() for c in ln.strip().strip("|").split("|")]
            rule = f"{cells[0]}: {cells[2]}" if len(cells) > 2 else ln
    return verdict, kind, rule


GATE_CHANGED_NOTICE = "this branch changes the gate itself"


def step_registry_gate(ctx: Ctx) -> Result:
    env = {"REGISTRY_GATE_OFFLINE": "1", "GOWORK": "off"}  # the fetch step already fetched (or was told not to)
    r = run_cmds(ctx, [("registry gate", ["bash", "scripts/ci/gate_local.sh", ctx.base], {"env": env})])
    verdict, kind, rule = gate_detail(r.log)
    if r.status == PASS:
        r.detail = one_line(f"{verdict or 'passed'}; {kind}" if kind else verdict or "passed", 100)
        if GATE_CHANGED_NOTICE in r.log:
            # CI judges the pull request with the gate code of the base branch: this verdict only previews the new gate.
            r.partial = "the branch changes the gate itself: CI judges it with main's copy of the gate, this run previews the new one"
            r.detail = one_line("PREVIEW ONLY, " + r.partial + "; " + r.detail, 220)
    elif rule:
        r.detail = one_line(f"{verdict or 'FAIL'}: {rule}", 160)
    return r


def step_changelog(ctx: Ctx) -> Result:
    r = run_cmds(ctx, [
        ("changelog gate", [ctx.py, "scripts/ci/check_changelog_pr.py", "--base", ctx.base, "--head", "HEAD"], {}),
        ("fragments", [ctx.py, "scripts/ci/changelog_fragments.py", "check"], {}),
    ])
    if r.status == PASS:
        gate = [ln for ln in r.log.splitlines() if ln.startswith("changelog gate:")]
        r.detail = one_line(gate[-1][len("changelog gate: "):] if gate else "ok", 90)
    return r


def applies_ci_scripts(ctx: Ctx) -> Optional[str]:
    return scoped(ctx, static_trigger(ctx.files), "workflow, scripts/ci, Dockerfile, compose, chart, Makefile or manifest file")


def step_ci_scripts(ctx: Ctx) -> Result:
    if not py_has(ctx, "yaml"):
        return Result(SKIPPED, "PyYAML missing for the scripts/ci tests: " + VENV_HINT, unverified=True)
    if ctx.scope_all:
        cmds = [("unittest", [ctx.py, "-m", "unittest", "discover", "-s", "scripts/ci/tests"], {})]
        return run_cmds(ctx, cmds, "scripts/ci/tests full discovery (changelog-tests)")
    names = ctx.sys.listdir("scripts/ci/tests")
    chosen = {t for f in ctx.files for t in tests_for_ci_file(f, names)}
    if static_trigger(ctx.files):
        chosen.update(t for t in STATIC_TESTS if t in names)
    py = sorted(f"scripts/ci/tests/{t}" for t in chosen if t.endswith(".py"))
    sh = sorted(f"scripts/ci/tests/{t}" for t in chosen if t.endswith(".sh"))
    cmds = []
    if py:
        cmds.append(("unittest", [ctx.py, "-m", "unittest", *py], {}))
    cmds += [(os.path.basename(t), ["bash", t], {}) for t in sh]
    if not cmds:
        return Result(SKIPPED, "no scripts/ci test maps to the changed files")
    r = run_cmds(ctx, cmds, f"{len(py)} python + {len(sh)} shell test file(s) of the changed files (CI discovers every test)")
    if r.status == PASS:
        r.partial = "CI runs the full discovery of scripts/ci/tests on every pull request; this ran the tests of the changed files and the static set"
    return r


def applies_gate_selftests(ctx: Ctx) -> Optional[str]:
    if ctx.scope_all or under(ctx.files, *GATE_SELFTEST_INPUTS):
        return None
    return "diff touches nothing the self-tests clone (changelog.d, registry, features page); a changed gate script runs them in ci-script-tests"


def step_gate_selftests(ctx: Ctx) -> Result:
    if not ctx.scope_all:
        return Result(
            SKIPPED,
            "CI conformance-inventory runs this (~8 min, clones the repository with changelog.d, the registry and docs/features.md): "
            "run PREPUSH_SCOPE=all",
            unverified=True,
        )
    cmds = [(os.path.basename(t), ["bash", t], {"env": {"GOWORK": "off"}}) for t in GATE_SELFTESTS]
    cmds.append((os.path.basename(GATE_SELFTEST_PY), [ctx.py, "-m", "unittest", GATE_SELFTEST_PY], {}))
    return run_cmds(ctx, cmds, "registry gate self-tests + classifier toolchain test (conformance.yaml)")


def applies_lint_logs_selftests(ctx: Ctx) -> Optional[str]:
    return scoped(ctx, under(ctx.files, "patches/"), "patches/ file")


def logs_pin_tree(ctx: Ctx):
    """(path, reason): a pristine shallow clone of the logs-pin VictoriaLogs tag, cached outside the repository."""
    try:
        m = re.search(r"VL_VERSION_LOGS:\s*(v[\d.]+)", (ctx.root / ".github/workflows/ci.yaml").read_text(encoding="utf-8"))
    except OSError:
        m = None
    if not m:
        return None, "cannot read VL_VERSION_LOGS from ci.yaml"
    ver = m.group(1)
    cache = Path(os.path.expanduser(ctx.sys.environ.get("PRE_PUSH_CACHE") or "~/.cache/lakehouse-pre-push")) / f"vl-logs-{ver}"
    if (cache / "go.mod").is_file():
        return cache, ""
    if ctx.offline:
        return None, f"no cached clone of VictoriaLogs {ver} and PRE_PUSH_OFFLINE=1"
    cache.parent.mkdir(parents=True, exist_ok=True)
    rc, out = ctx.sys.run(["git", "clone", "--quiet", "--depth", "1", "--branch", ver,
                           "https://github.com/VictoriaMetrics/VictoriaLogs.git", str(cache)])
    if rc != 0:
        shutil.rmtree(cache, ignore_errors=True)
        return None, f"cannot clone VictoriaLogs {ver}: {one_line(failure_line(out, rc), 60)}"
    return cache, ""


def step_lint_logs_selftests(ctx: Ctx) -> Result:
    pin, why = logs_pin_tree(ctx)
    if pin is None:
        return Result(SKIPPED, f"{why}; CI lint-logs runs the patch-set checks", unverified=True)
    env = {"LOGS_PIN_TREE": str(pin)}
    cmds = [("check_patches_equal_test.sh", ["bash", LINT_LOGS_SELFTESTS[0]], {"env": env}),
            ("check_patches_equal.sh", ["bash", "scripts/ci/check_patches_equal.sh"], {"env": env})]
    if ctx.scope_all:
        cmds += [(os.path.basename(t), ["bash", t], {}) for t in LINT_LOGS_SELFTESTS[1:]]
    return run_cmds(ctx, cmds, "patch-set consistency against a pristine pin clone" + (" + the four other lint-logs self-tests" if ctx.scope_all else ""))


def tool_pin(ctx: Ctx, tool: str) -> str:
    pins = {"actionlint": (".github/workflows/workflow-lint.yaml", r'ACTIONLINT_VERSION:\s*"?([\d.]+)"?'),
            "golangci-lint": (".github/workflows/ci.yaml", r"GOLANGCI_LINT_VERSION:\s*v?([\d.]+)")}
    f, rx = pins[tool]
    try:
        m = re.search(rx, (ctx.root / f).read_text(encoding="utf-8"))
    except OSError:
        return ""
    return m.group(1) if m else ""


def version_note(ctx: Ctx, tool: str, exe: str) -> str:
    pin = tool_pin(ctx, tool)
    rc, out = ctx.sys.run([exe, "--version"])
    have = re.search(r"(\d+\.\d+\.\d+)", out) if rc == 0 else None
    if pin and have and have.group(1) != pin:
        return f" (local {have.group(1)}, CI pins {pin}: findings can differ)"
    return ""


def find_tool(ctx: Ctx, name: str) -> Optional[str]:
    """On PATH, or next to the interpreter of PRE_PUSH_PYTHON (a venv that installed it)."""
    exe = ctx.sys.which(name)
    if exe:
        return exe
    sibling = Path(ctx.py).parent / name
    return str(sibling) if sibling.is_file() else None


def step_actionlint(ctx: Ctx) -> Result:
    exe = ctx.sys.which("actionlint")
    if not exe:
        return Result(SKIPPED, "actionlint not installed (brew install actionlint); CI workflow-lint runs it", unverified=True)
    r = run_cmds(ctx, [("actionlint", [exe, "-shellcheck=", "-pyflakes="], {})])
    if r.status == PASS:
        r.detail = "workflows clean" + version_note(ctx, "actionlint", exe)
    return r


def step_zizmor(ctx: Ctx) -> Result:
    exe = find_tool(ctx, "zizmor")
    if not exe:
        return Result(SKIPPED, "zizmor not installed: pip install --require-hashes --only-binary=:all: -r "
                      ".github/workflow-lint-requirements.txt into the PRE_PUSH_PYTHON venv (GH_TOKEN enables the online audits); CI workflow-lint runs it",
                      unverified=True)
    r = run_cmds(ctx, [("zizmor", [exe, "--no-progress", "--config", ".github/zizmor.yml", ".github"], {})])
    if r.status == PASS:
        r.detail = "no findings beyond .github/zizmor.yml"
        if not (ctx.sys.environ.get("GH_TOKEN") or ctx.sys.environ.get("GITHUB_TOKEN")):
            r.partial = "no GH_TOKEN: the online audits (CI runs them with the workflow token) were not run"
            r.detail += "; " + r.partial
    return r


def harness_env() -> tuple:
    """(env, private dir): the harness tests get their own TMPDIR and compose project name, so they never share the
    stack lock (<tmp>/lh-ab-<project>.lock) or the port block of a proof or A/B run that is going on."""
    d = tempfile.mkdtemp(prefix="pp-harness-")
    return {"TMPDIR": d, "AB_PROJECT": f"lh-pp-{os.getpid()}"}, d


def generator_drift(ctx: Ctx) -> Result:
    """The committed fixture cases must equal the generator's output (proof-metrics.yaml). The generator runs in a
    copy of HEAD under a temporary directory, never in the worktree, so it cannot overwrite edits."""
    tmp = tempfile.mkdtemp(prefix="pp-fixgen-")
    try:
        tar = os.path.join(tmp, "proof.tar")
        rc, out = ctx.git("archive", "--format=tar", "-o", tar, "HEAD", "--", "scripts/proof")
        if rc != 0:
            return Result(FAIL, f"git archive HEAD scripts/proof failed: {one_line(failure_line(out, rc), 60)}", out)
        work = os.path.join(tmp, "tree")
        os.makedirs(work)
        with tarfile.open(tar) as tf:
            tf.extractall(work, filter="data") if sys.version_info >= (3, 12) else tf.extractall(work)  # noqa: S202 - our own archive
        cases = os.path.join(work, "scripts", "proof", "fixtures", "cases")
        before = os.path.join(tmp, "before")
        shutil.copytree(cases, before)
        rc, out = ctx.sys.run([ctx.py, "scripts/proof/fixtures/_gen.py"], cwd=work)
        if rc != 0:
            return Result(FAIL, f"_gen.py exit {rc}: {one_line(failure_line(out, rc), 60)}", out)
        diff = tree_diff(before, cases)
        if diff:
            return Result(FAIL, f"fixture generator output is not committed ({len(diff)} file(s) differ, e.g. {diff[0]}); run scripts/proof/fixtures/_gen.py and commit",
                          "\n".join(diff))
        return Result(PASS, "generator output equals the committed cases", out)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def tree_diff(a: str, b: str) -> list:
    """Relative paths that exist on one side only or differ in content."""
    def files(root):
        return {os.path.relpath(os.path.join(dp, f), root) for dp, _, fs in os.walk(root) for f in fs}
    fa, fb = files(a), files(b)
    out = [f"only before: {f}" for f in sorted(fa - fb)] + [f"only after: {f}" for f in sorted(fb - fa)]
    for f in sorted(fa & fb):
        if Path(a, f).read_bytes() != Path(b, f).read_bytes():
            out.append(f"changed: {f}")
    return out


def applies_harness(ctx: Ctx) -> Optional[str]:
    return scoped(ctx, any(f.startswith(HARNESS_PREFIXES) for f in ctx.files), "scripts/bench/ab, scripts/proof, proof compose, proof deployment or tiecut golden file")


def step_harness(ctx: Ctx) -> Result:
    missing = [m for m in ("pytest", "yaml", "pyflakes", "pytest_cov") if not py_has(ctx, m)]
    if missing:
        return Result(SKIPPED, f"{', '.join(missing)} missing for the scripts/bench/ab and scripts/proof checks: " + VENV_HINT, unverified=True)
    env, tmp = harness_env()
    try:
        cov = ["--cov=scripts.proof.metrics", "--cov=scripts.proof.runner", "--cov=scripts.proof.visual", "--cov=scripts.proof.stack",
               "--cov=scripts.proof.comment", "--cov-report=term-missing", "--cov-fail-under=90"]
        r = run_cmds(ctx, [
            ("pyflakes", [ctx.py, "-m", "pyflakes", "scripts/proof"], {}),
            ("pytest scripts/bench/ab/tests", [ctx.py, "-m", "pytest", "scripts/bench/ab/tests", "-q", "-p", "no:cacheprovider"], {"env": env}),
            ("pytest scripts/proof/tests", [ctx.py, "-m", "pytest", "scripts/proof/tests", "-q", "-p", "no:cacheprovider", *cov], {"env": env}),
            ("fixtures --check", [ctx.py, "-m", "scripts.proof.metrics", "scripts/proof/fixtures/cases", "--check"], {}),
        ], "pyflakes + ab tests + proof tests (coverage floor 90) + fixtures --check")
        if r.status != PASS:
            return r
        g = generator_drift(ctx)
        g.log = r.log + "\n" + g.log
        if g.status == PASS:
            g.detail = r.detail + " + generator output committed"
            g.partial = "`docker compose config -q` of the proof stack (proof-metrics.yaml) was not run: no Docker locally"
        return g
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def step_charts(ctx: Ctx) -> Result:
    exe = ctx.sys.which("helm")
    if not exe:
        return Result(SKIPPED, "helm not installed; CI job helm runs lint + template tests", unverified=True)
    return run_cmds(ctx, [
        ("helm lint", [exe, "lint", "charts/victoria-lakehouse/", "--strict"], {}),
        ("test_images.sh", ["bash", "charts/victoria-lakehouse/test_images.sh"], {}),
        ("test_templates.sh", ["bash", "charts/victoria-lakehouse/test_templates.sh"], {}),
        ("test_release_image_tags.py", [ctx.py, "scripts/ci/tests/test_release_image_tags.py"], {}),
    ], "helm lint --strict + test_images.sh + test_templates.sh + test_release_image_tags.py")


def step_deps(ctx: Ctx) -> Result:
    if not ctx.sys.which("make"):
        return Result(SKIPPED, "make not installed", unverified=True)
    return run_cmds(ctx, [("make deps", ["make", "deps-logs", "deps-traces", "deps-vt"], {})], "VictoriaLogs/VictoriaTraces trees present")


def step_conformance(ctx: Ctx) -> Result:
    if not ctx.sys.which("make"):
        return Result(SKIPPED, "make not installed", unverified=True)
    return run_cmds(ctx, [("make conformance-check", ["make", "conformance-check"], {"env": ctx.go_env()})],
                    "confgen -check, conformance tests, ab selection check (CI runs it on every pull request)")


# ------------------------------------------------------------------------------------------ Go steps
def module_dir(ctx: Ctx, module: str) -> Path:
    return ctx.root / TRACES_DIR if module == "traces" else ctx.root


def go_compiles(module: str):
    """The Go compile-time jobs (lint, vet, build) are required checks that always run in CI. They can only change
    when a Go file, an embedded asset or a module-wide input changed (a logs-module change reaches the traces module
    through its replace directive)."""
    def applies(ctx: Ctx) -> Optional[str]:
        if ctx.scope_all or go_plan(ctx)[module]["compile"]:
            return None
        return f"no Go file, embedded asset or module-wide input of the {module} build changed (CI runs this job; the diff cannot change its result)"
    return applies


def step_lint(module: str):
    def run(ctx: Ctx) -> Result:
        exe = os.path.expanduser(ctx.sys.environ.get("GOLANGCI_LINT") or "") or ctx.sys.which("golangci-lint")
        if not exe:
            return Result(SKIPPED, "golangci-lint not installed (CI pins it in ci.yaml GOLANGCI_LINT_VERSION); set GOLANGCI_LINT=<path>", unverified=True)
        cmds = []
        for tags, pats, only_new in lint_invocations(module):
            cmd = [exe, "run"]
            if only_new:
                cmd.append(f"--new-from-rev={ctx.lint_base()}")
            if tags:
                cmd += ["--build-tags", tags]
            cmds.append(("golangci-lint", cmd + pats, {"cwd": module_dir(ctx, module), "env": ctx.go_env()}))
        return run_cmds(ctx, cmds, "whole packages as CI (the tagged tests/ runs: findings new since the merge base)" + version_note(ctx, "golangci-lint", exe))
    return run


def gofmt_paths(ctx: Ctx, module: str) -> list:
    if module == "traces":
        return ["internal/", *[n for n in ctx.sys.listdir(TRACES_DIR) if n.endswith(".go")]]
    return ["cmd/", "internal/"]


def step_vet(module: str):
    def run(ctx: Ctx) -> Result:
        kw = {"cwd": module_dir(ctx, module), "env": ctx.go_env()}
        rc, out = ctx.sys.run(["gofmt", "-s", "-l", *gofmt_paths(ctx, module)], **kw)
        log = f"$ gofmt -s -l\n{out}"
        if rc != 0:
            return Result(FAIL, f"gofmt exit {rc}: {one_line(failure_line(out, rc), 70)}", log)
        if out.strip():
            return Result(FAIL, f"gofmt -s required on: {one_line(' '.join(out.split()), 90)}", log)
        r = run_cmds(ctx, [("go vet", ["go", "vet", *vet_patterns(module)], kw)], "gofmt -s + go vet, the whole module as CI")
        r.log = log + "\n" + r.log
        return r
    return run


def step_build(module: str):
    def run(ctx: Ctx) -> Result:
        kw = {"cwd": module_dir(ctx, module), "env": ctx.go_env()}
        return run_cmds(ctx, [("go build", ["go", "build", *vet_patterns(module)], kw)], "whole module builds (host platform)")
    return run


def go_list_dirs(ctx: Ctx, module: str, pats) -> tuple:
    """(module-relative package dirs, error text) of the packages the patterns match."""
    base = module_dir(ctx, module)
    rc, out = ctx.sys.run(["go", "list", "-f", "{{.Dir}}", *pats], cwd=base, env=ctx.go_env())
    if rc != 0:
        return [], f"go list exit {rc}: {one_line(failure_line(out, rc), 70)}"
    return sorted(os.path.relpath(ln.strip(), str(base)) for ln in out.splitlines() if ln.strip()), ""


def is_parquets3(d: str) -> bool:
    return d.rstrip("/").endswith("internal/storage/parquets3")


def test_targets(ctx: Ctx, module: str) -> dict:
    """{module-relative package dir: -run regex or ""} of the packages the diff makes worth running."""
    st = go_plan(ctx)[module]
    base = module_dir(ctx, module)
    out: dict = {}
    for d in vet_test_dirs(module, st["dirs"]):
        if (base / d).is_dir() and any((base / d).glob("*.go")):
            out[d] = ""
    for d, run in st["readers"].items():
        if (base / d).is_dir() and any((base / d).glob("*.go")) and out.get(d, None) != "":
            out[d] = run
    return out


def go_test_cmds(ctx: Ctx, module: str, targets: dict) -> list:
    """-short only for parquets3: that is the only package CI runs with it (ci.yaml 'Test logs module')."""
    common = ["-count=1", "-p", "2", "-timeout=10m"] + (["-race"] if ctx.race else [])
    kw = {"cwd": module_dir(ctx, module), "env": ctx.go_env()}
    whole = sorted(d for d, r in targets.items() if r == "" and not is_parquets3(d))
    short = sorted(d for d, r in targets.items() if r == "" and is_parquets3(d))
    cmds = []
    if whole:
        cmds.append(("go test", ["go", "test", *common, *[pattern(d) for d in whole]], kw))
    if short:
        cmds.append(("go test -short (parquets3)", ["go", "test", "-short", *common, *[pattern(d) for d in short]], kw))
    for run in sorted({r for r in targets.values() if r}):
        pk = sorted(d for d, r in targets.items() if r == run)
        cmds.append(("go test -run", ["go", "test", "-run", run, *common, *[pattern(d) for d in pk]], kw))
    return cmds


def step_test(module: str):
    def run(ctx: Ctx) -> Result:
        st = go_plan(ctx)[module]
        if ctx.scope_all:
            pats = ["./internal/...", "./cmd/..."] if module == "logs" else ["./internal/...", "."]
            dirs, err = go_list_dirs(ctx, module, pats)
            if err or not dirs:
                return Result(FAIL, err or "go list found no package to test")
            targets = {d: "" for d in dirs}
        elif st["wide"]:
            return Result(SKIPPED, "go.mod/go.sum/patches/.golangci.yml changed for this module: lint, vet and build ran module-wide, "
                          "but CI runs the whole suite and this step cannot pick packages; run make pre-push PREPUSH_SCOPE=all", unverified=True)
        else:
            targets = test_targets(ctx, module)
        unmapped = [] if ctx.scope_all else st["unmapped"]
        if not targets:
            if unmapped:
                return Result(SKIPPED, f"cannot tell which Go tests read {one_line(', '.join(unmapped), 60)} ({len(unmapped)} path(s) with no row in GO_TEST_READERS "
                              "or GO_INERT); CI runs the whole suite: run PREPUSH_SCOPE=all", unverified=True)
            return Result(SKIPPED, "no changed path is a Go package or is read by a Go test (CI runs this job; the diff cannot change its result)")
        cmds = go_test_cmds(ctx, module, targets)
        r = run_cmds(ctx, cmds, f"{len(targets)} package(s); -short only for parquets3 as CI" + (", -race" if ctx.race else ", no -race (CI uses it)"))
        if r.status == PASS and unmapped:
            r.partial = f"{len(unmapped)} changed path(s) no row maps to a Go test ({one_line(', '.join(unmapped), 50)}): CI runs the whole suite"
        elif r.status == PASS and not ctx.scope_all:
            r.partial = "packages picked from the diff, unsharded, no coverage gate (CI runs every package)"
        return r
    return run


def go_test_applies(module: str):
    def applies(ctx: Ctx) -> Optional[str]:
        st = go_plan(ctx)[module]
        if ctx.scope_all or st["wide"] or st["dirs"] or st["readers"] or st["unmapped"]:
            return None
        return "no changed path is a Go package or is read by a Go test (CI runs this job; the diff cannot change its result)"
    return applies


def build_steps() -> list:
    always = lambda ctx: None  # noqa: E731
    steps = [
        Step("fetch-base", always, step_fetch),
        Step("scope", always, step_scope),
        Step("contains-main", always, step_contains_main),
        Step("commits-signed", always, step_signed),
        Step("no-ai-attribution", always, step_attribution),
        Step("registry-gate", always, step_registry_gate),
        Step("changelog-gate", always, step_changelog),
        Step("ci-script-tests", applies_ci_scripts, step_ci_scripts),
        Step("registry-gate-selftests", applies_gate_selftests, step_gate_selftests),
        Step("lint-logs-selftests", applies_lint_logs_selftests, step_lint_logs_selftests),
        Step("actionlint", lambda c: scoped(c, under(c.files, ".github/"), ".github/ file"), step_actionlint),
        Step("zizmor", lambda c: scoped(c, under(c.files, ".github/"), ".github/ file"), step_zizmor),
        Step("harness-tests", applies_harness, step_harness),
        Step("chart-tests", lambda c: scoped(c, under(c.files, "charts/"), "charts/ file"), step_charts),
        Step("deps", always, step_deps),
        Step("conformance-check", always, step_conformance),
    ]
    for module in ("logs", "traces"):
        steps.append(Step(f"golangci-lint ({module})", go_compiles(module), step_lint(module)))
        steps.append(Step(f"go vet ({module})", go_compiles(module), step_vet(module)))
        steps.append(Step(f"go build ({module})", go_compiles(module), step_build(module)))
        steps.append(Step(f"go test ({module})", go_test_applies(module), step_test(module)))
    return steps


# ------------------------------------------------------------------------------------------ run + report
def execute(ctx: Ctx, steps, progress: Optional[Callable[[str], None]] = None) -> list:
    outcomes = []
    for step in steps:
        t0 = time.monotonic()
        try:
            reason = step.applies(ctx)
            if reason:
                res = Result(SKIPPED, reason)
            else:
                if progress:
                    progress(f"... {step.name}")
                res = step.run(ctx)
        except Exception as ex:  # noqa: BLE001 - a crashing step is a FAIL, never a silent pass
            res = Result(FAIL, f"internal error: {type(ex).__name__}: {ex}")
        outcomes.append(Outcome(step.name, res.status, res.detail, time.monotonic() - t0, res.log, res.unverified, res.partial))
    return outcomes


def exit_code(outcomes) -> int:
    return 1 if any(o.status == FAIL for o in outcomes) else 0


def fmt_secs(s: float) -> str:
    return f"{s:.1f}s" if s < 100 else f"{s / 60:.1f}m"


def render_table(outcomes) -> str:
    rows = [("STEP", "RESULT", "TIME", "DETAIL")]
    for o in outcomes:
        rows.append((o.name, o.status, fmt_secs(o.seconds), one_line(o.detail, 110)))
    w = [max(len(r[i]) for r in rows) for i in range(3)]
    lines = []
    for i, r in enumerate(rows):
        lines.append(f"{r[0]:<{w[0]}}  {r[1]:<{w[1]}}  {r[2]:>{w[2]}}  {r[3]}".rstrip())
        if i == 0:
            lines.append("-" * min(150, sum(w) + 6 + 40))
    return "\n".join(lines)


def not_verified(outcomes) -> list:
    """Rows that did not fully check what CI checks: skipped for a missing tool or input, or run in part."""
    return [o for o in outcomes if o.unverified or (o.partial and o.status != FAIL)]


def jobs_not_run(outcomes) -> tuple:
    """([always-run CI jobs with a local step that was SKIPPED], [always-run CI jobs with no local step at all])."""
    status = {o.name: o.status for o in outcomes}
    skipped, never = [], []
    for job, spec in CI_JOBS.items():
        if not spec["always"]:
            continue
        if not spec["local"]:
            never.append(job)
            continue
        off = [n for n in spec["local"] if status.get(n) == SKIPPED]
        if off:
            skipped.append(f"{job} ({', '.join(off)})")
    return skipped, never


def render_summary(outcomes) -> str:
    n = {s: sum(1 for o in outcomes if o.status == s) for s in (PASS, FAIL, SKIPPED)}
    total = sum(o.seconds for o in outcomes)
    nv = not_verified(outcomes)
    word = "FAIL" if n[FAIL] else "PASS"
    mark = f" ({len(nv)} not verified)" if nv else ""
    out = [f"RESULT: {word}{mark} - {n[FAIL]} failed, {n[PASS]} passed, {n[SKIPPED]} skipped in {fmt_secs(total)}"]
    if nv:
        out.append("NOT VERIFIED locally (CI still runs these, or runs more than this did):")
        for o in nv:
            why = o.partial or o.detail
            out.append(f"  {o.name}: {one_line(why, 150)}")
    skipped, never = jobs_not_run(outcomes)
    if skipped:
        out.append("Always-run CI jobs with a local step that was skipped: " + "; ".join(skipped))
    if never:
        out.append("Always-run CI jobs with no local equivalent (they cannot be run here): " + ", ".join(never))
    return "\n".join(out)


def render_failures(outcomes, logdir: Optional[Path]) -> str:
    parts = []
    for o in outcomes:
        if o.status == FAIL:
            parts.append(f"--- {o.name}: {o.detail}")
            if o.log.strip():
                parts.append(tail(o.log, 25))
    if logdir:
        parts.append(f"full logs: {logdir}")
    return "\n".join(parts)


def render_checklist() -> str:
    lines = ["PR evidence checklist (skill section 1a; the PR body is not parsed, check it yourself):"]
    for i, p in enumerate(EVIDENCE_PARTS, 1):
        lines.append(f"  {i:>2}. {p}")
    for p in EVIDENCE_EXTRA:
        lines.append(f"   -  {p}")
    return "\n".join(lines)


def log_dir(ctx: Ctx) -> Path:
    """One fixed directory per worktree (under its git dir, never committed), overwritten by every run."""
    rc, out = ctx.git("rev-parse", "--git-path", "pre-push-logs")
    rel = out.strip().splitlines()[-1] if rc == 0 and out.strip() else ""
    d = (ctx.root / rel) if rel else Path(tempfile.gettempdir()) / "pre-push-logs"
    return d


def write_logs(outcomes, d: Path) -> Optional[Path]:
    if not any(o.log.strip() for o in outcomes):
        return None
    d.mkdir(parents=True, exist_ok=True)
    for old in d.glob("*.log"):
        old.unlink()
    for o in outcomes:
        if o.log.strip():
            (d / (re.sub(r"[^A-Za-z0-9._-]+", "_", o.name) + ".log")).write_text(o.log, encoding="utf-8")
    return d


def parse_args(argv):
    ap = argparse.ArgumentParser(description="Preview the fast, always-run tier of CI locally before a push.")
    ap.add_argument("--base", default="origin/main", help="base to compare with (default origin/main)")
    ap.add_argument("--scope", default="diff", help="diff: only what the diff touches; all: every step, whatever the diff (make: PREPUSH_SCOPE)")
    ap.add_argument("--race", action="store_true", help="add -race to the go test")
    ap.add_argument("--offline", action="store_true", default=os.environ.get("PRE_PUSH_OFFLINE") == "1",
                    help="do not fetch origin/main (the verdict may be against a stale base)")
    return ap.parse_args(argv)


def main(argv=None, system: Optional[Sys] = None, out=None) -> int:
    out = out or sys.stdout
    a = parse_args(sys.argv[1:] if argv is None else argv)
    if a.scope not in ("diff", "all"):
        print(f"pre-push: unknown PREPUSH_SCOPE {a.scope!r} (diff or all)", file=sys.stderr)
        return 2
    if system is None:
        p = subprocess.run(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if p.returncode != 0:
            print("pre-push: not inside a git repository", file=sys.stderr)
            return 2
        system = Sys(Path(p.stdout.strip()))
    ctx = Ctx(sys=system, base=a.base, scope_all=a.scope == "all", race=a.race, offline=a.offline,
              py=os.path.expanduser(system.environ.get("PRE_PUSH_PYTHON") or sys.executable or "python3"))
    rc, _ = ctx.git("rev-parse", "--verify", "-q", a.base + "^{commit}")
    if rc != 0 and not (a.base.startswith("origin/") and not a.offline):
        print(f"pre-push: base {a.base} does not resolve", file=sys.stderr)
        return 2
    print(f"pre-push: base {a.base}, scope {a.scope}{', -race' if a.race else ''}, repo {ctx.root}", file=out)
    outcomes = execute(ctx, build_steps(), progress=lambda m: print(m, file=sys.stderr, flush=True))
    logdir = write_logs(outcomes, log_dir(ctx))
    print("", file=out)
    print(render_table(outcomes), file=out)
    print("", file=out)
    print(render_summary(outcomes), file=out)
    fails = render_failures(outcomes, logdir)
    if exit_code(outcomes):
        print("", file=out)
        print(fails, file=out)
    print("", file=out)
    print(render_checklist(), file=out)
    return exit_code(outcomes)


if __name__ == "__main__":
    raise SystemExit(main())
