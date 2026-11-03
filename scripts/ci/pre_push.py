#!/usr/bin/env python3
"""`make pre-push`: the scoped fast tier of CI, run locally before a push.

It runs, in one go and with the same inputs CI uses, the checks that fail most often AFTER a push, so
a red head is caught before it costs a CI cycle. It re-implements nothing: every step calls the tool
or script CI calls (scripts/ci/pr_classify.py, gate_local.sh, check_changelog_pr.py,
changelog_fragments.py, `make conformance-check`, actionlint, zizmor, the scripts/ci unit tests,
pytest of the proof harness, helm, golangci-lint, go vet, go test).

    make pre-push                  what the diff against origin/main touches
    make pre-push SCOPE=all        every step, whatever the diff
    make pre-push RACE=1           adds -race to the focused go test

Scope comes from the committed diff `merge-base(origin/main, HEAD)..HEAD` (what a push publishes),
the same input scripts/ci/pr_classify.py --ci-scope gets in CI. Uncommitted edits are NOT judged by the
gates (they read HEAD); the lint and test steps read the working tree, so a dirty tree is reported.

Result per step: PASS, FAIL, or SKIPPED(reason). Exit 0 when nothing FAILed, 1 when a step FAILed,
2 on a usage error (not a git repository, base not resolvable); `make` turns any non-zero exit into 2.
The full output of every step that printed any is kept in <git dir>/pre-push-logs/, overwritten by each run. A SKIPPED step that is skipped
because a tool is missing is listed as NOT VERIFIED: CI will still run it.

This is a preview, not a replacement: it cannot run Docker-stack jobs, flaky-test retries, code
scanning, or the review-thread checks (see docs and CONTRIBUTING.md, "make pre-push").

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
# Paths whose change can make `make conformance-check` fail: the registry and its tests, the generated
# docs, the changelog fragments (an `added` bullet needs a feature), the perf-shape sets, and any product file.
CONFORMANCE_PREFIXES = ("tests/", "docs/", "changelog.d/", "scripts/bench/ab/")
CONFORMANCE_FILES = ("README.md", "UPSTREAM_COVERAGE.md", "CHANGELOG.md")
# The cheap static tests of workflows, tool pins, image digests and Dependabot coverage (about 30 s together,
# measured). They read .github/, the shell scripts of scripts/ci, the Makefile, Dockerfiles, compose files and
# manifests, so they run when the diff touches any of those (STATIC_*). The full `unittest discover` of
# scripts/ci/tests that the changelog-tests job runs took about 400 s here (measured), so it runs for
# SCOPE=all only; a changed scripts/ci file runs its own tests (tests_for_ci_file) plus the static set.
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
# conformance.yaml runs these next to `make conformance-check` (about 156 s on CI, 345 s + 86 s here under load, measured).
GATE_SELFTESTS = ("scripts/ci/tests/test_check_registry_touch.sh", "scripts/ci/tests/test_gate_workflow_emulation.sh")
STATIC_PREFIXES = (".github/", "scripts/ci/", "deployment/", "charts/")
STATIC_NAMES = re.compile(r"(^|/)(Makefile|go\.mod|go\.sum|Dockerfile[^/]*|requirements[^/]*\.(txt|in)|docker-compose[^/]*\.ya?ml)$")
# Inputs that shape a whole Go module: no package is "touched", yet CI runs everything for them.
WIDE_MODULE_FILES = {
    "go.mod": ("logs",), "go.sum": ("logs",),
    "lakehouse-traces/go.mod": ("traces",), "lakehouse-traces/go.sum": ("traces",),
    ".golangci.yml": ("logs", "traces"),
}
# patches/<set>/...: the set names the module it patches; a file directly under patches/ concerns both.
PATCH_SET_MODULES = {"vl-logs": ("logs",), "vl-traces": ("traces",), "vt-traces": ("traces",)}
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


# ------------------------------------------------------------------------------------------ data types
@dataclasses.dataclass
class Result:
    status: str
    detail: str = ""
    log: str = ""
    unverified: bool = False  # SKIPPED because a tool is missing: CI will still run the check


@dataclasses.dataclass
class Outcome:
    name: str
    status: str
    detail: str
    seconds: float
    log: str = ""
    unverified: bool = False


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


# ------------------------------------------------------------------------------------------ pure scope logic
def under(files, *prefixes) -> bool:
    return any(f.startswith(p) for f in files for p in prefixes)


def is_go_path(f: str) -> bool:
    return not (f.startswith("deps/") or "/deps/" in f or "/vendor/" in f) and (f.endswith(".go") or "/testdata/" in f)


def go_pkg_dir(f: str) -> str:
    """The directory of the Go package a changed path belongs to (testdata belongs to its parent)."""
    if "/testdata/" in f:
        return f[: f.index("/testdata/")]
    return os.path.dirname(f) or "."


def split_modules(files) -> dict:
    """Module name -> sorted package dirs relative to the module root, for the changed Go paths."""
    out: dict = {"logs": set(), "traces": set()}
    pre = TRACES_DIR + "/"
    for f in files:
        if not is_go_path(f):
            continue
        d = go_pkg_dir(f)
        if f.startswith(pre):
            rel = d[len(pre):] if d != TRACES_DIR else "."
            out["traces"].add(rel or ".")
        else:
            out["logs"].add(d)
    return {k: sorted(v) for k, v in out.items()}


def pattern(d: str) -> str:
    return "./" + d if d != "." else "."


def vet_test_dirs(module: str, dirs) -> list:
    """Directories that get go vet and go test: tests/ holds build-tagged suites that need Docker (CI jobs
    e2e, parity and conformance run them), so it is linted but not run here."""
    return [d for d in dirs if not (module == "logs" and (d == "tests" or d.startswith("tests/")))]


def lint_invocations(module: str, dirs, scope_all: bool) -> list:
    """[(build tags, [patterns])] mirroring the lint steps of .github/workflows/ci.yaml."""
    if module == "traces":
        if scope_all:
            return [("", ["./..."])]
        return [("", [pattern(d) for d in dirs])] if dirs else []
    if scope_all:
        return [
            ("", ["./internal/...", "./cmd/..."]),
            ("parity,chaos", ["./tests/parity/..."]),
            ("e2e,chaos", ["./tests/ingestmatrix/...", "./tests/e2e/...", "./tests/conformance/..."]),
        ]
    plain = [pattern(d) for d in dirs if d.startswith(("internal/", "cmd/")) or d in ("internal", "cmd")]
    parity = [pattern(d) for d in dirs if d == "tests/parity" or d.startswith("tests/parity/")]
    tagged = [
        pattern(d) for d in dirs
        if re.match(r"tests/(ingestmatrix|e2e|conformance)(/|$)", d)
    ]
    out = []
    if plain:
        out.append(("", plain))
    if parity:
        out.append(("parity,chaos", parity))
    if tagged:
        out.append(("e2e,chaos", tagged))
    return out


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


# ------------------------------------------------------------------------------------------ step helpers
def scoped(ctx: Ctx, present: bool, what: str) -> Optional[str]:
    """None when the step should run: SCOPE=all, or the diff touches `what`."""
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
            lines = [ln for ln in out.strip().splitlines() if ln.strip()]
            why = lines[-1] if lines else f"exit {rc}"
            return Result(FAIL, f"{label} exit {rc}: {one_line(why, 70)}", "\n".join(log))
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
    dirty = ctx.git("status", "--porcelain", "--untracked-files=no")[1].strip()
    note = f"; {len(dirty.splitlines())} uncommitted tracked change(s) NOT seen by the gates" if dirty else ""
    mode = "SCOPE=all" if ctx.scope_all else "diff"
    return Result(
        PASS,
        f"{mode}: {len(ctx.files)} file(s), ci_class={c.get('ci_class', '?')} skip_go={c.get('skip_go', '?')} "
        f"product={c.get('product', '?')}{note}",
        out,
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


def step_registry_gate(ctx: Ctx) -> Result:
    env = {"REGISTRY_GATE_OFFLINE": "1", "GOWORK": "off"}  # the fetch step already fetched (or was told not to)
    r = run_cmds(ctx, [("registry gate", ["bash", "scripts/ci/gate_local.sh", ctx.base], {"env": env})])
    verdict, kind, rule = gate_detail(r.log)
    if r.status == PASS:
        r.detail = one_line(f"{verdict or 'passed'}; {kind}" if kind else verdict or "passed", 100)
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
        # the registry gate self-tests of conformance.yaml: shell scripts, so discovery does not run them
        cmds += [(os.path.basename(t), ["bash", t], {"env": {"GOWORK": "off"}}) for t in GATE_SELFTESTS]
        return run_cmds(ctx, cmds, "scripts/ci/tests full discovery (changelog-tests) + registry gate self-tests (conformance.yaml)")
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
    return run_cmds(ctx, cmds, f"{len(py)} python + {len(sh)} shell test file(s)")


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
    return r


def step_harness(ctx: Ctx) -> Result:
    if not py_has(ctx, "pytest") or not py_has(ctx, "yaml"):
        return Result(SKIPPED, "pytest/PyYAML missing for scripts/bench/ab and scripts/proof tests: " + VENV_HINT, unverified=True)
    cmds = [("pytest", [ctx.py, "-m", "pytest", "scripts/bench/ab/tests", "scripts/proof/tests", "-q", "-p", "no:cacheprovider"], {})]
    return run_cmds(ctx, cmds, "scripts/bench/ab/tests + scripts/proof/tests")


def step_charts(ctx: Ctx) -> Result:
    exe = ctx.sys.which("helm")
    if not exe:
        return Result(SKIPPED, "helm not installed; CI job helm runs lint + template tests", unverified=True)
    return run_cmds(ctx, [
        ("helm lint", [exe, "lint", "charts/victoria-lakehouse/", "--strict"], {}),
        ("test_images.sh", ["bash", "charts/victoria-lakehouse/test_images.sh"], {}),
        ("test_templates.sh", ["bash", "charts/victoria-lakehouse/test_templates.sh"], {}),
    ], "helm lint --strict + test_images.sh + test_templates.sh")


def step_deps(ctx: Ctx) -> Result:
    if not ctx.sys.which("make"):
        return Result(SKIPPED, "make not installed", unverified=True)
    return run_cmds(ctx, [("make deps", ["make", "deps-logs", "deps-traces", "deps-vt"], {})], "VictoriaLogs/VictoriaTraces trees present")


def step_conformance(ctx: Ctx) -> Result:
    if not ctx.sys.which("make"):
        return Result(SKIPPED, "make not installed", unverified=True)
    return run_cmds(ctx, [("make conformance-check", ["make", "conformance-check"], {"env": ctx.go_env()})],
                    "confgen -check, conformance tests, ab selection check")


def applies_conformance(ctx: Ctx) -> Optional[str]:
    present = ctx.classify.get("product") == "1" or not ctx.classify_ok and bool(ctx.files)
    present = present or under(ctx.files, *CONFORMANCE_PREFIXES) or any(f in CONFORMANCE_FILES for f in ctx.files)
    return scoped(ctx, present, "registry, docs, test, fragment or product file")


def go_state(ctx: Ctx) -> dict:
    """Cached per module: the touched package dirs, `all` (SCOPE=all or a module-wide input changed) and `wide`."""
    if "go" not in ctx.cache:
        mods = split_modules(ctx.files)
        wide = wide_modules(ctx.files)
        ctx.cache["go"] = {
            m: {"dirs": mods[m], "all": ctx.scope_all or m in wide, "wide": m in wide} for m in ("logs", "traces")
        }
    return ctx.cache["go"]


def go_skip(ctx: Ctx, module: str) -> Optional[str]:
    if not ctx.scope_all and ctx.classify_ok and ctx.classify.get("skip_go") == "true":
        return "CI scope: skip_go=true"
    st = go_state(ctx)[module]
    if not st["dirs"] and not st["all"]:
        return f"diff touches no Go file of the {module} module"
    return None


def module_dir(ctx: Ctx, module: str) -> Path:
    return ctx.root / TRACES_DIR if module == "traces" else ctx.root


def live_dirs(ctx: Ctx, module: str, dirs) -> list:
    base = module_dir(ctx, module)
    return [d for d in dirs if (base / d).is_dir() and any((base / d).glob("*.go"))]


def vt_patterns(ctx: Ctx, module: str) -> list:
    st = go_state(ctx)[module]
    if st["all"]:
        return ["./..."] if module == "traces" else ["./internal/...", "./cmd/..."]
    return [pattern(d) for d in live_dirs(ctx, module, vet_test_dirs(module, st["dirs"]))]


def step_lint(module: str):
    def run(ctx: Ctx) -> Result:
        exe = os.path.expanduser(ctx.sys.environ.get("GOLANGCI_LINT") or "") or ctx.sys.which("golangci-lint")
        if not exe:
            return Result(SKIPPED, "golangci-lint not installed (CI pins it in ci.yaml GOLANGCI_LINT_VERSION); set GOLANGCI_LINT=<path>", unverified=True)
        st = go_state(ctx)[module]
        invs = lint_invocations(module, [] if st["all"] else live_dirs(ctx, module, st["dirs"]), st["all"])
        if not invs:
            return Result(SKIPPED, "no lintable package in the diff (tests/ packages other than the CI-linted ones)")
        cmds = []
        for tags, pats in invs:
            cmd = [exe, "run", f"--new-from-rev={ctx.base}"]
            if tags:
                cmd += ["--build-tags", tags]
            cmds.append(("golangci-lint", cmd + pats, {"cwd": module_dir(ctx, module), "env": ctx.go_env()}))
        r = run_cmds(ctx, cmds, f"no new findings vs {ctx.base}" + version_note(ctx, "golangci-lint", exe))
        return r
    return run


def step_vet(module: str):
    def run(ctx: Ctx) -> Result:
        pats = vt_patterns(ctx, module)
        if not pats:
            return Result(SKIPPED, "no vet-able package in the diff")
        return run_cmds(ctx, [("go vet", ["go", "vet", *pats], {"cwd": module_dir(ctx, module), "env": ctx.go_env()})],
                        f"{len(pats)} package pattern(s)")
    return run


def step_test(module: str):
    def run(ctx: Ctx) -> Result:
        st = go_state(ctx)[module]
        if st["wide"] and not ctx.scope_all:
            return Result(SKIPPED, "go.mod/go.sum/patches/.golangci.yml changed for this module: lint and vet ran module-wide, "
                          "but CI runs the whole suite and this step cannot pick packages; run make pre-push SCOPE=all", unverified=True)
        pats = vt_patterns(ctx, module)
        if not pats:
            return Result(SKIPPED, "no testable package in the diff")
        cmd = ["go", "test", "-short", "-count=1", "-p", "2", "-timeout=10m"]
        if ctx.race:
            cmd.append("-race")
        return run_cmds(ctx, [("go test", cmd + pats, {"cwd": module_dir(ctx, module), "env": ctx.go_env()})],
                        f"{len(pats)} package pattern(s) -short" + (" -race" if ctx.race else ""))
    return run


def go_applies(module: str):
    return lambda ctx: go_skip(ctx, module)


def deps_applies(ctx: Ctx) -> Optional[str]:
    for m in ("logs", "traces"):
        if go_skip(ctx, m) is None:
            return None
    return "no Go step in scope"


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
        Step("actionlint", lambda c: scoped(c, under(c.files, ".github/"), ".github/ file"), step_actionlint),
        Step("zizmor", lambda c: scoped(c, under(c.files, ".github/"), ".github/ file"), step_zizmor),
        Step("harness-tests", lambda c: scoped(c, under(c.files, "scripts/bench/ab/", "scripts/proof/"), "scripts/bench/ab or scripts/proof file"), step_harness),
        Step("chart-tests", lambda c: scoped(c, under(c.files, "charts/"), "charts/ file"), step_charts),
        Step("deps", deps_applies, step_deps),
        Step("conformance-check", applies_conformance, step_conformance),
    ]
    for module in ("logs", "traces"):
        steps.append(Step(f"golangci-lint ({module})", go_applies(module), step_lint(module)))
        steps.append(Step(f"go vet ({module})", go_applies(module), step_vet(module)))
        steps.append(Step(f"go test ({module})", go_applies(module), step_test(module)))
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
        outcomes.append(Outcome(step.name, res.status, res.detail, time.monotonic() - t0, res.log, res.unverified))
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


def render_summary(outcomes) -> str:
    n = {s: sum(1 for o in outcomes if o.status == s) for s in (PASS, FAIL, SKIPPED)}
    total = sum(o.seconds for o in outcomes)
    head = f"RESULT: {'FAIL' if n[FAIL] else 'PASS'} ({n[FAIL]} failed, {n[PASS]} passed, {n[SKIPPED]} skipped) in {fmt_secs(total)}"
    unv = [o for o in outcomes if o.unverified]
    out = [head]
    if unv:
        out.append("NOT VERIFIED locally (tool missing, CI will still run these): " + ", ".join(o.name for o in unv))
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
    ap = argparse.ArgumentParser(description="Run the scoped fast tier of CI locally before a push.")
    ap.add_argument("--base", default="origin/main", help="base to compare with (default origin/main)")
    ap.add_argument("--scope", default="diff", help="diff: only what the diff touches; all: every step regardless of the diff")
    ap.add_argument("--race", action="store_true", help="add -race to the focused go test")
    ap.add_argument("--offline", action="store_true", default=os.environ.get("PRE_PUSH_OFFLINE") == "1",
                    help="do not fetch origin/main (the verdict may be against a stale base)")
    return ap.parse_args(argv)


def main(argv=None, system: Optional[Sys] = None, out=None) -> int:
    out = out or sys.stdout
    a = parse_args(sys.argv[1:] if argv is None else argv)
    if a.scope not in ("diff", "all"):
        print(f"pre-push: unknown SCOPE {a.scope!r} (diff or all)", file=sys.stderr)
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
