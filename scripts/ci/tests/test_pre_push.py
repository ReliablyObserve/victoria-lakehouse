"""Tests of scripts/ci/pre_push.py (`make pre-push`): scope mapping, skip reasons, failure propagation,
exit codes, commit hygiene against a real git repository, and the Makefile wiring.

Run: python3 -m unittest scripts/ci/tests/test_pre_push.py
"""
from __future__ import annotations

import io
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
sys.path.insert(0, str(HERE.parent))
import pre_push as pp  # noqa: E402


_TMP = []


def mkdtemp(prefix="pp-test-"):
    d = tempfile.mkdtemp(prefix=prefix)
    _TMP.append(d)
    return d


def tearDownModule():
    for d in _TMP:
        shutil.rmtree(d, ignore_errors=True)


def real_calls(s):
    """The recorded commands without the `--version` probes that only feed a note."""
    return [c for c in s.calls if c[0][1:] != ["--version"]]


class FakeSys(pp.Sys):
    """Records every command; `script(cmd, kw)` returns (rc, out) or None for the default (0, '')."""

    def __init__(self, root, script=None, tools=(), environ=None):
        super().__init__(pathlib.Path(root), environ or {"PATH": ""})
        self.script = script
        self.tools = set(tools)
        self.calls = []

    def run(self, cmd, cwd=None, env=None, timeout=1800):
        kw = {"cwd": cwd, "env": env}
        self.calls.append((list(cmd), kw))
        if self.script:
            r = self.script(list(cmd), kw)
            if r is not None:
                return r
        return 0, ""

    def which(self, name):
        return "/fake/bin/" + name if name in self.tools else None


def make_ctx(files=(), classify=None, classify_ok=True, scope_all=False, race=False, sys_=None, root=None):
    root = root or mkdtemp()
    c = pp.Ctx(sys=sys_ or FakeSys(root), scope_all=scope_all, race=race, py="python3")
    c.files = list(files)
    c.classify = dict(classify or {})
    c.classify_ok = classify_ok
    c.merge_base = "abc123"
    return c


def running(ctx):
    """Names of the steps whose `applies` says run, among the ones scope can switch off."""
    always = {"fetch-base", "scope", "contains-main", "commits-signed", "no-ai-attribution", "registry-gate", "changelog-gate"}
    return {s.name for s in pp.build_steps() if s.name not in always and s.applies(ctx) is None}


# The proof-coverage generator lists every file that names a patch path as a consumer of that patch. This tool is
# not one, so the cases below join "patches/" with the set name instead of spelling the path out.
PATCHES = "patches/"
GO_LOGS = {"golangci-lint (logs)", "go vet (logs)", "go test (logs)"}
GO_TRACES = {"golangci-lint (traces)", "go vet (traces)", "go test (traces)"}
ALL_OPTIONAL = {s.name for s in pp.build_steps()} - {
    "fetch-base", "scope", "contains-main", "commits-signed", "no-ai-attribution", "registry-gate", "changelog-gate"}


class ScopeMapping(unittest.TestCase):
    CASES = [
        # name, files, classify, expected running steps
        ("nothing changed", [], {}, set()),
        ("docs page", ["docs/read-path.md"], {"product": "0", "skip_go": "true"}, {"conformance-check"}),
        ("workflow", [".github/workflows/ci.yaml"], {"product": "0", "skip_go": "true"},
         {"ci-script-tests", "actionlint", "zizmor"}),
        ("dockerfile", ["Dockerfile.logs"], {"product": "1", "skip_go": "false"}, {"ci-script-tests", "conformance-check"}),
        ("requirements lock", ["scripts/proof/requirements.txt"], {"product": "0", "skip_go": "true"},
         {"ci-script-tests", "harness-tests"}),
        ("compose file", ["deployment/docker/docker-compose-e2e.yml"], {"product": "0", "skip_go": "true"}, {"ci-script-tests"}),
        ("root go.mod: module-wide", ["go.mod"], {"product": "0", "skip_go": "false"}, {"ci-script-tests", "deps"} | GO_LOGS),
        ("go.sum", ["go.sum"], {"product": "0", "skip_go": "false"}, {"ci-script-tests", "deps"} | GO_LOGS),
        ("traces go.mod: module-wide", ["lakehouse-traces/go.mod"], {"product": "0", "skip_go": "false"},
         {"ci-script-tests", "deps"} | GO_TRACES),
        ("logs patch", [PATCHES + "vl-logs/x.patch"], {"product": "1", "skip_go": "false"},
         {"deps", "conformance-check"} | GO_LOGS),
        ("traces patch", [PATCHES + "vt-traces/x.go.src"], {"product": "1", "skip_go": "false"},
         {"deps", "conformance-check"} | GO_TRACES),
        ("shared patch", [PATCHES + "README.md"], {"product": "1", "skip_go": "false"},
         {"deps", "conformance-check"} | GO_LOGS | GO_TRACES),
        ("linter config", [".golangci.yml"], {"product": "0", "skip_go": "false"}, {"deps"} | GO_LOGS | GO_TRACES),
        ("ci script", ["scripts/ci/check_changelog_pr.py"], {"product": "0", "skip_go": "true"}, {"ci-script-tests"}),
        ("unrelated script", ["scripts/cost/model.py"], {"product": "0", "skip_go": "true"}, set()),
        ("ab harness", ["scripts/bench/ab/pr_smoke.py"], {"product": "0", "skip_go": "true"},
         {"harness-tests", "conformance-check"}),
        ("proof harness", ["scripts/proof/runner_fit.py"], {"product": "0", "skip_go": "true"}, {"harness-tests"}),
        ("chart", ["charts/victoria-lakehouse/values.yaml"], {"product": "1", "skip_go": "false"},
         {"chart-tests", "conformance-check", "ci-script-tests"}),
        ("registry row", ["tests/conformance/registry/rows/x.yaml"], {"product": "0", "skip_go": "false"},
         {"conformance-check"}),
        ("changelog fragment", ["changelog.d/9.fixed.md"], {"product": "0", "skip_go": "true"}, {"conformance-check"}),
        ("logs go file", ["internal/storage/x.go"], {"product": "1", "skip_go": "false"},
         {"deps", "conformance-check"} | GO_LOGS),
        ("logs testdata only", ["internal/storage/testdata/a.golden"], {"product": "1", "skip_go": "false"},
         {"deps", "conformance-check"} | GO_LOGS),
        ("traces go file", ["lakehouse-traces/internal/x/y.go"], {"product": "1", "skip_go": "false"},
         {"deps", "conformance-check"} | GO_TRACES),
        ("mirrored go in both", ["internal/a/a.go", "lakehouse-traces/internal/a/a.go"], {"product": "1", "skip_go": "false"},
         {"deps", "conformance-check"} | GO_LOGS | GO_TRACES),
        ("go file but CI scope skips go", ["internal/a/a.go"], {"product": "0", "skip_go": "true"}, set()),
        ("deps tree is not a diff", ["deps/VictoriaLogs/app/x.go"], {"product": "0", "skip_go": "false"}, set()),
        ("classifier failed: fail open", ["README.md"], None, {"conformance-check"}),
        ("prose readme", ["README.md"], {"product": "0", "skip_go": "true"}, {"conformance-check"}),
        ("makefile alone", ["Makefile"], {"product": "0", "skip_go": "false"}, {"ci-script-tests"}),
    ]

    def test_table(self):
        for name, files, classify, want in self.CASES:
            with self.subTest(name):
                ctx = make_ctx(files, classify, classify_ok=classify is not None)
                self.assertEqual(running(ctx), want)

    def test_scope_all_runs_everything_whatever_the_diff(self):
        ctx = make_ctx([], {"skip_go": "true"}, scope_all=True)
        self.assertEqual(running(ctx), ALL_OPTIONAL)

    def test_every_optional_step_has_a_scope_rule(self):
        # a step added without a scope rule would run on every diff: it must be in the table above or be always-on
        self.assertEqual(running(make_ctx([])), set())

    def test_skip_reason_names_what_is_missing(self):
        ctx = make_ctx(["docs/x.md"], {"product": "0", "skip_go": "true"})
        reasons = {s.name: s.applies(ctx) for s in pp.build_steps()}
        self.assertIn(".github/", reasons["actionlint"])
        self.assertIn("charts/", reasons["chart-tests"])
        self.assertEqual(reasons["go test (traces)"], "CI scope: skip_go=true")
        ctx2 = make_ctx(["docs/x.md"], {"product": "0", "skip_go": "false"})
        self.assertEqual(pp.go_skip(ctx2, "traces"), "diff touches no Go file of the traces module")
        ctx3 = make_ctx(["internal/a/a.go"], {"product": "0", "skip_go": "true"})
        self.assertEqual(pp.go_skip(ctx3, "logs"), "CI scope: skip_go=true")


class GoPlanning(unittest.TestCase):
    def test_split_modules(self):
        files = ["internal/a/a.go", "internal/a/a_test.go", "internal/b/testdata/x.json", "lakehouse-traces/main.go",
                 "lakehouse-traces/internal/t/t.go", "deps/x/y.go", "lakehouse-traces/deps/v/z.go", "README.md",
                 "internal/ui/vmui/index.html"]
        self.assertEqual(pp.split_modules(files), {"logs": ["internal/a", "internal/b"], "traces": [".", "internal/t"]})
        self.assertEqual(pp.split_modules(["go.mod", "patches/x.patch"]), {"logs": [], "traces": []})

    def test_tests_dir_is_linted_but_not_run(self):
        self.assertEqual(pp.vet_test_dirs("logs", ["internal/a", "tests/e2e", "tests", "scripts/ci/helmdrift"]),
                         ["internal/a", "scripts/ci/helmdrift"])
        self.assertEqual(pp.vet_test_dirs("traces", ["tests/x"]), ["tests/x"])

    def test_lint_invocations_mirror_ci(self):
        self.assertEqual(pp.lint_invocations("logs", ["internal/a", "scripts/ci/helmdrift"], False), [("", ["./internal/a"])])
        self.assertEqual(
            pp.lint_invocations("logs", ["internal/a", "tests/parity", "tests/conformance/x"], False),
            [("", ["./internal/a"]), ("parity,chaos", ["./tests/parity"]), ("e2e,chaos", ["./tests/conformance/x"])])
        self.assertEqual(pp.lint_invocations("logs", ["scripts/ci/helmdrift"], False), [])
        self.assertEqual(pp.lint_invocations("traces", [".", "internal/t"], False), [("", [".", "./internal/t"])])
        self.assertEqual(len(pp.lint_invocations("logs", [], True)), 3)
        self.assertEqual(pp.lint_invocations("traces", [], True), [("", ["./..."])])


def go_tree(root, *dirs):
    for d in dirs:
        p = pathlib.Path(root) / d
        p.mkdir(parents=True, exist_ok=True)
        (p / "x.go").write_text("package x\n")


class GoCommands(unittest.TestCase):
    def setUp(self):
        self.root = mkdtemp("pp-go-")
        go_tree(self.root, "internal/a", "lakehouse-traces/internal/t")

    def ctx(self, files, **kw):
        s = FakeSys(self.root, tools={"golangci-lint"})
        return make_ctx(files, {"skip_go": "false"}, sys_=s, root=self.root, **kw), s

    def test_go_test_is_short_count1_memcapped_and_not_race_by_default(self):
        ctx, s = self.ctx(["internal/a/a.go"])
        r = pp.step_test("logs")(ctx)
        self.assertEqual(r.status, pp.PASS)
        cmd, kw = s.calls[-1]
        self.assertEqual(cmd[:3], ["go", "test", "-short"])
        self.assertIn("-count=1", cmd)
        self.assertEqual(cmd[cmd.index("-p") + 1], "2")  # several packages share one host
        self.assertIn("-timeout=10m", cmd)
        self.assertNotIn("-race", cmd)
        self.assertEqual(cmd[-1], "./internal/a")
        self.assertEqual(kw["env"], {"GOWORK": "off", "GOMEMLIMIT": "4GiB"})
        self.assertEqual(pathlib.Path(kw["cwd"]), pathlib.Path(self.root))

    def test_race_flag_adds_race(self):
        ctx, s = self.ctx(["internal/a/a.go"], race=True)
        pp.step_test("logs")(ctx)
        self.assertIn("-race", s.calls[-1][0])

    def test_existing_gomemlimit_is_respected(self):
        s = FakeSys(self.root, environ={"PATH": "", "GOMEMLIMIT": "2GiB"})
        ctx = make_ctx(["internal/a/a.go"], {"skip_go": "false"}, sys_=s, root=self.root)
        pp.step_test("logs")(ctx)
        self.assertEqual(s.calls[-1][1]["env"]["GOMEMLIMIT"], "2GiB")

    def test_traces_runs_in_its_module_dir(self):
        ctx, s = self.ctx(["lakehouse-traces/internal/t/t.go"])
        pp.step_vet("traces")(ctx)
        cmd, kw = s.calls[-1]
        self.assertEqual(cmd, ["go", "vet", "./internal/t"])
        self.assertEqual(pathlib.Path(kw["cwd"]), pathlib.Path(self.root) / "lakehouse-traces")

    def test_deleted_package_dir_is_not_run(self):
        ctx, s = self.ctx(["internal/gone/g.go"])
        self.assertEqual(pp.step_test("logs")(ctx).status, pp.SKIPPED)
        self.assertEqual(s.calls, [])

    def test_lint_is_new_from_base_in_the_right_dir(self):
        ctx, s = self.ctx(["lakehouse-traces/internal/t/t.go"])
        r = pp.step_lint("traces")(ctx)
        self.assertEqual(r.status, pp.PASS)
        cmd, kw = real_calls(s)[0]
        self.assertEqual(cmd[1:4], ["run", "--new-from-rev=origin/main", "./internal/t"])
        self.assertEqual(kw["env"]["GOWORK"], "off")

    def test_golangci_lint_env_expands_the_home_directory(self):
        s = FakeSys(self.root, environ={"PATH": "", "GOLANGCI_LINT": "~/bin/golangci-lint"})
        ctx = make_ctx(["internal/a/a.go"], {"skip_go": "false"}, sys_=s, root=self.root)
        pp.step_lint("logs")(ctx)
        self.assertEqual(real_calls(s)[0][0][0], os.path.expanduser("~/bin/golangci-lint"))

    def test_scope_all_go_patterns(self):
        ctx, s = self.ctx([], scope_all=True)
        pp.step_test("logs")(ctx)
        self.assertEqual(s.calls[-1][0][-2:], ["./internal/...", "./cmd/..."])
        pp.step_test("traces")(ctx)
        self.assertEqual(s.calls[-1][0][-1], "./...")

    def test_missing_golangci_lint_is_skipped_and_unverified(self):
        s = FakeSys(self.root)
        ctx = make_ctx(["internal/a/a.go"], {"skip_go": "false"}, sys_=s, root=self.root)
        r = pp.step_lint("logs")(ctx)
        self.assertEqual((r.status, r.unverified), (pp.SKIPPED, True))
        self.assertIn("golangci-lint", r.detail)
        self.assertEqual(s.calls, [])


class ToolSkips(unittest.TestCase):
    def test_missing_tools_are_skipped_with_a_reason_and_marked_unverified(self):
        ctx = make_ctx()
        for fn, word in [(pp.step_actionlint, "actionlint"), (pp.step_zizmor, "zizmor"), (pp.step_charts, "helm")]:
            with self.subTest(word):
                r = fn(ctx)
                self.assertEqual((r.status, r.unverified), (pp.SKIPPED, True))
                self.assertIn(word, r.detail)

    def test_harness_without_pytest_points_at_the_venv(self):
        def script(cmd, kw):
            return (1, "No module named pytest") if cmd[:2] == ["python3", "-c"] else None
        ctx = make_ctx(sys_=FakeSys(mkdtemp(), script))
        r = pp.step_harness(ctx)
        self.assertEqual((r.status, r.unverified), (pp.SKIPPED, True))
        self.assertIn("venv", r.detail)
        self.assertIn("PRE_PUSH_PYTHON", r.detail)

    def test_present_tools_run_the_ci_commands(self):
        s = FakeSys(mkdtemp(), tools={"actionlint", "zizmor", "helm"})
        ctx = make_ctx(sys_=s)
        self.assertEqual(pp.step_actionlint(ctx).status, pp.PASS)
        self.assertEqual(real_calls(s)[-1][0][1:], ["-shellcheck=", "-pyflakes="])
        self.assertEqual(pp.step_zizmor(ctx).status, pp.PASS)
        self.assertEqual(real_calls(s)[-1][0][1:], ["--no-progress", "--config", ".github/zizmor.yml", ".github"])
        self.assertEqual(pp.step_charts(ctx).status, pp.PASS)
        self.assertEqual(real_calls(s)[-3][0][1:], ["lint", "charts/victoria-lakehouse/", "--strict"])
        self.assertTrue(real_calls(s)[-1][0][1].endswith("test_templates.sh"))

    def test_ci_script_selection(self):
        s = FakeSys(REPO, tools=())
        cases = [
            ("workflow only: the static set", [".github/workflows/ci.yaml"], {"test_workflow_permissions.py", "test_proof_comment.py"}, set(), {"test_pr_classify_paths.py"}),
            ("a gate script: its tests and the static set", ["scripts/ci/check_changelog_pr.py"],
             {"test_check_changelog_pr.py", "test_workflow_permissions.py"}, set(), {"test_pr_classify_paths.py"}),
            ("a classifier: all its test files", ["scripts/ci/pr_classify.py"],
             {"test_pr_classify_paths.py", "test_pr_classify_ci_scope.py", "test_pr_classify_toolchain.py"}, set(), {"test_check_changelog_pr.py"}),
            ("a shell script: its shell test", ["scripts/ci/gate_local.sh"], set(), {"gate_local_test.sh"}, set()),
            ("a changed test file", ["scripts/ci/tests/test_pre_push.py"], {"test_pre_push.py"}, set(), set()),
        ]
        for name, files, want_py, want_sh, not_py in cases:
            with self.subTest(name):
                s.calls.clear()
                ctx = make_ctx(files, sys_=s, root=str(REPO))
                self.assertEqual(pp.step_ci_scripts(ctx).status, pp.PASS)
                py = {os.path.basename(c) for cmd, _ in s.calls for c in cmd if c.endswith(".py") and "/tests/" in c}
                sh = {os.path.basename(c) for cmd, _ in s.calls for c in cmd if c.endswith(".sh")}
                self.assertTrue(want_py <= py, (want_py - py))
                self.assertEqual(sh, want_sh)
                self.assertFalse(py & not_py)
                self.assertNotIn("discover", [c for cmd, _ in s.calls for c in cmd])

    def test_scope_all_runs_the_full_discovery(self):
        s = FakeSys(REPO)
        ctx = make_ctx([], scope_all=True, sys_=s, root=str(REPO))
        pp.step_ci_scripts(ctx)
        discover = [c for cmd, _ in s.calls for c in cmd if c == "discover"]
        self.assertEqual(discover, ["discover"])
        shells = [cmd[1] for cmd, _ in s.calls if cmd[0] == "bash"]
        self.assertEqual(shells, list(pp.GATE_SELFTESTS))
        for t in pp.GATE_SELFTESTS:
            self.assertTrue((REPO / t).is_file(), t)

    def test_the_static_test_list_names_files_that_exist(self):
        for t in pp.STATIC_TESTS:
            self.assertTrue((HERE / t).is_file(), t)

    def test_tests_for_ci_file_table(self):
        names = ["test_check_changelog_pr.py", "test_pr_classify_paths.py", "test_pr_classify_release.py", "gate_local_test.sh",
                 "test_check_registry_touch.sh", "gate_replay_expected.txt", "testdata"]
        table = [
            ("scripts/ci/check_changelog_pr.py", ["test_check_changelog_pr.py"]),
            ("scripts/ci/pr_classify.py", ["test_pr_classify_paths.py", "test_pr_classify_release.py"]),
            ("scripts/ci/gate_local.sh", ["gate_local_test.sh"]),
            ("scripts/ci/check_registry_touch.sh", ["test_check_registry_touch.sh"]),
            ("scripts/ci/tests/test_pr_classify_paths.py", ["test_pr_classify_paths.py"]),
            ("scripts/ci/tests/gate_local_test.sh", ["gate_local_test.sh"]),
            ("scripts/ci/tests/gate_replay_expected.txt", []),
            ("scripts/ci/tests/testdata/x.json", []),
            ("scripts/ci/requirements.txt", []),
            ("scripts/bench/ab/shapes.json", []),
            ("docs/x.md", []),
        ]
        for f, want in table:
            with self.subTest(f):
                self.assertEqual(sorted(pp.tests_for_ci_file(f, names)), sorted(want))

    def test_static_trigger_and_wide_module_tables(self):
        for f, want in [(".github/workflows/x.yaml", True), ("Makefile", True), ("Dockerfile.traces", True), ("go.sum", True),
                        ("lakehouse-traces/go.mod", True), ("scripts/proof/requirements.txt", True), ("deployment/docker/x.yml", True),
                        ("docs/read-path.md", False), ("internal/a/a.go", False), ("scripts/bench/ab/x.py", False)]:
            with self.subTest(f):
                self.assertEqual(pp.static_trigger([f]), want)
        for f, want in [("go.mod", {"logs"}), ("go.sum", {"logs"}), ("lakehouse-traces/go.mod", {"traces"}),
                        ("lakehouse-traces/go.sum", {"traces"}), (".golangci.yml", {"logs", "traces"}),
                        (PATCHES + "vl-logs/a.patch", {"logs"}), (PATCHES + "vl-traces/a.patch", {"traces"}),
                        (PATCHES + "vt-traces/a.go.src", {"traces"}), (PATCHES + "other/a", {"logs", "traces"}),
                        ("internal/a/a.go", set()), ("docs/go.mod.md", set())]:
            with self.subTest(f):
                self.assertEqual(pp.wide_modules([f]), want)

    def test_module_wide_input_lints_and_vets_everything_but_cannot_pick_tests(self):
        root = mkdtemp("pp-wide-")
        go_tree(root, "internal/a")
        s = FakeSys(root, tools={"golangci-lint"})
        ctx = make_ctx(["go.mod"], {"skip_go": "false"}, sys_=s, root=root)
        self.assertEqual(pp.step_lint("logs")(ctx).status, pp.PASS)
        lint_cmds = [c[0] for c in real_calls(s)]
        self.assertEqual(len(lint_cmds), 3)  # the three CI lint invocations: plain, parity tags, e2e tags
        self.assertEqual(lint_cmds[0][-2:], ["./internal/...", "./cmd/..."])
        self.assertEqual(pp.step_vet("logs")(ctx).status, pp.PASS)
        self.assertEqual(s.calls[-1][0], ["go", "vet", "./internal/...", "./cmd/..."])
        n = len(s.calls)
        r = pp.step_test("logs")(ctx)
        self.assertEqual((r.status, r.unverified, len(s.calls)), (pp.SKIPPED, True, n))
        self.assertIn("SCOPE=all", r.detail)

    def test_zizmor_is_found_next_to_the_venv_interpreter(self):
        venv = pathlib.Path(mkdtemp("pp-venv-"))
        (venv / "zizmor").write_text("#!/bin/sh\n")
        s = FakeSys(mkdtemp())
        ctx = make_ctx(sys_=s)
        ctx.py = str(venv / "python")
        self.assertEqual(pp.step_zizmor(ctx).status, pp.PASS)
        self.assertEqual(s.calls[-1][0][0], str(venv / "zizmor"))
        ctx.py = "python3"
        r = pp.step_zizmor(ctx)
        self.assertIn("workflow-lint-requirements.txt", r.detail)


class FailurePropagation(unittest.TestCase):
    def steps(self, results):
        return [pp.Step(n, lambda c: None, (lambda r: lambda c: r)(r)) for n, r in results]

    def test_exit_codes(self):
        cases = [
            ("all pass", [("a", pp.Result(pp.PASS)), ("b", pp.Result(pp.PASS))], 0),
            ("skipped only", [("a", pp.Result(pp.SKIPPED, "x")), ("b", pp.Result(pp.SKIPPED, "y", unverified=True))], 0),
            ("one fail", [("a", pp.Result(pp.PASS)), ("b", pp.Result(pp.FAIL, "boom")), ("c", pp.Result(pp.PASS))], 1),
            ("last fails", [("a", pp.Result(pp.PASS)), ("b", pp.Result(pp.FAIL))], 1),
            ("first fails and later steps still run", [("a", pp.Result(pp.FAIL)), ("b", pp.Result(pp.PASS))], 1),
        ]
        for name, results, want in cases:
            with self.subTest(name):
                outcomes = pp.execute(make_ctx(), self.steps(results))
                self.assertEqual(len(outcomes), len(results))
                self.assertEqual(pp.exit_code(outcomes), want)

    def test_a_crashing_step_is_a_fail_not_a_pass(self):
        def boom(c):
            raise RuntimeError("bad")
        outcomes = pp.execute(make_ctx(), [pp.Step("x", lambda c: None, boom), pp.Step("y", lambda c: None, lambda c: pp.Result(pp.PASS))])
        self.assertEqual([o.status for o in outcomes], [pp.FAIL, pp.PASS])
        self.assertIn("internal error", outcomes[0].detail)
        self.assertEqual(pp.exit_code(outcomes), 1)

    def test_a_crashing_scope_rule_is_a_fail(self):
        def boom(c):
            raise ValueError("rule")
        outcomes = pp.execute(make_ctx(), [pp.Step("x", boom, lambda c: pp.Result(pp.PASS))])
        self.assertEqual(outcomes[0].status, pp.FAIL)

    def test_not_in_scope_never_runs_the_step(self):
        ran = []
        steps = [pp.Step("x", lambda c: "not in diff", lambda c: ran.append(1) or pp.Result(pp.FAIL))]
        outcomes = pp.execute(make_ctx(), steps)
        self.assertEqual((outcomes[0].status, outcomes[0].detail, ran), (pp.SKIPPED, "not in diff", []))

    def test_failing_command_fails_the_step_with_its_last_line(self):
        def script(cmd, kw):
            return (3, "line1\nthe real error\n") if "check_changelog_pr.py" in " ".join(cmd) else None
        ctx = make_ctx(sys_=FakeSys(mkdtemp(), script))
        r = pp.step_changelog(ctx)
        self.assertEqual(r.status, pp.FAIL)
        self.assertIn("exit 3", r.detail)
        self.assertIn("the real error", r.detail)

    def test_changelog_step_runs_the_gate_and_the_fragment_check(self):
        s = FakeSys(mkdtemp())
        self.assertEqual(pp.step_changelog(make_ctx(sys_=s)).status, pp.PASS)
        joined = [" ".join(c) for c, _ in s.calls]
        self.assertTrue(any("check_changelog_pr.py --base origin/main --head HEAD" in j for j in joined))
        self.assertTrue(any(j.endswith("changelog_fragments.py check") for j in joined))
        # a bad fragment alone fails the step
        s2 = FakeSys(mkdtemp(), lambda cmd, kw: (1, "bad fragment\n") if "changelog_fragments.py" in " ".join(cmd) else None)
        r = pp.step_changelog(make_ctx(sys_=s2))
        self.assertEqual(r.status, pp.FAIL)
        self.assertIn("fragments", r.detail)

    def test_command_not_found_is_a_fail(self):
        def script(cmd, kw):
            return (127, "command not found: go")
        ctx = make_ctx(sys_=FakeSys(mkdtemp(), script))
        self.assertEqual(pp.step_deps(make_ctx(sys_=FakeSys(mkdtemp(), script, tools={"make"}))).status, pp.FAIL)
        self.assertEqual(pp.step_conformance(ctx).status, pp.SKIPPED)  # no make on this fake PATH

    def test_registry_gate_failure_names_the_rule(self):
        report = ("## Registry gate: FAIL\n\n| Rule | Verdict | Detail |\n|---|---|---|\n"
                  "| Rule 1: a product change changes the registry | FAIL | internal/x.go changed, no registry row |\n")
        ctx = make_ctx(sys_=FakeSys(mkdtemp(), lambda cmd, kw: (1, report) if cmd[0] == "bash" else None))
        r = pp.step_registry_gate(ctx)
        self.assertEqual(r.status, pp.FAIL)
        self.assertIn("Rule 1", r.detail)
        self.assertIn("no registry row", r.detail)
        self.assertEqual(ctx.sys.calls[-1][1]["env"]["REGISTRY_GATE_OFFLINE"], "1")

    def test_registry_gate_pass_detail(self):
        ctx = make_ctx(sys_=FakeSys(mkdtemp(), lambda cmd, kw: (0, "## Registry gate: PASS\n\nPR kind: not a product change\n")))
        r = pp.step_registry_gate(ctx)
        self.assertEqual(r.status, pp.PASS)
        self.assertIn("PASS", r.detail)


class Fetch(unittest.TestCase):
    def test_fetch_failure_is_a_fail_offline_is_unverified(self):
        ctx = make_ctx(sys_=FakeSys(mkdtemp(), lambda cmd, kw: (128, "no network") if cmd[:2] == ["git", "fetch"] else None))
        self.assertEqual(pp.step_fetch(ctx).status, pp.FAIL)
        ctx.offline = True
        r = pp.step_fetch(ctx)
        self.assertEqual((r.status, r.unverified), (pp.SKIPPED, True))
        ctx.offline, ctx.base = False, "abc123"
        self.assertEqual(pp.step_fetch(ctx).status, pp.SKIPPED)


class Hygiene(unittest.TestCase):
    def test_signature_table(self):
        self.assertEqual(pp.commit_signature_problems("aaaaaaaa11\x1fG\x1fok\nbbbbbbbb22\x1fG\x1fok2\n"), [])
        for flag in ("N", "E", "B", "U", "X", ""):
            with self.subTest(flag):
                bad = pp.commit_signature_problems(f"aaaaaaaa11\x1fG\x1fok\ncccccccc33\x1f{flag}\x1fwip\n")
                self.assertEqual(len(bad), 1)
                self.assertTrue(bad[0].startswith("cccccccc"))

    ATTR = [
        ("clean", "fix: a\n\nbody text\n", "Jane\x1fj@x.org\x1fJane\x1fj@x.org", 0),
        ("co-author trailer", "fix: a\n\nCo-Authored-By: Someone <s@x.org>\n", "Jane\x1fj@x.org\x1fJane\x1fj@x.org", 1),
        ("lowercase trailer", "fix: a\n\nco-authored-by: Claude <noreply@anthropic.com>\n", "Jane\x1fj@x.org\x1fJane\x1fj@x.org", 1),
        ("session trailer", "fix: a\n\nClaude-Session: https://claude.ai/code/session_x\n", "Jane\x1fj@x.org\x1fJane\x1fj@x.org", 1),
        ("generated with", "fix: a\n\n\U0001F916 Generated with [Claude Code](https://claude.com/claude-code)\n", "Jane\x1fj@x.org\x1fJane\x1fj@x.org", 1),
        ("robot emoji only", "fix: a\n\n\U0001F916\n", "Jane\x1fj@x.org\x1fJane\x1fj@x.org", 1),
        ("anthropic address only", "fix: a\n\nnoreply@anthropic.com\n", "Jane\x1fj@x.org\x1fJane\x1fj@x.org", 1),
        ("ai committer", "fix: a\n", "Jane\x1fj@x.org\x1fClaude\x1fnoreply@anthropic.com", 1),
        ("word in subject is fine", "docs: stop agents editing .claude/ files\n", "Jane\x1fj@x.org\x1fJane\x1fj@x.org", 0),
    ]

    def test_attribution_table(self):
        for name, body, ident, n in self.ATTR:
            with self.subTest(name):
                rec = "aaaaaaaa11bb\x1f" + ident + "\x1f" + body + "\x1e"
                self.assertEqual(len(pp.attribution_problems(rec)), n)

    def test_one_bad_commit_among_clean_ones(self):
        clean = "aaaaaaaa\x1fJ\x1fj@x\x1fJ\x1fj@x\x1fok\x1e\n"
        bad = "bbbbbbbb\x1fJ\x1fj@x\x1fJ\x1fj@x\x1fx\n\nCo-Authored-By: C <c@x>\x1e\n"
        self.assertEqual(pp.attribution_problems(clean + bad + clean), ["bbbbbbbb: Co-Authored-By trailer"])


class RealGit(unittest.TestCase):
    """The hygiene steps against a real repository (no signing configured, so every commit is unsigned)."""

    def setUp(self):
        self.d = pathlib.Path(mkdtemp("pp-git-"))
        self.env = dict(os.environ, GIT_AUTHOR_NAME="T", GIT_AUTHOR_EMAIL="t@x.org", GIT_COMMITTER_NAME="T",
                        GIT_COMMITTER_EMAIL="t@x.org", GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_SYSTEM="/dev/null")
        self.g("init", "-q", "-b", "main")
        self.g("config", "commit.gpgsign", "false")
        self.commit("base")
        self.g("update-ref", "refs/remotes/origin/main", "HEAD")
        self.g("checkout", "-q", "-b", "feature")

    def g(self, *a):
        p = subprocess.run(["git", *a], cwd=self.d, env=self.env, capture_output=True, text=True, check=True)
        return p.stdout.strip()

    def commit(self, msg):
        (self.d / f"f{len(list(self.d.iterdir()))}.txt").write_text(msg)
        self.g("add", "-A")
        self.g("commit", "-q", "-m", msg)

    def ctx(self):
        c = pp.Ctx(sys=pp.Sys(self.d, self.env), py=sys.executable)
        c.merge_base = self.g("merge-base", "origin/main", "HEAD")
        return c

    def test_contains_main(self):
        self.commit("work")
        self.assertEqual(pp.step_contains_main(self.ctx()).status, pp.PASS)
        # origin/main moves on without the feature branch
        self.g("checkout", "-q", "main")
        self.commit("main moves 1")
        self.commit("main moves 2")
        self.g("update-ref", "refs/remotes/origin/main", "HEAD")
        self.g("checkout", "-q", "feature")
        r = pp.step_contains_main(self.ctx())
        self.assertEqual(r.status, pp.FAIL)
        self.assertIn("2 commit(s) behind", r.detail)

    def test_unsigned_commits_fail_and_no_commits_skip(self):
        self.assertEqual(pp.step_signed(self.ctx()).status, pp.SKIPPED)
        self.assertEqual(pp.step_attribution(self.ctx()).status, pp.SKIPPED)
        self.commit("work")
        r = pp.step_signed(self.ctx())
        self.assertEqual(r.status, pp.FAIL)
        self.assertIn("not signed", r.detail)

    def test_attribution_trailer_in_a_real_commit(self):
        self.commit("clean one")
        self.assertEqual(pp.step_attribution(self.ctx()).status, pp.PASS)
        self.commit("bad one\n\nCo-Authored-By: Claude <noreply@anthropic.com>")
        r = pp.step_attribution(self.ctx())
        self.assertEqual(r.status, pp.FAIL)
        self.assertIn("Co-Authored-By", r.detail)

    def test_scope_step_reads_the_committed_diff(self):
        self.commit("work")
        (self.d / "scripts" / "ci").mkdir(parents=True)
        (self.d / "scripts" / "ci" / "pr_classify.py").write_text(
            "print('product=0')\nprint('skip_go=true')\nprint('ci_class=docs')\n")
        self.g("add", "-A")
        self.g("commit", "-q", "-m", "tooling")
        c = pp.Ctx(sys=pp.Sys(self.d, self.env), py=sys.executable)
        r = pp.step_scope(c)
        self.assertEqual(r.status, pp.PASS, r.detail)
        self.assertIn("scripts/ci/pr_classify.py", c.files)
        self.assertEqual(c.classify["skip_go"], "true")
        self.assertIn("ci_class=docs", r.detail)

    def test_logs_go_to_one_fixed_dir_under_the_git_dir_and_are_overwritten(self):
        c = pp.Ctx(sys=pp.Sys(self.d, self.env), py=sys.executable)
        d = pp.log_dir(c)
        self.assertEqual(d, self.d / ".git" / "pre-push-logs")
        first = [pp.Outcome("a step", pp.FAIL, "x", 0.1, "first run output")]
        self.assertEqual(pp.write_logs(first, d), d)
        self.assertEqual((d / "a_step.log").read_text(), "first run output")
        pp.write_logs([pp.Outcome("other", pp.FAIL, "x", 0.1, "second")], d)
        self.assertEqual(sorted(p.name for p in d.iterdir()), ["other.log"])  # the previous run's logs are gone
        self.assertIsNone(pp.write_logs([pp.Outcome("quiet", pp.PASS, "", 0.0)], d))

    def test_scope_step_fails_open_when_the_classifier_breaks(self):
        self.commit("work")
        c = pp.Ctx(sys=pp.Sys(self.d, self.env), py=sys.executable)
        r = pp.step_scope(c)  # no scripts/ci/pr_classify.py in this repo
        self.assertEqual(r.status, pp.FAIL)
        self.assertFalse(c.classify_ok)
        self.assertEqual(pp.go_skip(c, "logs"), "diff touches no Go file of the logs module")


class Reporting(unittest.TestCase):
    def outcomes(self):
        return [pp.Outcome("a", pp.PASS, "fine", 0.4), pp.Outcome("b", pp.FAIL, "broke", 125.0, "x\ny"),
                pp.Outcome("c", pp.SKIPPED, "no tool", 0.0, unverified=True), pp.Outcome("d", pp.SKIPPED, "not in diff", 0.0)]

    def test_table_has_every_step_status_and_time(self):
        t = pp.render_table(self.outcomes())
        for needle in ("a ", "PASS", "FAIL", "SKIPPED", "0.4s", "2.1m", "not in diff"):
            self.assertIn(needle, t)
        self.assertEqual(len(t.splitlines()), 6)

    def test_summary_counts_and_lists_unverified(self):
        s = pp.render_summary(self.outcomes())
        self.assertIn("RESULT: FAIL (1 failed, 1 passed, 2 skipped)", s)
        self.assertIn("NOT VERIFIED locally", s)
        self.assertIn("c", s.splitlines()[1])
        self.assertNotIn("d", s.splitlines()[1].split(":", 1)[1])
        self.assertTrue(pp.render_summary(self.outcomes()[:1]).startswith("RESULT: PASS"))

    def test_checklist_has_the_eleven_parts(self):
        c = pp.render_checklist()
        self.assertEqual(len(pp.EVIDENCE_PARTS), 11)
        for i in range(1, 12):
            self.assertIn(f"{i:>2}. ", c)

    def test_parse_keyvals(self):
        self.assertEqual(pp.parse_keyvals("product=0\nskip_go=true\nreason=\njunk line\nci_detail=a=b\n"),
                         {"product": "0", "skip_go": "true", "reason": "", "ci_detail": "a=b"})


class MainEntry(unittest.TestCase):
    def script(self, fail=None, files="docs/x.md\0", classify="product=0\nskip_go=true\nci_class=docs\n"):
        def s(cmd, kw):
            j = " ".join(cmd)
            if fail and fail in j:
                return 1, "it failed\n"
            if cmd[:2] == ["git", "merge-base"]:
                return 0, "abc123\n"
            if cmd[:2] == ["git", "rev-parse"]:
                return 0, "abc123\n"
            if cmd[:2] == ["git", "diff"]:
                return 0, files
            if "pr_classify.py" in j:
                return 0, classify
            if cmd[:2] == ["git", "log"]:
                return 0, ""
            if cmd[:2] == ["git", "status"]:
                return 0, ""
            return None
        return s

    def run_main(self, argv, **kw):
        out = io.StringIO()
        errbuf = io.StringIO()
        old = sys.stderr
        sys.stderr = errbuf
        try:
            rc = pp.main(argv, system=FakeSys(mkdtemp(), self.script(**kw), tools={"make", "golangci-lint"}), out=out)
        finally:
            sys.stderr = old
        return rc, out.getvalue()

    def test_green_run_exits_zero_and_prints_table_and_checklist(self):
        rc, text = self.run_main([])
        self.assertEqual(rc, 0, text)
        self.assertIn("RESULT: PASS", text)
        self.assertIn("conformance-check", text)
        self.assertIn("PR evidence checklist", text)

    def test_failing_step_exits_one_and_shows_its_output(self):
        rc, text = self.run_main([], fail="check_changelog_pr.py")
        self.assertEqual(rc, 1)
        self.assertIn("RESULT: FAIL (1 failed", text)
        self.assertIn("changelog-gate", text)
        self.assertIn("it failed", text)

    def test_scope_all_runs_every_step(self):
        rc, text = self.run_main(["--scope", "all"])
        self.assertEqual(rc, 0, text)
        self.assertNotIn("diff touches no", text)
        self.assertIn("SCOPE=all", text)
        self.assertNotIn("SKIPPED", "\n".join(l for l in text.splitlines() if l.startswith(("go ", "golangci", "conformance", "deps"))))

    def test_race_flag_reaches_go_test(self):
        _, text = self.run_main(["--race"], files="internal/a/a.go\0", classify="product=1\nskip_go=false\nci_class=product\n")
        self.assertIn("-race", text.splitlines()[0])

    def test_bad_scope_is_a_usage_error(self):
        rc, _ = self.run_main(["--scope", "everything"])
        self.assertEqual(rc, 2)

    def test_unresolvable_base_is_a_usage_error(self):
        out = io.StringIO()
        sysx = FakeSys(mkdtemp(), lambda cmd, kw: (1, "") if cmd[:2] == ["git", "rev-parse"] else None)
        err = io.StringIO()
        old, sys.stderr = sys.stderr, err
        try:
            rc = pp.main(["--base", "nope", "--offline"], system=sysx, out=out)
        finally:
            sys.stderr = old
        self.assertEqual(rc, 2)


class MakefileWiring(unittest.TestCase):
    def test_target_exists_maps_scope_and_race(self):
        mk = (REPO / "Makefile").read_text()
        self.assertRegex(mk, r"(?m)^\.PHONY:.*\bpre-push\b")
        m = re.search(r"(?ms)^pre-push:.*?(?=^\S)", mk)
        self.assertIsNotNone(m)
        body = m.group(0)
        self.assertIn("scripts/ci/pre_push.py", body)
        self.assertIn("$(SCOPE)", body)
        self.assertIn("--race", body)
        self.assertIn("$(GATE_BASE)", body)


if __name__ == "__main__":
    unittest.main()
