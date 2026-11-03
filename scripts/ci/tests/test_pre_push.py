"""Tests of scripts/ci/pre_push.py (`make pre-push`): scope mapping, skip reasons, failure propagation,
exit codes, commit hygiene against a real git repository, and the Makefile wiring.

Run: python3 -m unittest scripts/ci/tests/test_pre_push.py
"""
from __future__ import annotations

import importlib.util
import io
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tarfile
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


def fake_archive(cmd):
    """Answer `git archive --format=tar -o <tar> HEAD -- scripts/proof` with a tar of a tiny fixture tree."""
    if cmd[:2] != ["git", "archive"]:
        return None
    tar = cmd[cmd.index("-o") + 1]
    src = pathlib.Path(tar).parent / "src"
    (src / "scripts/proof/fixtures/cases").mkdir(parents=True)
    (src / "scripts/proof/fixtures/cases/a.json").write_text("{}")
    (src / "scripts/proof/fixtures/_gen.py").write_text("")
    with tarfile.open(tar, "w") as tf:
        tf.add(src / "scripts", arcname="scripts")
    shutil.rmtree(src)
    return 0, ""


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


ALWAYS = {"fetch-base", "scope", "contains-main", "commits-signed", "no-ai-attribution", "registry-gate", "changelog-gate",
          "deps", "conformance-check"}


def running(ctx):
    """Names of the steps whose `applies` says run, among the ones scope can switch off."""
    return {s.name for s in pp.build_steps() if s.name not in ALWAYS and s.applies(ctx) is None}


# The proof-coverage generator lists every file that names a patch path as a consumer of that patch. This tool is
# not one, so the cases below join "patches/" with the set name instead of spelling the path out.
PATCHES = "patches/"
LOGS_COMPILE = {"golangci-lint (logs)", "go vet (logs)", "go build (logs)"}
TRACES_COMPILE = {"golangci-lint (traces)", "go vet (traces)", "go build (traces)"}
GO_LOGS = LOGS_COMPILE | {"go test (logs)"}
GO_TRACES = TRACES_COMPILE | {"go test (traces)"}
# a change in the logs module compiles into the traces module (replace ../), so vet, lint and build run there too
LOGS_CHANGE = GO_LOGS | TRACES_COMPILE
ALL_OPTIONAL = {s.name for s in pp.build_steps()} - ALWAYS


class ScopeMapping(unittest.TestCase):
    # Changed expectations against #865, each a finding of the review: "docs page", "go file but CI scope skips go",
    # "classifier failed", "prose readme", "registry row" and "changelog fragment" (F1, F3, F5), "logs go file" and the
    # patch and go.mod rows (F2: both modules vet, lint and build), "unrelated script" (conformance always runs: F3).
    CASES = [
        # name, files, classify, expected running steps
        ("nothing changed", [], {}, set()),
        ("docs page: the Go tests that read docs/ run", ["docs/read-path.md"], {"product": "0", "skip_go": "true"}, {"go test (logs)"}),
        ("workflow", [".github/workflows/ci.yaml"], {"product": "0", "skip_go": "true"},
         {"ci-script-tests", "actionlint", "zizmor"}),
        ("dockerfile", ["Dockerfile.logs"], {"product": "1", "skip_go": "false"}, {"ci-script-tests"}),
        ("requirements lock", ["scripts/proof/requirements.txt"], {"product": "0", "skip_go": "true"},
         {"ci-script-tests", "harness-tests"}),
        ("compose file", ["deployment/docker/docker-compose-e2e.yml"], {"product": "0", "skip_go": "true"},
         {"ci-script-tests", "go test (logs)"}),
        ("root go.mod: module-wide", ["go.mod"], {"product": "0", "skip_go": "false"}, {"ci-script-tests"} | LOGS_CHANGE),
        ("go.sum", ["go.sum"], {"product": "0", "skip_go": "false"}, {"ci-script-tests"} | LOGS_CHANGE),
        ("traces go.mod: module-wide", ["lakehouse-traces/go.mod"], {"product": "0", "skip_go": "false"},
         {"ci-script-tests"} | GO_TRACES),
        ("logs patch", [PATCHES + "vl-logs/x.patch"], {"product": "1", "skip_go": "false"},
         {"lint-logs-selftests"} | LOGS_CHANGE),
        ("traces patch", [PATCHES + "vt-traces/x.go.src"], {"product": "1", "skip_go": "false"},
         {"lint-logs-selftests"} | GO_TRACES),
        ("shared patch", [PATCHES + "README.md"], {"product": "1", "skip_go": "false"},
         {"lint-logs-selftests"} | LOGS_CHANGE | GO_TRACES),
        ("linter config", [".golangci.yml"], {"product": "0", "skip_go": "false"}, LOGS_CHANGE | GO_TRACES),
        ("ci script", ["scripts/ci/check_changelog_pr.py"], {"product": "0", "skip_go": "true"}, {"ci-script-tests"}),
        ("unrelated script", ["scripts/cost/model.py"], {"product": "0", "skip_go": "true"}, set()),
        ("ab harness", ["scripts/bench/ab/pr_smoke.py"], {"product": "0", "skip_go": "true"}, {"harness-tests"}),
        ("proof harness", ["scripts/proof/runner_fit.py"], {"product": "0", "skip_go": "true"}, {"harness-tests"}),
        ("proof compose file", ["deployment/docker/docker-compose-proof.yml"], {"product": "0", "skip_go": "true"},
         {"ci-script-tests", "harness-tests", "go test (logs)"}),
        ("chart", ["charts/victoria-lakehouse/values.yaml"], {"product": "1", "skip_go": "false"},
         {"chart-tests", "ci-script-tests", "go test (logs)"}),
        ("registry row", ["tests/conformance/registry/rows/x.yaml"], {"product": "0", "skip_go": "false"},
         {"registry-gate-selftests"}),
        ("changelog fragment", ["changelog.d/9.fixed.md"], {"product": "0", "skip_go": "true"}, {"registry-gate-selftests"}),
        ("logs go file", ["internal/storage/x.go"], {"product": "1", "skip_go": "false"}, LOGS_CHANGE),
        ("logs testdata only", ["internal/storage/testdata/a.golden"], {"product": "1", "skip_go": "false"}, {"go test (logs)"}),
        ("traces go file", ["lakehouse-traces/internal/x/y.go"], {"product": "1", "skip_go": "false"}, GO_TRACES),
        ("mirrored go in both", ["internal/a/a.go", "lakehouse-traces/internal/a/a.go"], {"product": "1", "skip_go": "false"},
         LOGS_CHANGE | GO_TRACES),
        ("go file with CI scope skip_go=true still runs every Go step", ["internal/a/a.go"], {"product": "0", "skip_go": "true"},
         LOGS_CHANGE),
        ("deps tree is not a diff", ["deps/VictoriaLogs/app/x.go"], {"product": "0", "skip_go": "false"}, set()),
        ("classifier failed: fail open", ["README.md"], None, {"go test (logs)"}),
        ("prose readme", ["README.md"], {"product": "0", "skip_go": "true"}, {"go test (logs)"}),
        ("makefile alone", ["Makefile"], {"product": "0", "skip_go": "false"}, {"ci-script-tests"}),
        ("inert top-level files", ["CHANGELOG.md", "UPSTREAM_COVERAGE.md", "LICENSE"], {"product": "0", "skip_go": "true"}, set()),
        ("unknown top-level path: tests unverified", ["newdir/x.txt"], {"product": "0", "skip_go": "true"}, {"go test (logs)"}),
        ("alerts", ["alerts/alerts-lakehouse.yml"], {"product": "1", "skip_go": "false"}, {"go test (logs)"}),
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
        ctx = make_ctx(["scripts/cost/x.py"], {"product": "0", "skip_go": "true"})
        reasons = {s.name: s.applies(ctx) for s in pp.build_steps()}
        self.assertIn(".github/", reasons["actionlint"])
        self.assertIn("charts/", reasons["chart-tests"])
        self.assertIn("the diff cannot change its result", reasons["go test (traces)"])
        self.assertIn("CI runs this job", reasons["go vet (logs)"])
        for r in reasons.values():
            self.assertNotIn("skip_go", r or "")


class GoPlanning(unittest.TestCase):
    def plan(self, files, *dirs):
        root = mkdtemp("pp-plan-")
        go_tree(root, *dirs)
        return pp.go_plan(make_ctx(files, root=root))

    def test_go_files_map_to_their_package_and_testdata_to_its_parent(self):
        p = self.plan(["internal/a/a.go", "internal/a/a_test.go", "internal/b/testdata/x.json", "lakehouse-traces/main.go",
                       "lakehouse-traces/internal/t/t.go", "deps/x/y.go", "lakehouse-traces/deps/v/z.go"])
        self.assertEqual(p["logs"]["dirs"], ["internal/a"])
        self.assertEqual(p["traces"]["dirs"], [".", "internal/t"])
        self.assertTrue(p["logs"]["compile"] and p["traces"]["compile"])

    def test_testdata_alone_is_a_test_input_not_a_compile_input(self):
        p = self.plan(["internal/b/testdata/x.json"], "internal/b")
        self.assertEqual(p["logs"]["dirs"], ["internal/b"])
        self.assertFalse(p["logs"]["compile"])

    def test_an_embedded_asset_belongs_to_the_nearest_package_and_recompiles_it(self):
        root = mkdtemp("pp-plan-")
        go_tree(root, "internal/ui")
        (pathlib.Path(root) / "internal/ui/embed.go").write_text("package x\n\n//go:embed static\nvar assets string\n")
        p = pp.go_plan(make_ctx(["internal/ui/static/app.js", "internal/ui/vmui/index.html"], root=root))
        self.assertEqual(p["logs"]["dirs"], ["internal/ui"])
        self.assertTrue(p["logs"]["compile"])
        self.assertEqual(p["logs"]["unmapped"], [])

    def test_a_non_go_file_in_a_package_without_embed_is_a_test_input_only(self):
        p = self.plan(["tests/conformance/registry/rows/x.yaml"], "tests/conformance/registry")
        self.assertEqual(p["logs"]["dirs"], ["tests/conformance/registry"])
        self.assertFalse(p["logs"]["compile"])

    def test_markdown_under_a_package_runs_its_tests_but_does_not_recompile(self):
        p = self.plan(["internal/a/README.md"], "internal/a")
        self.assertEqual((p["logs"]["dirs"], p["logs"]["compile"]), (["internal/a"], False))

    def test_readers(self):
        p = self.plan(["docs/x.md", "alerts/a.yml", "charts/victoria-lakehouse/templates/pdb.yaml", "README.md"])
        self.assertEqual(p["logs"]["readers"], {"internal/config": "", "internal/delete": "", "internal/metrics": "",
                                                "internal/compaction": "^TestCompaction_PDB_NoSimultaneousEviction$"})
        self.assertFalse(p["logs"]["compile"])
        self.assertEqual(p["logs"]["unmapped"], [])

    def test_a_whole_package_change_beats_a_narrowed_reader(self):
        root = mkdtemp("pp-plan-")
        go_tree(root, "internal/compaction")
        ctx = make_ctx(["charts/x.yaml", "internal/compaction/c.go"], root=root)
        self.assertEqual(pp.test_targets(ctx, "logs"), {"internal/compaction": ""})
        ctx = make_ctx(["charts/x.yaml"], root=root)
        self.assertEqual(pp.test_targets(ctx, "logs"), {"internal/compaction": "^TestCompaction_PDB_NoSimultaneousEviction$"})

    def test_module_wide_inputs_compile_both_modules_for_logs(self):
        p = self.plan(["go.mod"])
        self.assertTrue(p["logs"]["wide"] and p["logs"]["compile"] and p["traces"]["compile"])
        self.assertFalse(p["traces"]["wide"])
        p = self.plan(["lakehouse-traces/go.sum"])
        self.assertTrue(p["traces"]["wide"] and not p["logs"]["compile"])

    def test_tests_dir_is_linted_but_not_run(self):
        self.assertEqual(pp.vet_test_dirs("logs", ["internal/a", "tests/e2e", "tests", "scripts/ci/helmdrift"]),
                         ["internal/a", "scripts/ci/helmdrift"])
        self.assertEqual(pp.vet_test_dirs("traces", ["tests/x"]), ["tests/x"])

    def test_lint_invocations_mirror_ci(self):
        self.assertEqual(pp.lint_invocations("logs"), [
            ("", ["./internal/...", "./cmd/..."], False),
            ("e2e,chaos", ["./tests/ingestmatrix/...", "./tests/e2e/...", "./tests/conformance/..."], True),
            ("parity,chaos", ["./tests/parity/..."], True)])
        self.assertEqual(pp.lint_invocations("traces"), [("", ["./..."], False), ("parity,chaos,e2e", ["./..."], True)])

    def test_failure_line_prefers_the_first_error_line(self):
        self.assertEqual(pp.failure_line("a\nx.go:3:4: undefined: Foo\n   | ^\n", 1), "x.go:3:4: undefined: Foo")
        self.assertEqual(pp.failure_line("nothing special\nlast", 1), "last")
        self.assertEqual(pp.failure_line("", 7), "exit 7")

    def test_failure_line_tiers_skip_expected_error_output_of_passing_tests(self):
        out = ("::error::proof-comment: HTTP 500\n.....\nFAIL: test_x (mod.Class.test_x)\nTraceback\nAssertionError: boom\n")
        self.assertEqual(pp.failure_line(out, 1), "FAIL: test_x (mod.Class.test_x)")
        self.assertEqual(pp.failure_line("ok pkg 0.1s\n--- FAIL: TestX (0.00s)\nFAIL\n", 1), "--- FAIL: TestX (0.00s)")
        self.assertEqual(pp.failure_line("# pkg\ninternal/a/a.go:12:3: undefined: Foo\n", 1), "internal/a/a.go:12:3: undefined: Foo")


def go_tree(root, *dirs):
    for d in dirs:
        p = pathlib.Path(root) / d
        p.mkdir(parents=True, exist_ok=True)
        (p / "x.go").write_text("package x\n")


class GoCommands(unittest.TestCase):
    def setUp(self):
        self.root = mkdtemp("pp-go-")
        go_tree(self.root, "internal/a", "internal/storage/parquets3", "lakehouse-traces/internal/t")

    def ctx(self, files, **kw):
        s = FakeSys(self.root, tools={"golangci-lint"})
        return make_ctx(files, {"skip_go": "false"}, sys_=s, root=self.root, **kw), s

    def test_go_test_is_count1_memcapped_not_short_and_not_race_by_default(self):
        ctx, s = self.ctx(["internal/a/a.go"])
        r = pp.step_test("logs")(ctx)
        self.assertEqual(r.status, pp.PASS)
        cmd, kw = s.calls[-1]
        self.assertEqual(cmd[:2], ["go", "test"])
        self.assertNotIn("-short", cmd)  # CI passes -short to parquets3 only (F6)
        self.assertIn("-count=1", cmd)
        self.assertEqual(cmd[cmd.index("-p") + 1], "2")  # several packages share one host
        self.assertIn("-timeout=10m", cmd)
        self.assertNotIn("-race", cmd)
        self.assertEqual(cmd[-1], "./internal/a")
        self.assertEqual(kw["env"], {"GOWORK": "off", "GOMEMLIMIT": "4GiB"})
        self.assertEqual(pathlib.Path(kw["cwd"]), pathlib.Path(self.root))

    def test_parquets3_alone_gets_short(self):
        ctx, s = self.ctx(["internal/a/a.go", "internal/storage/parquets3/p.go"])
        pp.step_test("logs")(ctx)
        tests = [c for c, _ in s.calls if c[:2] == ["go", "test"]]
        self.assertEqual(len(tests), 2)
        short = [c for c in tests if "-short" in c]
        self.assertEqual([c[-1] for c in short], ["./internal/storage/parquets3"])
        self.assertEqual([c[-1] for c in tests if "-short" not in c], ["./internal/a"])

    def test_race_flag_adds_race(self):
        ctx, s = self.ctx(["internal/a/a.go"], race=True)
        pp.step_test("logs")(ctx)
        self.assertIn("-race", s.calls[-1][0])

    def test_existing_gomemlimit_is_respected(self):
        s = FakeSys(self.root, environ={"PATH": "", "GOMEMLIMIT": "2GiB"})
        ctx = make_ctx(["internal/a/a.go"], {"skip_go": "false"}, sys_=s, root=self.root)
        pp.step_test("logs")(ctx)
        self.assertEqual(s.calls[-1][1]["env"]["GOMEMLIMIT"], "2GiB")

    def test_traces_runs_in_its_module_dir_and_vets_the_whole_module(self):
        ctx, s = self.ctx(["lakehouse-traces/internal/t/t.go"])
        self.assertEqual(pp.step_vet("traces")(ctx).status, pp.PASS)
        gofmt, vet = s.calls[-2], s.calls[-1]
        self.assertEqual(vet[0], ["go", "vet", "./..."])
        self.assertEqual(pathlib.Path(vet[1]["cwd"]), pathlib.Path(self.root) / "lakehouse-traces")
        self.assertEqual(gofmt[0][:3], ["gofmt", "-s", "-l"])
        self.assertIn("internal/", gofmt[0])

    def test_gofmt_findings_fail_the_vet_row(self):
        s = FakeSys(self.root, lambda cmd, kw: (0, "internal/a/a.go\n") if cmd[0] == "gofmt" else None)
        ctx = make_ctx(["internal/a/a.go"], root=self.root, sys_=s)
        r = pp.step_vet("logs")(ctx)
        self.assertEqual(r.status, pp.FAIL)
        self.assertIn("gofmt -s required on: internal/a/a.go", r.detail)
        self.assertEqual([c for c, _ in s.calls if c[0] == "go"], [])

    def test_deleted_package_dir_is_not_run(self):
        ctx, s = self.ctx(["internal/gone/g.go"])
        self.assertEqual(pp.step_test("logs")(ctx).status, pp.SKIPPED)
        self.assertEqual(s.calls, [])

    def test_lint_checks_whole_packages_and_only_new_findings_for_tagged_tests(self):
        ctx, s = self.ctx(["lakehouse-traces/internal/t/t.go"])
        ctx.merge_base = "mb123"
        r = pp.step_lint("traces")(ctx)
        self.assertEqual(r.status, pp.PASS)
        (c1, kw1), (c2, _) = real_calls(s)
        self.assertEqual(c1[1:], ["run", "./..."])
        self.assertEqual(c2[1:], ["run", "--new-from-rev=mb123", "--build-tags", "parity,chaos,e2e", "./..."])
        self.assertEqual(kw1["env"]["GOWORK"], "off")
        self.assertEqual(pathlib.Path(kw1["cwd"]), pathlib.Path(self.root) / "lakehouse-traces")

    def test_golangci_lint_env_expands_the_home_directory(self):
        s = FakeSys(self.root, environ={"PATH": "", "GOLANGCI_LINT": "~/bin/golangci-lint"})
        ctx = make_ctx(["internal/a/a.go"], {"skip_go": "false"}, sys_=s, root=self.root)
        pp.step_lint("logs")(ctx)
        self.assertEqual(real_calls(s)[0][0][0], os.path.expanduser("~/bin/golangci-lint"))

    def test_scope_all_go_test_lists_packages_with_go_list(self):
        def script(cmd, kw):
            if cmd[:2] == ["go", "list"]:
                return 0, f"{self.root}/internal/a\n{self.root}/internal/storage/parquets3\n"
            return None
        s = FakeSys(self.root, script)
        ctx = make_ctx([], scope_all=True, sys_=s, root=self.root)
        self.assertEqual(pp.step_test("logs")(ctx).status, pp.PASS)
        tests = [c for c, _ in s.calls if c[:2] == ["go", "test"]]
        self.assertEqual([c[-1] for c in tests if "-short" in c], ["./internal/storage/parquets3"])
        self.assertEqual([c[-1] for c in tests if "-short" not in c], ["./internal/a"])

    def test_scope_all_go_list_failure_is_a_fail(self):
        s = FakeSys(self.root, lambda cmd, kw: (1, "go: cannot find module") if cmd[:2] == ["go", "list"] else None)
        ctx = make_ctx([], scope_all=True, sys_=s, root=self.root)
        self.assertEqual(pp.step_test("logs")(ctx).status, pp.FAIL)

    def test_missing_golangci_lint_is_skipped_and_unverified(self):
        s = FakeSys(self.root)
        ctx = make_ctx(["internal/a/a.go"], {"skip_go": "false"}, sys_=s, root=self.root)
        r = pp.step_lint("logs")(ctx)
        self.assertEqual((r.status, r.unverified), (pp.SKIPPED, True))
        self.assertIn("golangci-lint", r.detail)
        self.assertEqual(s.calls, [])

    def test_a_passing_test_run_of_picked_packages_says_it_is_partial(self):
        ctx, _ = self.ctx(["internal/a/a.go"])
        self.assertIn("CI runs every package", pp.step_test("logs")(ctx).partial)

    def test_unmapped_paths_next_to_a_package_make_the_pass_partial(self):
        ctx, _ = self.ctx(["internal/a/a.go", "newdir/x.txt"])
        r = pp.step_test("logs")(ctx)
        self.assertEqual(r.status, pp.PASS)
        self.assertIn("newdir/x.txt", r.partial)

    def test_reader_runs_only_the_test_that_reads_the_file(self):
        go_tree(self.root, "internal/compaction")
        ctx, s = self.ctx(["charts/victoria-lakehouse/templates/pdb.yaml"])
        pp.step_test("logs")(ctx)
        cmd = s.calls[-1][0]
        self.assertEqual(cmd[:4], ["go", "test", "-run", "^TestCompaction_PDB_NoSimultaneousEviction$"])
        self.assertEqual(cmd[-1], "./internal/compaction")


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
        self.assertEqual(real_calls(s)[-4][0][1:], ["lint", "charts/victoria-lakehouse/", "--strict"])
        self.assertTrue(real_calls(s)[-2][0][1].endswith("test_templates.sh"))
        self.assertTrue(real_calls(s)[-1][0][-1].endswith("test_release_image_tags.py"))

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
        self.assertEqual([cmd[1] for cmd, _ in s.calls if cmd[0] == "bash"], [])  # the shell self-tests have their own rows
        for t in pp.GATE_SELFTESTS + (pp.GATE_SELFTEST_PY,) + pp.LINT_LOGS_SELFTESTS:
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
        self.assertEqual(len(lint_cmds), 3)  # the three CI lint invocations: plain, e2e tags, parity tags
        self.assertEqual(lint_cmds[0][-2:], ["./internal/...", "./cmd/..."])
        self.assertEqual(pp.step_vet("logs")(ctx).status, pp.PASS)
        self.assertEqual(s.calls[-1][0], ["go", "vet", "./internal/...", "./cmd/..."])
        n = len(s.calls)
        r = pp.step_test("logs")(ctx)
        self.assertEqual((r.status, r.unverified, len(s.calls)), (pp.SKIPPED, True, n))
        self.assertIn("PREPUSH_SCOPE=all", r.detail)

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
        self.assertTrue(s.startswith("RESULT: FAIL (1 not verified) - 1 failed, 1 passed, 2 skipped"), s)
        self.assertIn("NOT VERIFIED locally", s)
        self.assertIn("  c: no tool", s)
        self.assertNotIn("  d:", s)
        self.assertTrue(pp.render_summary(self.outcomes()[:1]).startswith("RESULT: PASS - 0 failed, 1 passed, 0 skipped"))

    def test_partial_rows_count_as_not_verified_and_are_named(self):
        o = [pp.Outcome("registry-gate", pp.PASS, "PREVIEW", 0.1, partial="the branch changes the gate itself"),
             pp.Outcome("deps", pp.PASS, "ok", 0.1)]
        s = pp.render_summary(o)
        self.assertTrue(s.startswith("RESULT: PASS (1 not verified) - "), s)
        self.assertIn("  registry-gate: the branch changes the gate itself", s)

    def test_summary_lists_the_always_run_ci_jobs_that_were_skipped(self):
        o = [pp.Outcome("go test (logs)", pp.SKIPPED, "no changed path", 0.0),
             pp.Outcome("go vet (logs)", pp.PASS, "ok", 1.0),
             pp.Outcome("harness-tests", pp.SKIPPED, "diff touches no scripts", 0.0)]
        s = pp.render_summary(o)
        self.assertIn("Always-run CI jobs with a local step that was skipped:", s)
        self.assertIn("ci.yaml:test-logs (go test (logs))", s)
        self.assertNotIn("proof-metrics.yaml", s.split("Always-run CI jobs with no local equivalent")[0])  # path-filtered, not always-run
        self.assertIn("ci.yaml:docker-logs", s.split("Always-run CI jobs with no local equivalent")[1])

    def test_checklist_has_the_eleven_parts(self):
        c = pp.render_checklist()
        self.assertEqual(len(pp.EVIDENCE_PARTS), 11)
        for i in range(1, 12):
            self.assertIn(f"{i:>2}. ", c)

    def test_parse_keyvals(self):
        self.assertEqual(pp.parse_keyvals("product=0\nskip_go=true\nreason=\njunk line\nci_detail=a=b\n"),
                         {"product": "0", "skip_go": "true", "reason": "", "ci_detail": "a=b"})


class MainEntry(unittest.TestCase):
    def script(self, root, fail=None, files="docs/x.md\0", classify="product=0\nskip_go=true\nci_class=docs\n"):
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
            if cmd[:2] == ["go", "list"]:
                return 0, f"{root}/internal/a\n"
            return fake_archive(cmd)
        return s

    def run_main(self, argv, **kw):
        out = io.StringIO()
        errbuf = io.StringIO()
        old = sys.stderr
        sys.stderr = errbuf
        root = mkdtemp()
        go_tree(root, "internal/a", "lakehouse-traces/internal/t")
        try:
            rc = pp.main(argv, system=FakeSys(root, self.script(root, **kw), tools={"make", "golangci-lint", "helm", "actionlint", "zizmor"}), out=out)
        finally:
            sys.stderr = old
        return rc, out.getvalue()

    def test_green_run_exits_zero_and_prints_table_and_checklist(self):
        rc, text = self.run_main([])
        self.assertEqual(rc, 0, text)
        self.assertIn("RESULT: PASS", text)
        self.assertIn("conformance-check", text)
        self.assertNotRegex(text, r"conformance-check\s+SKIPPED")
        self.assertIn("PR evidence checklist", text)

    def test_failing_step_exits_one_and_shows_its_output(self):
        rc, text = self.run_main([], fail="check_changelog_pr.py")
        self.assertEqual(rc, 1)
        self.assertRegex(text, r"RESULT: FAIL( \(\d+ not verified\))? - 1 failed")
        self.assertIn("changelog-gate", text)
        self.assertIn("it failed", text)

    def test_scope_all_runs_every_step(self):
        rc, text = self.run_main(["--scope", "all"])
        self.assertEqual(rc, 0, text)
        self.assertNotIn("diff touches no", text)
        self.assertIn("PREPUSH_SCOPE=all", text)
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
        self.assertIn("$(PREPUSH_SCOPE)", body)
        self.assertIn("--race", body)
        self.assertIn("$(GATE_BASE)", body)


# ---------------------------------------------------------------------------------------------------------------
# Reproductions of the independent review of #865. Each probe drives the real step list (build_steps + execute) on a
# fake system and asserts what CI would do; they use only API that existed before the follow-up, so they run (and
# fail) against the tool as merged in #865. Finding ids F1..F15 are the review's.
ALWAYS_STEPS = {"fetch-base", "scope", "contains-main", "commits-signed", "no-ai-attribution", "registry-gate", "changelog-gate"}
TOOLS = ("make", "golangci-lint", "helm", "actionlint", "zizmor", "go", "gofmt")


def probe_root(*dirs):
    root = mkdtemp("pp-probe-")
    go_tree(root, "internal/a", "lakehouse-traces/internal/t", *dirs)
    return root


def run_steps(files, classify=None, root=None, scope_all=False, race=False, script=None, environ=None, names=None):
    """{step name: Outcome} of the optional steps for a diff, and the system that recorded every command."""
    root = root or probe_root()
    s = FakeSys(root, script, tools=TOOLS, environ=environ or {"PATH": ""})
    ctx = make_ctx(files, classify if classify is not None else {"product": "0", "skip_go": "false"}, scope_all=scope_all,
                   race=race, sys_=s, root=root)
    steps = [st for st in pp.build_steps() if st.name not in ALWAYS_STEPS and (names is None or st.name in names)]
    return {o.name: o for o in pp.execute(ctx, steps)}, s


def cmds_of(s, first=None):
    return [c for c, _ in real_calls(s) if first is None or c[0] == first]


def go_test_patterns(s):
    return [c for c in cmds_of(s, "go") if c[1] == "test"]


class ReviewProbes(unittest.TestCase):
    """One test per finding of the #865 review."""

    # F1: required Go jobs run on every pull request; a docs-only change breaks internal/delete's docs test
    def test_F1_docs_only_change_runs_the_tests_that_read_docs(self):
        root = probe_root("internal/delete", "internal/config")
        out, s = run_steps(["docs/operations.md"], {"product": "0", "skip_go": "true"}, root=root)
        self.assertEqual(out["go test (logs)"].status, pp.PASS, out["go test (logs)"].detail)
        joined = [" ".join(c) for c in go_test_patterns(s)]
        self.assertTrue(any("./internal/delete" in j for j in joined), joined)
        self.assertTrue(any("./internal/config" in j for j in joined), joined)

    def test_F1_alerts_and_dashboards_run_the_metrics_tests(self):
        root = probe_root("internal/metrics")
        for f in ("alerts/alerts-lakehouse.yml", "dashboards/victoria-lakehouse.json"):
            with self.subTest(f):
                out, s = run_steps([f], {"product": "0", "skip_go": "true"}, root=root)
                self.assertEqual(out["go test (logs)"].status, pp.PASS)
                self.assertTrue(any("./internal/metrics" in " ".join(c) for c in go_test_patterns(s)))

    def test_F1_chart_pdb_runs_the_compaction_test_that_reads_it(self):
        root = probe_root("internal/compaction")
        out, s = run_steps(["charts/victoria-lakehouse/templates/pdb.yaml"], {"product": "1", "skip_go": "false"}, root=root)
        self.assertEqual(out["go test (logs)"].status, pp.PASS)
        joined = [" ".join(c) for c in go_test_patterns(s)]
        self.assertTrue(any("./internal/compaction" in j and "TestCompaction_PDB_NoSimultaneousEviction" in j for j in joined), joined)

    def test_F1_an_embedded_asset_belongs_to_its_package(self):
        root = probe_root("internal/ui")
        (pathlib.Path(root) / "internal/ui/static").mkdir()
        (pathlib.Path(root) / "internal/ui/embed.go").write_text("package x\n\n//go:embed static\nvar assets string\n")
        out, s = run_steps(["internal/ui/static/app.js"], {"product": "1", "skip_go": "false"}, root=root)
        self.assertEqual(out["go test (logs)"].status, pp.PASS, out["go test (logs)"].detail)
        self.assertTrue(any("./internal/ui" in " ".join(c) for c in go_test_patterns(s)))
        self.assertEqual(out["go vet (logs)"].status, pp.PASS)

    def test_F1_skip_go_never_skips_a_go_step(self):
        out, _ = run_steps(["internal/a/a.go", "lakehouse-traces/internal/t/t.go"], {"product": "0", "skip_go": "true"})
        for name in ("golangci-lint (logs)", "go vet (logs)", "go test (logs)", "go vet (traces)", "go test (traces)"):
            with self.subTest(name):
                self.assertEqual(out[name].status, pp.PASS, out[name].detail)
                self.assertNotIn("skip_go", out[name].detail)

    def test_F1_an_unmapped_path_is_not_silently_green(self):
        out, s = run_steps(["somenewdir/data.txt"], {"product": "0", "skip_go": "false"})
        o = out["go test (logs)"]
        self.assertEqual((o.status, o.unverified), (pp.SKIPPED, True), o.detail)
        self.assertIn("somenewdir/data.txt", o.detail)

    # F2: CI vets and builds whole modules; a rename in one package breaks its dependents and the other module
    def test_F2_a_go_change_vets_and_builds_both_modules_whole(self):
        out, s = run_steps(["internal/a/a.go"])
        vets = [c for c in cmds_of(s, "go") if c[1] == "vet"]
        self.assertIn(["go", "vet", "./internal/...", "./cmd/..."], vets)
        self.assertIn(["go", "vet", "./..."], vets)
        builds = [c for c in cmds_of(s, "go") if c[1] == "build"]
        self.assertIn(["go", "build", "./internal/...", "./cmd/..."], builds)
        self.assertIn(["go", "build", "./..."], builds)
        self.assertEqual(out["go vet (traces)"].status, pp.PASS)

    def test_F2_a_traces_only_change_does_not_vet_the_logs_module(self):
        out, s = run_steps(["lakehouse-traces/internal/t/t.go"])
        self.assertEqual(out["go vet (logs)"].status, pp.SKIPPED)
        self.assertEqual(out["go vet (traces)"].status, pp.PASS)

    # F3: conformance-check runs on every pull request
    def test_F3_conformance_check_always_runs(self):
        for f in ("scripts/cost/model.py", "internal/a/a.go", "somewhere/else.txt"):
            with self.subTest(f):
                out, s = run_steps([f])
                self.assertEqual(out["conformance-check"].status, pp.PASS)
                self.assertTrue(any(c[:2] == ["make", "conformance-check"] for c in cmds_of(s, "make")))
                self.assertEqual(out["deps"].status, pp.PASS)

    # F4: CI's plain lint has no only-new-issues; the tagged tests/ runs do
    def test_F4_plain_lint_checks_whole_packages_tagged_runs_are_only_new(self):
        out, s = run_steps(["internal/a/a.go"], {"product": "0", "skip_go": "false"})
        lints = cmds_of(s, "/fake/bin/golangci-lint")
        plain = [c for c in lints if "--build-tags" not in c]
        tagged = [c for c in lints if "--build-tags" in c]
        self.assertTrue(plain and tagged)
        for c in plain:
            self.assertFalse([a for a in c if a.startswith("--new-from-rev")], c)
        self.assertTrue(any(c[-2:] == ["./internal/...", "./cmd/..."] for c in plain), plain)
        self.assertTrue(any(c[-1] == "./..." for c in plain), plain)  # traces
        for c in tagged:
            self.assertTrue([a for a in c if a.startswith("--new-from-rev=")], c)

    # F5: the registry gate self-tests clone changelog.d and the registry
    def test_F5_changelog_fragment_marks_the_gate_selftests_not_verified(self):
        for f in ("changelog.d/9.fixed.md", "tests/conformance/registry/rows/x.yaml", "docs/features.md"):
            with self.subTest(f):
                out, _ = run_steps([f])
                o = out["registry-gate-selftests"]
                self.assertEqual((o.status, o.unverified), (pp.SKIPPED, True), o.detail)
                self.assertIn("CI conformance-inventory runs this", o.detail)

    # F6: -short is a parquets3-only flag in CI
    def test_F6_short_only_for_parquets3(self):
        root = probe_root("internal/delete", "internal/storage/parquets3")
        out, s = run_steps(["internal/delete/d.go", "internal/storage/parquets3/p.go"], root=root)
        tests = go_test_patterns(s)
        for c in tests:
            if "-short" in c:
                self.assertEqual([a for a in c if a.startswith("./")], ["./internal/storage/parquets3"], c)
            else:
                self.assertIn("./internal/delete", c)
        self.assertTrue(any("-short" in c for c in tests))
        self.assertTrue(any("-short" not in c and "./internal/delete" in c for c in tests))

    # F7/F8: the harness job runs more than pytest, with its own temporary directory and project name
    def test_F7_harness_runs_what_proof_metrics_runs(self):
        out, s = run_steps(["scripts/proof/runner.py"], script=lambda cmd, kw: fake_archive(cmd))
        joined = [" ".join(c) for c, _ in real_calls(s)]
        self.assertTrue(any("pyflakes scripts/proof" in j for j in joined), joined)
        self.assertTrue(any("--cov-fail-under=90" in j for j in joined), joined)
        self.assertTrue(any("scripts.proof.metrics scripts/proof/fixtures/cases --check" in j for j in joined), joined)

    def test_F7_harness_triggers_include_the_proof_stack_files(self):
        for f in ("deployment/docker/proof/seed.sh", "deployment/docker/docker-compose-proof.yml", "tests/parity/testdata/tiecut_golden.json"):
            with self.subTest(f):
                self.assertIn("harness-tests", running(make_ctx([f])))

    def test_F8_harness_has_a_private_tmpdir_and_project(self):
        out, s = run_steps(["scripts/proof/runner.py"], script=lambda cmd, kw: fake_archive(cmd), environ={"PATH": "", "TMPDIR": "/shared/tmp", "AB_PROJECT": "lh-ab"})
        envs = [kw["env"] for c, kw in s.calls if "pytest" in c and kw.get("env")]
        self.assertTrue(envs, "pytest was not run with its own environment")
        for e in envs:
            self.assertNotEqual(e["TMPDIR"], "/shared/tmp")
            self.assertNotEqual(e["AB_PROJECT"], "lh-ab")
            self.assertFalse(os.path.exists(e["TMPDIR"]), "the private TMPDIR is removed after the step")

    # F9: the headline cannot say PASS while rows are not verified
    def test_F9_headline_counts_what_was_not_verified(self):
        o = [pp.Outcome("a", pp.PASS, "fine", 0.1), pp.Outcome("c", pp.SKIPPED, "no tool", 0.0, unverified=True)]
        head = pp.render_summary(o).splitlines()[0]
        self.assertTrue(head.startswith("RESULT: PASS (1 not verified)"), head)

    # F10: a branch that changes the gate gets a preview, not a verdict
    def test_F10_gate_change_is_marked_unverified_in_the_row(self):
        report = "## Registry gate: PASS\n\nPR kind: tooling\n"
        notice = "notice: this branch changes the gate itself. CI judges it with the gate code of origin/main\n"
        ctx = make_ctx(sys_=FakeSys(mkdtemp(), lambda cmd, kw: (0, notice + report) if cmd[0] == "bash" else None))
        r = pp.step_registry_gate(ctx)
        self.assertEqual(r.status, pp.PASS)
        self.assertIn("changes the gate itself", r.detail)
        o = pp.execute(ctx, [pp.Step("registry-gate", lambda c: None, lambda c: r)])[0]
        self.assertTrue(o.unverified or o.partial)

    # F13: the failure detail is the first error line, not actionlint's trailing marker
    def test_F13_failure_detail_is_the_first_error_line(self):
        out = (".github/workflows/x.yaml:12:9: property \"foo\" is not defined [expression]\n"
               "   |\n12 |       - run: echo ${{ foo }}\n   |         ^~~~\n")
        ctx = make_ctx(sys_=FakeSys(mkdtemp(), lambda cmd, kw: (1, out) if "actionlint" in cmd[0] else None, tools={"actionlint"}))
        r = pp.step_actionlint(ctx)
        self.assertEqual(r.status, pp.FAIL)
        self.assertIn("x.yaml:12:9", r.detail)

    def test_F13_the_make_variable_does_not_collide_with_an_exported_scope(self):
        mk = (REPO / "Makefile").read_text()
        body = re.search(r"(?ms)^pre-push:.*?(?=^\S)", mk).group(0)
        self.assertIn("$(PREPUSH_SCOPE)", body)
        self.assertNotRegex(mk, r"(?m)^SCOPE\s*\?=")


# ---------------------------------------------------------------------------------------------------------------
# Drift guards: the tables of pre_push.py are checked against the repository they describe.
def git_files(*patterns):
    p = subprocess.run(["git", "ls-files", *patterns], cwd=REPO, capture_output=True, text=True)
    if p.returncode != 0:
        raise unittest.SkipTest("not a git checkout")
    return [f for f in p.stdout.split("\n") if f]


GO_LITERAL = re.compile(r'"((?:[^"\\\n]|\\.)*)"|`([^`]*)`')
# top-level names whose readers are not rows of GO_TEST_READERS: module-wide inputs (go.mod, go.sum, go.work, patches/ make
# the step NOT VERIFIED), and the Go source roots (tests that scan sources of other packages are a documented limit).
DRIFT_WIDE = {"go.mod", "go.sum", "go.work", "go.work.sum", "patches", ".golangci.yml"}
DRIFT_SOURCE_ROOTS = {"internal", "cmd", "lakehouse-traces"}


def ci_tested_package(f):
    d = os.path.dirname(f)
    if f.startswith("lakehouse-traces/"):
        return d == "lakehouse-traces" or d.startswith("lakehouse-traces/internal")
    return d.startswith(("internal", "cmd"))


def go_test_references():
    """{top-level repository name: {(module, package dir)}} for string literals of the Go files CI tests that name it."""
    tops = sorted({f.split("/")[0] for f in git_files()})
    out: dict = {}
    for f in git_files("*.go"):
        if "/deps/" in f or f.startswith("deps/") or not ci_tested_package(f):
            continue
        text = (REPO / f).read_text(encoding="utf-8", errors="replace")
        module = "traces" if f.startswith("lakehouse-traces/") else "logs"
        d = os.path.dirname(f)
        d = d[len("lakehouse-traces/"):] if module == "traces" and d != "lakehouse-traces" else ("." if module == "traces" else d)
        for m in GO_LITERAL.finditer(text):
            s = m.group(1) or m.group(2) or ""
            if s.startswith("github.com/"):
                continue
            for top in tops:
                if s == top or re.search(r"(^|/)" + re.escape(top) + r"(/|$)", s):
                    out.setdefault(top, set()).add((module, d))
    return out


class GoTestReadersDrift(unittest.TestCase):
    """A Go test that reads a repository file the table does not know about would be skipped by a docs-only change
    (the false green of review finding F1): the test fails until the table has the row."""

    @classmethod
    def setUpClass(cls):
        cls.refs = go_test_references()

    def table_pairs(self, top):
        return {(m, d) for prefix, rows in pp.GO_TEST_READERS.items() if prefix.split("/")[0] == top for m, d, _ in rows}

    def test_every_go_test_that_reads_a_repository_path_has_a_row(self):
        for top, pairs in sorted(self.refs.items()):
            if top in DRIFT_WIDE or top in DRIFT_SOURCE_ROOTS:
                continue
            with self.subTest(top):
                known = self.table_pairs(top)
                self.assertTrue(known or pp.is_inert(top + "/") or pp.is_inert(top),
                                f"{top}: read by {sorted(pairs)} but in neither GO_TEST_READERS nor GO_INERT")
                if not known:
                    self.fail(f"{top} is GO_INERT but a Go test of {sorted(pairs)} names it: add a GO_TEST_READERS row")
                self.assertLessEqual(pairs, known, f"{top}: add the missing package to GO_TEST_READERS")

    def test_every_row_is_still_read_by_its_package(self):
        for prefix, rows in pp.GO_TEST_READERS.items():
            top = prefix.split("/")[0]
            for m, d, _ in rows:
                with self.subTest(f"{prefix} -> {d}"):
                    self.assertIn((m, d), self.refs.get(top, set()), "the package no longer names the path: drop the row")

    def test_narrowed_rows_name_the_only_test_that_reads_the_path(self):
        for prefix, rows in pp.GO_TEST_READERS.items():
            top = prefix.split("/")[0]
            for m, d, run in rows:
                if not run:
                    continue
                with self.subTest(f"{prefix} -> {d} {run}"):
                    funcs = set()
                    for f in git_files(f"{d}/*_test.go" if m == "logs" else f"lakehouse-traces/{d}/*_test.go"):
                        text = (REPO / f).read_text(encoding="utf-8", errors="replace")
                        for lit in GO_LITERAL.finditer(text):
                            s = lit.group(1) or lit.group(2) or ""
                            if s == top or re.search(r"(^|/)" + re.escape(top) + r"(/|$)", s):
                                head = text[: lit.start()]
                                fn = list(re.finditer(r"(?m)^func (\w+)\(", head))
                                funcs.add(fn[-1].group(1) if fn else "")
                    self.assertTrue(funcs, "no reader found")
                    for fn in funcs:
                        self.assertTrue(fn.startswith("Test") and re.search(run, fn),
                                        f"{fn} reads {top}/ but -run {run} does not select it: use the whole package")

    def test_inert_prefixes_are_not_read_by_any_go_test_of_a_ci_tested_package(self):
        for top in sorted(self.refs):
            if top in DRIFT_WIDE or top in DRIFT_SOURCE_ROOTS or top in {p.split("/")[0] for p in pp.GO_TEST_READERS}:
                continue
            self.fail(f"{top} is read by {sorted(self.refs[top])} and is not in GO_TEST_READERS")

    def test_every_top_level_path_of_the_repository_is_classified(self):
        """A new top-level directory or file must be a reader row, inert, wide, a Go source root or a package."""
        for top in sorted({f.split("/")[0] for f in git_files()}):
            with self.subTest(top):
                f = top + "/x" if (REPO / top).is_dir() else top
                ok = (top in DRIFT_WIDE or top in DRIFT_SOURCE_ROOTS or pp.is_inert(f) or bool(pp.reader_rows(f))
                      or pp.is_inert(top) or top.startswith("Dockerfile") or top in {"README.md", "go.work", ".github"})
                self.assertTrue(ok, f"{top} is not classified for the Go test step: add it to GO_TEST_READERS or GO_INERT")


def workflow_jobs(name):
    import yaml  # PyYAML: installed by the changelog-tests job (scripts/ci/requirements.txt)
    return yaml.safe_load((REPO / ".github/workflows" / name).read_text(encoding="utf-8"))["jobs"]


def workflow_paths_filtered(name):
    import yaml
    doc = yaml.safe_load((REPO / ".github/workflows" / name).read_text(encoding="utf-8"))
    on = doc.get("on", doc.get(True)) or {}
    return bool(isinstance(on, dict) and (on.get("pull_request") or {}).get("paths"))


def step_key(step):
    if step.get("name"):
        return step["name"]
    uses = step.get("uses", "")
    if uses.startswith("golangci/golangci-lint-action"):
        return ("golangci-lint " + (step.get("with") or {}).get("args", "")).strip()
    return None


@unittest.skipUnless(shutil.which("git") and importlib.util.find_spec("yaml"), "needs git and PyYAML")
class CiJobMapDrift(unittest.TestCase):
    """A job or a step added to a mirrored workflow must be added to CI_JOBS, so a new CI check is noticed instead of
    silently missing from the preview (review finding F11)."""

    WORKFLOWS = ("ci.yaml", "conformance.yaml", "proof-metrics.yaml", "changelog-pr.yaml", "workflow-lint.yaml")

    def test_every_job_of_a_mirrored_workflow_is_in_the_map_and_vice_versa(self):
        actual = {f"{w}:{j}" for w in self.WORKFLOWS for j in workflow_jobs(w)}
        self.assertEqual(sorted(actual - set(pp.CI_JOBS)), [], "new CI job: add it to CI_JOBS (and to the CONTRIBUTING table)")
        self.assertEqual(sorted(set(pp.CI_JOBS) - actual), [], "CI_JOBS names a job that no longer exists")

    def test_every_step_of_a_mirrored_job_is_in_the_map_and_vice_versa(self):
        for key, spec in pp.CI_JOBS.items():
            if spec["steps"] is None:
                continue
            w, j = key.split(":")
            with self.subTest(key):
                actual = {k for k in (step_key(s) for s in workflow_jobs(w)[j].get("steps", [])) if k}
                actual -= {"Prepare VL deps (logs)"}  # none: deps job only
                self.assertEqual(sorted(actual - set(spec["steps"])), [], "new CI step: add it to CI_JOBS")
                self.assertEqual(sorted(set(spec["steps"]) - actual), [], "CI_JOBS names a step that no longer exists")

    def test_always_flag_matches_the_job_condition(self):
        for key, spec in pp.CI_JOBS.items():
            w, j = key.split(":")
            with self.subTest(key):
                self.assertEqual(spec["always"], "if" not in workflow_jobs(w)[j] and not workflow_paths_filtered(w))

    def test_local_steps_exist_and_covers_name_them(self):
        names = {s.name for s in pp.build_steps()}
        for key, spec in pp.CI_JOBS.items():
            with self.subTest(key):
                for n in spec["local"]:
                    self.assertIn(n, names)
                for step, covered in (spec["steps"] or {}).items():
                    if not covered.startswith(pp.NC):
                        self.assertTrue(any(covered.startswith(n) for n in names), f"{step}: {covered}")

    def test_every_pre_push_step_that_mirrors_ci_is_in_the_map(self):
        mirrored = {n for spec in pp.CI_JOBS.values() for n in spec["local"]}
        local_only = {"fetch-base", "contains-main", "commits-signed", "no-ai-attribution", "registry-gate"}  # owner rules, other workflows
        for s in pp.build_steps():
            if s.name not in mirrored:
                self.assertIn(s.name, local_only, f"{s.name} mirrors no CI job in CI_JOBS")


class GateAndSelftestSteps(unittest.TestCase):
    def test_selftests_run_only_under_scope_all_and_say_so_otherwise(self):
        s = FakeSys(mkdtemp())
        r = pp.step_gate_selftests(make_ctx(["changelog.d/1.fixed.md"], sys_=s))
        self.assertEqual((r.status, r.unverified, s.calls), (pp.SKIPPED, True, []))
        self.assertIn("CI conformance-inventory runs this", r.detail)
        s = FakeSys(mkdtemp())
        r = pp.step_gate_selftests(make_ctx([], scope_all=True, sys_=s))
        self.assertEqual(r.status, pp.PASS)
        ran = [" ".join(c) for c, _ in s.calls]
        self.assertTrue(any("test_check_registry_touch.sh" in x for x in ran))
        self.assertTrue(any("test_gate_workflow_emulation.sh" in x for x in ran))
        self.assertTrue(any("test_pr_classify_toolchain.py" in x for x in ran))

    def test_selftests_do_not_apply_to_an_unrelated_diff(self):
        ctx = make_ctx(["internal/a/a.go"])
        self.assertIn("diff touches nothing", pp.applies_gate_selftests(ctx))

    def test_gate_notice_marks_the_row_partial_and_previews(self):
        notice = "notice: this branch changes the gate itself. CI judges it with the gate code of origin/main\n## Registry gate: PASS\n"
        r = pp.step_registry_gate(make_ctx(sys_=FakeSys(mkdtemp(), lambda c, k: (0, notice) if c[0] == "bash" else None)))
        self.assertIn("PREVIEW ONLY", r.detail)
        self.assertTrue(r.partial)
        clean = pp.step_registry_gate(make_ctx(sys_=FakeSys(mkdtemp(), lambda c, k: (0, "## Registry gate: PASS\n") if c[0] == "bash" else None)))
        self.assertFalse(clean.partial)

    def test_lint_logs_selftests_need_a_pin_clone_and_are_unverified_offline(self):
        root = mkdtemp()
        (pathlib.Path(root) / ".github/workflows").mkdir(parents=True)
        (pathlib.Path(root) / ".github/workflows/ci.yaml").write_text("env:\n  VL_VERSION_LOGS: v1.53.0\n")
        cache = mkdtemp()
        s = FakeSys(root, environ={"PATH": "", "PRE_PUSH_CACHE": cache})
        ctx = make_ctx([PATCHES + "vl-logs/x.patch"], sys_=s, root=root)
        ctx.offline = True
        r = pp.step_lint_logs_selftests(ctx)
        self.assertEqual((r.status, r.unverified), (pp.SKIPPED, True))
        self.assertIn("v1.53.0", r.detail)
        # with a cached pristine clone the checks run with LOGS_PIN_TREE pointing at it
        pin = pathlib.Path(cache) / "vl-logs-v1.53.0"
        pin.mkdir()
        (pin / "go.mod").write_text("module x\n")
        r = pp.step_lint_logs_selftests(ctx)
        self.assertEqual(r.status, pp.PASS)
        envs = [kw["env"] for c, kw in s.calls if kw.get("env")]
        self.assertTrue(envs and all(e["LOGS_PIN_TREE"] == str(pin) for e in envs))
        ran = [os.path.basename(c[1]) for c, _ in s.calls if c[0] == "bash"]
        self.assertEqual(ran, ["check_patches_equal_test.sh", "check_patches_equal.sh"])

    def test_scope_all_runs_the_four_other_lint_logs_selftests(self):
        root = mkdtemp()
        (pathlib.Path(root) / ".github/workflows").mkdir(parents=True)
        (pathlib.Path(root) / ".github/workflows/ci.yaml").write_text("env:\n  VL_VERSION_LOGS: v1.53.0\n")
        cache = mkdtemp()
        pin = pathlib.Path(cache) / "vl-logs-v1.53.0"
        pin.mkdir()
        (pin / "go.mod").write_text("module x\n")
        s = FakeSys(root, environ={"PATH": "", "PRE_PUSH_CACHE": cache})
        pp.step_lint_logs_selftests(make_ctx([], scope_all=True, sys_=s, root=root))
        ran = {os.path.basename(c[1]) for c, _ in s.calls if c[0] == "bash"}
        self.assertEqual(ran, {os.path.basename(t) for t in pp.LINT_LOGS_SELFTESTS} | {"check_patches_equal.sh"})


class Honesty(unittest.TestCase):
    def test_tree_note_counts_tracked_and_untracked_files(self):
        self.assertEqual(pp.tree_note(""), "")
        self.assertIn("1 uncommitted tracked change(s)", pp.tree_note(" M a.go\n"))
        n = pp.tree_note(" M a.go\n?? b.go\n?? c.go\n")
        self.assertIn("1 uncommitted tracked change(s) and 2 untracked file(s)", n)
        only = pp.tree_note("?? new.go\n")
        self.assertIn("1 untracked file(s)", only)
        self.assertNotIn("tracked change", only)

    def test_zizmor_without_a_token_is_partial(self):
        s = FakeSys(mkdtemp(), tools={"zizmor"}, environ={"PATH": ""})
        r = pp.step_zizmor(make_ctx(sys_=s))
        self.assertEqual(r.status, pp.PASS)
        self.assertIn("online audits", r.partial)
        s = FakeSys(mkdtemp(), tools={"zizmor"}, environ={"PATH": "", "GH_TOKEN": "x"})
        self.assertFalse(pp.step_zizmor(make_ctx(sys_=s)).partial)

    def test_classifier_change_is_noted_on_the_scope_row(self):
        def script(cmd, kw):
            j = " ".join(cmd)
            if cmd[:2] == ["git", "merge-base"]:
                return 0, "abc\n"
            if cmd[:2] == ["git", "diff"]:
                return 0, "scripts/ci/pr_classify.py\0"
            if "pr_classify.py" in j:
                return 0, "product=0\nskip_go=true\n"
            return 0, ""
        r = pp.step_scope(make_ctx(sys_=FakeSys(mkdtemp(), script)))
        self.assertIn("base branch", r.partial)

    def test_summary_names_never_run_jobs_and_never_calls_a_partial_run_clean(self):
        o = [pp.Outcome("go test (logs)", pp.PASS, "1 package(s)", 1.0, partial="picked from the diff")]
        s = pp.render_summary(o)
        self.assertTrue(s.startswith("RESULT: PASS (1 not verified)"))
        self.assertIn("ci.yaml:helm-config-drift", s)
        self.assertIn("ci.yaml:docker-logs", s)

    def test_go_test_pass_is_never_reported_as_the_full_suite(self):
        root = probe_root("internal/a")
        out, _ = run_steps(["internal/a/a.go"], root=root)
        self.assertTrue(out["go test (logs)"].partial)
        self.assertIn("CI runs every package", out["go test (logs)"].partial)


class FixtureGenerator(unittest.TestCase):
    """The generator-committed check of proof-metrics.yaml runs in a copy of HEAD: it must catch drift and must never
    write into the worktree (a developer's uncommitted edits)."""

    def setUp(self):
        self.d = pathlib.Path(mkdtemp("pp-gen-"))
        self.env = dict(os.environ, GIT_AUTHOR_NAME="T", GIT_AUTHOR_EMAIL="t@x.org", GIT_COMMITTER_NAME="T",
                        GIT_COMMITTER_EMAIL="t@x.org", GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_SYSTEM="/dev/null")
        fx = self.d / "scripts/proof/fixtures"
        (fx / "cases").mkdir(parents=True)
        (fx / "cases/a.json").write_text('{"a": 1}\n')
        self.gen = fx / "_gen.py"
        self.gen.write_text(
            "import os\nHERE = os.path.dirname(os.path.abspath(__file__))\n"
            "os.makedirs(os.path.join(HERE, 'cases'), exist_ok=True)\n"
            "open(os.path.join(HERE, 'cases', 'a.json'), 'w').write('{\"a\": 1}\\n')\n")
        self.g("init", "-q", "-b", "main")
        self.g("add", "-A")
        self.g("commit", "-q", "-m", "base")

    def g(self, *a):
        subprocess.run(["git", *a], cwd=self.d, env=self.env, capture_output=True, text=True, check=True)

    def ctx(self):
        return pp.Ctx(sys=pp.Sys(self.d, self.env), py=sys.executable)

    def test_committed_output_passes_and_the_worktree_is_untouched(self):
        (self.d / "scripts/proof/fixtures/cases/a.json").write_text('{"a": 1}\n')  # unchanged
        r = pp.generator_drift(self.ctx())
        self.assertEqual(r.status, pp.PASS, r.detail)

    def test_drift_between_the_generator_and_the_committed_cases_fails(self):
        self.gen.write_text(self.gen.read_text().replace('"a": 1', '"a": 2'))
        self.g("add", "-A")
        self.g("commit", "-q", "-m", "generator changed, cases not regenerated")
        r = pp.generator_drift(self.ctx())
        self.assertEqual(r.status, pp.FAIL)
        self.assertIn("not committed", r.detail)

    def test_a_case_the_generator_creates_but_nobody_committed_fails(self):
        self.gen.write_text(self.gen.read_text() + "open(os.path.join(HERE, 'cases', 'new.json'), 'w').write('{}')\n")
        self.g("add", "-A")
        self.g("commit", "-q", "-m", "generator adds a case")
        self.assertEqual(pp.generator_drift(self.ctx()).status, pp.FAIL)

    def test_the_generator_never_writes_into_the_worktree(self):
        edited = self.d / "scripts/proof/fixtures/cases/a.json"
        edited.write_text('{"a": "uncommitted edit"}\n')
        untracked = self.d / "scripts/proof/fixtures/cases/mine.json"
        untracked.write_text("{}\n")
        pp.generator_drift(self.ctx())
        self.assertEqual(edited.read_text(), '{"a": "uncommitted edit"}\n')
        self.assertTrue(untracked.exists())

    def test_the_temporary_copy_is_removed(self):
        before = set(os.listdir(tempfile.gettempdir()))
        pp.generator_drift(self.ctx())
        leftovers = [n for n in set(os.listdir(tempfile.gettempdir())) - before if n.startswith("pp-fixgen-")]
        self.assertEqual(leftovers, [])


class HarnessStep(unittest.TestCase):
    def test_each_proof_metrics_command_runs_in_order_with_the_ci_flags(self):
        out, s = run_steps(["scripts/proof/x.py"], script=lambda cmd, kw: fake_archive(cmd), names={"harness-tests"})
        self.assertEqual(out["harness-tests"].status, pp.PASS, out["harness-tests"].detail)
        ran = [c for c, _ in real_calls(s) if c[1:2] == ["-m"]]
        names = [c[2] for c in ran]
        self.assertEqual(names, ["pyflakes", "pytest", "pytest", "scripts.proof.metrics"])
        proof = [c for c in ran if c[2] == "pytest" and "scripts/proof/tests" in c][0]
        for cov in ("scripts.proof.metrics", "scripts.proof.runner", "scripts.proof.visual", "scripts.proof.stack", "scripts.proof.comment"):
            self.assertIn(f"--cov={cov}", proof)
        self.assertIn("--cov-fail-under=90", proof)
        self.assertIn("docker compose config", out["harness-tests"].partial)

    def test_a_failing_pyflakes_stops_the_step(self):
        def script(cmd, kw):
            return (1, "scripts/proof/x.py:1:1 'os' imported but unused\n") if "pyflakes" in cmd else fake_archive(cmd)
        out, _ = run_steps(["scripts/proof/x.py"], script=script, names={"harness-tests"})
        self.assertEqual(out["harness-tests"].status, pp.FAIL)
        self.assertIn("imported but unused", out["harness-tests"].detail)

    def test_missing_pyflakes_or_cov_is_unverified_with_the_venv_hint(self):
        def script(cmd, kw):
            return (1, "No module named pytest_cov") if cmd[:2] == ["python3", "-c"] and "pytest_cov" in cmd[2] else None
        out, _ = run_steps(["scripts/proof/x.py"], script=script, names={"harness-tests"})
        o = out["harness-tests"]
        self.assertEqual((o.status, o.unverified), (pp.SKIPPED, True))
        self.assertIn("pytest_cov", o.detail)
        self.assertIn("PRE_PUSH_PYTHON", o.detail)


class DocsDoNotOverstate(unittest.TestCase):
    """The review's wording finding: the tool is a preview, and its docs say what it covers and what it cannot catch."""

    @classmethod
    def setUpClass(cls):
        cls.doc = (REPO / "CONTRIBUTING.md").read_text(encoding="utf-8")
        cls.section = cls.doc[cls.doc.index("### Before you push"):cls.doc.index("### Test time budget")]

    def test_no_overstating_claims_remain(self):
        for text, where in ((pp.__doc__, "pre_push.py"), (self.section, "CONTRIBUTING.md"), ((REPO / "Makefile").read_text(), "Makefile")):
            for phrase in ("same inputs CI uses", "scoped fast tier of CI locally", "calls the tools and scripts CI calls (it re-implements"):
                self.assertNotIn(phrase, text, where)
        self.assertIn("preview", self.section)
        self.assertIn("is not CI", self.section)
        self.assertIn("NOT the same as CI", pp.__doc__)

    def test_the_scope_variable_is_prefixed_everywhere_it_is_documented(self):
        for text, where in ((self.section, "CONTRIBUTING.md"), (pp.__doc__, "pre_push.py"), ((REPO / "Makefile").read_text(), "Makefile")):
            self.assertIsNone(re.search(r"(?<![A-Z_])SCOPE=all", text), where)
            self.assertIsNone(re.search(r"(?m)^SCOPE\s*\?=", text), where)
        self.assertIn("PREPUSH_SCOPE=all", self.section)

    def test_every_step_has_a_row_in_the_per_step_table(self):
        for s in pp.build_steps():
            self.assertIn(f"`{s.name}`", self.section, f"{s.name} is not in the table")

    def test_the_table_cites_workflow_lines_that_exist(self):
        refs = re.findall(r"(\b[\w-]+\.yaml):(\d+)(?:-(\d+))?", self.section)
        self.assertGreater(len(refs), 15)
        for wf, a, b in refs:
            path = REPO / ".github/workflows" / wf
            self.assertTrue(path.is_file(), wf)
            self.assertLessEqual(int(b or a), len(path.read_text().splitlines()), f"{wf}:{a}")

    def test_what_it_cannot_catch_is_listed(self):
        tail_ = self.section[self.section.index("cannot catch"):]
        for word in ("Docker", "Flaky", "Linux-only", "coverage", "-race", "Code scanning", "depend on the touched ones"):
            self.assertIn(word, tail_)


if __name__ == "__main__":
    unittest.main()
