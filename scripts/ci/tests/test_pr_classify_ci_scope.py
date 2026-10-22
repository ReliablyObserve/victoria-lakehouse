"""pr_classify.py --ci-scope: which CI groups does a change need?

Four layers:
  * the path table (ci_scope_decide with a fake diff reader): every class and every path that must run everything;
  * the workflow-edit rule and the digest-pin rule on real temp git repositories, through the script itself;
  * the registry-gate keys: printed byte for byte as without the flag;
  * the recorded pull requests (testdata/ci_scope_replay.json): the scope of 70 merged changes is replayed from
    their file lists and the recorded diff verdicts. The live replay over the git objects is
    ci_scope_replay_check.py (it needs the full history, so it is not discovered here).
"""
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(HERE, ".."))
import pr_classify as pc  # noqa: E402

SCRIPT = os.path.join(HERE, "..", "pr_classify.py")
FIXTURE = os.path.join(HERE, "testdata", "ci_scope_replay.json")
EVENT = ["--ci-scope", "--event", "pull_request"]
LEGACY_KEYS = ("exempt", "product", "reason", "detail", "gate_file")


class FakeEvidence:
    """The two content checks of the scope rules, answered from a table."""

    def __init__(self, pins=(), workflows=None):
        self.pins = set(pins)
        self.workflows = workflows or {}

    def image_pins_only(self, path):
        return path in self.pins

    def workflow_jobs(self, path):
        return self.workflows[path]


def scope(files, pins=(), workflows=None, release=False):
    return pc.ci_scope_decide(list(files), FakeEvidence(pins, workflows), release)


def group(res):
    return res["skip_go"], res["skip_image"], res["skip_parity"]


def full(res):
    return res["ci_class"] == "product" and group(res) == ("false", "false", "false") and res["force"] == "" and res["ci_reason"]


class PathTable(unittest.TestCase):
    """Every path that must run everything, and every class of change that may not."""

    PRODUCT = (
        "go.mod", "go.sum", "go.work", "lakehouse-traces/go.mod", "lakehouse-traces/go.sum", "Makefile",
        "patches/vl-logs/external.go.src", "patches/vl-traces/x.patch", "patches/README.md", "patches/vl-traces/DIVERGENCE.md",
        "patches/upstream-copies/vl-logs/external_query.RunQueryExternal.txt", "Dockerfile", "Dockerfile.logs", "Dockerfile.traces",
        "charts/victoria-lakehouse/templates/deployment.yaml", "charts/victoria-lakehouse/values.yaml",
        "charts/victoria-lakehouse/Chart.yaml", "charts/victoria-lakehouse/templates/NOTES.txt",
        "tests/conformance/registry/rows/x.yaml", "tests/conformance/registry/features.yaml",
        "tests/conformance/registry/lockcode.go", "tests/parity/known_failures.txt", "tests/parity/lock_cells.txt",
        "tests/parity/queries_test.go", "tests/e2e/run.sh", "tests/ingestmatrix/corpus.json", "tests/s3compat/go.mod",
        "internal/storage/a.go", "internal/storage/a_test.go", "internal/storage/testdata/in.json", "internal/ui/static/app.js",
        "cmd/lakehouse-logs/main.go", "lakehouse-traces/internal/x/y.go",
        "scripts/foo/main.go", "scripts/x/go.mod", "scripts/x/go.sum", "scripts/ci/tests/helper_test.go",
        "alerts/lakehouse.yaml", "dashboards/lakehouse.json", ".golangci.yml", ".golangci.yaml",
        "deployment/docker/Dockerfile.logs", "deployment/docker/Dockerfile.traces",
        "some-new-top-level-dir/file.txt", "LICENSE", ".gitignore", "bench-results/run.json",
        # the classifier, its wiring and its fixtures never scope themselves
        "scripts/ci/pr_classify.py", "scripts/ci/check_changelog_pr.py", ".github/actions/ci-scope/action.yml",
        ".github/actions/ci-scope/helper.sh", "scripts/ci/tests/ci_scope_replay_check.py",
        "scripts/ci/tests/testdata/ci_scope_replay.json",
        # paths that are not clean text
        "docs/a\nb.md", "docs/a\rb.md", "docs/a\x85b.md", "docs/\udcffb.md", "docs/a\x00b.md", "/docs/a.md", "docs/../go.mod",
    )

    def test_each_product_path_runs_everything(self):
        for p in self.PRODUCT:
            with self.subTest(path=repr(p)):
                self.assertTrue(full(scope([p])), scope([p]))

    def test_a_product_path_among_infra_paths_runs_everything(self):
        res = scope(["docs/a.md", "scripts/ci/x.sh", "internal/a.go", ".github/workflows/e2e.yaml"])
        self.assertTrue(full(res))
        self.assertIn("internal/a.go", res["ci_reason"])

    def test_infra_classes(self):
        cases = {
            "docs": ["docs/x.md", "CHANGELOG.md", "README.md", "website/docs/x.md", "website/src/pages/index.tsx",
                     "internal/x/README.md", "internal/x/RUNBOOK.md", "charts/c/README.md", "charts/c/notes.txt",
                     "tests/e2e/README.md", "lakehouse-traces/README.md"],
            "ci": ["scripts/ci/foo.sh", "scripts/ci/tests/test_x.py", "scripts/bench/ab/run.py", ".github/dependabot.yml",
                   ".github/CODEOWNERS", ".github/workflows/e2e.yaml", ".github/workflows/s3-compat.yaml",
                   "tests/playwright/spec.ts", "tests/verification/matrix.yaml"],
            "deps": ["scripts/proof/requirements.txt", "tests/readers/requirements.txt", "tests/playwright/package-lock.json",
                     "website/package.json"],
            "stack": ["deployment/docker/docker-compose-e2e.yml", "deployment/docker/Dockerfile.grafana",
                      "deployment/docker/minio-src/minio.Dockerfile", "deployment/docker/lakehouse-e2e-config.yml",
                      "tests/s3compat/compose/minio.yml", "tests/readers/Dockerfile.duckdb", "scripts/bench/compaction/compose.yml"],
        }
        for cls, paths in cases.items():
            for p in paths:
                with self.subTest(cls=cls, path=p):
                    res = scope([p])
                    # website/package.json is documentation for the report only; every class skips the three groups
                    self.assertEqual(group(res), ("true", "true", "true"), res)
                    self.assertEqual(res["force"], "")
                    self.assertEqual(res["ci_reason"], "")
                    if p != "website/package.json":
                        self.assertEqual(res["ci_class"], cls, res)

    def test_mixed_infra_is_the_union(self):
        res = scope(["docs/x.md", "scripts/ci/foo.sh"])
        self.assertEqual((res["ci_class"], group(res)), ("mixed-infra", ("true", "true", "true")))

    def test_parity_tooling_keeps_the_parity_suite(self):
        for p in ("Dockerfile.datagen", "tests/parity/docker-compose.yml", "deployment/docker/Dockerfile.upstream-probe",
                  "scripts/ci/parity_ratchet.py", "scripts/ci/tests/test_parity_ratchet.py", "scripts/ci/requirements.txt"):
            with self.subTest(path=p):
                res = scope([p])
                self.assertEqual(group(res), ("true", "true", "false"), res)
                self.assertEqual(res["force"], "")

    def test_release_image_digest_pins_keep_image_and_parity(self):
        res = scope(["Dockerfile.logs", "Dockerfile.traces", "CHANGELOG.md"], pins={"Dockerfile.logs", "Dockerfile.traces"})
        self.assertEqual((res["ci_class"], group(res)), ("mixed-infra", ("true", "false", "false")))
        res = scope(["Dockerfile.logs"], pins={"Dockerfile.logs"})
        self.assertEqual((res["ci_class"], group(res)), ("release-image-pin", ("true", "false", "false")))
        # any other change to a release Dockerfile is a product change
        self.assertTrue(full(scope(["Dockerfile.logs", "Dockerfile.traces"], pins={"Dockerfile.logs"})))

    def test_release_metadata_shape_skips_every_group(self):
        res = scope(["CHANGELOG.md", "charts/victoria-lakehouse/Chart.yaml"], release=True)
        self.assertEqual((res["ci_class"], group(res), res["force"]), ("release-metadata", ("true", "true", "true"), ""))
        # the same files without the verified shape: the chart is packaged, so everything runs
        self.assertTrue(full(scope(["CHANGELOG.md", "charts/victoria-lakehouse/Chart.yaml"])))

    def test_consumer_scripts_force_their_jobs(self):
        res = scope(["scripts/ci/sharded_go_test.sh"])
        self.assertEqual(group(res), ("true", "true", "true"))
        self.assertEqual(res["force"], "ci.yaml:test-logs,ci.yaml:test-parquets3-heavy,ci.yaml:test-traces")
        self.assertEqual(scope(["scripts/ci/trivy_fetch_db.sh"])["force"], "security.yaml:trivy-image")
        self.assertEqual(scope(["scripts/bench/field_metadata/run.py"])["force"], "ci.yaml:field-metadata-perf")
        self.assertEqual(scope(["scripts/ci/parquet-readback/check.py"])["force"], "ci.yaml:parquet-readback")
        self.assertEqual(scope(["scripts/bench/compaction/ab.py"])["force"], "ci.yaml:storage-health-compaction")
        self.assertEqual(scope(["scripts/ci/short_gated_tests.sh"])["force"], "ci.yaml:test-parquets3-heavy")
        self.assertEqual(scope(["scripts/ci/gotest_report.py"])["force"].count("ci.yaml:"), 4)

    def test_codeql_configuration_forces_the_go_analysis(self):
        res = scope([".github/codeql/codeql-config.yml"])
        self.assertEqual((res["ci_class"], group(res), res["force"]), ("ci", ("true", "true", "true"), "codeql.yaml:analyze"))
        self.assertEqual(scope([".github/dependabot.yml"])["force"], "")  # the rest of .github/ forces nothing

    def test_every_consumer_path_exists(self):
        """A consumer entry that matches no file is dead: the script moved and the map did not follow."""
        for pattern, jobs in pc.CI_CONSUMERS:
            with self.subTest(pattern=pattern):
                where = os.path.join(ROOT, os.path.dirname(pattern))
                self.assertTrue(os.path.isdir(where), pattern)
                if "*" not in pattern:
                    self.assertTrue(os.path.isfile(os.path.join(ROOT, pattern)), pattern)
                for j in jobs:
                    wf, job = j.split(":")
                    with open(os.path.join(ROOT, ".github", "workflows", wf), encoding="utf-8") as fh:
                        self.assertRegex(fh.read(), r"(?m)^  " + re.escape(job) + r":\s*$")

    def test_empty_diff_runs_everything(self):
        self.assertTrue(full(scope([])))

    def test_workflow_that_cannot_be_judged_runs_everything(self):
        res = scope([".github/workflows/ci.yaml"], workflows={".github/workflows/ci.yaml": None})
        self.assertTrue(full(res))

    def test_scoped_answers_keep_the_invariant(self):
        for files in (["docs/a.md"], ["scripts/ci/x.sh"], ["Dockerfile.logs"], ["tests/parity/docker-compose.yml"], ["internal/a.go"]):
            res = scope(files, pins={"Dockerfile.logs"})
            if res["skip_go"] == "false":
                self.assertEqual((res["skip_image"], res["skip_parity"]), ("false", "false"), files)


class Repo:
    """A throwaway repository with a base commit and a head commit."""

    def __init__(self, base, head, tags=()):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name
        self.git("init", "-q", "-b", "main")
        self.write(base)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "base")
        self.base = self.rev("HEAD")
        for path, text in head.items():
            if text is None:
                os.remove(os.path.join(self.d, path))
        self.write({p: t for p, t in head.items() if t is not None})
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "head")
        self.head = self.rev("HEAD")
        for t in tags:
            self.git("tag", t, self.base)

    def git(self, *a):
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@e", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", *a],
                       cwd=self.d, check=True, capture_output=True)

    def rev(self, r):
        return subprocess.run(["git", "rev-parse", r], cwd=self.d, check=True, capture_output=True, text=True).stdout.strip()

    def write(self, files):
        for p, t in files.items():
            full_path = os.path.join(self.d, p)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w", encoding="utf-8", newline="") as fh:
                fh.write(t)

    def run(self, *extra, author="", approvers=None):
        cmd = [sys.executable, SCRIPT, "--base", self.base, "--head", self.head, *extra]
        if author:
            cmd += ["--author", author]
        if approvers:
            path = os.path.join(self.d, ".approvers-for-test")  # untracked: the diff is between commits
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("\n".join(approvers) + "\n")
            cmd += ["--approvers", path]
        return subprocess.run(cmd, cwd=self.d, capture_output=True, text=True)

    def scope(self, **kw):
        r = self.run(*EVENT, **kw)
        assert r.returncode == 0, r.stderr
        return dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)

    def close(self):
        self.tmp.cleanup()


WF = """name: CI
on:
  pull_request:
env:
  GOLANGCI_LINT_VERSION: v2.0.0
jobs:
  # the dependency cache
  deps:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - run: make deps
  test-logs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-go@v6
      - run: go test ./...
  lint-logs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - run: golangci-lint run
"""
SHA_A = "a" * 40
SHA_B = "b" * 40


class WorkflowEdits(unittest.TestCase):
    PATH = ".github/workflows/ci.yaml"

    def edit(self, fn, base=WF, extra_head=None):
        files = {self.PATH: fn(base)}
        files.update(extra_head or {})
        r = Repo({self.PATH: base}, files)
        self.addCleanup(r.close)
        return r.scope()

    def test_ref_bumps_force_no_job(self):
        for old, new in (("actions/checkout@v7", "actions/checkout@" + SHA_A + " # v7.0.1"),
                         ("actions/checkout@" + SHA_A, "actions/checkout@" + SHA_B + " # v7.0.2")):
            res = self.edit(lambda t: t.replace(old, new), base=WF.replace("actions/checkout@v7", old))
            self.assertEqual((res["ci_class"], group(res), res["force"]), ("deps", ("true", "true", "true"), ""), res)

    def test_a_different_action_forces_its_job(self):
        res = self.edit(lambda t: t.replace("actions/setup-go@v6", "other/setup-go@v6"))
        self.assertEqual((group(res), res["force"]), (("true", "true", "true"), "ci.yaml:test-logs"), res)

    def test_a_pin_that_moves_to_another_job_is_not_a_ref_bump(self):
        base = WF.replace("actions/checkout@v7", "actions/checkout@" + SHA_A)
        # the checkout step leaves deps and appears in lint-logs with another ref
        head = base.replace("      - uses: actions/checkout@" + SHA_A + "\n      - run: make deps", "      - run: make deps", 1)
        head = head.replace("  lint-logs:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@" + SHA_A,
                            "  lint-logs:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@" + SHA_B)
        r = Repo({self.PATH: base}, {self.PATH: head})
        self.addCleanup(r.close)
        res = r.scope()
        self.assertEqual(res["force"], "ci.yaml:deps,ci.yaml:lint-logs", res)

    def test_a_ref_bump_next_to_a_run_change_forces_only_that_job(self):
        res = self.edit(lambda t: t.replace("actions/setup-go@v6", "actions/setup-go@v7").replace("golangci-lint run", "golangci-lint run ./..."))
        self.assertEqual(res["force"], "ci.yaml:lint-logs", res)

    def test_lines_inside_a_job_force_that_job(self):
        res = self.edit(lambda t: t.replace("go test ./...", "go test -race ./..."))
        self.assertEqual((res["ci_class"], group(res), res["force"]), ("ci", ("true", "true", "true"), "ci.yaml:test-logs"), res)

    def test_a_removed_line_belongs_to_the_job_it_was_in(self):
        res = self.edit(lambda t: t.replace("      - run: make deps\n", ""))
        self.assertEqual(res["force"], "ci.yaml:deps", res)

    def test_a_new_job_is_forced(self):
        res = self.edit(lambda t: t + "  extra:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n")
        self.assertEqual(res["force"], "ci.yaml:extra", res)

    def test_lines_outside_every_job_run_the_whole_workflow(self):
        res = self.edit(lambda t: t.replace("v2.0.0", "v2.1.0"))
        self.assertEqual(group(res), ("true", "true", "true"), res)
        self.assertEqual(res["force"], "ci.yaml:deps,ci.yaml:lint-logs,ci.yaml:test-logs", res)
        res = self.edit(lambda t: t.replace("name: CI", "name: CI main"))
        self.assertEqual(res["force"].count("ci.yaml:"), 3)

    def test_a_workflow_without_jobs_runs_everything(self):
        res = self.edit(lambda t: t + "\n# edit\n", base="name: CI\non:\n  push:\n")
        self.assertEqual(res["ci_class"], "product", res)
        self.assertTrue(res["ci_reason"])

    def test_the_parity_workflow_keeps_the_parity_suite(self):
        base = "name: P\non:\n  push:\njobs:\n  parity:\n    steps:\n      - run: make parity\n"
        files = {".github/workflows/parity.yaml": base.replace("make parity", "make parity-all")}
        r = Repo({".github/workflows/parity.yaml": base}, files)
        self.addCleanup(r.close)
        res = r.scope()
        self.assertEqual((group(res), res["force"]), (("true", "true", "false"), "parity.yaml:parity"), res)

    def test_other_workflows_are_infra_without_force(self):
        r = Repo({".github/workflows/e2e.yaml": "name: E\njobs:\n  e2e:\n    steps:\n      - run: a\n"},
                 {".github/workflows/e2e.yaml": "name: E\njobs:\n  e2e:\n    steps:\n      - run: b\n"})
        self.addCleanup(r.close)
        res = r.scope()
        self.assertEqual((res["ci_class"], group(res), res["force"]), ("ci", ("true", "true", "true"), ""), res)

    def test_a_yaml_document_marker_line_is_a_changed_line(self):
        res = self.edit(lambda t: t.replace("  test-logs:\n", "  test-logs:\n    ---\n"))
        self.assertEqual(res["force"], "ci.yaml:test-logs", res)


class DockerfileDigests(unittest.TestCase):
    DF = "FROM golang:1.27@sha256:" + "1" * 64 + " AS build\nRUN go build\nFROM gcr.io/distroless/static:nonroot@sha256:" + "2" * 64 + "\n"

    def run_edit(self, fn, path="Dockerfile.logs"):
        r = Repo({path: self.DF}, {path: fn(self.DF)})
        self.addCleanup(r.close)
        return r.scope()

    def test_digest_only_edit_keeps_image_and_parity_but_skips_go(self):
        res = self.run_edit(lambda t: t.replace("1" * 64, "3" * 64))
        self.assertEqual((res["ci_class"], group(res)), ("release-image-pin", ("true", "false", "false")), res)

    def test_any_other_edit_runs_everything(self):
        for fn in (lambda t: t.replace("RUN go build", "RUN go build -v"), lambda t: t.replace("golang:1.27", "golang:1.28"),
                   lambda t: t.replace("1" * 64, "3" * 63)):
            self.assertTrue(full(self.run_edit(fn)))

    def test_a_digest_edit_in_a_test_stack_dockerfile_is_not_a_release_pin(self):
        res = self.run_edit(lambda t: t.replace("1" * 64, "3" * 64), path="deployment/docker/Dockerfile.grafana")
        self.assertEqual(group(res), ("true", "true", "true"), res)
        self.assertEqual(res["ci_class"], "stack")


class ReleaseMetadata(unittest.TestCase):
    CHART = 'apiVersion: v2\nname: c\ndescription: d\nversion: 0.146.4\nappVersion: "0.146.4"\n'
    CL_BASE = ("# Changelog\n\n## [Unreleased]\n\n### Added\n\n- **API proof.** text.\n\n"
               "## [0.146.4] - 2026-10-07\n\n### Changed\n\n- **Gate.** text.\n")
    CL_HEAD = ("# Changelog\n\n## [Unreleased]\n\n## [0.146.5] - 2026-10-07\n\n### Added\n\n- **API proof.** text.\n\n"
               "## [0.146.4] - 2026-10-07\n\n### Changed\n\n- **Gate.** text.\n")

    def repo(self):
        r = Repo({"CHANGELOG.md": self.CL_BASE, "charts/victoria-lakehouse/Chart.yaml": self.CHART},
                 {"CHANGELOG.md": self.CL_HEAD, "charts/victoria-lakehouse/Chart.yaml": self.CHART.replace("0.146.4", "0.146.5")})
        self.addCleanup(r.close)
        return r

    def test_exact_shape_by_an_approved_author_is_scoped(self):
        res = self.repo().scope(author="szibis", approvers=["szibis"])
        self.assertEqual((res["ci_class"], group(res), res["force"]), ("release-metadata", ("true", "true", "true"), ""), res)

    def test_nothing_is_decided_from_the_title_or_an_unapproved_author(self):
        r = self.repo()
        for kw in ({"author": "stranger", "approvers": ["szibis"]}, {"author": "", "approvers": ["szibis"]}, {}):
            res = r.scope(**kw)
            self.assertTrue(full(res), (kw, res))
        out = r.run(*EVENT, "--title", "chore: release metadata", author="stranger", approvers=["szibis"]).stdout
        self.assertIn("skip_go=false", out)


class EventsAndErrors(unittest.TestCase):
    def setUp(self):
        self.r = Repo({"docs/a.md": "a\n"}, {"docs/a.md": "b\n"})
        self.addCleanup(self.r.close)

    def keys(self, out):
        return dict(ln.split("=", 1) for ln in out.splitlines())

    def test_only_a_pull_request_is_scoped(self):
        self.assertEqual(self.r.scope()["skip_go"], "true")
        for event in ("push", "schedule", "workflow_dispatch", "pull_request_target", "", "PULL_REQUEST", "unknown"):
            with self.subTest(event=event):
                res = self.keys(self.r.run("--ci-scope", "--event", event).stdout)
                self.assertEqual((res["ci_class"], group(res), res["force"]), ("product", ("false", "false", "false"), ""))
                self.assertIn("always a full run", res["ci_reason"])

    def test_no_event_is_a_full_run(self):
        res = self.keys(self.r.run("--ci-scope").stdout)
        self.assertEqual(group(res), ("false", "false", "false"))

    def test_an_unreadable_diff_gives_no_scope_at_all(self):
        r = subprocess.run([sys.executable, SCRIPT, "--base", "0" * 40, "--head", "HEAD", *EVENT], cwd=self.r.d, capture_output=True, text=True)
        self.assertNotEqual(r.returncode, 0)
        self.assertNotIn("skip_go=true", r.stdout)

    def test_an_internal_error_in_the_scope_logic_widens_the_run(self):
        # the scope block is the only code in a try: simulate a failing reader through the module itself
        orig = pc.GitEvidence
        try:
            class Boom:
                def __init__(self, *a):
                    raise RuntimeError("boom")
            pc.GitEvidence = Boom
            import io
            import contextlib
            argv = sys.argv
            sys.argv = ["pr_classify.py", "--base", self.r.base, "--head", self.r.head, *EVENT]
            cwd = os.getcwd()
            os.chdir(self.r.d)
            buf = io.StringIO()
            try:
                with contextlib.redirect_stdout(buf):
                    self.assertEqual(pc.main(), 0)
            finally:
                os.chdir(cwd)
                sys.argv = argv
        finally:
            pc.GitEvidence = orig
        res = self.keys(buf.getvalue())
        self.assertEqual(group(res), ("false", "false", "false"))
        self.assertEqual(res["ci_reason"], "classifier error: RuntimeError")


class RegistryGateKeysAreUnchanged(unittest.TestCase):
    """The flag appends; the five registry-gate keys are printed exactly as without it."""

    CASES = (
        ({"docs/a.md": "a\n"}, {"docs/a.md": "b\n"}),
        ({"internal/a.go": "package a\n"}, {"internal/a.go": "package a // x\n"}),
        ({"Makefile": "VT_VERSION := v1\n"}, {"Makefile": "VT_VERSION := v2\n"}),
        ({"scripts/ci/x.sh": "a\n", "go.mod": "module x\n"}, {"scripts/ci/x.sh": "b\n", "go.mod": "module x\n"}),
        ({"Dockerfile.logs": DockerfileDigests.DF}, {"Dockerfile.logs": DockerfileDigests.DF.replace("1" * 64, "3" * 64)}),
        ({"docs/a\nb.md": "a\n"}, {"docs/a\nb.md": "b\n"}),
        ({"CHANGELOG.md": ReleaseMetadata.CL_BASE, "charts/victoria-lakehouse/Chart.yaml": ReleaseMetadata.CHART},
         {"CHANGELOG.md": ReleaseMetadata.CL_HEAD, "charts/victoria-lakehouse/Chart.yaml": ReleaseMetadata.CHART.replace("0.146.4", "0.146.5")}),
    )

    def test_flag_only_appends(self):
        for base, head in self.CASES:
            r = Repo(base, head)
            self.addCleanup(r.close)
            without = r.run(author="szibis", approvers=["szibis"]).stdout
            for event in ("pull_request", "push", ""):
                with self.subTest(files=sorted(head), event=event):
                    with_flag = r.run("--ci-scope", "--event", event, author="szibis", approvers=["szibis"])
                    self.assertEqual(with_flag.returncode, 0, with_flag.stderr)
                    self.assertTrue(with_flag.stdout.startswith(without))
                    extra = with_flag.stdout[len(without):].splitlines()
                    self.assertEqual([ln.split("=", 1)[0] for ln in extra], list(pc.CI_SCOPE_KEYS))
            self.assertEqual([ln.split("=", 1)[0] for ln in without.splitlines() if "=" in ln][:4], list(LEGACY_KEYS[:4]))

    def test_without_the_flag_nothing_new_is_printed(self):
        r = Repo({"docs/a.md": "a\n"}, {"docs/a.md": "b\n"})
        self.addCleanup(r.close)
        keys = {ln.split("=", 1)[0] for ln in r.run().stdout.splitlines()}
        self.assertLessEqual(keys, set(LEGACY_KEYS))
        # --event alone changes nothing either
        self.assertEqual(r.run("--event", "pull_request").stdout, r.run().stdout)


class RecordedPullRequests(unittest.TestCase):
    """70 merged pull requests (#491 to #686) from their recorded file lists and diff verdicts."""

    # Classes assigned by hand from each pull request's title and files; they are this test's own oracle and do not
    # come from the code under test.
    RELEASE = {676, 675, 655, 654, 626, 620, 610, 592, 589, 586, 552, 549, 510, 492}
    DOCS = {630, 593, 517}
    PRODUCT = {652, 624, 616, 613, 604, 596, 585, 581, 578, 551, 491}
    TESTS_ONLY = {643, 637, 615, 494}
    # tooling PRs that edit Go (the registry gate's own Go), a Go module under scripts/, or the Makefile
    TOOLING_WITH_GO_OR_MAKEFILE = {686, 657, 554, 548, 547, 544, 512, 499}
    # #626 is not the exact release-metadata shape (its CHANGELOG lost a bullet, restored by #630): the registry
    # gate refuses it too (exempt=none), so it is a product change here as well.
    NOT_EXACT_RELEASE = {626}
    # #672 edits scripts/ci/check_changelog_pr.py, a classifier file
    CLASSIFIER_EDIT = {672}

    @classmethod
    def setUpClass(cls):
        with open(FIXTURE, encoding="utf-8") as fh:
            cls.data = json.load(fh)
        cls.cases = {c["pr"]: c for c in cls.data["cases"]}

    def decide(self, case):
        wf = {p: (None if v is None else (tuple(v["jobs"]), v["detail"])) for p, v in case["evidence"]["workflows"].items()}
        ev = FakeEvidence(case["evidence"]["image_pins_only"], wf)
        res = pc.ci_scope_decide(case["files"], ev, "exempt=release-metadata" in case["legacy"])
        return res

    def test_the_fixture_is_the_seventy_pull_requests(self):
        self.assertEqual(len(self.cases), 70)
        self.assertEqual(min(self.cases), 491)
        self.assertEqual(max(self.cases), 686)
        for c in self.cases.values():
            self.assertRegex(c["sha"], r"^[0-9a-f]{40}$")
            self.assertTrue(c["files"], c["pr"])

    def test_the_recorded_expectation_is_reproduced(self):
        for pr, c in self.cases.items():
            with self.subTest(pr=pr):
                res = self.decide(c)
                self.assertEqual({k: res[k] for k in c["expected"]}, c["expected"])

    def test_classes_against_the_hand_labels(self):
        always_full = self.PRODUCT | self.TESTS_ONLY | self.TOOLING_WITH_GO_OR_MAKEFILE | self.NOT_EXACT_RELEASE | self.CLASSIFIER_EDIT
        for pr, c in self.cases.items():
            res = self.decide(c)
            with self.subTest(pr=pr):
                if pr in always_full:
                    self.assertTrue(full(res), res)
                else:
                    self.assertEqual(res["skip_go"], "true", res)
                if pr in self.RELEASE - self.NOT_EXACT_RELEASE:
                    self.assertEqual((res["ci_class"], group(res), res["force"]), ("release-metadata", ("true", "true", "true"), ""))
                if pr in self.DOCS:
                    self.assertEqual(group(res), ("true", "true", "true"))
        self.assertIn("exempt=none", self.cases[626]["legacy"])

    def test_counts(self):
        classes = {}
        for c in self.cases.values():
            r = self.decide(c)
            classes[r["ci_class"]] = classes.get(r["ci_class"], 0) + 1
        self.assertEqual(classes["product"], 25)  # 15 product and tests-only, 8 Go/Makefile tooling, #672, #626
        self.assertEqual(classes["release-metadata"], 13)
        self.assertEqual(sum(classes.values()), 70)

    def test_key_rows(self):
        # #659 pins container images by digest in compose files and test Dockerfiles: no Go, but the parity suite runs
        r = self.decide(self.cases[659])
        self.assertEqual((group(r), r["force"]), (("true", "true", "false"), ""))
        # #658 pins tool versions in eight workflows: the edited jobs run, nothing else of the Go group
        r = self.decide(self.cases[658])
        self.assertEqual(group(r)[0], "true")
        want = {"ci.yaml:helm-config-drift", "ci.yaml:parquet-readback", "ci.yaml:security-logs", "ci.yaml:security-traces",
                "security.yaml:govulncheck", "security.yaml:gosec", "security.yaml:gitleaks", "parity.yaml:parity"}
        self.assertLessEqual(want, set(r["force"].split(",")))
        self.assertNotIn("ci.yaml:test-logs", r["force"])
        self.assertEqual(r["skip_parity"], "false")
        # #656 moves action tags to SHAs in 26 workflows and also edits non-uses lines of the fuzz jobs and the top of codeql.yaml
        r = self.decide(self.cases[656])
        force = set(r["force"].split(","))
        self.assertLessEqual({"fuzz-stress-memleak.yaml:fuzz-logs", "fuzz-stress-memleak.yaml:fuzz-traces",
                              "codeql.yaml:analyze", "codeql.yaml:analyze-actions"}, force)
        self.assertNotIn("ci.yaml:test-logs", force)
        # #588 changed ci.yaml outside any job (a lint version pin): every job of ci.yaml is forced
        r = self.decide(self.cases[588])
        jobs = self.cases[588]["evidence"]["workflows"][".github/workflows/ci.yaml"]["jobs"]
        self.assertLessEqual({"test-logs", "test-traces", "lint-logs", "docker-logs", "docker-traces"}, set(jobs))
        self.assertEqual(set(r["force"].split(",")), {"ci.yaml:" + j for j in jobs})

    def test_invariant_over_all_recorded_pull_requests(self):
        for pr, c in self.cases.items():
            r = self.decide(c)
            with self.subTest(pr=pr):
                if r["skip_go"] == "false":
                    self.assertEqual((r["skip_image"], r["skip_parity"], r["force"]), ("false", "false", ""))
                for job in filter(None, r["force"].split(",")):
                    self.assertRegex(job, r"^[a-z0-9-]+\.yaml:[A-Za-z0-9_-]+$")

    def test_registry_gate_keys_recorded_with_the_legacy_classifier(self):
        for pr, c in self.cases.items():
            with self.subTest(pr=pr):
                self.assertEqual([ln.split("=", 1)[0] for ln in c["legacy"] if not ln.startswith("gate_file=")], ["exempt", "product", "reason", "detail"])


class ThisChange(unittest.TestCase):
    def test_a_change_to_the_classifier_or_its_wiring_runs_everything(self):
        for p in ("scripts/ci/pr_classify.py", "scripts/ci/check_changelog_pr.py", ".github/actions/ci-scope/action.yml"):
            self.assertTrue(full(scope([p, "docs/a.md"])), p)


if __name__ == "__main__":
    unittest.main()
