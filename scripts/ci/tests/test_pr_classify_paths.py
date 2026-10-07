import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import pr_classify as pc  # noqa: E402

GO_MOD = """module x

go 1.22

require (
\tgithub.com/a/b v1.0.0
\tgithub.com/VictoriaMetrics/c v1.0.0
\tgithub.com/parquet-go/parquet-go v0.20.0
\tgithub.com/aws/aws-sdk-go-v2/service/s3 v1.0.0
)
"""


class ProductPaths(unittest.TestCase):
    def reason(self, path):
        return pc.product_reason(path)

    def test_shipped_build_files(self):
        for p in ("Dockerfile", "Dockerfile.logs", "Dockerfile.traces", "deployment/docker/Dockerfile.logs", "go.mod", "go.sum"):
            self.assertIsNotNone(self.reason(p), p)
        for p in ("Dockerfile.loki-vl-proxy", "Dockerfile.grafana", "Dockerfile.datagen", "tests/s3compat/go.mod", "Makefile"):
            self.assertIsNone(self.reason(p), p)

    def test_product_trees_and_their_docs_and_tests(self):
        for p in ("internal/x/a.go", "cmd/lakehouse-logs/main.go", "lakehouse-traces/internal/y/z.go", "internal/ui/static/app.js",
                  "lakehouse-traces/go.mod", "patches/p.patch", "charts/c/templates/NOTES.txt", "charts/c/values.yaml"):
            self.assertIsNotNone(self.reason(p), p)
        for p in ("internal/x/a_test.go", "internal/x/testdata/in.json", "internal/x/README.md", "internal/x/RUNBOOK.md",
                  "internal/x/test_helper.sh", "charts/c/README.md", "charts/c/test_templates.sh", "docs/x.md", "scripts/ci/x.sh",
                  ".github/workflows/x.yaml", "tests/e2e/a_test.go"):
            self.assertIsNone(self.reason(p), p)

    def test_test_prefix_only_for_scripts(self):
        self.assertIsNotNone(self.reason("internal/ui/test_hooks.go"))
        self.assertIsNone(self.reason("internal/x/test_hooks.py"))
        self.assertIsNone(self.reason("internal/x/hooks_test.sh"))


class GoMod(unittest.TestCase):
    def only(self, head):
        return pc.go_mod_dependency_only(GO_MOD, head)

    def test_plain_bump_is_dependency_only(self):
        self.assertTrue(self.only(GO_MOD.replace("github.com/a/b v1.0.0", "github.com/a/b v1.1.0")))
        self.assertTrue(self.only(GO_MOD.replace("\n)\n", "\n\tgithub.com/d/e v1.0.0\n)\n")))

    def test_single_line_require(self):
        base = "module x\n\ngo 1.22\n\nrequire github.com/a/b v1.0.0\n"
        self.assertTrue(pc.go_mod_dependency_only(base, base.replace("v1.0.0", "v1.2.0")))
        crit = "module x\n\ngo 1.22\n\nrequire github.com/VictoriaMetrics/c v1.0.0\n"
        self.assertFalse(pc.go_mod_dependency_only(crit, crit.replace("v1.0.0", "v1.2.0")))
        self.assertFalse(pc.go_mod_dependency_only(base, base.replace("go 1.22", "go 1.23")))

    def test_other_directives_are_never_dependency_only(self):
        for edit in (GO_MOD.replace("go 1.22", "go 1.23"), GO_MOD + "\nreplace github.com/a/b => github.com/evil/b v0.0.1\n",
                     GO_MOD + "\ntoolchain go1.99\n", GO_MOD.replace("module x", "module y")):
            self.assertFalse(self.only(edit))

    def test_storage_critical_modules_need_coverage(self):
        for mod in ("github.com/VictoriaMetrics/c", "github.com/parquet-go/parquet-go", "github.com/aws/aws-sdk-go-v2/service/s3"):
            base = GO_MOD
            old = [l for l in GO_MOD.splitlines() if mod in l][0]
            self.assertFalse(self.only(base.replace(old, old.rsplit(" ", 1)[0] + " v9.9.9")), mod)

    def test_quoted_module_path_is_still_the_module(self):
        head = GO_MOD.replace("github.com/VictoriaMetrics/c v1.0.0", '"github.com/VictoriaMetrics/c" v1.5.0')
        self.assertFalse(self.only(head))


class DependencyOnly(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name
        self.cwd = os.getcwd()
        os.chdir(self.d)
        self.git("init", "-q", "-b", "main")
        self.write({"go.mod": GO_MOD, "go.sum": "a\n", "requirements.txt": "x==1\n", "Makefile": "all:\n"})
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "base")
        self.base = self.git("rev-parse", "HEAD").strip()

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def git(self, *a):
        return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@e", "-c", "commit.gpgsign=false", *a],
                              cwd=self.d, check=True, capture_output=True, text=True).stdout

    def write(self, files):
        for p, t in files.items():
            with open(os.path.join(self.d, p), "w", encoding="utf-8") as fh:
                fh.write(t)

    def verdict(self, files, commits):
        self.write(files)
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", "change")
        return pc.dependency_only(list(files), commits, self.base, "HEAD")

    BUMP = {"go.mod": GO_MOD.replace("github.com/a/b v1.0.0", "github.com/a/b v1.1.0"), "go.sum": "a\nb\n"}

    def test_bump_with_dependency_commits(self):
        self.assertTrue(self.verdict(self.BUMP, ["build(deps): bump a/b"]))

    def test_no_commits_is_not_dependency_only(self):
        self.assertFalse(self.verdict(self.BUMP, []))
        self.assertFalse(pc.dependency_only(["go.mod"], ["", "  "], self.base, "HEAD"))

    def test_merge_commits_alone_are_not_enough(self):
        self.assertFalse(self.verdict(self.BUMP, ["Merge branch 'x'"]))

    def test_other_subjects_are_not_dependency_only(self):
        self.assertFalse(self.verdict(self.BUMP, ["build(deps): bump", "feat: x"]))
        self.assertFalse(self.verdict(self.BUMP, ["fix: bump"]))

    def test_makefile_is_not_a_dependency_manifest(self):
        self.assertFalse(pc.is_dependency_manifest("Makefile"))
        self.assertFalse(self.verdict({"Makefile": "all:\n\techo\n"}, ["build(deps): bump"]))
        self.assertTrue(pc.is_dependency_manifest("tests/requirements-dev.txt"))
        self.assertFalse(pc.is_dependency_manifest("requirements.md"))

    def test_critical_bump_is_not_dependency_only(self):
        head = GO_MOD.replace("github.com/parquet-go/parquet-go v0.20.0", "github.com/parquet-go/parquet-go v0.21.0")
        self.assertFalse(self.verdict({"go.mod": head, "go.sum": "a\nz\n"}, ["build(deps): bump parquet-go"]))


if __name__ == "__main__":
    unittest.main()
