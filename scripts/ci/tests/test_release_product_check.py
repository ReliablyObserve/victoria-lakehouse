"""Tests for scripts/ci/release_product_check.py: a merge that changes nothing that ships is not released.

The table (testdata/release_product_check_cases.json) holds the file lists of real merged
squash commits, with the content of the files the decision reads, and the owner's expected
decision. Every row is replayed in a scratch git repository (parent commit, then the commit)
through the real decision function and the real registry-gate classifier (pr_classify.py), so
the table proves the whole chain. The `[skip release]` marker is out of the picture: it is
handled per commit by decide_range() (RangeDecision below) and the titles in the table do not carry it.
"""
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import release_product_check as r  # noqa: E402

CLASSIFIER = os.path.join(HERE, "..", "pr_classify.py")
with open(os.path.join(HERE, "testdata", "release_product_check_cases.json"), encoding="utf-8") as fh:
    TABLE = json.load(fh)
BLOBS = TABLE["blobs"]
CASES = {c["id"]: c for c in TABLE["cases"]}

# The rule the workflow had on main before this check (the path regex of the "Check for skip
# conditions" step), kept to show what main does with each row.
MAIN_RULE = re.compile(r"^(docs/|\.github/|README\.md|CHANGELOG\.md|CONTRIBUTING\.md|SECURITY\.md|CODE_OF_CONDUCT\.md|LICENSE|website/|scripts/ci/|charts/victoria-lakehouse/Chart\.yaml|tests/verification/)")

D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64
# well-formed go.sum lines of an inert module: the gate's classifier calls such a change dependency-only
SUM_OLD = "golang.org/x/net v0.1.0 h1:abc=\n"
SUM_NEW = SUM_OLD + "golang.org/x/net v9.9.9 h1:def=\n"


def git(cwd, *args, check=True):
    p = subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@e", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", *args],
                       cwd=cwd, capture_output=True, text=True)
    if check and p.returncode:
        raise AssertionError(p.stderr)
    return p.stdout.strip()


class Repo:
    """A scratch repository: commit `base` files, then `head` changes on top."""

    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name
        git(self.d, "init", "-q", "-b", "main")

    def close(self):
        self.tmp.cleanup()

    def write(self, path, text):
        full = os.path.join(self.d, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)

    def commit(self, msg, changes, allow_empty=False):
        """changes: {path: text | None (delete)}"""
        for p, t in changes.items():
            if t is None:
                os.remove(os.path.join(self.d, p))
            else:
                self.write(p, t)
        git(self.d, "add", "-A")
        git(self.d, "commit", "-q", "-m", msg, *(["--allow-empty"] if allow_empty else []))
        return git(self.d, "rev-parse", "HEAD")

    def decide(self, sha, **kw):
        kw.setdefault("classifier", CLASSIFIER)
        return r.decide(self.d, sha, **kw)


def replay(base_files, head_files):
    """A repo with a parent holding base_files and a commit applying head_files (None deletes)."""
    repo = Repo()
    parent = repo.commit("base", base_files)
    sha = repo.commit("head", head_files)
    return repo, parent, sha


def case_changes(case):
    base, head = {}, {}
    for f in case["files"]:
        st, p = f["status"], f["path"]
        if "base" in f:
            b = BLOBS[f["base"]] if f["base"] else None
            h = BLOBS[f["head"]] if f["head"] else None
        else:
            b = None if st == "A" else "x\n"
            h = None if st == "D" else "y\n"
        if b is not None:
            base[p] = b
        head[p] = h
    return base, head


class FixtureTable(unittest.TestCase):
    def test_every_row_gets_the_owner_decision(self):
        for cid, case in CASES.items():
            with self.subTest(cid, why=case["why"]):
                base, head = case_changes(case)
                repo, parent, sha = replay({**base, "README.md": "r\n"}, head)
                try:
                    v = repo.decide(sha)
                finally:
                    repo.close()
                self.assertEqual("skip" if v.skip else "release", case["expect"], v.reason)

    def test_the_table_covers_both_kinds_and_the_required_rows(self):
        self.assertEqual({"pr-652": "release", "pr-693": "release", "pr-687": "skip", "pr-613": "release",
                          "pr-686": "skip", "pr-656": "skip", "pr-659": "skip", "pr-688": "skip"},
                         {k: CASES[k]["expect"] for k in ("pr-652", "pr-693", "pr-687", "pr-613", "pr-686", "pr-656", "pr-659", "pr-688")})
        self.assertGreaterEqual(sum(c["expect"] == "skip" for c in CASES.values()), 15)
        self.assertGreaterEqual(sum(c["expect"] == "release" for c in CASES.values()), 4)

    def test_main_released_the_rows_it_did_not_skip(self):
        # Failing-first evidence: with the marker out of the picture, main's path regex releases
        # every row below although the owner's decision is skip (tooling, test stacks, locks, digest pins).
        main_releases = []
        for cid, case in CASES.items():
            files = [f["path"] for f in case["files"]]
            if case["expect"] == "skip" and not all(MAIN_RULE.match(p) for p in files):
                main_releases.append(cid)
        for cid in ("pr-687", "pr-659", "pr-653", "pr-666", "pr-668", "pr-669", "pr-678", "pr-661", "pr-674", "pr-663", "pr-686", "pr-658", "pr-688"):
            self.assertIn(cid, main_releases)


class DockerfileDigests(unittest.TestCase):
    """#687 adds digests where none existed (skip); a digest that changes or goes away rebuilds the image."""

    LOGS_BASE = BLOBS[next(f for f in CASES["pr-687"]["files"] if f["path"] == "Dockerfile.logs")["base"]]
    LOGS_PINNED = BLOBS[next(f for f in CASES["pr-687"]["files"] if f["path"] == "Dockerfile.logs")["head"]]
    DATAGEN_PINNED = BLOBS[next(f for f in CASES["pr-687"]["files"] if f["path"] == "Dockerfile.datagen")["head"]]

    def decide(self, base, head, path="Dockerfile.logs"):
        repo, _, sha = replay({path: base}, {path: head})
        try:
            return repo.decide(sha)
        finally:
            repo.close()

    def digests(self, text):
        return re.findall(r"sha256:[0-9a-f]{64}", text)

    def test_the_real_687_shape_is_a_pure_addition(self):
        self.assertEqual(len(self.digests(self.LOGS_BASE)), 0)
        self.assertGreaterEqual(len(self.digests(self.LOGS_PINNED)), 2)
        self.assertTrue(self.decide(self.LOGS_BASE, self.LOGS_PINNED).skip)

    def test_a_changed_digest_releases(self):
        first = self.digests(self.LOGS_PINNED)[0]
        v = self.decide(self.LOGS_PINNED, self.LOGS_PINNED.replace(first, D2))
        self.assertFalse(v.skip, v.reason)
        self.assertIn("Dockerfile.logs", v.reason)

    def test_a_changed_digest_in_every_release_dockerfile_releases(self):
        for path in ("Dockerfile", "Dockerfile.logs", "Dockerfile.traces"):
            with self.subTest(path):
                self.assertFalse(self.decide(self.LOGS_PINNED, self.LOGS_PINNED.replace(self.digests(self.LOGS_PINNED)[1], D1), path).skip)

    def test_a_removed_digest_releases(self):
        self.assertFalse(self.decide(self.LOGS_PINNED, self.LOGS_BASE).skip)

    def test_a_tag_moving_together_with_a_digest_releases(self):
        head = re.sub(r"(golang:[0-9.]+)(-alpine3\.23)@sha256:[0-9a-f]{64}", r"\g<1>1\2@" + D2, self.LOGS_PINNED, count=1)
        self.assertNotEqual(head, self.LOGS_PINNED)
        self.assertFalse(self.decide(self.LOGS_BASE, head).skip)

    def test_a_digest_added_next_to_another_edit_releases(self):
        self.assertFalse(self.decide(self.LOGS_BASE, self.LOGS_PINNED + "RUN true\n").skip)

    def test_a_digest_added_to_a_different_image_name_releases(self):
        head = self.LOGS_PINNED.replace("gcr.io/distroless/static-debian12:nonroot", "gcr.io/distroless/base-debian12:nonroot", 1)
        self.assertFalse(self.decide(self.LOGS_BASE, head).skip)

    def test_a_digest_on_an_untagged_image_releases(self):
        self.assertFalse(self.decide("FROM scratch\n", "FROM scratch@" + D1 + "\n").skip)

    def test_a_dockerfile_that_is_not_valid_text_releases(self):
        repo = Repo()
        try:
            repo.commit("base", {"Dockerfile.logs": self.LOGS_BASE})
            with open(os.path.join(repo.d, "Dockerfile.logs"), "wb") as fh:
                fh.write(self.LOGS_PINNED.encode() + b"\xff\xfe\n")
            git(repo.d, "add", "-A")
            git(repo.d, "commit", "-q", "-m", "x")
            self.assertFalse(repo.decide(git(repo.d, "rev-parse", "HEAD")).skip)
        finally:
            repo.close()

    def test_a_deleted_release_dockerfile_releases(self):
        repo, _, sha = replay({"Dockerfile.logs": self.LOGS_BASE}, {"Dockerfile.logs": None})
        try:
            self.assertFalse(repo.decide(sha).skip)
        finally:
            repo.close()

    def test_the_test_tool_dockerfile_is_not_released(self):
        # Dockerfile.datagen is not built by auto-release: a digest change there is a test-stack change
        old = self.digests(self.DATAGEN_PINNED)[0]
        v = self.decide(self.DATAGEN_PINNED, self.DATAGEN_PINNED.replace(old, D2), "Dockerfile.datagen")
        self.assertTrue(v.skip, v.reason)

    def test_digest_added_only_unit_table(self):
        pinned = "FROM --platform=$BUILDPLATFORM golang:1.27-alpine3.23@" + D1 + " AS b\nRUN x\n"
        plain = pinned.replace("@" + D1, "")
        self.assertTrue(r.digest_added_only(plain, pinned))
        self.assertFalse(r.digest_added_only(pinned, plain))
        self.assertFalse(r.digest_added_only(pinned, pinned.replace(D1, D2)))
        self.assertFalse(r.digest_added_only(plain, plain))
        self.assertFalse(r.digest_added_only(plain, pinned + "\n"))
        self.assertFalse(r.digest_added_only(plain, pinned.replace("sha256:" + "1" * 64, "sha256:" + "1" * 63)))
        self.assertFalse(r.digest_added_only(plain + "RUN cat <<EOF\nx\nEOF\n", pinned + "RUN cat <<EOF\nx\nEOF\n"))
        self.assertFalse(r.digest_added_only("# escape=`\n" + plain, "# escape=`\n" + pinned))
        self.assertFalse(r.digest_added_only(plain.replace(" AS b", " AS c"), pinned))


class GoModulesAndPins(unittest.TestCase):
    GOMOD = BLOBS[next(f for f in CASES["pr-652"]["files"] if f["path"] == "go.mod")["head"]]
    TRACES_MOD = BLOBS[next(f for f in CASES["pr-652"]["files"] if f["path"] == "lakehouse-traces/go.mod")["head"]]

    def one(self, files_base, files_head):
        repo, _, sha = replay(files_base, files_head)
        try:
            return repo.decide(sha)
        finally:
            repo.close()

    def bumped(self, text):
        m = re.search(r"^\t(golang\.org/x/net) (v[0-9.]+)", text, re.M)
        self.assertTrue(m, "fixture has golang.org/x/net")
        return text.replace(m.group(0), f"\t{m.group(1)} v9.9.9")

    def test_a_dependabot_bump_of_the_root_module_releases(self):
        v = self.one({"go.mod": self.GOMOD, "go.sum": SUM_OLD}, {"go.mod": self.bumped(self.GOMOD), "go.sum": SUM_NEW})
        self.assertFalse(v.skip, v.reason)

    def test_the_classifier_alone_calls_that_bump_dependency_only(self):
        # why the file rule exists: the gate waives a dependency bump, a release must not
        repo, parent, sha = replay({"go.mod": self.GOMOD, "go.sum": SUM_OLD}, {"go.mod": self.bumped(self.GOMOD), "go.sum": SUM_NEW})
        try:
            kv = r.run_classifier(repo.d, parent, sha, CLASSIFIER)
        finally:
            repo.close()
        self.assertEqual((kv["product"], kv["exempt"]), ("0", "dependency-only"))
        # and go.sum of the traces module alone, the same way
        repo, parent, sha = replay({"lakehouse-traces/go.sum": SUM_OLD}, {"lakehouse-traces/go.sum": SUM_NEW})
        try:
            kv = r.run_classifier(repo.d, parent, sha, CLASSIFIER)
        finally:
            repo.close()
        self.assertEqual((kv["product"], kv["exempt"]), ("0", "dependency-only"))

    def test_go_sum_of_the_traces_module_alone_releases(self):
        self.assertFalse(self.one({"lakehouse-traces/go.sum": SUM_OLD}, {"lakehouse-traces/go.sum": SUM_NEW}).skip)

    def test_go_work_and_the_traces_go_mod_release(self):
        for p, a, b in (("go.work", "go 1.27.2\n", "go 1.27.3\n"), ("lakehouse-traces/go.mod", self.TRACES_MOD, self.bumped(self.TRACES_MOD)),
                        ("go.work.sum", "a\n", "b\n")):
            with self.subTest(p):
                self.assertFalse(self.one({p: a}, {p: b}).skip)

    def test_a_go_mod_of_another_module_does_not_release(self):
        # tooling modules (the bench programs, the conformance tooling) are not shipped
        self.assertTrue(self.one({"scripts/bench/x/go.mod": self.GOMOD}, {"scripts/bench/x/go.mod": self.bumped(self.GOMOD)}).skip)

    def test_tooling_locks_do_not_release(self):
        for p in ("scripts/proof/requirements.txt", "scripts/ci/requirements.txt", "tests/readers/requirements.txt", "website/package-lock.json"):
            with self.subTest(p):
                v = self.one({p: "pytest==1.0.0\n"}, {p: "pytest==2.0.0\n"})
                self.assertTrue(v.skip, v.reason)

    def test_makefile_pins_release_other_makefile_edits_do_not(self):
        mk = "VL_VERSION_LOGS := v1.53.0\nVL_COMMIT_TRACES := c945d2949e98\nVT_VERSION := v0.12.0\n\nbuild:\n\techo\n"
        self.assertFalse(self.one({"Makefile": mk}, {"Makefile": mk.replace("v0.12.0", "v0.13.0")}).skip)
        self.assertTrue(self.one({"Makefile": mk}, {"Makefile": mk.replace("echo", "echo hi")}).skip)

    def test_the_release_pins_in_auto_release_yaml_release(self):
        wf = "env:\n  GOWORK: \"off\"\n  VL_VERSION_LOGS: \"v1.53.0\"\n  VL_COMMIT_TRACES: \"c945d2949e98\"\n  VT_VERSION: \"v0.12.0\"\njobs: {}\n"
        for old, new in (("v1.53.0", "v1.54.0"), ("c945d2949e98", "d945d2949e98"), ("v0.12.0", "v0.13.0")):
            with self.subTest(old):
                v = self.one({".github/workflows/auto-release.yaml": wf}, {".github/workflows/auto-release.yaml": wf.replace(old, new)})
                self.assertFalse(v.skip, v.reason)
        v = self.one({".github/workflows/auto-release.yaml": wf}, {".github/workflows/auto-release.yaml": wf + "# job split\n"})
        self.assertTrue(v.skip, v.reason)

    def test_pins_changed_unit(self):
        a = '  VT_VERSION: "v0.12.0"\n  OTHER: 1\n'
        self.assertFalse(r.pins_changed(a, a.replace("OTHER: 1", "OTHER: 2")))
        self.assertTrue(r.pins_changed(a, a.replace("v0.12.0", "v0.13.0")))
        self.assertTrue(r.pins_changed(a, "  OTHER: 1\n"))


class ProductPaths(unittest.TestCase):
    def one(self, base, head):
        repo, _, sha = replay(base, head)
        try:
            return repo.decide(sha)
        finally:
            repo.close()

    def test_product_trees_release(self):
        for p in ("internal/x/a.go", "cmd/lakehouse-logs/main.go", "lakehouse-traces/main.go", "patches/vl-logs/a.src",
                  "charts/victoria-lakehouse/templates/d.yaml", "charts/victoria-lakehouse/values.yaml", "internal/ui/vmui/a.js"):
            with self.subTest(p):
                self.assertFalse(self.one({p: "a\n"}, {p: "b\n"}).skip)

    def test_tests_docs_ci_and_scripts_do_not_release(self):
        for p in ("internal/x/a_test.go", "internal/x/testdata/a.json", "docs/a.md", "README.md", "scripts/bench/run.sh", "scripts/ci/gate.sh",
                  ".github/workflows/ci.yaml", "tests/e2e/a.go", "tests/conformance/registry/features/a.yaml", "deployment/docker/docker-compose-e2e.yml",
                  "deployment/docker/Dockerfile.loki", "Dockerfile.datagen", "website/src/a.ts", "CHANGELOG.md", "dashboards/a.json"):
            with self.subTest(p):
                v = self.one({p: "a\n"}, {p: "b\n"})
                self.assertTrue(v.skip, f"{p}: {v.reason}")

    def test_one_product_file_among_tooling_files_releases(self):
        v = self.one({"docs/a.md": "a\n", "internal/x/a.go": "a\n", "scripts/ci/a.py": "a\n"},
                     {"docs/a.md": "b\n", "internal/x/a.go": "b\n", "scripts/ci/a.py": "b\n"})
        self.assertFalse(v.skip)
        self.assertIn("internal/x/a.go", v.reason)

    def test_a_dockerfile_logic_change_releases(self):
        base = "FROM golang:1.27-alpine3.23 AS b\nRUN go build\n"
        self.assertFalse(self.one({"Dockerfile.traces": base}, {"Dockerfile.traces": base.replace("go build", "go build -tags x")}).skip)

    def test_a_golang_image_bump_releases_even_though_the_gate_calls_it_toolchain_only(self):
        base = "FROM golang:1.27.2-alpine3.23 AS b\nRUN go build\n"
        repo, parent, sha = replay({"Dockerfile.logs": base}, {"Dockerfile.logs": base.replace("1.27.2", "1.27.3")})
        try:
            self.assertEqual(r.run_classifier(repo.d, parent, sha, CLASSIFIER)["exempt"], "toolchain-only")
            self.assertFalse(repo.decide(sha).skip)
        finally:
            repo.close()

    def test_a_test_stack_golang_bump_does_not_release(self):
        base = "services:\n  x:\n    image: golang:1.27.2-alpine3.23\n"
        repo, parent, sha = replay({"tests/parity/docker-compose.yml": base}, {"tests/parity/docker-compose.yml": base.replace("1.27.2", "1.27.3")})
        try:
            self.assertEqual(r.run_classifier(repo.d, parent, sha, CLASSIFIER)["exempt"], "toolchain-only")
            self.assertTrue(repo.decide(sha).skip)
        finally:
            repo.close()

    def test_chart_yaml_alone_keeps_the_old_skip(self):
        self.assertTrue(self.one({"charts/victoria-lakehouse/Chart.yaml": "version: 1.0.0\n"},
                                 {"charts/victoria-lakehouse/Chart.yaml": "version: 1.0.1\n"}).skip)

    def test_rename_in_either_direction_is_seen_as_delete_plus_add(self):
        for src, dst in (("internal/x/a.go", "docs/a.md"), ("docs/a.md", "internal/x/a.go")):
            with self.subTest(src=src, dst=dst):
                repo = Repo()
                try:
                    repo.commit("base", {src: "content one\ncontent two\ncontent three\n"})
                    os.makedirs(os.path.dirname(os.path.join(repo.d, dst)), exist_ok=True)
                    git(repo.d, "mv", src, dst)
                    git(repo.d, "commit", "-q", "-m", "rename")
                    self.assertFalse(repo.decide(git(repo.d, "rev-parse", "HEAD")).skip)
                finally:
                    repo.close()

    def test_a_rename_between_tooling_paths_does_not_release(self):
        repo = Repo()
        try:
            repo.commit("base", {"scripts/bench/a.sh": "echo one\necho two\necho three\n"})
            git(repo.d, "mv", "scripts/bench/a.sh", "scripts/bench/b.sh")
            git(repo.d, "commit", "-q", "-m", "rename")
            self.assertTrue(repo.decide(git(repo.d, "rev-parse", "HEAD")).skip)
        finally:
            repo.close()

    def test_a_mode_change_follows_the_path(self):
        for path, want_skip in (("internal/x/run.sh", False), ("scripts/bench/run.sh", True)):
            with self.subTest(path):
                repo = Repo()
                try:
                    repo.commit("base", {path: "#!/bin/sh\n"})
                    os.chmod(os.path.join(repo.d, path), 0o755)
                    git(repo.d, "add", "-A")
                    git(repo.d, "commit", "-q", "-m", "chmod")
                    v = repo.decide(git(repo.d, "rev-parse", "HEAD"))
                    self.assertEqual(v.skip, want_skip, v.reason)
                finally:
                    repo.close()


class FailSafe(unittest.TestCase):
    """Every doubt releases."""

    def setUp(self):
        self.repo = Repo()
        self.addCleanup(self.repo.close)
        self.parent = self.repo.commit("base", {"docs/a.md": "a\n", "scripts/bench/a.sh": "a\n"})
        self.sha = self.repo.commit("tooling only", {"scripts/bench/a.sh": "b\n"})

    def decide(self, **kw):
        return self.repo.decide(self.sha, **kw)

    def test_the_baseline_skips(self):
        self.assertTrue(self.decide().skip)

    def test_a_merge_commit_releases(self):
        git(self.repo.d, "checkout", "-q", "-b", "side", self.parent)
        side = self.repo.commit("side", {"internal/x/a.go": "a\n"})
        git(self.repo.d, "checkout", "-q", "main")
        git(self.repo.d, "merge", "-q", "--no-ff", "-m", "merge", "side")
        merge = git(self.repo.d, "rev-parse", "HEAD")
        self.assertEqual(len(git(self.repo.d, "rev-list", "--parents", "-n1", merge).split()) - 1, 2)
        v = self.repo.decide(merge, classifier=CLASSIFIER)
        self.assertFalse(v.skip)
        self.assertIn("2 parents", v.reason)
        self.assertNotEqual(side, merge)

    def test_the_root_commit_releases(self):
        root = Repo()
        self.addCleanup(root.close)
        sha = root.commit("root", {"docs/a.md": "a\n"})
        self.assertFalse(root.decide(sha).skip)

    def test_an_empty_diff_releases(self):
        empty = self.repo.commit("empty", {}, allow_empty=True)
        v = self.repo.decide(empty, classifier=CLASSIFIER)
        self.assertFalse(v.skip)
        self.assertIn("empty", v.reason)

    def test_a_bad_sha_releases(self):
        for sha in ("", "HEAD", "z" * 40, "a" * 40):
            with self.subTest(sha):
                self.assertFalse(self.repo.decide(sha).skip)

    def test_not_a_repository_releases(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertFalse(r.decide(d, self.sha).skip)

    def _classifier_script(self, body):
        path = os.path.join(self.repo.d, "..", f"fake_classifier_{abs(hash(body))}.py")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(body)
        self.addCleanup(os.remove, path)
        return path

    def test_a_failing_classifier_releases(self):
        # the tooling-only commit above would skip via the classifier; use a path the legacy rule does not cover
        sha = self.repo.commit("bench", {"scripts/bench/c.sh": "c\n"})
        for body in ("import sys\nsys.exit(1)\n",                                 # exit 1
                     "print('product=maybe')\n",                                   # unexpected value
                     "print('exempt=none')\n",                                     # no product line
                     "",                                                           # no output
                     "raise RuntimeError('boom')\n",                                # traceback
                     "import time\ntime.sleep(5)\n"):                              # timeout (patched below)
            with self.subTest(body[:30]):
                script = self._classifier_script(body)
                old = r.CLASSIFY_TIMEOUT
                r.CLASSIFY_TIMEOUT = 1
                try:
                    v = self.repo.decide(sha, classifier=script)
                finally:
                    r.CLASSIFY_TIMEOUT = old
                self.assertFalse(v.skip, v.reason)
                self.assertIn("cannot decide", v.reason)

    def test_a_classifier_that_says_product_releases_and_one_that_says_not_skips(self):
        sha = self.repo.commit("bench", {"scripts/bench/d.sh": "d\n"})
        self.assertFalse(self.repo.decide(sha, classifier=self._classifier_script("print('product=1')\nprint('reason=product code: x')\n")).skip)
        self.assertTrue(self.repo.decide(sha, classifier=self._classifier_script("print('product=0')\n")).skip)

    def test_the_parents_classifier_decides_not_the_commits_own(self):
        # the parent's pr_classify says product=1; the commit rewrites it to say product=0:
        # the change cannot vote on its own merge
        repo = Repo()
        self.addCleanup(repo.close)
        repo.commit("base", {"scripts/ci/pr_classify.py": "print('product=1')\nprint('reason=parent')\n", "scripts/bench/a.sh": "a\n"})
        sha = repo.commit("change", {"scripts/ci/pr_classify.py": "print('product=0')\n", "scripts/bench/a.sh": "b\n"})
        v = r.decide(repo.d, sha)
        self.assertFalse(v.skip)
        self.assertIn("parent", v.reason)

    def test_a_parent_without_the_classifier_releases(self):
        repo = Repo()
        self.addCleanup(repo.close)
        repo.commit("base", {"scripts/bench/a.sh": "a\n"})
        sha = repo.commit("change", {"scripts/bench/a.sh": "b\n"})
        v = r.decide(repo.d, sha)
        self.assertFalse(v.skip)
        self.assertIn("cannot decide", v.reason)

    def test_the_real_classifier_through_the_parents_copy(self):
        repo = Repo()
        self.addCleanup(repo.close)
        files = {}
        for name in ("pr_classify.py", "check_changelog_pr.py"):
            with open(os.path.join(HERE, "..", name), encoding="utf-8") as fh:
                files["scripts/ci/" + name] = fh.read()
        repo.commit("base", {**files, "scripts/bench/a.sh": "a\n", "internal/x/a.go": "a\n"})
        self.assertTrue(r.decide(repo.d, repo.commit("tooling", {"scripts/bench/a.sh": "b\n"})).skip)
        self.assertFalse(r.decide(repo.d, repo.commit("product", {"internal/x/a.go": "b\n"})).skip)

    def test_an_exception_inside_the_decision_never_escapes(self):
        def boom(*a):
            raise MemoryError("x")
        sha = self.repo.commit("bench", {"scripts/bench/e.sh": "e\n"})
        v = self.repo.decide(sha, classify=boom)
        self.assertFalse(v.skip)


class RangeDecision(unittest.TestCase):
    """#756: the decision is made on last-tag..HEAD, never on the pushed commit alone.

    Histories are built in scratch repositories with the real subjects and file lists of the 2026-10-23
    burst (#753 metadata, #736 product, #729 test-only, #743 CI-only) and the real registry-gate classifier
    in the base commit.
    """

    MAIN = "refs/heads/main"
    PRODUCT = {"internal/storage/field_names.go": "package storage\n", "internal/storage/field_names_test.go": "package storage\n",
               "CHANGELOG.md": "c\n", "docs/features.md": "f\n", "tests/conformance/registry/features/query.yaml": "q\n"}
    METADATA = {"CHANGELOG.md": "meta\n", "charts/victoria-lakehouse/Chart.yaml": "v\n"}
    TESTONLY = {"internal/storage/parquets3/acked_rows_pre1970_test.go": "package p\n",
                "lakehouse-traces/internal/storage/parquets3/acked_rows_pre1970_test.go": "package p\n"}
    CIONLY = {".github/workflows/ci.yaml": "ci\n", "scripts/ci/tests/test_workflow_concurrency.py": "t\n", "docs/security-posture.md": "s\n"}

    def setUp(self):
        self.repo = Repo()
        self.addCleanup(self.repo.close)
        files = {}
        for name in ("pr_classify.py", "check_changelog_pr.py"):
            with open(os.path.join(HERE, "..", name), encoding="utf-8") as fh:
                files["scripts/ci/" + name] = fh.read()
        self.base = self.repo.commit("base", {**files, "internal/storage/field_names.go": "package storage\n", "CHANGELOG.md": "c0\n"})
        self.n = 0
        self.title_calls = []
        self.titles = {}

    def step(self, subject, changes):
        """A commit with changes made distinct (so every commit really changes its files)."""
        self.n += 1
        return self.repo.commit(subject, {p: f"{t}{self.n}\n" for p, t in changes.items()})

    def tag(self, name, sha):
        git(self.repo.d, "tag", name, sha)

    def titles_for(self, sha):
        def fetch(number):
            self.title_calls.append(sha)
            return list(self.titles.get(sha, []))
        return fetch

    def decide(self, head, tag="v0.167.5", **kw):
        kw.setdefault("classifier", CLASSIFIER)
        kw.setdefault("titles_for", self.titles_for)
        return r.decide_range(self.repo.d, head, tag, **kw)

    def burst(self, with_product=True):
        """tag on the metadata commit's parent, then metadata, product, test-only, CI-only (all with the real subjects)."""
        released = self.step("fix(security): constant-time key compare and a body cap in the cross-signal hint handler (#732)",
                             {"internal/storage/field_names.go": "package storage\n"})
        self.tag("v0.167.5", released)
        self.step("chore: release metadata for v0.167.4 [skip release] (#753)", self.METADATA)
        product = None
        if with_product:
            product = self.step("fix(field_names): keep no block memory after the read callback returns (#734) (#736)", self.PRODUCT)
            self.titles[product] = ["fix(field_names): keep no block memory after the read callback returns (#734)"]
        self.step("test: poll for buffer visibility in the pre-1970 property test and fail loudly [skip release] (#729)", self.TESTONLY)
        head = self.step("ci: cancel superseded and conflicting pull request runs [skip release] (#743)", self.CIONLY)
        return head, product

    # the exact #736 sequence
    def test_the_lost_product_merge_behind_two_skip_release_merges_is_released(self):
        head, product = self.burst()
        v = self.decide(head)
        self.assertFalse(v.skip, v.reason)
        self.assertIn(product[:12], v.reason)

    def test_the_same_burst_without_the_product_commit_is_not_released(self):
        head, _ = self.burst(with_product=False)
        v = self.decide(head)
        self.assertTrue(v.skip, v.reason)

    def test_every_run_of_the_burst_answers_for_the_range_not_its_own_commit(self):
        head, product = self.burst()
        runs = git(self.repo.d, "rev-list", "--reverse", f"{product}..{head}").split()
        # the run of the product merge, and of each later merge whose own commit is skip-release: all owe the release
        for sha in [product, *runs]:
            with self.subTest(sha=sha[:8]):
                self.assertFalse(self.decide(sha).skip)

    def test_the_head_marker_alone_does_not_decide(self):
        head, _ = self.burst()
        self.assertIn("[skip release]", git(self.repo.d, "log", "-1", "--format=%s", head))
        self.assertFalse(self.decide(head).skip)

    # the first run after a tag, the metadata commit, the re-run
    def test_the_commit_a_tag_points_at_owes_nothing(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        v = self.decide(released, "v0.1.0")
        self.assertTrue(v.skip, v.reason)

    def test_the_release_metadata_commit_right_after_a_release_does_not_release(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        meta = self.step("chore: release metadata for v0.1.0 [skip release] (#2)", self.METADATA)
        v = self.decide(meta, "v0.1.0")
        self.assertTrue(v.skip, v.reason)
        self.assertEqual(self.title_calls, [])  # no lookup: the subject carries the marker

    def test_the_metadata_commit_is_released_over_when_a_product_commit_follows_it(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        self.step("chore: release metadata for v0.1.0 [skip release] (#2)", self.METADATA)
        nxt = self.step("fix: y (#3)", self.PRODUCT)
        self.assertFalse(self.decide(nxt, "v0.1.0").skip)

    def test_a_rerun_after_the_release_is_idempotent(self):
        head, product = self.burst()
        self.assertFalse(self.decide(head).skip)
        self.tag("v0.167.6", head)  # what the release run does
        for sha in (head, product):  # a manual re-run of the finished run, or of an older one
            with self.subTest(sha=sha[:8]):
                v = self.decide(sha, "v0.167.6")
                self.assertTrue(v.skip, v.reason)
        later = self.step("docs: y [skip release] (#9)", {"docs/a.md": "a"})
        self.assertTrue(self.decide(later, "v0.167.6").skip)

    # several commits, a revert, a merge commit
    def test_the_oldest_of_several_product_commits_is_enough(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        self.step("docs: a (#2)", {"docs/a.md": "a"})
        self.step("feat: b (#3)", {"internal/storage/b.go": "package storage\n"})
        self.step("test: c [skip release] (#4)", self.TESTONLY)
        head = self.step("docs: d (#5)", {"docs/a.md": "d"})
        self.assertFalse(self.decide(head, "v0.1.0").skip)

    def test_only_non_product_commits_in_the_range_do_not_release(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        self.step("docs: a (#2)", {"docs/a.md": "a"})
        self.step("ci: b (#3)", self.CIONLY)
        self.step("bench: c (#4)", {"scripts/bench/run.sh": "x"})
        head = self.step("chore: release metadata for v0.0.9 [skip release] (#5)", self.METADATA)
        self.assertTrue(self.decide(head, "v0.1.0").skip)

    def test_a_revert_is_a_product_commit_and_releases(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        self.step("feat: b (#2)", {"internal/storage/b.go": "package storage\n"})
        head = self.step('Revert "feat: b" (#3)', {"internal/storage/b.go": "package storage\n"})
        self.assertFalse(self.decide(head, "v0.1.0").skip)

    def test_a_merge_commit_in_the_range_releases(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        git(self.repo.d, "checkout", "-q", "-b", "side", released)
        self.step("docs: s [skip release]", {"docs/s.md": "s"})
        git(self.repo.d, "checkout", "-q", "main")
        self.step("docs: m [skip release]", {"docs/m.md": "m"})
        git(self.repo.d, "merge", "-q", "--no-ff", "-m", "Merge branch side", "side")
        head = git(self.repo.d, "rev-parse", "HEAD")
        v = self.decide(head, "v0.1.0")
        self.assertFalse(v.skip, v.reason)
        self.assertIn("2 parents", v.reason)

    # the marker, per commit
    def test_a_marker_in_the_subject_of_a_product_commit_is_honoured_per_commit(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        head = self.step("refactor: tidy [skip release] (#2)", {"internal/storage/b.go": "package storage\n"})
        self.assertTrue(self.decide(head, "v0.1.0").skip)
        self.assertEqual(self.title_calls, [])

    def test_a_marker_only_in_the_pull_request_title_is_honoured_per_commit(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        marked = self.step("refactor: tidy (#2)", {"internal/storage/b.go": "package storage\n"})
        self.titles[marked] = ["refactor: tidy [skip release]"]
        head = self.step("docs: d (#3)", {"docs/a.md": "d"})
        self.assertTrue(self.decide(head, "v0.1.0").skip)
        self.assertEqual(self.title_calls, [marked])  # only the commit that would release is looked up

    def test_an_unreadable_title_fails_closed_unless_another_commit_is_owed(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        unknown = self.step("refactor: tidy (#2)", {"internal/storage/b.go": "package storage\n"})

        def down(sha):
            def fetch(number):
                raise OSError("api down")
            return fetch
        v = self.decide(unknown, "v0.1.0", titles_for=down)
        self.assertTrue(v.skip and v.failed, v)
        self.assertIn(unknown[:12], v.reason)
        # a second owed commit with a readable title releases: the release covers the unreadable one too
        owed = self.step("fix: z (#3)", {"internal/storage/c.go": "package storage\n"})
        titles = {unknown: None, owed: ["fix: z"]}

        def mixed(sha):
            return down(sha) if titles[sha] is None else (lambda number: titles[sha])
        v = self.decide(owed, "v0.1.0", titles_for=mixed)
        self.assertFalse(v.skip or v.failed, v)

    def test_per_commit_rules_still_apply_inside_the_range(self):
        # Go module files release, a pure digest addition does not
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        self.repo.commit("docker: base", {"Dockerfile.logs": BLOBS[next(f for f in CASES["pr-687"]["files"] if f["path"] == "Dockerfile.logs")["base"]]})
        self.tag("v0.1.1", git(self.repo.d, "rev-parse", "HEAD"))
        digest_added = self.repo.commit("docker: pin digests (#687)", {"Dockerfile.logs": BLOBS[next(f for f in CASES["pr-687"]["files"] if f["path"] == "Dockerfile.logs")["head"]]})
        self.assertTrue(self.decide(digest_added, "v0.1.1").skip)
        gomod = self.step("deps: bump (#8)", {"go.mod": "module x\n"})
        self.assertFalse(self.decide(gomod, "v0.1.1").skip)

    # doubts release
    def test_no_usable_tag_releases(self):
        head = self.step("docs: a [skip release]", {"docs/a.md": "a"})
        for tag in ("v0.0.0", "", "v1.2.3", "main", "v1.2.3-rc1", "v1.2.3\n", "v1.2.3$(touch pwned)", "v1.2.3;id", "../v1", "v01.2.3"):
            with self.subTest(tag=tag):
                v = self.decide(head, tag)
                self.assertFalse(v.skip, v.reason)
        self.assertFalse(os.path.exists(os.path.join(self.repo.d, "pwned")))

    def test_a_tag_that_is_not_an_ancestor_releases(self):
        side = self.step("fix: s", self.PRODUCT)
        git(self.repo.d, "checkout", "-q", "-b", "other", self.base)
        other = self.step("fix: o", {"internal/storage/o.go": "package storage\n"})
        self.tag("v0.1.0", other)
        git(self.repo.d, "checkout", "-q", "main")
        v = self.decide(side, "v0.1.0")
        self.assertFalse(v.skip, v.reason)
        self.assertIn("ancestor", v.reason)

    def test_a_long_range_releases(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        for i in range(r.MAX_RANGE + 1):
            head = self.step(f"docs: {i} [skip release]", {"docs/a.md": "a"})
        v = self.decide(head, "v0.1.0")
        self.assertFalse(v.skip, v.reason)
        self.assertIn("more than", v.reason)

    def test_a_manual_run_always_releases_even_at_a_tagged_commit(self):
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        self.assertFalse(self.decide(released, "v0.1.0", event="workflow_dispatch").skip)

    def test_another_ref_is_refused_loudly(self):
        head, _ = self.burst()
        v = self.decide(head, ref="refs/heads/x")
        self.assertTrue(v.skip and v.failed, v)

    def test_a_bad_sha_releases(self):
        for sha in ("", "HEAD", "z" * 40):
            with self.subTest(sha):
                self.assertFalse(self.decide(sha, "v0.1.0").skip)

    def test_the_decision_is_made_with_the_parents_classifier_of_each_commit(self):
        # a commit that rewrites the classifier to say product=0 cannot hide itself
        released = self.step("fix: x (#1)", self.PRODUCT)
        self.tag("v0.1.0", released)
        self.repo.commit("rewrite", {"scripts/ci/pr_classify.py": "print('product=1')\nprint('reason=parent')\n", "scripts/bench/a.sh": "a\n"})
        head = self.repo.commit("change", {"scripts/ci/pr_classify.py": "print('product=0')\n", "scripts/bench/a.sh": "b\n"})
        v = self.decide(head, "v0.1.0", classifier=None)
        self.assertFalse(v.skip, v.reason)

    def test_an_exception_never_escapes(self):
        def boom(*a):
            raise MemoryError("x")
        head, _ = self.burst()
        v = self.decide(head, classify=boom)
        self.assertFalse(v.skip, v.reason)


class Entrypoint(unittest.TestCase):
    def run_main(self, repo, env):
        with tempfile.TemporaryDirectory() as out:
            paths = {k: os.path.join(out, k) for k in ("GITHUB_OUTPUT", "GITHUB_STEP_SUMMARY")}
            e = {k: v for k, v in os.environ.items() if k not in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "GITHUB_REF", "LATEST_TAG", "EVENT_NAME")}
            e.update(env)
            e.update(paths)
            p = subprocess.run([sys.executable, "-I", os.path.join(HERE, "..", "release_product_check.py")], cwd=repo,
                               env=e, capture_output=True, text=True)
            written = {k: pathlib.Path(v).read_text() if os.path.exists(v) else "" for k, v in paths.items()}
        return p, written

    def setUp(self):
        self.repo = Repo()
        self.addCleanup(self.repo.close)
        files = {}
        for name in ("pr_classify.py", "check_changelog_pr.py", "release_skip.py"):
            with open(os.path.join(HERE, "..", name), encoding="utf-8") as fh:
                files["scripts/ci/" + name] = fh.read()
        self.repo.commit("base", {**files, "internal/x/a.go": "a\n", "docs/a.md": "a\n"})
        self.tagged = self.repo.commit("fix: x (#1)", {"internal/x/a.go": "b\n"})
        git(self.repo.d, "tag", "v0.1.0", self.tagged)

    def test_garbage_environment_exits_zero_and_releases(self):
        p, w = self.run_main(self.repo.d, {"GITHUB_SHA": "not-a-sha", "EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main"})
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(w["GITHUB_OUTPUT"], "skip=false\n")
        self.assertIn("release", w["GITHUB_STEP_SUMMARY"])

    def test_a_skip_is_written_for_the_step_output(self):
        sha = self.repo.commit("docs", {"docs/a.md": "b\n"})
        p, w = self.run_main(self.repo.d, {"GITHUB_SHA": sha, "EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main", "LATEST_TAG": "v0.1.0"})
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertEqual(w["GITHUB_OUTPUT"], "skip=true\n")
        self.assertIn("skip (no release owed)", p.stdout)

    def test_an_owed_commit_behind_a_skip_release_head_releases(self):
        self.repo.commit("fix: lost (#2)", {"internal/x/a.go": "c\n"})
        sha = self.repo.commit("ci: y [skip release] (#3)", {"docs/a.md": "c\n"})
        # no token: the title of the owed commit cannot be read, which fails closed and turns the run red
        p, w = self.run_main(self.repo.d, {"GITHUB_SHA": sha, "EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main", "LATEST_TAG": "v0.1.0"})
        self.assertEqual((p.returncode, w["GITHUB_OUTPUT"]), (1, "skip=true\n"))
        self.assertTrue(any(l.startswith("::warning::") for l in p.stdout.splitlines()), p.stdout)

    def test_a_direct_push_needs_a_readable_title_too(self):
        # a direct push: the lookup is by SHA and needs the token as well
        self.repo.commit("fix: direct", {"internal/x/a.go": "d\n"})
        sha = self.repo.commit("ci: y [skip release]", {"docs/a.md": "d\n"})
        p, w = self.run_main(self.repo.d, {"GITHUB_SHA": sha, "EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main", "LATEST_TAG": "v0.1.0"})
        self.assertEqual(w["GITHUB_OUTPUT"], "skip=true\n")

    def test_manual_run_releases_and_other_ref_is_refused(self):
        sha = self.repo.commit("docs", {"docs/a.md": "b\n"})
        p, w = self.run_main(self.repo.d, {"GITHUB_SHA": sha, "EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/x"})
        self.assertEqual((p.returncode, w["GITHUB_OUTPUT"]), (0, "skip=false\n"))
        p, w = self.run_main(self.repo.d, {"GITHUB_SHA": sha, "EVENT_NAME": "push", "GITHUB_REF": "refs/heads/x", "LATEST_TAG": "v0.1.0"})
        self.assertEqual((p.returncode, w["GITHUB_OUTPUT"]), (1, "skip=true\n"))

    def test_the_workflow_runs_the_range_check_after_the_version_step_and_never_gates_on_the_head_marker(self):
        import yaml
        text = (pathlib.Path(HERE).parents[2] / ".github" / "workflows" / "auto-release.yaml").read_text(encoding="utf-8")
        wf = yaml.safe_load(text)
        steps = wf["jobs"]["decide"]["steps"]
        ids = [s.get("id") for s in steps]
        self.assertNotIn("skip_release", ids)
        self.assertLess(ids.index("version"), ids.index("skip"))
        self.assertLess(ids.index("skip"), ids.index("final"))
        step = steps[ids.index("skip")]
        self.assertEqual(step["run"].strip(), "python3 scripts/ci/release_product_check.py")
        self.assertEqual(step["env"]["LATEST_TAG"], "${{ steps.version.outputs.latest_tag }}")
        self.assertEqual(step["env"]["GITHUB_TOKEN"], "${{ secrets.GITHUB_TOKEN }}")
        self.assertNotIn("BEFORE_SHA", step["env"])
        self.assertNotIn("if", step)
        self.assertNotIn("if", steps[ids.index("version")])
        self.assertNotIn("skip_release", text)
        self.assertIn("SKIP_PRODUCT: ${{ steps.skip.outputs.skip }}", text)
        self.assertIn("needs.decide.outputs.release == 'true'", text)
        self.assertEqual(steps[0]["with"]["fetch-depth"], 0)  # the range needs the history and the tags


if __name__ == "__main__":
    unittest.main()
