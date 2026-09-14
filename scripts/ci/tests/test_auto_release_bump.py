"""Tests for the version-bump step of .github/workflows/auto-release.yaml.

The step's shell script is extracted from the workflow and executed against a
scratch git repository with a fake `gh`, so the label and size rules are tested
exactly as they run in CI.
"""

import os
import pathlib
import shutil
import stat
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github" / "workflows" / "auto-release.yaml"

FAKE_GH = """#!/usr/bin/env bash
case "$*" in
  *additions,deletions,files*) echo "${FAKE_PR_TOTAL:-0} ${FAKE_PR_FILES:-0}" ;;
  *labels*) printf '%s' "${FAKE_LABELS:-}" | tr ',' '\\n' ;;
esac
"""


def grep_supports_perl() -> bool:
    try:
        return subprocess.run(
            ["grep", "-oP", "x"], input="x", capture_output=True, text=True
        ).returncode == 0
    except OSError:
        return False


def load_workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())


def bump_script() -> str:
    for step in load_workflow()["jobs"]["release"]["steps"]:
        if step.get("name") == "Determine version bump":
            return step["run"]
    raise AssertionError("step 'Determine version bump' not found")


class WorkflowShapeTests(unittest.TestCase):
    def test_release_runs_are_serialized(self):
        concurrency = load_workflow().get("concurrency")
        self.assertIsNotNone(concurrency, "auto-release must declare a concurrency group")
        self.assertEqual(concurrency.get("group"), "auto-release")
        self.assertFalse(concurrency.get("cancel-in-progress"))


@unittest.skipUnless(
    grep_supports_perl() or os.environ.get("CI"),
    "the step uses GNU grep -P; runs in CI",
)
class BumpScriptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        repo = self.tmp / "repo"
        repo.mkdir()
        git = ["git", "-C", str(repo)]
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        subprocess.run(git + ["-c", "user.name=t", "-c", "user.email=t@t", "commit",
                              "-q", "--allow-empty", "-m", "base"], check=True)
        for tag in ("v0.121.0", "v0.121.1", "v0.122.0"):
            subprocess.run(git + ["tag", tag], check=True)
        self.repo = repo
        bindir = self.tmp / "bin"
        bindir.mkdir()
        gh = bindir / "gh"
        gh.write_text(FAKE_GH)
        gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
        self.bindir = bindir
        self.script = self.tmp / "bump.sh"
        self.script.write_text("set -e\n" + bump_script())

    def run_bump(self, commit_msg, labels="", total=0, files=0):
        out = self.tmp / "github_output"
        out.write_text("")
        env = dict(os.environ)
        env.update(
            PATH=f"{self.bindir}{os.pathsep}{env['PATH']}",
            COMMIT_MSG=commit_msg,
            GITHUB_OUTPUT=str(out),
            FAKE_LABELS=labels,
            FAKE_PR_TOTAL=str(total),
            FAKE_PR_FILES=str(files),
        )
        subprocess.run(["bash", str(self.script)], cwd=self.repo, env=env,
                       check=True, capture_output=True, text=True)
        return dict(line.split("=", 1) for line in out.read_text().splitlines() if "=" in line)

    def test_latest_tag_is_the_highest_version(self):
        self.assertEqual(self.run_bump("fix: small (#1)", labels="bugfix")["latest_tag"], "v0.122.0")

    def test_explicit_patch_label_wins_over_size(self):
        got = self.run_bump("fix(delete): big fix (#202)", labels="bugfix", total=11109, files=74)
        self.assertEqual((got["bump"], got["next"]), ("patch", "v0.122.1"))

    def test_performance_label_wins_over_size(self):
        got = self.run_bump("perf: big (#9)", labels="performance", total=9000, files=10)
        self.assertEqual((got["bump"], got["next"]), ("patch", "v0.122.1"))

    def test_unlabeled_large_pr_is_xl(self):
        got = self.run_bump("chore: huge (#10)", total=11109, files=74)
        self.assertEqual((got["bump"], got["next"]), ("xl", "v0.132.0"))

    def test_feature_label_is_minor_even_when_large(self):
        got = self.run_bump("conformance: catalog (#197)", labels="feature", total=12000, files=120)
        self.assertEqual((got["bump"], got["next"]), ("minor", "v0.123.0"))

    def test_unlabeled_small_feat_commit_is_minor(self):
        got = self.run_bump("feat: small thing (#11)", total=40, files=2)
        self.assertEqual((got["bump"], got["next"]), ("minor", "v0.123.0"))

    def test_unlabeled_small_fix_is_patch(self):
        got = self.run_bump("fix: tiny (#12)", total=10, files=1)
        self.assertEqual((got["bump"], got["next"]), ("patch", "v0.122.1"))


if __name__ == "__main__":
    unittest.main()
