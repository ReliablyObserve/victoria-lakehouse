import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import release_skip as r  # noqa: E402


def titles(mapping):
    def fetch(n):
        return mapping[n]
    return fetch


def boom(n):
    raise OSError("network down")


class Decide(unittest.TestCase):
    def test_marker_in_commit_subject(self):
        self.assertTrue(r.decide("chore: x [skip release]\n\nbody", boom)[0])

    def test_marker_only_in_the_pr_title_squash(self):
        # the #441 / #449 shape: the squash subject is the commit subject, the marker is in the PR title.
        skip, why = r.decide("test(manifest): RangePartitions writer test (#441)", titles({"441": "test(manifest): x [skip release]"}))
        self.assertTrue(skip)
        self.assertIn("#441", why)

    def test_squash_without_marker_releases(self):
        self.assertFalse(r.decide("fix(read): x (#429)", titles({"429": "fix(read): x"}))[0])

    def test_api_failure_fails_closed(self):
        skip, why = r.decide("fix: x (#9)", boom)
        self.assertTrue(skip)
        self.assertIn("::warning::", why)
        self.assertIn("workflow_dispatch", why)

    def test_non_squash_subject_never_calls_the_api(self):
        self.assertFalse(r.decide("Merge branch 'x' into main", boom)[0])
        self.assertFalse(r.decide("fix: mentions (#12) in the middle of the subject", boom)[0])

    def test_marker_in_the_body_only_does_not_skip(self):
        self.assertFalse(r.decide("fix: x\n\nnot a title [skip release]", boom)[0])

    def test_manual_run_has_no_commit_message(self):
        self.assertFalse(r.decide("", boom)[0])
        self.assertFalse(r.decide("  \n", boom)[0])


class Cli(unittest.TestCase):
    def run_cli(self, msg, env=None):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "out")
            e = {k: v for k, v in os.environ.items() if k not in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "COMMIT_MSG")}
            e.update({"COMMIT_MSG": msg, "GITHUB_OUTPUT": out, **(env or {})})
            p = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "..", "release_skip.py")],
                               capture_output=True, text=True, env=e)
            with open(out, encoding="utf-8") as fh:
                return p, fh.read()

    def test_skip_written_for_marker(self):
        p, out = self.run_cli("chore: x [skip release]")
        self.assertEqual(p.returncode, 0)
        self.assertEqual(out, "skip=true\n")

    def test_missing_token_on_a_squash_fails_closed(self):
        p, out = self.run_cli("fix: x (#9)")
        self.assertEqual(out, "skip=true\n")
        self.assertIn("workflow_dispatch", p.stdout)

    def test_plain_commit_releases(self):
        p, out = self.run_cli("fix: direct push")
        self.assertEqual(out, "skip=false\n")


if __name__ == "__main__":
    unittest.main()
