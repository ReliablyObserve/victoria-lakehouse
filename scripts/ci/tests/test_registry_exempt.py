import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import registry_exempt as r  # noqa: E402


def ev(kind, actor, label="registry-exempt"):
    return {"event": kind, "actor": {"login": actor} if actor else None, "label": {"name": label}}


OWNERS = {"szibis"}


class Decide(unittest.TestCase):
    def test_owner_applied(self):
        ok, _ = r.decide([ev("labeled", "SZIBIS")], OWNERS)
        self.assertTrue(ok)

    def test_non_owner_applied(self):
        ok, msg = r.decide([ev("labeled", "mallory")], OWNERS)
        self.assertFalse(ok)
        self.assertIn("not an approver", msg)

    def test_relabeled_by_non_owner(self):
        ok, msg = r.decide([ev("labeled", "szibis"), ev("unlabeled", "szibis"), ev("labeled", "mallory")], OWNERS)
        self.assertFalse(ok)
        self.assertIn("mallory", msg)

    def test_owner_relabels_after_non_owner(self):
        ok, _ = r.decide([ev("labeled", "mallory"), ev("unlabeled", "mallory"), ev("labeled", "szibis")], OWNERS)
        self.assertTrue(ok)

    def test_removed(self):
        ok, _ = r.decide([ev("labeled", "szibis"), ev("unlabeled", "szibis")], OWNERS)
        self.assertFalse(ok)

    def test_other_labels_ignored(self):
        ok, _ = r.decide([ev("labeled", "szibis", "bug")], OWNERS)
        self.assertFalse(ok)

    def test_unknown_actor_and_no_approvers(self):
        self.assertFalse(r.decide([ev("labeled", None)], OWNERS)[0])
        self.assertFalse(r.decide([ev("labeled", "szibis")], set())[0])

    def test_parse_approvers(self):
        self.assertEqual(r.parse_approvers("# c\nSzibis  # owner\n\n"), {"szibis"})


class Cli(unittest.TestCase):
    def run_cli(self, events, approvers="szibis\n", env=None):
        with tempfile.TemporaryDirectory() as d:
            ap = os.path.join(d, "approvers")
            open(ap, "w").write(approvers)
            args = [sys.executable, os.path.join(os.path.dirname(__file__), "..", "registry_exempt.py"), "--approvers", ap]
            if events is not None:
                ef = os.path.join(d, "e.json")
                json.dump(events, open(ef, "w"))
                args += ["--events-file", ef]
            e = {k: v for k, v in os.environ.items() if k not in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "PR_NUMBER")}
            e.update(env or {})
            return subprocess.run(args, capture_output=True, text=True, env=e)

    def test_owner_ok(self):
        self.assertEqual(self.run_cli([ev("labeled", "szibis")]).returncode, 0)

    def test_no_token_fails_closed(self):
        p = self.run_cli(None)
        self.assertEqual(p.returncode, 1)
        self.assertIn("only works in CI", p.stdout)

    def test_api_failure_fails_closed(self):
        p = self.run_cli(None, env={"GITHUB_TOKEN": "x", "GITHUB_REPOSITORY": "invalid/invalid", "PR_NUMBER": "1"})
        self.assertEqual(p.returncode, 1)
        self.assertIn("cannot verify", p.stdout)

    def test_bad_events_file_fails_closed(self):
        self.assertEqual(self.run_cli({"not": "a list"}).returncode, 1)


if __name__ == "__main__":
    unittest.main()
