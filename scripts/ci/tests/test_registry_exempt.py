import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import registry_exempt as r  # noqa: E402

OWNERS = {"szibis"}


class Decide(unittest.TestCase):
    def test_owner_applying_the_label_in_its_own_run(self):
        ok, msg = r.decide("labeled", "registry-exempt", "SZIBIS", OWNERS)
        self.assertTrue(ok)
        self.assertIn("szibis", msg)

    def test_non_owner_denied(self):
        ok, msg = r.decide("labeled", "registry-exempt", "mallory", OWNERS)
        self.assertFalse(ok)
        self.assertIn("not an approver", msg)

    def test_every_other_event_denies(self):
        for action in ("opened", "synchronize", "reopened", "edited", "unlabeled", "", "push"):
            ok, msg = r.decide(action, "registry-exempt", "szibis", OWNERS)
            self.assertFalse(ok, action)
            self.assertIn("applies the label after the last push", msg)

    def test_another_label_denies(self):
        ok, msg = r.decide("labeled", "size/XL", "szibis", OWNERS)
        self.assertFalse(ok)
        self.assertIn("size/XL", msg)
        self.assertFalse(r.decide("labeled", "", "szibis", OWNERS)[0])

    def test_unknown_actor_denies(self):
        self.assertFalse(r.decide("labeled", "registry-exempt", "", OWNERS)[0])
        self.assertFalse(r.decide("labeled", "registry-exempt", None, OWNERS)[0])

    def test_no_approvers_denies(self):
        self.assertFalse(r.decide("labeled", "registry-exempt", "szibis", set())[0])

    def test_parse_approvers(self):
        self.assertEqual(r.parse_approvers("# c\nSzibis  # owner\n\n"), {"szibis"})
        self.assertEqual(r.parse_approvers(""), set())


class Cli(unittest.TestCase):
    ENV = ("EVENT_ACTION", "LABEL_NAME", "SENDER")

    def run_cli(self, env=None, approvers="szibis\n", args=()):
        with tempfile.TemporaryDirectory() as d:
            ap = os.path.join(d, "approvers")
            if approvers is not None:
                with open(ap, "w", encoding="utf-8") as fh:
                    fh.write(approvers)
            e = {k: v for k, v in os.environ.items() if k not in self.ENV}
            e.update(env or {})
            return subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "..", "registry_exempt.py"),
                                   "--approvers", ap, *args], capture_output=True, text=True, env=e)

    def test_owner_from_the_environment(self):
        p = self.run_cli({"EVENT_ACTION": "labeled", "LABEL_NAME": "registry-exempt", "SENDER": "szibis"})
        self.assertEqual(p.returncode, 0)

    def test_owner_from_arguments(self):
        p = self.run_cli(args=("--action", "labeled", "--label", "registry-exempt", "--sender", "szibis"))
        self.assertEqual(p.returncode, 0)

    def test_nothing_set_fails_closed(self):
        self.assertEqual(self.run_cli().returncode, 1)

    def test_synchronize_denies(self):
        p = self.run_cli({"EVENT_ACTION": "synchronize", "LABEL_NAME": "registry-exempt", "SENDER": "szibis"})
        self.assertEqual(p.returncode, 1)
        self.assertIn("last push", p.stdout)

    def test_missing_approvers_file_fails_closed(self):
        p = self.run_cli({"EVENT_ACTION": "labeled", "LABEL_NAME": "registry-exempt", "SENDER": "szibis"}, approvers=None)
        self.assertEqual(p.returncode, 1)


if __name__ == "__main__":
    unittest.main()
