"""The Semgrep SARIF summary used by .github/workflows/semgrep.yaml.

Run: python3 -m unittest discover -s scripts/ci/tests -t . -p test_semgrep_sarif_summary.py -v
"""
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "ci" / "semgrep_sarif_summary.py"


def result(rule, level, uri):
    return {"ruleId": rule, "level": level, "locations": [{"physicalLocation": {"artifactLocation": {"uri": uri}}}]}


def run(doc=None, path=None):
    with tempfile.TemporaryDirectory() as tmp:
        target = path or str(pathlib.Path(tmp) / "semgrep.sarif")
        if doc is not None:
            pathlib.Path(target).write_text(json.dumps(doc))
        return subprocess.run([sys.executable, "-I", str(SCRIPT), target], capture_output=True, text=True, check=False)


class SummaryTest(unittest.TestCase):
    def test_counts_by_level_rule_and_path(self):
        doc = {"runs": [{"results": [
            result("a.rule", "error", "internal/x/a.go"),
            result("a.rule", "warning", "internal/x/b.go"),
            result("b.rule", "warning", ".github/workflows/ci.yaml"),
        ]}]}
        proc = run(doc)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("Total findings: **3**", proc.stdout)
        self.assertIn("| error | 1 |", proc.stdout)
        self.assertIn("| warning | 2 |", proc.stdout)
        self.assertIn("| `a.rule` | 2 |", proc.stdout)
        self.assertIn("| `internal/x` | 2 |", proc.stdout)

    def test_zero_findings_is_reported_as_zero(self):
        proc = run({"runs": [{"results": []}]})
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("Total findings: **0**", proc.stdout)

    def test_findings_never_fail_the_summary(self):
        proc = run({"runs": [{"results": [result("r", "error", "a.go")] * 50}]})
        self.assertEqual(proc.returncode, 0)

    def test_missing_file_is_an_error_not_zero_findings(self):
        proc = run(path="/nonexistent/semgrep.sarif")
        self.assertEqual(proc.returncode, 2)
        self.assertNotIn("Total findings", proc.stdout)

    def test_garbage_file_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad = pathlib.Path(tmp) / "bad.sarif"
            bad.write_text("not json")
            proc = run(path=str(bad))
        self.assertEqual(proc.returncode, 2)


if __name__ == "__main__":
    unittest.main()
