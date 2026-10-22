"""Static and behaviour checks for the vulnerability scan gate.

The scan is a required check, so it has to be able to fail:

  * every trivy-action step sets `exit-code: "1"`, `severity: HIGH,CRITICAL` and
    `ignore-unfixed: "true"`, and none of them is allowed to continue on error (the one step
    that must fail on purpose, in the gate fixture job, is the only exception);
  * the dependency scan (`trivy`) and the scan of the four release images
    (`trivy-image-{logs,logs-fips,traces,traces-fips}`) exist;
  * each scan job downloads the vulnerability database in its own step first
    (scripts/ci/trivy_fetch_db.sh), so a download problem is told apart from a finding;
  * the `trivy` job keeps its name: it is a required check context;
  * a `.trivyignore`, if the repository has one, gives every entry a reason and a future
    `expires:` date.

scripts/ci/trivy_fetch_db.sh is also run here against a stub `trivy`.

Run: python3 -I -m unittest discover -s scripts/ci/tests -p test_trivy_gate.py -v
"""

import datetime
import os
import pathlib
import stat
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github" / "workflows"
SECURITY = WORKFLOWS / "security.yaml"
FETCH_DB = ROOT / "scripts" / "ci" / "trivy_fetch_db.sh"
TRIVYIGNORE = ROOT / ".trivyignore"

TRIVY_ACTION = "aquasecurity/trivy-action@"
FIXTURE_JOB = "trivy-gate-fixture"
IMAGE_TARGETS = ["logs", "logs-fips", "traces", "traces-fips"]


def trivy_steps(doc: dict):
    """Yield (job id, step index, step) for every trivy-action step of a workflow document."""
    for job_id, job in (doc.get("jobs") or {}).items():
        for index, step in enumerate(job.get("steps") or []):
            if str(step.get("uses", "")).startswith(TRIVY_ACTION):
                yield job_id, index, step


def gate_problems(name: str, doc: dict) -> list:
    """One message per trivy-action step that cannot fail the job on a fixable finding."""
    problems = []
    for job_id, _, step in trivy_steps(doc):
        where = f"{name}: job {job_id}, step {step.get('name', '?')!r}"
        args = step.get("with") or {}
        if str(args.get("exit-code", "")) != "1":
            problems.append(f"{where}: exit-code must be \"1\" (got {args.get('exit-code')!r})")
        if args.get("severity") != "HIGH,CRITICAL":
            problems.append(f"{where}: severity must be HIGH,CRITICAL (got {args.get('severity')!r})")
        if str(args.get("ignore-unfixed", "")).lower() != "true":
            problems.append(f"{where}: ignore-unfixed must be \"true\"")
        if step.get("continue-on-error") and job_id != FIXTURE_JOB:
            problems.append(f"{where}: continue-on-error would hide a finding")
    return problems


def ignore_problems(text: str, today: datetime.date) -> list:
    """Problems of a .trivyignore: an entry needs a comment line with a reason and `expires:`."""
    problems = []
    comments = []
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line:
            comments = []
            continue
        if line.startswith("#"):
            comments.append(line.lstrip("# ").strip())
            continue
        expires = [c for c in comments if c.lower().startswith("expires:")]
        reasons = [c for c in comments if c and not c.lower().startswith("expires:")]
        if not reasons:
            problems.append(f".trivyignore:{number}: {line} has no reason comment above it")
        if len(expires) != 1:
            problems.append(f".trivyignore:{number}: {line} needs exactly one `# expires: YYYY-MM-DD` comment above it")
            continue
        try:
            date = datetime.date.fromisoformat(expires[0].split(":", 1)[1].strip())
        except ValueError:
            problems.append(f".trivyignore:{number}: {line} has an unreadable expires date")
            continue
        if date < today:
            problems.append(f".trivyignore:{number}: {line} expired on {date}")
    return problems


class GateCheckerTest(unittest.TestCase):
    """The checkers themselves, on small documents."""

    def doc(self, **args):
        with_args = {"exit-code": "1", "severity": "HIGH,CRITICAL", "ignore-unfixed": "true"}
        with_args.update(args)
        step = {"name": "scan", "uses": TRIVY_ACTION + "0" * 40, "with": with_args}
        return {"jobs": {"scan": {"steps": [step]}}}

    def test_complete_step_passes(self):
        self.assertEqual(gate_problems("w", self.doc()), [])

    def test_missing_exit_code_is_rejected(self):
        doc = self.doc()
        del doc["jobs"]["scan"]["steps"][0]["with"]["exit-code"]
        self.assertEqual(len(gate_problems("w", doc)), 1)

    def test_exit_code_zero_is_rejected(self):
        self.assertEqual(len(gate_problems("w", self.doc(**{"exit-code": "0"}))), 1)

    def test_other_severity_is_rejected(self):
        self.assertEqual(len(gate_problems("w", self.doc(severity="CRITICAL"))), 1)

    def test_unfixed_findings_policy_is_required(self):
        self.assertEqual(len(gate_problems("w", self.doc(**{"ignore-unfixed": "false"}))), 1)

    def test_continue_on_error_is_rejected_outside_the_fixture_job(self):
        doc = self.doc()
        doc["jobs"]["scan"]["steps"][0]["continue-on-error"] = True
        self.assertEqual(len(gate_problems("w", doc)), 1)

    def test_ignore_entry_needs_reason_and_expiry(self):
        today = datetime.date(2026, 10, 22)
        self.assertEqual(ignore_problems("", today), [])
        ok = "# upstream fix not released yet\n# expires: 2026-12-01\nCVE-2026-0001\n"
        self.assertEqual(ignore_problems(ok, today), [])
        self.assertEqual(len(ignore_problems("CVE-2026-0001\n", today)), 2)
        self.assertEqual(len(ignore_problems("# why\nCVE-2026-0001\n", today)), 1)
        self.assertEqual(len(ignore_problems("# expires: 2026-12-01\nCVE-2026-0001\n", today)), 1)
        self.assertEqual(len(ignore_problems("# why\n# expires: 2026-10-01\nCVE-2026-0001\n", today)), 1)
        self.assertEqual(len(ignore_problems("# why\n# expires: soon\nCVE-2026-0001\n", today)), 1)


class WorkflowGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docs = {}
        for path in sorted(list(WORKFLOWS.glob("*.yaml")) + list(WORKFLOWS.glob("*.yml"))):
            text = path.read_text()
            if TRIVY_ACTION in text:
                cls.docs[path.name] = yaml.safe_load(text)
        cls.security = yaml.safe_load(SECURITY.read_text())

    def test_every_trivy_step_can_fail(self):
        problems = []
        for name, doc in self.docs.items():
            problems += gate_problems(name, doc)
        self.assertEqual(problems, [])

    def test_trivy_is_only_used_by_the_security_workflow(self):
        self.assertEqual(sorted(self.docs), ["security.yaml"])

    def test_scan_steps_use_the_prepared_database(self):
        for job_id, _, step in trivy_steps(self.security):
            args = step["with"]
            self.assertEqual(str(args.get("skip-setup-trivy")), "true", job_id)
            self.assertEqual(str(args.get("cache")), "false", job_id)
            self.assertEqual(str((step.get("env") or {}).get("TRIVY_SKIP_DB_UPDATE")), "true", job_id)

    def test_database_download_is_its_own_step_before_the_scan(self):
        for job_id, job in self.security["jobs"].items():
            steps = job["steps"]
            scans = [i for i, s in enumerate(steps) if str(s.get("uses", "")).startswith(TRIVY_ACTION)]
            if not scans:
                continue
            fetch = [i for i, s in enumerate(steps) if "scripts/ci/trivy_fetch_db.sh" in str(s.get("run", ""))]
            self.assertEqual(len(fetch), 1, f"{job_id}: one database download step")
            self.assertLess(fetch[0], scans[0], f"{job_id}: download before the first scan")

    def test_dependency_scan_keeps_its_required_check_name(self):
        job = self.security["jobs"]["trivy"]
        self.assertNotIn("name", job, "the required check context is the job id `trivy`")
        scans = [s for s in job["steps"] if str(s.get("uses", "")).startswith(TRIVY_ACTION)]
        self.assertEqual(len(scans), 1)
        self.assertEqual(scans[0]["with"]["scan-type"], "fs")
        self.assertEqual(scans[0]["with"]["scan-ref"], ".")
        self.assertIn("vuln", scans[0]["with"]["scanners"].split(","))

    def test_all_release_images_are_scanned(self):
        job = self.security["jobs"]["trivy-image"]
        self.assertEqual(job["name"], "trivy-image-${{ matrix.target }}")
        targets = [entry["target"] for entry in job["strategy"]["matrix"]["include"]]
        self.assertEqual(targets, IMAGE_TARGETS)
        variants = {(e["module"], e["fips"]) for e in job["strategy"]["matrix"]["include"]}
        self.assertEqual(variants, {("logs", "0"), ("logs", "1"), ("traces", "0"), ("traces", "1")})
        scans = [s for s in job["steps"] if str(s.get("uses", "")).startswith(TRIVY_ACTION)]
        self.assertEqual(len(scans), 1)
        self.assertEqual(scans[0]["with"]["scan-type"], "image")
        build = next(s for s in job["steps"] if s.get("name") == "Build image")
        self.assertIn('-f "Dockerfile.${MODULE}"', build["run"])
        self.assertIn('--build-arg FIPS="${FIPS_ARG}"', build["run"])
        self.assertNotIn("push", build["run"])
        # the scanned reference is the image that was built
        self.assertEqual(build["env"]["IMAGE"], scans[0]["with"]["image-ref"])

    def test_fixture_job_proves_both_outcomes(self):
        job = self.security["jobs"][FIXTURE_JOB]
        scans = [s for s in job["steps"] if str(s.get("uses", "")).startswith(TRIVY_ACTION)]
        self.assertEqual(len(scans), 2)
        clean, vulnerable = scans
        self.assertFalse(clean.get("continue-on-error"))
        self.assertTrue(vulnerable.get("continue-on-error"))
        self.assertEqual(vulnerable.get("id"), "vulnerable")
        check = next(s for s in job["steps"] if s.get("name", "").startswith("Check the gate failed"))
        self.assertIn("steps.vulnerable.outcome", str(check["env"]))
        self.assertIn("failure", check["run"])

    def test_trivyignore_entries_have_reason_and_expiry(self):
        if not TRIVYIGNORE.exists():
            self.skipTest("no .trivyignore: nothing is waived")
        self.assertEqual(ignore_problems(TRIVYIGNORE.read_text(), datetime.date.today()), [])


class FetchDbTest(unittest.TestCase):
    """scripts/ci/trivy_fetch_db.sh against a stub trivy that records its calls."""

    def run_script(self, stub_body: str, cache_has_db: bool = False, attempts: int = 2):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = pathlib.Path(tmp)
            bin_dir = tmp_path / "bin"
            bin_dir.mkdir()
            calls = tmp_path / "calls.txt"
            stub = bin_dir / "trivy"
            stub.write_text("#!/usr/bin/env bash\n" f'echo "$*" >> "{calls}"\n' + stub_body)
            stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
            cache = tmp_path / "cache"
            (cache / "db").mkdir(parents=True)
            if cache_has_db:
                (cache / "db" / "trivy.db").write_text("old database")
            env = dict(
                os.environ,
                PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                TRIVY_CACHE_DIR=str(cache),
                TRIVY_DB_ATTEMPTS=str(attempts),
                TRIVY_DB_SLEEP_STEP="0",
                TRIVY_DB_REPOS="one.example/db:2 two.example/db:2",
            )
            result = subprocess.run(
                ["bash", str(FETCH_DB)], env=env, capture_output=True, text=True, check=False
            )
            made = calls.read_text().splitlines() if calls.exists() else []
            return result, made

    def test_first_mirror_works(self):
        result, calls = self.run_script("exit 0\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 1)
        self.assertIn("--db-repository one.example/db:2", calls[0])
        self.assertIn("--download-db-only", calls[0])

    def test_next_mirror_is_tried_when_the_first_fails(self):
        stub = 'case "$*" in *one.example*) exit 1;; esac\nexit 0\n'
        result, calls = self.run_script(stub)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertIn("two.example/db:2", calls[1])

    def test_every_attempt_failing_without_a_cached_database_fails_clearly(self):
        result, calls = self.run_script("exit 1\n", cache_has_db=False, attempts=2)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 4, "2 rounds over 2 mirrors")
        self.assertIn("not a scan finding", result.stdout)

    def test_a_cached_database_is_used_with_a_warning(self):
        result, calls = self.run_script("exit 1\n", cache_has_db=True, attempts=1)
        self.assertEqual(result.returncode, 0)
        self.assertIn("::warning::", result.stdout)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
