"""Static properties of the two registry-gate workflows that a PR must not be able to lose."""
import os
import re
import unittest

import yaml

ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..")
WORKFLOWS = ".github/workflows"


def load(name):
    with open(os.path.join(ROOT, WORKFLOWS, name), encoding="utf-8") as fh:
        text = fh.read()
    return text, yaml.safe_load(text)


def steps(doc):
    for job in doc["jobs"].values():
        yield from job["steps"]


class GateWorkflows(unittest.TestCase):
    NAMES = ("registry-gate.yaml", "registry-gate-base.yaml")

    def test_every_action_is_pinned_by_full_sha(self):
        for name in self.NAMES:
            _, doc = load(name)
            for step in steps(doc):
                if "uses" in step:
                    self.assertRegex(step["uses"], r"^[\w.-]+/[\w.-]+@[0-9a-f]{40}$", f"{name}: {step['uses']}")

    def test_pinned_actions_carry_a_version_comment(self):
        for name in self.NAMES:
            text, _ = load(name)
            for line in text.splitlines():
                if re.search(r"uses: \S+@[0-9a-f]{40}", line):
                    self.assertRegex(line, r"# v\d", f"{name}: {line.strip()}")

    def test_setup_go_has_no_cache_and_no_cache_action(self):
        for name in self.NAMES:
            text, doc = load(name)
            self.assertNotIn("actions/cache", text, name)
            for step in steps(doc):
                if step.get("uses", "").startswith("actions/setup-go@"):
                    self.assertIs(step["with"].get("cache"), False, name)

    def test_no_job_level_if(self):
        for name in self.NAMES:
            _, doc = load(name)
            for job in doc["jobs"].values():
                self.assertNotIn("if", job, name)

    def test_no_event_text_inside_a_run_script(self):
        for name in self.NAMES:
            _, doc = load(name)
            for step in steps(doc):
                self.assertNotRegex(step.get("run", ""), r"\$\{\{\s*github\.event", f"{name}: {step.get('name')}")

    def test_the_label_events_rerun_the_gate_and_not_the_heavy_workflow(self):
        _, gate = load("registry-gate.yaml")
        _, base = load("registry-gate-base.yaml")
        _, heavy = load("conformance.yaml")
        want = {"opened", "synchronize", "reopened", "labeled", "unlabeled", "edited"}
        self.assertEqual(set(gate[True]["pull_request"]["types"]), want)
        self.assertEqual(set(base[True]["pull_request_target"]["types"]), want)
        self.assertEqual(set(heavy[True]["pull_request"]["types"]), {"opened", "synchronize", "reopened"})

    def test_there_is_no_transition_fallback_and_no_token(self):
        # Issue 456: the bootstrap is on main, so a base without it is refused, not emulated.
        text, doc = load("registry-gate.yaml")
        self.assertEqual(doc["permissions"], {"contents": "read"})
        gate_step = [s for s in steps(doc) if s.get("name", "").startswith("Product changes")][0]
        self.assertNotIn("GITHUB_TOKEN", gate_step["env"])
        self.assertNotIn("<<'BOOT'", text)
        self.assertIn("rebase this PR onto the current", text)

    def test_the_pull_request_target_gate_never_checks_the_pr_out(self):
        text, doc = load("registry-gate-base.yaml")
        self.assertEqual(doc["permissions"], {"contents": "read"})
        self.assertNotIn("worktree add", text)
        for step in steps(doc):
            if step.get("uses", "").startswith("actions/checkout@"):
                self.assertNotIn("ref", step.get("with", {}), "the default checkout of pull_request_target is the base")
        gate_step = [s for s in steps(doc) if s.get("name", "").startswith("Product changes")][0]
        self.assertEqual(gate_step["env"]["HEAD_REV"], "${{ github.event.pull_request.head.sha }}")
        fetch = [s for s in steps(doc) if "Fetch" in s.get("name", "")][0]
        self.assertEqual(fetch["env"]["GIT_LFS_SKIP_SMUDGE"], "1")

    def test_the_gate_job_names_are_the_required_check_names(self):
        self.assertEqual(list(load("registry-gate.yaml")[1]["jobs"]), ["registry-gate"])
        self.assertEqual(list(load("registry-gate-base.yaml")[1]["jobs"]), ["registry-gate-base"])
        self.assertEqual(list(load("conformance.yaml")[1]["jobs"]), ["conformance-inventory"])

    def test_the_ratchet_runs_isolated_with_floors_and_registry(self):
        text, _ = load("parity.yaml")
        m = re.search(r"python -I scripts/ci/parity_ratchet\.py[^\n]*\\\n(?:[^\n]*\\\n)*[^\n]*", text)
        self.assertIsNotNone(m, "the ratchet must run with python -I")
        self.assertIn("--lock-cells tests/parity/lock_cells.txt", m.group(0))
        self.assertIn("--registry tests/conformance/registry/rows", m.group(0))
        self.assertIn("--allowlist tests/parity/known_failures.txt", m.group(0))


    def test_the_parity_job_generates_a_fresh_nonce_for_the_suite_and_the_ratchet(self):
        text, doc = load("parity.yaml")
        run = [s for s in steps(doc) if s.get("name", "").startswith("Run parity tests")][0]["run"]
        self.assertIn("PARITY_LOCK_NONCE=$(openssl rand -hex 16)", run)
        self.assertIn('echo "PARITY_LOCK_NONCE=$PARITY_LOCK_NONCE" >> "$GITHUB_ENV"', run)
        self.assertLess(run.index("openssl rand"), run.index("docker compose"), "the nonce exists before the suite starts")
        with open(os.path.join(ROOT, "tests/parity/docker-compose.yml"), encoding="utf-8") as fh:
            self.assertIn("PARITY_LOCK_NONCE: ${PARITY_LOCK_NONCE:-}", fh.read())
        with open(os.path.join(ROOT, "tests/parity/lock_cells_test.go"), encoding="utf-8") as fh:
            self.assertIn('os.Getenv("PARITY_LOCK_NONCE")', fh.read())


if __name__ == "__main__":
    unittest.main()
