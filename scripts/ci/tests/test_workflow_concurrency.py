"""Static and behavioural checks for the workflow concurrency rules and the conflicting-run sweeper.

Rules for every workflow in .github/workflows/*.y*ml that runs on `pull_request` or `pull_request_target`:

  missing        no top-level `concurrency:` (a new push to the pull request would not cancel the run of the
                 previous head, so both run to the end)
  group          the group does not contain `github.event.pull_request.number` (runs of different pull requests
                 would cancel each other, or those of one pull request would never meet); a SERIALISED workflow
                 may use `github.ref` instead, which is `refs/pull/N/merge` for a pull request
  cancel         `cancel-in-progress` is neither the pull-request-only expression
                 `${{ github.event_name == 'pull_request' }}` nor, for a workflow that has no other trigger, `true`
  main-cancel    a workflow with a trigger other than a pull request sets `cancel-in-progress: true`: a run of
                 main, a schedule or a manual run could be cancelled (checked for every workflow)
  fallback       a workflow with a trigger other than a pull request has a group without `github.run_id` as the
                 fallback, so a queued run of main could be replaced by the next one (a cancelled pending run is
                 a run that never reports); the two workflows that serialise on purpose are listed in SERIALISED

`cancel-conflicting-pr-runs.yaml` is checked for least privilege (no workflow permissions, one job with exactly
`actions: write` and `pull-requests: read`, no action, no expression inside a `run:` script), and its script is run
against a stub `gh` to prove it cancels only `pull_request` runs of the head of a CONFLICTING pull request.

Run: python3 -I -m unittest discover -s scripts/ci/tests -t . -p test_workflow_concurrency.py -v
"""
import json
import os
import pathlib
import re
import shutil
import stat
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]
PR_EVENTS = {"pull_request", "pull_request_target"}
PR_ONLY_EXPR = "${{ github.event_name == 'pull_request' }}"
SWEEPER = "cancel-conflicting-pr-runs.yaml"

# Workflows whose runs on one ref must queue behind each other (never run side by side) and so keep a
# ref-based group. They still cancel only a superseded pull request run.
SERIALISED = {
    "minio-images.yaml": "pushes immutable image tags; two runs on one ref must not race on the same tag",
    "docs-site.yaml": "deploys GitHub Pages; two deploys of one ref must not run side by side",
}


def workflow_files(root: pathlib.Path):
    wf = root / ".github" / "workflows"
    return sorted(list(wf.glob("*.yaml")) + list(wf.glob("*.yml")))


def triggers(doc: dict) -> set:
    # PyYAML reads the bare key `on` as the boolean True.
    on = doc.get("on", doc.get(True))
    if isinstance(on, str):
        return {on}
    if isinstance(on, list):
        return set(on)
    return set((on or {}).keys())


def _norm(expr) -> str:
    return re.sub(r"\s+", " ", str(expr)).strip()


def check_workflow(name: str, doc: dict) -> list:
    """Return (rule, message) for each violation in the parsed workflow `doc` of file `name`."""
    found = []
    trig = triggers(doc)
    other = trig - PR_EVENTS
    conc = doc.get("concurrency")
    if isinstance(conc, str):
        conc = {"group": conc}
    cancel = _norm(conc.get("cancel-in-progress")) if isinstance(conc, dict) and "cancel-in-progress" in conc else ""
    if other and cancel == "True":
        found.append(("main-cancel", f"{name}: `cancel-in-progress: true` with the trigger(s) {sorted(other)} could cancel a run that is not a superseded pull request run"))
    if not (trig & PR_EVENTS):
        return found
    if not isinstance(conc, dict):
        found.append(("missing", f"{name}: runs on a pull request and has no top-level `concurrency:`"))
        return found
    group = _norm(conc.get("group", ""))
    serialised = name in SERIALISED and "github.ref" in group  # refs/pull/N/merge is per pull request already
    if "github.event.pull_request.number" not in group and not serialised:
        found.append(("group", f"{name}: the concurrency group `{group}` does not contain github.event.pull_request.number"))
    if other:
        if cancel != PR_ONLY_EXPR:
            found.append(("cancel", f"{name}: `cancel-in-progress` must be `{PR_ONLY_EXPR}` (the workflow also runs on {sorted(other)}), not `{cancel}`"))
        if "github.run_id" not in group and name not in SERIALISED:
            found.append(("fallback", f"{name}: the group `{group}` has no `github.run_id` fallback, so a queued run of {sorted(other)} could be replaced by the next one"))
    elif cancel not in (PR_ONLY_EXPR, "True"):
        found.append(("cancel", f"{name}: `cancel-in-progress` must be `true` or `{PR_ONLY_EXPR}`, not `{cancel}`"))
    return found


class CheckWorkflowFixtures(unittest.TestCase):
    """The checker itself: it must be able to fail."""

    def parse(self, text):
        return yaml.safe_load(text)

    def rules(self, text, name="w.yaml"):
        return [r for r, _ in check_workflow(name, self.parse(text))]

    GOOD = (
        "on:\n  pull_request:\n  push:\n    branches: [main]\n"
        "concurrency:\n  group: ${{ github.workflow }}-${{ github.event.pull_request.number || github.run_id }}\n"
        "  cancel-in-progress: ${{ github.event_name == 'pull_request' }}\njobs: {}\n"
    )

    def test_the_standard_block_passes(self):
        self.assertEqual(self.rules(self.GOOD), [])

    def test_a_workflow_without_a_pull_request_trigger_needs_nothing(self):
        self.assertEqual(self.rules("on:\n  schedule:\n    - cron: '0 1 * * *'\njobs: {}\n"), [])

    def test_missing_concurrency_is_rejected(self):
        self.assertEqual(self.rules("on:\n  pull_request:\njobs: {}\n"), ["missing"])

    def test_missing_concurrency_on_pull_request_target_is_rejected(self):
        self.assertEqual(self.rules("on:\n  pull_request_target:\njobs: {}\n"), ["missing"])

    def test_a_group_without_the_pull_request_number_is_rejected(self):
        text = self.GOOD.replace("github.event.pull_request.number || ", "")
        self.assertIn("group", self.rules(text))

    def test_cancel_in_progress_false_is_rejected(self):
        text = self.GOOD.replace("${{ github.event_name == 'pull_request' }}", "false")
        self.assertEqual(self.rules(text), ["cancel"])

    def test_cancel_in_progress_true_with_a_push_trigger_is_rejected(self):
        text = self.GOOD.replace("${{ github.event_name == 'pull_request' }}", "true")
        self.assertEqual(sorted(self.rules(text)), ["cancel", "main-cancel"])

    def test_cancel_in_progress_true_without_a_pull_request_trigger_is_rejected(self):
        text = "on:\n  push:\n    branches: [main]\nconcurrency:\n  group: x\n  cancel-in-progress: true\njobs: {}\n"
        self.assertEqual(self.rules(text), ["main-cancel"])

    def test_cancel_in_progress_true_is_fine_for_a_pull_request_only_workflow(self):
        text = "on:\n  pull_request:\nconcurrency:\n  group: x-${{ github.event.pull_request.number }}\n  cancel-in-progress: true\njobs: {}\n"
        self.assertEqual(self.rules(text), [])

    def test_a_ref_fallback_is_rejected_unless_the_workflow_is_serialised(self):
        text = self.GOOD.replace("github.run_id", "github.ref")
        self.assertEqual(self.rules(text), ["fallback"])
        self.assertEqual(self.rules(text, name="minio-images.yaml"), [])

    def test_the_trigger_list_form_is_understood(self):
        self.assertEqual(self.rules("on: [pull_request, push]\njobs: {}\n"), ["missing"])
        self.assertEqual(self.rules("on: pull_request\njobs: {}\n"), ["missing"])


class RepositoryWorkflows(unittest.TestCase):
    def docs(self):
        return {p.name: yaml.safe_load(p.read_text()) for p in workflow_files(ROOT)}

    def test_every_workflow_passes(self):
        problems = []
        for name, doc in self.docs().items():
            problems += [msg for _, msg in check_workflow(name, doc)]
        self.assertEqual(problems, [], "\n".join(problems))

    def test_the_serialised_list_has_no_stale_entries(self):
        docs = self.docs()
        stale = []
        for name, reason in SERIALISED.items():
            conc = (docs.get(name) or {}).get("concurrency") or {}
            if not reason.strip() or "github.ref" not in str(conc.get("group", "")):
                stale.append(name)
        self.assertEqual(stale, [], f"SERIALISED entries that no longer use a ref group: {stale}")

    def test_there_are_pull_request_workflows_to_check(self):
        n = sum(1 for d in self.docs().values() if triggers(d) & PR_EVENTS)
        self.assertGreaterEqual(n, 10)


class SweeperWorkflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / ".github" / "workflows" / SWEEPER
        cls.text = cls.path.read_text()
        cls.doc = yaml.safe_load(cls.text)

    def test_it_runs_only_on_push_to_main_and_manually(self):
        on = self.doc.get("on", self.doc.get(True))
        self.assertEqual(set(on), {"push", "workflow_dispatch"})
        self.assertEqual(on["push"]["branches"], ["main"])

    def test_it_holds_no_permission_at_the_workflow_level(self):
        self.assertEqual(self.doc["permissions"], {})

    def test_its_single_job_holds_exactly_actions_write_and_pull_requests_read(self):
        jobs = self.doc["jobs"]
        self.assertEqual(len(jobs), 1)
        (job,) = jobs.values()
        self.assertEqual(job["permissions"], {"actions": "write", "pull-requests": "read"})

    def test_it_uses_no_action_and_no_checkout(self):
        (job,) = self.doc["jobs"].values()
        self.assertTrue(all("uses" not in step for step in job["steps"]))

    def test_no_expression_is_expanded_inside_a_script(self):
        (job,) = self.doc["jobs"].values()
        for step in job["steps"]:
            self.assertNotIn("${{", step.get("run", ""), "an expression in a run: script; pass it through env:")

    def test_it_is_not_a_pull_request_workflow(self):
        self.assertEqual(triggers(self.doc) & PR_EVENTS, set())


@unittest.skipUnless(shutil.which("jq") and shutil.which("bash"), "needs jq and bash")
class SweeperScript(unittest.TestCase):
    """Run the workflow's script against a stub `gh` and a no-op `sleep`."""

    HEAD_SHA = "f" * 40

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="sweeper-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        (self.tmp / "bin").mkdir()
        (self.tmp / "state").mkdir()
        self.script = self.tmp / "sweep.sh"
        (job,) = yaml.safe_load((ROOT / ".github" / "workflows" / SWEEPER).read_text())["jobs"].values()
        self.script.write_text(job["steps"][0]["run"])
        gh = self.tmp / "bin" / "gh"
        gh.write_text(
            "#!/bin/sh\n"
            'echo "gh $*" >> "$STUB_DIR/calls"\n'
            'case "$1 $2" in\n'
            '  "pr list") [ -f "$STUB_DIR/list_fails" ] && exit 1; cat "$STUB_DIR/prs.json" ;;\n'
            '  "pr view") cat "$STUB_DIR/view_$3" ;;\n'
            '  "run list") f="$STUB_DIR/runs_${4}_${8}.json"; if [ -f "$f" ]; then cat "$f"; else echo "[]"; fi ;;\n'
            '  "run cancel") echo "$3" >> "$STUB_DIR/cancelled" ;;\n'
            '  *) exit 2 ;;\n'
            "esac\n"
        )
        sleep = self.tmp / "bin" / "sleep"
        sleep.write_text("#!/bin/sh\nexit 0\n")
        for p in (gh, sleep):
            p.chmod(p.stat().st_mode | stat.S_IXUSR)

    def sha(self, c):
        return c * 40

    def write(self, name, obj):
        (self.tmp / "state" / name).write_text(obj if isinstance(obj, str) else json.dumps(obj))

    def run_script(self, dry_run="false"):
        env = {
            "PATH": f"{self.tmp / 'bin'}:{os.environ['PATH']}",
            "STUB_DIR": str(self.tmp / "state"),
            "GITHUB_SHA": self.HEAD_SHA,
            "DRY_RUN": dry_run,
            "GH_TOKEN": "x",
            "GH_REPO": "o/r",
        }
        r = subprocess.run(["bash", str(self.script)], env=env, capture_output=True, text=True, timeout=60)
        cancelled_file = self.tmp / "state" / "cancelled"
        cancelled = cancelled_file.read_text().split() if cancelled_file.exists() else []
        return r, cancelled

    def scenario(self):
        pr = lambda n, c, m: {"number": n, "headRefOid": self.sha(c), "mergeable": m}
        self.write("prs.json", [
            pr(1, "1", "CONFLICTING"),   # conflicting: its pull_request runs are cancelled
            pr(2, "2", "MERGEABLE"),     # clean: left alone
            pr(3, "3", "UNKNOWN"),       # unknown, resolves to CONFLICTING while polling
            pr(4, "4", "UNKNOWN"),       # unknown for good: left alone (fail open)
            pr(5, "f", "CONFLICTING"),   # its head is the commit of the sweep itself: left alone
        ])
        run = lambda i, ev: {"databaseId": i, "event": ev}
        self.write(f"runs_{self.sha('1')}_queued.json", [run(101, "pull_request")])
        self.write(f"runs_{self.sha('1')}_in_progress.json", [run(102, "pull_request"), run(103, "push")])
        self.write(f"runs_{self.sha('2')}_in_progress.json", [run(201, "pull_request")])
        self.write(f"runs_{self.sha('3')}_waiting.json", [run(301, "pull_request")])
        self.write(f"runs_{self.sha('4')}_in_progress.json", [run(401, "pull_request")])
        self.write(f"runs_{self.sha('f')}_in_progress.json", [run(501, "pull_request")])
        self.write("view_3", "CONFLICTING")
        self.write("view_4", "UNKNOWN")

    def test_it_cancels_only_pull_request_runs_of_conflicting_heads(self):
        self.scenario()
        r, cancelled = self.run_script()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(sorted(cancelled), ["101", "102", "301"], r.stdout)

    def test_a_dry_run_cancels_nothing_and_says_what_it_would_do(self):
        self.scenario()
        r, cancelled = self.run_script(dry_run="true")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(cancelled, [])
        for i in ("101", "102", "301"):
            self.assertIn(f"would cancel run {i}", r.stdout)
        self.assertNotIn("run 103", r.stdout)

    def test_a_failing_pull_request_listing_is_fail_open(self):
        self.scenario()
        self.write("list_fails", "")
        r, cancelled = self.run_script()
        self.assertEqual(r.returncode, 0)
        self.assertEqual(cancelled, [])

    def test_no_open_pull_request_is_a_no_op(self):
        self.write("prs.json", [])
        r, cancelled = self.run_script()
        self.assertEqual((r.returncode, cancelled), (0, []))

    def test_a_run_that_cannot_be_cancelled_does_not_fail_the_sweep(self):
        self.scenario()
        gh = self.tmp / "bin" / "gh"
        gh.write_text(gh.read_text().replace('"run cancel") echo "$3" >> "$STUB_DIR/cancelled" ;;', '"run cancel") exit 1 ;;'))
        r, cancelled = self.run_script()
        self.assertEqual((r.returncode, cancelled), (0, []))
        self.assertIn("could not be cancelled", r.stdout)


if __name__ == "__main__":
    unittest.main()
