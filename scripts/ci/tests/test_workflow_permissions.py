"""Static check: every workflow states its token permissions, and only reviewed jobs can mint an OIDC token.

Scans .github/workflows/*.y*ml. It fails on

  no-permissions  a workflow with no `permissions:` at the workflow level and a job without its own
                  (the job would run with the repository default token, which can be read/write)
  write-all       `permissions: write-all` at the workflow or job level
  id-token        `id-token: write` in a workflow that is not in ID_TOKEN_ALLOWED below. An OIDC token lets a
                  job prove its identity to an external service (publish a result, deploy, sign), so each
                  workflow that may mint one is listed here with the reason. Add the future attestation and
                  signing jobs to the list when they land, in the same change.

An ID_TOKEN_ALLOWED entry whose workflow no longer holds `id-token: write` fails the test, so the list
cannot rot.

A workflow that runs on `workflow_run` executes base-branch code with a token the triggering pull request cannot
see, and may consume that pull request's artifacts. It is held to a stricter contract:

  run-top-level   the workflow-level `permissions:` is not exactly `{}`
  run-unlisted    the workflow is not in WORKFLOW_RUN_JOB_PERMISSIONS, or a job is not listed for it
  run-job-perms   a job's `permissions:` is not exactly the reviewed entry (any new scope, or any widening, fails)
  run-checkout    an `actions/checkout` step has `ref`, `repository` or `token` (it could fetch pull request code)

Every WORKFLOW_RUN_JOB_PERMISSIONS entry must still match a workflow, a job and its permissions, so the list cannot rot.

Run: python3 -m unittest discover -s scripts/ci/tests -t . -p test_workflow_permissions.py -v
"""
import pathlib
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]

ID_TOKEN_ALLOWED = {
    "scorecard.yaml": "publishes the OpenSSF Scorecard result to the public Scorecard API (OIDC proves the workflow identity)",
    "docs-site.yaml": "GitHub Pages deployment (actions/deploy-pages requires id-token: write)",
}


# Reviewed permissions of every job of every `workflow_run` workflow (the exact dict; nothing else is allowed).
WORKFLOW_RUN_JOB_PERMISSIONS = {
    "pr-proof-comment.yaml": {
        # reads the producer's artifact and the PR list; no write scope
        "validate": {"actions": "read", "contents": "read", "pull-requests": "read"},
        # pushes validated PNG files to the data-only pr-visuals branch (D10: github-actions[bot], unsigned)
        "publish": {"contents": "write", "pull-requests": "read"},
        # upserts the sticky PR comments (issues: write is not needed for PR comments)
        "comment": {"contents": "read", "pull-requests": "write"},
        # workflow_dispatch fixture dry run: cannot write
        "dry-run": {"contents": "read"},
    },
}


# `workflow_run` workflows that exist for the default branch only (`branches: [main]`), consume no pull request
# artifact and were reviewed before this contract. They keep their own permissions; a new entry needs a reason here.
WORKFLOW_RUN_MAIN_ONLY = {
    "badges.yaml": "follows the CI run on main only (workflow_run branches: [main]) and commits the generated badges; no pull request data",
}


def workflow_files(root: pathlib.Path):
    wf = root / ".github" / "workflows"
    return sorted(list(wf.glob("*.yaml")) + list(wf.glob("*.yml")))


def _grants(perms, name):
    """True when a `permissions:` value grants `name: write` (or write-all, which grants everything)."""
    if perms == "write-all":
        return True
    return isinstance(perms, dict) and perms.get(name) == "write"


def check_workflow(name: str, doc: dict) -> list:
    """Return (rule, message) for each violation in the parsed workflow `doc` of file `name`."""
    found = []
    wf_perms = doc.get("permissions")
    jobs = doc.get("jobs") or {}
    if wf_perms is None:
        for job_id, job in sorted(jobs.items()):
            if "permissions" not in (job or {}):
                found.append(("no-permissions", f"{name}: job `{job_id}` has no `permissions:` and the workflow has none"))
    scopes = [("workflow", wf_perms)] + [(f"job `{j}`", (v or {}).get("permissions")) for j, v in sorted(jobs.items())]
    for where, perms in scopes:
        if perms == "write-all":
            found.append(("write-all", f"{name}: {where} sets `permissions: write-all`"))
    if any(_grants(perms, "id-token") for _, perms in scopes) and name not in ID_TOKEN_ALLOWED:
        found.append(("id-token", f"{name}: grants `id-token: write` but is not in ID_TOKEN_ALLOWED (with a reason)"))
    return found + check_workflow_run(name, doc)


def runs_on_workflow_run(doc: dict) -> bool:
    on = doc.get("on", doc.get(True))  # PyYAML reads the bare key `on` as the boolean True
    if isinstance(on, str):
        return on == "workflow_run"
    return "workflow_run" in (on or {})


def check_workflow_run(name: str, doc: dict) -> list:
    """The stricter contract of a `workflow_run` workflow (see the module docstring)."""
    if not runs_on_workflow_run(doc) or name in WORKFLOW_RUN_MAIN_ONLY:
        return []
    found = []
    if doc.get("permissions") != {}:
        found.append(("run-top-level", f"{name}: a workflow_run workflow must set `permissions: {{}}` at the top"))
    jobs = doc.get("jobs") or {}
    reviewed = WORKFLOW_RUN_JOB_PERMISSIONS.get(name)
    if reviewed is None:
        found.append(("run-unlisted", f"{name}: runs on workflow_run but is not in WORKFLOW_RUN_JOB_PERMISSIONS"))
        reviewed = {}
    for job_id, job in sorted(jobs.items()):
        job = job or {}
        if job_id not in reviewed:
            if name in WORKFLOW_RUN_JOB_PERMISSIONS:
                found.append(("run-unlisted", f"{name}: job `{job_id}` is not in WORKFLOW_RUN_JOB_PERMISSIONS"))
        elif job.get("permissions") != reviewed[job_id]:
            found.append(("run-job-perms", f"{name}: job `{job_id}` has permissions {job.get('permissions')!r}, reviewed {reviewed[job_id]!r}"))
        for step in job.get("steps") or []:
            if str(step.get("uses", "")).startswith("actions/checkout@") and set(step.get("with") or {}) & {"ref", "repository", "token"}:
                found.append(("run-checkout", f"{name}: job `{job_id}` checks out with ref/repository/token"))
    return found


def minters(name: str, doc: dict) -> bool:
    jobs = doc.get("jobs") or {}
    return _grants(doc.get("permissions"), "id-token") or any(_grants((j or {}).get("permissions"), "id-token") for j in jobs.values())


class CheckWorkflowFixtures(unittest.TestCase):
    """The checker itself: it must be able to fail."""

    def parse(self, text):
        return yaml.safe_load(text)

    def test_workflow_level_permissions_cover_all_jobs(self):
        doc = self.parse("permissions:\n  contents: read\njobs:\n  a:\n    runs-on: x\n")
        self.assertEqual(check_workflow("w.yaml", doc), [])

    def test_job_level_permissions_cover_their_job(self):
        doc = self.parse("jobs:\n  a:\n    permissions:\n      contents: read\n    runs-on: x\n")
        self.assertEqual(check_workflow("w.yaml", doc), [])

    def test_missing_permissions_is_rejected(self):
        doc = self.parse("jobs:\n  a:\n    runs-on: x\n")
        self.assertEqual([r for r, _ in check_workflow("w.yaml", doc)], ["no-permissions"])

    def test_one_job_without_permissions_is_rejected(self):
        doc = self.parse("jobs:\n  a:\n    permissions: {contents: read}\n    runs-on: x\n  b:\n    runs-on: x\n")
        self.assertEqual([r for r, _ in check_workflow("w.yaml", doc)], ["no-permissions"])

    def test_empty_permissions_is_explicit(self):
        doc = self.parse("permissions: {}\njobs:\n  a:\n    runs-on: x\n")
        self.assertEqual(check_workflow("w.yaml", doc), [])

    def test_write_all_is_rejected(self):
        doc = self.parse("permissions: write-all\njobs:\n  a:\n    runs-on: x\n")
        self.assertIn("write-all", [r for r, _ in check_workflow("w.yaml", doc)])

    def test_id_token_in_an_unlisted_workflow_is_rejected(self):
        doc = self.parse("permissions:\n  contents: read\njobs:\n  a:\n    permissions:\n      id-token: write\n    runs-on: x\n")
        self.assertEqual([r for r, _ in check_workflow("w.yaml", doc)], ["id-token"])

    def test_workflow_level_id_token_in_an_unlisted_workflow_is_rejected(self):
        doc = self.parse("permissions:\n  id-token: write\njobs:\n  a:\n    runs-on: x\n")
        self.assertEqual([r for r, _ in check_workflow("w.yaml", doc)], ["id-token"])

    def test_id_token_read_is_not_a_grant(self):
        doc = self.parse("permissions:\n  id-token: none\njobs:\n  a:\n    runs-on: x\n")
        self.assertEqual(check_workflow("w.yaml", doc), [])

    def test_listed_workflow_may_hold_id_token(self):
        doc = self.parse("permissions: read-all\njobs:\n  a:\n    permissions:\n      id-token: write\n    runs-on: x\n")
        self.assertEqual(check_workflow("scorecard.yaml", doc), [])


class WorkflowRunFixtures(unittest.TestCase):
    """The workflow_run contract must be able to fail."""

    NAME = "pr-proof-comment.yaml"

    def doc(self, **job_perms):
        jobs = {j: {"runs-on": "x", "permissions": dict(p), "steps": []} for j, p in WORKFLOW_RUN_JOB_PERMISSIONS[self.NAME].items()}
        for j, p in job_perms.items():
            jobs[j]["permissions"] = p
        return {"on": {"workflow_run": {"workflows": ["a"]}}, "permissions": {}, "jobs": jobs}

    def rules(self, doc, name=None):
        return sorted(r for r, _ in check_workflow(name or self.NAME, doc))

    def test_the_reviewed_shape_passes(self):
        self.assertEqual(self.rules(self.doc()), [])

    def test_a_widened_job_is_rejected(self):
        self.assertEqual(self.rules(self.doc(comment={"contents": "read", "pull-requests": "write", "issues": "write"})), ["run-job-perms"])
        self.assertEqual(self.rules(self.doc(validate={"actions": "read", "contents": "write", "pull-requests": "read"})), ["run-job-perms"])
        self.assertEqual(self.rules(self.doc(**{"dry-run": {"contents": "read", "pull-requests": "write"}})), ["run-job-perms"])

    def test_a_job_with_no_permissions_is_rejected(self):
        d = self.doc()
        del d["jobs"]["comment"]["permissions"]
        self.assertEqual(self.rules(d), ["run-job-perms"])

    def test_a_missing_top_level_empty_permissions_is_rejected(self):
        d = self.doc()
        d["permissions"] = {"contents": "read"}
        self.assertEqual(self.rules(d), ["run-top-level"])
        del d["permissions"]
        self.assertEqual(self.rules(d), ["run-top-level"])

    def test_a_new_job_or_a_new_workflow_is_rejected(self):
        d = self.doc()
        d["jobs"]["extra"] = {"runs-on": "x", "permissions": {"contents": "read"}}
        self.assertEqual(self.rules(d), ["run-unlisted"])
        self.assertEqual(self.rules(self.doc(), name="other.yaml"), ["run-unlisted"])

    def test_a_checkout_of_another_ref_is_rejected(self):
        for key in ("ref", "repository", "token"):
            d = self.doc()
            d["jobs"]["validate"]["steps"] = [{"uses": "actions/checkout@" + "0" * 40, "with": {key: "x"}}]
            self.assertEqual(self.rules(d), ["run-checkout"], key)

    def test_a_workflow_without_workflow_run_is_not_affected(self):
        self.assertEqual(self.rules({"on": {"pull_request": None}, "permissions": {"contents": "read"}, "jobs": {}}, name="x.yaml"), [])

    def test_a_string_trigger_is_recognised(self):
        self.assertEqual(self.rules({"on": "workflow_run", "permissions": {}, "jobs": {}}, name="x.yaml"), ["run-unlisted"])


class RepositoryWorkflows(unittest.TestCase):
    def docs(self):
        return {p.name: yaml.safe_load(p.read_text()) for p in workflow_files(ROOT)}

    def test_every_workflow_passes(self):
        problems = []
        for name, doc in self.docs().items():
            problems += [msg for _, msg in check_workflow(name, doc)]
        self.assertEqual(problems, [], "\n".join(problems))

    def test_the_id_token_allowlist_has_no_stale_entries(self):
        docs = self.docs()
        stale = [n for n in ID_TOKEN_ALLOWED if n not in docs or not minters(n, docs[n])]
        self.assertEqual(stale, [], f"ID_TOKEN_ALLOWED entries that no longer hold id-token: write: {stale}")

    def test_the_workflow_run_list_has_no_stale_entries(self):
        docs = self.docs()
        stale = []
        for name, jobs in WORKFLOW_RUN_JOB_PERMISSIONS.items():
            doc = docs.get(name)
            if doc is None or not runs_on_workflow_run(doc):
                stale.append(name)
                continue
            stale += [f"{name}:{j}" for j in jobs if j not in (doc.get("jobs") or {})]
        self.assertEqual(stale, [], f"WORKFLOW_RUN_JOB_PERMISSIONS entries without a matching workflow_run job: {stale}")

    def test_every_workflow_run_workflow_is_listed(self):
        unlisted = [n for n, d in self.docs().items()
                    if runs_on_workflow_run(d) and n not in WORKFLOW_RUN_JOB_PERMISSIONS and n not in WORKFLOW_RUN_MAIN_ONLY]
        self.assertEqual(unlisted, [])

    def test_the_main_only_exemptions_are_real_and_restricted_to_main(self):
        docs = self.docs()
        for name, reason in WORKFLOW_RUN_MAIN_ONLY.items():
            self.assertTrue(reason.strip())
            self.assertIn(name, docs)
            on = docs[name].get("on", docs[name].get(True))
            self.assertEqual(on["workflow_run"].get("branches"), ["main"], f"{name} must only follow runs of main")

    def test_every_allowlist_entry_has_a_reason(self):
        self.assertTrue(all(reason.strip() for reason in ID_TOKEN_ALLOWED.values()))


if __name__ == "__main__":
    unittest.main()
