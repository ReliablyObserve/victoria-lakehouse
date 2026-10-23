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

    def test_every_allowlist_entry_has_a_reason(self):
        self.assertTrue(all(reason.strip() for reason in ID_TOKEN_ALLOWED.values()))


if __name__ == "__main__":
    unittest.main()
