"""CI scope wiring: the canonical job guard, the required checks it must never gate, and the action script.

A workflow job may be skipped by the CI scope in exactly one shape (the "canonical" `if:`), so that

  * an empty, missing or garbled scope output, a failed `scope` job and a failed classifier all run the job
    (`!= 'true'` skips only on an explicit `true`, and `!cancelled()` keeps the job alive when `scope` failed,
    because a job whose `needs` failed or was skipped is skipped otherwise);
  * a job listed in `force` (an edit to the job or to a script it runs) runs although its group is skipped;
  * no required status check is ever gated by a scope output (a skipped required job would read as passing).

This file lints every workflow for that, proves the lint can fail (fixtures), and runs the `run:` script of
the composite action .github/actions/ci-scope exactly as the runner does (`bash --noprofile --norc -eo pipefail`)
in temporary git repositories.

Run: python3 -m unittest scripts/ci/tests/test_ci_scope_workflows.py -v
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github" / "workflows"
ACTION = ROOT / ".github" / "actions" / "ci-scope" / "action.yml"
REQUIRED_FILE = pathlib.Path(__file__).resolve().parent / "testdata" / "required_checks.txt"

GROUPS = ("go", "image", "parity")
LOCAL_ACTION = "./.github/actions/ci-scope"
# any reference to a scope output in an expression
SCOPE_REF = re.compile(r"needs\.scope\b|outputs\.(?:skip_(?:go|image|parity)|force)\b")


def canonical_if(workflow_file: str, job: str, group: str = "go") -> str:
    """The only `if:` that lets a scope output skip a job."""
    return (
        f"${{{{ !cancelled() && (needs.scope.outputs.skip_{group} != 'true' || "
        f"contains(format(',{{0}},', needs.scope.outputs.force), ',{workflow_file}:{job},')) }}}}"
    )


def required_contexts() -> "set[str]":
    lines = REQUIRED_FILE.read_text(encoding="utf-8").splitlines()
    return {ln.strip() for ln in lines if ln.strip() and not ln.startswith("#")}


def context_base(name: str) -> str:
    """`build-logs (linux, amd64)` -> `build-logs`: a matrix leg is reported as `<name> (<leg>)`."""
    return name.split(" (", 1)[0]


def as_list(v) -> "list[str]":
    if v is None:
        return []
    return [v] if isinstance(v, str) else [str(x) for x in v]


def steps_of(job: dict) -> "list[dict]":
    return [s for s in job.get("steps") or [] if isinstance(s, dict)]


def lint_workflow(workflow_file: str, doc: dict, required: "set[str]") -> "list[str]":
    """One message per violation of the scope rules in a parsed workflow."""
    problems: "list[str]" = []
    required_bases = {context_base(c) for c in required}
    wf_perms = doc.get("permissions")
    for jid, job in (doc.get("jobs") or {}).items():
        where = f"{workflow_file}:{jid}"
        cond = job.get("if")
        cond_s = "" if cond is None else str(cond)
        needs = as_list(job.get("needs"))
        canon = {canonical_if(workflow_file, jid, g) for g in GROUPS}
        gated = SCOPE_REF.search(cond_s) is not None or "scope" in needs

        if cond_s in canon:
            if needs.count("scope") != 1:
                problems.append(f"{where}: the canonical if reads needs.scope but the job does not need `scope`")
        elif gated:
            if "scope" in needs and "!cancelled()" not in cond_s:
                problems.append(
                    f"{where}: needs `scope` without the `!cancelled()` guard "
                    "(a failed scope job would skip the job, and a skipped job reads as passing)"
                )
            else:
                problems.append(
                    f"{where}: a scope output is used in a non-canonical job `if:` ({cond_s!r}); expected exactly "
                    f"{canonical_if(workflow_file, jid)!r} (or the image/parity twin)"
                )

        # a scope output anywhere else in the job (a step `if:`, `env`, `with`, `run`) is not a supported shape
        rest = {k: v for k, v in job.items() if k != "if"}
        if re.search(r"needs\.scope\b", json.dumps(rest)):
            problems.append(f"{where}: needs.scope is used outside the job-level `if:`")
        for step in steps_of(job):
            if "if" in step and re.search(r"outputs\.skip_(?:go|image|parity)\b|needs\.scope\b", str(step["if"])):
                problems.append(f"{where}: a step `if:` reads a scope output ({step['if']!r})")

        # a required check always runs
        name = job.get("name", jid)
        if isinstance(name, str) and "${{" not in name and context_base(name) in required_bases and (
            "scope" in needs or SCOPE_REF.search(cond_s)
        ):
            problems.append(f"{where}: reports the required check `{name}` and must not be gated by the CI scope")

        # the scope job itself
        if jid == "scope" and cond is not None:
            problems.append(f"{where}: the scope job must always run (no `if:`)")

        # the way the action is called
        uses_action = [s for s in steps_of(job) if str(s.get("uses", "")).split("#")[0].strip() == LOCAL_ACTION]
        if uses_action:
            ss = steps_of(job)
            idx = ss.index(uses_action[0])
            checkout = [s for s in ss[:idx] if str(s.get("uses", "")).startswith("actions/checkout@")]
            if not checkout or str((checkout[-1].get("with") or {}).get("fetch-depth")) != "0":
                problems.append(f"{where}: the action needs a checkout with fetch-depth: 0 before it (merge base)")
            elif (checkout[-1].get("with") or {}).get("persist-credentials") is not False:
                problems.append(f"{where}: the checkout before the action must set persist-credentials: false")
            envs = [job.get("env") or {}, uses_action[0].get("env") or {}]
            if not any(e.get("CI_SCOPE") == "${{ vars.CI_SCOPE }}" for e in envs):
                problems.append(f"{where}: CI_SCOPE must be passed as `CI_SCOPE: ${{{{ vars.CI_SCOPE }}}}` in env")
            perms = job.get("permissions", wf_perms)
            if perms != {"contents": "read"}:
                problems.append(f"{where}: a job that computes the scope needs exactly `contents: read`")
            if "skip_go" not in (job.get("outputs") or {}) or "force" not in (job.get("outputs") or {}):
                problems.append(f"{where}: must expose skip_go and force as job outputs")
    return problems


def load_workflows() -> "dict[str, dict]":
    out = {}
    for p in sorted(list(WORKFLOWS.glob("*.yaml")) + list(WORKFLOWS.glob("*.yml"))):
        out[p.name] = yaml.safe_load(p.read_text(encoding="utf-8"))
    return out


# ---------------------------------------------------------------- the lint, on fixtures --

def wf(jobs: dict, **top) -> dict:
    return {"name": "x", "on": {"pull_request": None}, "permissions": {"contents": "read"}, "jobs": jobs, **top}


def scope_job() -> dict:
    return {
        "runs-on": "ubuntu-latest",
        "env": {"CI_SCOPE": "${{ vars.CI_SCOPE }}"},
        "outputs": {"skip_go": "${{ steps.s.outputs.skip_go }}", "force": "${{ steps.s.outputs.force }}"},
        "steps": [
            {"uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1", "with": {"fetch-depth": 0, "persist-credentials": False}},
            {"id": "s", "uses": LOCAL_ACTION},
        ],
    }


def heavy(cond=None, needs=("scope",), **extra) -> dict:
    job = {"runs-on": "ubuntu-latest", "steps": [{"run": "true"}]}
    if needs:
        job["needs"] = list(needs)
    if cond is not None:
        job["if"] = cond
    job.update(extra)
    return job


class LintFixtures(unittest.TestCase):
    REQ = {"test-logs", "build-logs (linux, amd64)"}

    def lint(self, jobs, **top):
        return lint_workflow("w.yaml", wf(jobs, **top), self.REQ)

    def good(self, jid="fuzz"):
        return heavy(canonical_if("w.yaml", jid))

    def test_canonical_form_passes(self):
        self.assertEqual(self.lint({"scope": scope_job(), "fuzz": self.good()}), [])

    def test_image_and_parity_twins_pass(self):
        jobs = {"scope": scope_job(), "a": heavy(canonical_if("w.yaml", "a", "image")), "b": heavy(canonical_if("w.yaml", "b", "parity"))}
        self.assertEqual(self.lint(jobs), [])

    def test_no_scope_usage_is_not_flagged(self):
        jobs = {"a": {"runs-on": "x", "steps": [{"run": "true"}]}, "b": {"runs-on": "x", "if": "github.event_name == 'push'", "steps": []}}
        self.assertEqual(self.lint(jobs), [])

    def test_positive_form_is_rejected(self):
        cond = canonical_if("w.yaml", "fuzz").replace("!= 'true'", "== 'false'")
        self.assertTrue(self.lint({"scope": scope_job(), "fuzz": heavy(cond)}))

    def test_always_instead_of_not_cancelled_is_rejected(self):
        cond = canonical_if("w.yaml", "fuzz").replace("!cancelled()", "always()")
        self.assertTrue(self.lint({"scope": scope_job(), "fuzz": heavy(cond)}))

    def test_missing_guard_is_rejected(self):
        cond = "${{ needs.scope.outputs.skip_go != 'true' }}"
        msg = self.lint({"scope": scope_job(), "fuzz": heavy(cond)})
        self.assertTrue(any("!cancelled()" in m for m in msg), msg)

    def test_needs_scope_without_any_if_is_rejected(self):
        msg = self.lint({"scope": scope_job(), "fuzz": heavy(None)})
        self.assertTrue(any("!cancelled()" in m for m in msg), msg)

    def test_force_check_without_the_commas_is_rejected(self):
        cond = canonical_if("w.yaml", "fuzz").replace(",w.yaml:fuzz,", "w.yaml:fuzz")
        self.assertTrue(self.lint({"scope": scope_job(), "fuzz": heavy(cond)}))

    def test_force_check_for_another_job_is_rejected(self):
        cond = canonical_if("w.yaml", "other")
        self.assertTrue(self.lint({"scope": scope_job(), "fuzz": heavy(cond)}))

    def test_force_check_dropped_is_rejected(self):
        cond = "${{ !cancelled() && needs.scope.outputs.skip_go != 'true' }}"
        self.assertTrue(self.lint({"scope": scope_job(), "fuzz": heavy(cond)}))

    def test_extra_condition_is_rejected(self):
        cond = canonical_if("w.yaml", "fuzz")[:-3] + " && github.actor != 'x' }}"
        self.assertTrue(self.lint({"scope": scope_job(), "fuzz": heavy(cond)}))

    def test_canonical_if_without_needs_is_rejected(self):
        self.assertTrue(self.lint({"scope": scope_job(), "fuzz": heavy(canonical_if("w.yaml", "fuzz"), needs=())}))

    def test_step_level_if_on_a_scope_output_is_rejected(self):
        job = self.good()
        job["steps"] = [{"if": "needs.scope.outputs.skip_go != 'true'", "run": "true"}]
        self.assertTrue(any("step" in m or "outside" in m for m in self.lint({"scope": scope_job(), "fuzz": job})))

    def test_scope_output_in_env_is_rejected(self):
        job = self.good()
        job["env"] = {"X": "${{ needs.scope.outputs.skip_go }}"}
        self.assertTrue(any("outside" in m for m in self.lint({"scope": scope_job(), "fuzz": job})))

    def test_required_check_gated_by_scope_is_rejected(self):
        jobs = {"scope": scope_job(), "test-logs": heavy(canonical_if("w.yaml", "test-logs"))}
        self.assertTrue(any("required check" in m for m in self.lint(jobs)))

    def test_required_check_by_name_is_rejected(self):
        job = heavy(canonical_if("w.yaml", "t"), name="test-logs")
        self.assertTrue(any("required check" in m for m in self.lint({"scope": scope_job(), "t": job})))

    def test_required_matrix_check_is_rejected(self):
        job = heavy(canonical_if("w.yaml", "build-logs"))
        job["strategy"] = {"matrix": {"os": ["linux"]}}
        self.assertTrue(any("required check" in m for m in self.lint({"scope": scope_job(), "build-logs": job})))

    def test_required_check_that_only_needs_scope_is_rejected(self):
        jobs = {"scope": scope_job(), "test-logs": heavy(None)}
        self.assertTrue(any("required check" in m for m in self.lint(jobs)))

    def test_required_check_without_scope_passes(self):
        jobs = {"test-logs": {"runs-on": "x", "steps": [{"run": "true"}]}}
        self.assertEqual(self.lint(jobs), [])

    def test_scope_job_must_not_be_conditional(self):
        sj = scope_job()
        sj["if"] = "github.event_name == 'pull_request'"
        self.assertTrue(any("always run" in m for m in self.lint({"scope": sj})))

    def test_shallow_checkout_is_rejected(self):
        sj = scope_job()
        sj["steps"][0]["with"]["fetch-depth"] = 1
        self.assertTrue(any("fetch-depth" in m for m in self.lint({"scope": sj})))

    def test_credentials_must_not_persist(self):
        sj = scope_job()
        del sj["steps"][0]["with"]["persist-credentials"]
        self.assertTrue(any("persist-credentials" in m for m in self.lint({"scope": sj})))

    def test_override_variable_must_be_passed_through_env(self):
        sj = scope_job()
        del sj["env"]
        self.assertTrue(any("CI_SCOPE" in m for m in self.lint({"scope": sj})))

    def test_scope_job_needs_read_only_contents(self):
        sj = scope_job()
        sj["permissions"] = {"contents": "write"}
        self.assertTrue(any("contents: read" in m for m in self.lint({"scope": sj})))
        self.assertTrue(any("contents: read" in m for m in self.lint({"scope": scope_job()}, permissions={"contents": "write"})))

    def test_scope_job_must_expose_its_outputs(self):
        sj = scope_job()
        del sj["outputs"]["force"]
        self.assertTrue(any("outputs" in m for m in self.lint({"scope": sj})))


# ---------------------------------------------------------------- the repository --

class RepositoryWiring(unittest.TestCase):
    # every job of the fuzz/stress/memleak workflow the scope may skip; fuzz-matrix-drift always runs
    FUZZ_GATED = ("memleak-logs", "memleak-traces", "stress-logs", "stress-traces", "fuzz-logs", "fuzz-traces")

    def test_required_checks_file_is_the_protection_rule_size(self):
        self.assertEqual(len(required_contexts()), 21)

    def test_every_workflow_follows_the_scope_rules(self):
        problems = []
        req = required_contexts()
        for name, doc in load_workflows().items():
            problems += lint_workflow(name, doc, req)
        self.assertEqual(problems, [], "\n" + "\n".join(problems))

    def test_fuzz_workflow_gates_exactly_its_heavy_jobs(self):
        doc = load_workflows()["fuzz-stress-memleak.yaml"]
        jobs = doc["jobs"]
        self.assertIn("scope", jobs)
        for jid in self.FUZZ_GATED:
            self.assertEqual(jobs[jid].get("if"), canonical_if("fuzz-stress-memleak.yaml", jid), jid)
            self.assertEqual(as_list(jobs[jid].get("needs")), ["scope"], jid)
        self.assertEqual(sorted(j for j in jobs if j not in self.FUZZ_GATED), ["fuzz-matrix-drift", "scope"])
        self.assertNotIn("needs", jobs["fuzz-matrix-drift"])
        self.assertNotIn("if", jobs["fuzz-matrix-drift"])

    def test_fuzz_workflow_keeps_its_triggers_and_job_names(self):
        doc = load_workflows()["fuzz-stress-memleak.yaml"]
        on = doc.get("on", doc.get(True))
        self.assertEqual(sorted(on), ["pull_request", "push", "schedule", "workflow_dispatch"])
        self.assertIsNone(on["pull_request"])  # no path or branch filter: a skipped workflow would leave checks pending
        for jid in self.FUZZ_GATED:
            self.assertNotIn("name", doc["jobs"][jid], jid)

    def test_scope_job_has_a_timeout(self):
        self.assertLessEqual(load_workflows()["fuzz-stress-memleak.yaml"]["jobs"]["scope"]["timeout-minutes"], 10)

    def test_every_job_of_a_workflow_that_forces_is_named_in_the_classifier_vocabulary(self):
        # `force` entries are <workflow file>:<job id>; the ids used in canonical ifs must be real job ids
        for name, doc in load_workflows().items():
            for jid, job in doc["jobs"].items():
                if SCOPE_REF.search(str(job.get("if", ""))):
                    self.assertEqual(job["if"], canonical_if(name, jid), f"{name}:{jid}")


class ActionDefinition(unittest.TestCase):
    def setUp(self):
        self.doc = yaml.safe_load(ACTION.read_text(encoding="utf-8"))

    def test_composite_with_one_script_step(self):
        self.assertEqual(self.doc["runs"]["using"], "composite")
        self.assertEqual(len(self.doc["runs"]["steps"]), 1)
        self.assertEqual(self.doc["runs"]["steps"][0]["shell"], "bash")

    def test_no_expression_in_the_script(self):
        # inputs and the override variable reach the script as environment variables only
        self.assertNotIn("${{", self.doc["runs"]["steps"][0]["run"])

    def test_action_has_no_external_dependency_and_does_not_check_out(self):
        text = ACTION.read_text(encoding="utf-8")
        self.assertNotRegex(text, r"(?m)^\s*-?\s*uses:")

    def test_outputs_are_the_scope_outputs(self):
        self.assertEqual(sorted(self.doc["outputs"]), ["force", "skip_go", "skip_image", "skip_parity"])
        for k, v in self.doc["outputs"].items():
            self.assertEqual(v["value"], "${{ steps.scope.outputs.%s }}" % k)

    def test_script_passes_the_event_to_the_classifier(self):
        run = self.doc["runs"]["steps"][0]["run"]
        self.assertRegex(run, r'--ci-scope --event "\$EVENT"')

    def test_script_has_no_unquoted_override_read(self):
        run = self.doc["runs"]["steps"][0]["run"]
        self.assertIn('"${CI_SCOPE:-}" != full', run)


# ---------------------------------------------------------------- the action script --

SCRIPT = yaml.safe_load(ACTION.read_text(encoding="utf-8"))["runs"]["steps"][0]["run"]
FULL = {"skip_go": "false", "skip_image": "false", "skip_parity": "false", "force": ""}


class ActionScript(unittest.TestCase):
    """Runs the action's script the way the runner does, in a temporary repository."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ci-scope-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.repo = os.path.join(self.tmp, "repo")
        os.makedirs(self.repo)
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull,
                        GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@example.invalid",
                        GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@example.invalid")
        self.git("init", "-q", "-b", "main")
        self.git("config", "commit.gpgsign", "false")

    def git(self, *args):
        r = subprocess.run(["git", "-C", self.repo, *args], capture_output=True, text=True, env=self.env)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.strip()

    def write(self, rel, text):
        p = os.path.join(self.repo, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        pathlib.Path(p).write_text(text, encoding="utf-8")

    def commit(self, msg):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", msg)
        return self.git("rev-parse", "HEAD")

    def install_classifier(self):
        for f in ("pr_classify.py", "check_changelog_pr.py"):
            self.write(f"scripts/ci/{f}", (ROOT / "scripts" / "ci" / f).read_text(encoding="utf-8"))
        self.write(".github/release-metadata-approvers", "")

    def base(self, classifier=True, extra=None):
        self.write("README.md", "base\n")
        if classifier:
            self.install_classifier()
        for rel, text in (extra or {}).items():
            self.write(rel, text)
        return self.commit("base")

    def run_script(self, base_sha, event="pull_request", ci_scope=None, author="someone"):
        out, summary = os.path.join(self.tmp, "out"), os.path.join(self.tmp, "summary")
        for p in (out, summary):
            pathlib.Path(p).write_text("")
        script = os.path.join(self.tmp, "script.sh")
        pathlib.Path(script).write_text(SCRIPT, encoding="utf-8")
        env = dict(self.env, GITHUB_OUTPUT=out, GITHUB_STEP_SUMMARY=summary, RUNNER_TEMP=os.path.join(self.tmp, "runner"),
                   EVENT=event, BASE_SHA=base_sha, AUTHOR=author)
        env.pop("CI_SCOPE", None)
        if ci_scope is not None:
            env["CI_SCOPE"] = ci_scope
        os.makedirs(env["RUNNER_TEMP"], exist_ok=True)
        r = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", script], cwd=self.repo,
                           capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)  # the script never fails the job
        outputs = dict(ln.split("=", 1) for ln in pathlib.Path(out).read_text().splitlines())
        return outputs, pathlib.Path(summary).read_text()

    # --- scoped

    def test_docs_only_pull_request_is_scoped(self):
        b = self.base()
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        out, summary = self.run_script(b)
        self.assertEqual(out, {"skip_go": "true", "skip_image": "true", "skip_parity": "true", "force": ""})
        self.assertIn("### CI scope", summary)
        self.assertIn("ci_class=docs", summary)

    def test_product_pull_request_runs_everything(self):
        b = self.base()
        self.write("internal/x/x.go", "package x\n")
        self.commit("go")
        self.assertEqual(self.run_script(b)[0], FULL)

    def test_mixed_pull_request_runs_everything(self):
        b = self.base()
        self.write("docs/x.md", "x\n")
        self.write("internal/x/x.go", "package x\n")
        self.commit("both")
        self.assertEqual(self.run_script(b)[0], FULL)

    def test_force_list_is_passed_through(self):
        wf_text = (WORKFLOWS / "fuzz-stress-memleak.yaml").read_text(encoding="utf-8")
        b = self.base(extra={".github/workflows/fuzz-stress-memleak.yaml": wf_text})
        marker = "  fuzz-logs:\n    needs: [scope]\n"
        self.assertIn(marker, wf_text)
        i = wf_text.index(marker)
        j = wf_text.index("timeout-minutes: 45", i)
        self.write(".github/workflows/fuzz-stress-memleak.yaml", wf_text[:j] + "timeout-minutes: 46" + wf_text[j + len("timeout-minutes: 45"):])
        self.commit("edit a job")
        out, _ = self.run_script(b)
        self.assertEqual(out["skip_go"], "true")
        self.assertEqual(out["force"], "fuzz-stress-memleak.yaml:fuzz-logs")

    def test_the_merge_base_classifier_decides_not_the_pull_request(self):
        b = self.base()
        # the pull request replaces the classifier by one that says "skip everything" and changes Go code
        self.write("scripts/ci/pr_classify.py", "print('skip_go=true')\nprint('skip_image=true')\nprint('skip_parity=true')\nprint('force=')\n")
        self.write("internal/x/x.go", "package x\n")
        self.commit("tamper")
        self.assertEqual(self.run_script(b)[0], FULL)

    def test_other_ci_scope_value_does_not_override(self):
        b = self.base()
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        self.assertEqual(self.run_script(b, ci_scope="")[0]["skip_go"], "true")
        self.assertEqual(self.run_script(b, ci_scope="yes")[0]["skip_go"], "true")

    # --- full run by design

    def test_events_other_than_pull_request_run_everything(self):
        b = self.base()
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        for event in ("push", "schedule", "workflow_dispatch", "pull_request_target", "merge_group", ""):
            with self.subTest(event=event):
                out, summary = self.run_script(b, event=event)
                self.assertEqual(out, FULL)
                self.assertIn("full run", summary)

    def test_repository_variable_forces_a_full_run(self):
        b = self.base()
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        out, summary = self.run_script(b, ci_scope="full")
        self.assertEqual(out, FULL)
        self.assertIn("CI_SCOPE=full", summary)

    # --- fail open

    def test_unknown_base_commit_runs_everything(self):
        self.base()
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        self.assertEqual(self.run_script("0123456789abcdef0123456789abcdef01234567")[0], FULL)

    def test_empty_base_commit_runs_everything(self):
        self.base()
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        self.assertEqual(self.run_script("")[0], FULL)

    def test_unrelated_history_has_no_merge_base(self):
        b = self.base()
        self.git("checkout", "-q", "--orphan", "other")
        self.write("docs/y.md", "y\n")
        self.commit("orphan")
        self.assertEqual(self.run_script(b)[0], FULL)

    def test_base_without_a_classifier_runs_everything(self):
        b = self.base(classifier=False)
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        self.assertEqual(self.run_script(b)[0], FULL)

    def test_base_classifier_that_predates_the_flag_runs_everything(self):
        old = "import argparse\nap = argparse.ArgumentParser()\nap.add_argument('--base')\nap.add_argument('--head')\nap.add_argument('--author')\nap.add_argument('--approvers')\nap.parse_args()\nprint('exempt=none')\nprint('product=0')\n"
        b = self.base(classifier=False, extra={"scripts/ci/pr_classify.py": old})
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        self.assertEqual(self.run_script(b)[0], FULL)

    def test_classifier_that_crashes_runs_everything(self):
        b = self.base(classifier=False, extra={"scripts/ci/pr_classify.py": "raise SystemExit(3)\n"})
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        self.assertEqual(self.run_script(b)[0], FULL)

    def test_garbled_classifier_output_runs_everything(self):
        cases = {
            "bad value": "skip_go=maybe\nskip_image=true\nskip_parity=true\nforce=\n",
            "missing key": "skip_go=true\nskip_image=true\nforce=\n",
            "duplicated key": "skip_go=true\nskip_go=true\nskip_image=true\nskip_parity=true\nforce=\n",
            "empty value": "skip_go=\nskip_image=true\nskip_parity=true\nforce=\n",
            "bad force": "skip_go=true\nskip_image=true\nskip_parity=true\nforce=a b;c\n",
            "no output": "",
        }
        for label, body in cases.items():
            with self.subTest(label):
                self.tmp_repo_reset()
                script = "import sys\nsys.stdout.write(%r)\n" % body
                b = self.base(classifier=False, extra={"scripts/ci/pr_classify.py": script})
                self.write("docs/x.md", "x\n")
                self.commit("docs")
                self.assertEqual(self.run_script(b)[0], FULL)

    def tmp_repo_reset(self):
        shutil.rmtree(self.repo)
        os.makedirs(self.repo)
        self.git("init", "-q", "-b", "main")
        self.git("config", "commit.gpgsign", "false")

    def test_scope_is_taken_from_the_checkout_head_not_the_base_sha_alone(self):
        # HEAD is a merge commit of the pull request into the base, as actions/checkout leaves it
        b = self.base()
        self.git("checkout", "-q", "-b", "pr")
        self.write("docs/x.md", "x\n")
        self.commit("docs")
        self.git("checkout", "-q", "main")
        self.write("internal/y/y.go", "package y\n")
        main_tip = self.commit("main moved")
        self.git("checkout", "-q", "-b", "merge")
        self.git("merge", "-q", "--no-ff", "-m", "merge", "pr")
        out, _ = self.run_script(main_tip)
        self.assertEqual(out["skip_go"], "true")  # only the pull request's own change counts
        out, _ = self.run_script(b)  # a stale base SHA still resolves the merge base
        self.assertEqual(out["skip_go"], "false")  # the base moved on: main's Go change is in the diff, so run all


if __name__ == "__main__":
    unittest.main()
