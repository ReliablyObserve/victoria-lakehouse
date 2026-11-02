"""The shape of the API matrix part of .github/workflows/pr-proof.yaml: read-only, informational, always reporting, in step with api_proof.py and pr_smoke.py."""

import os
import re

import pytest
import yaml

import api_proof
import pr_smoke
from scripts.ci import proof_comment as pc

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
PATH = os.path.join(ROOT, ".github", "workflows", "pr-proof.yaml")


def workflow_text():
    with open(PATH, encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="module")
def wf():
    with open(PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def steps(job):
    return [s for s in job["steps"] if isinstance(s, dict)]


def step(job, name):
    return next(s for s in steps(job) if s.get("name") == name)


def test_the_name_and_path_are_the_ones_the_commenter_accepts(wf):
    assert wf["name"] == "PR proof" and ".github/workflows/pr-proof.yaml" in pc.PRODUCERS
    assert "api-matrix" in pc.PRODUCERS[".github/workflows/pr-proof.yaml"]


def test_it_runs_on_every_pull_request_without_a_paths_filter_so_the_check_can_become_required(wf):
    on = wf.get("on", wf.get(True))
    assert set(on) == {"pull_request", "workflow_dispatch"}
    assert on["pull_request"] == {"branches": ["main"]}, "a required check must never stay Pending: no paths filter; the scope is decided inside"


def test_nothing_can_write_and_no_job_gets_a_secret(wf):
    assert wf["permissions"] == {}
    for jid, job in wf["jobs"].items():
        assert job["permissions"] in ({}, {"contents": "read"}), jid
    with open(PATH, encoding="utf-8") as f:
        text = f.read()
    assert not re.search(r"^\s*[\w-]+:\s*write\b", text, re.M) and "write-all" not in text and "secrets." not in text and "pull_request_target" not in text


def test_no_run_script_interpolates_an_expression(wf):
    for jid, job in wf["jobs"].items():
        for s in steps(job):
            assert "${{" not in s.get("run", ""), f"{jid}: {s.get('name')}"


def test_every_action_is_pinned_by_full_sha_and_every_checkout_drops_the_token(wf):
    for job in wf["jobs"].values():
        for s in steps(job):
            if "uses" in s:
                assert re.fullmatch(r"[\w./-]+@[0-9a-f]{40}", s["uses"]), s["uses"]
            if str(s.get("uses", "")).startswith("actions/checkout@"):
                assert s["with"]["persist-credentials"] is False


def test_the_scope_gates_the_heavy_jobs_and_the_api_job_always_runs_on_a_pull_request(wf):
    jobs = wf["jobs"]
    assert jobs["disk-probe"]["if"] == jobs["runner-fit"]["if"] == "${{ needs.plan.outputs.runner_fit == 'true' }}"
    assert jobs["api"]["if"] == "${{ !cancelled() && github.event_name == 'pull_request' }}"
    assert set(jobs["plan"]["outputs"]) == {"matrix", "api", "runner_fit"}
    scope = step(jobs["plan"], "Scope")
    assert "api_proof.py scope --event \"$EVENT\"" in scope["run"] and scope["env"]["EVENT"] == "${{ github.event_name }}"
    heavy = [s for s in steps(jobs["api"]) if s.get("name") in ("Test the harness", "Prepare the base", "Up, seed and API matrix", "Install the harness dependencies")]
    assert len(heavy) == 4 and all(s["if"] == "${{ env.SCOPE == 'true' }}" for s in heavy)


def test_the_comment_artifact_is_written_and_uploaded_on_every_outcome_but_a_cancel(wf):
    job = wf["jobs"]["api"]
    bundle = step(job, "Write the comment artifact")
    assert bundle["if"] == "${{ !cancelled() }}"
    assert "api_proof.py bundle --out-dir lh-proof-comment --data-dir api-matrix-data" in bundle["run"]
    assert bundle["env"]["HEAD_SHA"] == "${{ github.event.pull_request.head.sha }}" and bundle["env"]["PR_NUMBER"] == "${{ github.event.pull_request.number }}"
    ups = {s["with"]["name"]: s for s in steps(job) if str(s.get("uses", "")).startswith("actions/upload-artifact@")}
    assert set(ups) == {pc.ARTIFACT_NAME, "api-matrix-data"} == {"lh-proof-comment", "api-matrix-data"}
    assert ups["lh-proof-comment"]["with"]["path"] == "lh-proof-comment/"
    for s in ups.values():
        assert s["if"] == "${{ !cancelled() }}" and s["with"]["if-no-files-found"] == "ignore"


def help_of(main, capsys, *args):
    with pytest.raises(SystemExit):
        main([*args, "--help"])
    return capsys.readouterr().out


def test_the_scripts_are_called_with_options_they_have(wf, capsys):
    job = wf["jobs"]["api"]
    matrix = step(job, "Up, seed and API matrix")["run"]
    assert "pr_smoke.py --api-only" in matrix and "set +e" in matrix and 'echo "rc=$rc" >> "$GITHUB_OUTPUT"' in matrix and matrix.rstrip().endswith("exit 0")
    smoke_help = help_of(pr_smoke.main, capsys)
    for o in re.findall(r"(?<![\w-])(--[a-z][a-z-]*)", matrix.split("pr_smoke.py", 1)[1]):
        assert o in smoke_help, o
    bundle = step(job, "Write the comment artifact")["run"].replace("\\\n", " ")
    bundle_help = help_of(api_proof.main, capsys, "bundle")
    for o in re.findall(r"(?<![\w-])(--[a-z][a-z-]*)", bundle.split("api_proof.py bundle", 1)[1].split("\n", 1)[0]):
        assert o in bundle_help, o
    scope_help = help_of(api_proof.main, capsys, "scope")
    for o in re.findall(r"(?<![\w-])(--[a-z][a-z-]*)", step(wf["jobs"]["plan"], "Scope")["run"].split("api_proof.py", 1)[1]):
        assert o in scope_help, o


def test_the_project_name_and_the_scratch_path_are_the_reviewed_ones(wf):
    job = wf["jobs"]["api"]
    assert job["env"]["AB_PROJECT"] == "lh-apimx"
    assert step(job, "Choose the scratch paths")["run"] == 'echo "OUT=$RUNNER_TEMP/api-matrix" >> "$GITHUB_ENV"'  # not /tmp: a RAM-backed tmpfs on the runner
    text = workflow_text().replace("never a prune", "").replace("no prune", "")
    assert "stack.py down" in step(job, "Teardown")["run"] and "prune" not in text


def test_the_gate_reports_on_every_pull_request_and_maps_the_exit_codes(wf):
    gate = wf["jobs"]["gate-deterministic"]
    assert gate["name"] == "gate-deterministic" and gate["needs"] == ["plan", "api"]
    assert gate["if"] == "${{ !cancelled() && github.event_name == 'pull_request' }}" and gate["permissions"] == {}
    run = step(gate, "State of the API matrix")["run"]
    for code in ("0)", "1)", "2)", "*)"):
        assert code in run
    assert 'exit 1' in run and 'exit 2' in run and 'exit 3' in run
    assert gate["steps"][0]["env"]["RC"] == "${{ needs.api.outputs.rc }}" and wf["jobs"]["api"]["outputs"]["rc"] == "${{ steps.matrix.outputs.rc }}"


def test_gate_deterministic_is_not_a_required_check_yet():
    """Whether it becomes required is the owner's decision (a direction change); this change does not make it one."""
    with open(os.path.join(ROOT, "scripts", "ci", "tests", "testdata", "required_checks.txt"), encoding="utf-8") as f:
        required = f.read().split("\n")
    assert "gate-deterministic" not in required and "api" not in required
