"""The shape of .github/workflows/pr-quality.yaml: read-only, informational, product PRs only, in step with ci.yaml and quality.py."""

import os
import re

import pytest
import yaml

import quality
import quality_bundle as qb

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
WF = os.path.join(ROOT, ".github", "workflows")


def load(name):
    with open(os.path.join(WF, name), encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def wf():
    return load("pr-quality.yaml")


def steps(job):
    return [s for s in job["steps"] if isinstance(s, dict)]


def test_it_runs_on_pull_requests_only_and_holds_no_token_by_default(wf):
    on = wf.get("on", wf.get(True))
    assert list(on) == ["pull_request"], "no pull_request_target, workflow_run, schedule or push: the data job must never get a write token or secrets"
    assert wf["permissions"] == {}


def test_no_job_can_write_and_the_report_job_reads_only_what_it_needs(wf):
    assert wf["jobs"]["scope"]["permissions"] == {"contents": "read"}
    assert wf["jobs"]["report"]["permissions"] == {"contents": "read", "actions": "read"}
    text = open(os.path.join(WF, "pr-quality.yaml"), encoding="utf-8").read()
    assert not re.search(r"^\s*[\w-]+:\s*write\b", text, re.M) and "write-all" not in text
    assert "secrets." not in text


def test_the_job_is_gated_by_the_ci_scope_in_the_canonical_form_and_is_not_required(wf):
    job = wf["jobs"]["report"]
    assert job["needs"] == "scope"
    assert job["if"] == ("${{ !cancelled() && (needs.scope.outputs.skip_go != 'true' || "
                         "contains(format(',{0},', needs.scope.outputs.force), ',pr-quality.yaml:report,')) }}")
    required = open(os.path.join(ROOT, "scripts", "ci", "tests", "testdata", "required_checks.txt"), encoding="utf-8").read().split("\n")
    assert job["name"] == "pr-quality" and "pr-quality" not in required and "report" not in required


def test_no_run_script_interpolates_an_expression(wf):
    """Event data reaches the scripts through env only, never through `${{ }}` inside a script (script injection)."""
    for jid, job in wf["jobs"].items():
        for s in steps(job):
            assert "${{" not in s.get("run", ""), f"{jid}: {s.get('name')}"


def test_every_checkout_drops_the_token(wf):
    for job in wf["jobs"].values():
        for s in steps(job):
            if str(s.get("uses", "")).startswith("actions/checkout@"):
                assert s["with"]["persist-credentials"] is False


def test_the_artifact_is_named_pr_quality_and_holds_what_bundle_writes(wf):
    up = [s for s in steps(wf["jobs"]["report"]) if str(s.get("uses", "")).startswith("actions/upload-artifact@")]
    assert len(up) == 1 and up[0]["with"]["name"] == "pr-quality" and up[0]["with"]["path"] == "pr-quality/"
    bundle = next(s for s in steps(wf["jobs"]["report"]) if s.get("id") == "bundle")
    assert "--out-dir pr-quality" in bundle["run"] and "quality.py bundle" in bundle["run"]
    assert {qb.QUALITY_FILE, qb.REPORT_FILE, qb.MANIFEST_FILE} == {"quality.json", "pr-quality.md", "manifest.json"}


def test_every_quality_py_subcommand_and_option_the_workflow_uses_exists(wf, capsys):
    text = "\n".join(s.get("run", "") for s in steps(wf["jobs"]["report"])).replace("\\\n", " ")
    calls = re.findall(r"ab/quality\.py\s+([a-z]+)([^\n]*)", text)
    assert {c for c, _ in calls} == {"packages", "collect", "flaky", "bundle"}
    for cmd, rest in calls:
        with pytest.raises(SystemExit):
            quality.main([cmd, "--help"])
        helptext = capsys.readouterr().out
        for opt in re.findall(r"(?<![\w-])(--[a-z][a-z-]*)", rest):
            assert opt in helptext, f"quality.py {cmd} has no option {opt}"


def test_the_pytest_files_it_runs_exist(wf):
    text = "\n".join(s.get("run", "") for s in steps(wf["jobs"]["report"]))
    files = re.findall(r"scripts/bench/ab/tests/\S+\.py", text)
    assert files and all(os.path.isfile(os.path.join(ROOT, f)) for f in files)


def test_pins_and_the_deps_cache_equal_those_of_ci_yaml(wf):
    """A drifted pin would build deps nobody else caches (or the wrong ones); the key and paths must be ci.yaml's."""
    ci = load("ci.yaml")
    for k in ("VL_VERSION_LOGS", "VL_COMMIT_TRACES", "VT_VERSION", "GOWORK", "GOLANGCI_LINT_VERSION"):
        assert wf["env"][k] == ci["env"][k], k
    mine = next(s for s in steps(wf["jobs"]["report"]) if s.get("id") == "deps")["with"]
    theirs = next(s for s in steps(ci["jobs"]["deps"]) if str(s.get("uses", "")).startswith("actions/cache/save@"))["with"]
    assert mine["key"] == theirs["key"] and mine["path"] == theirs["path"]


def test_python_dependencies_are_installed_with_hashes(wf):
    run = next(s for s in steps(wf["jobs"]["report"]) if "pip install" in s.get("run", ""))["run"]
    assert "--require-hashes" in run
    for f in re.findall(r"-r (\S+)", run):
        assert os.path.isfile(os.path.join(ROOT, f))
