"""api_proof.py: the scope of the API matrix and the artifact it hands the commenter, checked against the commenter's own parser and sanitiser."""
import json
import os

import pytest

import api_proof
import comment
import known
import selection
from scripts.ci import proof_comment as pc
from test_api_triage import aa, judged

REPO = "ReliablyObserve/victoria-lakehouse"
HEAD = "c" * 40


# ------------------------------------------------------------------------------------------------ scope
@pytest.mark.parametrize("files,api,fit", [
    (["internal/storage/x.go"], True, False),
    (["lakehouse-traces/internal/y.go"], True, False),
    (["go.mod"], True, False),
    (["Dockerfile.logs"], True, False),
    (["internal/ui/static/app.js"], True, False),                      # an asset of a runtime tree
    (["scripts/bench/ab/api_matrix.py"], True, True),                  # the harness: the matrix and the runner-fit measurement
    (["scripts/proof/stack.py"], True, False),
    (["scripts/proof/runner_fit.py"], True, True),
    (["deployment/docker/docker-compose-proof.yml"], True, True),
    ([".github/workflows/pr-proof.yaml"], True, True),
    (["tests/conformance/registry/rows/vl/pipes.yaml"], True, False),  # new rows must be exercised
    (["scripts/bench/ab/shapes.json"], False, True),                   # perf shapes only: the matrix does not read them
    (["docs/pr-perf-smoke.md", "README.md"], False, False),
    ([".github/workflows/ci.yaml"], False, False),
    (["internal/storage/x_test.go"], False, False),
    (["charts/victoria-lakehouse/values.yaml"], False, False),
    ([], False, False),
])
def test_scope(files, api, fit):
    assert api_proof.api_scope(files) is api
    assert api_proof.runner_fit_scope(files) is fit


def test_scope_command_prints_the_two_outputs(monkeypatch, capsys):
    monkeypatch.setattr(selection, "changed_files", lambda b, h: ["internal/a.go"])
    assert api_proof.main(["scope", "--event", "pull_request", "--base", "origin/main", "--head", "HEAD"]) == 0
    assert capsys.readouterr().out.split() == ["api=true", "runner_fit=false"]
    monkeypatch.setattr(selection, "changed_files", lambda b, h: (_ for _ in ()).throw(AssertionError("a manual run has no diff")))
    api_proof.main(["scope", "--event", "workflow_dispatch"])
    assert capsys.readouterr().out.split() == ["api=false", "runner_fit=true"]


# ------------------------------------------------------------------------------------------------ the artifact
def passing_run(tmp_path, exit_code=0, with_known=True):
    m, rows = aa()
    for r in m["results"]:
        if r["id"] == "vl.pipe.extract.basic":
            r.update(samples=["regressed", "improved", "same"], verdict="nondeterministic")
    j = judged(m, rows)
    # a tracked difference that cites an issue of this repository
    rows = [dict(r) for r in rows]
    for r in rows:
        if r["id"] == "vl.select.field_values.limit":
            r["differ_note"] = "https://github.com/ReliablyObserve/victoria-lakehouse/issues/452"
    k = known.join(known.observed_from_api(j), rows, {})
    text, v = comment.render_api(j, {"run": True, "sets": {}, "changed": 3}, {"base": "a" * 40, "head": HEAD, "registry": "base aaaaaaaaaa"}, k)
    api = tmp_path / "api"
    api.mkdir()
    (api / "api-matrix.md").write_text(text)
    (api / "api-verdict.json").write_text(json.dumps(dict(v, exit=exit_code)))
    (api / "api-matrix.json").write_text(json.dumps(j))
    if with_known:
        (api / "known-api.json").write_text(json.dumps(k))
    return api, text


def bundle(tmp_path, api, *extra, pr=123):
    out, data = tmp_path / "lh-proof-comment", tmp_path / "data"
    rc = api_proof.main(["bundle", "--out-dir", str(out), "--data-dir", str(data), "--api-dir", str(api), "--pr-number", str(pr), "--head-sha", HEAD,
                         "--base-sha", "a" * 40, "--repo", REPO, "--run-id", "987654", *extra])
    assert rc == 0
    return out, data


def read(out):
    manifest = pc.parse_manifest((out / "manifest.json").read_bytes())
    return manifest, (out / "api-matrix.md").read_text()


@pytest.mark.parametrize("exit_code,verdict", [(0, "pass"), (1, "fail"), (2, "incomplete"), (3, "incomplete"), (4, "incomplete")])
def test_the_exit_code_of_the_run_is_the_verdict_of_the_manifest(tmp_path, exit_code, verdict):
    api, text = passing_run(tmp_path, exit_code)
    out, _ = bundle(tmp_path, api)
    manifest, body = read(out)
    assert manifest["comments"] == [{"kind": "api-matrix", "verdict": verdict}] and body == text and manifest["images"] == []
    assert sorted(os.listdir(out)) == ["api-matrix.md", "manifest.json"]


def test_the_manifest_and_files_pass_the_commenters_own_checks_for_the_pr_proof_producer(tmp_path):
    api, _ = passing_run(tmp_path)
    out, _ = bundle(tmp_path, api)
    manifest, body = read(out)
    files = {"manifest.json": (out / "manifest.json").read_bytes(), "api-matrix.md": body.encode()}
    pc.check_files(files, manifest, ".github/workflows/pr-proof.yaml")
    with pytest.raises(pc.Reject):  # PR Quality's workflow may not post this kind
        pc.check_files(files, manifest, ".github/workflows/pr-quality.yaml")
    assert manifest["pr_number"] == 123 and manifest["head_sha"] == HEAD


def test_the_body_goes_through_the_commenters_sanitiser_changed_only_by_zero_width_spaces_and_idempotently(tmp_path):
    """The row ids `lh.admin.parity.schema@logs` carry an `@`: the commenter defuses a mention with a zero-width space, even in a code span."""
    api, text = passing_run(tmp_path)
    out, _ = bundle(tmp_path, api)
    manifest, body = read(out)
    refs = manifest["allowed_issue_refs"]
    once, used = pc.sanitize_body(pc.body_text(body.encode(), "api-matrix"), repo=REPO, image_names=[], allowed_refs=refs)
    assert once.replace("\u200b", "") == pc.body_text(body.encode(), "api-matrix") and used == set()
    twice, _ = pc.sanitize_body(once, repo=REPO, image_names=[], allowed_refs=refs)
    assert twice == once


def test_only_issues_of_this_repository_become_allowed_issue_refs(tmp_path):
    api, _ = passing_run(tmp_path)
    out, _ = bundle(tmp_path, api)
    manifest, _ = read(out)
    assert manifest["allowed_issue_refs"] == [452]
    other = {"items": [{"tracked_by": [{"issues": ["loki-vl-proxy#77", "victoria-lakehouse#99999999", "victoria-lakehouse#5"]}]}]}
    assert api_proof.issue_numbers(None) == [] and api_proof.issue_numbers(other) == [5]  # another repository's link is neutralised by the commenter


def test_the_data_artifact_holds_the_machine_readable_results(tmp_path):
    api, _ = passing_run(tmp_path)
    _, data = bundle(tmp_path, api)
    assert sorted(os.listdir(data)) == ["api-matrix.json", "api-verdict.json", "known-api.json"]


def test_no_verdict_file_means_a_did_not_complete_comment_with_the_run_link(tmp_path):
    empty = tmp_path / "api"
    empty.mkdir()
    out, data = bundle(tmp_path, empty)
    manifest, body = read(out)
    assert manifest["comments"] == [{"kind": "api-matrix", "verdict": "incomplete"}]
    assert body.startswith("<!-- lh-api-matrix -->\n### ") and "did not complete" in body.splitlines()[1]
    assert f"https://github.com/{REPO}/actions/runs/987654" in body
    once, _ = pc.sanitize_body(pc.body_text(body.encode(), "api-matrix"), repo=REPO, image_names=[], allowed_refs=[])
    assert once == pc.body_text(body.encode(), "api-matrix")  # the link is the one kind the commenter keeps
    assert os.listdir(data) == []


def test_a_verdict_without_its_comment_is_also_incomplete(tmp_path):
    api = tmp_path / "api"
    api.mkdir()
    (api / "api-verdict.json").write_text('{"exit": 0}')
    out, _ = bundle(tmp_path, api)
    assert read(out)[0]["comments"][0]["verdict"] == "incomplete"


def test_an_explicit_reason_is_in_the_comment_and_a_bad_run_id_is_not_a_link(tmp_path):
    empty = tmp_path / "api"
    empty.mkdir()
    out, _ = bundle(tmp_path, empty, "--reason", "the stack did not come up")
    assert "the stack did not come up" in read(out)[1]
    out2 = tmp_path / "o2"
    api_proof.main(["bundle", "--out-dir", str(out2), "--api-dir", str(empty), "--pr-number", "5", "--head-sha", HEAD, "--repo", "a b", "--run-id", "x/../y"])
    assert "https://" not in (out2 / "api-matrix.md").read_text()


def test_skipped_writes_the_skipped_comment(tmp_path):
    empty = tmp_path / "api"
    empty.mkdir()
    out, _ = bundle(tmp_path, empty, "--skipped", "--changed", "4")
    manifest, body = read(out)
    assert manifest["comments"][0]["verdict"] == "skipped" and "skipped" in body.splitlines()[1] and "4 file(s)" in body


def test_a_value_the_commenter_would_refuse_stops_the_producer(tmp_path):
    empty = tmp_path / "api"
    empty.mkdir()
    with pytest.raises(pc.Reject):
        bundle(tmp_path, empty, pr=0)


def test_a_huge_body_is_cut_at_a_line_with_the_marker_kept(tmp_path):
    api = tmp_path / "api"
    api.mkdir()
    (api / "api-matrix.md").write_text("<!-- lh-api-matrix -->\n### RED\n\n" + "| a | b |\n" * 20000)
    (api / "api-verdict.json").write_text('{"exit": 1}')
    out, _ = bundle(tmp_path, api)
    body = read(out)[1]
    assert len(body) <= api_proof.BODY_CAP and body.startswith("<!-- lh-api-matrix -->\n") and "cut to fit" in body and body.endswith("run._\n")
