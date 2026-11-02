"""The API matrix judged against the BASE registry: pending and absent rows are triaged, a pull request cannot downgrade its own failure.

The fixtures are a trim of a real A/A run of the proof stack on `ubuntu-latest` (base and PR are the same code, so every difference in it is
noise or a difference that already exists): 29 of the 227 exercised rows, 28 of them `pending: true` in the registry.
Without the triage that run was RED (one `new` and 18 `untracked`), so every pull request would have been red.
"""
import copy
import json
import os
import subprocess

import abreg
import api_matrix
import comment
import known

HERE = os.path.dirname(os.path.abspath(__file__))


def fixture(name):
    with open(os.path.join(HERE, "fixtures", name), encoding="utf-8") as f:
        return json.load(f)


def aa():
    m = fixture("aa_api_matrix.json")
    return m, fixture("aa_rows.json")


def judged(matrix, base_rows, exec_rows=None):
    """What api_matrix.run_all does after the requests: the triage from the base rows, then the summary."""
    m = copy.deepcopy(matrix)
    ids = api_matrix.untriaged_ids(exec_rows if exec_rows is not None else base_rows, base_rows)
    m["summary"] = api_matrix.summarize(m["results"], m["skipped"], ids)
    return m


def the_flapping_row(m):
    return next(r for r in m["results"] if r["id"] == "vl.pipe.extract.basic")


SEL = {"run": True, "sets": {}, "changed": 3}
META = {"base": "a" * 40, "head": "b" * 40}


# ------------------------------------------------------------------------------------------------ the triage
def test_a_difference_on_a_pending_row_is_listed_not_gated_and_nothing_else_changes():
    m, rows = aa()
    the_flapping_row(m).update(samples=["regressed", "improved", "same"], verdict="nondeterministic")  # re-sampled: it flips, so it is noise
    m = judged(m, rows)
    k = known.join(known.observed_from_api(m), rows, {})
    assert k["counts"] == {"unstable": 1, "pending": 15, "tracked": 6} and k["failed"] is False
    assert m["summary"]["failing"] == []
    text, v = comment.render_api(m, SEL, META, k)
    assert v["exit"] == 0 and "pending triage" in text and "UNTRACKED" not in text


def test_without_the_triage_the_same_run_is_red_as_it_was_on_main():
    """The failing-first half: judged as before (no triage, no pending) the A/A run fails on 15 untracked rows and the flapping one."""
    m, rows = aa()
    the_flapping_row(m).update(samples=["regressed", "improved", "same"], verdict="nondeterministic")
    m["summary"] = api_matrix.summarize(m["results"], m["skipped"])  # nothing untriaged
    old_rows = [dict(r, pending=False) for r in rows]
    k = known.join(known.observed_from_api(m), old_rows, {})
    assert k["failed"] is True and k["counts"] == {"new": 1, "untracked": 15, "tracked": 6}
    assert m["summary"]["failing"] == ["vl.pipe.extract.basic"]


def test_a_single_sample_of_a_flapping_row_gates_and_the_confirm_rounds_exist_to_prevent_that():
    m, rows = aa()
    one = judged(m, rows)  # the A/A artifact had one round: the row 'regressed' once, deterministically as far as anyone can tell
    assert one["summary"]["failing"] == ["vl.pipe.extract.basic"]
    assert known.join(known.observed_from_api(one), rows, {})["failed"] is True


def test_a_pending_row_that_regresses_in_every_sample_still_gates():
    """Pending exempts the untriaged differences from hot, never a NEW one: the pull request made the answer worse."""
    m, rows = aa()
    row = next(r for r in m["results"] if r["id"] == "vt.select.logsql_hits.basic")
    row.update(verdict="regressed", samples=["regressed"] * 8, pr_score=10.0)
    m = judged(m, rows)
    assert "vt.select.logsql_hits.basic" in m["summary"]["failing"]
    k = known.join(known.observed_from_api(m), rows, {})
    assert k["failed"] is True and any(i["tracking"] == "new" and i["what"] == "vt.select.logsql_hits.basic" for i in k["items"])


def test_a_pull_request_cannot_make_a_row_pending_or_add_a_gap_row_to_downgrade_its_own_failure():
    m, rows = aa()
    base = [dict(r) for r in rows]
    target = next(r for r in base if r["id"] == "vl.select.field_values.limit")
    target.pop("pending", None)  # non-pending on the base
    pr = copy.deepcopy(base)
    next(r for r in pr if r["id"] == "vl.select.field_values.limit")["pending"] = True  # the PR sets pending on it
    ids = api_matrix.untriaged_ids(pr, base)
    assert "vl.select.field_values.limit" not in ids  # judged by the base
    # the PR adds an `expect: differ` row with an issue for a row the base says `pass`: the base's rows judge, so it stays untracked
    row = next(r for r in m["results"] if r["id"] == "vt.select.vmalert.differ")
    row["row"] = "vt.new.row"
    base_without = [r for r in base if r["id"] != "vt.select.vmalert.differ"]
    pr_rows = base_without + [{"id": "vt.new.row", "surface": "vt", "expect": "differ", "differ_note": "https://github.com/ReliablyObserve/victoria-lakehouse/issues/1"}]
    obs = [o for o in known.observed_from_api(judged(m, base_without, exec_rows=pr_rows)) if o["what"] == "vt.new.row"]
    assert obs and known.join(obs, base_without, {})["items"][0]["tracking"] in ("pending", "untracked")
    assert known.join(obs, pr_rows, {})["items"][0]["tracking"] == "tracked"  # (what judging by the PR's rows would have done)


def test_a_row_the_base_registry_does_not_have_is_triaged_like_a_pending_row():
    base = [{"id": "a", "expect": "pass"}, {"id": "b", "expect": "pass", "pending": True}]
    pr = base + [{"id": "c", "expect": "pass"}]
    assert api_matrix.untriaged_ids(pr, base) == {"b", "c"}
    assert api_matrix.untriaged_ids(base, base) == {"b"}


def test_the_single_non_pending_row_is_gated_exactly_as_before():
    m, rows = aa()
    nonpending = next(r["id"] for r in rows if not r.get("pending"))
    row = next(r for r in m["results"] if r["row"] == nonpending)
    row.update(verdict="regressed", samples=["regressed", "regressed", "nondeterministic"], pr_score=0.0)
    row["samples"] = ["regressed", "exact"]
    row["verdict"] = "nondeterministic"  # a flip is still gated on a triaged row when one sample had the PR worse (the existing rule)
    j = judged(m, rows)
    assert row["id"] in j["summary"]["failing"]
    k = known.join(known.observed_from_api(j), rows, {})
    assert k["failed"] is True and k["items"][0]["tracking"] == "new"


def test_a_flip_on_a_pending_row_is_listed_as_unstable_not_failing():
    m, rows = aa()
    the_flapping_row(m).update(verdict="nondeterministic", samples=["regressed", "improved"])
    j = judged(m, rows)
    assert "vl.pipe.extract.basic" not in j["summary"]["failing"]
    k = known.join(known.observed_from_api(j), rows, {})
    assert next(i for i in k["items"] if i["what"] == "vl.pipe.extract.basic")["tracking"] == "unstable" and k["failed"] is False


# ------------------------------------------------------------------------------------------------ the rows of the base
def git(cwd, *args):
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@example.com", GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@example.com")
    return subprocess.run(["git", "-c", "commit.gpgsign=false", "-C", str(cwd), *args], check=True, capture_output=True, text=True, env=env).stdout


def test_registry_at_reads_the_rows_and_the_gaps_doc_of_the_ref_not_of_the_working_tree(tmp_path):
    reg = tmp_path / "tests" / "conformance" / "registry" / "rows" / "vl"
    reg.mkdir(parents=True)
    (reg / "a.yaml").write_text("- {id: vl.a, surface: vl, expect: pass, pending: true}\n")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "parity-and-gaps.md").write_text("## B2 a gap https://github.com/ReliablyObserve/victoria-lakehouse/issues/7\n")
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "add", "-A")
    git(tmp_path, "commit", "-q", "-m", "base")
    base = git(tmp_path, "rev-parse", "HEAD").strip()
    # the pull request: the row is no longer pending, a differ row and a gap are added (committed, so that HEAD is not the base)
    (reg / "a.yaml").write_text("- {id: vl.a, surface: vl, expect: pass}\n- {id: vl.new, surface: vl, expect: differ, differ_note: 'https://github.com/ReliablyObserve/victoria-lakehouse/issues/9'}\n")
    (tmp_path / "docs" / "parity-and-gaps.md").write_text("## B2 a gap https://github.com/ReliablyObserve/victoria-lakehouse/issues/8\n")
    git(tmp_path, "add", "-A")
    git(tmp_path, "commit", "-q", "-m", "pr")
    rows, gaps = abreg.registry_at(base, root=str(tmp_path))
    assert [r["id"] for r in rows] == ["vl.a"] and rows[0]["pending"] is True
    assert gaps.get("B2") == ["victoria-lakehouse#7"]  # not #8 of the pull request
    assert rows[0]["_file"].endswith("rows/vl/a.yaml")
    head_rows = abreg.load_rows(str(tmp_path / "tests" / "conformance" / "registry"))
    assert [r["id"] for r in head_rows] == ["vl.a", "vl.new"]


def test_registry_at_fails_loudly_for_a_ref_that_does_not_exist(tmp_path):
    git(tmp_path, "init", "-q", "-b", "main")
    (tmp_path / "x").write_text("x")
    git(tmp_path, "add", "-A")
    git(tmp_path, "commit", "-q", "-m", "x")
    try:
        abreg.registry_at("0" * 40, root=str(tmp_path))
    except (RuntimeError, subprocess.CalledProcessError):
        return
    raise AssertionError("a missing base must not fall back to the working tree")


# ------------------------------------------------------------------------------------------------ the confirming rounds
def res(id_, samples, verdict=None, status=("200", "200", "200"), untriaged=False):
    return {"id": id_, "row": id_, "verdict": verdict or api_matrix.classify_samples(samples), "samples": samples, "untriaged": untriaged,
            "status": {"ref": status[0], "base": status[1], "pr": status[2]}, "status_changed": status[1] != status[2], "p50_ms": {}, "notes": []}


def test_a_failing_row_is_sent_again_and_its_samples_are_merged_so_a_flip_is_called_nondeterministic(monkeypatch):
    sent = []
    again = res("r", ["improved", "same", "regressed", "improved"])
    monkeypatch.setattr(api_matrix, "run_request", lambda state, req, rounds: sent.append((req["id"], rounds)) or dict(again))
    first = [res("r", ["regressed"]), res("ok", ["exact"])]
    out = api_matrix.confirm_failing({}, {"r": {"id": "r"}, "ok": {"id": "ok"}}, first, 4)
    assert sent == [("r", 4)]  # only the failing row, with the extra rounds
    r = next(x for x in out if x["id"] == "r")
    assert r["verdict"] == "nondeterministic" and r["samples"] == ["regressed", "improved", "same", "regressed", "improved"] and r["confirmed"] == 4
    assert next(x for x in out if x["id"] == "ok") == first[1]


def test_a_real_regression_stays_regressed_after_the_confirming_rounds(monkeypatch):
    monkeypatch.setattr(api_matrix, "run_request", lambda state, req, rounds: res("r", ["regressed"] * rounds))
    out = api_matrix.confirm_failing({}, {"r": {"id": "r"}}, [res("r", ["regressed"])], 4)
    assert out[0]["verdict"] == "regressed" and api_matrix.failing(out[0])


def test_no_confirming_rounds_means_the_first_pass_stands(monkeypatch):
    monkeypatch.setattr(api_matrix, "run_request", lambda *a: (_ for _ in ()).throw(AssertionError("must not be sent")))
    first = [res("r", ["regressed"])]
    assert api_matrix.confirm_failing({}, {"r": {"id": "r"}}, first, 0) == first


# ------------------------------------------------------------------------------------------------ the states of the check
def mk(results, skipped=0, untriaged=frozenset()):
    return {"results": results, "skipped": [], "rounds": 3, "summary": api_matrix.summarize(results, [], untriaged)}


def up(id_, signal="logs", ref="200", **kw):
    r = res(id_, ["exact"], status=(ref, "200", "200"))
    r.update(signal=signal, surface="vl-native" if signal == "logs" else "vt-native", path="/p", lh_only=False, base_score=100, pr_score=100, base_worst="", pr_worst="",
             latency="same")
    r.update(kw)
    return r


def test_the_states_pass_failed_invalid_and_the_exit_codes():
    ok_text, ok = comment.render_api(mk([up("a"), up("b")]), SEL, META, known.join([], [], {}))
    assert ok["exit"] == 0 and ok["state"] == "passed" and ok_text.splitlines()[1].startswith("### GREEN") or "NEUTRAL" in ok_text.splitlines()[1]
    bad = mk([up("a"), up("b", verdict="regressed", samples=["regressed"] * 5, pr_score=10)])
    text, v = comment.render_api(bad, SEL, META, known.join([], [], {}))
    assert v["exit"] == 1 and v["state"] == "failed" and "### RED" in text.splitlines()[1]


def test_a_broken_reference_is_an_invalid_run_not_a_failed_one():
    rows = [up(f"r{i}", ref="0") for i in range(5)] + [up(f"s{i}") for i in range(5)]  # hot did not answer half of the logs rows
    text, v = comment.render_api(mk(rows), SEL, META, known.join([], [], {}))
    assert v["exit"] == 2 and v["state"] == "invalid" and "invalid run" in text.splitlines()[1]
    # one or two such rows are listed, not an invalid run
    few = [up("a", ref="502"), up("b")] + [up(f"s{i}") for i in range(10)]
    assert comment.render_api(mk(few), SEL, META, known.join([], [], {}))[1]["exit"] == 0
    # an invalid run is stronger than a failure
    rows.append(up("x", verdict="regressed", samples=["regressed"] * 5))
    assert comment.render_api(mk(rows), SEL, META, known.join([], [], {}))[1]["exit"] == 2


def test_a_run_that_exercised_no_row_proves_nothing():
    text, v = comment.render_api(mk([]), SEL, META, known.join([], [], {}))
    assert v["exit"] == 2 and "no registry row" in text


def test_the_header_counts_per_signal_and_the_gated_and_pending_rows():
    m, rows = aa()
    j = judged(m, rows)
    text, v = comment.render_api(j, SEL, META, known.join(known.observed_from_api(j), rows, {}))
    assert "logs:" in text and "traces:" in text
    assert v["gated_rows"] == 1 and v["pending_rows"] == len(j["results"]) - 1
    assert "**1 row is gated**" in text and f"**{len(j['results']) - 1} are pending triage**" in text


def test_the_stamp_names_the_registry_that_judged():
    m, rows = aa()
    j = judged(m, rows)
    text, _ = comment.render_api(j, SEL, dict(META, registry="base aaaaaaaaaa"), known.join([], rows, {}))
    assert "rows judged by the registry of the base aaaaaaaaaa" in text


def test_the_pending_differences_are_one_collapsed_section_and_not_rows_of_the_main_table():
    m, rows = aa()
    the_flapping_row(m).update(verdict="nondeterministic", samples=["regressed", "improved"])
    j = judged(m, rows)
    text, _ = comment.render_api(j, SEL, META, known.join(known.observed_from_api(j), rows, {}))
    table = text.split("<details>")[0]
    assert "vt.select.logsql_hits.basic" not in table  # a pending 'same' difference is in the collapsed section
    assert "pending triage" in text and "vt.select.logsql_hits.basic" in text
