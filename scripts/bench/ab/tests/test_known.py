"""known.py: every difference is tracked by a registry row (and an issue), new, or UNTRACKED."""
import known

ROWS = [
    {"id": "vl.a.issue", "expect": "differ", "differ_note": "tracked https://github.com/ReliablyObserve/victoria-lakehouse/issues/452", "refs": {"doc": "docs/x.md"}},
    {"id": "vl.a.gap", "expect": "differ", "differ_note": "see docs/parity-and-gaps.md #B2", "refs": {}},
    {"id": "vl.a.noissue", "expect": "unsupported", "differ_note": "by design", "refs": {}},
    {"id": "vl.a.pass", "expect": "pass", "refs": {}},
]
GAPS = {"B2": [280, 378]}


def obs(status, covers, source="perf", what="logs fv_service"):
    return {"source": source, "what": what, "status": status, "covers": covers, "where": ["cold/1h/numeric"], "signal": "logs"}


def test_issue_numbers_and_gap_ids_are_read_from_urls_and_hash_references():
    from abreg import gap_ids, issue_numbers
    assert issue_numbers("see https://github.com/ReliablyObserve/victoria-lakehouse/issues/452 and #458, not #A6 or #7") == [452, 458]
    # a pull request link and an issue of another project are not issues that track a Lakehouse difference
    assert issue_numbers("https://github.com/ReliablyObserve/victoria-lakehouse/pull/9 https://github.com/VictoriaMetrics/VictoriaLogs/issues/100 (#100)") == []
    assert issue_numbers("https://github.com/ReliablyObserve/loki-vl-proxy/issues/77") == [77]
    assert gap_ids("gap #A6 and #B2, issue #458") == ["A6", "B2"]


def test_a_pre_existing_difference_with_a_row_and_an_issue_is_tracked():
    k = known.join([obs("pre-existing", ["vl.a.issue"])], ROWS, GAPS)
    it = k["items"][0]
    assert it["tracking"] == "tracked" and it["tracked_by"][0]["issues"] == [452] and k["failed"] is False


def test_a_gap_id_resolves_to_the_issues_of_its_entry_in_the_gaps_doc():
    k = known.join([obs("pre-existing", ["vl.a.gap"])], ROWS, GAPS)
    assert k["items"][0]["tracked_by"][0]["issues"] == [280, 378] and k["items"][0]["tracking"] == "tracked"


def test_a_difference_on_a_row_that_says_pass_is_untracked_and_fails():
    k = known.join([obs("pre-existing", ["vl.a.pass"])], ROWS, GAPS)
    assert k["items"][0]["tracking"] == "untracked" and k["failed"] is True
    md = "\n".join(known.render(k))
    assert "UNTRACKED" in md and "Known differences and gaps" in md


def test_a_difference_with_no_row_at_all_is_untracked():
    assert known.join([obs("pre-existing", ["lh.feature.unknown"])], ROWS, GAPS)["items"][0]["tracking"] == "untracked"


def test_a_new_difference_fails_even_when_a_row_records_an_older_one():
    k = known.join([obs("new", ["vl.a.issue"])], ROWS, GAPS)
    assert k["items"][0]["tracking"] == "new" and k["failed"] is True
    assert "NEW, introduced by this PR" in "\n".join(known.render(k))


def test_a_fixed_difference_names_the_rows_that_can_flip_to_pass():
    k = known.join([obs("fixed", ["vl.a.issue"])], ROWS, GAPS)
    assert k["items"][0]["tracking"] == "fixed" and k["items"][0]["flip"] == ["vl.a.issue"] and k["failed"] is False


def test_a_tracked_row_without_an_issue_passes_unless_issues_are_required():
    assert known.join([obs("pre-existing", ["vl.a.noissue"])], ROWS, GAPS)["failed"] is False
    k = known.join([obs("pre-existing", ["vl.a.noissue"])], ROWS, GAPS, require_issue=True)
    assert k["items"][0]["tracking"] == "tracked-no-issue" and k["failed"] is True


def test_registry_stats_count_rows_with_and_without_an_issue():
    st = known.registry_gap_stats(ROWS, GAPS)
    assert st == {"rows": 3, "with_issue": 2, "without_issue": 1, "without_issue_gap_id_only": 0, "without_issue_ids": ["vl.a.noissue"]}
    assert "3 known differences" in known.stats_line(st)


def test_observations_from_perf_cells_group_by_shape_layer_and_class():
    summary = {"cells": [{"shape": "s", "difference": "pre-existing", "signal": "logs", "layer": "cold", "range": "1h", "form": "numeric"},
                         {"shape": "s", "difference": "pre-existing", "signal": "logs", "layer": "cold", "range": "full", "form": "numeric"},
                         {"shape": "s", "difference": "pre-existing", "signal": "logs", "layer": "compacted", "range": "1h", "form": "numeric"},
                         {"shape": "s", "difference": "none", "signal": "logs", "layer": "buffer", "range": "1h", "form": "numeric"}]}
    spec = {"sets": {"x": {"shapes": [{"name": "s", "covers": ["vl.a.pass"]}]}}}
    o = known.observed_from_perf(summary, spec)
    assert [(x["what"], x["where"]) for x in o] == [("s [cold]", ["1h/numeric", "full/numeric"]), ("s [compacted]", ["1h/numeric"])]
    # the other S3 latency profiles are observed too, with their profile in `where`
    more = known.observed_from_perf({"cells": []}, spec, {"50": summary})
    assert more[0]["where"][0] == "1h/numeric/50 ms"


def test_a_row_of_the_other_signal_does_not_track_a_difference():
    rows = [{"id": "vt.a.differ", "surface": "vt", "expect": "differ", "differ_note": "https://github.com/ReliablyObserve/victoria-lakehouse/issues/9"},
            {"id": "vl.a.pass", "surface": "vl", "expect": "pass"}]
    o = obs("pre-existing", ["vt.a.differ", "vl.a.pass"])  # a logs shape covering a traces row
    assert known.join([o], rows, {})["items"][0]["tracking"] == "untracked"
    traces = dict(obs("pre-existing", ["vt.a.differ"]), signal="traces")
    assert known.join([traces], rows, {})["items"][0]["tracking"] == "tracked"


def test_a_flip_between_samples_is_listed_and_fails_only_when_a_sample_regressed():
    m = {"results": [{"id": "a", "row": "r1", "surface": "vl-native", "path": "/p", "signal": "logs", "verdict": "nondeterministic", "samples": ["same", "fixed"],
                      "pr_score": 90, "base_score": 100, "notes": []},
                     {"id": "b", "row": "r2", "surface": "vl-native", "path": "/p", "signal": "logs", "verdict": "nondeterministic", "samples": ["same", "regressed"],
                      "pr_score": 90, "base_score": 100, "notes": []}]}
    o = {x["covers"][0]: x["status"] for x in known.observed_from_api(m)}
    assert o == {"r1": "unstable", "r2": "new"}
    k = known.join(known.observed_from_api(m), [], {})
    assert {i["what"]: i["tracking"] for i in k["items"]} == {"r1": "unstable", "r2": "new"} and k["failing"] == 1
    assert "FLAKY" in "\n".join(known.render(k))


def test_observations_from_the_api_matrix_follow_the_verdict():
    m = {"results": [
        {"id": "a", "row": "r1", "surface": "vl-native", "path": "/p", "signal": "logs", "verdict": "regressed", "pr_score": 50, "base_score": 100, "notes": []},
        {"id": "b", "row": "r2", "surface": "vl-native", "path": "/p", "signal": "logs", "verdict": "same", "pr_score": 50, "base_score": 50, "notes": []},
        {"id": "c", "row": "r3", "surface": "vl-native", "path": "/p", "signal": "logs", "verdict": "fixed", "pr_score": 100, "base_score": 50, "notes": []},
        {"id": "d", "row": "r4", "surface": "vl-native", "path": "/p", "signal": "logs", "verdict": "exact", "pr_score": 100, "base_score": 100, "notes": []},
        {"id": "e", "row": "r5", "surface": "vl-native", "path": "/p", "signal": "logs", "verdict": "arbitrary", "pr_score": 0, "base_score": 0, "notes": []},
        {"id": "f", "row": "r6", "surface": "vl-native", "path": "/p", "signal": "logs", "verdict": "improved", "pr_score": 60, "base_score": 50, "notes": []}]}
    o = {x["covers"][0]: x["status"] for x in known.observed_from_api(m)}
    assert o == {"r1": "new", "r2": "pre-existing", "r3": "fixed", "r6": "pre-existing"}  # exact and arbitrary are not differences
