"""report.py: verdicts, the noise rule, result parity against the reference, S3 cost."""
import report
from helpers import cell_rows, raw, row

K = "control-logs|logs count_total|cold|1h|numeric"


def one(base_ms, pr_ms, **kw):
    return report.summarize(raw(cell_rows(K, base_ms, pr_ms, **kw)), "t")["cells"][0]


def test_noise_rule_is_the_larger_of_15_percent_and_3_ms():
    assert one(100, 114)["verdict"] == "same"          # +14 ms but under 15 %
    assert one(100, 118)["verdict"] == "slower"        # beyond both
    assert one(10, 12.9)["verdict"] == "same"          # +29 % but 2.9 ms: under the floor
    assert one(10, 13.5)["verdict"] == "slower"
    assert one(100, 80)["verdict"] == "faster"
    assert one(1.0, 3.9)["verdict"] == "same"          # 3x of a 1 ms query is 2.9 ms: noise


def test_warm_p50_excludes_the_cold_run_and_cold_is_reported():
    c = one(10, 10)
    assert c["cold"]["base"] == 30 and c["p50"]["base"] == 10


def test_invalid_answers_never_count_as_latency():
    rows = cell_rows(K, 10, 10)
    for r in rows:
        if r["target"] == "pr" and r["run"] in (2, 3):
            r.update(valid=False, reason="empty result", status=200, result="", ms=0.1)  # a fast wrong answer
    c = report.summarize(raw(rows), "t")["cells"][0]
    assert c["p50"]["pr"] == 10 and c["n_invalid"]["pr"] == 2 and c["invalid_reasons"]["pr"] == ["empty result"]


def test_broken_and_fixed():
    rows = [r for r in cell_rows(K, 10, 10) if not (r["target"] == "pr")] + [row(K, "pr", i, 1, valid=False, reason="http 500") for i in range(6)]
    assert report.summarize(raw(rows), "t")["cells"][0]["verdict"] == "broken"
    rows = [r for r in cell_rows(K, 10, 10) if not (r["target"] == "base")] + [row(K, "base", i, 1, valid=False, reason="http 500") for i in range(6)]
    assert report.summarize(raw(rows), "t")["cells"][0]["verdict"] == "fixed"


def test_difference_classes_against_the_reference():
    same = one(10, 10)
    assert same["vs_ref"] == {"base": "same", "pr": "same"} and same["difference"] == "none"
    pre = one(10, 10, base_result="41", pr_result="41")
    assert pre["difference"] == "pre-existing"
    new = one(10, 10, pr_result="43")
    assert new["difference"] == "new"
    changed = one(10, 10, base_result="41", pr_result="43")
    assert changed["difference"] == "new"           # differs from the reference AND from what the base answered
    fixed = one(10, 10, base_result="41", pr_result="42")
    assert fixed["difference"] == "fixed"


def test_s3_cost_flags_more_gets_beyond_noise_only():
    s3 = {K: {"base": {"cold": {"gets": 10, "requests": 10, "bytes": 1e6}, "warm_per_query": {"gets": 10, "requests": 10, "bytes": 1e6}},
              "pr": {"cold": {"gets": 14, "requests": 14, "bytes": 1.05e6}, "warm_per_query": {"gets": 10.5, "requests": 10.5, "bytes": 1e6}}}}
    c = report.summarize(raw(cell_rows(K, 10, 10), s3), "t")["cells"][0]
    assert c["s3_verdict"] == {"cold": "more", "warm": "same"} and c["control"] is True
    s3[K]["pr"]["cold"] = {"gets": 5, "requests": 5, "bytes": 5e5}
    assert report.summarize(raw(cell_rows(K, 10, 10), s3), "t")["cells"][0]["s3_verdict"]["cold"] == "less"
    assert report.s3_verdict(None, None) == "n/a"


def test_cold_not_cleared_is_reported():
    rows = cell_rows(K, 10, 10)
    for r in rows:
        if r["target"] == "pr" and r["run"] == 0:
            r["cold_cleared"] = False
    s = report.summarize(raw(rows), "t")
    assert s["cold_not_cleared"] == [K]


def test_confirm_replaces_only_the_moved_cells_and_marks_them():
    first = report.summarize(raw(cell_rows(K, 10, 20)), "t")
    assert first["cells"][0]["verdict"] == "slower"
    again = report.summarize(raw(cell_rows(K, 10, 10, runs=7)), "again")
    merged = report.merge_confirm(first, again)
    assert merged["cells"][0]["verdict"] == "same" and merged["cells"][0]["confirmed"] and merged["verdicts"] == {"same": 1}


def test_shapes_marked_vs_ref_false_compare_base_with_pr_only():
    skip = frozenset({"logs count_total"})
    c = report.summarize(raw(cell_rows(K, 10, 10, base_result="41", pr_result="41")), "t", ref_skip=skip)["cells"][0]
    assert c["vs_ref"] == {"base": "n/a", "pr": "n/a"} and c["difference"] == "none"
    c = report.summarize(raw(cell_rows(K, 10, 10, base_result="41", pr_result="43")), "t", ref_skip=skip)["cells"][0]
    assert c["difference"] == "new"  # not compared with hot, but the PR answers differently from the base
    spec = {"sets": {"s": {"shapes": [{"name": "a", "vs_ref": False}, {"name": "b"}]}}}
    assert report.ref_skip_from(spec) == frozenset({"a"})


def test_apply_ref_skip_rederives_a_saved_summary():
    s = report.summarize(raw(cell_rows(K, 10, 10, base_result="41", pr_result="41")), "t")
    assert s["differences"] == {"pre-existing": 1}
    out = report.apply_ref_skip(s, frozenset({"logs count_total"}))
    assert out["differences"] == {"none": 1} and out["cells"][0]["vs_ref"]["pr"] == "n/a"


def partial_rows(target, bad_runs, runs=6, reason="http 500", result="42", **kw):
    """rows of one cell where the PR (or another target) gets the given runs wrong."""
    rows = cell_rows(K, 10, 10, runs=runs, **kw)
    for r in rows:
        if r["target"] == target and r["run"] in bad_runs:
            r.update(valid=False, reason=reason, status=500 if reason.startswith("http") else 200, result="")
    return rows


def test_b1_a_pr_that_answers_the_cold_run_and_flaps_on_every_warm_run_is_unstable_not_n_a():
    """Scenario 1: cold run right, every warm run different: validate marks the warm ones `flapping`, they leave p50, nothing was reported."""
    rows = partial_rows("pr", range(1, 6), reason="flapping (differs from cell's first valid result)")
    c = report.summarize(raw(rows), "t")["cells"][0]
    assert c["verdict"] == "unstable" and c["p50"]["pr"] is None and c["n_invalid"]["pr"] == 5


def test_b1_four_of_six_pr_requests_failing_is_unstable_and_a_base_with_the_same_failures_is_not():
    rows = partial_rows("pr", (1, 2, 3, 4))
    assert report.summarize(raw(rows), "t")["cells"][0]["verdict"] == "unstable"
    both = partial_rows("pr", (1, 2))
    for r in both:
        if r["target"] == "base" and r["run"] in (1, 2):
            r.update(valid=False, reason="http 500", status=500, result="")
    assert report.summarize(raw(both), "t")["cells"][0]["verdict"] in ("same", "slower", "faster")  # the base is as flaky: not the PR's doing


def test_b1_unstable_fails_the_check_and_is_listed():
    import comment
    s = report.summarize(raw(partial_rows("pr", (1, 2, 3, 4))), "t")
    text, v = comment.render_perf(s, {"changed": 1, "sets": {}, "why": {}}, {"base": "a" * 40, "head": "b" * 40})
    assert v["exit"] == 1 and v["failing"]["unstable"] == 1 and "UNSTABLE" in text and "error, empty or flapping answer" in text


def test_m2_an_invalid_hot_answer_does_not_hide_a_pr_answer_that_changed():
    rows = cell_rows(K, 10, 10, base_result="17132", pr_result="1")
    for r in rows:
        if r["target"] == "ref":
            r.update(valid=False, reason="http 500", status=500, result="")
    s = report.summarize(raw(rows), "t")
    c = s["cells"][0]
    assert c["ref_invalid"] and c["vs_ref"] == {"base": "n/a", "pr": "n/a"} and c["difference"] == "new"
    assert s["ref_invalid"] == [K]
    import comment
    text, v = comment.render_perf(s, {"changed": 1, "sets": {}, "why": {}}, {"base": "a" * 40, "head": "b" * 40})
    # one such cell is listed and the PR/base difference fails; it does not invalidate the run (N3)
    assert v["exit"] == 1 and "gave no valid answer" in text and v["ref_invalid"] == [K] and v["failing"]["new_difference"] == 1


def test_m3_a_remeasure_cannot_erase_a_new_difference_and_the_totals_follow_the_cells():
    first = report.summarize(raw(cell_rows(K, 10, 25, pr_result="43")), "t")   # slower AND a new difference
    assert first["cells"][0]["verdict"] == "slower" and first["cells"][0]["difference"] == "new"
    again = report.summarize(raw(cell_rows(K, 10, 10, runs=7)), "again")        # the re-measure's answers match hot
    merged = report.merge_confirm(first, again)
    assert merged["cells"][0]["difference"] == "new" and merged["cells"][0]["first_pass_difference"] == "new"
    assert merged["differences"] == {"new": 1} and merged["verdicts"] == {"same": 1}


def test_s3_cost_names_the_metric_that_changed():
    s3 = {K: {"base": {"cold": {"gets": 40, "requests": 40, "bytes": 0.19e6}, "warm_per_query": None},
              "pr": {"cold": {"gets": 29, "requests": 29, "bytes": 7.45e6}, "warm_per_query": None}}}
    c = report.summarize(raw(cell_rows(K, 10, 10), s3), "t")["cells"][0]
    assert c["s3_why"]["cold"] == {"gets": "less", "requests": "less", "bytes": "more"}
    import comment
    assert comment.s3_flag(c) == "less GETs (cold); less requests (cold); more bytes (cold)"


def test_n2_a_transient_pr_error_that_does_not_reproduce_is_flaky_not_failing_and_one_that_reproduces_still_fails():
    first = report.summarize(raw(partial_rows("pr", (3,))), "t")           # 1 HTTP 500 in 6 PR requests
    assert first["cells"][0]["verdict"] == "unstable"
    clean = report.summarize(raw(cell_rows(K, 10, 10, runs=7)), "again")
    merged = report.merge_confirm(first, clean)
    c = merged["cells"][0]
    assert c["verdict"] == "same" and c["flaky"] and c["first_pass"] == "unstable" and merged["flaky"] == [K]
    first = report.summarize(raw(partial_rows("pr", (3,))), "t")
    again = report.summarize(raw(partial_rows("pr", (2, 5), runs=7)), "again")   # it happens again
    merged = report.merge_confirm(first, again)
    assert merged["cells"][0]["verdict"] == "unstable" and not merged["cells"][0].get("flaky") and merged["flaky"] == []


def test_n3_a_shape_that_is_empty_on_every_target_is_vacuous_not_an_invalid_cell():
    rows = cell_rows(K, 10, 10)
    for r in rows:
        r.update(valid=False, reason="empty result", status=200, result="")
    c = report.summarize(raw(rows), "t")["cells"][0]
    assert c["vacuous"] is True and not c["ref_invalid"]


def test_window_shift_moves_the_end_earlier_inside_the_layer_and_is_ignored_on_a_tiny_one():
    import measure
    st = {"layers": {"cold": {"start": "2026-10-12T11:00:00Z", "end": "2026-10-12T13:00:00Z"}, "tiny": {"start": "2026-10-12T12:59:00Z", "end": "2026-10-12T13:00:00Z"}}}
    ranges = {"1h": 3600, "full": None}
    assert measure.window_for(st, "cold", "1h", ranges, 300) == ("2026-10-12T11:55:00Z", "2026-10-12T12:55:00Z")
    assert measure.window_for(st, "cold", "full", ranges, 300) == ("2026-10-12T11:00:00Z", "2026-10-12T12:55:00Z")
    assert measure.window_for(st, "cold", "1h", ranges) == ("2026-10-12T12:00:00Z", "2026-10-12T13:00:00Z")
    assert measure.window_for(st, "tiny", "full", ranges, 300) == ("2026-10-12T12:59:00Z", "2026-10-12T13:00:00Z")


def test_a_sampled_shape_is_never_held_to_its_first_answer_and_never_compared():
    """Found by the real A/A run: Tempo's tag list differs between calls of one system, so the flapping rule made A/A cells `new`."""
    rows = cell_rows(K, 10, 10, base_result="41", pr_result="43")
    c = report.summarize(raw(rows), "t", sample_skip=frozenset({"logs count_total"}))["cells"][0]
    assert c["difference"] == "none" and c["vs_ref"] == {"base": "n/a", "pr": "n/a"} and c["verdict"] == "same"
    spec = {"sets": {"s": {"shapes": [{"name": "a", "sample": True}, {"name": "b"}]}}}
    assert report.sample_skip_from(spec) == frozenset({"a"})
    out = report.apply_ref_skip(report.summarize(raw(rows), "t"), frozenset(), frozenset({"logs count_total"}))
    assert out["differences"] == {"none": 1}
