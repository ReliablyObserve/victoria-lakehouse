"""comment.py: the sticky comments are verdict first, only changed rows, and end with the known differences."""
import comment
import known
import report
from helpers import cell_rows, raw

K1 = "control-logs|logs count_total|cold|1h|numeric"
K2 = "control-logs|logs level_filter|cold|1h|numeric"
SEL = {"run": True, "changed": 2, "sets": {"control-logs": ["logs count_total"]}, "why": {"control-logs/logs count_total": ["internal/x/y.go"]}}
META = {"base": "a" * 40, "head": "b" * 40, "versions": {"VictoriaLogs": "v1.53.0"}, "latency": [{"expect_ms": 0, "added_ms": 0.3, "ok": True}], "elapsed_s": 600}


def summary(*cells):
    rows = []
    for key, base, pr, kw in cells:
        rows += cell_rows(key, base, pr, shape=key.split("|")[1], **kw)
    return report.summarize(raw(rows), "t")


def test_a_quiet_pr_is_neutral_collapses_the_cells_and_exits_zero():
    s = summary((K1, 10, 10, {}), (K2, 10, 10, {}))
    text, v = comment.render_perf(s, SEL, META, known.join([], [], {}))
    assert text.splitlines()[0] == comment.PERF_MARKER and text.splitlines()[1].startswith("### NEUTRAL")
    assert "2 unchanged cells" in text and "Every cell is within noise" in text and v["exit"] == 0 and v["state"] == "passed"
    assert "#### Known differences and gaps" in text


def test_a_confirmed_slowdown_fails_and_is_shown_first():
    s = summary((K1, 10, 10, {}), (K2, 10, 30, {}))
    text, v = comment.render_perf(s, SEL, META)
    assert "### RED" in text.splitlines()[1] and v["exit"] == 1
    assert "A confirmed regression" in text and "| SLOWER |" in text and text.index("level_filter") < text.index("unchanged cells")


def test_a_new_difference_from_the_reference_fails_and_a_preexisting_one_does_not():
    s = summary((K1, 10, 10, {"pr_result": "43"}))
    text, v = comment.render_perf(s, SEL, META)
    assert v["exit"] == 1 and v["new_differences"] == 1 and "differs, new in this PR" in text
    pre = summary((K1, 10, 10, {"base_result": "41", "pr_result": "41"}))
    text, v = comment.render_perf(pre, SEL, META)
    assert v["exit"] == 0 and "differs (as base)" in text


def test_untracked_known_difference_fails_the_check():
    pre = summary((K1, 10, 10, {"base_result": "41", "pr_result": "41"}))
    k = known.join(known.observed_from_perf(pre, {"sets": {"control-logs": {"shapes": [{"name": "logs count_total", "covers": ["vl.x"]}]}}}),
                   [{"id": "vl.x", "expect": "pass"}], {})
    text, v = comment.render_perf(pre, SEL, META, k)
    assert v["exit"] == 1 and "UNTRACKED" in text


def test_more_s3_gets_on_a_control_shape_fails_even_when_latency_is_flat():
    s3 = {K1: {"base": {"cold": {"gets": 10, "requests": 10, "bytes": 1e6}, "warm_per_query": None},
               "pr": {"cold": {"gets": 20, "requests": 20, "bytes": 2e6}, "warm_per_query": None}}}
    s = report.summarize(raw(cell_rows(K1, 10, 10, shape="logs count_total"), s3), "t")
    text, v = comment.render_perf(s, SEL, META)
    assert v["exit"] == 1 and "More S3 GETs or bytes on a control shape" in text and "10 -> 20" in text


def test_a_cell_with_no_valid_answer_and_a_failed_probe_make_the_run_invalid():
    rows = cell_rows(K2, 10, 10, shape="logs level_filter")
    for r in rows:
        if r["target"] != "ref":
            r.update(valid=False, reason="http 500", result="")
    text, v = comment.render_perf(report.summarize(raw(rows), "t"), SEL, META)
    assert v["exit"] == 2 and "invalid run" in text
    bad = dict(META, latency=[{"expect_ms": 50, "added_ms": 0.2, "ok": False}])
    text, v = comment.render_perf(summary((K1, 10, 10, {})), SEL, bad)
    assert v["exit"] == 2 and "latency probe failed" in text


def test_skipped_and_errored_comments():
    text, v = comment.render_perf(None, {"changed": 3}, META)
    assert "skipped" in text and v == {"state": "skipped", "failed": False, "exit": 0}
    text, v = comment.errored(SEL, META, "boom", comment.API_MARKER, "API")
    assert v["exit"] == 3 and "did not complete" in text and "boom" in text


def api_result(**kw):
    r = {"id": "vl.select.x", "row": "vl.select.x", "surface": "vl-native", "signal": "logs", "path": "/p", "verdict": "exact", "base_score": 100, "pr_score": 100,
         "base_worst": "", "pr_worst": "", "status": {"ref": "200", "base": "200", "pr": "200"}, "p50_ms": {"ref": 1, "base": 2, "pr": 2},
         "status_changed": False, "latency": "same", "notes": []}
    r.update(kw)
    return r


def matrix(results, skipped=None):
    s = {"requests": len(results), "verdicts": {}, "failing": [r["id"] for r in results if r["verdict"] in ("regressed",) or r["status_changed"]],
         "status_changes": [], "slower": [], "not_exercised": 3, "not_exercised_reasons": {"no HTTP request": 3}}
    for r in results:
        s["verdicts"][r["verdict"]] = s["verdicts"].get(r["verdict"], 0) + 1
    return {"results": results, "summary": s, "rounds": 3}


def test_api_matrix_lists_only_what_differs_and_counts_the_rest():
    m = matrix([api_result(), api_result(id="b", verdict="regressed", pr_score=40, pr_worst="row_set"), api_result(id="c", verdict="arbitrary")])
    text, v = comment.render_api(m, SEL, META)
    assert text.splitlines()[0] == comment.API_MARKER and "### RED" in text.splitlines()[1] and v["exit"] == 1
    assert "`b`" in text and "1 rows match the reference exactly" in text and "3 registry rows not exercised" in text
    assert "rows that prove nothing" in text and "#### Known differences and gaps" not in text  # no known.json given
    text, v = comment.render_api(matrix([api_result()]), SEL, META, known.join([], [], {}))
    assert v["exit"] == 0 and "NEUTRAL" in text and "#### Known differences and gaps" in text


def test_formatting_helpers():
    assert comment.ms(3.14159) == "3.1 ms" and comment.ms(12.4) == "12 ms" and comment.ms(1234) == "1.2 s" and comment.ms(None) == "-"
    assert comment.ms(5, "500") == "**500**" and comment.ratio(10, 5) == "2.0x" and comment.ratio(None, 5) == "-"
    assert comment.fmt_pair(10, 12) == "10 -> 12" and comment.fmt_pair(10, 10) == "10" and comment.fmt_pair(None, 3) == "-"
    assert comment.score(100.0) == "100%" and comment.score(99.999) == "100%" and comment.score(87.6) == "88%"


def test_m1_the_other_latency_profiles_reach_the_verdict():
    quiet = summary((K1, 10, 10, {}))
    at50 = summary((K1, 10, 50, {"pr_result": "43"}))   # 5x slower and a new difference, only at 50 ms
    text, v = comment.render_perf(quiet, SEL, META, known.join([], [], {}), None, {"50": at50})
    assert v["exit"] == 1 and v["failing"]["slower"] == 1 and v["failing"]["new_difference"] == 1
    assert "50 ms" in text.split("###")[1] and "### RED" in text.splitlines()[1]
    ok, v = comment.render_perf(quiet, SEL, META, known.join([], [], {}), None, {"50": summary((K1, 10, 10, {}))})
    assert v["exit"] == 0


def test_the_known_differences_of_an_extra_profile_are_part_of_the_known_section():
    spec = {"sets": {"control-logs": {"shapes": [{"name": "logs count_total", "covers": ["vl.x"]}]}}}
    at50 = summary((K1, 10, 10, {"base_result": "41", "pr_result": "41"}))
    obs = known.observed_from_perf(summary((K1, 10, 10, {})), spec, {"50": at50})
    assert obs and obs[0]["where"][0].endswith("/50 ms")


def test_the_s3_gate_on_every_shape_is_opt_in():
    s3 = {"enumeration|logs field_names|cold|1h|numeric": {"base": {"cold": {"gets": 6, "requests": 6, "bytes": 1e6}, "warm_per_query": None},
                                                            "pr": {"cold": {"gets": 6, "requests": 6, "bytes": 2e6}, "warm_per_query": None}}}
    key = "enumeration|logs field_names|cold|1h|numeric"
    s = report.summarize(raw(cell_rows(key, 10, 10, shape="logs field_names", set_="enumeration"), s3), "t")
    assert comment.render_perf(s, SEL, META)[1]["exit"] == 0
    text, v = comment.render_perf(s, SEL, META, gate_s3_all=True)
    assert v["exit"] == 1 and "more bytes (cold)" in text


def test_n2_n3_flaky_and_vacuous_cells_are_listed_without_failing_and_a_few_hot_gaps_do_not_invalidate_the_run():
    s = summary((K1, 10, 10, {}), (K2, 10, 10, {}))
    s["flaky"] = [K1]
    s["vacuous"] = [K2]
    text, v = comment.render_perf(s, SEL, META)
    assert v["exit"] == 0 and "FLAKY on base and PR" in text and "empty on every target" in text and v["flaky"] == [K1]
    few = dict(s, ref_invalid=[K1])
    text, v = comment.render_perf(few, SEL, META)
    assert v["exit"] == 0 and "asked twice" in text


def ref_cells(n_logs, n_traces, bad_logs, bad_traces):
    """summary cells of two signals, the first `bad_*` of each without a hot answer."""
    cells = []
    for sig, n, bad in (("logs", n_logs, bad_logs), ("traces", n_traces, bad_traces)):
        for i in range(n):
            c = summary((f"control-{sig}|{sig} count_total|cold|{i}h|numeric", 10, 10, {}))["cells"][0]
            c.update(signal=sig, ref_invalid=i < bad)
            cells.append(c)
    return cells


def test_n3_a_reference_that_is_broken_in_most_cells_invalidates_the_run():
    cells = ref_cells(10, 0, 5, 0)
    s = dict(summary((K1, 10, 10, {})), cells=cells, ref_invalid=[c["cell"] for c in cells if c["ref_invalid"]])
    text, v = comment.render_perf(s, SEL, META)
    assert v["exit"] == 2 and "reference stack is broken" in text


def test_r2_the_share_of_cells_without_a_hot_answer_is_counted_per_signal_with_a_10_percent_limit():
    """34 of 173 cells without a hot comparison used to pass; one signal can no longer hide behind the other."""
    assert comment.REF_INVALID_SHARE == 0.10
    assert comment.reference_broken(ref_cells(40, 40, 2, 2)) is False          # 2 of 40 in each: within max(2, 10 %)
    assert comment.reference_broken(ref_cells(40, 40, 5, 0)) is True           # 5 of 40 logs cells: over 4
    assert comment.reference_broken(ref_cells(100, 10, 0, 3)) is True          # 3 of 10 traces cells, though 3 of 110 overall
    assert comment.reference_broken(ref_cells(173, 0, 17, 0)) is False and comment.reference_broken(ref_cells(173, 0, 18, 0)) is True
    cells = ref_cells(10, 0, 2, 0)
    s = dict(summary((K1, 10, 10, {})), cells=cells, ref_invalid=[c["cell"] for c in cells if c["ref_invalid"]])
    text, v = comment.render_perf(s, SEL, META)
    assert v["exit"] == 0 and "asked twice" in text and v["ref_invalid"]
