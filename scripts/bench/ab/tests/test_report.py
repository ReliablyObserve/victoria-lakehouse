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
