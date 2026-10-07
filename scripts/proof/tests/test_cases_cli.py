import json
import os
import time

import pytest

from scripts.proof.metrics import CORE_SURFACES, SURFACES
from scripts.proof.metrics.__main__ import main
from scripts.proof.metrics.cases import (
    case_dirs, check_expectations, evaluate_case, load_case, run_dir,
)
from scripts.proof.metrics.evaluate import answer_empty, evaluate_body
from scripts.proof.metrics.report import render_table
from scripts.proof.metrics.series import evaluate_series
from scripts.proof.metrics.rows import evaluate_rows

FIX = os.path.join(os.path.dirname(__file__), "..", "fixtures", "cases")


def A(body, status=200, ms=1.0, **kw):
    return {"status": status, "latency_ms": ms, "body": body, **kw}


# ---- the fixture corpus
def test_every_fixture_meets_its_recorded_expectation():
    cases = run_dir(FIX)
    assert len(cases) >= 30
    problems = {r.id: check_expectations(m, r) for m, r in cases if check_expectations(m, r)}
    assert problems == {}


def test_fixtures_cover_every_verdict_and_every_surface():
    cases = [r for _, r in run_dir(FIX)]
    assert {r.verdict for r in cases} == {
        "exact", "same", "fixed", "improved", "regressed", "not-reproduced-on-base", "vacuous", "blocked",
        "nondeterministic"}
    assert {r.surface for r in cases} == set(SURFACES)


def test_fixtures_are_native_first_and_core_is_most_complete():
    cases = [r for _, r in run_dir(FIX)]
    order = [SURFACES.index(r.surface) for r in cases]
    assert order == sorted(order)  # vl-native, vt-native, jaeger, then loki, tempo
    counts = {s: sum(1 for r in cases if r.surface == s) for s in SURFACES}
    core = sum(counts[s] for s in CORE_SURFACES)
    assert core > 3 * (counts["loki"] + counts["tempo"]) - 3
    assert counts["vl-native"] > counts["loki"] and counts["jaeger"] > counts["tempo"]


def test_known_gap_numbers_from_the_spec():
    got = {r.id: (m, r) for m, r in run_dir(FIX)}
    _, b2 = got["vt-native/b2_field_names_recall"]
    assert b2.base.details["value_set"]["recall"] == pytest.approx(34.92, abs=0.01)  # 22 of 63
    _, b5 = got["vl-native/b5_stats_range_distribution"]
    assert b5.base.details["totals"]["delta"] == 0 and b5.base.facets["points_within_tol"] < 100
    _, b8 = got["vl-native/b8_429_sort_order"]
    assert b8.base.facets["row_set"] == 100 and b8.base.facets["order"] < 100 and b8.pr.facets["order"] == 100
    _, c430 = got["jaeger/430_trace_events_links_scope"]
    assert c430.base.details["span_fields"]["per_field"]["events"] < 100
    assert c430.pr.details["span_fields"]["per_field"]["events"] == 100
    _, b1 = got["vl-native/b1_junk_fields"]
    assert b1.base.details["field_coverage"]["extra"] == ["<null>", "account_id", "ded_s0x"]


def test_no_fixture_mentions_internal_material():
    for dirpath, _, files in os.walk(FIX):
        for f in files:
            txt = open(os.path.join(dirpath, f)).read().lower()
            assert "claude" not in txt and "design-doc" not in txt


def test_case_dirs_skip_resample_subdirs():
    ids = [os.path.basename(d) for d in case_dirs(FIX)]
    assert "flaky_answer" in ids and not any(i.startswith("resample") for i in ids)


# ---- evaluate_case behaviours
META = {"id": "x", "surface": "vl-native", "kind": "rows"}


def test_missing_answer_is_harness_error():
    assert evaluate_case(META, {"base": A(""), "pr": A("")}).verdict == "harness-error"


def test_malformed_answer_is_harness_error_not_a_pass():
    meta = {**META, "kind": "series_prom"}
    r = evaluate_case(meta, {"ref": A({"data": {"result": [{"metric": {}, "values": [[]]}]}}), "base": A({}), "pr": A({})})
    assert r.verdict == "harness-error" and "error" in r.latency


def test_unknown_kind_and_surface_rejected():
    with pytest.raises(ValueError):
        evaluate_body({**META, "kind": "nope"}, A(""), A(""))
    with pytest.raises(ValueError):
        evaluate_case({**META, "surface": "nope"}, {})
    with pytest.raises(ValueError):
        evaluate_case({**META, "surface": "jaeger", "signal": "logs"}, {})


def test_blocked_variants_and_lh_only_blocked_by_base():
    rows = '{"_time":"2026-01-01T00:00:00Z","_msg":"a"}'
    for kw in ({"status": 502}, {"timeout": True}, {"warnings": ["partial"]}):
        ref = A(rows, **({"status": 200} | kw)) if "status" not in kw else A(rows, **kw)
        assert evaluate_case(META, {"ref": ref, "base": A(rows), "pr": A(rows)}).verdict == "blocked"
    meta = {**META, "kind": "schema", "lh_only": True}
    assert evaluate_case(meta, {"base": A({"a": 1}, status=503), "pr": A({"a": 1})}).verdict == "blocked"


def test_vacuous_needs_all_sides_empty_200():
    assert evaluate_case(META, {"ref": A(""), "base": A(""), "pr": A("")}).verdict == "vacuous"
    rows = '{"_time":"2026-01-01T00:00:00Z","_msg":"a"}'
    assert evaluate_case(META, {"ref": A(""), "base": A(rows), "pr": A("")}).verdict != "vacuous"
    # a shared error answer is not an empty 200
    err = A("bad", status=400)
    assert evaluate_case(META, {"ref": err, "base": err, "pr": err}).verdict == "exact"


def test_resamples_only_classify_never_retry_into_a_pass():
    rows = '{"_time":"2026-01-01T00:00:00Z","_msg":"a"}'
    bad = A("")
    good = {"ref": A(rows), "base": A(rows), "pr": A(rows)}
    flip = {"ref": A(rows), "base": A(rows), "pr": bad}
    assert evaluate_case(META, good, [good, good]).verdict == "exact"
    assert evaluate_case(META, good, [flip]).verdict == "nondeterministic"
    # a consistently regressed answer stays regressed; re-sampling never turns it into a pass
    assert evaluate_case(META, flip, [flip, flip]).verdict == "regressed"


def test_lh_only_truth_requires_truth_answers():
    meta = {**META, "kind": "schema", "lh_only": True, "truth": {"shape": "scalar", "lh_path": "n"}}
    assert evaluate_case(meta, {"base": A({"n": 1}), "pr": A({"n": 1})}).verdict == "harness-error"


def test_error_answers_are_scored_on_status_and_error_only():
    r = evaluate_body(META, A("x", status=400), A("x", status=400))
    assert r.exact and set(r.facets) == {"status", "error"}


@pytest.mark.parametrize("kind,empty,full", [
    ("rows", "", '{"a":1}'),
    ("series_prom", {"data": {"result": []}}, {"data": {"result": [{"metric": {}, "value": [1, "1"]}]}}),
    ("series_hits", {"hits": []}, {"hits": [{"fields": {}, "timestamps": ["2026-01-01T00:00:00Z"], "values": [1]}]}),
    ("series_tempo", {"series": []}, {"series": [{"labels": [], "samples": [{"timestampMs": "1", "value": 1}]}]}),
    ("values", {"values": []}, {"values": [{"value": "a", "hits": 1}]}),
    ("count", {"data": {"result": []}}, {"data": {"result": [{"metric": {}, "value": [1, "3"]}]}}),
    ("trace_jaeger", {"data": []}, {"data": [{"traceID": "t", "spans": []}]}),
    ("trace_otlp", {}, {"trace": {"resourceSpans": [{"scopeSpans": [{"spans": [{"traceId": "t", "spanId": "s"}]}]}]}}),
    ("tempo_search", {"traces": []}, {"traces": [{"traceID": "t"}]}),
    ("json", {}, {"a": 1}),
])
def test_answer_empty_per_kind(kind, empty, full):
    assert answer_empty(kind, A(empty)) and not answer_empty(kind, A(full))
    assert not answer_empty(kind, A("e", status=500))


def test_every_kind_evaluates():
    bodies = {"series_prom": {"data": {"result": []}}, "series_hits": {"hits": []}, "series_tempo": {"series": []},
              "values": {"values": []}, "count": {"data": {"result": []}}, "trace_jaeger": {"data": []},
              "trace_otlp": {}, "tempo_search": {"traces": []}, "json": {"a": 1}, "schema": {"a": 1}, "rows": ""}
    for kind, b in bodies.items():
        assert evaluate_body({**META, "kind": kind}, A(b), A(b)).exact, kind


def test_traces_rows_default_to_span_identity():
    meta = {**META, "signal": "traces"}
    a = '{"trace_id":"t","span_id":"1","name":"a"}'
    b = '{"trace_id":"t","span_id":"1","name":"b"}'
    r = evaluate_body(meta, A(a), A(b))
    assert r.details["value_equality"]["paired_rows"] == 1 and r.details["value_equality"]["min_field"] == "name"


def test_skip_fields_and_tolerance_from_meta():
    meta = {**META, "kind": "series_prom", "rel_tol": 0.1}
    mk = lambda v: {"data": {"result": [{"metric": {}, "value": [1, str(v)]}]}}  # noqa: E731
    assert evaluate_body(meta, A(mk(100)), A(mk(105))).exact
    assert not evaluate_body({**meta, "rel_tol": 1e-9}, A(mk(100)), A(mk(105))).exact
    rows_meta = {**META, "skip_fields": ["took"]}
    assert evaluate_body(rows_meta, A('{"took":1,"a":1}'), A('{"took":2,"a":1}')).exact


def test_check_expectations_reports_mismatches():
    meta, res = load_case(os.path.join(FIX, "vl-native", "stats_count_fixed"))
    assert check_expectations(meta, res) == []
    bad = {**meta, "expect": {"verdict": "exact", "base": {"count": 50.0}, "details": {"base.count.ref": 1, "pr.count.nope": 3}}}
    assert len(check_expectations(bad, res)) == 4
    assert check_expectations({}, res) == ["no expectation recorded"]


# ---- CLI
def test_cli_table_and_json(capsys):
    assert main([FIX, "--check"]) == 0
    out = capsys.readouterr().out
    assert "base%" in out and "FIXED" in out and "signal rollup" in out
    assert out.index("vl-native/b1_junk_fields") < out.index("jaeger/430") < out.index("tempo/430")
    assert main([FIX, "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert {d["surface"] for d in data} == set(SURFACES)


def test_cli_core_filter_and_exit_codes(capsys, tmp_path):
    assert main([FIX, "--core"]) == 0
    out = capsys.readouterr().out
    assert "tempo/" not in out and "loki/" not in out
    assert main([FIX, "--surface", "tempo"]) == 0
    assert "tempo/search_exact" in capsys.readouterr().out
    assert main([FIX, "--fail-on-regression"]) == 1
    assert main([FIX, "--surface", "loki", "--fail-on-regression"]) == 0
    capsys.readouterr()
    assert main([str(tmp_path)]) == 2  # no cases under that directory


def test_cli_check_fails_on_a_wrong_expectation(tmp_path, capsys):
    d = tmp_path / "c"
    d.mkdir()
    rows = '{"_time":"2026-01-01T00:00:00Z","_msg":"a"}'
    (d / "meta.json").write_text(json.dumps({"id": "c", "surface": "vl-native", "kind": "rows", "expect": {"verdict": "fixed"}}))
    for n in ("ref", "base", "pr"):
        (d / f"{n}.json").write_text(json.dumps(A(rows)))
    assert main([str(tmp_path), "--check"]) == 1
    assert "EXPECTATION c: verdict exact != expected fixed" in capsys.readouterr().err


def test_render_table_shows_base_to_pr_worst_facet():
    out = render_table([r for _, r in run_dir(FIX)])
    assert "points_within_tol 50 -> exact" in out
    assert "failing:" in out


# ---- performance: 100k rows, 10k series within a few seconds
def test_performance_100k_rows_and_10k_series():
    n = 100_000
    ref = [{"_time": f"2026-01-01T00:{(i // 60) % 60:02d}:{i % 60:02d}.{i % 1000:09d}Z", "_stream_id": "s%d" % (i % 50),
            "_msg": f"msg {i}", "level": "info" if i % 3 else "warn"} for i in range(n)]
    ans = [dict(r) for r in ref]
    for i in range(0, n, 997):
        ans[i]["level"] = "error"
    ans[1000:1100] = reversed(ans[1000:1100])
    t0 = time.perf_counter()
    res = evaluate_rows(ref, ans, order=["_time"])
    rows_s = time.perf_counter() - t0
    assert res.facets["value_equality"] < 100 and res.facets["order"] < 100

    pts = 100
    series = {f"s{i}": {t: float(i + t) for t in range(pts)} for i in range(10_000)}
    other = {k: dict(v) for k, v in series.items()}
    for i in range(0, 10_000, 100):
        other[f"s{i}"][3] += 1.0
    t0 = time.perf_counter()
    sres = evaluate_series(series, other)
    series_s = time.perf_counter() - t0
    assert sres.details["points_within_tol"]["within"] == 10_000 * pts - 100
    print(f"PERF rows100k={rows_s:.2f}s series10k_x{pts}pts={series_s:.2f}s")
    assert rows_s < 15 and series_s < 10
