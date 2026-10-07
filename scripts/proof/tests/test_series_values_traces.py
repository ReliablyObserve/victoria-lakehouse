import math

import pytest

from scripts.proof.metrics.common import UnknownShape
from scripts.proof.metrics.decode import (
    decode_hits, decode_prom, decode_tempo_metrics, decode_values, load_body, stats_scalar,
)
from scripts.proof.metrics.series import evaluate_series
from scripts.proof.metrics.trace import (
    decode_jaeger, decode_otlp, decode_tempo_search, evaluate_traces,
)
from scripts.proof.metrics.values import evaluate_values


def S(**series):
    return {k: dict(enumerate(v)) for k, v in series.items()}


# ---- M7
def test_series_identical_is_exact():
    s = S(a=[1, 2, 3], b=[4, 5, 6])
    assert evaluate_series(s, S(a=[1, 2, 3], b=[4, 5, 6])).exact


def test_series_empty_both_exact_and_one_side_empty_not():
    assert evaluate_series({}, {}).exact
    r = evaluate_series(S(a=[1]), {})
    assert r.facets["series_set"] == 0.0 and r.facets["points_within_tol"] == 0.0


def test_series_set_jaccard_and_extra_missing():
    r = evaluate_series(S(a=[1], b=[1]), S(a=[1], c=[1]))
    assert r.details["series_set"]["jaccard"] == pytest.approx(100 / 3)
    assert r.details["series_set"]["missing"] == 1 and r.details["series_set"]["extra"] == 1


def test_right_totals_wrong_distribution():
    ref = S(a=[1, 2, 3, 4])
    ans = S(a=[2, 1, 4, 3])
    r = evaluate_series(ref, ans)
    assert r.details["totals"]["delta"] == 0 and r.facets["totals"] == 100.0
    assert r.facets["points_within_tol"] == 0.0
    assert r.details["points_within_tol"]["rel_err_max"] == 1.0


def test_timestamp_alignment_and_missing_points_count_as_out():
    ref = {"a": {0: 1.0, 1: 2.0, 2: 3.0, 3: 4.0}}
    ans = {"a": {0: 1.0, 1: 2.0, 2: 3.0}}
    r = evaluate_series(ref, ans)
    assert r.facets["ts_alignment"] == 75.0
    assert r.facets["points_within_tol"] == 75.0


def test_error_percentiles():
    ref = {"a": {i: 100.0 for i in range(100)}}
    ans = {"a": {i: 100.0 + (1.0 if i < 10 else 0.0) for i in range(100)}}
    d = evaluate_series(ref, ans, rel_tol=1e-9).details["points_within_tol"]
    assert d["within"] == 90 and d["rel_err_p50"] == 0.0
    assert d["rel_err_p95"] == pytest.approx(0.01) and d["rel_err_max"] == pytest.approx(0.01)
    assert evaluate_series(ref, ans, rel_tol=0.02).exact  # row tolerance accepts it


def test_nan_and_inf_agreement():
    nan = float("nan")
    r = evaluate_series({"a": {0: nan, 1: 1.0}}, {"a": {0: nan, 1: 1.0}})
    assert r.exact
    r = evaluate_series({"a": {0: nan, 1: 1.0}}, {"a": {0: 0.0, 1: 1.0}})
    assert r.facets["nan_agreement"] == 50.0 and r.facets["points_within_tol"] == 50.0
    assert evaluate_series({"a": {0: math.inf}}, {"a": {0: math.inf}}).exact
    r = evaluate_series({"a": {0: 1.0}}, {"a": {0: math.inf}})
    assert r.details["points_within_tol"]["infinite_errors"] == 1


def test_huge_values_relative():
    assert evaluate_series({"a": {0: 1e300}}, {"a": {0: 1e300}}).exact
    assert not evaluate_series({"a": {0: 1e300}}, {"a": {0: 1.1e300}}).exact


def test_decoders_prom_hits_tempo():
    prom = {"data": {"resultType": "matrix", "result": [{"metric": {"l": "x"}, "values": [[1, "2"], [2, "NaN"]]}]}}
    s = decode_prom(prom)
    (k, pts), = s.items()
    assert pts[1_000_000_000] == 2.0 and math.isnan(pts[2_000_000_000])
    vec = {"data": {"resultType": "vector", "result": [{"metric": {}, "value": [5, "7"]}]}}
    assert stats_scalar(vec) == 7.0
    assert stats_scalar({"data": {"resultType": "vector", "result": []}}) is None
    assert stats_scalar(None) is None
    hits = {"hits": [{"fields": {"a": "b"}, "timestamps": ["2026-01-01T00:00:00Z"], "values": [3]}]}
    h = decode_hits(hits)
    assert list(next(iter(h.values())).values()) == [3.0]
    with pytest.raises(UnknownShape):
        decode_hits({"hits": [{"timestamps": ["bad"], "values": [1]}]})
    t = decode_tempo_metrics({"series": [{"labels": [{"key": "s", "value": {"stringValue": "a"}}],
                                          "samples": [{"timestampMs": "1000", "value": 2}]}]})
    assert next(iter(t.values())) == {1_000_000_000: 2.0}
    assert decode_prom(None) == {} and decode_hits(None) == {}
    for bad in ("not a dict", {"data": {"resultType": "scalar"}}, {"status": "success"}):
        with pytest.raises(UnknownShape):
            decode_prom(bad)
    with pytest.raises(UnknownShape):
        decode_hits({"nothits": 1})
    with pytest.raises(UnknownShape):
        decode_tempo_metrics({"nope": 1})


def test_load_body():
    assert load_body('{"a": 1}') == {"a": 1}
    assert load_body("a\nb") == "a\nb"
    assert load_body("{broken") == "{broken"
    assert load_body({"x": 1}) == {"x": 1}


# ---- M8
def test_values_recall_and_precision():
    ref = {f"f{i}": 1 for i in range(63)}
    ans = {f"f{i}": 1 for i in range(22)}
    r = evaluate_values(ref, ans)
    assert r.details["value_set"]["recall"] == pytest.approx(100 * 22 / 63)
    assert r.details["value_set"]["precision"] == 100.0
    assert r.facets["value_set"] == pytest.approx(34.92, abs=0.01)
    assert len(r.details["value_set"]["missing"]) == 20  # listing is capped


def test_values_hits_equality_and_error():
    ref = {"a": 100, "b": 200, "c": 300, "d": None}
    ans = {"a": 100, "b": 210, "c": 300, "d": None}
    r = evaluate_values(ref, ans)
    assert r.facets["hits_equality"] == pytest.approx(100 * 2 / 3)
    assert r.details["hits_equality"]["rel_err_p95"] == pytest.approx(0.05)
    assert r.facets["value_set"] == 100.0


def test_values_without_hits_have_no_hits_facet_and_empty_is_exact():
    assert "hits_equality" not in evaluate_values({"a": None}, {"a": None}).facets
    assert evaluate_values({}, {}).exact


def test_decode_values_shapes():
    assert decode_values({"values": [{"value": "x", "hits": "5"}]}) == {"x": 5.0}
    assert decode_values({"data": ["a", "b"]}) == {"a": None, "b": None}
    assert decode_values({"data": [{"name": "op", "spanKind": "server"}]}) == {"op": None}
    assert decode_values({"tagNames": ["x"]}) == {"x": None}
    assert decode_values({"scopes": [{"name": "span", "tags": ["a"]}]}) == {"span.a": None}
    assert decode_values(["a", 1]) == {"a": None, "1": None}
    assert decode_values({"data": [{"operationName": "o"}]}) == {"o": None}
    assert decode_values({"data": [{"tagValues": ["v"]}]}) == {"v": None}
    assert decode_values(None) == {}


# ---- M9
def jt(tid="a" * 32, n=3, events=True, links=True, scope=True, parent_of=None):
    spans = []
    for i in range(n):
        sp = {"traceID": tid, "spanID": f"s{i}", "operationName": f"op{i}", "startTime": 1000 + i, "duration": 5,
              "processID": "p", "tags": [{"key": "k", "value": i}], "logs": [], "references": []}
        par = (parent_of or {}).get(i, i - 1 if i else None)
        if par is not None:
            sp["references"].append({"refType": "CHILD_OF", "spanID": f"s{par}", "traceID": tid})
        if scope:
            sp["tags"].append({"key": "otel.scope.name", "value": "lib"})
        if events and i == 0:
            sp["logs"].append({"timestamp": 5, "fields": [{"key": "event", "value": "boom"}]})
        if links and i == 1:
            sp["references"].append({"refType": "FOLLOWS_FROM", "traceID": "b" * 32, "spanID": "x"})
        spans.append(sp)
    return {"data": [{"traceID": tid, "spans": spans, "processes": {"p": {"serviceName": "svc"}}}]}


def test_trace_identical_exact_and_decode_fields():
    t = decode_jaeger(jt())
    sp = t["a" * 32]["s0"]
    assert sp["service"] == "svc" and sp["scope"] == "lib" and sp["start"] == 1_000_000
    assert sp["events"][0]["name"] == "boom" and t["a" * 32]["s1"]["links"][0]["span_id"] == "x"
    assert evaluate_traces(t, decode_jaeger(jt())).exact


def test_trace_missing_events_links_scope_named():
    r = evaluate_traces(decode_jaeger(jt()), decode_jaeger(jt(events=False, links=False, scope=False)))
    pf = r.details["span_fields"]["per_field"]
    assert pf["events"] < 100 and pf["links"] < 100 and pf["scope"] == 0.0 and pf["name"] == 100.0
    assert r.details["span_fields"]["min_field"] == "scope"
    assert r.facets["span_set"] == 100.0 and r.facets["parent_links"] == 100.0


def test_trace_parent_link_agreement_and_span_count_delta():
    r = evaluate_traces(decode_jaeger(jt(n=4)), decode_jaeger(jt(n=4, parent_of={3: 0})))
    assert r.facets["parent_links"] == 75.0
    r = evaluate_traces(decode_jaeger(jt(n=4)), decode_jaeger(jt(n=3)))
    assert r.facets["span_set"] == 75.0 and r.details["span_count"]["delta"] == -1
    assert r.facets["span_count"] == 75.0


def test_trace_set_jaccard_and_search_only():
    ref = {"t1": {}, "t2": {}, "t3": {}}
    r = evaluate_traces(ref, {"t1": {}, "t2": {}})
    assert r.facets["trace_set"] == pytest.approx(100 * 2 / 3) and set(r.facets) == {"trace_set"}
    assert evaluate_traces({}, {}).exact
    assert decode_tempo_search({"traces": [{"traceID": "x"}]}) == {"x": {}}
    assert decode_tempo_search(None) == {}


def otlp(tid="c" * 32, events=True, links=True, scope="lib", status="STATUS_CODE_OK"):
    sp = {"traceId": tid, "spanId": "s1", "parentSpanId": "", "name": "n", "kind": "SPAN_KIND_SERVER",
          "startTimeUnixNano": "100", "endTimeUnixNano": "160", "attributes": [{"key": "a", "value": {"intValue": "3"}}],
          "status": {"code": status}}
    if events:
        sp["events"] = [{"timeUnixNano": "110", "name": "e", "attributes": []}]
    if links:
        sp["links"] = [{"traceId": "d" * 32, "spanId": "l1"}]
    return {"trace": {"resourceSpans": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "svc"}}]},
                                         "scopeSpans": [{"scope": {"name": scope} if scope else {}, "spans": [sp]}]}]}}


def test_otlp_decode_and_compare():
    t = decode_otlp(otlp())
    sp = t["c" * 32]["s1"]
    assert sp["duration"] == 60 and sp["attributes"] == {"a": 3} and sp["service"] == "svc" and sp["parent"] is None
    assert evaluate_traces(t, decode_otlp(otlp())).exact
    r = evaluate_traces(t, decode_otlp(otlp(events=False, links=False, scope=None, status="STATUS_CODE_ERROR")))
    pf = r.details["span_fields"]["per_field"]
    assert pf["events"] == pf["links"] == pf["scope"] == pf["status"] == 0.0
    assert decode_otlp(None) == {} and decode_otlp({"batches": []}) == {}


def test_jaeger_otlp_decoders_survive_junk():
    assert decode_jaeger(None) == {} and decode_jaeger({"data": None}) == {}
    assert decode_jaeger({"data": [{"traceID": "t", "spans": [{"spanID": "s"}]}]})["t"]["s"]["service"] is None
