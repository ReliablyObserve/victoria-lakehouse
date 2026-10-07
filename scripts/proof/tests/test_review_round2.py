"""Behaviours added after the independent review: one test per finding."""
import copy
import json
import math
import os

import pytest

from scripts.proof.metrics import CORE_SURFACES
from scripts.proof.metrics.__main__ import main
from scripts.proof.metrics.cases import evaluate_case
from scripts.proof.metrics.common import (
    LH_SURFACES, SURFACE_SIGNAL, UnknownShape, canon, display_pct, numbers_equal,
)
from scripts.proof.metrics.decode import (
    decode_values, loki_streams_rows, stats_vector,
)
from scripts.proof.metrics.evaluate import answer_empty, evaluate_body, evaluate_request
from scripts.proof.metrics.generic import evaluate_truth, latency_ratio
from scripts.proof.metrics.rows import evaluate_count_vector, evaluate_rows
from scripts.proof.metrics.trace import decode_jaeger, decode_otlp, evaluate_traces
from scripts.proof.metrics.verdict import (
    EXCLUDED_SAMPLES, classify, classify_samples,
)
from scripts.proof.metrics.common import FacetResult

HERE = os.path.dirname(__file__)
REC = os.path.join(HERE, "..", "fixtures", "recorded")
FIX = os.path.join(HERE, "..", "fixtures", "cases")


def recorded(name):
    return json.load(open(os.path.join(REC, name + ".json")))["body"]


def A(body, status=200, ms=10.0, **kw):
    return {"status": status, "latency_ms": ms, "body": body, **kw}


def nd(rows):
    return "\n".join(json.dumps(r) for r in rows)


ROWS = [{"_time": "2026-01-01T00:00:0%dZ" % i, "_stream_id": "s", "_msg": "m%d" % i, "level": "info"} for i in range(4)]
META = {"id": "x", "surface": "vl-native", "kind": "rows"}


# ---- H1/H2: every field the VT fix touched is seen, on both trace decoders (recorded VT v0.12.0 bodies)
def _jaeger():
    return json.loads(recorded("jaeger_trace"))


def _span0(tr):
    return tr["data"][0]["spans"][0]


def _set_tag(sp, key, value):
    for t in sp["tags"]:
        if t["key"] == key:
            t["value"] = value
            return
    sp["tags"].append({"key": key, "type": "string", "value": value})


def _j_name(t): _span0(t)["operationName"] += "x"
def _j_service(t): t["data"][0]["processes"][_span0(t)["processID"]]["serviceName"] += "x"
def _j_start(t): _span0(t)["startTime"] += 1
def _j_duration(t): _span0(t)["duration"] += 1
def _j_status(t): _set_tag(_span0(t), "error", "true")
def _j_status_message(t): _set_tag(_span0(t), "otel.status_description", "changed")
def _j_kind(t): _set_tag(_span0(t), "span.kind", "producer")
def _j_attributes(t): _set_tag(_span0(t), "http.method", "POST")
def _j_scope(t): _set_tag(_span0(t), "otel.scope.name", "other")
def _j_scope_version(t): _set_tag(_span0(t), "otel.scope.version", "9.9.9")
def _j_scope_attributes(t): _set_tag(_span0(t), "scope_attr:scope.team", "other")
def _j_resource(t): t["data"][0]["processes"][_span0(t)["processID"]]["tags"][0]["value"] += "x"
def _j_events(t): _span0(t)["logs"][0]["fields"][0]["value"] += "x"
def _j_links(t): _span0(t)["references"][0]["spanID"] = "ffffffffffffffff"


JAEGER_MUTATIONS = {f: v for f, v in [
    ("name", _j_name), ("service", _j_service), ("start", _j_start), ("duration", _j_duration),
    ("status", _j_status), ("status_message", _j_status_message), ("kind", _j_kind),
    ("attributes", _j_attributes), ("scope", _j_scope), ("scope_version", _j_scope_version),
    ("scope_attributes", _j_scope_attributes), ("resource", _j_resource), ("events", _j_events), ("links", _j_links)]}


@pytest.mark.parametrize("field", sorted(JAEGER_MUTATIONS))
def test_jaeger_change_in_each_field_is_detected(field):
    base = _jaeger()
    changed = copy.deepcopy(base)
    JAEGER_MUTATIONS[field](changed)
    r = evaluate_traces(decode_jaeger(base), decode_jaeger(changed))
    assert not r.exact
    assert r.details["span_fields"]["per_field"][field] < 100, r.details["span_fields"]


def test_jaeger_error_tag_is_the_status_and_recorded_fields_decode():
    sp = next(iter(decode_jaeger(_jaeger())["0af7651916cd43dd8448eb211c80319c"].values()))
    assert sp["status"] in ("true", "false")  # VT's error tag
    assert sp["scope"] == "proof-instrumentation" and sp["scope_version"] == "1.2.3"
    assert sp["scope_attributes"] == {"scope.team": "obs"}
    assert sp["resource"]["host.name"] == "host-1" and sp["service"] == "proof-cart"
    spans = decode_jaeger(_jaeger())["0af7651916cd43dd8448eb211c80319c"]
    assert any(s["status_message"] == "upstream failed" for s in spans.values())


def _otlp():
    return json.loads(recorded("tempo_trace_v2"))


def _o_span(t):
    for rs in t["trace"]["resourceSpans"]:
        for ss in rs["scopeSpans"]:
            for sp in ss["spans"]:
                if sp.get("events") and sp.get("links"):
                    return rs, ss, sp
    raise AssertionError("no span with events and links in the recorded trace")


def _o_name(t): _o_span(t)[2]["name"] += "x"
def _o_service(t):
    for kv in _o_span(t)[0]["resource"]["attributes"]:
        if kv["key"] == "service.name":
            kv["value"]["stringValue"] += "x"
def _o_start(t):
    sp = _o_span(t)[2]
    sp["startTimeUnixNano"] = str(int(sp["startTimeUnixNano"]) + 1)
def _o_duration(t):
    sp = _o_span(t)[2]
    sp["endTimeUnixNano"] = str(int(sp["endTimeUnixNano"]) + 1)
def _o_status(t): _o_span(t)[2].setdefault("status", {})["code"] = "STATUS_CODE_ERROR"
def _o_status_message(t): _o_span(t)[2].setdefault("status", {})["message"] = "changed"
def _o_kind(t): _o_span(t)[2]["kind"] = "SPAN_KIND_PRODUCER"
def _o_attributes(t): _o_span(t)[2]["attributes"][0]["value"]["stringValue"] += "x"
def _o_scope(t): _o_span(t)[1]["scope"]["name"] += "x"
def _o_scope_version(t): _o_span(t)[1]["scope"]["version"] = "9.9.9"
def _o_scope_attributes(t): _o_span(t)[1]["scope"]["attributes"][0]["value"]["stringValue"] += "x"
def _o_resource(t):
    for kv in _o_span(t)[0]["resource"]["attributes"]:
        if kv["key"] == "host.name":
            kv["value"]["stringValue"] += "x"
def _o_events(t): _o_span(t)[2]["events"][0]["attributes"][0]["value"]["stringValue"] += "x"
def _o_links(t): _o_span(t)[2]["links"][0]["attributes"][0]["value"]["stringValue"] += "x"


OTLP_MUTATIONS = {f: v for f, v in [
    ("name", _o_name), ("service", _o_service), ("start", _o_start), ("duration", _o_duration),
    ("status", _o_status), ("status_message", _o_status_message), ("kind", _o_kind),
    ("attributes", _o_attributes), ("scope", _o_scope), ("scope_version", _o_scope_version),
    ("scope_attributes", _o_scope_attributes), ("resource", _o_resource), ("events", _o_events), ("links", _o_links)]}


@pytest.mark.parametrize("field", sorted(OTLP_MUTATIONS))
def test_otlp_change_in_each_field_is_detected(field):
    base = _otlp()
    changed = copy.deepcopy(base)
    OTLP_MUTATIONS[field](changed)
    r = evaluate_traces(decode_otlp(base), decode_otlp(changed))
    assert not r.exact
    assert r.details["span_fields"]["per_field"][field] < 100, r.details["span_fields"]


def test_otlp_link_flags_and_trace_state_are_compared():
    base = _otlp()
    for flag_field, val in (("flags", 7), ("traceState", "other=1")):
        changed = copy.deepcopy(base)
        _o_span(changed)[2]["links"][0][flag_field] = val
        assert not evaluate_traces(decode_otlp(base), decode_otlp(changed)).exact


def test_vt_row_fields_are_the_real_names():
    """The recorded VT row for a span carries the field names the loss in #430 was about."""
    row = json.loads(recorded("vt_query_trace_rows").splitlines()[0])
    for k in ("_msg", "scope_name", "scope_version", "start_time_unix_nano", "end_time_unix_nano", "flags",
              "dropped_attributes_count", "dropped_events_count", "dropped_links_count", "status_code"):
        assert k in row, k
    assert row["_msg"] == "-"
    assert any(k.startswith("event:event_name:") for k in row)
    assert any(k.startswith("event:event_attr:") for k in row)
    assert any(k.startswith("link:link_trace_id:") for k in row)
    assert any(k.startswith("link:link_attr:") for k in row)
    assert any(k.startswith("scope_attr:") for k in row)


# ---- H3: an error on one side
def test_error_on_one_side_scores_the_body_facets_zero():
    case = evaluate_case(META, {"ref": A(nd(ROWS)), "base": A("boom", 500), "pr": A(nd(ROWS[:3]))})
    assert case.verdict == "improved"
    for f in ("row_set", "field_coverage", "value_equality", "count"):
        assert case.base.facets[f] == 0.0, f
    assert case.base.facets["status"] == 0.0


def test_pr_error_with_good_base_is_regressed_and_two_different_errors_score_status_only():
    case = evaluate_case(META, {"ref": A(nd(ROWS)), "base": A(nd(ROWS)), "pr": A("boom", 500)})
    assert case.verdict == "regressed"
    both = evaluate_body(META, A("a", 400), A("b", 400))
    assert set(both.facets) == {"status", "error"}


@pytest.mark.parametrize("kind,body", [
    ("series_prom", {"data": {"resultType": "vector", "result": [{"metric": {"a": "b"}, "value": [1, "5"]}]}}),
    ("series_hits", {"hits": [{"fields": {}, "timestamps": ["2026-01-01T00:00:00Z"], "values": [3]}]}),
    ("values", {"values": [{"value": "a", "hits": 1}]}),
    ("count", {"data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, "5"]}]}}),
    ("trace_jaeger", json.loads(recorded("jaeger_trace"))),
    ("trace_otlp", json.loads(recorded("tempo_trace_v2"))),
    ("tempo_search", json.loads(recorded("tempo_search"))),
    ("json", {"a": 1}),
    ("schema", {"a": 1}),
])
def test_error_on_one_side_never_scores_100_on_any_kind(kind, body):
    r = evaluate_body({**META, "kind": kind}, A(body), A("boom", 500))
    body_facets = {k: v for k, v in r.facets.items() if k not in ("status", "error")}
    assert body_facets and all(v == 0.0 for v in body_facets.values()), body_facets


# ---- M1: skip fields and the recorded tie-cut decision
def test_tie_decision_recorded_by_the_runner_is_consumed():
    t = "2026-01-01T00:00:50.000000000Z"
    g = [{"_time": t, "_stream_id": "s", "_msg": c} for c in "AB"]
    head = [{"_time": "2026-01-01T00:00:5%d.000000000Z" % i, "_stream_id": "s", "_msg": "h%d" % i} for i in (3, 2, 1)]
    ref, sut = head + [g[0]], head + [g[1]]
    ok = evaluate_rows(ref, sut, order=["_time"], tie_decision={"explained": True, "why": ""})
    assert ok.exact and any("tie-cut accepted" in n for n in ok.notes)
    no = evaluate_rows(ref, sut, order=["_time"], tie_decision={"explained": False, "why": "group differs"})
    assert not no.exact and any("group differs" in n for n in no.notes)
    # the decision rides in the answer envelope
    meta = {**META, "order": ["_time"]}
    b, p = evaluate_request(meta, {"ref": A(nd(ref)), "base": A(nd(ref)), "pr": {**A(nd(sut)), "tie_cut": {"explained": True}}})
    assert p.exact and not evaluate_request(meta, {"ref": A(nd(ref)), "base": A(nd(ref)), "pr": A(nd(sut))})[1].exact


# ---- M2: unknown shapes are errors; the new shapes decode
def test_unknown_shapes_become_harness_errors():
    for kind, body in (("values", {"unexpected": 1}), ("series_hits", {"x": 1}), ("trace_jaeger", {"nodata": 1}),
                       ("tempo_search", {"x": 1}), ("trace_otlp", {"x": 1}), ("series_prom", {"data": {"resultType": "scalar"}}),
                       ("rows", "<html>oops</html>"), ("series_tempo", {"x": 1})):
        meta = {**META, "kind": kind}
        assert evaluate_case(meta, {"ref": A(body), "base": A(body), "pr": A(body)}).verdict == "harness-error", kind


def test_an_unknown_value_shape_on_one_side_only_is_a_harness_error():
    with pytest.raises(UnknownShape):
        decode_values({"unexpected": 1})
    known = {"values": [{"value": "a", "hits": 1}]}
    case = evaluate_case({**META, "kind": "values"}, {"ref": A(known), "base": A(known), "pr": A({"unexpected": 1})})
    assert case.verdict == "harness-error" and "UnknownShape" in case.latency["error"]


def test_tempo_tag_values_facets_dependencies_and_loki_streams_decode():
    tv = json.loads(recorded("tempo_tag_values"))
    assert set(decode_values(tv)) == {x["value"] for x in tv["tagValues"]}
    dep = {"data": [{"parent": "a", "child": "b", "callCount": 3}, {"parent": "b", "child": "c", "callCount": 1}], "errors": None}
    assert decode_values(dep) == {"a->b": 3.0, "b->c": 1.0}
    fac = json.loads(recorded("vl_facets"))
    out = decode_values(fac)
    assert out and all(k.count("=") >= 1 for k in out) and all(isinstance(v, float) for v in out.values())
    streams = {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"app": "a"}, "values": [["1", "x"], ["2", "y"]]}]}}
    rows = loki_streams_rows(streams)
    assert [r["_msg"] for r in rows] == ["x", "y"] and rows[0]["stream.app"] == "a"
    assert answer_empty("series_prom", A({"status": "success", "data": {"resultType": "streams", "result": []}}))
    with pytest.raises(UnknownShape):
        decode_values({"values": [["nested"]]})


def test_dependencies_changed_edge_is_detected():
    meta = {**META, "kind": "values", "surface": "jaeger"}
    mk = lambda n: {"data": [{"parent": "a", "child": "b", "callCount": n}], "errors": None}  # noqa: E731
    _, p = evaluate_request(meta, {"ref": A(mk(3)), "base": A(mk(3)), "pr": A(mk(4))})
    assert p.facets["hits_equality"] == 0.0 and p.facets["value_set"] == 100.0


def test_count_over_a_stats_by_vector():
    vec = json.loads(recorded("vl_stats_query_by"))
    d = stats_vector(vec)
    assert len(d) == 4 and sum(d.values()) == 300
    assert evaluate_count_vector(d, dict(d)).exact
    short = {k: v - 1 if i == 0 else v for i, (k, v) in enumerate(d.items())}
    r = evaluate_count_vector(d, short)
    assert r.facets["count"] < 100 and len(r.details["count"]["differing"]) == 1
    assert evaluate_count_vector(d, {k: v for k, v in list(d.items())[1:]}).facets["count"] == 0.0
    assert evaluate_count_vector({}, {}).exact
    nan = {"{}": math.nan}
    assert evaluate_count_vector(nan, dict(nan)).exact
    one = evaluate_count_vector({"{}": 100.0}, {"{}": 97.0})
    assert one.details["count"]["delta"] == -3 and one.facets["count"] == pytest.approx(97.0)


# ---- M3: lost hits
def test_lost_hits_score_zero():
    ref = {"values": [{"value": "a", "hits": 5}, {"value": "b", "hits": 3}]}
    r = evaluate_request({**META, "kind": "values"}, {"ref": A(ref), "base": A(ref), "pr": A({"values": [{"value": "a"}, {"value": "b"}]})})[1]
    assert r.facets["hits_equality"] == 0.0 and r.details["hits_equality"]["lost_hits"] == 2
    # one side without any hits and no value in common still scores 0, never a pass
    r2 = evaluate_request({**META, "kind": "values"}, {"ref": A(ref), "base": A(ref), "pr": A({"values": [{"value": "z"}]})})[1]
    assert r2.facets["hits_equality"] == 0.0


# ---- M4: the signal follows the surface
def test_signal_comes_from_the_surface_not_from_meta():
    row = '{"trace_id":"t","span_id":"1","name":"a"}'
    other = '{"trace_id":"t","span_id":"1","name":"b"}'
    vt = evaluate_body({"id": "x", "surface": "vt-native", "kind": "rows"}, A(row), A(other))
    assert vt.details["value_equality"]["paired_rows"] == 1  # span identity, no signal field in meta
    vl = evaluate_body(META, A(row), A(other))
    assert vl.details["value_equality"]["paired_rows"] == 0  # log identity
    with pytest.raises(ValueError):
        evaluate_case({**META, "signal": "traces"}, {})
    assert SURFACE_SIGNAL["lh-logs"] == "logs" and SURFACE_SIGNAL["lh-traces"] == "traces"


# ---- M5: latency counts only valid answers
def test_latency_excludes_wrong_answers_both_ways():
    case = evaluate_case(META, {"ref": A(nd(ROWS), ms=10), "base": A(nd(ROWS), ms=10), "pr": A(nd(ROWS[:1]), ms=1)})
    assert case.verdict == "regressed" and case.latency["invalid"]["pr"] == 1 and case.latency["pr_over_base"] is None
    fixed = evaluate_case(META, {"ref": A(nd(ROWS), ms=10), "base": A(nd(ROWS[:1]), ms=1), "pr": A(nd(ROWS), ms=12)})
    assert fixed.verdict == "fixed" and fixed.latency["invalid"]["base"] == 1
    ok = evaluate_case(META, {"ref": A(nd(ROWS), ms=10), "base": A(nd(ROWS), ms=10), "pr": A(nd(ROWS), ms=20)})
    assert ok.latency["pr_over_base"] == 2.0 and ok.latency["invalid"] == {"pr": 0, "base": 0, "ref": 0}
    assert latency_ratio([(1.0, True)], [(2.0, True)])["pr_over_base"] == 0.5


# ---- M6: Lakehouse-only surfaces
def test_lh_surfaces_are_core_and_lakehouse_only():
    assert set(LH_SURFACES) == {"lh-logs", "lh-traces"} <= set(CORE_SURFACES)
    meta = {"id": "o", "surface": "lh-logs", "kind": "schema"}
    # no reference needed: the PR is compared with base
    case = evaluate_case(meta, {"base": A({"n": 1}), "pr": A({"n": 2})})
    assert case.verdict == "exact"
    # and a core LH row that answers empty is a broken harness, not a pass
    assert evaluate_case(meta, {"base": A({}), "pr": A({})}).verdict == "harness-error"


def test_core_flag_includes_lh_surfaces_as_their_own_group(capsys):
    assert main([FIX, "--core"]) == 0
    out = capsys.readouterr().out
    assert "lh-logs/" in out and "lh-traces/" in out and "logs/lh-logs:" in out and "tempo/" not in out


def test_surface_and_core_together_is_an_error(capsys):
    with pytest.raises(SystemExit) as e:
        main([FIX, "--core", "--surface", "tempo"])
    assert e.value.code == 2


# ---- M8: re-sampling
def test_blocked_and_harness_error_samples_are_not_flakes():
    assert classify_samples(["regressed", "blocked"]) == "regressed"
    assert classify_samples(["blocked", "exact", "exact"]) == "exact"
    assert classify_samples(["exact", "harness-error", "regressed"]) == "nondeterministic"
    assert classify_samples(["blocked", "blocked"]) == "blocked"
    assert classify_samples(["harness-error", "blocked"]) == "harness-error"
    assert set(EXCLUDED_SAMPLES) == {"blocked", "harness-error"}
    first = {"ref": A(nd(ROWS)), "base": A(nd(ROWS)), "pr": A(nd(ROWS[:3]))}
    second = {"ref": A("down", 503), "base": A(nd(ROWS)), "pr": A(nd(ROWS[:3]))}
    case = evaluate_case(META, first, [second])
    assert case.verdict == "regressed" and case.excluded_samples == ["blocked"]


# ---- L2: canon
def test_canon_ints_stay_ints_and_inf_nan_do_not_overflow():
    assert canon(10 ** 30) == 10 ** 30 and isinstance(canon(10 ** 30), int)
    assert canon(2 ** 63 + 1) == 2 ** 63 + 1  # no float rounding of a big counter
    assert canon(float("inf")) == "+Inf" and canon(float("-inf")) == "-Inf" and canon(float("nan")) == "NaN"
    assert canon(1.0) == 1 and canon(1e300) == 1e300 and canon(True) is True
    assert canon({"a": float("inf")}) == {"a": "+Inf"}
    assert display_pct(float("nan")) == "NaN"


# ---- L5: tolerance boundary
def test_numbers_equal_tolerance_is_relative_to_the_larger_value():
    assert numbers_equal(1.0, 2.0, rel=0.6)  # |diff| 1 <= 0.6 * max(1, 2)
    assert not numbers_equal(1.0, 2.0, rel=0.4)
    assert numbers_equal(100.0, 100.0 * (1 + 0.5e-9), rel=1e-9)
    assert numbers_equal(-1.0, -2.0, rel=0.6)


# ---- L8
def test_row_score_tolerance_is_passed_to_the_classifier():
    meta = {**META, "kind": "series_prom"}
    mk = lambda v: {"data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, str(v)]}]}}  # noqa: E731
    # base 98 and PR 97 differ by 1 score point: regressed at the default tolerance, same under a wider one
    wide = evaluate_case({**meta, "score_tolerance": 50.0, "kind": "count"}, {"ref": A(mk(100)), "base": A(mk(98)), "pr": A(mk(97))})
    narrow = evaluate_case({**meta, "kind": "count"}, {"ref": A(mk(100)), "base": A(mk(98)), "pr": A(mk(97))})
    assert wide.verdict == "same" and narrow.verdict == "regressed"
    assert classify(FacetResult({"a": 90.0}), FacetResult({"a": 89.0}), tol=2.0) == "same"


def test_coverage_file_is_git_ignored():
    root = os.path.join(HERE, "..", "..", "..")
    lines = open(os.path.join(root, ".gitignore")).read().splitlines()
    assert ".coverage" in lines


# ---- M13 shapes bound to the real Lakehouse API keys
def test_truth_scalar_sum_over_groups():
    vec = json.loads(recorded("vl_stats_query_by"))
    ov = {"total_rows": 300, "total_files": 4}
    assert evaluate_truth("scalar_sum", ov, vec, lh_path="total_rows").exact
    r = evaluate_truth("scalar_sum", {"total_rows": 290}, vec, lh_path="total_rows", unflushed_rows=10)
    assert r.facets["truth"] == pytest.approx(100 * (1 - 10 / 300)) and r.details["unflushed_rows"] == 10


def test_truth_per_key_list_with_where_and_rel_tol():
    vec = json.loads(recorded("vl_stats_count_uniq"))
    lh = {"fields": [{"name": "level", "cardinality": 4, "indexed": True},
                     {"name": "http.method", "cardinality": 5, "indexed": True},
                     {"name": "skipped", "cardinality": 1, "indexed": False}]}
    kw = dict(lh_path="fields", key_field="name", value_field="cardinality", key_label="__name__", where={"indexed": True})
    assert evaluate_truth("per_key", lh, vec, **kw).exact
    off = {"fields": [dict(f, cardinality=f["cardinality"] + 1) if f["name"] == "http.method" else f for f in lh["fields"]]}
    assert not evaluate_truth("per_key", off, vec, **kw).exact
    assert evaluate_truth("per_key", off, vec, rel_tol=0.25, **kw).exact  # HLL tolerance
    without_filter = evaluate_truth("per_key", lh, vec, lh_path="fields", key_field="name", value_field="cardinality", key_label="__name__")
    assert not without_filter.exact  # the non-indexed field has no truth


def test_truth_tenant_set_real_shapes():
    hot = json.loads(recorded("vt_tenant_ids"))
    lh = {"tenants": [{"account_id": "0", "project_id": "0"}], "total_tenants": 1}
    assert evaluate_truth("tenant_set", lh, hot, lh_path="tenants").exact
    assert evaluate_truth("tenant_set", ["0:0"], [0]).exact  # a bare number is an account with project 0
