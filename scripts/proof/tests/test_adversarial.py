"""Adversarial inputs from the independent review of the metrics library: each asserts the correct behaviour."""
import json

from scripts.proof.metrics.common import canon
from scripts.proof.metrics.decode import decode_values
from scripts.proof.metrics.evaluate import evaluate_request
from scripts.proof.metrics.cases import evaluate_case
from scripts.proof.metrics.trace import decode_jaeger, decode_otlp, evaluate_traces


def ans(body, status=200, ms=10.0):
    return {"status": status, "latency_ms": ms, "body": body}


def nd(rows):
    return "\n".join(json.dumps(r) for r in rows)


ROWS = [
    {"_time": "2026-01-01T00:00:0%dZ" % i, "_stream_id": "s", "_msg": "m%d" % i, "level": "info"}
    for i in range(4)
]


# F1: base 500 error, PR returns a partial answer: closer to ref, never "regressed"
def test_base_error_pr_partial_is_not_regressed():
    meta = {"id": "x", "surface": "vl-native", "kind": "rows"}
    case = evaluate_case(meta, {"ref": ans(nd(ROWS)), "base": ans("boom", 500), "pr": ans(nd(ROWS[:3]))})
    assert case.verdict in ("improved",), case.verdict


# F2: skip_fields + accepted tie cut: the re-read rows carry the skipped field
def test_tie_cut_with_skip_fields():
    g = [{"_time": "2026-01-01T00:00:00.000000005Z", "_stream_id": "s", "_msg": "tie " + c, "req_id": c} for c in "ABC"]
    head = [{"_time": "2026-01-01T00:00:0%dZ" % (9 - i), "_stream_id": "s", "_msg": "top%d" % i, "req_id": "h"} for i in range(3)]
    ref_rows, sut_rows = head + [g[0], g[1]], head + [g[0], g[2]]
    meta = {"id": "t", "surface": "vl-native", "kind": "rows", "order": ["_time"], "skip_fields": ["req_id"],
            "query": "* | sort by (_time) desc | limit 5", "tie_group": {"ref": nd(g), "sut": nd(g)}}
    b, p = evaluate_request(meta, {"ref": ans(nd(ref_rows)), "base": ans(nd(ref_rows)), "pr": ans(nd(sut_rows))})
    assert p.exact, (p.facets, p.notes)


# F3: field_values where the answer lost its hits: must not be exact
def test_values_missing_hits_not_exact():
    meta = {"id": "v", "surface": "vl-native", "kind": "values"}
    ref = {"values": [{"value": "a", "hits": 5}, {"value": "b", "hits": 3}]}
    bad = {"values": [{"value": "a"}, {"value": "b"}]}
    b, p = evaluate_request(meta, {"ref": ans(ref), "base": ans(ref), "pr": ans(bad)})
    assert not p.exact, p.facets


# F4: Tempo tag values (VT SearchTagValuesResponse): values must decode
def test_tempo_tag_values_decode():
    body = {"tagValues": [{"type": "string", "value": "cart"}, {"type": "string", "value": "pay"}], "metrics": {"inspectedBytes": "0"}}
    assert set(decode_values(body)) == {"cart", "pay"}


# F5: Loki streams result: different log lines must not compare equal
def test_loki_streams_lines_differ():
    a = {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"app": "a"}, "values": [["1790000000000000000", "line one"]]}]}}
    b = {"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"app": "b"}, "values": [["1790000000000000000", "totally different"]]}]}}
    meta = {"id": "l", "surface": "loki", "kind": "series_prom"}
    _, p = evaluate_request(meta, {"ref": ans(a), "base": ans(a), "pr": ans(b)})
    assert not p.exact, p.facets


# F6: VT Jaeger: scope version / status description / process tags changes must be visible
def _vt_jaeger(scope_version="1.0", status_desc="boom", proc_tags=None, link_attr=True):
    return {"data": [{"traceID": "t1", "processes": {"p1": {"serviceName": "svc", "tags": proc_tags or [
        {"key": "host.name", "type": "string", "value": "h1"}]}},
        "spans": [{"traceID": "t1", "spanID": "s1", "operationName": "op", "references": [],
                   "startTime": 1, "duration": 2, "processID": "p1", "logs": [], "warnings": None,
                   "tags": [{"key": "span.kind", "type": "string", "value": "server"},
                            {"key": "error", "type": "string", "value": "true"},
                            {"key": "otel.status_description", "type": "string", "value": status_desc},
                            {"key": "otel.scope.name", "type": "string", "value": "lib"},
                            {"key": "otel.scope.version", "type": "string", "value": scope_version}]}],
        "warnings": None}], "errors": None, "limit": 0, "offset": 0, "total": 1}


def test_jaeger_scope_version_change_visible():
    r = evaluate_traces(decode_jaeger(_vt_jaeger()), decode_jaeger(_vt_jaeger(scope_version="2.0")))
    assert not r.exact, r.facets


def test_jaeger_status_description_change_visible():
    r = evaluate_traces(decode_jaeger(_vt_jaeger()), decode_jaeger(_vt_jaeger(status_desc="")))
    assert not r.exact, r.facets


def test_jaeger_process_tags_change_visible():
    r = evaluate_traces(decode_jaeger(_vt_jaeger()), decode_jaeger(_vt_jaeger(proc_tags=[{"key": "host.name", "type": "string", "value": "OTHER"}])))
    assert not r.exact, r.facets


# F7: Tempo OTLP (VT TraceByIDV2JSON): scope attributes / link attributes / status message / resource attrs
def _otlp(scope_attrs=True, link_attrs=True, msg="m", res_extra="h1"):
    return {"trace": {"resourceSpans": [{"resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": "svc"}},
        {"key": "host.name", "value": {"stringValue": res_extra}}]},
        "scopeSpans": [{"scope": {"name": "lib", "version": "1",
                                  "attributes": [{"key": "k", "value": {"stringValue": "v"}}] if scope_attrs else []},
                        "spans": [{"traceId": "AAAA", "spanId": "BBBB", "name": "op", "kind": "SPAN_KIND_SERVER",
                                   "startTimeUnixNano": "1", "endTimeUnixNano": "2", "attributes": [],
                                   "status": {"message": msg, "code": "STATUS_CODE_ERROR"},
                                   "links": [{"traceId": "CCCC", "spanId": "DDDD",
                                              "attributes": [{"key": "a", "value": {"stringValue": "b"}}] if link_attrs else []}]}]}]}]}}


def test_otlp_scope_attrs_visible():
    assert not evaluate_traces(decode_otlp(_otlp()), decode_otlp(_otlp(scope_attrs=False))).exact


def test_otlp_link_attrs_visible():
    assert not evaluate_traces(decode_otlp(_otlp()), decode_otlp(_otlp(link_attrs=False))).exact


def test_otlp_status_message_visible():
    assert not evaluate_traces(decode_otlp(_otlp()), decode_otlp(_otlp(msg=""))).exact


def test_otlp_resource_attrs_visible():
    assert not evaluate_traces(decode_otlp(_otlp()), decode_otlp(_otlp(res_extra="h2"))).exact


# F8: Jaeger dependencies endpoint: must not decode to empty (silently vacuous)
def test_jaeger_dependencies_decode():
    body = {"data": [{"parent": "a", "child": "b", "callCount": 3}], "errors": None, "total": 1}
    assert decode_values(body), "dependencies decode to nothing"


# F9: canon must not crash on Infinity in a body
def test_canon_inf():
    canon(float("inf"))


# F10: a body shape no decoder knows must not be a silent pass/vacuous
def test_unknown_shape_is_not_vacuous():
    meta = {"id": "u", "surface": "vl-native", "kind": "values"}
    facets = {"facets": [{"field_name": "level", "values": [{"field_value": "info", "hits": 3}]}]}
    other = {"facets": [{"field_name": "level", "values": [{"field_value": "warn", "hits": 9}]}]}
    case = evaluate_case(meta, {"ref": ans(facets), "base": ans(facets), "pr": ans(other)})
    assert case.verdict not in ("vacuous", "exact"), case.verdict


# F11: M14 latency must count only validated answers: a wrong PR answer is invalid
def test_latency_ignores_wrong_answers():
    meta = {"id": "lat", "surface": "vl-native", "kind": "rows"}
    case = evaluate_case(meta, {"ref": ans(nd(ROWS), ms=10), "base": ans(nd(ROWS), ms=10), "pr": ans(nd(ROWS[:1]), ms=1)})
    assert case.latency["invalid"]["pr"] == 1, case.latency


# F12: a blocked reference in a re-sample is not a PR flake
def test_resample_blocked_ref_not_nondeterministic():
    meta = {"id": "rb", "surface": "vl-native", "kind": "rows"}
    first = {"ref": ans(nd(ROWS)), "base": ans(nd(ROWS)), "pr": ans(nd(ROWS[:3]))}
    second = {"ref": ans("unavailable", 503), "base": ans(nd(ROWS)), "pr": ans(nd(ROWS[:3]))}
    case = evaluate_case(meta, first, [second])
    assert case.verdict != "nondeterministic", case.samples


# F13: a body that is NDJSON with a single row still parses (load_body json-parses a single line)
def test_single_row_ndjson_string():
    one = json.dumps(ROWS[0])
    meta = {"id": "one", "surface": "vl-native", "kind": "rows"}
    b, p = evaluate_request(meta, {"ref": ans(one), "base": ans(one), "pr": ans(one)})
    assert p.exact
