"""validate.py ports scripts/bench/run.sh's answer validation: same strings, same verdicts."""
import json
import os
import subprocess
import tempfile

import pytest

import validate

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
RUN_SH = os.path.join(ROOT, "scripts", "bench", "run.sh")

FIXTURES = {
    "scalar": '{"n":"17132"}\n',
    "groupby": '{"service.name":"api-gateway","n":"3499"}\n{"service.name":"web","n":"100"}\n',
    "groupby_swapped": '{"service.name":"api-gateway","n":"3000"}\n{"service.name":"web","n":"599"}\n',
    "scan_logs": '{"_msg":"hello","service.name":"a"}\n{"_msg":"world","service.name":"b"}\n',
    "scan_traces": '{"trace_id":"t1","span_id":"s1"}\n{"trace_id":"t2","span_id":"s2"}\n',
    "scan_traces_reordered": '{"trace_id":"t2","span_id":"s2"}\n{"trace_id":"t1","span_id":"s1"}\n',
    "spans": '{"n":"8"}\n',
    "values": '{"values":[{"value":"INFO","hits":10},{"value":"ERROR","hits":3}]}\n',
    "empty": "",
    "malformed": "not json at all, definitely not\n",
    "partial": '{"n":"1"}\nnot json\n{"n":"2"}\n',
}
# (run.sh qkind, fixture) pairs the differential test compares
CASES = [("count_total", "scalar"), ("count_total", "empty"), ("count_total", "partial"), ("count_by_service", "groupby"),
         ("count_by_service", "groupby_swapped"), ("count_by_service", "empty"), ("count_by_service", "malformed"), ("scan", "scan_logs"),
         ("scan", "scan_traces"), ("scan", "scan_traces_reordered"), ("scan", "malformed"), ("trace_by_id", "spans"), ("trace_by_id", "empty"),
         ("fv_level", "values"), ("fv_level", "empty"), ("fv_level", "malformed")]


def bash_extract(qkind, body):
    """run.sh's own extract_result, sourced the way scripts/bench/tests/extract_result_test.sh does."""
    with tempfile.TemporaryDirectory() as tmp:
        funcs = os.path.join(tmp, "funcs.sh")
        with open(RUN_SH, encoding="utf-8") as f:
            text = f.read()
        head = text.split("# --- time helpers", 1)[0]
        with open(funcs, "w", encoding="utf-8") as f:
            f.write(head)
        data = os.path.join(tmp, "body")
        with open(data, "w", encoding="utf-8") as f:
            f.write(body)
        r = subprocess.run(["bash", "-c", f'BENCH_TMP={tmp}; source "{funcs}"; extract_result {qkind} lakehouse "{data}"'],
                           capture_output=True, text=True, timeout=60)
        return r.stdout.strip()


@pytest.mark.skipif(not os.path.exists(RUN_SH), reason="run.sh not in the tree")
@pytest.mark.parametrize("qkind,fixture", CASES)
def test_matches_run_sh_extract_result(qkind, fixture):
    body = FIXTURES[fixture]
    assert validate.extract_result(validate.RUNSH_KIND[qkind], body) == bash_extract(qkind, body)


def test_group_by_hash_catches_same_total_different_groups():
    a = validate.extract_result("groupby", FIXTURES["groupby"])
    b = validate.extract_result("groupby", FIXTURES["groupby_swapped"])
    assert a.split(";")[1] == b.split(";")[1] == "total=3599" and a != b


def test_scan_hash_is_order_independent():
    assert validate.extract_result("scan", FIXTURES["scan_traces"]) == validate.extract_result("scan", FIXTURES["scan_traces_reordered"])


def test_values_hash_includes_hits():
    other = '{"values":[{"value":"INFO","hits":10},{"value":"ERROR","hits":4}]}\n'
    assert validate.extract_result("values", FIXTURES["values"]) != validate.extract_result("values", other)


def test_error_status_empty_and_flapping_are_never_valid():
    assert validate.validate("scalar", 500, '{"n":"1"}') == (False, "http 500", "")
    assert validate.validate("scalar", 200, "")[1] == "empty result"
    assert validate.validate("groupby", 200, "")[1] == "empty-body"
    assert validate.validate("scalar", 200, "garbage\n")[1] == "parse-error"
    ok, _, first = validate.validate("scalar", 200, '{"n":"5"}')
    assert ok
    assert validate.validate("scalar", 200, '{"n":"6"}', first_result=first)[1].startswith("flapping")
    assert validate.validate("scalar", 200, '{"n":"0"}', miss=True)[0] is True  # a documented miss may be empty


def test_truncated_scan_is_valid_by_membership_and_cardinality():
    window_keys = {f"m{i}" for i in range(1500)}
    body = "\n".join(json.dumps({"_msg": f"m{i}"}) for i in range(validate.SCAN_LIMIT)) + "\n"
    assert validate.validate("scan", 200, body, window_rows=1500, window_keys=window_keys)[0] is True
    foreign = body + json.dumps({"_msg": "stranger"}) + "\n"
    short = "\n".join(json.dumps({"_msg": f"m{i}"}) for i in range(10)) + "\n"
    assert "membership" in validate.validate("scan", 200, body.replace("m0\"", "stranger\"", 1), window_rows=1500, window_keys=window_keys)[1]
    assert "truncated scan returned" in validate.validate("scan", 200, short, window_rows=1500, window_keys=window_keys)[1]
    assert validate.validate("scan", 200, foreign, window_rows=1500, window_keys=window_keys)[0] is False


def test_json_kind_ignores_order_and_volatile_members():
    a = json.dumps({"data": [{"b": 1, "took": 5}, {"a": 2}]})
    b = json.dumps({"data": [{"a": 2}, {"b": 1, "took": 99}]})
    assert validate.extract_result("json", a) == validate.extract_result("json", b)
    assert validate.extract_result("json", a) != validate.extract_result("json", json.dumps({"data": [{"a": 3}, {"b": 1}]}))


def jaeger(order, labels):
    procs = {labels[0]: {"serviceName": "a", "tags": []}, labels[1]: {"serviceName": "b", "tags": []}}
    spans = [{"spanID": "s1", "operationName": "x", "processID": labels[0], "startTime": 1, "duration": 2, "tags": []},
             {"spanID": "s2", "operationName": "y", "processID": labels[1], "startTime": 1, "duration": 2, "tags": []}]
    return json.dumps({"data": [{"traceID": "t1", "processes": procs, "spans": [spans[i] for i in order]}]})


def test_m8_trace_answers_are_compared_structurally_with_the_proof_decoders():
    """Span order and processID labels are arrival artefacts, not differences; a changed span or a missing one is."""
    a = validate.extract_result("trace_jaeger", jaeger((0, 1), ("p1", "p2")))
    assert a == validate.extract_result("trace_jaeger", jaeger((1, 0), ("p9", "p3"))) and a.startswith("traces=1;spans=2;hash=")
    changed = json.loads(jaeger((0, 1), ("p1", "p2")))
    changed["data"][0]["spans"][0]["operationName"] = "other"
    assert validate.extract_result("trace_jaeger", json.dumps(changed)) != a
    dropped = json.loads(jaeger((0, 1), ("p1", "p2")))
    dropped["data"][0]["spans"].pop()
    assert validate.extract_result("trace_jaeger", json.dumps(dropped)) != a
    assert validate.validate("trace_jaeger", 200, '{"data": []}')[1] == "empty result"
    assert validate.validate("trace_jaeger", 200, "not json")[1] == "parse-error"
    assert validate.validate("tempo_search", 200, '{"traces": []}')[0] is False
    assert isinstance(validate.extract_result("trace_otlp", "{}"), str)  # an unknown OTLP shape is invalid or empty, never a crash


def test_the_trace_shapes_of_shapes_json_use_the_structural_kinds():
    import abreg
    spec = abreg.load_json(abreg.SHAPES)
    kinds = {sh["name"]: sh["kind"] for s in spec["sets"].values() for sh in s["shapes"]}
    for name in ("jaeger trace_by_id", "jaeger search by_service", "jaeger search min_duration"):
        assert kinds[name] == "trace_jaeger"
    assert kinds["tempo v2 trace_by_id"] == "trace_otlp" and kinds["tempo search service"] == "tempo_search"


def tempo_series(value):
    return json.dumps({"series": [{"labels": [{"key": "x", "value": {"stringValue": "a"}}],
                                   "samples": [{"timestampMs": "1000", "value": value}, {"timestampMs": "2000", "value": 2.0}]}]})


def test_traceql_metrics_ignore_float_noise_beyond_six_digits_but_not_a_real_difference():
    """Found by the real A/A run: the same Lakehouse build answered TraceQL rates that differ in the last digits between calls."""
    a = validate.extract_result("series_tempo", tempo_series(0.123456789))
    assert a == validate.extract_result("series_tempo", tempo_series(0.123456781)) and a.startswith("series=1;points=2;hash=")
    assert a != validate.extract_result("series_tempo", tempo_series(0.1235))
    assert validate.validate("series_tempo", 200, '{"series": []}')[1] == "empty result"
    assert validate.validate("series_tempo", 200, "nope")[1] == "parse-error"
    import abreg
    kinds = {sh["name"]: sh["kind"] for s in abreg.load_json(abreg.SHAPES)["sets"].values() for sh in s["shapes"]}
    assert kinds["traceql rate"] == kinds["traceql rate by_service"] == kinds["traceql quantile"] == "series_tempo"
