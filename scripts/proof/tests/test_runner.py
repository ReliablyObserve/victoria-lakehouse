"""API row runner: row validation, windows and tenant forms, the HTTP client and an end-to-end run against stub targets."""
import glob
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from scripts.proof.runner import client, rows as R, run as runner
from scripts.proof.runner.report import markdown, write_reports
from scripts.proof.metrics.cases import run_dir

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
STATE = {"H": "2026-10-07T15:00:00Z", "cold": {"start": "2026-10-07T11:00:00Z", "end": "2026-10-07T15:00:00Z"},
         "buffer": {"start": "2026-10-07T15:00:00Z", "end": "2026-10-07T16:00:00Z"}, "ports": {}}


def registry_ids():
    ids = set()
    for f in glob.glob(os.path.join(ROOT, "tests/conformance/registry/rows/**/*.yaml"), recursive=True):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                ids.update(re.findall(r"\bid:\s*([\w.]+)", line))
    return ids


def test_shipped_rows_are_valid_and_core_rows_exist_in_the_registry():
    ids = registry_ids()
    for name in ("core", "field-values", "audit"):
        path = os.path.join(R.ROWS_DIR, name + ".json")
        doc = json.load(open(path))
        rows = R.load_rows(path)
        assert rows
        if doc["registry_check"]:
            assert [r["row"] for r in rows if r["row"] not in ids] == []


def test_both_signals_and_native_surfaces_are_in_core():
    surfaces = {r["surface"] for r in R.load_tier(["core"])}
    assert {"vl-native", "vt-native", "jaeger"} <= surfaces


@pytest.mark.parametrize("bad", [
    {"id": "x", "surface": "nope", "kind": "values", "path": "/a"},
    {"id": "x", "surface": "vl-native", "kind": "nope", "path": "/a"},
    {"id": "x", "surface": "vl-native", "kind": "values", "path": "a"},
    {"id": "x", "surface": "vl-native", "kind": "values", "path": "/a", "layers": ["warm"]},
    {"id": "x", "surface": "vl-native", "kind": "values", "path": "/a", "forms": ["dn"]},
    {"id": "x", "surface": "vl-native", "kind": "values", "path": "/a", "window": "x"},
    {"surface": "vl-native", "kind": "values", "path": "/a"},
])
def test_invalid_rows_are_rejected(bad):
    with pytest.raises(R.RowError):
        R.validate_row(bad)


def test_duplicate_ids_are_rejected(tmp_path):
    p = tmp_path / "d.json"
    row = {"id": "a", "surface": "vl-native", "kind": "values", "path": "/a"}
    p.write_text(json.dumps({"rows": [row, row]}))
    with pytest.raises(R.RowError):
        R.load_rows(str(p))
    with pytest.raises(R.RowError):
        R.load_tier([str(p)])


def test_tier_runs_the_same_row_once_and_rejects_conflicting_definitions(tmp_path):
    row = {"id": "a", "surface": "vl-native", "kind": "values", "path": "/a"}
    for n in ("1", "2"):
        (tmp_path / f"{n}.json").write_text(json.dumps({"rows": [row]}))
    assert [r["id"] for r in R.load_tier([str(tmp_path / "1.json"), str(tmp_path / "2.json")])] == ["a"]
    (tmp_path / "3.json").write_text(json.dumps({"rows": [{**row, "path": "/b"}]}))
    with pytest.raises(R.RowError):
        R.load_tier([str(tmp_path / "1.json"), str(tmp_path / "3.json")])


def test_expand_windows_forms_layers_and_methods():
    row = {"id": "f", "surface": "vl-native", "kind": "values", "path": "/select/logsql/field_values", "params": {"query": "*"}}
    reqs = R.expand(row, STATE)
    assert {(r["form"], r["layer"]) for r in reqs} == {("numeric", "cold"), ("numeric", "buffer"), ("alias", "cold"), ("alias", "buffer")}
    cold = next(r for r in reqs if r["layer"] == "cold")
    assert cold["params"]["start"] == "2026-10-07T11:00:00Z" and cold["params"]["end"] == "2026-10-07T15:00:00Z"
    assert cold["method"] == "POST" and cold["signal"] == "logs"
    buf = next(r for r in reqs if r["layer"] == "buffer")
    assert buf["params"]["start"] == "2026-10-07T15:00:00Z"
    # windows are absolute and hour aligned
    assert all(re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:00:00Z", r["params"][k]) for r in reqs for k in ("start", "end"))


def test_window_modes():
    jaeger = {"id": "j", "surface": "jaeger", "kind": "values", "path": "/select/jaeger/api/traces", "window": "jaeger", "layers": ["all"], "forms": ["numeric"]}
    p = R.expand(jaeger, STATE)[0]
    assert p["method"] == "GET" and p["signal"] == "traces"
    assert p["params"]["start"] == str(1791370800 * 10**6)
    assert p["params"]["end"] == str(1791388800 * 10**6)
    none = {**jaeger, "window": "none"}
    assert R.expand(none, STATE)[0]["params"] == {}
    allw = {"id": "a", "surface": "vl-native", "kind": "values", "path": "/select/logsql/x", "layers": ["all"], "forms": ["numeric"]}
    assert R.expand(allw, STATE)[0]["params"] == {"start": "2026-10-07T11:00:00Z", "end": "2026-10-07T16:00:00Z"}


def test_end_placeholder_and_picks():
    row = {"id": "s", "surface": "vl-native", "kind": "count", "path": "/select/logsql/stats_query/{t}", "params": {"time": "{END}", "q": "{pick}"},
           "window": "none", "layers": ["cold"], "forms": ["numeric"]}
    r = R.expand(row, STATE, {"pick": "abc", "t": "x"})[0]
    assert r["params"] == {"time": "2026-10-07T15:00:00Z", "q": "abc"} and r["path"].endswith("/x")


def test_tenant_headers_alias_goes_to_lakehouse_as_orgid_and_to_hot_as_numeric():
    assert R.tenant_headers("numeric", "base") == {"AccountID": "0", "ProjectID": "0"}
    assert R.tenant_headers("alias", "ref") == {"AccountID": "1001", "ProjectID": "0"}
    assert R.tenant_headers("alias", "pr") == {"X-Scope-OrgID": "acme-corp"}
    assert R.tenant_headers("numeric1001", "pr")["AccountID"] == "1001"
    with pytest.raises(R.RowError):
        R.tenant_headers("x", "ref")


# ---- client against a stub server ----

class Stub(BaseHTTPRequestHandler):
    routes = {}
    calls = []

    def log_message(self, *a):
        pass

    def _serve(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n).decode() if n else ""
        Stub.calls.append((self.command, self.path, dict(self.headers), body))
        status, out = self.server.routes.get(self.path.split("?")[0], (404, "nope"))
        if callable(out):
            out = out()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(out.encode())

    do_GET = do_POST = _serve


def serve(routes):
    srv = HTTPServer(("127.0.0.1", 0), Stub)
    srv.routes = routes
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def test_client_records_status_body_latency_and_transport_errors():
    srv = serve({"/ok": (200, '{"a":1}'), "/boom": (503, "down")})
    base = f"http://127.0.0.1:{srv.server_port}"
    try:
        e = client.send("GET", base + "/ok", {"x": "1"}, {"AccountID": "0"})
        assert e["status"] == 200 and e["body"] == '{"a":1}' and e["latency_ms"] >= 0 and "error_kind" not in e
        assert any(c[1] == "/ok?x=1" and {k.lower(): v for k, v in c[2].items()}.get("accountid") == "0" for c in Stub.calls)
        e = client.send("POST", base + "/ok", {"query": "a b"}, {})
        assert e["status"] == 200
        assert any(c[0] == "POST" and c[3] == "query=a+b" for c in Stub.calls)
        e = client.send("GET", base + "/boom", {}, {})
        assert e["status"] == 503 and e["body"] == "down"
    finally:
        srv.shutdown()
        srv.server_close()
    e = client.send("GET", base + "/ok", {}, {}, timeout=1)  # the server is gone
    assert e["status"] == 0 and e["error_kind"] == "connect" and e["timeout"] is False


def test_client_timeout_is_not_a_5xx():
    import time
    srv = serve({"/slow": (200, lambda: time.sleep(1.5) or "{}")})
    try:
        e = client.send("GET", f"http://127.0.0.1:{srv.server_port}/slow", {}, {}, timeout=0.3)
    finally:
        srv.shutdown()
    assert e["status"] == 0 and e["error_kind"] == "timeout" and e["timeout"] is True


# ---- end to end: three stub targets ----

VALUES = {"values": [{"value": "GET", "hits": 5}, {"value": "POST", "hits": 3}]}
COUNT = {"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, "8"]}]}}


def targets(base_values, pr_values, count="8"):
    def mk(values):
        def logs_query():
            return json.dumps({"c": count})
        return serve({"/select/logsql/field_values": (200, json.dumps({"values": values})),
                      "/select/logsql/query": (200, logs_query),
                      "/internal/buffer/query": (200, "")})
    return {"ref": mk(VALUES["values"]), "base": mk(base_values), "pr": mk(pr_values)}


def state_for(srvs):
    st = json.loads(json.dumps(STATE))
    for t, s in srvs.items():
        st["ports"][f"{t}-logs"] = s.server_port
        st["ports"][f"{t}-traces"] = s.server_port
    return st


FV_ROW = {"id": "fv", "row": "vl.select.field_values.basic", "surface": "vl-native", "kind": "values",
          "path": "/select/logsql/field_values", "params": {"query": "*", "field": "http.method", "limit": "100"},
          "layers": ["cold"], "forms": ["numeric"]}


def run_rows(tmp_path, srvs, rows, monkeypatch, seed=True):
    st = state_for(srvs)
    monkeypatch.setattr(runner, "buffered_rows", lambda *a, **k: None)
    if seed:
        runner.seed_equality(st, forms=("numeric",), layers=("cold",))
    runner.run(st, rows, str(tmp_path))
    items = run_dir(os.path.join(str(tmp_path), "cases"))
    return st, items


def stop(srvs):
    for s in srvs.values():
        s.shutdown()
        s.server_close()


def test_end_to_end_exact_fixed_and_regressed(tmp_path, monkeypatch):
    # base lost a value (B), the PR answers like the reference: fixed
    srvs = targets(base_values=[{"value": "GET", "hits": 5}], pr_values=VALUES["values"])
    try:
        st, items = run_rows(tmp_path, srvs, [FV_ROW], monkeypatch)
    finally:
        stop(srvs)
    (meta, res), = items
    assert res.verdict == "fixed" and meta["form"] == "numeric" and meta["layer"] == "cold"
    d = os.path.join(str(tmp_path), "cases", "vl-native", "fv.numeric.cold")
    assert json.load(open(os.path.join(d, "pr.json")))["status"] == 200
    assert os.path.isdir(os.path.join(d, "resample-2"))  # a request that is not exact is sampled twice more
    lines = [json.loads(x) for x in __import__("gzip").open(os.path.join(str(tmp_path), "bodies.jsonl.gz"), "rt")]
    assert {x["target"] for x in lines} == {"ref", "base", "pr"} and {x["round"] for x in lines} == {0, 1, 2}
    write_reports(str(tmp_path), items, st, {"x": 1}, seconds=1.0)
    md = open(os.path.join(str(tmp_path), "report.md")).read()
    assert "fixed" in md.lower() or "FIXED" in md
    rep = json.load(open(os.path.join(str(tmp_path), "report.json")))
    assert rep["results"][0]["verdict"] == "fixed" and rep["results"][0]["base_score"] < 100 and rep["results"][0]["pr_score"] == 100


def test_end_to_end_regression_exact_has_no_resamples(tmp_path, monkeypatch):
    srvs = targets(base_values=VALUES["values"], pr_values=[{"value": "GET", "hits": 5}])
    try:
        _, items = run_rows(tmp_path, srvs, [FV_ROW], monkeypatch)
        assert items[0][1].verdict == "regressed"
    finally:
        stop(srvs)
    srvs = targets(base_values=VALUES["values"], pr_values=VALUES["values"])
    out = tmp_path / "exact"
    out.mkdir()
    try:
        _, items = run_rows(out, srvs, [FV_ROW], monkeypatch)
    finally:
        stop(srvs)
    assert items[0][1].verdict == "exact"
    assert not os.path.exists(os.path.join(str(out), "cases", "vl-native", "fv.numeric.cold", "resample-1"))


def test_seed_equality_failure_is_an_incomplete_run(tmp_path, monkeypatch):
    srvs = targets(VALUES["values"], VALUES["values"])
    srvs["pr"].shutdown()
    srvs["pr"] = serve({"/select/logsql/query": (200, json.dumps({"c": "7"}))})  # the PR holds other rows
    try:
        with pytest.raises(SystemExit) as e:
            run_rows(tmp_path, srvs, [FV_ROW], monkeypatch)
    finally:
        stop(srvs)
    assert e.value.code == 2 and "seed equality" in str(e.value)


def test_unreachable_target_fails_seed_equality(monkeypatch):
    srvs = targets(VALUES["values"], VALUES["values"])
    st = state_for(srvs)
    stop(srvs)
    with pytest.raises(SystemExit) as e:
        runner.seed_equality(st, forms=("numeric",), layers=("cold",))
    assert e.value.code == 2


def test_expand_with_a_pick_function_resolves_per_form_and_layer():
    row = {"id": "t", "surface": "jaeger", "kind": "trace_jaeger", "path": "/select/jaeger/api/traces/{tid}", "window": "none"}
    reqs = R.expand(row, STATE, lambda form, layer: {"tid": f"{form}-{layer}"})
    assert {r["path"].rsplit("/", 1)[1] for r in reqs} == {"numeric-cold", "numeric-buffer", "alias-cold", "alias-buffer"}


def test_pick_takes_a_value_from_the_reference_and_fails_loudly_when_absent():
    srv = serve({"/select/logsql/query": (200, '{"trace_id":"abc"}\n')})
    st = state_for({"ref": srv})
    try:
        row = {"id": "p", "surface": "jaeger", "kind": "values", "path": "/t/{tid}", "pick": {"tid": {"query": "q", "field": "trace_id", "signal": "traces"}}}
        assert runner.resolve_picks(row, st) == {"tid": "abc"}
        assert runner.resolve_picks(row, st, "buffer", "alias") == {"tid": "abc"}
        assert any(c[3].startswith("query=q") and "start=2026-10-07T15" in c[3] and c[2].get("Accountid") == "1001" for c in Stub.calls)
        srv.routes = {"/select/logsql/query": (200, "")}
        with pytest.raises(SystemExit) as e:
            runner.resolve_picks(row, st)
        assert e.value.code == 2
    finally:
        srv.shutdown()


def test_meta_carries_claims_and_options():
    row = {"id": "q", "row": "reg.row", "surface": "vl-native", "kind": "rows", "path": "/x", "claimed_gap": "B8", "may_be_empty": True,
           "meta": {"order": "as-upstream", "skip_fields": ["_stream"]}}
    req = R.expand(row, STATE)[0]
    m = runner.meta_of(req)
    assert m["claimed_gap"] == "B8" and m["may_be_empty"] and m["order"] == "as-upstream" and m["row"] == "reg.row"
    assert runner.case_id(req) == "vl-native/q.numeric.cold"
    assert runner.count_query("logs") != runner.count_query("traces")


def test_markdown_table_has_a_verdict_per_request(tmp_path, monkeypatch):
    srvs = targets(VALUES["values"], VALUES["values"])
    try:
        st, items = run_rows(tmp_path, srvs, [FV_ROW], monkeypatch)
    finally:
        stop(srvs)
    md = markdown(items, st, {"a": 1}, label="PR 438")
    assert "PR 438 %" in md and "`fv.numeric.cold`" in md and "checked" in md
