"""API row runner: row validation, windows and tenant forms, the HTTP client and an end-to-end run against stub targets."""
import glob
import gzip
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from scripts.proof.runner import client, rows as R, run as runner
from scripts.proof.runner.report import markdown, write_reports
from scripts.proof import stack
from scripts.proof.jsonio import load_json, read_text
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
        doc = load_json(path)
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
            out = out(body, dict(self.headers)) if out.__code__.co_argcount == 2 else out()
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
    assert load_json(os.path.join(d, "pr.json"))["status"] == 200
    assert os.path.isdir(os.path.join(d, "resample-2"))  # a request that is not exact is sampled twice more
    with gzip.open(os.path.join(str(tmp_path), "bodies.jsonl.gz"), "rt") as gz:
        lines = [json.loads(x) for x in gz]
    assert {x["target"] for x in lines} == {"ref", "base", "pr"} and {x["round"] for x in lines} == {0, 1, 2}
    write_reports(str(tmp_path), items, st, {"x": 1}, seconds=1.0)
    md = read_text(os.path.join(str(tmp_path), "report.md"))
    assert "fixed" in md.lower() or "FIXED" in md
    rep = load_json(os.path.join(str(tmp_path), "report.json"))
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
        picked = runner.resolve_picks(row, st)
        picked_buffer_alias = runner.resolve_picks(row, st, "buffer", "alias")
        assert picked == {"tid": "abc"} and picked_buffer_alias == {"tid": "abc"}
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
    assert runner.COUNT_ALL != runner.COUNT_SPANS


def test_markdown_table_has_a_verdict_per_request(tmp_path, monkeypatch):
    srvs = targets(VALUES["values"], VALUES["values"])
    try:
        st, items = run_rows(tmp_path, srvs, [FV_ROW], monkeypatch)
    finally:
        stop(srvs)
    md = markdown(items, st, {"a": 1}, label="PR 438")
    assert "PR 438 %" in md and "`fv.numeric.cold`" in md and "checked" in md


# ---- seed: stability, equality of counts and of rows, every form and layer ----

def window_server(count_by_window, rows_by_window=None, tenant_counts=None):
    """A stub target answering `* | stats count()` per (account, window start) and the identity rows per window."""
    def q(body, headers):
        from urllib.parse import parse_qs
        p = {k: v[0] for k, v in parse_qs(body).items()}
        h = {k.lower(): v for k, v in headers.items()}
        acct = h.get("accountid") or ("1001" if h.get("x-scope-orgid") == "acme-corp" else "0")
        key = (acct, p["start"][:13])
        if "stats count" in p["query"]:
            return json.dumps({"c": str(count_by_window(key))})
        rows = (rows_by_window or (lambda k: ["r%d" % i for i in range(count_by_window(k))]))(key)
        return "\n".join(json.dumps({"_time": "t", "_msg": r, "span_id": r, "trace_id": r, "name": r}) for r in rows)
    return serve({"/select/logsql/query": (200, q)})


def three(count_a, count_b=None, count_c=None, rows_b=None):
    return {"ref": window_server(count_a), "base": window_server(count_b or count_a, rows_b), "pr": window_server(count_c or count_a)}


def test_seed_equality_covers_alias_buffer_all_and_numeric1001():
    ok = lambda key: 5  # noqa: E731
    srvs = three(ok)
    try:
        seen = runner.seed_equality(state_for(srvs))
    finally:
        stop(srvs)
    assert {k.split("/", 1)[1] for k in seen} == {f"{f}/{l}" for f in ("numeric", "numeric1001", "alias") for l in ("cold", "buffer", "all")}
    # a difference in exactly one form/layer is found in each of them
    for form_acct, layer_hour in (("1001", "2026-10-07T11"), ("0", "2026-10-07T15"), ("1001", "2026-10-07T15")):
        srvs = three(ok, count_b=lambda key, fa=form_acct, lh=layer_hour: 4 if (key[0] == fa and key[1] == lh) else 5)
        try:
            with pytest.raises(SystemExit) as e:
                runner.seed_equality(state_for(srvs))
        finally:
            stop(srvs)
        assert e.value.code == 2


def test_seed_equality_compares_rows_not_only_counts_and_rejects_zero_everywhere():
    srvs = three(lambda key: 3, rows_b=lambda key: ["x", "y", "z"])  # base: same count, other rows
    try:
        with pytest.raises(SystemExit) as e:
            runner.seed_equality(state_for(srvs), forms=("numeric",), layers=("cold",))
    finally:
        stop(srvs)
    assert "different rows" in str(e.value)
    srvs = three(lambda key: 0)
    try:
        with pytest.raises(SystemExit) as e:
            runner.seed_equality(state_for(srvs), forms=("numeric",), layers=("cold",))
    finally:
        stop(srvs)
    assert "no rows at all" in str(e.value)


def test_wait_stable_waits_for_late_rows_and_fails_on_a_count_that_keeps_moving(monkeypatch):
    monkeypatch.setattr(runner.time, "sleep", lambda s: None)
    reads = iter([{"a": 189}, {"a": 242}, {"a": 242}])
    monkeypatch.setattr(runner, "snapshot", lambda *a, **k: next(reads))
    runner.wait_stable({})
    n = iter(range(100))
    monkeypatch.setattr(runner, "snapshot", lambda *a, **k: {"a": next(n)})
    with pytest.raises(SystemExit) as e:
        runner.wait_stable({}, timeout=-1)
    assert e.value.code == 2 and "still moving" in str(e.value)
    monkeypatch.setattr(runner, "snapshot", lambda *a, **k: {"a": None})
    with pytest.raises(SystemExit):
        runner.wait_stable({}, timeout=-1)


def test_snapshot_counts_all_rows_and_span_rows_for_traces():
    srvs = three(lambda key: 5)
    try:
        snap = runner.snapshot(state_for(srvs), ("numeric",), ("cold",))
    finally:
        stop(srvs)
    assert "traces/numeric/cold/ref/spans" in snap and "logs/numeric/cold/base/all" in snap and "logs/numeric/cold/base/spans" not in snap


def test_buffer_rows_use_the_account_of_the_tenant_form(monkeypatch):
    seen = []
    monkeypatch.setattr(stack, "buffered_rows", lambda port, mode, start, end, acct, proj: seen.append(acct) or 1)
    st = {"ports": {"base-logs": 1}, "buffer": {"start": "a", "end": "b"}}
    for form in ("numeric", "alias", "numeric1001", "keyorder"):
        runner.buffered_rows(st, "base", "logs", form, "buffer")
    assert seen == ["0", "1001", "1001", "7"]


def test_resample_allowance_is_not_used_by_fixed_requests_and_skips_are_reported(tmp_path, monkeypatch):
    # three requests: base lost a value everywhere (fixed), allowance 0
    srvs = targets(base_values=[{"value": "GET", "hits": 5}], pr_values=VALUES["values"])
    rows = [{**FV_ROW, "id": f"fv{i}"} for i in range(3)]
    try:
        st = state_for(srvs)
        monkeypatch.setattr(runner, "buffered_rows", lambda *a, **k: None)
        runner.run(st, rows, str(tmp_path), allowance=0)
        for i in range(3):  # fixed requests are re-sampled for free, whatever the allowance
            assert os.path.isdir(os.path.join(str(tmp_path), "cases", "vl-native", f"fv{i}.numeric.cold", "resample-2"))
    finally:
        stop(srvs)
    # a regressed request past the allowance is not re-sampled and says so
    srvs = targets(base_values=VALUES["values"], pr_values=[{"value": "GET", "hits": 5}])
    out = tmp_path / "reg"
    out.mkdir()
    try:
        st = state_for(srvs)
        runner.run(st, rows[:2], str(out), allowance=1)
    finally:
        stop(srvs)
    metas = [load_json(os.path.join(str(out), "cases", "vl-native", f"fv{i}.numeric.cold", "meta.json")) for i in range(2)]
    assert [bool(m.get("resample_skipped")) for m in metas] == [False, True]


def test_limit_arbitrary_rows_fetch_the_unlimited_reference_answer(tmp_path, monkeypatch):
    full = [{"value": v, "hits": 1} for v in "abcde"]
    ref = serve({"/select/logsql/field_values": (200, lambda body, h: json.dumps({"values": full[:2] if "limit=2" in body else full})),
                 "/select/logsql/query": (200, '{"c":"1"}')})
    other = serve({"/select/logsql/field_values": (200, json.dumps({"values": full[3:]})), "/select/logsql/query": (200, '{"c":"1"}')})
    row = {**FV_ROW, "id": "lim", "params": {"query": "*", "field": "f", "limit": "2"}, "limit_arbitrary": True}
    monkeypatch.setattr(runner, "buffered_rows", lambda *a, **k: None)
    try:
        st = state_for({"ref": ref, "base": other, "pr": other})
        runner.run(st, [row], str(tmp_path))
    finally:
        stop({"a": ref, "b": other})
    meta = load_json(os.path.join(str(tmp_path), "cases", "vl-native", "lim.numeric.cold", "meta.json"))
    assert meta["universe"] == list("abcde") and meta["limit_arbitrary"]
    (m, res), = run_dir(os.path.join(str(tmp_path), "cases"))
    assert res.verdict == "exact"  # two members of the full answer, whichever they are


def test_same_4xx_on_every_target_is_a_harness_error_unless_the_row_expects_it(tmp_path, monkeypatch):
    mk = lambda: serve({"/select/logsql/field_values": (400, "bad request"), "/select/logsql/query": (200, '{"c":"1"}')})  # noqa: E731
    srvs = {"ref": mk(), "base": mk(), "pr": mk()}
    monkeypatch.setattr(runner, "buffered_rows", lambda *a, **k: None)
    try:
        st = state_for(srvs)
        runner.run(st, [FV_ROW], str(tmp_path))
        runner.run(st, [{**FV_ROW, "id": "exp", "expect_error": True}], str(tmp_path))
    finally:
        stop(srvs)
    got = {m["request_id"]: r.verdict for m, r in run_dir(os.path.join(str(tmp_path), "cases"))}
    assert got == {"fv": "harness-error", "exp": "exact"}


def test_the_keyorder_fixture_is_checked_for_logs_only():
    srvs = three(lambda key: 5)
    try:
        seen = runner.seed_equality(state_for(srvs), forms=("numeric", "keyorder"), layers=("cold",))
        snap = runner.snapshot(state_for(srvs), ("keyorder",), ("cold",))
    finally:
        stop(srvs)
    assert "logs/keyorder/cold" in seen and "traces/keyorder/cold" not in seen and "traces/numeric/cold" in seen
    assert not any(k.startswith("traces/") for k in snap)


def test_a_regressed_request_is_resampled_after_many_fixed_ones_used_no_allowance(tmp_path, monkeypatch):
    """Fixed requests are re-sampled for free: they must leave the allowance of the regressed one untouched."""
    # base lost a value on the `fixed*` rows, the PR lost one on the `reg` row
    def mk(role):
        def f(body, headers):
            lost = [{"value": "GET", "hits": 5}]
            if role == "base":
                vals = lost if "field=fixed" in body else VALUES["values"]
            elif role == "pr":
                vals = lost if "field=reg" in body else VALUES["values"]
            else:
                vals = VALUES["values"]
            return json.dumps({"values": vals})
        return serve({"/select/logsql/field_values": (200, f), "/select/logsql/query": (200, '{"c":"1"}')})
    srvs = {"ref": mk("ref"), "base": mk("base"), "pr": mk("pr")}
    rows = [{**FV_ROW, "id": f"fx{i}", "params": {"query": "*", "field": f"fixed{i}", "limit": "100"}} for i in range(3)]
    rows.append({**FV_ROW, "id": "rg", "params": {"query": "*", "field": "reg", "limit": "100"}})
    monkeypatch.setattr(runner, "buffered_rows", lambda *a, **k: None)
    try:
        runner.run(state_for(srvs), rows, str(tmp_path), allowance=1)
    finally:
        stop(srvs)
    meta = load_json(os.path.join(str(tmp_path), "cases", "vl-native", "rg.numeric.cold", "meta.json"))
    assert not meta.get("resample_skipped")
    assert os.path.isdir(os.path.join(str(tmp_path), "cases", "vl-native", "rg.numeric.cold", "resample-2"))


def test_main_waits_for_stable_counts_before_comparing_and_not_when_the_seed_check_is_off(tmp_path, monkeypatch):
    srvs = targets(VALUES["values"], VALUES["values"])
    sp = tmp_path / "state.json"
    sp.write_text(json.dumps(state_for(srvs)))
    rows = tmp_path / "rows.json"
    rows.write_text(json.dumps({"rows": [FV_ROW]}))
    calls = []
    monkeypatch.setattr(runner, "wait_stable", lambda state, forms=None, **k: calls.append(("wait", tuple(forms))))
    monkeypatch.setattr(runner, "seed_equality", lambda state, forms=None, **k: calls.append(("seed", tuple(forms))) or {})
    monkeypatch.setattr(runner, "buffered_rows", lambda *a, **k: None)
    try:
        runner.main(["--state", str(sp), "--out", str(tmp_path / "o1"), "--tier", str(rows)])
        runner.main(["--state", str(sp), "--out", str(tmp_path / "o2"), "--tier", str(rows), "--no-seed-check"])
    finally:
        stop(srvs)
    assert [c[0] for c in calls] == ["wait", "seed"]
    assert calls[0][1] == calls[1][1] == runner.SEED_FORMS


def test_the_keyorder_form_joins_the_seed_check_when_the_state_has_the_fixture(tmp_path, monkeypatch):
    srvs = targets(VALUES["values"], VALUES["values"])
    st = state_for(srvs)
    st["keyorder"] = True
    sp = tmp_path / "state.json"
    sp.write_text(json.dumps(st))
    rows = tmp_path / "rows.json"
    rows.write_text(json.dumps({"rows": [FV_ROW]}))
    seen = []
    monkeypatch.setattr(runner, "wait_stable", lambda state, forms=None, **k: seen.append(forms))
    monkeypatch.setattr(runner, "seed_equality", lambda state, forms=None, **k: {})
    monkeypatch.setattr(runner, "buffered_rows", lambda *a, **k: None)
    try:
        runner.main(["--state", str(sp), "--out", str(tmp_path / "o"), "--tier", str(rows)])
    finally:
        stop(srvs)
    assert seen == [runner.SEED_FORMS + ("keyorder",)]
