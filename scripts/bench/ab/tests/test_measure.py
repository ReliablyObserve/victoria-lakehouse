"""measure.py: windows, request building for every API, the S3 accounting protocol and the cold run, against fake servers."""

import pytest

import abreg
import measure
from fakes import Server

SPEC = abreg.load_json(abreg.SHAPES)
STATE_WIN = {"cold": {"start": "2026-10-12T11:00:00Z", "end": "2026-10-12T13:00:00Z"},
             "compacted": {"start": "2026-10-12T07:00:00Z", "end": "2026-10-12T11:00:00Z"},
             "buffer": {"start": "2026-10-12T13:00:00Z", "end": "2026-10-12T14:00:00Z"},
             "after-restart": {"start": "2026-10-12T13:00:00Z", "end": "2026-10-12T14:00:00Z"}}


def state(ports=None):
    p = {f"{t}-{s}": 1 for t in ("ref", "base", "pr") for s in ("logs", "traces")}
    p.update(ports or {})
    return {"ports": p, "layers": STATE_WIN, "H": "2026-10-12T13:00:00Z", "project": "t"}


def test_windows_end_at_the_layer_end_and_never_start_before_it():
    st = state()
    assert measure.window_for(st, "cold", "1h", SPEC["ranges"]) == ("2026-10-12T12:00:00Z", "2026-10-12T13:00:00Z")
    assert measure.window_for(st, "cold", "full", SPEC["ranges"]) == ("2026-10-12T11:00:00Z", "2026-10-12T13:00:00Z")
    assert measure.window_for(st, "cold", "15m", SPEC["ranges"])[0] == "2026-10-12T12:45:00Z"
    assert measure.window_for(st, "compacted", "full", SPEC["ranges"]) == ("2026-10-12T07:00:00Z", "2026-10-12T11:00:00Z")
    assert measure.window_for(st, "buffer", "1h", SPEC["ranges"]) == ("2026-10-12T13:00:00Z", "2026-10-12T14:00:00Z")


def shape(name):
    for s in SPEC["sets"].values():
        for sh in s["shapes"]:
            if sh["name"] == name:
                return sh
    raise KeyError(name)


def test_request_for_each_api_style():
    st = state({"ref-traces": 5, "base-logs": 6})
    s, e = "2026-10-12T12:00:00Z", "2026-10-12T13:00:00Z"
    r = measure.build_request(shape("logs count_total"), st, "base", s, e, "numeric")
    assert r["method"] == "POST" and r["url"] == "http://127.0.0.1:6/select/logsql/query" and r["params"] == {"query": "* | stats count() n", "start": s, "end": e}
    assert r["headers"] == {"AccountID": "0", "ProjectID": "0"}
    t = measure.build_request(shape("traces count_total"), st, "ref", s, e, "numeric")
    assert t["params"]["disable_latency_offset"] == "true" and t["url"].startswith("http://127.0.0.1:5/")
    j = measure.build_request(shape("jaeger search by_service"), st, "ref", s, e, "numeric")
    assert j["method"] == "GET" and j["params"]["start"] == "1791806400000000" and j["params"]["service"] == "payment-service"
    d = measure.build_request(shape("jaeger dependencies"), st, "ref", s, e, "numeric")
    assert d["params"]["endTs"] == "1791810000000" and "start" not in d["params"]
    sv = measure.build_request(shape("jaeger services"), st, "ref", s, e, "numeric")
    assert sv["params"] == {}
    t2 = measure.build_request(shape("tempo search service"), st, "ref", s, e, "alias")
    assert t2["headers"] == {"AccountID": "1001", "ProjectID": "0"}  # hot has no aliases
    t3 = measure.build_request(shape("tempo search service"), st, "base", s, e, "alias")
    assert t3["headers"] == {"X-Scope-OrgID": "acme-corp"}
    p = measure.build_request(shape("traces trace_by_id count"), st, "ref", s, e, "numeric", {"trace_id": "abc"})
    assert p["params"]["query"] == "trace_id:=abc | stats count() n"
    with pytest.raises(measure.CellError):
        measure.build_request(shape("traces trace_by_id count"), st, "ref", s, e, "numeric", {})


def test_plan_expands_layers_ranges_and_respects_the_selection():
    cells = measure.plan(SPEC, ["control-logs"])
    assert len(cells) == 8 * 4 and {c["layer"] for c in cells} == {"cold", "compacted", "buffer", "after-restart"}
    only = measure.plan(SPEC, ["control-logs"], {"control-logs": ["logs count_total"]})
    assert len(only) == 4 and all(c["shape"]["name"] == "logs count_total" for c in only)
    with pytest.raises(measure.CellError):
        measure.plan(SPEC, ["nope"])


def test_parse_metrics_counts_gets_but_not_puts_and_reads_process_figures():
    text = "\n".join(['# HELP x', 'lakehouse_s3_requests_total{op="GetObject"} 7', 'lakehouse_s3_requests_total{op="GET"} 2',
                      'lakehouse_s3_requests_total{op="PutObject"} 3', 'lakehouse_s3_requests_total{op="ListObjectsV2"} 4',
                      "lakehouse_s3_bytes_read_total 1000", "process_cpu_seconds_total 1.5", "process_resident_memory_bytes 2e8"])
    m = measure.parse_metrics(text)
    assert m["gets"] == 9 and m["requests"] == 13 and m["bytes"] == 1000 and m["cpu_s"] == 1.5 and m["rss"] == 2e8
    assert measure.s3_delta(m, dict(m, gets=12, requests=16, bytes=1500)) == {"gets": 3, "requests": 3, "bytes": 500}
    assert measure.s3_delta(None, m) is None


def test_settle_waits_until_the_metrics_caught_up():
    waited = []
    s = measure.Settle()
    s.last["k"] = 100.0
    s.scrape("k", "http://127.0.0.1:1", sleep=waited.append, clock=lambda: 100.3)
    assert waited and waited[0] == pytest.approx(measure.SETTLE_S - 0.3)
    waited.clear()
    s.scrape("k", "http://127.0.0.1:1", sleep=waited.append, clock=lambda: 105.0)
    assert waited == []


class FakeLakehouse(Server):
    """answers the count query, serves /metrics whose GET counter moves with the requests, and records cache clears."""

    def __init__(self, answer='{"n":"42"}\n', status=200, clear_status=200):
        self.answer, self.status_code, self.clear_status = answer, status, clear_status
        self.gets = 0
        self.cleared = 0
        super().__init__(self.handle)

    def handle(self, method, path, headers, body):
        if path.startswith("/metrics"):
            return 200, f'lakehouse_s3_requests_total{{op="GetObject"}} {self.gets}\nlakehouse_s3_bytes_read_total {self.gets * 100}\n', {}
        if path.startswith("/internal/cache/clear"):
            self.cleared += 1
            return self.clear_status, '{"cleared":true}', {}
        self.gets += 3
        return self.status_code, self.answer, {}


def test_run_cell_clears_the_cache_before_the_cold_run_and_accounts_s3_per_phase(monkeypatch):
    monkeypatch.setattr(measure, "SETTLE_S", 0.0)
    ref, base, pr = FakeLakehouse(), FakeLakehouse(), FakeLakehouse()
    try:
        st = state({"ref-logs": ref.port, "base-logs": base.port, "pr-logs": pr.port})
        cell = measure.plan(SPEC, ["control-logs"], {"control-logs": ["logs count_total"]})[0]
        rows, acct = measure.run_cell(cell, st, SPEC, 4, {})
        assert len(rows) == 12 and all(r["valid"] for r in rows)
        assert base.cleared == 1 and pr.cleared == 1 and ref.cleared == 0
        assert [r["cold"] for r in rows if r["target"] == "pr"] == [True, False, False, False]
        assert all(r["cold_cleared"] is True for r in rows if r["target"] == "base" and r["cold"])
        assert acct["s3"]["pr"]["cold"] == {"gets": 3.0, "requests": 3.0, "bytes": 300.0}
        assert acct["s3"]["pr"]["warm_per_query"]["gets"] == 3.0
        assert {r["result"] for r in rows} == {"42"}
    finally:
        for s in (ref, base, pr):
            s.close()


def test_a_cold_run_that_could_not_clear_its_cache_says_so_and_a_bad_answer_is_not_a_latency(monkeypatch):
    monkeypatch.setattr(measure, "SETTLE_S", 0.0)
    ref, base, pr = FakeLakehouse(), FakeLakehouse(clear_status=403), FakeLakehouse(answer="")
    try:
        st = state({"ref-logs": ref.port, "base-logs": base.port, "pr-logs": pr.port})
        cell = measure.plan(SPEC, ["control-logs"], {"control-logs": ["logs count_total"]})[0]
        rows, _ = measure.run_cell(cell, st, SPEC, 3, {})
        assert [r["cold_cleared"] for r in rows if r["target"] == "base" and r["cold"]] == [False]
        bad = [r for r in rows if r["target"] == "pr"]
        assert all(not r["valid"] and r["reason"] == "empty result" for r in bad)
    finally:
        for s in (ref, base, pr):
            s.close()


class FlakyHot(FakeLakehouse):
    """hot: the first `bad` requests answer 500, then it is healthy."""

    def __init__(self, bad, **kw):
        self.bad = bad
        super().__init__(**kw)

    def handle(self, method, path, headers, body):
        if not path.startswith(("/metrics", "/internal")) and self.bad > 0:
            self.bad -= 1
            return 500, "boom", {}
        return super().handle(method, path, headers, body)


def test_n3_hot_is_asked_once_more_when_it_gave_nothing_valid(monkeypatch):
    monkeypatch.setattr(measure, "SETTLE_S", 0.0)
    ref, base, pr = FlakyHot(bad=3), FakeLakehouse(), FakeLakehouse()   # 3 runs of the cell: every first-pass hot answer fails
    try:
        st = state({"ref-logs": ref.port, "base-logs": base.port, "pr-logs": pr.port})
        cell = measure.plan(SPEC, ["control-logs"], {"control-logs": ["logs count_total"]})[0]
        rows, _ = measure.run_cell(cell, st, SPEC, 3, {})
        hot = [r for r in rows if r["target"] == "ref"]
        assert len(hot) == 3 and all(r["valid"] and r["retried"] for r in hot)
    finally:
        for s in (ref, base, pr):
            s.close()


def test_n3_hot_stays_invalid_when_it_keeps_failing_and_an_empty_answer_is_not_retried(monkeypatch):
    monkeypatch.setattr(measure, "SETTLE_S", 0.0)
    ref, base, pr = FlakyHot(bad=99), FakeLakehouse(), FakeLakehouse()
    empty = FakeLakehouse(answer="")
    try:
        st = state({"ref-logs": ref.port, "base-logs": base.port, "pr-logs": pr.port})
        cell = measure.plan(SPEC, ["control-logs"], {"control-logs": ["logs count_total"]})[0]
        rows, _ = measure.run_cell(cell, st, SPEC, 2, {})
        assert not any(r["valid"] for r in rows if r["target"] == "ref") and all(r.get("retried") for r in rows if r["target"] == "ref")
        st2 = state({"ref-logs": empty.port, "base-logs": base.port, "pr-logs": pr.port})
        rows, _ = measure.run_cell(cell, st2, SPEC, 2, {})
        assert not any(r.get("retried") for r in rows) and measure.cell_ref_needs_retry(rows) is False
    finally:
        for s in (ref, base, pr, empty):
            s.close()
