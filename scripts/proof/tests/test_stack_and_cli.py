"""Stack helpers (state windows, datagen command, buffer count) and the runner command line."""
import datetime as dt
import json
import os
import re

import pytest

from scripts.proof import stack
from scripts.proof.runner import run as runner
from scripts.proof.tests.test_runner import FV_ROW, STATE, VALUES, serve, state_for, stop, targets


def test_state_windows_are_hour_aligned_and_disjoint():
    st = stack.make_state(dt.datetime(2026, 10, 7, 16, 37, 12, tzinfo=dt.timezone.utc))
    assert st["H"] == "2026-10-07T15:00:00Z"
    assert st["cold"] == {"start": "2026-10-07T11:00:00Z", "end": "2026-10-07T15:00:00Z"}
    assert st["buffer"] == {"start": "2026-10-07T15:00:00Z", "end": "2026-10-07T16:00:00Z"}
    for w in (st["cold"], st["buffer"]):
        assert all(re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:00:00Z", w[k]) for k in w)
    assert st["tenants"]["alias"] == {"acme-corp": "1001:0"}
    assert set(st["ports"].values()) <= set(range(48000, 48500)) and len(set(st["ports"].values())) == len(st["ports"])


def test_datagen_command_uses_the_same_seed_and_now_for_every_side():
    t = stack.TENANTS[1]
    a = stack.datagen_cmd("2026-10-07T15:00:00Z", 4, t, hot=True, lh="base")
    b = stack.datagen_cmd("2026-10-07T15:00:00Z", 4, t, hot=False, lh="pr")
    assert "--seed=12" in a and "--seed=12" in b and "--now=2026-10-07T15:00:00Z" in a and "--now=2026-10-07T15:00:00Z" in b
    assert "--vl-endpoint=http://vl-ref:9428" in a and "--vl-endpoint=http://vl-ref:9428" not in b
    assert "--lh-logs-endpoint=http://lh-logs-base:9428" in a and "--lh-traces-endpoint=http://lh-traces-pr:10428" in b
    assert "--account-id=1001" in a and stack.NETWORK in a


def test_buffered_rows_counts_span_rows_only_for_traces(monkeypatch):
    body = '{"span_id":"a"}\n{"x":1}\n\nnot json\n{"span_id":"b"}\n'
    monkeypatch.setattr(stack, "http_json", lambda url, headers=None, timeout=30: body)
    assert stack.buffered_rows(1, "traces", "2026-10-07T15:00:00Z", "2026-10-07T16:00:00Z", "0", "0") == 2
    assert stack.buffered_rows(1, "logs", "2026-10-07T15:00:00Z", "2026-10-07T16:00:00Z", "0", "0") == 4


def test_http_json_reads_a_loopback_server():
    srv = serve({"/p": (200, '{"a": 1}')})
    try:
        assert json.loads(stack.http_json(f"http://127.0.0.1:{srv.server_port}/p")) == {"a": 1}
    finally:
        srv.shutdown()
        srv.server_close()


def test_wait_drained_returns_when_buffers_are_empty_and_raises_otherwise(monkeypatch):
    monkeypatch.setattr(stack, "buffered_rows", lambda *a, **k: 0)
    stack.wait_drained(STATE, timeout=1)
    monkeypatch.setattr(stack, "buffered_rows", lambda *a, **k: 3)
    monkeypatch.setattr(stack.time, "sleep", lambda s: None)
    with pytest.raises(SystemExit):
        stack.wait_drained(STATE, timeout=0)


def test_pick_trace_asks_the_reference(monkeypatch):
    seen = {}

    def fake(url, headers=None, timeout=30):
        seen["url"] = url
        return '{"trace_id":"abc123"}\n'
    monkeypatch.setattr(stack, "http_json", fake)
    assert stack.pick_trace(STATE) == "abc123"
    assert str(stack.PORTS["ref-traces"]) in seen["url"] and "sort+by+%28trace_id%29" in seen["url"]


def test_buffered_rows_in_runner_is_informational_only(monkeypatch):
    srvs = targets(VALUES["values"], VALUES["values"])
    st = state_for(srvs)
    try:
        assert runner.buffered_rows(st, "ref", "logs", "numeric", "buffer") is None
        assert runner.buffered_rows(st, "base", "logs", "numeric", "cold") is None
        assert runner.buffered_rows(st, "base", "logs", "numeric", "buffer") == 0
        monkeypatch.setattr(stack, "buffered_rows", lambda *a, **k: 1 / 0)
        assert runner.buffered_rows(st, "base", "logs", "numeric", "buffer") is None  # never fatal
    finally:
        stop(srvs)


def test_count_rows_handles_bad_answers():
    srv = serve({"/select/logsql/query": (200, "not json"), })
    st = state_for({"ref": srv})
    try:
        assert runner.count_rows(st, "ref", "logs", "numeric", "cold") is None
        srv.routes = {"/select/logsql/query": (500, "x")}
        assert runner.count_rows(st, "ref", "logs", "numeric", "cold") is None
    finally:
        srv.shutdown()
        srv.server_close()


def test_cli_exit_codes_and_reports(tmp_path, monkeypatch, capsys):
    srvs = targets(base_values=VALUES["values"], pr_values=[{"value": "GET", "hits": 5}])
    st = state_for(srvs)
    sp = tmp_path / "state.json"
    sp.write_text(json.dumps(st))
    rows = tmp_path / "rows.json"
    rows.write_text(json.dumps({"rows": [FV_ROW, {**FV_ROW, "id": "other"}]}))
    monkeypatch.setattr(runner, "buffered_rows", lambda *a, **k: None)
    try:
        rc = runner.main(["--state", str(sp), "--out", str(tmp_path / "o"), "--tier", str(rows), "--only", "^fv$", "--no-seed-check"])
    finally:
        stop(srvs)
    assert rc == 1  # the PR regressed
    out = capsys.readouterr().out
    assert "fv.numeric.cold" in out and "other" not in out
    assert os.path.exists(tmp_path / "o" / "report.md") and os.path.exists(tmp_path / "o" / "bodies.jsonl.gz")


def test_settled_buffered_waits_for_two_equal_nonzero_reads(monkeypatch):
    reads = iter([{"a": 0}, {"a": 5}, {"a": 8}, {"a": 8}])
    monkeypatch.setattr(stack, "read_buffered", lambda state: next(reads))
    monkeypatch.setattr(stack.time, "sleep", lambda s: None)
    assert stack.settled_buffered(STATE) == {"a": 8}
    monkeypatch.setattr(stack, "read_buffered", lambda state: {"a": 0})
    with pytest.raises(SystemExit):
        stack.settled_buffered(STATE, timeout=-1)


def test_read_buffered_covers_both_variants_signals_and_tenants(monkeypatch):
    monkeypatch.setattr(stack, "buffered_rows", lambda *a, **k: 7)
    got = stack.read_buffered(STATE)
    assert len(got) == 8 and set(got.values()) == {7} and "pr-traces-1001" in got
