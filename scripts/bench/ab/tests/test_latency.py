"""Latency injection is verified, not assumed (the 2026-09 v3.3 baseline had none: wrong container, error swallowed)."""
import time
import types

import pytest

import latency
from fakes import FakeToxiproxy, Server


def test_set_verifies_the_toxic_and_clear_removes_it():
    s = FakeToxiproxy()
    try:
        assert latency.current(s.url) is None
        assert latency.set_latency(s.url, 50) == 50.0
        assert latency.current(s.url) == 50.0
        assert latency.set_latency(s.url, 100) == 100.0  # replaces in place (409 -> update)
        assert latency.current(s.url) == 100.0
        assert latency.set_latency(s.url, 0) == 0.0
        assert latency.current(s.url) is None
        latency.clear(s.url)  # clearing nothing is fine
    finally:
        s.close()


def test_an_injector_that_fails_raises_instead_of_being_swallowed():
    s = FakeToxiproxy(broken=True)
    try:
        with pytest.raises(latency.LatencyError):
            latency.set_latency(s.url, 50)
    finally:
        s.close()


def test_an_injector_that_answers_200_but_changes_nothing_is_caught_by_the_read_back():
    s = FakeToxiproxy(ignores=True)
    try:
        with pytest.raises(latency.LatencyError, match="reports None"):
            latency.set_latency(s.url, 50)
    finally:
        s.close()


def test_toxic_is_downstream_only_so_a_request_pays_the_delay_once():
    p = latency.toxic_payload(50, 3)
    assert p["stream"] == "downstream" and p["attributes"] == {"latency": 50, "jitter": 3}


def test_probe_evaluation_passes_on_the_injected_delay_and_fails_off_it():
    direct = [1.0, 1.1, 0.9, 1.0, 1.2]
    assert latency.evaluate(direct, [d + 51 for d in direct], 50)["ok"] is True
    assert latency.evaluate(direct, [d + 0.3 for d in direct], 50)["ok"] is False  # the old injector: nothing was added
    assert latency.evaluate(direct, [d + 0.3 for d in direct], 0)["ok"] is True
    assert latency.evaluate(direct, [d + 90 for d in direct], 50)["ok"] is False  # twice the delay is wrong too
    with pytest.raises(ValueError):
        latency.evaluate([], [], 50)


def test_tolerance_is_a_floor_or_a_quarter():
    assert latency.tolerance(0) == latency.TOL_MIN_MS
    assert latency.tolerance(100) == 25


class FakeClock:
    """Virtual time for `latency._time_get`: only the servers move it, so a timed request costs exactly what its handler adds."""

    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


def _legs(monkeypatch, delay_s):
    """Two real HTTP servers, the second costing `delay_s` of virtual time per request; the probe reads the virtual clock."""
    clock = FakeClock()
    monkeypatch.setattr(latency, "time", types.SimpleNamespace(perf_counter=clock.now, sleep=time.sleep))

    def slow(method, path, headers, body):
        clock.advance(delay_s)
        return 200, "ok", {}
    return Server(lambda *a: (200, "ok", {})), Server(slow)


def test_probe_times_the_proxied_leg_against_the_direct_leg(monkeypatch):
    # Virtual clock: the answer is exact and scheduling cannot move it (the wall-clock version of this test flaked under load, #862).
    fast, delayed = _legs(monkeypatch, 0.3)
    try:
        res = latency.probe(fast.url + "/health", delayed.url + "/health", 300, n=5, warm=1)
        assert res["added_ms"] == 300.0 and res["ok"] is True
        assert res["direct_p50_ms"] == 0.0 and res["proxied_p50_ms"] == 300.0     # the delay lands on the proxied leg only
        assert res["samples"] == 5 and len(delayed.calls) == 6 and len(fast.calls) == 6   # warm + n pairs, both legs requested
        before = len(fast.calls)
        same = latency.probe(fast.url + "/health", fast.url + "/health", 300, n=4, warm=0)
        assert same["added_ms"] == 0.0 and same["ok"] is False                      # no delay between the legs is reported as such
        assert same["samples"] == 4 and len(fast.calls) - before == 8               # n is honoured (not a fixed 5): 4 pairs, no warm-up
        swapped = latency.probe(delayed.url + "/health", fast.url + "/health", 300, n=5, warm=1)
        assert swapped["added_ms"] == -300.0 and swapped["ok"] is False             # the legs are not interchangeable
    finally:
        fast.close()
        delayed.close()


def test_probe_measures_real_elapsed_time_of_the_proxied_leg():
    # Real clock, one-sided: a sleep never returns early, so the proxied leg takes at least the injected delay however loaded the host is.
    delay_s = 0.2

    def slow(method, path, headers, body):
        time.sleep(delay_s)
        return 200, "ok", {}
    fast, delayed = Server(lambda *a: (200, "ok", {})), Server(slow)
    try:
        res = latency.probe(fast.url + "/health", delayed.url + "/health", delay_s * 1000, n=3, warm=0)
        assert res["proxied_p50_ms"] >= delay_s * 1000 - 1     # 1 ms: perf_counter and sleep resolution
        assert res["added_ms"] > delay_s * 1000 / 2            # only a direct-leg median of about 100 ms breaks this: measured direct p50 <= 1.39 ms at load average 185 (about 70x)
    finally:
        fast.close()
        delayed.close()


def test_lakehouse_s3_histogram_view():
    text = "x 1\nlakehouse_s3_request_duration_seconds_sum 2.5\nlakehouse_s3_request_duration_seconds_count 10\n"
    assert latency.s3_duration(text) == (2.5, 10.0)
    assert latency.mean_ms((2.5, 10), (3.5, 20)) == pytest.approx(100.0)
    assert latency.mean_ms((1, 5), (1, 5)) is None
    assert latency.lh_view_ok(5.0, 56.0, 50)["ok"] is True
    assert latency.lh_view_ok(5.0, 90.0, 50)["ok"] is True      # queueing on shared connections: measured +85 ms for 50 ms
    assert latency.lh_view_ok(5.0, 8.0, 50)["ok"] is False      # the traffic does not cross the proxy
    assert latency.lh_view_ok(5.0, 200.0, 50)["ok"] is False    # far more than the injected delay
    assert latency.lh_view_ok(None, 56.0, 50)["ok"] is False


def test_cli_probe_refuses_when_the_proxy_does_not_hold_the_toxic():
    s = FakeToxiproxy()
    try:
        assert latency.main(["probe", "--api", s.url, "--direct", s.url, "--proxied", s.url, "--expect", "50"]) == 1
        assert latency.main(["set", "--api", s.url, "50"]) == 0
        assert latency.main(["clear", "--api", s.url]) == 0
        assert latency.main(["set", "--api", s.url, "50"]) == 0
        s.broken = True
        assert latency.main(["clear", "--api", s.url]) == 1
    finally:
        s.close()


def test_lakehouse_own_view_takes_the_histogram_difference_around_one_cold_query():
    state = {"sum": 1.0, "count": 10.0}

    def handler(method, path, headers, body):
        if path.startswith("/metrics"):
            return 200, f"lakehouse_s3_request_duration_seconds_sum {state['sum']}\nlakehouse_s3_request_duration_seconds_count {state['count']}\n", {}
        if path.startswith("/internal/cache/clear"):
            return (200, "{}", {}) if headers.get("Authorization") == "Bearer key" else (401, "no", {})
        return 404, "", {}
    s = Server(handler)
    try:
        def query():
            state["sum"] += 0.4   # four requests of 100 ms
            state["count"] += 4
        assert latency.lh_s3_mean_ms(s.url, query, "key", settle_s=0, sleep=lambda x: None) == pytest.approx(100.0)
        assert latency.lh_s3_mean_ms(s.url, query, "wrong", settle_s=0, sleep=lambda x: None) is None  # the cache could not be cleared: no figure
    finally:
        s.close()
