"""Latency injection is verified, not assumed (the 2026-09 v3.3 baseline had none: wrong container, error swallowed)."""
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


def test_probe_times_the_proxied_leg_against_the_direct_leg():
    import time

    def slow(method, path, headers, body):
        time.sleep(0.03)
        return 200, "ok", {}
    fast, delayed = Server(lambda *a: (200, "ok", {})), Server(slow)
    try:
        res = latency.probe(fast.url + "/health", delayed.url + "/health", 30, n=5, warm=1)
        assert res["ok"] and 20 < res["added_ms"] < 80
        assert latency.probe(fast.url + "/health", fast.url + "/health", 30, n=5, warm=1)["ok"] is False
    finally:
        fast.close()
        delayed.close()


def test_lakehouse_s3_histogram_view():
    text = "x 1\nlakehouse_s3_request_duration_seconds_sum 2.5\nlakehouse_s3_request_duration_seconds_count 10\n"
    assert latency.s3_duration(text) == (2.5, 10.0)
    assert latency.mean_ms((2.5, 10), (3.5, 20)) == pytest.approx(100.0)
    assert latency.mean_ms((1, 5), (1, 5)) is None
    assert latency.lh_view_ok(5.0, 56.0, 50)["ok"] is True
    assert latency.lh_view_ok(5.0, 8.0, 50)["ok"] is False
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
