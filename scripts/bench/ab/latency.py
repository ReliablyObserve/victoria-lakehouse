#!/usr/bin/env python3
"""S3 latency injection that is verified, not assumed.

The 2026-09 baseline (v3.3) ran with "S3 latency 20/50/100 ms" columns that had no latency at all: the
injector (scripts/inject-s3-latency.sh) exec'd into a toxiproxy container named for a different compose
project, the failure was swallowed by `|| true` / a missing `set -e` in the caller, and nothing measured the
delay afterwards. This module fixes all three:

  * the toxic is set through toxiproxy's HTTP API, the call must answer 2xx, and the toxic is read back;
  * `probe` times the same tiny request through the proxy and directly to the S3 service, interleaved, and
    fails (exit 1) when the measured difference is off the injected value by more than the tolerance;
  * `lh_s3_view` reads Lakehouse's own `lakehouse_s3_request_duration_seconds` histogram, so the run also
    shows that Lakehouse's S3 traffic really crosses the proxy (the original bug was a wrong path).

  latency.py set   --api URL 50 [--jitter 0]      add or replace the toxic, verify it, print it
  latency.py clear --api URL                      remove the toxic, verify it is gone
  latency.py probe --api URL --direct URL --proxied URL --expect 50 [--n 15]
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request

PROXY = "s3"
TOXIC = "ab-latency"
# The probe passes when the measured added delay is within max(TOL_MIN_MS, TOL_REL * expected) of the injected one.
TOL_MIN_MS = 10.0
TOL_REL = 0.25


class LatencyError(RuntimeError):
    pass


def _call(method: str, url: str, body: dict | None = None, timeout: float = 10.0) -> tuple[int, bytes]:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 - loopback test stack
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def toxic_payload(ms: float, jitter_ms: float = 0.0) -> dict:
    """The toxiproxy request body. Downstream only: one delay per response, the way a remote region adds
    one round trip to a request; upstream as well would count it twice."""
    return {"name": TOXIC, "type": "latency", "stream": "downstream", "toxicity": 1.0,
            "attributes": {"latency": int(round(ms)), "jitter": int(round(jitter_ms))}}


def list_toxics(api: str) -> list[dict]:
    status, body = _call("GET", f"{api}/proxies/{PROXY}/toxics")
    if status != 200:
        raise LatencyError(f"toxiproxy {api} answered {status} for the toxic list of proxy '{PROXY}': {body[:200]!r}")
    return json.loads(body)


def current(api: str) -> float | None:
    """The injected latency in ms, None when there is no toxic."""
    for t in list_toxics(api):
        if t.get("name") == TOXIC:
            return float(t["attributes"]["latency"])
    return None


def set_latency(api: str, ms: float, jitter_ms: float = 0.0) -> float:
    """Inject `ms` of latency (0 removes the toxic). Raises unless the proxy confirms the value."""
    if ms <= 0:
        return clear(api)
    status, body = _call("POST", f"{api}/proxies/{PROXY}/toxics", toxic_payload(ms, jitter_ms))
    if status == 409:  # exists: replace it in place
        status, body = _call("POST", f"{api}/proxies/{PROXY}/toxics/{TOXIC}", toxic_payload(ms, jitter_ms))
    if status != 200:
        raise LatencyError(f"toxiproxy refused the latency toxic: HTTP {status} {body[:200]!r}")
    got = current(api)
    if got != float(int(round(ms))):
        raise LatencyError(f"the toxic was set to {ms} ms but the proxy reports {got}")
    return got


def clear(api: str) -> float:
    status, body = _call("DELETE", f"{api}/proxies/{PROXY}/toxics/{TOXIC}")
    if status not in (200, 204, 404):
        raise LatencyError(f"toxiproxy could not remove the toxic: HTTP {status} {body[:200]!r}")
    if current(api) is not None:
        raise LatencyError("the latency toxic is still present after removal")
    return 0.0


def _time_get(url: str, timeout: float = 10.0) -> float:
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310 - loopback test stack
            r.read()
    except urllib.error.HTTPError:
        pass  # any HTTP answer is a round trip; only a transport failure is not
    return (time.perf_counter() - t0) * 1000.0


def tolerance(expect_ms: float) -> float:
    return max(TOL_MIN_MS, TOL_REL * expect_ms)


def evaluate(direct_ms: list[float], proxied_ms: list[float], expect_ms: float) -> dict:
    """Pure part of the probe: the added delay is the median of the pairwise differences."""
    if not direct_ms or len(direct_ms) != len(proxied_ms):
        raise ValueError("the probe needs the same positive number of direct and proxied samples")
    diffs = [p - d for d, p in zip(direct_ms, proxied_ms)]
    delta = statistics.median(diffs)
    tol = tolerance(expect_ms)
    return {"expect_ms": expect_ms, "tolerance_ms": round(tol, 1), "added_ms": round(delta, 1),
            "direct_p50_ms": round(statistics.median(direct_ms), 2), "proxied_p50_ms": round(statistics.median(proxied_ms), 2),
            "samples": len(diffs), "ok": abs(delta - expect_ms) <= tol}


def probe(direct_url: str, proxied_url: str, expect_ms: float, n: int = 15, warm: int = 3) -> dict:
    """Time `n` pairs of the same request, direct and through the proxy, interleaved."""
    for _ in range(warm):
        _time_get(direct_url)
        _time_get(proxied_url)
    direct, proxied = [], []
    for _ in range(n):
        direct.append(_time_get(direct_url))
        proxied.append(_time_get(proxied_url))
    return evaluate(direct, proxied, expect_ms)


def metric_value(text: str, name: str) -> float | None:
    """The value of an unlabelled sample `name` in Prometheus text format."""
    for line in text.splitlines():
        if line.startswith(name + " ") or line.startswith(name + "\t"):
            try:
                return float(line.split()[-1])
            except ValueError:
                return None
    return None


def s3_duration(metrics_text: str) -> tuple[float, float]:
    """(sum seconds, count) of lakehouse_s3_request_duration_seconds."""
    return (metric_value(metrics_text, "lakehouse_s3_request_duration_seconds_sum") or 0.0,
            metric_value(metrics_text, "lakehouse_s3_request_duration_seconds_count") or 0.0)


def mean_ms(before: tuple[float, float], after: tuple[float, float]) -> float | None:
    """Mean S3 request duration in ms over the window between two scrapes; None when no request happened."""
    dn = after[1] - before[1]
    if dn <= 0:
        return None
    return (after[0] - before[0]) / dn * 1000.0


def lh_view_ok(mean_at_zero: float | None, mean_at_l: float | None, expect_ms: float) -> dict:
    """Lakehouse's own view: the mean S3 request duration rose by about the injected latency."""
    if mean_at_zero is None or mean_at_l is None:
        return {"ok": False, "reason": "no S3 request observed in a window"}
    rise = mean_at_l - mean_at_zero
    tol = max(TOL_MIN_MS, 0.35 * expect_ms)  # a mixed GET/range workload: looser than the single-request probe
    return {"ok": abs(rise - expect_ms) <= tol, "mean_at_0_ms": round(mean_at_zero, 1), "mean_at_expect_ms": round(mean_at_l, 1),
            "rise_ms": round(rise, 1), "expect_ms": expect_ms, "tolerance_ms": round(tol, 1)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("set")
    s.add_argument("--api", required=True)
    s.add_argument("ms", type=float)
    s.add_argument("--jitter", type=float, default=0.0)
    c = sub.add_parser("clear")
    c.add_argument("--api", required=True)
    p = sub.add_parser("probe")
    p.add_argument("--api", required=True)
    p.add_argument("--direct", required=True)
    p.add_argument("--proxied", required=True)
    p.add_argument("--expect", type=float, required=True)
    p.add_argument("--n", type=int, default=15)
    p.add_argument("--timing-only", action="store_true",
                   help="do not require the proxy to report the toxic; judge the measured delay alone (shows what an injector that swallowed its error left behind)")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "set":
            print(json.dumps({"injected_ms": set_latency(a.api, a.ms, a.jitter)}))
            return 0
        if a.cmd == "clear":
            print(json.dumps({"injected_ms": clear(a.api)}))
            return 0
        got = None if a.timing_only else current(a.api)
        if not a.timing_only and (got or 0.0) != a.expect:
            print(json.dumps({"ok": False, "reason": f"the proxy has {got} ms injected, the probe expects {a.expect}"}))
            return 1
        res = probe(a.direct, a.proxied, a.expect, a.n)
        print(json.dumps(res))
        return 0 if res["ok"] else 1
    except LatencyError as e:
        print(json.dumps({"ok": False, "reason": str(e)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
