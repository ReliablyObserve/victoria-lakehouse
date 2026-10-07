"""One HTTP request to one target, recorded as the answer envelope the metrics library scores."""
from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request

TIMEOUT_S = 60.0


def envelope(status: int, body: str, latency_ms: float, content_type: str = "", *, error_kind: str = "",
             warnings: list | None = None, **extra) -> dict:
    """Raw body text, HTTP status, latency, and the transport error kept apart from the status
    (a timeout is not a 5xx; the metrics library treats `timeout` as blocked)."""
    out = {"status": status, "body": body, "latency_ms": round(latency_ms, 2), "content_type": content_type}
    if error_kind:
        out["error_kind"] = error_kind
        out["timeout"] = error_kind == "timeout"
    if warnings:
        out["warnings"] = warnings
    out.update(extra)
    return out


def send(method: str, url: str, params: dict, headers: dict, timeout: float = TIMEOUT_S) -> dict:
    data = None
    full = url
    if method == "POST":
        data = urllib.parse.urlencode(params).encode()
        headers = {**headers, "Content-Type": "application/x-www-form-urlencoded"}
    elif params:
        full = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, data=data, headers=headers, method=method)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 - loopback proof stack
            body = r.read().decode("utf-8", "replace")
            return envelope(r.status, body, (time.perf_counter() - t0) * 1000, r.headers.get("Content-Type", ""))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        return envelope(e.code, body, (time.perf_counter() - t0) * 1000, e.headers.get("Content-Type", ""))
    except TimeoutError:
        return envelope(0, "", (time.perf_counter() - t0) * 1000, error_kind="timeout")
    except (urllib.error.URLError, OSError) as e:
        kind = "timeout" if "timed out" in str(e).lower() else "connect"
        return envelope(0, "", (time.perf_counter() - t0) * 1000, error_kind=kind)
