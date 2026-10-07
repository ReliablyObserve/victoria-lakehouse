"""Record reference answers from hot VictoriaLogs v1.53.0 and VictoriaTraces v0.12.0.

Not run in CI. The stack was two plain containers (own names, loopback ports) fed by
`go run ./cmd/datagen -logs 300 -traces 60 -hours-back 1 -seed 437 -now <T>` plus one
hand-written OTLP/JSON trace (4 spans with events, links, scope attributes, status
message and resource attributes, see OTLP_TRACE below) posted to
/insert/opentelemetry/v1/traces. Queries use absolute windows.

  python3 scripts/proof/fixtures/record.py --vl http://127.0.0.1:56128 --vt http://127.0.0.1:56110

Each answer is stored as recorded/<name>.json: request, status, content type, raw body text.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recorded")
START, END = "2026-10-07T06:00:00Z", "2026-10-07T08:00:00Z"
TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
START_US, END_US = 1791352800000000, 1791360000000000  # the window in microseconds


def fetch(base: str, method: str, path: str, params: dict) -> dict:
    data = urllib.parse.urlencode(params).encode()
    if method == "GET":
        url, body = f"{base}{path}?{data.decode()}" if params else f"{base}{path}", None
    else:
        url, body = f"{base}{path}", data
    req = urllib.request.Request(url, data=body, method=method)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status, ctype, text = r.status, r.headers.get("Content-Type", ""), r.read().decode()
    except urllib.error.HTTPError as e:
        status, ctype, text = e.code, e.headers.get("Content-Type", ""), e.read().decode()
    return {"request": {"method": method, "path": path, "params": params}, "status": status,
            "content_type": ctype, "latency_ms": round((time.perf_counter() - t0) * 1000, 2), "body": text}


def save(name: str, rec: dict) -> None:
    raw = json.dumps(rec, indent=1, sort_keys=True) + "\n"
    if len(raw) > 64 * 1024:
        with gzip.GzipFile(os.path.join(OUT, name + ".json.gz"), "wb", mtime=0) as f:
            f.write(raw.encode())
    else:
        with open(os.path.join(OUT, name + ".json"), "w") as f:
            f.write(raw)
    print(f"{name}: {rec['status']} {len(rec['body'])} bytes")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vl", required=True)
    ap.add_argument("--vt", required=True)
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    w = {"start": START, "end": END}
    vl = [
        ("vl_query", "POST", "/select/logsql/query", {**w, "query": "level:ERROR | sort by (_time) desc | limit 12"}),
        ("vl_query_sorted", "POST", "/select/logsql/query", {**w, "query": "* | sort by (_time) desc | limit 120"}),
        ("vl_tenant_ids", "POST", "/select/tenant_ids", {**w}),
        ("vl_hits", "POST", "/select/logsql/hits", {**w, "query": "*", "step": "30m"}),
        ("vl_hits_by_level", "POST", "/select/logsql/hits", {**w, "query": "*", "step": "30m", "field": "level"}),
        ("vl_field_names", "POST", "/select/logsql/field_names", {**w, "query": "*"}),
        ("vl_field_values", "POST", "/select/logsql/field_values", {**w, "query": "*", "field": "level"}),
        ("vl_streams", "POST", "/select/logsql/streams", {**w, "query": "*", "limit": "5"}),
        ("vl_stats_query", "POST", "/select/logsql/stats_query", {**w, "query": "* | stats count() c"}),
        ("vl_stats_query_by", "POST", "/select/logsql/stats_query", {**w, "query": "* | stats by (level) count() c"}),
        ("vl_stats_count_uniq", "POST", "/select/logsql/stats_query", {**w, "query": '* | stats count_uniq(level) as level, count_uniq(http.method) as "http.method"'}),
        ("vl_stats_query_range", "POST", "/select/logsql/stats_query_range", {**w, "query": "* | stats by (level) count() c", "step": "30m"}),
        ("vl_facets", "POST", "/select/logsql/facets", {**w, "query": "*", "limit": "3"}),
        ("vl_bad_query", "POST", "/select/logsql/query", {**w, "query": "* | nosuchpipe"}),
    ]
    vt = [
        ("vt_query_trace_rows", "POST", "/select/logsql/query", {**w, "query": f"trace_id:{TRACE_ID} | sort by (start_time_unix_nano)"}),
        ("vt_field_names", "POST", "/select/logsql/field_names", {**w, "query": f"trace_id:{TRACE_ID}"}),
        ("vt_field_values_service", "POST", "/select/logsql/field_values", {**w, "query": "*", "field": "resource_attr:service.name"}),
        ("vt_stats_query", "POST", "/select/logsql/stats_query", {**w, "query": "* | stats count() c"}),
        ("jaeger_trace", "GET", f"/select/jaeger/api/traces/{TRACE_ID}", {}),
        ("vt_tenant_ids", "POST", "/select/tenant_ids", {**w}),
        ("jaeger_search_order", "GET", "/select/jaeger/api/traces", {"service": "order-service", "start": str(START_US), "end": str(END_US), "limit": "5"}),
        ("jaeger_services", "GET", "/select/jaeger/api/services", {}),
        ("jaeger_operations", "GET", "/select/jaeger/api/services/proof-cart/operations", {}),
        ("jaeger_search", "GET", "/select/jaeger/api/traces", {"service": "proof-cart", "start": str(START_US), "end": str(END_US), "limit": "5"}),
        ("jaeger_dependencies", "GET", "/select/jaeger/api/dependencies", {"endTs": str(END_US // 1000), "lookback": "7200000"}),
        ("jaeger_trace_missing", "GET", "/select/jaeger/api/traces/00000000000000000000000000000001", {}),
        ("tempo_trace_v2", "GET", f"/select/tempo/api/v2/traces/{TRACE_ID}", {"start": str(START_US // 10**6), "end": str(END_US // 10**6)}),
        ("tempo_search", "GET", "/select/tempo/api/search", {"q": '{ resource.service.name = "proof-cart" }', "start": str(START_US // 10**6), "end": str(END_US // 10**6), "limit": "5"}),
        ("tempo_tag_values", "GET", "/select/tempo/api/v2/search/tag/resource.service.name/values", {"start": str(START_US // 10**6), "end": str(END_US // 10**6)}),
        ("tempo_tags", "GET", "/select/tempo/api/v2/search/tags", {"start": str(START_US // 10**6), "end": str(END_US // 10**6)}),
    ]
    for name, m, p, q in vl:
        save(name, fetch(a.vl, m, p, q))
    for name, m, p, q in vt:
        save(name, fetch(a.vt, m, p, q))


if __name__ == "__main__":
    main()
