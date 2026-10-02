#!/usr/bin/env python3
"""The hand-written batches of the reader matrix, sent to Lakehouse over its insert APIs.

  golden.py send <round 1|2> <manifest-dir>

`golden` (AccountID 4294967294, the largest writable tenant; 4294967295 is reserved and rejected, which is checked too):
  * timestamps that use every nanosecond digit position and every digit value (.123456789 ... .000000001),
    a row exactly on midnight and one a nanosecond before it, so the day directory and the half-open
    time windows are decided to the nanosecond;
  * odd map keys (spaces, quotes, `=`, upper case, non-ASCII, dots, a 100-character key (VictoriaLogs and VictoriaTraces limit field names to 128 characters, VictoriaTraces counting its `span_attr:` prefix: with a 120-character span attribute key the spans were dropped silently and a 200-character log field key made the whole line the message, so the longest key that is stored is used));
  * rows that exercise every check: errors of api-gateway, `format: nginx`, `rpc.system: grpc`,
    several rows (spans) per trace.
`bloom` (AccountID 4402, ProjectID 3): 60 rows and 60 spans in one hour per round, every trace ID
distinct, which makes every high-cardinality column's split-block bloom filter 96 bytes.

Each round sends half of the rows (alternating), so every hour partition gets two files and compaction
has something to merge. The manifest of the part is written before anything is sent.
"""
import json
import os
import random
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

import lib

ODD_KEYS = ["key with spaces", "quote'key", "a=b", "UPPER.Case", "unicode.ключ", "dots.in.a.key", "k" * 100, "tab-and-dash_key"]
DIGIT_FRACTIONS = [123456789, 987654321, 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 999999999, 0, 555555555]
SERVICES = ["api-gateway", "user-service", "order-service"]


def day_start(days_ago=2):
    d = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_ago)
    return int(d.timestamp()) * 10**9


def golden_rows():
    """(logs, spans): every field the manifest and the checks need, nothing random."""
    d0 = day_start()
    midnight = d0 + 24 * 3600 * 10**9
    stamps = []
    # last hour of day D: one row a nanosecond before midnight, the others on odd fractions
    stamps += [midnight - 1, midnight - 3600 * 10**9 + 1, midnight - 1800 * 10**9 + 123456789]
    stamps += [midnight - 600 * 10**9 + f for f in DIGIT_FRACTIONS[:6]]
    # first hour of day D+1: exactly on midnight, one nanosecond after, then the digit fractions
    stamps += [midnight, midnight + 1]
    stamps += [midnight + 60 * 10**9 + f for f in DIGIT_FRACTIONS]
    stamps = sorted(set(stamps))
    logs, spans = [], []
    for i, ts in enumerate(stamps):
        svc = SERVICES[i % 3]
        attrs = {"format": "nginx" if i % 3 == 0 else "logfmt", ODD_KEYS[i % len(ODD_KEYS)]: "v%d" % i}
        if i % 4 == 0:
            attrs["component"] = "grpc"
        logs.append({"ts": ts, "service": svc, "level": "ERROR" if i % 3 != 1 else "INFO", "msg": "golden row %d" % i,
                     "trace_id": "%032x" % (0x6010 + i // 3), "span_id": "%016x" % (0x700 + i), "attrs": attrs})
        sattrs = {"rpc.system": "grpc" if i % 2 == 0 else "http", ODD_KEYS[(i + 3) % len(ODD_KEYS)]: "s%d" % i, "thread.id": str(i)}
        spans.append({"ts": ts, "dur": 1000 + i, "service": svc, "error": i % 3 != 1, "name": "golden span %d" % i,
                      "trace_id": "%032x" % (0x7010 + i // 3), "span_id": "%016x" % (0x800 + i),
                      "parent": "" if i % 3 == 0 else "%016x" % (0x800 + i - 1), "attrs": sattrs})
    return logs, spans


def bloom_rows(round_no):
    rng = random.Random(4402 + round_no)
    base = day_start(1) + 10 * 3600 * 10**9      # hour 10 of yesterday
    logs, spans = [], []
    for i in range(60):
        ts = base + rng.randrange(0, 3600 * 10**9)
        svc = SERVICES[i % 3]
        tid = "%032x" % rng.getrandbits(128)
        logs.append({"ts": ts, "service": svc, "level": "ERROR" if i % 4 == 0 else "INFO", "msg": "bloom row %d/%d" % (round_no, i),
                     "trace_id": tid, "span_id": "%016x" % rng.getrandbits(64), "attrs": {"format": "nginx" if i % 2 == 0 else "logfmt"}})
        spans.append({"ts": ts + 1000, "dur": 1000, "service": svc, "error": i % 5 == 0, "name": "bloom span %d/%d" % (round_no, i),
                      "trace_id": tid, "span_id": "%016x" % rng.getrandbits(64), "parent": "",
                      "attrs": {"rpc.system": "grpc" if i % 2 == 0 else "http"}})
    # correlated rows: a trace that more than one log row carries (the trace-by-ID check needs one)
    for i in range(1, 6):
        logs[i]["trace_id"] = logs[0]["trace_id"]
    return logs, spans


def manifest_of(name, logs, spans):
    def sig(rows, kind):
        m = {"count": len(rows), "by_service": {}, "errors": 0, "field_filter": 0, "map_column": "log.attributes" if kind == "logs" else "span.attributes",
             "map_keys": {}, "map_filter": 0, "timestamps": sorted(r["ts"] for r in rows), "trace_counts": {}}
        for r in rows:
            m["by_service"][r["service"]] = m["by_service"].get(r["service"], 0) + 1
            err = r["level"] == "ERROR" if kind == "logs" else r["error"]
            if err:
                m["errors"] += 1
                if r["service"] == "api-gateway":
                    m["field_filter"] += 1
            for k, v in r["attrs"].items():
                m["map_keys"][k] = m["map_keys"].get(k, 0) + 1
                if (kind == "logs" and k == "format" and v == "nginx") or (kind == "traces" and k == "rpc.system" and v == "grpc"):
                    m["map_filter"] += 1
            m["trace_counts"][r["trace_id"]] = m["trace_counts"].get(r["trace_id"], 0) + 1
        m["ts_min"], m["ts_max"] = m["timestamps"][0], m["timestamps"][-1]
        return m
    t = lib.TENANTS[name]
    return {"name": name, "account_id": str(t["account"]), "project_id": str(t["project"]), "seed": 0,
            "logs": sig(logs, "logs"), "traces": sig(spans, "traces")}


def post(url, body, headers, ctype):
    req = urllib.request.Request(url, data=body, headers=dict(headers, **{"Content-Type": ctype}))
    with urllib.request.urlopen(req, timeout=120) as r:
        r.read()


def send_logs(name, rows):
    t = lib.TENANTS[name]
    body = "\n".join(json.dumps(dict(
        {"_time": lib.rfc3339_ns(r["ts"]), "_msg": r["msg"], "level": r["level"], "severity_number": 17 if r["level"] == "ERROR" else 9,
         "service.name": r["service"], "trace_id": r["trace_id"], "span_id": r["span_id"]}, **r["attrs"])) for r in rows) + "\n"
    post(lib.LOGS_URL + "/insert/jsonline?_stream_fields=service.name,level", body.encode(), t["headers"], "application/x-ndjson")


def send_traces(name, rows):
    t = lib.TENANTS[name]
    by = {}
    for r in rows:
        by.setdefault(r["service"], []).append(r)
    rs = []
    for svc, rr in by.items():
        spans = []
        for r in rr:
            end = r["ts"]
            sp = {"traceId": r["trace_id"], "spanId": r["span_id"], "name": r["name"], "kind": 2,
                  "startTimeUnixNano": str(end - r["dur"]), "endTimeUnixNano": str(end),
                  "attributes": [{"key": k, "value": {"stringValue": v}} for k, v in r["attrs"].items()]}
            if r["parent"]:
                sp["parentSpanId"] = r["parent"]
            if r["error"]:
                sp["status"] = {"code": 2}
            spans.append(sp)
        rs.append({"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": svc}}]},
                   "scopeSpans": [{"scope": {"name": "golden"}, "spans": spans}]})
    post(lib.TRACES_URL + "/insert/opentelemetry/v1/traces", json.dumps({"resourceSpans": rs}).encode(), t["headers"], "application/json")


def assert_reserved_tenant_rejected():
    """AccountID 4294967295 is reserved: both insert APIs answer 400 and store nothing."""
    import urllib.error
    hdr = {"AccountID": "4294967295", "ProjectID": "0"}
    for name, url, body, ctype in (
            ("logs", lib.LOGS_URL + "/insert/jsonline", b'{"_msg":"x","_time":"2026-01-01T00:00:00Z"}\n', "application/x-ndjson"),
            ("traces", lib.TRACES_URL + "/insert/opentelemetry/v1/traces", b'{"resourceSpans":[]}', "application/json")):
        try:
            post(url, body, hdr, ctype)
        except urllib.error.HTTPError as e:
            assert e.code == 400 and b"reserved" in e.read(), "%s: AccountID 4294967295 must be rejected as reserved" % name
            continue
        raise AssertionError("%s: AccountID 4294967295 was accepted; it is reserved" % name)
    print("golden.py: AccountID 4294967295 is rejected as reserved by both insert APIs")


def main():
    round_no, mdir = int(sys.argv[2]), sys.argv[3]
    if round_no == 1:
        assert_reserved_tenant_rejected()
    os.makedirs(mdir, exist_ok=True)
    plan = {"golden": golden_rows(), "bloom": bloom_rows(round_no)}
    for name, (logs, spans) in plan.items():
        if name == "golden":  # alternate rows go in alternate rounds
            logs = [r for i, r in enumerate(logs) if i % 2 == round_no % 2]
            spans = [r for i, r in enumerate(spans) if i % 2 == round_no % 2]
        lib.write_json(os.path.join(mdir, "%s-r%d.json" % (name, round_no)), manifest_of(name, logs, spans))
        send_logs(name, logs)
        send_traces(name, spans)
        print("golden.py round %d: %s %d logs + %d spans sent" % (round_no, name, len(logs), len(spans)))


if __name__ == "__main__":
    if sys.argv[1] != "send":
        sys.exit(__doc__)
    main()
