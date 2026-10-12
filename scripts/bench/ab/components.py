#!/usr/bin/env python3
"""Component figures for the perf smoke's summary: ingest throughput, forced compaction, base vs PR (and hot).

  components.py --state OUT/state.json --out OUT/components.json

Ingest: the same deterministic batch of rows (logs, jsonline) and spans (OTLP/JSON) goes to hot VictoriaLogs /
VictoriaTraces, to Lakehouse base and to Lakehouse PR, under a tenant (99:0) no query of the matrix reads, in
batches, `repeats` times; the figure is rows per second of the time the acknowledgements took (measured: the wall
time of the POSTs, which for Lakehouse includes the durable buffer write). Compaction: the forced recompaction of the
older hours that stack.py ran (state `ab.recompact`): files in/out, bytes written and seconds.

Not measured here (said in the table, never invented): flush latency, manifest refresh time.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import zlib
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402

TENANT = {"AccountID": "99", "ProjectID": "0"}
ROWS = 12000
SPANS = 6000
BATCH = 2000
REPEATS = 3
SERVICES = ("ab-gateway", "ab-orders", "ab-payments", "ab-users", "ab-notify")


def logs_batch(start: dt.datetime, n: int, tag: str) -> bytes:
    """Deterministic jsonline rows: no clock, no randomness."""
    lines = []
    for i in range(n):
        ts = (start + dt.timedelta(milliseconds=i * 7)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        lines.append(json.dumps({"ts": ts, "msg": f"ab component {tag} request {i} served in {i % 97} ms", "service.name": SERVICES[i % 5],
                                 "level": ("INFO", "WARN", "ERROR", "DEBUG")[i % 4], "req": f"{tag}-{i:06d}"}))
    return ("\n".join(lines) + "\n").encode()


def spans_batch(start: dt.datetime, n: int, tag: str) -> bytes:
    base = int(start.timestamp() * 1e9)
    spans = []
    for i in range(n):
        t0 = base + i * 7_000_000
        spans.append({"traceId": f"{zlib.crc32(f'{tag}/{i // 4}'.encode()):08x}" * 4, "spanId": f"{(i + 1) * 7919 % (1 << 64):016x}",
                      "name": f"op-{i % 11}", "kind": 2, "startTimeUnixNano": str(t0), "endTimeUnixNano": str(t0 + (1 + i % 53) * 1_000_000),
                      "attributes": [{"key": "http.status_code", "value": {"intValue": str(200 + (i % 3) * 100)}}]})
    body = {"resourceSpans": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": SERVICES[zlib.crc32(tag.encode()) % 5]}}]},
                               "scopeSpans": [{"scope": {"name": "ab"}, "spans": spans}]}]}
    return json.dumps(body).encode()


def post(url: str, data: bytes, content_type: str, timeout: float = 120.0) -> int:
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": content_type, **TENANT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 - loopback test stack
            r.read()
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def rate(count: int, seconds: float) -> float | None:
    return round(count / seconds) if seconds > 0 else None


def ingest(url: str, signal: str, start: dt.datetime, repeats: int = REPEATS) -> dict:
    """Rows (spans) per second over `repeats` runs of equal batches; every POST must answer 2xx."""
    total, secs, bad = 0, 0.0, 0
    n = ROWS if signal == "logs" else SPANS
    for rep in range(repeats):
        for off in range(0, n, BATCH):
            tag = f"{signal}-{rep}-{off}"
            t0 = start + dt.timedelta(seconds=rep * 60 + off // 100)
            if signal == "logs":
                body, ep, ct = logs_batch(t0, BATCH, tag), "/insert/jsonline?_stream_fields=service.name&_msg_field=msg&_time_field=ts", "application/stream+json"
            else:
                body, ep, ct = spans_batch(t0, BATCH, tag), "/insert/opentelemetry/v1/traces", "application/json"
            t = time.perf_counter()
            status = post(url + ep, body, ct)
            secs += time.perf_counter() - t
            if not 200 <= status < 300:
                bad += 1
            total += BATCH
    return {"rows": total, "seconds": round(secs, 2), "per_s": rate(total, secs), "bad_posts": bad}


def compaction_rows(state: dict) -> list[dict]:
    out = []
    for sig in ("logs", "traces"):
        figs = {}
        for t in ("base", "pr"):
            hours = (state.get("ab", {}).get("recompact") or {}).get(f"{t}-{sig}", {})
            inp = outp = byt = 0
            secs = 0.0
            for r in hours.values():
                try:
                    body = json.loads(r["body"]) if r["body"].startswith("{") and r["body"].endswith("}") else {}
                except ValueError:
                    body = {}
                inp += int(body.get("input_files", 0))
                outp += int(body.get("output_files", 0))
                byt += int(body.get("bytes_written", 0))
                secs += float(r.get("seconds", 0))
            figs[t] = {"in": inp, "out": outp, "bytes": byt, "seconds": round(secs, 2)}
        out.append({"component": "forced compaction of the older hours", "signal": sig,
                    "base": f"{figs['base']['in']} -> {figs['base']['out']} files, {figs['base']['bytes'] / 1e6:.2f} MB, {figs['base']['seconds']} s",
                    "pr": f"{figs['pr']['in']} -> {figs['pr']['out']} files, {figs['pr']['bytes'] / 1e6:.2f} MB, {figs['pr']['seconds']} s",
                    "unit": "files, MB written, wall s", "note": "POST /lakehouse/compaction/recompact per hour partition"})
    return out


def measure(state: dict, repeats: int = REPEATS) -> dict:
    start = dt.datetime.strptime(state["buffer"]["start"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc) + dt.timedelta(minutes=30)
    rows = []
    for sig, unit in (("logs", "rows/s"), ("traces", "spans/s")):
        figs = {}
        for t in ("ref", "base", "pr"):
            figs[t] = ingest(f"http://127.0.0.1:{state['ports'][f'{t}-{sig}']}", sig, start, repeats)
        bad = sum(f["bad_posts"] for f in figs.values())
        rows.append({"component": "ingest throughput (acknowledged)", "signal": sig, "ref": figs["ref"]["per_s"], "base": figs["base"]["per_s"],
                     "pr": figs["pr"]["per_s"], "unit": unit,
                     "note": f"{figs['base']['rows']} {unit.split('/')[0]} in batches of {BATCH}, tenant 99:0" + (f"; {bad} POSTs failed" if bad else "")})
    rows += compaction_rows(state)
    rows.append({"component": "flush latency, manifest refresh", "signal": "both", "base": "not measured", "pr": "not measured", "unit": "",
                 "note": "phase 1 does not time the flush; the stack parks the manifest refresh and the compaction scheduler while a layer is measured"})
    return {"rows": rows}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--repeats", type=int, default=REPEATS)
    a = ap.parse_args(argv)
    res = measure(abreg.load_json(a.state), a.repeats)
    abreg.dump_json(a.out, res)
    for r in res["rows"]:
        print(r)
    return 0


if __name__ == "__main__":
    sys.exit(main())
