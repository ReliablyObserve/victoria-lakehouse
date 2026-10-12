#!/usr/bin/env python3
"""The A/B matrix: every selected shape x layer x range x tenant form against hot VL/VT (ref), Lakehouse built
from the base commit (base) and from the PR (pr), interleaved per request, every answer validated.

  measure.py --state OUT/state.json --set control-logs --set control-traces --runs 6 --out OUT/raw.json

Per cell and target the first run is cold (the Lakehouse caches are cleared first, with the peer key and the
answer asserted: a cold run that could not clear its cache is recorded as such, never silently warm) and the
others are warm. The targets are asked one after the other for each run, in an order that rotates, on the same
absolute window (the seeded data is static, so the answers must be identical). The S3 counters of /metrics are
read per phase (before and after the cold run, after the warm runs), each time only once the process's metrics
have caught up (they show the state of about a second ago), so a cell carries the GETs, requests and bytes of its
cold query and the mean of its warm ones.
An invalid answer (error status, unparseable, empty, flapping) is kept in the row with its reason and never
counts as a latency.

Windows are the layers of stack.py make_state; a range ends at the layer's end, `full` is the whole layer.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import abreg  # noqa: E402,F401  (puts the repository root on sys.path)
import validate  # noqa: E402
from scripts.proof.runner import client  # noqa: E402
from scripts.proof.runner.rows import tenant_headers  # noqa: E402

TARGETS = ("ref", "base", "pr")
PEER_KEY = "proof-peer-key"
TIMEOUT_S = 90.0
FMT = "%Y-%m-%dT%H:%M:%SZ"
LOGS_PATH = "/select/logsql/query"
S3_KEYS = ("gets", "requests", "bytes")


class CellError(ValueError):
    pass


def parse_ts(s: str) -> dt.datetime:
    return dt.datetime.strptime(s, FMT).replace(tzinfo=dt.timezone.utc)


SHIFT_S = 300  # a re-measure moves every window this much earlier, so it does not time the caches the first pass filled


def window_for(state: dict, layer: str, rng: str, ranges: dict, shift_s: int = 0) -> tuple[str, str]:
    """(start, end) of a range in a layer; the range ends where the layer ends (`shift_s` earlier for a re-measure) and never starts
    before it. A shift that would leave less than a tenth of the window is ignored."""
    win = state["layers"][layer]
    end = parse_ts(win["end"])
    start = parse_ts(win["start"])
    if shift_s and (end - start).total_seconds() > shift_s * 10:
        end -= dt.timedelta(seconds=shift_s)
    secs = ranges.get(rng)
    if secs is not None:
        start = max(start, end - dt.timedelta(seconds=secs))
    return start.strftime(FMT), end.strftime(FMT)


def micros(ts: str) -> str:
    return str(int(parse_ts(ts).timestamp() * 1_000_000))


def millis(ts: str) -> str:
    return str(int(parse_ts(ts).timestamp() * 1000))


def window_params(mode: str, start: str, end: str) -> dict:
    if mode == "none":
        return {}
    if mode == "seconds":  # Tempo's start and end are unix seconds (RFC3339 is a 400)
        return {"start": str(int(parse_ts(start).timestamp())), "end": str(int(parse_ts(end).timestamp()))}
    if mode == "micros":
        return {"start": micros(start), "end": micros(end)}
    if mode == "end-ms":
        return {"endTs": millis(end)}
    return {"start": start, "end": end}


def base_url(state: dict, target: str, signal: str) -> str:
    return f"http://127.0.0.1:{state['ports'][f'{target}-{signal}']}"


def build_request(shape: dict, state: dict, target: str, start: str, end: str, form: str, picks: dict | None = None) -> dict:
    """One concrete request of a shape: method, URL, params and tenant headers for one target."""
    picks = picks or {}
    api = shape.get("api", "logsql")
    method = shape.get("method") or ("POST" if api == "logsql" else "GET")
    path = shape.get("path") or LOGS_PATH
    query = shape.get("query")
    if shape.get("pick") and shape["pick"] not in picks:
        raise CellError(f"shape {shape['name']!r} needs the pick {shape['pick']!r}")
    path = path.format(**picks) if "{" in path else path
    params = dict(shape.get("params") or {})
    if query is not None:
        params["query"] = query.format(**picks) if "{" in query else query
    params.update(window_params(shape.get("window", "start-end"), start, end))
    if shape["signal"] == "traces" and api == "logsql":
        params["disable_latency_offset"] = "true"
    return {"method": method, "url": base_url(state, target, shape["signal"]) + path, "params": params,
            "headers": tenant_headers(form, target)}


def send(req: dict) -> dict:
    return client.send(req["method"], req["url"], req["params"], req["headers"], timeout=TIMEOUT_S)


# ------------------------------------------------------------------------------------------------ metrics
def parse_metrics(text: str) -> dict:
    """The few samples the harness needs from a Lakehouse /metrics page."""
    out = {"gets": 0.0, "requests": 0.0, "bytes": 0.0, "range_reads": 0.0, "cpu_s": None, "rss": None}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        name, _, rest = line.partition(" ")
        try:
            val = float(rest.split()[-1])
        except (ValueError, IndexError):
            continue
        if name.startswith("lakehouse_s3_requests_total{"):
            op = name.split('op="', 1)[1].split('"', 1)[0] if 'op="' in name else ""
            if op in ("GET", "GetObject"):
                out["gets"] += val
            if op not in ("PUT", "PutObject", "DeleteObject", "CopyObject"):
                out["requests"] += val
        elif name == "lakehouse_s3_bytes_read_total":
            out["bytes"] = val
        elif name == "lakehouse_s3_range_reads_total":
            out["range_reads"] = val
        elif name == "process_cpu_seconds_total":
            out["cpu_s"] = val
        elif name == "process_resident_memory_bytes":
            out["rss"] = val
    return out


def scrape(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url + "/metrics", timeout=10) as r:  # noqa: S310 - loopback test stack
            return parse_metrics(r.read().decode("utf-8", "replace"))
    except (urllib.error.URLError, OSError):
        return None


def s3_delta(before: dict | None, after: dict | None) -> dict | None:
    if before is None or after is None:
        return None
    return {k: max(0.0, after[k] - before[k]) for k in S3_KEYS}


def clear_cache(url: str) -> bool:
    """Empty the Lakehouse caches (peer key required). True only when the call answered 2xx."""
    req = urllib.request.Request(url + "/internal/cache/clear", method="POST", headers={"Authorization": f"Bearer {PEER_KEY}"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:  # noqa: S310 - loopback test stack
            return 200 <= r.status < 300
    except (urllib.error.URLError, OSError):
        return False


# ------------------------------------------------------------------------------------------------ cells
def cell_key(set_name: str, shape: str, layer: str, rng: str, form: str) -> str:
    return "|".join((set_name, shape, layer, rng, form))


def plan(spec: dict, set_names: list[str], only: dict[str, list[str]] | None = None) -> list[dict]:
    """The cells of the chosen sets. `only` restricts a set to shape names; a cell outside the layers' ranges is skipped."""
    cells = []
    for sname in set_names:
        if sname not in spec["sets"]:
            raise CellError(f"unknown shape set {sname!r}")
        sset = spec["sets"][sname]
        for shape in sset["shapes"]:
            if only and sname in only and shape["name"] not in only[sname]:
                continue
            for layer in shape.get("layers") or sset.get("layers") or ["cold"]:
                if layer not in spec["layers"]:
                    raise CellError(f"{sname}/{shape['name']}: unknown layer {layer!r}")
                for rng in shape.get("ranges") or sset.get("ranges") or ["1h"]:
                    if rng not in spec["ranges"]:
                        raise CellError(f"{sname}/{shape['name']}: unknown range {rng!r}")
                    form = shape.get("tenant", "numeric")
                    cells.append({"set": sname, "shape": shape, "layer": layer, "range": rng, "form": form,
                                  "key": cell_key(sname, shape["name"], layer, rng, form)})
    return cells


def resolve_pick(state: dict, cell: dict, cache: dict, start: str, end: str) -> dict:
    """A value taken once from the reference for the cell's window and tenant form (a trace id)."""
    name = cell["shape"].get("pick")
    if not name:
        return {}
    key = (name, start, end, cell["form"])
    if key not in cache:
        if name != "trace_id":
            raise CellError(f"unknown pick {name!r}")
        env = client.send("POST", base_url(state, "ref", "traces") + LOGS_PATH,
                          {"query": "span_id:* | sort by (trace_id) | fields trace_id | limit 1", "start": start, "end": end,
                           "disable_latency_offset": "true"}, tenant_headers(cell["form"], "ref"), timeout=TIMEOUT_S)
        try:
            cache[key] = json.loads(env["body"].strip().splitlines()[0])["trace_id"]
        except (ValueError, KeyError, IndexError):
            raise CellError(f"no trace id for {key} on the reference: {env['status']} {env['body'][:100]}")
    return {name: cache[key]}


def strip_limit(query: str) -> str:
    return query.rsplit(" | limit ", 1)[0] if " | limit " in query else query


def scan_window(shape: dict, state: dict, target: str, start: str, end: str, form: str, picks: dict) -> tuple[int | None, set | None]:
    """The untimed reference of a truncated scan: the same query without its limit (row count and key set)."""
    unlimited = dict(shape, query=strip_limit(shape["query"]))
    env = send(build_request(unlimited, state, target, start, end, form, picks))
    if env["status"] != 200:
        return None, None
    got = validate.scan_keys(env["body"])
    return (None, None) if got is None else (got[0], set(got[1]))


SETTLE_S = 1.1  # /metrics shows the state of about a second ago (measured: a counter turns up 1.00-1.04 s after the request)


class Settle:
    """Per-process timestamp of the last request, so a scrape waits until its counters have caught up."""

    def __init__(self):
        self.last: dict[str, float] = {}

    def touch(self, key: str) -> None:
        self.last[key] = time.time()

    def scrape(self, key: str, url: str, sleep=time.sleep, clock=time.time) -> dict | None:
        wait = self.last.get(key, 0.0) + SETTLE_S - clock()
        if wait > 0:
            sleep(wait)
        return scrape(url)


def s3_per_query(delta: dict | None, n: int) -> dict | None:
    if delta is None or n <= 0:
        return None
    return {k: round(delta[k] / n, 2) for k in S3_KEYS}


def cell_ref_needs_retry(rows: list[dict]) -> bool:
    """Hot has no valid answer for the cell for a reason other than an empty one (an empty answer is a shape the data does not feed)."""
    ref = [r for r in rows if r["target"] == "ref"]
    return bool(ref) and not any(r["valid"] for r in ref) and any(r["reason"] != "empty result" for r in ref)


def run_cell(cell: dict, state: dict, spec: dict, runs: int, picks_cache: dict, settle: Settle | None = None, targets=TARGETS,
             lh_cold_clear: bool = True, shift_s: int = 0) -> tuple[list[dict], dict]:
    """All requests of one cell: `runs` per target, the first cold. Returns (raw rows, S3 accounting).

    The S3 counters are read per phase, not per request: once before the cold run, once after it, once after
    the warm runs, each only after the process's metrics caught up (SETTLE_S). The cold figure is the delta of
    the cold run; the warm figure is the mean per warm request.
    """
    settle = settle or Settle()
    shape = cell["shape"]
    start, end = window_for(state, cell["layer"], cell["range"], spec["ranges"], shift_s)
    picks = resolve_pick(state, cell, picks_cache, start, end)
    kind = shape["kind"]
    lh_targets = [t for t in targets if t != "ref"]
    keys = {t: f"{t}-{shape['signal']}" for t in lh_targets}
    urls = {t: base_url(state, t, shape["signal"]) for t in lh_targets}
    windows = {}
    if kind == "scan":
        for t in targets:
            windows[t] = scan_window(shape, state, t, start, end, cell["form"], picks)
            if t != "ref":
                settle.touch(keys[t])
    first = {t: None for t in targets}
    rows = []
    cleared = {t: (clear_cache(urls[t]) if lh_cold_clear else None) for t in lh_targets}
    for t in lh_targets:
        settle.touch(keys[t])
    snap0 = {t: settle.scrape(keys[t], urls[t]) for t in lh_targets}

    def one(run: int, t: str) -> dict:
        req = build_request(shape, state, t, start, end, cell["form"], picks)
        env = send(req)
        if t != "ref":
            settle.touch(keys[t])
        wr, wk = windows.get(t, (None, None))
        sample = bool(shape.get("sample"))  # a sampled answer differs between calls: it is validated but never held to the first result
        ok, reason, result = validate.validate(kind, env["status"], env["body"], first_result=None if sample else first[t], miss=False,
                                               window_rows=wr, window_keys=wk)
        truncated = kind == "scan" and wr is not None and wr > validate.SCAN_LIMIT
        if ok and truncated:
            # a truncated scan keeps an arbitrary subset: what two systems must agree on is the window it was cut from
            result = f"window rows={wr};hash={validate.sha(sorted(wk))}"
        elif ok and first[t] is None:
            first[t] = result
        row = {"cell": cell["key"], "set": cell["set"], "shape": shape["name"], "signal": shape["signal"], "layer": cell["layer"],
               "range": cell["range"], "form": cell["form"], "target": t, "run": run, "cold": run == 0, "status": env["status"],
               "ms": round(env["latency_ms"], 2), "valid": ok, "reason": reason, "result": result,
               "bytes_in": len(env["body"]), "window": [start, end]}
        if run == 0 and t != "ref":
            row["cold_cleared"] = cleared[t]
        if env.get("error_kind"):
            row["error_kind"] = env["error_kind"]
        return row

    def phase(run_range) -> None:
        for run in run_range:
            order = list(targets)
            shift = run % len(order)
            for t in order[shift:] + order[:shift]:
                rows.append(one(run, t))

    phase(range(0, 1))
    snap1 = {t: settle.scrape(keys[t], urls[t]) for t in lh_targets}
    phase(range(1, runs))
    snap2 = {t: settle.scrape(keys[t], urls[t]) for t in lh_targets}
    if "ref" in targets and cell_ref_needs_retry(rows):
        # hot gave nothing valid (a transient error): ask it once more before the cell is reported as having no reference
        rows[:] = [r for r in rows if r["target"] != "ref"]
        first["ref"] = None
        for run in range(runs):
            row = one(run, "ref")
            row["retried"] = True
            rows.append(row)
    s3 = {t: {"cold": s3_delta(snap0[t], snap1[t]), "warm_per_query": s3_per_query(s3_delta(snap1[t], snap2[t]), runs - 1)} for t in lh_targets}
    return rows, {"s3": s3, "snapshots": snap2}


def lh_urls(state: dict) -> dict[str, str]:
    return {f"{t}-{s}": base_url(state, t, s) for t in ("base", "pr") for s in ("logs", "traces")}


def run_matrix(state: dict, spec: dict, cells: list[dict], runs: int, log=lambda m: None, shift_s: int = 0) -> dict:
    """Run the cells; returns {"rows", "s3": {cell: {target: ...}}, "process": {target: cpu/rss}, "elapsed_s"}."""
    t0 = time.time()
    urls = lh_urls(state)
    settle = Settle()
    before = {k: scrape(u) for k, u in urls.items()}
    peak_rss = {k: (v or {}).get("rss") or 0.0 for k, v in before.items()}
    rows, s3, picks_cache = [], {}, {}
    for i, cell in enumerate(cells):
        r, acct = run_cell(cell, state, spec, runs, picks_cache, settle, shift_s=shift_s)
        rows += r
        s3[cell["key"]] = acct["s3"]
        for t, snap in acct["snapshots"].items():
            k = f"{t}-{cell['shape']['signal']}"
            if snap and snap["rss"]:
                peak_rss[k] = max(peak_rss[k], snap["rss"])
        log(f"cell {i + 1}/{len(cells)} {cell['key']}")
    after = {k: scrape(u) for k, u in urls.items()}
    for k, m in after.items():
        if m and m["rss"]:
            peak_rss[k] = max(peak_rss[k], m["rss"])
    process = {k: {"cpu_s": None if not (before[k] and after[k]) or before[k]["cpu_s"] is None else round(after[k]["cpu_s"] - before[k]["cpu_s"], 2),
                   "peak_rss_mb": round(peak_rss[k] / 1e6, 1)} for k in urls}
    return {"rows": rows, "s3": s3, "process": process, "elapsed_s": round(time.time() - t0, 1)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", required=True)
    ap.add_argument("--set", action="append", required=True)
    ap.add_argument("--shape", action="append", default=[], help="restrict to shape names (exact), repeatable")
    ap.add_argument("--runs", type=int, default=6, help="per target and cell; the first is cold")
    ap.add_argument("--shapes-file", default=abreg.SHAPES)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    state = abreg.load_json(a.state)
    spec = abreg.load_json(a.shapes_file)
    only = {s: a.shape for s in a.set} if a.shape else None
    cells = plan(spec, a.set, only)
    res = run_matrix(state, spec, cells, a.runs, log=lambda m: print(m, file=sys.stderr, flush=True))
    res.update(runs=a.runs, sets=a.set, state={k: state[k] for k in ("H", "layers", "project")})
    abreg.dump_json(a.out, res)
    bad = sum(1 for r in res["rows"] if not r["valid"])
    print(f"{len(res['rows'])} requests in {res['elapsed_s']}s, {bad} invalid", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
