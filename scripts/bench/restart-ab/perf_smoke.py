#!/usr/bin/env python3
"""A/B perf + correctness smoke for #272: main build vs fix build vs hot reference, interleaved per query.
usage: perf_smoke.py <repeats> <range_seconds> [out.json]"""
import json, sys, time, hashlib, statistics, urllib.request, urllib.parse
REPS, RANGE = int(sys.argv[1]), int(sys.argv[2])
OUT = sys.argv[3] if len(sys.argv) > 3 else "/tmp/fix272-vis/perf-raw.json"
cfg = {n: json.load(open(f"/tmp/fix272-vis/{p}.json")) for n, p in (("main", "fix272-vis-main"), ("fix", "fix272-vis-fix"))}
SYS = {"main": cfg["main"]["urls"], "fix": cfg["fix"]["urls"], "hot": cfg["fix"]["urls"]}
LOGS = {"main": "lh_logs", "fix": "lh_logs", "hot": "hot_logs"}
TRACES = {"main": "lh_traces", "fix": "lh_traces", "hot": "hot_traces"}
HDR = {"AccountID": "0", "ProjectID": "0"}
now = int(time.time())
S, E = now - RANGE, now + 3600


def get(url, timeout=60):
    req = urllib.request.Request(url, headers=HDR)
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            b = r.read()
            st = r.status
    except urllib.error.HTTPError as e:
        b, st = e.read(), e.code
    except Exception as e:
        b, st = str(e).encode(), 0
    return st, b.decode(errors="replace"), (time.perf_counter() - t) * 1000


def metric_sum(base, name):
    st, b, _ = get(base + "/metrics")
    tot = 0.0
    for l in b.splitlines():
        if l.startswith(name) and not l.startswith("#"):
            try:
                tot += float(l.rsplit(" ", 1)[1])
            except Exception:
                pass
    return tot


def s3_counters(base):
    return {"gets": metric_sum(base, "lakehouse_s3_gets_by_phase_total") + metric_sum(base, "lakehouse_s3_range_reads_total"),
            "bytes": metric_sum(base, "lakehouse_s3_bytes_read_total") + metric_sum(base, "lakehouse_s3_range_bytes_read_total")}


def norm_lines(body):
    msgs = []
    for l in body.splitlines():
        if l.strip():
            try:
                r = json.loads(l)
            except Exception:
                continue
            msgs.append(r.get("_msg", ""))
    return {"n": len(msgs), "hash": hashlib.sha1("\n".join(sorted(msgs)).encode()).hexdigest()[:10],
            "layers": {k: sum(1 for m in msgs if f"repro {k} " in m) for k in ("cold", "buffer")}}


def norm_stats(body):
    vals = []
    for l in body.splitlines():
        if l.strip():
            try:
                vals.append(json.loads(l))
            except Exception:
                pass
    return {"n": sum(int(v.get("n", 0)) for v in vals if "n" in v), "rows": len(vals),
            "hash": hashlib.sha1(json.dumps(sorted(json.dumps(v, sort_keys=True) for v in vals)).encode()).hexdigest()[:10]}


def norm_names(body):
    try:
        v = json.loads(body)["values"]
    except Exception:
        return {"n": -1}
    names = sorted(x["value"] for x in v)
    return {"n": len(names), "hash": hashlib.sha1(",".join(names).encode()).hexdigest()[:10]}


def norm_values(body):
    try:
        v = json.loads(body)["values"]
    except Exception:
        return {"n": -1}
    return {"n": sum(x["hits"] for x in v), "vals": {x["value"]: x["hits"] for x in v}}


def norm_hits(body):
    try:
        h = json.loads(body)["hits"]
    except Exception:
        return {"n": -1}
    return {"n": sum(sum(x["values"]) for x in h)}


def norm_jaeger_search(body):
    try:
        d = json.loads(body)["data"]
    except Exception:
        return {"n": -1}
    return {"n": len(d), "spans": sum(len(t["spans"]) for t in d), "hash": hashlib.sha1(",".join(sorted(t["traceID"] for t in d)).encode()).hexdigest()[:10]}


def norm_jaeger_trace(body):
    try:
        d = json.loads(body)["data"]
    except Exception:
        return {"n": -1}
    return {"n": len(d), "spans": sum(len(t["spans"]) for t in d)}


def norm_tempo_search(body):
    try:
        d = json.loads(body)["traces"]
    except Exception:
        return {"n": -1}
    return {"n": len(d), "hash": hashlib.sha1(",".join(sorted(t["traceID"] for t in d)).encode()).hexdigest()[:10]}


def norm_tempo_trace(body):
    try:
        d = json.loads(body)
        spans = 0
        for k in ("batches", "resourceSpans"):
            for b in d.get(k, []):
                for ss in b.get("scopeSpans", b.get("instrumentationLibrarySpans", [])):
                    spans += len(ss.get("spans", []))
        return {"n": spans}
    except Exception:
        return {"n": -1}


def tid(layer, i):
    return (layer[0] * 2 + f"{i:02d}").ljust(32, "a")


q = urllib.parse.quote
Q = {}


def lq(query):
    return lambda b: f"{b}/select/logsql/query?query={q(query)}&start={S}&end={E}&limit=1000"


Q["logs query=*"] = ("logs", lq("*"), norm_lines)
Q["logs filtered repro_layer:buffer"] = ("logs", lq("repro_layer:buffer"), norm_lines)
Q["logs stats count()"] = ("logs", lq("* | stats count() n"), norm_stats)
Q["logs stats by (repro_layer)"] = ("logs", lq("* | stats by (repro_layer) count() n"), norm_stats)
Q["logs field_names"] = ("logs", lambda b: f"{b}/select/logsql/field_names?query={q('*')}&start={S}&end={E}", norm_names)
Q["logs field_values repro_layer"] = ("logs", lambda b: f"{b}/select/logsql/field_values?query={q('*')}&field=repro_layer&start={S}&end={E}", norm_values)
Q["logs hits"] = ("logs", lambda b: f"{b}/select/logsql/hits?query={q('*')}&start={S}&end={E}&step=5m", norm_hits)
Q["traces logsql query=*"] = ("traces", lambda b: f"{b}/select/logsql/query?query={q('*')}&start={S}&end={E}&limit=1000", lambda body: {"n": sum(1 for l in body.splitlines() if l.strip())})
Q["traces logsql stats count()"] = ("traces", lq("* | stats count() n"), norm_stats)
Q["traces field_values name"] = ("traces", lambda b: f"{b}/select/logsql/field_values?query={q('*')}&field=name&start={S}&end={E}", norm_values)
Q["jaeger search service=repro"] = ("traces", lambda b: f"{b}/select/jaeger/api/traces?service=repro&start={S*1000000}&end={E*1000000}&limit=100", norm_jaeger_search)
Q["jaeger trace by ID (buffer)"] = ("traces", lambda b: f"{b}/select/jaeger/api/traces/{tid('buffer', 0)}", norm_jaeger_trace)
Q["jaeger trace by ID (cold)"] = ("traces", lambda b: f"{b}/select/jaeger/api/traces/{tid('cold', 0)}", norm_jaeger_trace)
Q["tempo search {}"] = ("traces", lambda b: f"{b}/select/tempo/api/search?q={q('{}')}&start={S}&end={E}&limit=100", norm_tempo_search)
Q["tempo trace by ID (buffer)"] = ("traces", lambda b: f"{b}/select/tempo/api/traces/{tid('buffer', 0)}", norm_tempo_trace)

res = {}
order = ["main", "fix", "hot"]
for name, (sig, urlf, norm) in Q.items():
    runs = {s: [] for s in order}
    ans = {s: None for s in order}
    s3 = {s: [] for s in order}
    for rep in range(REPS + 1):  # rep 0 = cold first run, reported separately
        for s in order:
            base = SYS[s][(LOGS if sig == "logs" else TRACES)[s]]
            before = s3_counters(base) if s != "hot" else None
            st, body, ms = get(urlf(base))
            after = s3_counters(base) if s != "hot" else None
            a = norm(body) if st == 200 else {"status": st, "body": body[:80]}
            runs[s].append(ms)
            if rep == 0:
                ans[s] = a
            elif a != ans[s]:
                ans[s] = {"UNSTABLE": [ans[s], a]}
            if s != "hot":
                s3[s].append({k: after[k] - before[k] for k in after})
    res[name] = {s: {"first_ms": runs[s][0], "warm": runs[s][1:], "answer": ans[s],
                     "s3_first": s3[s][0] if s3[s] else None, "s3_warm": s3[s][1:] if s3[s] else None} for s in order}
json.dump({"range_s": RANGE, "reps": REPS, "results": res}, open(OUT, "w"), indent=1)


def p(v, pc):
    v = sorted(v)
    return v[min(len(v) - 1, int(round(pc * (len(v) - 1))))]


print(f"range={RANGE}s reps={REPS}")
for name, r in res.items():
    line = [name.ljust(34)]
    for s in order:
        w = r[s]["warm"]
        line.append(f"{s}: first={r[s]['first_ms']:.0f} p50={statistics.median(w):.0f} p95={p(w, 0.95):.0f}ms")
    print(" | ".join(line))
