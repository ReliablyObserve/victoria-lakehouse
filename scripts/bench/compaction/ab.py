#!/usr/bin/env python3
"""Interleaved main/PR query A/B for the compaction run (stdlib only).

For every (signal, tenant, window, shape) the same request goes to the main
and the PR instance, REPS times, alternating which one goes first. Rep 1 is
reported as the cold run; the p50 of the rest is the warm number. A shape
counts only when BOTH answers are HTTP 200 and their canonical hashes are equal
(the sorted lines of the answer, parsed as JSON when they are JSON): a
different or failed answer is flagged and never enters a latency.

  ab.py [--out DIR] [--reps 6]

Reads DIR/ingest_start and DIR/runs.tsv written by run.sh. Windows are the
aligned hours 72 h and 30 h before the ingest start (where tenants 1001 and
1002 wrote), plus the 2 h one where tenant 1003 wrote.
"""
import argparse, hashlib, json, os, statistics, sys, time, urllib.error, urllib.parse, urllib.request

PORT = {("logs", "main"): 39701, ("logs", "pr"): 39702, ("traces", "main"): 39703, ("traces", "pr"): 39704}
TENANTS = ["1001", "1003"]


def windows(start):
    base = start - start % 3600
    return {f"{h}h": (base - h * 3600, base - h * 3600 + 3600) for h in (72, 30, 2)}


def shapes(sig, run):
    """name -> (path, params). Time range is added per window."""
    if sig == "logs":
        return {
            "stats count": ("/select/logsql/query", {"query": "* | stats count() c"}),
            "stats by _stream": ("/select/logsql/query", {"query": "* | stats by (_stream) count() c"}),
            "filtered run count": ("/select/logsql/query", {"query": '{run="%s"} | stats count() c' % run}),
            "raw _msg rows": ("/select/logsql/query", {"query": '{run="%s"} | fields _msg' % run}),
            "field_names": ("/select/logsql/field_names", {"query": "*"}),
            "field_values": ("/select/logsql/field_values", {"query": "*", "field": "service.name"}),
            "hits 10m": ("/select/logsql/hits", {"query": "*", "step": "10m"}),
        }
    return {
        "stats count": ("/select/logsql/query", {"query": "* | stats count() c"}),
        "stats by service": ("/select/logsql/query", {"query": '* | stats by ("resource_attr:service.name") count() c'}),
        "raw run rows": ("/select/logsql/query", {"query": '"span_attr:run":"%s" | fields "span_attr:seq", trace_id' % run}),
        "field_names": ("/select/logsql/field_names", {"query": "*"}),
        "jaeger services": ("/select/jaeger/api/services", {}),
    }


def call(url, tenant):
    req = urllib.request.Request(url, headers={"AccountID": tenant})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            body, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        body, status = e.read(), e.code
    except Exception as e:  # connection refused, timeout
        body, status = str(e).encode(), 0
    return status, time.perf_counter() - t0, body


def canon(body):
    lines = []
    for ln in body.decode("utf-8", "replace").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            ln = json.dumps(json.loads(ln), sort_keys=True)
        except ValueError:
            pass
        lines.append(ln)
    return hashlib.sha256("\n".join(sorted(lines)).encode()).hexdigest()[:16], len(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "out"))
    ap.add_argument("--reps", type=int, default=6)
    a = ap.parse_args()
    start = int(open(os.path.join(a.out, "ingest_start")).read())
    runs = {}
    for ln in open(os.path.join(a.out, "runs.tsv")):
        sig, tenant, run, rnd = ln.rstrip("\n").split("\t")
        runs.setdefault((sig, tenant), run)  # first run of the tenant
    results, bad = [], 0
    for sig in ("logs", "traces"):
        for tenant in TENANTS:
            run = runs.get((sig, tenant), "none")
            for wname, (ws, we) in windows(start).items():
                for sname, (path, params) in shapes(sig, run).items():
                    q = dict(params)
                    if "jaeger" not in sname:
                        q.update({"start": str(ws), "end": str(we), "disable_latency_offset": "true"})
                    qs = urllib.parse.urlencode(q)
                    times = {"main": [], "pr": []}
                    answers = {"main": [], "pr": []}
                    for rep in range(a.reps):
                        order = ("main", "pr") if rep % 2 == 0 else ("pr", "main")
                        for build in order:
                            status, el, body = call(f"http://127.0.0.1:{PORT[(sig, build)]}{path}?{qs}", tenant)
                            times[build].append(el)
                            answers[build].append((status, canon(body) if status == 200 else ("-", 0)))
                    ok = all(s == 200 for b in answers.values() for s, _ in b)
                    same = len({h for b in answers.values() for _, (h, _) in b}) == 1
                    rows = answers["main"][0][1][1]
                    verdict = "ok" if ok and same else ("non-200" if not ok else "ANSWERS DIFFER")
                    bad += verdict != "ok"
                    row = {"signal": sig, "tenant": tenant, "window": wname, "shape": sname, "verdict": verdict, "lines": rows}
                    for b in ("main", "pr"):
                        row[f"{b}_cold_ms"] = round(times[b][0] * 1000, 1)
                        row[f"{b}_p50_ms"] = round(statistics.median(times[b][1:]) * 1000, 1) if len(times[b]) > 1 else None
                    results.append(row)
                    print(json.dumps(row), flush=True)
    with open(os.path.join(a.out, "ab.json"), "w") as f:
        json.dump(results, f, indent=1)
    print("\n| signal | tenant | window | shape | lines | main cold | PR cold | main p50 | PR p50 | verdict |\n|---|---|---|---|---|---|---|---|---|---|")
    for r in results:
        print(f"| {r['signal']} | {r['tenant']} | {r['window']} | {r['shape']} | {r['lines']} | {r['main_cold_ms']} | {r['pr_cold_ms']} | {r['main_p50_ms']} | {r['pr_p50_ms']} | {r['verdict']} |")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
