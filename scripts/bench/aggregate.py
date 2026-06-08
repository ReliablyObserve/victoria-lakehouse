#!/usr/bin/env python3
"""Aggregate N independent benchmark run JSONs into one report that shows the
MEDIAN p95 across runs AND the run-to-run spread (min–max + coefficient of
variation), so single-run noise is exposed rather than presented as precision.
A cell is only shown if it is valid (result-equivalent) in EVERY run.

Usage: aggregate.py <out.md> <run1.json> <run2.json> [run3.json ...]
"""
import json
import statistics
import sys
from collections import defaultdict

BASE = {"logs": "victorialogs", "traces": "victoriatraces"}
TOL = 0.05


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def valid(row, base_res):
    if not row or row.get("iters", 0) == 0 or (row.get("avg_bytes", 0) or 0) == 0:
        return False
    r, b = as_int(row.get("result")), as_int(base_res)
    if r is not None and b is not None:
        if b == 0:
            return r == 0
        return abs(r - b) / b <= TOL
    return True


def main():
    out, runs = sys.argv[1], sys.argv[2:]
    # cell[(sig,q,rng,lat,sys)] = [p95 per run]
    cell = defaultdict(list)
    for rf in runs:
        rows = json.load(open(rf))
        g = defaultdict(dict)
        for r in rows:
            g[(r["signal"], r["query"], r["range"], r["latency_ms"])][r["system"]] = r
        for key, sysd in g.items():
            base_res = sysd.get(BASE[key[0]], {}).get("result")
            for s, row in sysd.items():
                if valid(row, base_res) and isinstance(row.get("p95_ms"), (int, float)):
                    cell[(*key, s)].append(row["p95_ms"])

    lines = [
        f"# Benchmark — aggregated over {len(runs)} runs (median p95 + run-to-run spread)",
        "",
        "Each cell: **median p95** across runs, with `[min–max]` and CV "
        "(coefficient of variation = stdev/mean). High CV = noisy/unstable cell. "
        "Only cells valid (result-equivalent) in **every** run are shown.",
        "",
        "| signal | query | range | lat | system | median p95 | min–max | CV |",
        "|---|---|---|---:|---|---:|---|---:|",
    ]
    rk = {"5m": 0, "15m": 1, "1h": 2, "6h": 3, "24h": 4, "7d": 5}
    for key in sorted(cell, key=lambda k: (k[0], k[1], rk.get(k[2], 9), k[3], k[4])):
        xs = cell[key]
        if len(xs) < len(runs):  # not valid in every run
            continue
        sig, q, rng, lat, s = key
        m = statistics.median(xs)
        cv = (statistics.pstdev(xs) / statistics.mean(xs)) if len(xs) > 1 and statistics.mean(xs) else 0
        flag = " ⚠️" if cv >= 0.3 else ""
        lines.append(f"| {sig} | {q} | {rng} | {lat}ms | {s} | {m:.1f} | "
                     f"{min(xs):.1f}–{max(xs):.1f} | {cv*100:.0f}%{flag} |")

    # summary: how noisy overall
    cvs = []
    for key, xs in cell.items():
        if len(xs) == len(runs) and len(xs) > 1 and statistics.mean(xs):
            cvs.append(statistics.pstdev(xs) / statistics.mean(xs))
    if cvs:
        lines[1] = (f"_{len(runs)} runs · median cell CV {statistics.median(cvs)*100:.0f}% · "
                    f"{sum(1 for c in cvs if c>=0.3)}/{len(cvs)} cells with CV≥30% (noisy)._")
    open(out, "w").write("\n".join(lines) + "\n")
    print(f"aggregated {len(runs)} runs -> {out}; median cell CV "
          f"{statistics.median(cvs)*100:.0f}%" if cvs else "no data")


if __name__ == "__main__":
    main()
