#!/usr/bin/env python3
"""Render the unified benchmark JSON into a markdown report — SPLIT by logs vs
traces, each with its own summary, plus an overall roll-up. Every latency is
validated against an equivalent, non-empty result; cells that errored, came back
empty, or diverge >5% from the baseline are flagged ✗ and excluded from the stats.

Usage: report.py <raw.json> <out.md>
"""
import json
import statistics
import sys
from collections import defaultdict

BASELINE = {"logs": "victorialogs", "traces": "victoriatraces"}
ENGINES = ["lakehouse", "clickhouse"]
TOL = 0.05


def num(v):
    return v if isinstance(v, (int, float)) else None


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def cell_status(row, base_result):
    if row is None:
        return False, "missing"
    if row.get("iters", 0) == 0:
        return False, "errored"
    if (row.get("avg_bytes", 0) or 0) == 0:
        return False, "empty (0 bytes)"
    r, b = as_int(row.get("result")), as_int(base_result)
    if r is not None and b is not None:
        # Strict: a 0-vs-nonzero result is a divergence too (don't let base==0
        # silently pass mismatched cells — that hid a broken traces-scan compare).
        if b == 0:
            if r != 0:
                return False, f"result {r} vs base 0"
        elif abs(r - b) / b > TOL:
            return False, f"result {r} vs base {b}"
    return True, ""


def ratio_str(v, base):
    if v is None or base in (None, 0):
        return "—"
    r = v / base
    flag = " 🔴" if r >= 10 else (" ⚠️" if r >= 3 else "")
    return f"{r:.1f}×{flag}"


def med(xs):
    return statistics.median(xs) if xs else None


def pct(xs, p):
    if not xs:
        return None
    s = sorted(xs)
    return s[min(int(len(s) * p / 100), len(s) - 1)]


def main():
    raw, out = sys.argv[1], sys.argv[2]
    with open(raw) as f:
        rows = json.load(f)
    g = defaultdict(dict)
    for r in rows:
        g[(r["signal"], r["query"], r["range"], r["latency_ms"])][r["system"]] = r

    signals = sorted({k[0] for k in g})
    invalid = []
    lines = ["# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse", ""]

    # ---- gather per-signal stats + rendered tables -------------------------
    overall = {}
    sections = {}
    for signal in signals:
        base_sys = BASELINE.get(signal)
        lh_ratios, ch_speedups, by_query = [], [], defaultdict(list)
        n_valid = n_invalid = 0
        table = [
            "| query | range | S3 lat | baseline p95 [res] | LH | CH |",
            "|---|---|---:|---:|---|---|",
        ]
        for key in sorted(k for k in g if k[0] == signal):
            _, query, rng, lat = key
            sysd = g[key]
            brow = sysd.get(base_sys, {})
            bp = num(brow.get("p95_ms"))
            bres = brow.get("result")
            cells = [f"{bp} [{bres}]"]
            for eng in ENGINES:
                row = sysd.get(eng)
                ok, note = cell_status(row, bres)
                p = num(row.get("p95_ms")) if row else None
                if not ok:
                    cells.append(f"✗ {note}")
                    invalid.append((signal, query, rng, lat, eng, note))
                    if eng == "lakehouse":
                        n_invalid += 1
                else:
                    cells.append(f"{p} ({ratio_str(p, bp)}) [{row.get('result')}]")
                    if eng == "lakehouse" and p and bp:
                        n_valid += 1
                        lh_ratios.append(p / bp)
                        by_query[query].append(p / bp)
                    if eng == "clickhouse" and p:
                        lhp = num(sysd.get("lakehouse", {}).get("p95_ms"))
                        if lhp:
                            ch_speedups.append(p / lhp)
            table.append(f"| {query} | {rng} | {lat}ms | {cells[0]} | {cells[1]} | {cells[2]} |")
        sections[signal] = table
        overall[signal] = dict(
            n_valid=n_valid, n_invalid=n_invalid,
            lh_med=med(lh_ratios), lh_p90=pct(lh_ratios, 90), lh_best=min(lh_ratios) if lh_ratios else None,
            ch_speedup=med(ch_speedups),
            by_query={q: med(v) for q, v in by_query.items()},
        )

    # ---- overall roll-up ---------------------------------------------------
    lines += ["## Overall", ""]
    tot_v = sum(o["n_valid"] for o in overall.values())
    tot_i = sum(o["n_invalid"] for o in overall.values())
    lines.append(f"- **{tot_v} valid LH cells, {tot_i} invalid** (excluded). "
                 f"Baseline = VL/VT on disk (gp3-simulated); LH + ClickHouse read the same S3 Parquet.")
    for signal in signals:
        o = overall[signal]
        lh = f"median **{o['lh_med']:.1f}×** baseline (p90 {o['lh_p90']:.1f}×, best {o['lh_best']:.1f}×)" if o["lh_med"] else "—"
        ch = f"**{o['ch_speedup']:.0f}× faster than ClickHouse**" if o["ch_speedup"] else "—"
        lines.append(f"- **{signal}**: LH {lh}; LH is {ch}. ({o['n_valid']} valid / {o['n_invalid']} invalid)")

    # ---- per-signal sections ----------------------------------------------
    for signal in signals:
        o = overall[signal]
        lines += ["", f"## {signal.capitalize()}", ""]
        if o["by_query"]:
            lines.append("**Per-query median LH vs baseline:** " +
                         ", ".join(f"{q} {r:.1f}×" for q, r in sorted(o["by_query"].items())))
            lines.append("")
        lines += sections[signal]

    if invalid:
        lines += ["", "## ⚠️ Invalid cells (excluded — not comparable)", ""]
        for signal, query, rng, lat, eng, note in invalid:
            lines.append(f"- {eng} — {signal}/{query}/{rng}/lat{lat}ms: **{note}**")

    with open(out, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
