#!/usr/bin/env python3
"""Render the unified benchmark JSON into a markdown table normalized to the
VL/VT disk baseline. One row per (signal, query, range, S3-latency); columns show
the baseline p95 and each engine's p95 with its ratio-to-baseline, so it's obvious
at a glance where Lakehouse lags the hot baseline and how it compares to ClickHouse
over the same S3 Parquet.

Usage: report.py <raw.json> <out.md>
"""
import json
import sys
from collections import defaultdict

BASELINE = {"logs": "victorialogs", "traces": "victoriatraces"}
ENGINES = ["lakehouse", "clickhouse"]  # shown relative to the baseline


def p95(row):
    v = row.get("p95_ms")
    return v if isinstance(v, (int, float)) else None


def ratio(v, base):
    if v is None or base in (None, 0):
        return "—"
    r = v / base
    flag = " ⚠️" if r >= 3 else (" 🔴" if r >= 10 else "")
    return f"{r:.1f}×{flag}"


def main():
    raw, out = sys.argv[1], sys.argv[2]
    rows = json.load(open(raw))
    # group[(signal,query,range,lat)][system] = row
    g = defaultdict(dict)
    for r in rows:
        g[(r["signal"], r["query"], r["range"], r["latency_ms"])][r["system"]] = r

    lines = [
        "# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse",
        "",
        "p95 latency (ms). **Baseline = VL/VT on disk** (gp3-simulated when run with "
        "`--disk-profile gp3-loop`). `×N` = slower than baseline; ⚠️ ≥3×, 🔴 ≥10×. "
        "LH and ClickHouse read the **same Parquet** on S3 behind the latency proxy.",
        "",
        "| signal | query | range | S3 lat | baseline p95 | LH p95 (×base) | CH p95 (×base) |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for key in sorted(g):
        signal, query, rng, lat = key
        systems = g[key]
        base_sys = BASELINE.get(signal)
        base = p95(systems.get(base_sys, {}))
        cells = [f"{base} " if base is not None else "— "]
        for eng in ENGINES:
            v = p95(systems.get(eng, {}))
            cells.append(f"{v if v is not None else '—'} ({ratio(v, base)})")
        lines.append(
            f"| {signal} | {query} | {rng} | {lat}ms | "
            f"{cells[0]}| {cells[1]} | {cells[2]} |"
        )

    # Headline callouts: worst LH-vs-baseline cells.
    worst = []
    for key, systems in g.items():
        base = p95(systems.get(BASELINE.get(key[0]), {}))
        lh = p95(systems.get("lakehouse", {}))
        if base and lh:
            worst.append((lh / base, key, lh, base))
    worst.sort(reverse=True)
    if worst:
        lines += ["", "## Where LH lags the baseline most", ""]
        for r, key, lh, base in worst[:8]:
            lines.append(f"- **{r:.1f}×** — {'/'.join(map(str, key))}: LH {lh}ms vs baseline {base}ms")

    open(out, "w").write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
