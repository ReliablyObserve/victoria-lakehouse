#!/usr/bin/env python3
"""Render the unified benchmark JSON into a markdown report normalized to the
VL/VT disk baseline — and, crucially, VALIDATE that each latency number is over an
equivalent, non-empty result. A fast p95 over an empty or divergent result is not
a win, so every cell is checked for: errors, zero bytes (empty response), and
result-equivalence with the baseline. Cells that fail validation are flagged and
excluded from the "where LH lags" ranking.

Usage: report.py <raw.json> <out.md>
"""
import json
import sys
from collections import defaultdict

BASELINE = {"logs": "victorialogs", "traces": "victoriatraces"}
ENGINES = ["lakehouse", "clickhouse"]
TOL = 0.05  # result within 5% of baseline counts as equivalent


def num(v):
    return v if isinstance(v, (int, float)) else None


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def cell_status(row, base_result):
    """Return (ok, note) for one system's cell."""
    if row is None:
        return False, "missing"
    if row.get("errors", 0) > 0 and (row.get("iters", 0) == 0):
        return False, "errored"
    if (row.get("avg_bytes", 0) or 0) == 0:
        return False, "empty (0 bytes)"
    r = as_int(row.get("result"))
    b = as_int(base_result)
    if r is not None and b not in (None, 0):
        if abs(r - b) / b > TOL:
            return False, f"result {r} vs base {b}"
    return True, ""


def ratio(v, base):
    if v is None or base in (None, 0):
        return "—"
    r = v / base
    flag = " 🔴" if r >= 10 else (" ⚠️" if r >= 3 else "")
    return f"{r:.1f}×{flag}"


def main():
    raw, out = sys.argv[1], sys.argv[2]
    rows = json.load(open(raw))
    g = defaultdict(dict)
    for r in rows:
        g[(r["signal"], r["query"], r["range"], r["latency_ms"])][r["system"]] = r

    lines = [
        "# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse",
        "",
        "p95 latency (ms), with **result-validity checks**. Baseline = VL/VT on disk "
        "(gp3-simulated under `--disk-profile gp3-loop`). LH and ClickHouse read the "
        "**same Parquet** on S3 behind the latency proxy. Each cell shows `p95 (×base) "
        "[result]`; **✗ = invalid** (errored / 0 bytes / result diverges >5% from "
        "baseline — latency NOT comparable).",
        "",
        "| signal | query | range | S3 lat | baseline p95 [res] | LH | CH |",
        "|---|---|---|---:|---:|---|---|",
    ]
    valid_worst = []
    invalid = []
    for key in sorted(g):
        signal, query, rng, lat = key
        systems = g[key]
        base_sys = BASELINE.get(signal)
        base_row = systems.get(base_sys, {})
        base_p95 = num(base_row.get("p95_ms"))
        base_res = base_row.get("result")
        b_ok, b_note = cell_status(base_row, base_res)
        base_cell = f"{base_p95} [{base_res}]" + ("" if b_ok else f" ✗{(' '+b_note) if b_note else ''}")
        eng_cells = []
        for eng in ENGINES:
            row = systems.get(eng)
            ok, note = cell_status(row, base_res)
            p = num(row.get("p95_ms")) if row else None
            if not ok:
                eng_cells.append(f"✗ {note}")
                invalid.append((signal, query, rng, lat, eng, note))
            else:
                eng_cells.append(f"{p} ({ratio(p, base_p95)}) [{row.get('result')}]")
                if eng == "lakehouse" and p and base_p95:
                    valid_worst.append((p / base_p95, key, p, base_p95))
        lines.append(f"| {signal} | {query} | {rng} | {lat}ms | {base_cell} | {eng_cells[0]} | {eng_cells[1]} |")

    if valid_worst:
        valid_worst.sort(reverse=True)
        lines += ["", "## Where LH lags the baseline most (valid cells only)", ""]
        for r, key, lh, base in valid_worst[:8]:
            lines.append(f"- **{r:.1f}×** — {'/'.join(map(str, key))}: LH {lh}ms vs baseline {base}ms")

    if invalid:
        lines += ["", "## ⚠️ Invalid cells (NOT comparable — fix before trusting)", ""]
        for signal, query, rng, lat, eng, note in invalid:
            lines.append(f"- {eng} — {signal}/{query}/{rng}/lat{lat}ms: **{note}**")

    open(out, "w").write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
