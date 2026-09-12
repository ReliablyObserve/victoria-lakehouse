#!/usr/bin/env python3
"""Render the unified benchmark JSON into a markdown report — SPLIT by logs vs
traces, each with its own summary, plus an overall roll-up.

Response validation (Task 6): a cell is only meaningful when EVERY timed
iteration returned a correct, valid response — a fast wrong/empty/error
answer is a broken response, never a latency sample. `run.sh` now validates
each iteration itself (HTTP 2xx, parseable, non-empty unless the query is a
documented miss scenario, stable/non-flapping) and excludes invalid
iterations from p50/p95/p99, recording `iters_valid`/`iters_invalid`/
`invalid_reasons` per cell. This report adds the CROSS-SYSTEM check on top:
a cell is `✗` when `iters_invalid > 0` (own iterations were bad), when its
`result` differs from the baseline system's result beyond the count
tolerance, or — for `scan` results, which carry a row-set content hash —
when that hash differs from the baseline's while the row COUNT is equal
("same count, different rows"). ClickHouse's `scan` is a different
projection (no comparable row key), so CH scan cells are compared by row
count only, never by hash.

Usage: report.py <raw.json> <out.md>
"""
import json
import re
import statistics
import sys
from collections import defaultdict

BASELINE = {"logs": "victorialogs", "traces": "victoriatraces"}
ENGINES = ["lakehouse", "clickhouse"]
TOL = 0.05
# Query kinds whose CORRECT answer can legitimately be empty/zero (a
# cross-signal lookup that doesn't correlate) — must match run.sh's
# MISS_QUERIES so a documented miss isn't flagged as a broken/empty baseline.
MISS_QUERIES = {"trace_lookup"}


def num(v):
    return v if isinstance(v, (int, float)) else None


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


_ROWS_RE = re.compile(r'^rows=(\d+)(?:;hash=([0-9a-f]+))?$')
_SPANS_RE = re.compile(r'^spans=(\d+)$')


def parse_result(v):
    """Parse a run.sh `result` value into {"count": int, "hash": str|None}.

    Formats: plain integer (scalar count/filter/group-by), "rows=N" or
    "rows=N;hash=H" (scan — hash only on non-CH engines, whose scan carries a
    comparable row-set key), "spans=N" (trace_by_id/trace_lookup). Returns
    None if unparseable.
    """
    if v is None:
        return None
    m = _ROWS_RE.match(v)
    if m:
        return {"count": int(m.group(1)), "hash": m.group(2)}
    m = _SPANS_RE.match(v)
    if m:
        return {"count": int(m.group(1)), "hash": None}
    n = as_int(v)
    if n is not None:
        return {"count": n, "hash": None}
    return None


def valid_str(row):
    """'k/N' validity string for one system's cell (falls back to the older
    iters/errors fields for pre-Task-6 JSON so old raw results still render)."""
    if not row:
        return "0/0"
    v, iv = row.get("iters_valid"), row.get("iters_invalid")
    if v is None:
        v = row.get("iters", 0) or 0
        iv = row.get("errors", 0) or 0
    return f"{v or 0}/{(v or 0) + (iv or 0)}"


def invalid_note(row):
    v, iv = row.get("iters_valid"), row.get("iters_invalid")
    if v is None:
        v = row.get("iters", 0) or 0
        iv = row.get("errors", 0) or 0
    reasons = row.get("invalid_reasons") or "unknown"
    return f"{iv or 0}/{(v or 0) + (iv or 0)} invalid: {reasons}"


def cell_status(row, brow):
    """A system's cell is invalid when: it's missing; its OWN iterations
    weren't all valid (`iters_invalid > 0`); its result diverges from the
    baseline's beyond the count tolerance; or (only when both sides carry a
    row-set hash, i.e. a non-CH `scan`) the row counts match but the hash
    doesn't — same count, different rows."""
    if row is None:
        return False, "missing"
    iv = row.get("iters_invalid")
    if iv is not None:
        if iv > 0:
            return False, invalid_note(row)
    else:
        # pre-Task-6 JSON: fall back to the old errored/empty checks.
        if row.get("iters", 0) == 0:
            return False, "errored"
        if (row.get("avg_bytes", 0) or 0) == 0:
            return False, "empty (0 bytes)"
    rp = parse_result(row.get("result"))
    bp = parse_result((brow or {}).get("result"))
    if rp is not None and bp is not None:
        r, b = rp["count"], bp["count"]
        if b == 0:
            if r != 0:
                return False, f"result {r} vs base 0"
        elif abs(r - b) / b > TOL:
            return False, f"result {r} vs base {b}"
        elif r == b and rp["hash"] is not None and bp["hash"] is not None and rp["hash"] != bp["hash"]:
            return False, f"same count ({r}), different rows (hash mismatch)"
    return True, ""


def base_status(brow, query):
    """The baseline (VL/VT) cell itself must be a real, valid result — a
    baseline that errored/flapped/returned 0 (unless `query` is a documented
    miss scenario) makes every ratio in the row meaningless, so every engine
    cell in that row is invalid (reason "baseline-empty"/its own invalid
    note), not just checked against a bogus baseline."""
    if not brow:
        return False, "baseline-empty (missing)"
    biv = brow.get("iters_invalid")
    if biv is not None and biv > 0:
        return False, f"baseline-{invalid_note(brow)}"
    bp = parse_result(brow.get("result"))
    if bp is None:
        return False, "baseline-empty (result=0/empty)"
    if bp["count"] == 0 and query not in MISS_QUERIES:
        return False, "baseline-empty (result=0/empty)"
    if (brow.get("avg_bytes", 0) or 0) == 0 and query not in MISS_QUERIES:
        return False, "baseline-empty (avg_bytes=0)"
    return True, ""


def shape_flag(row, brow):
    """⚠ shape: an engine's avg_bytes differs from the baseline's by more than
    10x either way — same row count, wildly different payload shape."""
    eb = (row.get("avg_bytes", 0) or 0) if row else 0
    bb = (brow.get("avg_bytes", 0) or 0) if brow else 0
    if eb and bb and (eb / bb >= 10 or bb / eb >= 10):
        return " ⚠ shape"
    return ""


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
    disk_profile = next((r["disk_profile"] for r in rows if r.get("disk_profile")), "unspecified")
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
            "| query | range | S3 lat | baseline p95 [res] | valid | LH | valid | CH | valid |",
            "|---|---|---:|---:|---:|---|---:|---|---:|",
        ]
        for key in sorted(k for k in g if k[0] == signal):
            _, query, rng, lat = key
            sysd = g[key]
            brow = sysd.get(base_sys, {})
            bp = num(brow.get("p95_ms"))
            bres = brow.get("result")
            cells = [f"{bp} [{bres}]"]
            valids = [valid_str(brow)]
            b_ok, b_note = base_status(brow, query)
            for eng in ENGINES:
                row = sysd.get(eng)
                if not b_ok:
                    ok, note = False, b_note
                else:
                    ok, note = cell_status(row, brow)
                p = num(row.get("p95_ms")) if row else None
                valids.append(valid_str(row))
                if not ok:
                    cells.append(f"✗ {note}")
                    invalid.append((signal, query, rng, lat, eng, note))
                    if eng == "lakehouse":
                        n_invalid += 1
                else:
                    cells.append(f"{p} ({ratio_str(p, bp)}) [{row.get('result')}]{shape_flag(row, brow)}")
                    if eng == "lakehouse" and p and bp:
                        n_valid += 1
                        lh_ratios.append(p / bp)
                        by_query[query].append(p / bp)
                    if eng == "clickhouse" and p:
                        lhp = num(sysd.get("lakehouse", {}).get("p95_ms"))
                        if lhp:
                            ch_speedups.append(p / lhp)
            table.append(
                f"| {query} | {rng} | {lat}ms | {cells[0]} | {valids[0]} | "
                f"{cells[1]} | {valids[1]} | {cells[2]} | {valids[2]} |"
            )
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
                 f"Baseline = VL/VT on disk (disk profile: {disk_profile}); "
                 f"LH + ClickHouse read the same S3 Parquet. Medians below use only "
                 f"fully valid cells (every system's iterations 20/20, results agreeing).")
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
