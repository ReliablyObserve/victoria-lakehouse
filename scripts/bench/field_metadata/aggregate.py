#!/usr/bin/env python3
"""Aggregate field-metadata matrix JSONL (TestFieldMetadataMatrix[Traces]) into
a before/after markdown table.

Validity rule (same as scripts/bench/report.py): a response that is not exact is
never timed. Per cell and build:
  set k/N    iterations whose value (or name) set equals the truth;
  exact k/N  iterations whose set AND per-value hit counts equal the truth;
  p50/p90    over EXACT iterations only. When a cell has no exact iteration but
             has set-exact ones (the pmeta catalog answers hits=1), the set-exact
             p50 is shown in brackets and marked, never as a clean number;
  GETs/bytes/RGs/pages  median per iteration (deterministic in this harness);
  path       'ram' (the whole answer came from metadata in RAM, no S3 read:
             the pmeta catalog before #239, per-object label counts after),
             'index' (RAM without that metadata: the sampled label index),
             'scan' (at least one object read), 'mixed' when iterations differ.

Registry rows for these cells are generated and checked by perf_rows.py.
"""
import json
import statistics
import sys
from collections import defaultdict


def pct(vals, p):
    if not vals:
        return None
    s = sorted(vals)
    k = (len(s) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def fmt_ms(ns):
    if ns is None:
        return "—"
    ms = ns / 1e6
    if ms < 1:
        return f"{ms:.3f}"
    if ms < 100:
        return f"{ms:.1f}"
    return f"{ms:.0f}"


def fmt_bytes(b):
    if b is None:
        return "—"
    if b >= 1 << 20:
        return f"{b / (1 << 20):.2f}M"
    if b >= 1 << 10:
        return f"{b / (1 << 10):.0f}K"
    return str(int(b))


def summarize(recs):
    n = len(recs)
    set_ok = [r for r in recs if r["set_ok"]]
    exact = [r for r in recs if r["hits_ok"]]
    med = lambda k: statistics.median([r[k] for r in recs]) if recs and k in recs[0] else None  # noqa: E731
    def src(r):
        if r.get("catalog_answers", 0) > 0:
            return "ram"
        return "index" if r.get("gets", 1) == 0 else "scan"  # RAM answer without that metadata = label index
    srcs = {src(r) for r in recs}
    path = srcs.pop() if len(srcs) == 1 else "mixed"
    timed = [r["ns"] for r in exact]
    bracket = False
    if not timed and set_ok:
        timed, bracket = [r["ns"] for r in set_ok], True
    return {
        "n": n, "set": len(set_ok), "exact": len(exact),
        "p50": pct(timed, 50), "p90": pct(timed, 90), "bracket": bracket,
        "gets": med("gets"), "bytes": med("bytes"), "rgs": med("row_groups"), "pages": med("pages"),
        "path": path, "truth": {r["truth"] for r in recs if "truth" in r},
        "values": statistics.median([r["values"] for r in recs]) if recs else None,
    }


def cell_str(s):
    if s is None:
        return ["—"] * 6
    p50 = fmt_ms(s["p50"])
    p90 = fmt_ms(s["p90"])
    if s["bracket"]:
        p50, p90 = f"[{p50}]†", f"[{p90}]†"
    elif s["exact"] == 0:
        p50 = p90 = "invalid"
    valid = f"{s['exact']}/{s['n']}" + ("" if s["set"] == s["exact"] else f" (set {s['set']})")
    return [p50, p90, valid, f"{s['gets']:.0f}" if s["gets"] is not None else "—", fmt_bytes(s["bytes"]), s["path"]]


def delta(b, a):
    if a is None or b is None:
        return "—"
    if a["exact"] == 0 and not a["bracket"]:
        return "after invalid"
    if b["exact"] == 0 and not b["bracket"]:
        return "before invalid (fast-wrong) → now exact" if a["exact"] else "before invalid"
    if not b["p50"] or not a["p50"]:
        return "—"
    r = f"{a['p50'] / b['p50']:.2f}×"
    return f"[{r}]†" if (a["bracket"] or b["bracket"]) else r


def main():
    path = sys.argv[1]
    groups = defaultdict(lambda: defaultdict(list))
    for line in open(path):
        r = json.loads(line)
        groups[r["cell"]][r["build"]].append(r)

    def order(cell):
        ep, *rest = cell.split("/")
        eps = ["fv_level", "fv_service", "field_names", "streams",
               "traces.fv_name", "traces.fv_service", "traces.streams", "traces.field_names"]
        return (eps.index(ep) if ep in eps else 99, rest)

    vl_cells = sorted(c for c in groups if "vl" in groups[c])
    if vl_cells:
        print("Hot VictoriaLogs (in-process upstream storage, same rows, local disk):")
        print()
        print("| cell | p50 ms | p90 ms | exact k/N |")
        print("|---|---|---|---|")
        for cell in vl_cells:
            s = summarize(groups[cell]["vl"])
            print(f"| {cell} | {fmt_ms(s['p50'])} | {fmt_ms(s['p90'])} | {s['exact']}/{s['n']} |")
        print()

    print("| cell | before p50 ms | p90 | exact k/N | GETs | S3 bytes | path | after p50 ms | p90 | exact k/N | GETs | S3 bytes | path | RGs/pages after | Δ p50 |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    warnings = []
    for cell in sorted((c for c in groups if "vl" not in groups[c]), key=order):
        b = summarize(groups[cell]["before"]) if groups[cell].get("before") else None
        a = summarize(groups[cell]["after"]) if groups[cell].get("after") else None
        if a and b and a["truth"] and b["truth"] and a["truth"] != b["truth"]:
            warnings.append(f"truth differs between builds for {cell}")
        rp = "—"
        if a and a["rgs"] is not None:
            rp = f"{a['rgs']:.0f}/{a['pages']:.0f}"
        print("| " + " | ".join([cell] + cell_str(b) + cell_str(a) + [rp, delta(b, a)]) + " |")
    print()
    print("† no iteration had exact hits (catalog and label-index answers carried hits=1); bracketed timing is over set-exact iterations only.")
    for w in warnings:
        print(f"WARNING: {w}")


if __name__ == "__main__":
    main()
