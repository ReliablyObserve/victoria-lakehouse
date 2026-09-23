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
  path       'catalog' (pmeta catalog answered), 'index' (answered from RAM
             without the catalog and without S3: the sampled label index),
             'scan' (projected row scan), 'mixed' when iterations differ.
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
            return "catalog"
        return "index" if r.get("gets", 1) == 0 else "scan"  # RAM answer without the catalog = label index
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


ROUTES = {"fv_level": ("/select/logsql/field_values", "level"),
          "fv_service": ("/select/logsql/field_values", "service.name"),
          "field_names": ("/select/logsql/field_names", None),
          "streams": ("/select/logsql/streams", None),
          "traces.fv_name": ("/select/logsql/field_values", "name"),
          "traces.fv_service": ("/select/logsql/field_values", "resource_attr:service.name"),
          "traces.streams": ("/select/logsql/streams", None)}


def emit_rows(groups):
    """Proposed conformance perf rows (not wired: the registry schema has no
    perf block yet). Budgets are the 'after' build's measured p50/p90 over exact
    iterations; counters are the deterministic per-request S3 economics."""
    for cell in sorted(c for c in groups if "after" in groups[c]):
        a = summarize(groups[cell]["after"])
        r0 = groups[cell]["after"][0]
        ep = r0["endpoint"]
        route, field = ROUTES[ep]
        traces = ep.startswith("traces.")
        rid = ("vt" if traces else "vl") + ".perf." + cell.replace("traces.", "").replace("/", ".") \
            .replace("=", "_").replace("pmeta_true", "pmeta_on").replace("pmeta_false", "pmeta_off")
        params = {"query": "*" if r0["filter"] == "none" else 'service.name:="svc-a"'}
        if field:
            params["field"] = field
        exact = a["exact"] == a["n"]
        sig = "vt" if traces else "vl"
        head = (f"- {{ id: {rid.lower()}, surface: {sig}, kind: select, origin: native, targets: [cold], "
                f"seed: [{'traces' if traces else 'logs'}.fieldmeta], layers: [perf], pending: true,")
        if exact:
            head += " expect: pass,"
        else:
            why = "hits=1 from a RAM index" if a["set"] == a["n"] else "value set is not the window's"
            head += f" expect: differ, differ_note: \"{a['exact']}/{a['n']} exact at v0.143.1: {why}\","
        print(head)
        print(f"    upstream: {{ route: {route} }}, request: {{ method: GET, path: {route}, params: {json.dumps(params)} }},")
        print("    compare: { type: values-with-hits, options: { hits_tolerance: \"0\" } }, refs: { doc: docs/perf/field-metadata-cells.md },")
        perf = [f"cell: \"{cell}\""]
        if exact:
            perf.append(f"budget: {{ p50_ms: {a['p50'] / 1e6:.3f}, p90_ms: {a['p90'] / 1e6:.3f}, valid: \"{a['exact']}/{a['n']}\" }}")
        counters = [f"s3_gets: {a['gets']:.0f}", f"s3_bytes: {a['bytes']:.0f}"]
        if a["rgs"] is not None:
            counters += [f"row_groups: {a['rgs']:.0f}", f"pages: {a['pages']:.0f}"]
        counters.append(f"path: {a['path']}")
        perf.append("counters: { " + ", ".join(counters) + " }")
        print("    perf: { " + ", ".join(perf) + " } }")


def main():
    path = sys.argv[1]
    groups = defaultdict(lambda: defaultdict(list))
    for line in open(path):
        r = json.loads(line)
        groups[r["cell"]][r["build"]].append(r)
    if len(sys.argv) > 2 and sys.argv[2] == "--rows":
        emit_rows(groups)
        return

    def order(cell):
        ep, *rest = cell.split("/")
        eps = ["fv_level", "fv_service", "field_names", "streams",
               "traces.fv_name", "traces.fv_service", "traces.streams"]
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
    print("† no iteration had exact hits (catalog and label-index answers carry hits=1); bracketed timing is over set-exact iterations only.")
    for w in warnings:
        print(f"WARNING: {w}")


if __name__ == "__main__":
    main()
