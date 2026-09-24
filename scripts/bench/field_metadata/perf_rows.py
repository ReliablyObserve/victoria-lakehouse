#!/usr/bin/env python3
"""Field-metadata perf cells <-> conformance registry rows.

  perf_rows.py gen   MATRIX.jsonl [--build after] [--at LABEL] > rows.yaml
      Writes one registry row per measured cell of BUILD (logs and traces):
      expect pass with a latency budget (p50/p90 over exact iterations) when
      every iteration was exact, expect differ otherwise; deterministic
      counters (S3 GETs and bytes, row groups, pages, answering path).

  perf_rows.py check MATRIX.jsonl ROWS.yaml [--build after]
      The CI gate. MATRIX is a fresh run of the harness (any S3 latency: the
      counters do not depend on it). Fails when
        - a harness cell has no row, or a row names a cell the harness lacks;
        - a pass row's cell is not exact in every iteration;
        - a differ row's cell became exact (promote it: regenerate the rows);
        - a counter grew past the row's (GETs, bytes, row groups, pages) or
          the answering path changed;
        - a compacted cell is not exact where the same flushed cell is.
      Latency budgets are not gated here: shared CI runners are not the
      machine the budgets were measured on. They are held by the per-PR
      matrix and three-way runs recorded in docs/perf.
"""
import argparse
import json
import os
import re
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aggregate import summarize  # noqa: E402

ENDPOINTS = {
    # cell endpoint: (surface, title, route, field)
    "fv_level": ("vl", "field_values level", "/select/logsql/field_values", "level"),
    "fv_service": ("vl", "field_values service.name (busy field)", "/select/logsql/field_values", "service.name"),
    "field_names": ("vl", "field_names", "/select/logsql/field_names", None),
    "streams": ("vl", "streams", "/select/logsql/streams", None),
    "traces.fv_name": ("vt", "traces field_values name", "/select/logsql/field_values", "name"),
    "traces.fv_service": ("vt", "traces field_values service.name", "/select/logsql/field_values", "resource_attr:service.name"),
    "traces.streams": ("vt", "traces streams", "/select/logsql/streams", None),
    "traces.field_names": ("vt", "traces field_names", "/select/logsql/field_names", None),
}
WINDOWS = {"whole": "whole hours", "cut": "cut window", "narrow": "narrow 30 s window", "edge": "hour-edge window"}
FILTERS = {"none": ("unfiltered", "*"), "svc": ("filter svc", 'service.name:="svc-a"')}
LAYOUTS = {"flushed": "flushed objects", "compacted": "compacted objects",
           "peer": "flushed objects + a peer's unflushed rows"}
COUNTERS = ["s3_gets", "s3_bytes", "row_groups", "pages"]
CELL_RE = re.compile(r"^(?P<ep>[a-z_.]+)/pmeta=(?P<pmeta>true|false)/layout=(?P<layout>\w+)/window=(?P<window>\w+)/filter=(?P<filter>\w+)/s3=(?P<lat>\d+)ms$")


def parse_cell(cell):
    m = CELL_RE.match(cell)
    if not m:
        raise ValueError(f"not a field-metadata cell: {cell}")
    return m.groupdict()


def load_matrix(path, build):
    groups = defaultdict(list)
    for line in open(path):
        r = json.loads(line)
        if r.get("build") == build:
            groups[r["cell"]].append(r)
    return groups


def counters_of(recs):
    """The worst iteration's counters. They are deterministic per cell except
    where concurrent reads race a cache fill (a filtered scan of a compacted
    object issues a varying number of 4 KiB range reads, and more of them at
    0 ms than at 100 ms), so the row records, and the gate compares, the
    maximum."""
    s = summarize(recs)
    c = {"s3_gets": max(r["gets"] for r in recs), "s3_bytes": max(r["bytes"] for r in recs)}
    if "row_groups" in recs[0]:
        c["row_groups"] = max(r["row_groups"] for r in recs)
        c["pages"] = max(r["pages"] for r in recs)
    c["path"] = s["path"]
    return c


def q(v):
    return json.dumps(v, ensure_ascii=True)


def row_for(cell, recs, at, shape_recs=None):
    """shape_recs: every record of the cell's shape across S3 latencies — the
    counters are held per shape (the gate compares a run at any latency), so
    the row records the worst of all of them."""
    s = summarize(recs)
    p = parse_cell(cell)
    surface, title, route, field = ENDPOINTS[p["ep"]]
    fdesc, query = FILTERS[p["filter"]]
    rid = (f"{surface}.perf.{p['ep'].replace('traces.', '')}.pmeta_{'on' if p['pmeta'] == 'true' else 'off'}"
           f".layout_{p['layout']}.window_{p['window']}.filter_{p['filter']}.s3_{p['lat']}ms")
    full_title = (f"{title} — pmeta {'on' if p['pmeta'] == 'true' else 'off'}, {LAYOUTS[p['layout']]}, "
                  f"{WINDOWS[p['window']]}, {fdesc}, S3 +{p['lat']}ms")
    exact = s["exact"] == s["n"]
    params = {"query": query}
    if field:
        params["field"] = field
    parts = [f"id: {q(rid)}", f"title: {q(full_title)}", f"surface: {q(surface)}", 'kind: "select"',
             'origin: "native"', f"expect: {q('pass' if exact else 'differ')}"]
    if not exact:
        why = "hits are not exact" if s["set"] == s["n"] else "value set is not the window's"
        note = "%d/%d exact at %s: %s" % (s["exact"], s["n"], at, why)
        parts.append(f"differ_note: {q(note)}")
    params_s = ", ".join(f"{k}: {q(v)}" for k, v in params.items())
    parts += ['targets: ["cold"]', f"seed: [{q(('traces' if surface == 'vt' else 'logs') + '.fieldmeta')}]",
              f"upstream: {{ route: {q(route)} }}",
              f"request: {{ method: \"GET\", path: {q(route)}, params: {{ {params_s} }} }}",
              'compare: { type: "values-with-hits", options: { hits_tolerance: "0" } }',
              'layers: ["perf"]', "pending: true", 'refs: { doc: "docs/perf/field-metadata-cells.md" }']
    perf = [f"cell: {q(cell)}"]
    if exact:
        perf.append(f"budget: {{ p50_ms: {s['p50'] / 1e6:.3f}, p90_ms: {max(s['p90'], s['p50']) / 1e6:.3f}, valid: \"{s['exact']}/{s['n']}\" }}")
    c = counters_of(shape_recs or recs)
    c["path"] = s["path"]
    perf.append("counters: { " + ", ".join(f"{k}: {q(v) if k == 'path' else v}" for k, v in c.items()) + " }")
    parts.append("perf: { " + ", ".join(perf) + " }")
    return "- { " + ", ".join(parts) + " }"


HEADER = """# Field-metadata performance cells: cold field_values / field_names / streams on both
# binaries, measured by internal/storage/parquets3/field_values_bench_test.go (and its traces
# twin) with every answer validated against the generator's truth, on the same rows flushed
# as small objects and compacted into hour objects. Generated by
# scripts/bench/field_metadata/perf_rows.py gen from the matrix in docs/perf/field-metadata-cells.md;
# a cell that is not exact is expect: differ and gets no budget. CI (field-metadata-perf)
# re-runs the harness and holds exactness and the deterministic counters (S3 GETs and bytes,
# row groups, pages, answering path) with perf_rows.py check.
"""


def cmd_gen(args):
    groups = load_matrix(args.matrix, args.build)
    if not groups:
        sys.exit(f"no records of build {args.build!r} in {args.matrix}")
    shapes = defaultdict(list)
    for cell, recs in groups.items():
        shapes[strip_latency(cell)].extend(recs)
    lines = [row_for(cell, groups[cell], args.at, shapes[strip_latency(cell)]) for cell in groups]
    lines.sort(key=lambda l: re.search(r'id: "([^"]+)"', l).group(1))
    sys.stdout.write(HEADER + "\n" + "\n".join(lines) + "\n")


def load_rows(path):
    """The rows file is one flow mapping per line; the fields the gate needs are
    read with a regex so the gate has no YAML dependency."""
    rows = {}
    for line in open(path):
        if not line.startswith("- {"):
            continue
        cell = re.search(r'cell: "([^"]+)"', line).group(1)
        expect = re.search(r'expect: "(\w+)"', line).group(1)
        c = {k: int(v) for k, v in re.findall(r"\b(s3_gets|s3_bytes|row_groups|pages): (\d+)", line)}
        c["path"] = re.search(r'path: "(\w+)"', line).group(1)
        rows[cell] = {"expect": expect, "counters": c}
    return rows


def strip_latency(cell):
    return re.sub(r"/s3=\d+ms$", "", cell)


def check(matrix_groups, rows):
    """Returns the list of gate failures (empty = pass)."""
    fails = []
    by_shape = defaultdict(list)
    for cell, recs in matrix_groups.items():
        by_shape[strip_latency(cell)].append(recs)
    row_shapes = {strip_latency(c) for c in rows}
    for shape in sorted(set(by_shape) - row_shapes):
        fails.append(f"{shape}: harness cell has no registry row")
    for shape in sorted(row_shapes - set(by_shape)):
        fails.append(f"{shape}: registry row names a cell the harness does not measure")
    for cell, row in sorted(rows.items()):
        runs = by_shape.get(strip_latency(cell))
        if not runs:
            continue
        ss = [summarize(recs) for recs in runs]
        exact = all(s["exact"] == s["n"] for s in ss)
        if row["expect"] == "pass" and not exact:
            got = ", ".join("%d/%d" % (s["exact"], s["n"]) for s in ss)
            diffs = sorted({r["diff"] for recs in runs for r in recs if r.get("diff")})
            why = f" — got/want: {'; '.join(diffs)[:300]}" if diffs else ""
            fails.append(f"{cell}: expect pass but not exact ({got}){why}")
        if row["expect"] == "differ" and exact:
            fails.append(f"{cell}: expect differ but now exact — regenerate the rows to promote it")
        for recs in runs:
            got = counters_of(recs)
            for k in COUNTERS:
                if k in row["counters"] and k in got and got[k] > row["counters"][k]:
                    fails.append(f"{cell}: {k} {got[k]} > registry {row['counters'][k]}")
            if got["path"] != row["counters"]["path"]:
                fails.append(f"{cell}: path {got['path']} != registry {row['counters']['path']}")
    # Compaction must not cost exactness: the same request on the compacted
    # layout is exact wherever it is exact on the flushed one.
    def all_exact(runs):
        return all(s["exact"] == s["n"] for s in (summarize(r) for r in runs))

    for shape, runs in sorted(by_shape.items()):
        if "/layout=flushed/" not in shape:
            continue
        twin = by_shape.get(shape.replace("/layout=flushed/", "/layout=compacted/"))
        if twin is None:
            continue
        if all_exact(runs) and not all_exact(twin):
            fails.append(f"{shape.replace('/layout=flushed/', '/layout=compacted/')}: not exact after compaction")
    return fails


def cmd_check(args):
    groups = load_matrix(args.matrix, args.build)
    if not groups:
        sys.exit(f"no records of build {args.build!r} in {args.matrix}")
    fails = check(groups, load_rows(args.rows))
    for f in fails:
        print(f"FAIL {f}")
    print(f"{len(groups)} cells measured, {len(fails)} failures")
    sys.exit(1 if fails else 0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gen")
    g.add_argument("matrix")
    g.add_argument("--build", default="after")
    g.add_argument("--at", default="this build", help="label in differ notes, e.g. a PR or version")
    g.set_defaults(fn=cmd_gen)
    c = sub.add_parser("check")
    c.add_argument("matrix")
    c.add_argument("rows")
    c.add_argument("--build", default="after")
    c.set_defaults(fn=cmd_check)
    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
