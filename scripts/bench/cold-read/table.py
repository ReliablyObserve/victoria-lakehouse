#!/usr/bin/env python3
"""Render the cold-read matrix (run.sh output) as markdown: base vs PR per shape and latency.

Reads <out>/<signal>-<build>-f<files>-L<layout>-<lat>ms.jsonl written by TestColdReadProfile and prints,
per signal / layout / file count, one table of p50 ms, GETs, MB read and sequential round trips for
base -> pr, plus an ANSWERS line: any shape whose answer differs between the builds, or fails its
ground-truth check, is listed (and the exit status is 1).
"""
import glob, json, os, re, sys
from collections import defaultdict

out = sys.argv[1]
rows = defaultdict(dict)  # (sig, files, layout, shape) -> {(build, lat): row}
pat = re.compile(r'^(logs|traces)-(\w+)-f(\w*)-L(\w+)-(\d+)ms\.jsonl$')
for fn in sorted(glob.glob(os.path.join(out, '*.jsonl'))):
    m = pat.match(os.path.basename(fn))
    if not m:
        continue
    sig, build, files, layout, lat = m.group(1), m.group(2), m.group(3), m.group(4), int(m.group(5))
    for line in open(fn):
        r = json.loads(line)
        rows[(sig, files, layout, r['Shape'])][(build, lat)] = r

def fmt(v):
    return f"{v:,.0f}" if v >= 10 else f"{v:.1f}"

bad = []
groups = defaultdict(list)
for (sig, files, layout, shape), cells in rows.items():
    groups[(sig, files, layout)].append((shape, cells))
for (sig, files, layout), shapes in sorted(groups.items()):
    lats = sorted({lat for _, c in shapes for (_, lat) in c})
    title = f"{sig}, layout {layout}" + ("" if files in ('', 'def') else f", {files} files")
    print(f"\n### {title}: p50 ms base -> PR (GETs, MB read, sequential round trips)\n")
    print("| shape | " + " | ".join(f"{l} ms" for l in lats) + " |")
    print("|---|" + "--:|" * len(lats))
    for shape, cells in sorted(shapes):
        cols = []
        for lat in lats:
            b, p = cells.get(('base', lat)), cells.get(('pr', lat))
            if not b or not p:
                cols.append("—")
                continue
            cols.append(f"{fmt(b['P50Ms'])} -> {fmt(p['P50Ms'])} ({b['Gets']}->{p['Gets']}, "
                        f"{b['Bytes']/1e6:.1f}->{p['Bytes']/1e6:.2f} MB, {b['Chain']}->{p['Chain']})")
            if b['Answer'] != p['Answer']:
                bad.append(f"{title} / {shape} @ {lat} ms: base {b['Answer'][:60]!r} != PR {p['Answer'][:60]!r}")
            for who, r in (('base', b), ('pr', p)):
                if r.get('Truth', 'ok').startswith('WRONG'):
                    bad.append(f"{title} / {shape} @ {lat} ms: {who} {r['Truth']}")
        print(f"| {shape.replace('|', chr(92) + '|')} | " + " | ".join(cols) + " |")
print("\nANSWERS: " + ("identical to the base build and to ground truth on every shape" if not bad else "\n- " + "\n- ".join(bad)))
sys.exit(1 if bad else 0)
