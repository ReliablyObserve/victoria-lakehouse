#!/usr/bin/env python3
"""Reports of the reader matrix.

  report.py <out-dir>                 Markdown matrix of the result-*.json files (PR / job summary)
  report.py --doc-table               the coverage table for docs/open-parquet-format.md
  report.py --check-doc [doc.md]      fail when the table in the doc is not the generated one
"""
import glob
import json
import os
import re
import sys

import gaps
import lib

HERE = os.path.dirname(os.path.abspath(__file__))
START, END = "<!-- readers-table:start -->", "<!-- readers-table:end -->"


def manifest():
    return lib.read_json(os.path.join(HERE, "engines.json"))


def short(issue):
    return "#" + issue.rsplit("/", 1)[1] if issue else None


# ---------------------------------------------------------------------------------------------
# the committed coverage table (derived from the manifest and the known gaps)
# ---------------------------------------------------------------------------------------------

def _covering(engine, signal, layer):
    return [g for g in gaps.GAPS if engine in g["engines"] and signal in g.get("signals", lib.SIGNALS)
            and layer in g.get("layers", ("raw", "compacted"))]


def verdict(engine, signals, layers):
    """yes: no gap touches the cells; no: every (signal, layer) cell is covered by a gap that fails every
    check; partial: a gap touches some cells or some checks."""
    pairs = [(s, l) for s in signals for l in layers]
    touching = [g for s, l in pairs for g in _covering(engine, s, l)]
    if not touching:
        return "yes"
    refs = ", ".join(sorted({short(g["issue"]) or "engine caveat" for g in touching}))
    if all(any(g.get("queries") is None and (s, l) in (g.get("full") or [(s, l)]) for g in _covering(engine, s, l)) for s, l in pairs):
        return "no (%s)" % refs
    return "partial (%s)" % refs


def doc_table():
    m = manifest()
    rows = ["| Engine | Version | Logs | Traces | Raw files | Compacted files | Partition pruning | CI-tested | CI job |",
            "|---|---|---|---|---|---|---|---|---|"]
    for e in m["engines"]:
        if e["id"] == "parquet-tools":
            rows.append("| %s | %s | inspect | inspect | yes | yes | n/a | yes | `%s` |" % (e["name"], e["version"], e["job"]))
            continue
        rows.append("| %s | %s | %s | %s | %s | %s | %s | yes | `%s` |" % (
            e["name"], e["version"],
            verdict(e["id"], ("logs",), ("raw", "compacted")), verdict(e["id"], ("traces",), ("raw", "compacted")),
            verdict(e["id"], lib.SIGNALS, ("raw",)), verdict(e["id"], lib.SIGNALS, ("compacted",)),
            verdict(e["id"], lib.SIGNALS, ("pruned",)), e["job"]))
    for c in m["cloud"]:
        eq = c["equivalent"]
        rows.append("| %s | n/a | documented | documented | documented | documented | documented | no, documented only | %s |" % (
            c["name"], ("closest tested engine: " + eq) if eq else "none"))
    rows += ["", "Known gaps behind the `no` and `partial` cells (each cell still runs in CI and must keep failing until its issue is fixed):", ""]
    for g in gaps.GAPS:
        ref = ("[%s](%s)" % (short(g["issue"]), g["issue"])) if g["issue"] else "engine caveat"
        rows.append("- %s, %s: %s. %s" % (", ".join(g["engines"]), ref, g.get("label", ""), g["note"]))
    return "\n".join(rows)


def check_doc(path):
    text = lib.read_text(path)
    m = re.search(re.escape(START) + r"\n(.*?)\n" + re.escape(END), text, re.S)
    if not m:
        sys.exit("%s: no %s ... %s block" % (path, START, END))
    want = doc_table()
    if m.group(1).strip() != want.strip():
        sys.exit("%s: the reader coverage table is stale; regenerate it with `python3 tests/readers/report.py --doc-table`.\n--- want ---\n%s" % (path, want))
    print("doc coverage table is current")


# ---------------------------------------------------------------------------------------------
# the result report
# ---------------------------------------------------------------------------------------------

def load(outdir):
    results, versions = [], {}
    for f in sorted(glob.glob(os.path.join(outdir, "result-*.json"))):
        d = lib.read_json(f)
        results += d["results"]
        versions.update(d["versions"])
    return results, versions


def cell_state(recs):
    if not recs:
        return "not run"
    if any(r["verdict"] == "FAIL" for r in recs):
        return "FAIL"
    gs = {q["gap"] for r in recs for q in r["queries"].values() if q.get("gap")}
    if gs:
        return "gap " + ", ".join(sorted(short(g) if g.startswith("http") else g for g in gs))
    return "PASS"


def report(outdir):
    results, versions = load(outdir)
    out = ["## External Parquet reader matrix", ""]
    out += ["Each engine ran its documented example (docs/open-parquet-format.md) against the S3 bucket and was compared with the writer-side truth",
            "(the manifests computed from the rows before they were sent, not with what Lakehouse reads back); ten queries per cell plus the list of files read,",
            "five tenants (numeric 4401:1, alias acme-corp, 3000000000:0, golden 4294967294:0, bloom 4402:3), raw and compacted files.", ""]
    out += ["| Engine | Version | Logs raw | Logs compacted | Traces raw | Traces compacted | Pruning | Tenant >= 2^31 (big, golden) |", "|---|---|---|---|---|---|---|---|"]
    engines_seen = []
    for r in results:
        if r["engine"] not in engines_seen:
            engines_seen.append(r["engine"])
    bad = 0
    for e in engines_seen:
        rs = [r for r in results if r["engine"] == e]
        cells = []
        for sig in lib.SIGNALS:
            for layer in ("raw", "compacted"):
                cells.append(cell_state([r for r in rs if r["signal"] == sig and r["layer"] == layer]))
        pr = cell_state([r for r in rs if r["layer"] == "pruned"])
        big = cell_state([r for r in rs if r["tenant"] in ("big", "golden")])
        bad += sum(1 for c in cells + [pr, big] if c == "FAIL")
        out.append("| %s | %s | %s | %s | %s |" % (e, versions.get(e, ""), " | ".join(cells), pr, big))
    out += ["", "**%s**" % ("FAIL: %d cell group(s) differ from the truth" % bad if bad else "No unexplained difference."), ""]
    gap_rows = sorted({(q["gap"], r["engine"]) for r in results for q in r["queries"].values() if q.get("gap")})
    if gap_rows:
        out += ["Known gaps (each still runs and must keep failing until its issue is fixed):", ""]
        for g, e in gap_rows:
            out.append("- %s: %s" % (e, g))
        out.append("")
    sh = os.path.join(outdir, "storage-health.json")
    if os.path.exists(sh):
        h = lib.read_json(sh)
        out += ["Storage health (#343, fixed by #347): once every group was compacted, the scans that followed did %s; %d Parquet objects before and after." % (
            "nothing" if h["ok"] else "NOT nothing (%s)" % "; ".join(h["violations"]), h["objects_after"]), ""]
    inv = os.path.join(outdir, "inventory.txt")
    if os.path.exists(inv):
        out += ["<details><summary>Fixture inventory</summary>", "", "```", lib.read_text(inv).rstrip(), "```", "</details>", ""]
    print("\n".join(out))
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--doc-table":
        print(doc_table())
    elif len(sys.argv) > 1 and sys.argv[1] == "--check-doc":
        check_doc(sys.argv[2] if len(sys.argv) > 2 else lib.DOC)
    else:
        sys.exit(report(sys.argv[1] if len(sys.argv) > 1 else "."))
