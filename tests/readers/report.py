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
    return json.load(open(os.path.join(HERE, "engines.json")))


def short(issue):
    return "#" + issue.rsplit("/", 1)[1] if issue else None


# ---------------------------------------------------------------------------------------------
# the committed coverage table (derived from the manifest and the known gaps)
# ---------------------------------------------------------------------------------------------

def gap_issues(engine, signals, layers):
    found = []
    for g in gaps.GAPS:
        if engine in g["engines"] and set(signals) & set(g.get("signals", lib.SIGNALS)) and set(layers) & set(g.get("layers", ("raw", "compacted"))):
            found.append(g)
    return found


def verdict(engine, signals, layers):
    gs = gap_issues(engine, signals, layers)
    if not gs:
        return "yes"
    full = [g for g in gs if g.get("queries") is None]
    refs = ", ".join(sorted({short(g["issue"]) or "engine caveat" for g in gs}))
    if full and set(layers) <= set(full[0].get("layers", ("raw", "compacted"))) and set(signals) <= set(full[0].get("signals", lib.SIGNALS)):
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
    text = open(path, encoding="utf-8").read()
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
        d = json.load(open(f))
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
    out += ["Each engine ran its documented example (docs/open-parquet-format.md) against the S3 bucket and was compared with Lakehouse's own answers;",
            "ten queries per cell, three tenants (numeric 4401:1, alias acme-corp, 3000000000:0), raw and compacted files.", ""]
    out += ["| Engine | Version | Logs raw | Logs compacted | Traces raw | Traces compacted | Pruning | Tenant >= 2^31 |", "|---|---|---|---|---|---|---|---|"]
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
        big = cell_state([r for r in rs if r["tenant"] == "big"])
        bad += sum(1 for c in cells + [pr, big] if c == "FAIL")
        out.append("| %s | %s | %s | %s | %s |" % (e, versions.get(e, ""), " | ".join(cells), pr, big))
    out += ["", "**%s**" % ("FAIL: %d cell group(s) differ from the oracle" % bad if bad else "No unexplained difference."), ""]
    gap_rows = sorted({(q["gap"], r["engine"]) for r in results for q in r["queries"].values() if q.get("gap")})
    if gap_rows:
        out += ["Known gaps (each still runs and must keep failing until its issue is fixed):", ""]
        for g, e in gap_rows:
            out.append("- %s: %s" % (e, g))
        out.append("")
    inv = os.path.join(outdir, "inventory.txt")
    if os.path.exists(inv):
        out += ["<details><summary>Fixture inventory</summary>", "", "```", open(inv).read().rstrip(), "```", "</details>", ""]
    print("\n".join(out))
    return 1 if bad else 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--doc-table":
        print(doc_table())
    elif len(sys.argv) > 1 and sys.argv[1] == "--check-doc":
        check_doc(sys.argv[2] if len(sys.argv) > 2 else lib.DOC)
    else:
        sys.exit(report(sys.argv[1] if len(sys.argv) > 1 else "."))
