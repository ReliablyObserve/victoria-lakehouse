#!/usr/bin/env python3
"""Run the documented example of every engine against the S3 bucket and compare with the oracle.

  matrix.py --engines duckdb,pyarrow --oracle-raw o1.json --oracle-compacted o2.json --out result.json
"""
import argparse
import json
import sys
import traceback

import engines
import lib
import gaps
from gaps import known_gap


# The layout of the dumped fixture ($OUT/fixture/<bucket>/...): written here literally and not taken from
# lib.BUCKET, so a runner that reads the wrong bucket for a layer is compared with the right inventory.
FIXTURE_DIR = {"raw": "obs-raw", "compacted": "obs-archive"}
# Engines whose documented example can say which objects it read (the `files` check).
REPORTS_FILES = ("duckdb", "pyarrow", "polars", "clickhouse", "trino", "spark")


def inventory(fixture_dir, layer, prefix, signal):
    """The Parquet objects of a tenant/signal in a layer, relative to <prefix>/<signal>/, from the dumped fixture."""
    import os
    root = os.path.join(fixture_dir, FIXTURE_DIR[layer], prefix, signal)
    out = []
    for d, _, files in os.walk(root):
        for f in files:
            if f.endswith(".parquet"):
                out.append(os.path.relpath(os.path.join(d, f), root))
    return sorted(out)


def judge(got, q, w, gap, tenant=None):
    """Status of one query of one cell.

    pass        equals the truth
    known-gap   differs from the truth in exactly the way the gap describes (its error text, or its exact wrong value)
    gap-closed  a known gap that now passes: red, so the entry is removed in the change that fixes it
    fail        anything else, including a different error or a different wrong answer under a gap
    """
    g = got.get(q)
    is_err = isinstance(g, engines.QueryError)
    ok = q in got and not is_err and g == w
    if not gap:
        st = "pass" if ok else "fail"
    elif ok:
        st = "gap-closed"
    else:
        st = "known-gap" if q in got and gaps.gap_matches(gap, tenant, g) else "fail"
    return {"status": st, "ok": st in ("pass", "known-gap"), "got": str(g) if is_err else g,
            "want": w, "gap": (gap["issue"] or gap["id"]) if gap else None}


def files_check(got, inv, gap, tenant):
    """The objects the engine read for the unfiltered scan are exactly the inventory of the layer. Under a
    gap that makes the unfiltered scan fail, the file list fails the same way."""
    g = got.get(lib.FILES_QUERY)
    base = {"want": "%d objects" % len(inv), "gap": (gap["issue"] or gap["id"]) if gap else None}
    if g is None:
        return dict(base, status="fail", ok=False, got=None, note="the example did not report the files it read")
    if isinstance(g, engines.QueryError):
        st = "known-gap" if gap and gaps.gap_matches(gap, tenant, g) else "fail"
        return dict(base, status=st, ok=st == "known-gap", got=str(g))
    ok = g == inv
    if gap:
        return dict(base, status="gap-closed", ok=False, got="%d objects" % len(g))
    return dict(base, status="pass" if ok else "fail", ok=ok, got=("%d objects" % len(g)) if ok else
                {"missing": sorted(set(inv) - set(g))[:3], "unexpected": sorted(set(g) - set(inv))[:3], "read": len(g)})


def judge_pruning(pr, ctl, want, gap, setup_error=None):
    """(status, pruned, controlled) of one pruning cell.

    pruned      the filtered query answered the truth for the real day (the garbage object was never opened)
    controlled  the unfiltered control FAILED with an error (the garbage object is really in reach). A control
                that returns a number, right or wrong, proves nothing: the engine may simply have skipped the
                object, and the filtered answer would then say nothing about pruning.
    A gap turns a failing cell into known-gap only when the failure is the one it describes, and a passing
    cell into gap-closed."""
    pruned = pr is not None and not isinstance(pr, engines.QueryError) and pr == want
    controlled = isinstance(ctl, engines.QueryError)
    ok = pruned and controlled
    if not gap:
        return ("pass" if ok else "fail"), pruned, controlled
    if ok:
        return "gap-closed", pruned, controlled
    failing = pr if isinstance(pr, engines.QueryError) else (engines.QueryError(setup_error) if setup_error else None)
    return ("known-gap" if failing is not None and gaps.gap_matches(gap, "prune", failing) else "fail"), pruned, controlled


def pruning_cells(a, truth, snips, facts):
    """Partition pruning proven without engine internals: the prefix holds one real day plus a
    partition whose only object is garbage. The doc example's dt filter must still return the
    truth for that day (the garbage object was never opened) while the unfiltered count on the same
    prefix must FAIL with an error (the control: the garbage really is in reach, so a cell that
    answers the filtered query without touching it was pruned, not lucky)."""
    out = []
    for eng in [e for e in a.engines.split(",") if e != "parquet-tools"]:
        for sig in lib.SIGNALS:
            key = "numeric/" + sig
            want = truth["answers"][key]
            params = truth["params"][key]
            cell = engines.Cell("numeric", sig, "compacted")
            cell.t = dict(cell.t, prefix="prune/1")
            rec = {"engine": eng, "cell": "prune/%s" % sig, "tenant": "prune", "signal": sig, "layer": "pruned", "queries": {}}
            try:
                if eng in engines.BATCH:
                    got = engines.BATCH[eng]([(cell, params, lib.find_snippet(snips, eng, sig))])[repr(cell)]
                    if isinstance(got, Exception):
                        raise got
                else:
                    got = engines.RUNNERS[eng](lib.find_snippet(snips, eng, sig), cell, params)
            except BaseException as e:  # noqa: BLE001
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise
                rec["error"] = "%s: %s" % (type(e).__name__, str(e)[:500])
                got = {}
            pr = got.get("dt_filter")
            ctl = got.get("count")
            gap = gaps.known_gap(eng, sig, "pruned", "prune", "dt_filter", facts, "prune/1")
            st, pruned, controlled = judge_pruning(pr, ctl, want["dt_filter"], gap, rec.get("error"))
            rec["queries"]["pruning"] = {"status": st, "ok": st in ("pass", "known-gap"), "got": str(pr), "want": want["dt_filter"],
                                         "control_failed": controlled, "gap": (gap["issue"] or gap["id"]) if gap else None}
            rec["ok"] = rec["queries"]["pruning"]["ok"]
            rec["verdict"] = {"pass": "PASS", "known-gap": "KNOWN-GAP"}.get(st, "FAIL")
            print("%-10s %-24s %s %s" % (eng, rec["cell"], rec["verdict"], "" if st == "pass" else "pruned=%s control_failed=%s got=%s %s" % (pruned, controlled, str(pr)[:100], rec.get("error", ""))))
            out.append(rec)
    return out


def inspection_cells(a, truth, snips):
    """parquet-tools: run the documented inspect/show commands on one object per tenant, signal
    and layer; the row count it prints must equal the footer's row count read by pyarrow."""
    import os
    import re
    import subprocess

    import pyarrow.parquet as pq
    fs = lib.s3fs_client()
    out = []
    literal = "dt=2026-01-01/hour=00/0123456789abcdef.parquet"
    for tenant in a.tenants.split(","):
        for sig in lib.SIGNALS:
            for layer in a.layers.split(","):
                t = lib.TENANTS[tenant]
                bucket = lib.BUCKET[layer]
                keys = sorted(k for k in fs.find("%s/%s/%s/" % (bucket, t["prefix"], sig)) if k.endswith(".parquet"))
                key = keys[len(keys) // 2]
                want = pq.ParquetFile(fs.open(key)).metadata.num_rows
                snip = lib.find_snippet(snips, "parquet-tools", sig)
                body = lib.materialise(snip["body"], truth["params"][tenant + "/" + sig], lib.S3_HOST, bucket, t["prefix"], t["account"])
                body = body.replace(literal.replace("2026-01-01", truth["params"][tenant + "/" + sig]["dt"]), key.split("/%s/" % sig, 1)[1])
                env = dict(os.environ, PATH=os.path.dirname(sys.executable) + os.pathsep + os.environ["PATH"])
                p = subprocess.run(["bash", "-euo", "pipefail", "-c", body], capture_output=True, text=True, env=env, timeout=300)
                m = re.search(r"num_rows:\s*(\d+)", p.stdout)
                got = int(m.group(1)) if m else None
                ok = p.returncode == 0 and got == want and "account_id" in p.stdout
                rec = {"engine": "parquet-tools", "cell": "%s/%s/%s" % (tenant, sig, layer), "tenant": tenant, "signal": sig, "layer": layer,
                       "queries": {"inspect": {"status": "pass" if ok else "fail", "ok": ok, "got": got, "want": want, "gap": None}},
                       "ok": ok, "verdict": "PASS" if ok else "FAIL"}
                if not ok:
                    rec["error"] = (p.stderr or p.stdout)[-400:]
                print("%-10s %-24s %s %s" % ("parquet-tools", rec["cell"], rec["verdict"], rec.get("error", "")))
                out.append(rec)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engines", required=True)
    ap.add_argument("--truth", required=True, help="truth.json: the writer-side expected answers and the query parameters")
    ap.add_argument("--facts", required=True, help="facts.json: bloom filter sizes and non-UTF-8 footer values of the objects (truth.py facts)")
    ap.add_argument("--fixture", required=True, help="the dumped fixture directory (the inventory of every layer)")
    ap.add_argument("--tenants", default="numeric,alias,big,golden,bloom")
    ap.add_argument("--layers", default="raw,compacted")
    ap.add_argument("--out", required=True)
    ap.add_argument("--pruning", action="store_true", help="also run the partition-pruning cells (needs fixture.py make-prune-fixture)")
    a = ap.parse_args()
    truth = lib.read_json(a.truth)
    facts = lib.read_json(a.facts)
    snips = lib.doc_snippets()
    results = []
    for eng in [e for e in a.engines.split(",") if e != "parquet-tools"]:
        batch = None
        if eng in engines.BATCH:  # engines that start a heavy runtime once for all cells
            items = []
            for tenant in a.tenants.split(","):
                for sig in lib.SIGNALS:
                    for layer in a.layers.split(","):
                        key = "%s/%s" % (tenant, sig)
                        items.append((engines.Cell(tenant, sig, layer), truth["params"][key], lib.find_snippet(snips, eng, sig)))
            batch = engines.BATCH[eng](items)
        for tenant in a.tenants.split(","):
            for sig in lib.SIGNALS:
                for layer in a.layers.split(","):
                    cell = engines.Cell(tenant, sig, layer)
                    key = "%s/%s" % (tenant, sig)
                    want = truth["answers"][key]
                    params = truth["params"][key]
                    rec = {"engine": eng, "cell": repr(cell), "tenant": tenant, "signal": sig, "layer": layer, "queries": {}}
                    try:
                        if batch is not None:
                            got = batch[repr(cell)]
                            if isinstance(got, Exception):
                                raise got
                        else:
                            snip = lib.find_snippet(snips, eng, sig)
                            got = engines.RUNNERS[eng](snip, cell, params)
                    except Exception as e:  # an engine that cannot run the example is a finding
                        rec["error"] = "%s: %s" % (type(e).__name__, str(e)[:500])
                        # The cell failed as a whole: every check fails with that error, so a gap's error text is matched against it.
                        got = {q: engines.QueryError(rec["error"]) for q in list(lib.QUERIES) + [lib.FILES_QUERY]}
                    extras = sorted(k for k in got if k not in lib.QUERIES and k != lib.FILES_QUERY)
                    for q in list(lib.QUERIES) + extras:
                        gap = known_gap(eng, sig, layer, tenant, q, facts, cell.t["prefix"]) if q in lib.QUERIES else None
                        rec["queries"][q] = judge(got, q, want[lib.base_query(q)], gap, tenant)
                    if eng in REPORTS_FILES:
                        rec["queries"][lib.FILES_QUERY] = files_check(got, inventory(a.fixture, layer, cell.t["prefix"], sig),
                                                                      known_gap(eng, sig, layer, tenant, "count", facts, cell.t["prefix"]), tenant)
                    else:
                        rec["queries"][lib.FILES_QUERY] = {"status": "n/a", "ok": True, "got": None, "want": None, "gap": None,
                                                           "note": "the engine cannot report which objects it read; the layer is checked structurally"}
                    rec["ok"] = all(v["ok"] for v in rec["queries"].values())
                    results.append(rec)
                    sts = [v["status"] for v in rec["queries"].values()]
                    verdict = "PASS" if rec["ok"] and "known-gap" not in sts else ("KNOWN-GAP" if rec["ok"] else "FAIL")
                    rec["verdict"] = verdict
                    print("%-10s %-24s %s %s" % (eng, repr(cell), verdict, "" if rec["ok"] and verdict == "PASS" else rec.get("error", ", ".join("%s=%s" % (q, v["status"]) for q, v in rec["queries"].items() if v["status"] != "pass"))[:160]))
    if a.pruning:
        results += pruning_cells(a, truth, snips, facts)
    if "parquet-tools" in a.engines.split(","):
        results += inspection_cells(a, truth, snips)
    lib.write_json(a.out, {"versions": engines.versions(), "results": results}, indent=1)
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
