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
from gaps import known_gap


def judge(got, q, w, gap):
    """Status of one query of one cell. A known gap that fails stays green but visible; a known
    gap that passes turns the matrix red (unless it is data dependent) so the entry is removed in
    the change that fixes it; anything else that differs from the oracle is a failure."""
    g = got.get(q)
    ok = q in got and not isinstance(g, engines.QueryError) and g == w
    st = "pass" if ok else "fail"
    if gap and not ok:
        st = "known-gap"
    elif gap and ok and not gap.get("optional"):
        st = "gap-closed"
    return {"status": st, "ok": st in ("pass", "known-gap"), "got": str(g) if isinstance(g, engines.QueryError) else g,
            "want": w, "gap": (gap["issue"] or gap["id"]) if gap else None}


def pruning_cells(a, oracles, snips):
    """Partition pruning proven without engine internals: the prefix holds one real day plus a
    partition whose only object is garbage. The doc example's dt filter must still return the
    oracle's answer for that day (the garbage object was never opened) while the unfiltered count
    on the same prefix must fail or differ (the control: the garbage really is in reach)."""
    out = []
    o = oracles["compacted"]
    for eng in [e for e in a.engines.split(",") if e != "parquet-tools"]:
        for sig in lib.SIGNALS:
            key = "numeric/" + sig
            want = o["answers"][key]
            params = o["params"][key]
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
            pruned = not isinstance(pr, engines.QueryError) and pr == want["dt_filter"]
            controlled = isinstance(ctl, engines.QueryError) or ctl is None or ctl != want["count"]
            gap = known_gap(eng, sig, "pruned", "prune", "dt_filter")
            st = "pass" if pruned and controlled else "fail"
            if gap and st == "fail":
                st = "known-gap"
            rec["queries"]["pruning"] = {"status": st, "ok": st != "fail", "got": str(pr), "want": want["dt_filter"],
                                         "control_failed": controlled, "gap": (gap["issue"] or gap["id"]) if gap else None}
            rec["ok"] = rec["queries"]["pruning"]["ok"]
            rec["verdict"] = "PASS" if st == "pass" else ("KNOWN-GAP" if st == "known-gap" else "FAIL")
            print("%-10s %-24s %s %s" % (eng, rec["cell"], rec["verdict"], "" if st == "pass" else "pruned=%s control=%s got=%s %s" % (pruned, controlled, str(pr)[:100], rec.get("error", ""))))
            out.append(rec)
    return out


def inspection_cells(a, oracles, snips):
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
                body = lib.materialise(snip["body"], oracles[layer]["params"][tenant + "/" + sig], lib.S3_HOST, bucket, t["prefix"], t["account"])
                body = body.replace(literal.replace("2026-01-01", oracles[layer]["params"][tenant + "/" + sig]["dt"]), key.split("/%s/" % sig, 1)[1])
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
    ap.add_argument("--oracle-raw", required=True)
    ap.add_argument("--oracle-compacted", required=True)
    ap.add_argument("--tenants", default="numeric,alias,big")
    ap.add_argument("--layers", default="raw,compacted")
    ap.add_argument("--out", required=True)
    ap.add_argument("--pruning", action="store_true", help="also run the partition-pruning cells (needs fixture.py make-prune-fixture)")
    a = ap.parse_args()
    oracles = {"raw": json.load(open(a.oracle_raw)), "compacted": json.load(open(a.oracle_compacted))}
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
                        items.append((engines.Cell(tenant, sig, layer), oracles[layer]["params"][key], lib.find_snippet(snips, eng, sig)))
            batch = engines.BATCH[eng](items)
        for tenant in a.tenants.split(","):
            for sig in lib.SIGNALS:
                for layer in a.layers.split(","):
                    cell = engines.Cell(tenant, sig, layer)
                    key = "%s/%s" % (tenant, sig)
                    want = oracles[layer]["answers"][key]
                    params = oracles[layer]["params"][key]
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
                        got = {}
                    extras = sorted(k for k in got if k not in lib.QUERIES)
                    for q in list(lib.QUERIES) + extras:
                        gap = known_gap(eng, sig, layer, tenant, q) if q in lib.QUERIES else None
                        rec["queries"][q] = judge(got, q, want[lib.base_query(q)], gap)
                    rec["ok"] = all(v["ok"] for v in rec["queries"].values())
                    results.append(rec)
                    sts = [v["status"] for v in rec["queries"].values()]
                    verdict = "PASS" if rec["ok"] and "known-gap" not in sts else ("KNOWN-GAP" if rec["ok"] else "FAIL")
                    rec["verdict"] = verdict
                    print("%-10s %-24s %s %s" % (eng, repr(cell), verdict, "" if rec["ok"] and verdict == "PASS" else rec.get("error", ", ".join("%s=%s" % (q, v["status"]) for q, v in rec["queries"].items() if v["status"] != "pass"))[:160]))
    if a.pruning:
        results += pruning_cells(a, oracles, snips)
    if "parquet-tools" in a.engines.split(","):
        results += inspection_cells(a, oracles, snips)
    json.dump({"versions": engines.versions(), "results": results}, open(a.out, "w"), indent=1)
    return 0 if all(r["ok"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
