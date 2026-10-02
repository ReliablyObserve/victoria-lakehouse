#!/usr/bin/env python3
"""Writer-side truth of the reader matrix.

Lakehouse is not its own oracle here. Every row the fixture sends is counted before it is sent
(datagen's --manifest for the generated tenants, golden.py for the hand-written ones), and these
manifests are the truth that Lakehouse's own answers, the Parquet files and every external engine
are compared with. A defect that Lakehouse reads back identically (a dropped map key, timestamps
cut to microseconds, rows filed under the wrong tenant) differs from the manifest.

  truth.py build  <manifest-dir> <manifest.json> <truth.json>
                                        merge the manifests, derive the query parameters and the
                                        expected answer of every check
  truth.py compare-oracle <truth.json> <oracle.json>
                                        Lakehouse's answers must equal the truth
  truth.py verify-files <manifest.json> <layer>
                                        read every Parquet object of the layer and compare it with the manifest
  truth.py facts <out.json>             bloom filter header sizes and non-UTF-8 footer values of every object (both layers)
  truth.py assert-bloom <facts.json>    the bloom tenant writes 96-byte filters (writer side of the #341 expectation)
"""
import glob
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import lib

CHECKS_NONZERO = ("count", "by_service", "field_filter", "time_range", "map_filter", "trace_by_id", "dt_filter", "ts_bounds", "tenant")


def merge_signal(parts):
    out = {"count": 0, "by_service": {}, "errors": 0, "field_filter": 0, "map_filter": 0, "map_keys": {},
           "timestamps": [], "trace_counts": {}, "map_column": parts[0]["map_column"]}
    for p in parts:
        out["count"] += p["count"]
        out["errors"] += p["errors"]
        out["field_filter"] += p["field_filter"]
        out["map_filter"] += p["map_filter"]
        out["timestamps"] += p["timestamps"]
        for field in ("by_service", "map_keys", "trace_counts"):
            for k, v in (p[field] or {}).items():
                out[field][k] = out[field].get(k, 0) + v
    out["timestamps"].sort()
    out["ts_min"], out["ts_max"] = out["timestamps"][0], out["timestamps"][-1]
    return out


def merge(manifest_dir):
    """{tenant name: {"account", "project", "logs": {...}, "traces": {...}}} from every *.json part."""
    by = {}
    for f in sorted(glob.glob(os.path.join(manifest_dir, "*.json"))):
        d = lib.read_json(f)
        by.setdefault(d["name"], []).append(d)
    out = {}
    for name, parts in by.items():
        t = lib.TENANTS[name]
        out[name] = {"account": t["account"], "project": t["project"],
                     "logs": merge_signal([p["logs"] for p in parts]),
                     "traces": merge_signal([p["traces"] for p in parts]),
                     "parts": len(parts)}
    return out


def derive_params(m):
    ts = m["timestamps"]
    n = len(ts)
    # The window edges are exact row timestamps (not whole seconds): a row sits on the inclusive
    # lower edge and a different row on the exclusive upper edge, so any rounding of the stored
    # timestamps moves a row in or out of the window.
    from_ns, to_ns = ts[n * 3 // 10], ts[n * 6 // 10]
    assert from_ns < to_ns, "the window is empty"
    mid = datetime.fromtimestamp((m["ts_min"] + m["ts_max"]) // 2 // 10**9, tz=timezone.utc)
    best = sorted(m["trace_counts"].items(), key=lambda kv: (-kv[1], kv[0]))[0]
    return {"from_ns": from_ns, "to_ns": to_ns, "trace_id": best[0], "dt": mid.strftime("%Y-%m-%d")}


def day_bounds_ns(dt):
    d0 = datetime.strptime(dt, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(d0.timestamp()) * 10**9, int((d0 + timedelta(days=1)).timestamp()) * 10**9


def expected_answers(name, tenant, m, p):
    ts = m["timestamps"]
    d0, d1 = day_bounds_ns(p["dt"])
    return {
        "count": m["count"],
        "by_service": dict(m["by_service"]),
        "field_filter": m["field_filter"],
        "time_range": sum(1 for t in ts if p["from_ns"] <= t < p["to_ns"]),
        "map_filter": m["map_filter"],
        "trace_by_id": m["trace_counts"][p["trace_id"]],
        "dt_filter": sum(1 for t in ts if d0 <= t < d1),
        "ts_bounds": [m["ts_min"], m["ts_max"]],
        "utc_check": 0,
        "tenant": [[tenant["account"], tenant["project"]]],
    }


def guard_nonzero(key, answers):
    """An answer of zero proves nothing (every engine agrees on empty): every check except
    utc_check, whose correct answer is zero, must be non-empty."""
    for q in CHECKS_NONZERO:
        v = answers[q]
        empty = (not v) if not isinstance(v, list) or q == "tenant" else not any(v)
        if q == "by_service":
            empty = not v or not any(v.values())
        assert not empty, "%s: the truth of %r is empty/zero, the check would prove nothing; change the fixture" % (key, q)


def build(manifest_dir, manifest_out, truth_out):
    m = merge(manifest_dir)
    missing = sorted(set(lib.TENANTS) - set(m))
    assert not missing, "no manifest for tenants %s" % missing
    truth = {"params": {}, "answers": {}}
    for name, t in m.items():
        for sig in lib.SIGNALS:
            key = "%s/%s" % (name, sig)
            p = derive_params(t[sig])
            a = expected_answers(name, lib.TENANTS[name], t[sig], p)
            guard_nonzero(key, a)
            truth["params"][key], truth["answers"][key] = p, a
    lib.write_json(manifest_out, m)
    lib.write_json(truth_out, truth, indent=1, sort_keys=True)
    for k, a in sorted(truth["answers"].items()):
        print("  truth %-14s count=%-5d time_range=%-4d trace_by_id=%-3d map_filter=%-4d field_filter=%d" % (
            k, a["count"], a["time_range"], a["trace_by_id"], a["map_filter"], a["field_filter"]))
    print("truth: %d tenants x 2 signals, every answer non-empty (utc_check excepted)" % len(m))


def compare_oracle(truth_path, oracle_path):
    truth, oracle = lib.read_json(truth_path), lib.read_json(oracle_path)
    diffs = []
    for k, want in truth["answers"].items():
        got = oracle["answers"][k]
        for q, w in want.items():
            if got[q] != w:
                diffs.append("%s %s: Lakehouse answered %r, the writer sent %r" % (k, q, got[q], w))
    if diffs:
        sys.exit("Lakehouse's answers differ from the writer-side truth (%d):\n  %s" % (len(diffs), "\n  ".join(diffs[:30])))
    print("oracle: Lakehouse's %s answers equal the writer-side truth (%d cells)" % (oracle["layer"], len(truth["answers"])))


# ---------------------------------------------------------------------------------------------
# the Parquet files against the manifest
# ---------------------------------------------------------------------------------------------

def parquet_keys(fs, prefix):
    return sorted(k for k in fs.find(prefix) if k.endswith(".parquet"))


def read_group(fs, keys, sig):
    import pyarrow.parquet as pq
    m = {"count": 0, "by_service": {}, "errors": 0, "field_filter": 0, "timestamps": [], "trace_counts": {},
         "map_rows": {}, "tenants": set(), "missing": set()}
    mapcol = "log.attributes" if sig == "logs" else "span.attributes"
    required = ["timestamp_unix_nano", "service.name", "severity_text" if sig == "logs" else "status.code", "trace_id",
                "account_id", "project_id", mapcol]
    for k in keys:
        pf = pq.ParquetFile(fs.open(k))
        names = pf.schema_arrow.names
        gone = [c for c in required if c not in names]
        if gone:  # a column a reader depends on is not in the file: report it, do not crash
            m["missing"].update(gone)
            continue
        t = pf.read(columns=[c for c in ["timestamp_unix_nano", "service.name", "severity_text", "status.code", "trace_id",
                                         "account_id", "project_id", mapcol] if c in names])
        d = {c: t[c].to_pylist() for c in t.column_names}
        n = t.num_rows
        m["count"] += n
        m["timestamps"] += d["timestamp_unix_nano"]
        for i in range(n):
            svc = d["service.name"][i]
            m["by_service"][svc] = m["by_service"].get(svc, 0) + 1
            err = d["severity_text"][i] == "ERROR" if sig == "logs" else d["status.code"][i] == 2
            if err:
                m["errors"] += 1
                if svc == "api-gateway":
                    m["field_filter"] += 1
            tid = d["trace_id"][i]
            if tid:
                m["trace_counts"][tid] = m["trace_counts"].get(tid, 0) + 1
            m["tenants"].add((d["account_id"][i], d["project_id"][i]))
            for key, _v in (d[mapcol][i] or []):
                m["map_rows"][key] = m["map_rows"].get(key, 0) + 1
    m["timestamps"].sort()
    return m


def verify_files(manifest_path, layer):
    """Every tenant/signal group of a layer, read straight from the Parquet files, equals the manifest:
    row count, rows per service, error rows, every timestamp (exact nanoseconds), spans per trace,
    the rows carrying each map key, and the tenant columns."""
    manifest = lib.read_json(manifest_path)
    fs = lib.s3fs_client()
    fs.invalidate_cache()
    bucket = lib.BUCKET[layer]
    bad, extra = [], {}
    for name, t in manifest.items():
        for sig in lib.SIGNALS:
            want = t[sig]
            got = read_group(fs, parquet_keys(fs, "%s/%s/%s/" % (bucket, lib.TENANTS[name]["prefix"], sig)), sig)
            tag = "%s/%s/%s" % (name, sig, layer)
            if got["missing"]:
                bad.append("%s: files lack the columns %s" % (tag, sorted(got["missing"])))
                continue
            for field in ("count", "errors", "field_filter"):
                if got[field] != want[field]:
                    bad.append("%s %s: files %r, writer %r" % (tag, field, got[field], want[field]))
            if got["by_service"] != want["by_service"]:
                bad.append("%s by_service: files %r, writer %r" % (tag, got["by_service"], want["by_service"]))
            if got["timestamps"] != want["timestamps"]:
                diff = [(a, b) for a, b in zip(got["timestamps"], want["timestamps"]) if a != b][:3]
                bad.append("%s timestamps differ (first: files, writer) %s" % (tag, diff))
            if got["trace_counts"] != want["trace_counts"]:
                bad.append("%s rows per trace differ: %d trace IDs in files, %d from the writer" % (tag, len(got["trace_counts"]), len(want["trace_counts"])))
            if got["tenants"] != {(t["account"], t["project"])}:
                bad.append("%s tenant columns %s, writer %s" % (tag, sorted(got["tenants"]), (t["account"], t["project"])))
            for key, n in want["map_keys"].items():
                if got["map_rows"].get(key, 0) != n:
                    extra.setdefault(tag, {})[key] = (got["map_rows"].get(key, 0), n)
            surplus = sorted(set(got["map_rows"]) - set(want["map_keys"]))
            if surplus:
                print("note %s: map keys the writer did not send (added by Lakehouse): %s" % (tag, surplus))
    # A map key that is promoted to its own column by the schema leaves the map; those are counted
    # through the column below, everything else must match exactly.
    real = []
    for tag, keys in extra.items():
        sig = tag.split("/")[1]
        nm = tag.split("/")[0]
        for key, (g, w) in keys.items():
            promoted = promoted_count(fs, bucket, nm, sig, key)
            if promoted != w:
                real.append("%s map key %r: %d rows carry it in the map%s, the writer sent %d" % (
                    tag, key, g, (" and %d in a column of that name" % promoted) if promoted is not None else " (no column of that name)", w))
    bad += real
    if bad:
        sys.exit("The %s Parquet files differ from the writer-side truth (%d):\n  %s" % (layer, len(bad), "\n  ".join(bad[:40])))
    print("files: every %s object equals the writer-side manifest (%d tenants x 2 signals: counts, services, errors, exact timestamps, traces, map keys)" % (layer, len(manifest)))


def promoted_count(fs, bucket, name, sig, key):
    import pyarrow.parquet as pq
    total, seen = 0, False
    for k in parquet_keys(fs, "%s/%s/%s/" % (bucket, lib.TENANTS[name]["prefix"], sig)):
        pf = pq.ParquetFile(fs.open(k))
        if key in pf.schema_arrow.names:
            seen = True
            # A promoted column holds "" (or 0) where the row did not carry the attribute.
            total += sum(1 for v in pf.read(columns=[key])[key].to_pylist() if v not in (None, ""))
    return total if seen else None


# ---------------------------------------------------------------------------------------------
# split-block bloom filter headers
# ---------------------------------------------------------------------------------------------

def sbbf_num_bytes(fs, key, offset):
    """numBytes of the BloomFilterHeader at `offset`: a Thrift compact struct whose first field is the i32 numBytes."""
    head = fs.cat_file(key, start=offset, end=offset + 12)
    assert head[0] == 0x15, "unexpected bloom filter header byte %#x" % head[0]
    n, shift, i = 0, 0, 1
    while True:
        b = head[i]
        n |= (b & 0x7F) << shift
        i += 1
        if not b & 0x80:
            break
        shift += 7
    return (n >> 1) ^ -(n & 1)


def cell_of(rel, layer):
    """(cell key, layer) of an object path relative to its bucket: <account>/<project>/<signal>/..."""
    parts = rel.split("/")
    if parts[0] == "prune":
        layer = "pruned"
    return "%s/%s/%s/%s" % (parts[0], parts[1], parts[2], layer)


def facts_scan(out_path):
    """What the files themselves say, read by the writer-side fixture step (not by an engine):
    the size of every split-block bloom filter header (the ClickHouse expectation, #341) and whether
    an object's footer key-value area holds a value that is not UTF-8 (the Polars / DataFusion
    expectation, #340). Both are properties of the bytes Lakehouse wrote, so the known gaps they
    explain are expected in exactly the cells the facts name, not wherever a random run happens to hit them."""
    import pyarrow.parquet as pq
    fs = lib.s3fs_client()
    fs.invalidate_cache()
    res = {"objects": {}, "cells": {}, "nonpow2_total": 0, "footer": {}, "nonutf8_total": 0}
    for layer, bucket in lib.BUCKET.items():
        for key in parquet_keys(fs, bucket):
            if key.endswith("/poison.parquet"):
                continue
            rel = key.split("/", 1)[1]
            cell = cell_of(rel, layer)
            md = pq.ParquetFile(fs.open(key)).metadata
            cols = {}
            for rg in range(md.num_row_groups):
                for c in range(md.num_columns):
                    cc = md.row_group(rg).column(c)
                    if cc.bloom_filter_offset is not None:
                        cols.setdefault(cc.path_in_schema, []).append(sbbf_num_bytes(fs, key, cc.bloom_filter_offset))
            res["objects"]["%s:%s" % (layer, rel)] = cols
            agg = res["cells"].setdefault(cell, {})
            for col, sizes in cols.items():
                agg.setdefault(col, set()).update(sizes)
            bad = []
            for k, v in (md.metadata or {}).items():
                try:
                    v.decode("utf-8")
                except UnicodeDecodeError:
                    bad.append(k.decode("ascii", "replace").split("_rg_")[0])
            f = res["footer"].setdefault(cell, {"objects": 0, "nonutf8": 0, "keys": []})
            f["objects"] += 1
            if bad:
                f["nonutf8"] += 1
                f["keys"] = sorted(set(f["keys"]) | set(bad))
                res["nonutf8_total"] += 1
    for cell, cols in res["cells"].items():
        for col in cols:
            cols[col] = sorted(cols[col])
            res["nonpow2_total"] += sum(1 for s in cols[col] if s & (s - 1))
    lib.write_json(out_path, res, indent=1, sort_keys=True)
    print("facts: %d objects scanned; %d (cell, column) bloom sizes are not a power of two; %d objects have a footer value that is not UTF-8" % (
        len(res["objects"]), res["nonpow2_total"], res["nonutf8_total"]))


def assert_bloom(path):
    """Writer side: the `bloom` tenant (60 rows in one hour per file) must produce 96-byte split-block
    bloom filters on its high-cardinality columns, a size that is not a power of two. When Lakehouse
    writes no such size anywhere any more (#341 is fixed), say so instead: the matrix then reports the
    ClickHouse gap as closed and asks for its removal."""
    res = lib.read_json(path)
    if res["nonpow2_total"] == 0:
        print("sbbf: no bloom filter has a non-power-of-two size any more (#341 fixed): the gap entry must be removed")
        return
    bad = []
    for sig in lib.SIGNALS:
        raw = res["cells"].get("%s/%s/raw" % (lib.TENANTS["bloom"]["prefix"], sig), {}).get("trace_id")
        if raw != [96]:
            bad.append("bloom/%s/raw trace_id bloom sizes %s, want [96]" % (sig, raw))
        comp = res["cells"].get("%s/%s/compacted" % (lib.TENANTS["bloom"]["prefix"], sig), {}).get("trace_id") or []
        if not nonpow2(comp):
            bad.append("bloom/%s/compacted trace_id bloom sizes %s: none is a non-power-of-two" % (sig, comp))
    if bad:
        sys.exit("the bloom tenant does not produce the expected bloom filter sizes: %s" % bad)
    print("sbbf: the bloom tenant writes 96-byte bloom filters (raw) and non-power-of-two ones after compaction")


def nonpow2(sizes):
    return any(s & (s - 1) for s in sizes)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "build":
        build(*sys.argv[2:5])
    elif cmd == "compare-oracle":
        compare_oracle(*sys.argv[2:4])
    elif cmd == "verify-files":
        verify_files(*sys.argv[2:4])
    elif cmd == "facts":
        facts_scan(sys.argv[2])
    elif cmd == "assert-bloom":
        assert_bloom(sys.argv[2])
    else:
        sys.exit(__doc__)
