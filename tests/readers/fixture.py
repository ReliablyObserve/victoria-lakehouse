#!/usr/bin/env python3
"""Fixture steps of the reader matrix that need an S3 client.

  fixture.py wait-flushed      wait until every tenant's Parquet rows equal Lakehouse's own count
  fixture.py snapshot-raw      copy every .parquet object of obs-archive to obs-raw (the raw layer)
  fixture.py wait-compacted    wait until compaction has rewritten the raw files
  fixture.py inventory         print object counts per tenant/signal and layer
"""
import sys
import time

import pyarrow.parquet as pq

import lib


def parquet_keys(fs, bucket):
    return sorted(k for k in fs.find(bucket) if k.endswith(".parquet"))


def rows_in(fs, keys):
    return sum(pq.ParquetFile(fs.open(k)).metadata.num_rows for k in keys)


def lh_counts():
    out = {}
    for tn, t in lib.TENANTS.items():
        for sig in lib.SIGNALS:
            out[(t["prefix"], sig)] = lib.lh_scalar(sig, t, "* | stats count() c")
    return out


def wait_flushed(timeout=300):
    """Wait until, twice in a row, every tenant/signal's Parquet rows equal what Lakehouse itself
    answers. Lakehouse's count is re-read on every poll: while the buffer is flushing it can differ
    from the settled value (a flushed row may be counted by the buffer and by S3 for a moment)."""
    fs = lib.s3fs_client()
    deadline = time.time() + timeout
    agreed = 0
    while True:
        fs.invalidate_cache()
        want = lh_counts()
        keys = parquet_keys(fs, "obs-archive")
        bad = []
        for (prefix, sig), n in want.items():
            ks = [k for k in keys if k.startswith("obs-archive/%s/%s/" % (prefix, sig))]
            got = rows_in(fs, ks)
            if got != n:
                bad.append((prefix, sig, got, n))
        agreed = agreed + 1 if not bad else 0
        if agreed >= 2:
            print("flushed: Parquet rows == Lakehouse rows for all %d tenant/signal pairs" % len(want), flush=True)
            return
        if time.time() > deadline:
            sys.exit("not flushed after %ds: %s" % (timeout, bad))
        time.sleep(5)


def snapshot_raw():
    fs = lib.s3fs_client()
    keys = parquet_keys(fs, "obs-archive")
    for k in keys:
        fs.copy("s3://" + k, "s3://obs-raw/" + k.split("/", 1)[1])
    print("raw layer: copied %d objects to obs-raw" % len(keys))


def per_group(keys):
    g = {}
    for k in keys:
        parts = k.split("/")
        g.setdefault("/".join(parts[1:4]), []).append(k)
    return g


def verify_compacted():
    """The compacted layer is complete and consistent: every tenant/signal group has compacted
    objects, and the Parquet rows still equal Lakehouse's own count (no row lost, none doubled by a
    half-finished rewrite)."""
    fs = lib.s3fs_client()
    fs.invalidate_cache()
    keys = parquet_keys(fs, "obs-archive")
    want = lh_counts()
    bad = []
    for (prefix, sig), n in want.items():
        ks = [k for k in keys if k.startswith("obs-archive/%s/%s/" % (prefix, sig))]
        compacted = [k for k in ks if "/compacted-" in k]
        got = rows_in(fs, ks)
        print("compacted layer %-22s files=%-3d compacted=%-3d rows=%d (lakehouse %d)" % ("%s/%s" % (prefix, sig), len(ks), len(compacted), got, n))
        if not compacted or got != n:
            bad.append((prefix, sig, len(compacted), got, n))
    if bad:
        sys.exit("compacted layer is not complete and consistent: %s" % bad)


def dump(outdir):
    import os
    fs = lib.s3fs_client()
    n = 0
    for bucket in ("obs-archive", "obs-raw"):
        for k in parquet_keys(fs, bucket):
            dst = os.path.join(outdir, k)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            fs.get("s3://" + k, dst)
            n += 1
    print("dumped %d objects to %s" % (n, outdir))


def restore(indir):
    import os
    fs = lib.s3fs_client()
    n = 0
    for root, _, files in os.walk(indir):
        for f in files:
            p = os.path.join(root, f)
            fs.put(p, "s3://" + os.path.relpath(p, indir))
            n += 1
    print("restored %d objects from %s" % (n, indir))


def wait_compacted(timeout=300):
    fs = lib.s3fs_client()
    raw = set(k.split("/", 1)[1] for k in parquet_keys(fs, "obs-raw"))
    deadline = time.time() + timeout
    while True:
        fs.invalidate_cache()
        cur = set(k.split("/", 1)[1] for k in parquet_keys(fs, "obs-archive"))
        gone = len(raw - cur)
        print("compaction: %d of %d raw objects replaced, %d objects now" % (gone, len(raw), len(cur)))
        # Stable for two polls and something was compacted for every tenant/signal group.
        groups_rewritten = {"/".join(k.split("/")[:3]) for k in (raw - cur)}
        want_groups = {"%s/%s" % (t["prefix"], s) for t in lib.TENANTS.values() for s in lib.SIGNALS}
        if want_groups <= groups_rewritten:
            time.sleep(10)
            fs.invalidate_cache()
            cur2 = set(k.split("/", 1)[1] for k in parquet_keys(fs, "obs-archive"))
            if cur2 == cur:
                return
        if time.time() > deadline:
            sys.exit("compaction did not finish within %ds (groups rewritten: %s)" % (timeout, sorted(groups_rewritten)))
        time.sleep(10)


POISON_DT = "2099-12-31"
PRUNE_PREFIX = "prune/1"


def make_prune_fixture(oracle_path):
    """A tenant prefix that holds one real day of the compacted layer plus a partition whose only
    object is garbage. A query that filters on dt and succeeds never opened the garbage object, so
    its engine pruned the partition; an unfiltered query on the same prefix must fail (the control)."""
    import json
    fs = lib.s3fs_client()
    oracle = json.load(open(oracle_path))
    for sig in lib.SIGNALS:
        dt = oracle["params"]["numeric/" + sig]["dt"]
        src = "obs-archive/%s/%s/dt=%s/" % (lib.TENANTS["numeric"]["prefix"], sig, dt)
        keys = [k for k in fs.find(src) if k.endswith(".parquet")]
        assert keys, "no objects under " + src
        for k in keys:
            fs.copy("s3://" + k, "s3://obs-archive/%s/%s/%s" % (PRUNE_PREFIX, sig, k.split("/%s/" % sig, 1)[1]))
        fs.pipe("obs-archive/%s/%s/dt=%s/hour=00/poison.parquet" % (PRUNE_PREFIX, sig, POISON_DT),
                b"PAR1 this is not a parquet file, any engine that opens it fails PAR1")
        print("prune fixture %s: %d real objects of dt=%s + 1 poisoned partition" % (sig, len(keys), dt))


def inventory():
    fs = lib.s3fs_client()
    for layer, bucket in lib.BUCKET.items():
        keys = [k for k in parquet_keys(fs, bucket) if not k.startswith("%s/%s/" % (bucket, PRUNE_PREFIX))]
        for grp, ks in sorted(per_group(keys).items()):
            print("%-9s %-22s files=%-4d rows=%d" % (layer, grp, len(ks), rows_in(fs, ks)))


if __name__ == "__main__":
    {"wait-flushed": wait_flushed, "snapshot-raw": snapshot_raw, "wait-compacted": wait_compacted,
     "inventory": inventory, "verify-compacted": verify_compacted,
     "dump": lambda: dump(sys.argv[2]), "restore": lambda: restore(sys.argv[2]),
     "make-prune-fixture": lambda: make_prune_fixture(sys.argv[2])}[sys.argv[1]]()
