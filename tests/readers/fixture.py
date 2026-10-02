#!/usr/bin/env python3
"""Fixture steps of the reader matrix that need an S3 client.

  fixture.py wait-flushed      wait until every tenant's Parquet rows equal Lakehouse's own count
  fixture.py snapshot-raw      copy every object of obs-archive to obs-raw (the raw layer, as Lakehouse left it)
  fixture.py verify-layers     the raw layer holds no compacted-* object and differs from the compacted layer
  fixture.py snapshot-archive  record the objects and compaction counters once every group is compacted
  fixture.py storage-health    the scans after that changed nothing (#343)
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


def buffer_state(base):
    """What the insert buffer of one binary says about itself: rows admitted and not yet written to S3, and its
    segments by state (active: the open one, always present; pending: sealed, not yet committed; committed)."""
    import urllib.request
    with urllib.request.urlopen(base + "/metrics", timeout=20) as r:
        text = r.read().decode()
    rows, segs = 0.0, {}
    for line in text.splitlines():
        if line.startswith("lakehouse_buffer_pending_rows "):
            rows += float(line.rsplit(" ", 1)[1])
        elif line.startswith("lakehouse_buffer_segments{"):
            state = line.split('state="', 1)[1].split('"', 1)[0]
            segs[state] = segs.get(state, 0.0) + float(line.rsplit(" ", 1)[1])
    return rows, segs


def buffer_drained():
    """True when both binaries' buffers hold no row that is not in S3: no pending rows and no sealed-but-uncommitted
    segment (there is always one empty active segment, so `active` is not looked at). The first Parquet object
    appearing is not 'flushed': a segment is written completely, per tenant, over several objects, and rows of the
    next segment may still be in the buffer."""
    bad = []
    for sig, base in (("logs", lib.LOGS_URL), ("traces", lib.TRACES_URL)):
        rows, segs = buffer_state(base)
        if rows or segs.get("pending"):
            bad.append("%s: pending_rows=%d segments=%s" % (sig, rows, segs))
    return bad


def wait_flushed(timeout=1200):
    """Wait until, twice in a row, both insert buffers are drained AND every tenant/signal's Parquet rows equal
    what Lakehouse itself answers. Lakehouse's count is re-read on every poll: while the buffer is flushing it
    serves the live segment's rows next to the objects already written. The insert buffer drains one segment at a time,
    one object per tenant, partition and slice: with five tenants over 48 hours that is minutes, not seconds."""
    fs = lib.s3fs_client()
    deadline = time.time() + timeout
    agreed = 0
    while True:
        fs.invalidate_cache()
        undrained = buffer_drained()
        want = lh_counts()
        keys = parquet_keys(fs, "obs-archive")
        bad = []
        for (prefix, sig), n in want.items():
            ks = [k for k in keys if k.startswith("obs-archive/%s/%s/" % (prefix, sig))]
            got = rows_in(fs, ks)
            if got != n:
                bad.append((prefix, sig, got, n))
        agreed = agreed + 1 if not bad and not undrained else 0
        if agreed >= 2:
            print("flushed: both insert buffers are drained and the Parquet rows equal Lakehouse's rows for all %d tenant/signal pairs" % len(want), flush=True)
            return
        if time.time() > deadline:
            sys.exit("not flushed after %ds: buffer %s; parquet rows differ for %s" % (timeout, undrained, bad))
        time.sleep(5)


def all_keys(fs, bucket):
    return sorted(k for k in fs.find(bucket))


def snapshot_raw():
    """The raw layer is the bucket as Lakehouse left it before compaction: every object, the
    `_pmeta.bundle`, `_meta/` and `_tombstones/` ones included, not only the Parquet files."""
    fs = lib.s3fs_client()
    keys = all_keys(fs, "obs-archive")
    for k in keys:
        fs.copy("s3://" + k, "s3://obs-raw/" + k.split("/", 1)[1])
    print("raw layer: copied %d objects (%d Parquet) to obs-raw" % (len(keys), len([k for k in keys if k.endswith(".parquet")])))


def relative(keys, bucket):
    return {k[len(bucket) + 1:] for k in keys}


def verify_layers():
    """The raw and the compacted layer are structurally different, so a matrix that reads the wrong
    one cannot pass by accident: obs-raw holds no compacted-* object, obs-archive holds compacted-*
    objects, and for every tenant/signal group the sets of Parquet objects differ."""
    fs = lib.s3fs_client()
    fs.invalidate_cache()
    raw = relative(parquet_keys(fs, "obs-raw"), "obs-raw")
    comp = relative(parquet_keys(fs, "obs-archive"), "obs-archive")
    comp = {k for k in comp if not k.startswith(PRUNE_PREFIX + "/")}
    bad = []
    leaked = sorted(k for k in raw if "/compacted-" in k)
    if leaked:
        bad.append("obs-raw holds compacted objects: %s" % leaked[:3])
    for t in lib.TENANTS.values():
        for sig in lib.SIGNALS:
            pre = "%s/%s/" % (t["prefix"], sig)
            r, c = {k for k in raw if k.startswith(pre)}, {k for k in comp if k.startswith(pre)}
            if not any("/compacted-" in k for k in c):
                bad.append("%s: the compacted layer holds no compacted-* object" % pre)
            if r == c:
                bad.append("%s: the raw and the compacted layer hold the same objects" % pre)
            if not (r - c):
                bad.append("%s: compaction removed none of the raw objects" % pre)
    if bad:
        sys.exit("raw and compacted layers are not structurally different: %s" % bad)
    print("layers: obs-raw has %d objects and no compacted-*, obs-archive %d objects incl. compacted-*; the sets differ for every tenant/signal" % (len(raw), len(comp)))


def metric(base, name):
    import urllib.request
    with urllib.request.urlopen(base + "/metrics", timeout=20) as r:
        text = r.read().decode()
    total = 0.0
    for line in text.splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            total += float(line.rsplit(" ", 1)[1])
    return total


def compaction_counters():
    out = {}
    for sig, base in (("logs", lib.LOGS_URL), ("traces", lib.TRACES_URL)):
        out[sig] = {n: metric(base, "lakehouse_compaction_" + n) for n in ("runs_total", "files_input_total", "files_output_total")}
    return out


def snapshot_archive(path):
    import json
    fs = lib.s3fs_client()
    fs.invalidate_cache()
    lib.write_json(path, {"objects": sorted(parquet_keys(fs, "obs-archive")), "counters": compaction_counters()})
    print("snapshot after the first productive scan: %s" % path)


def health_violations(before, now, new_scans_logs, new_scans_traces):
    """Why the scans after convergence were not no-ops (empty when they were): a scan reported compactions,
    a compaction counter moved, or the Parquet objects changed."""
    v = []
    for sig, n in (("logs", new_scans_logs), ("traces", new_scans_traces)):
        if n != 0:
            v.append("%s: %d scans after the compaction converged reported compactions again (a planner that rewrites what it just wrote)" % (sig, n))
        for k, val in before["counters"][sig].items():
            if now["counters"][sig][k] != val:
                v.append("%s: lakehouse_compaction_%s moved from %s to %s after convergence" % (sig, k, val, now["counters"][sig][k]))
    if now["objects"] != before["objects"]:
        v.append("the Parquet objects changed after convergence: %d added, %d removed" % (
            len(set(now["objects"]) - set(before["objects"])), len(set(before["objects"]) - set(now["objects"]))))
    return v


def storage_health(before_path, scans_logs, scans_traces, out_path):
    """#343 (fixed by #347): compaction counted the files of every tenant of a partition together, so
    a scan kept rewriting what it had just written. Once every group is compacted, later scans must
    do nothing: no scan that compacts, no run, no input or output file, the same Parquet objects."""
    import json
    before = lib.read_json(before_path)
    fs = lib.s3fs_client()
    fs.invalidate_cache()
    now = {"objects": sorted(parquet_keys(fs, "obs-archive")), "counters": compaction_counters()}
    res = {"new_scans_with_compactions": {"logs": scans_logs, "traces": scans_traces}, "objects_before": len(before["objects"]),
           "objects_after": len(now["objects"]), "counters_before": before["counters"], "counters_after": now["counters"],
           "violations": health_violations(before, now, scans_logs, scans_traces)}
    res["ok"] = not res["violations"]
    lib.write_json(out_path, res, indent=1)
    if res["violations"]:
        sys.exit("storage health (#343): the scans after convergence were not no-ops: %s" % res["violations"])
    print("storage health: the scans after convergence did 0 compactions (counters and %d objects unchanged)" % len(now["objects"]))


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
        for k in all_keys(fs, bucket):
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
    oracle = lib.read_json(oracle_path)
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
     "inventory": inventory, "verify-compacted": verify_compacted, "verify-layers": verify_layers,
     "snapshot-archive": lambda: snapshot_archive(sys.argv[2]),
     "storage-health": lambda: storage_health(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]),
     "dump": lambda: dump(sys.argv[2]), "restore": lambda: restore(sys.argv[2]),
     "make-prune-fixture": lambda: make_prune_fixture(sys.argv[2])}[sys.argv[1]]()
