# Compaction A/B on a live stack (issue #343)

Reproduces the before/after of per-tenant compaction planning on real binaries
and a real S3 server: the **main** build and the **PR** build ingest the same
data into separate buckets of one RustFS, compact on a fast cadence, and are
then compared on object layout, exact rows and query latency. Everything is
measured; nothing here is extrapolated.

Isolation: compose project `lhfix343`, ports 39700-39799 (S3 39700, logs
39701 main / 39702 PR, traces 39703 main / 39704 PR). Each instance has 2 CPUs
and 1 GiB. Do not use other projects' ports.

For a different isolated project name, pass the same name with
`docker compose -p NAME` and set `LH_COMPOSE_PROJECT` when running `snap.sh`.

## Steps

```bash
cd scripts/bench/compaction

# 1. Static linux builds of both refs into ./bin (needs docker for the arch,
#    and prepared deps/: make deps-logs deps-traces deps-vt)
./build.sh origin/main main
./build.sh HEAD pr

# 2. Bring the stack up (insert flush 5 s, compaction scan 20 s: lh.yml)
docker compose -p lhfix343 -f compose.yml up -d
for p in 39701 39702 39703 39704; do until curl -sf 127.0.0.1:$p/ready >/dev/null; do sleep 1; done; done

# 3. Ingest: 11 rounds x 7 s, both signals, 40 rows per call to main AND PR.
#    Rounds 1-4: tenants 1001 (72 h late), 1002 (30 h late), 1003 (2 h late);
#    rounds 5-11: tenant 1003 only. Writes ./out/{ingest_start,runs.tsv,acks/}
./run.sh

# 4. Let compaction work (a few scans of 20 s), then measure
sleep 120
./snap.sh   > out/snap-1.jsonl    # objects and top level per (build, signal, tenant, partition)
sleep 120
./snap.sh   > out/snap-2.jsonl    # settled? the second snapshot must equal the first
./verify.sh | tee out/verify.jsonl
./ab.py     | tee out/ab.md

# 5. Tear down (the volumes and the project's containers only; never prune)
docker compose -p lhfix343 -f compose.yml down -v --remove-orphans
```

## What each step produces

| Step | Numbers |
|---|---|
| `snap.sh` | Per (build, signal, tenant, partition): `objects`, `bytes`, `top_level`. The PR's quiet closed hours converge to one object per tenant-hour and remain stable between snapshots. This fixture places each tenant at a different hour, so it does not reproduce the cross-tenant lone-file churn; `TestScan_NoChurn_MultiTenantSingleFiles` and the multi-day simulation cover that case. Compare the two snapshots for settling. |
| `verify.sh` | Per instance `runs`, `acked`, `returned_distinct`, `missing_acked`, `duplicates`, `unacked_present`, `non200`. Compaction must not change any row: all of `missing_acked`, `duplicates`, `unacked_present`, `non200` are 0 on both builds. |
| `ab.py` | Interleaved main/PR queries (6 reps, rep 1 cold, p50 of the rest) for logs: stats count, stats by `_stream`, filtered `{run="..."}` count, raw `_msg` rows, field_names, field_values, hits 10m; for traces: stats count, `stats by ("resource_attr:service.name")`, raw `span_attr:seq, trace_id` rows, field_names, Jaeger services. Windows: the aligned hours 72 h, 30 h and 2 h before the ingest start; tenants 1001 and 1003. A shape counts only when both answers are HTTP 200 and their canonical hashes are equal; otherwise it is flagged (`non-200` / `ANSWERS DIFFER`) and the script exits non-zero. |

`durbench/` is the ingest/verify tool (stdlib only): `go build -o bin/durbench
./durbench` (run.sh and verify.sh build it on first use). Logs are sent as
jsonline with `_msg` carrying `seq=N run=R` and `run` as a stream field;
traces as OTLP JSON with `run` and `seq` span attributes; verify queries the
run and lists missing, duplicated and unacknowledged sequence numbers.

Honesty notes: the stack is a laptop-sized docker host, S3 is RustFS without
injected latency, and the data is a few hundred rows per tenant, so absolute
latencies say little; the object layout, the exactness and the settling are
what this run is for. Record the commit of each build with the numbers.
