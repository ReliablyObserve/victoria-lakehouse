# Comparative Benchmark — CORRECTED Results — 2026-06-05

The original `perf-bench-results.csv` had two measurement bugs that
made ClickHouse look artificially fast and skewed several scenarios.
This document captures the corrected run.

## Bugs in the original bench

1. **Wrong ClickHouse table names** — bench used `logs_all_tenants` and
   `traces_all_tenants` which don't exist in the deployed schema.
   Every CH query returned `UNKNOWN_TABLE` errors at ~77ms. That 77ms
   was the error response time, not real query work.

2. **No post-restart warm-wait** — VT's Jaeger search excludes traces
   newer than 30s (`LatencyOffset`, by design — waits for in-flight
   completion). When the bench started immediately after pod restart,
   ALL recent data fell within the cutoff and search returned 0 traces
   in ~60-byte empty envelopes. The "161ms LH cold" was 161ms of
   returning nothing.

## Corrected bench

- Use the real views: `lakehouse.otel_logs` and `lakehouse.otel_traces`
  (from `init-s3.sql`).
- 60s warm-wait before measuring, so spans settle past `LatencyOffset`.
- Sanity-check phase confirms each engine returns non-zero before
  the measured iterations begin.

| Scenario | Engine | n | p50 | p95 | p99 |
|---|---|---|---|---|---|
| deps | lh_cold_traces | 30 | **44** | 63 | 79 |
| deps | vt_hot | 30 | 53 | 86 | 112 |
| log_count_1h | lh_cold_logs | 35 | **33** | 44 | 51 |
| log_count_1h | vl_hot | 35 | 62 | 88 | 134 |
| log_count_1h | clickhouse | 35 | 176 | 199 | 211 |
| span_count_1h | lh_cold_traces | 30 | 125 | 145 | 153 |
| span_count_1h | vt_hot | 30 | **54** | 64 | 69 |
| span_count_1h | clickhouse | 30 | 168 | 227 | 254 |
| trace_search | lh_cold_traces | 35 | 158 | 192 | 229 |
| trace_search | vt_hot | 35 | **55** | 67 | 71 |
| trace_search | clickhouse | 30 | 161 | 180 | 180 |

## Relative ratios (cold = baseline)

- **deps**: vt_hot 53ms vs cold 44ms → **1.20×** (cold wins)
- **log_count_1h**: vl_hot 62ms → 1.88× / clickhouse 176ms → **5.33×** (cold wins big)
- **span_count_1h**: vt_hot 54ms → 0.44× (VT wins) / clickhouse 168ms → 1.35× (cold wins vs CH)
- **trace_search**: vt_hot 55ms → 0.35× (VT wins) / clickhouse 161ms → 1.02× (cold ≈ CH)

## Interpretation

**LH cold-tier wins where in-memory state matters:**
- `log_count_1h` (5.3× vs CH): the incremental `tenantAggregates`
  cache (PB-scale fix #2) serves this from memory, no S3 reads.
- `deps`: SG edges are pre-aggregated at write time, so the read
  is a fast LogsQL aggregation.

**LH cold-tier matches ClickHouse on raw Parquet workloads:**
- `span_count_1h` (1.35× faster than CH): our reader's bloom +
  footer prefetch + columnar projection is on par with CH's
  s3() function over the same files.
- `trace_search` (1.02× near parity with CH): combined two-phase
  search (trace-ID enumeration + span fetch) lands in the same
  ballpark as a single distinct-count aggregation.

**LH cold-tier loses to VT hot (~3× slower) on data-bound workloads:**
- This is the expected cold-vs-hot penalty. VT hot has every span
  in memory; LH cold has to open Parquet files from S3. No way to
  close that gap without caching everything in memory (which
  defeats the cold tier's reason for existing).

## What's NOT exercised

- Real cloud S3 with realistic tail latency (40ms p50, 600ms p99, 5s+ outliers)
- Multi-region eventual-consistency
- >10K active tenants
- >1M file manifest
- Concurrent read load (this run is sequential)

Those would shift the LH-cold-vs-CH comparison: CH at PB-scale tends
to scan more bytes per query because its bloom indexes are per-row-
group, not per-file like ours. Our footer-prefetch advantage should
widen at higher file counts.
