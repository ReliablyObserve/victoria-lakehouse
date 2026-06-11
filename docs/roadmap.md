# Roadmap

What is shipped and proven, what is in progress, and what is committed next.
Every "proven" claim links to results reproducible with the in-repo benchmark
suite (`scripts/bench/`, methodology in [benchmarks](benchmarks/full-scope-s3.md)).

## Shipped & proven

- **Unified partition metadata layer** — one metadata object per partition serving
  file statistics, field/value catalogs, and bloom pruning. Measured: metadata queries
  (field names/values) 2–10× faster than hot in-memory engines; cold Parquet scans at
  hot-engine parity without injected latency. See [full-scope results](benchmarks/full-scope-s3.md).
- **Counts and grouped counts served from metadata** — `count()`, `count() by (field)`,
  and single-field filtered counts answer from partition metadata with near-zero S3
  reads. Measured at 100 ms injected object-store latency: 2–3× faster than
  ClickHouse-over-S3 on the same data.
- **Multi-engine Parquet compatibility, enforced in CI** — every file readable by
  pyarrow and DuckDB, verified on each change (encodings, page index, row-level
  equality). See [open Parquet format](open-parquet-format.md).
- **Age-based compaction schedule** — compression level and row-group size step up as
  data ages (measured: −46% row groups on aged data at equal size).
- **Self-healing metadata** — partition metadata rebuilds from Parquet footers after
  damage or loss; compaction cycles repair gaps in historical aggregates.
- **Reliability evidence** — [soak and performance measurements](measurements/soak-and-bench-2026-06-05.md),
  [PB-scale resource sizing](architecture/pb-scale-resources-pmeta.md),
  [restart scenarios](architecture/scaling-restart-scenarios.md),
  [cost analysis](cost-comparison.md).

## In progress

- **Natively queryable insert buffer** — recently-ingested rows queryable through the
  same engine-native path as flushed data (both signals).
- **Adaptive S3 fetch strategies** — exact-range fetching for projected reads, available
  per-signal opt-in. Graduation to default is benchmark-gated: it must beat the current
  path on every scenario at both latency profiles before the default changes.

## Committed next

- **Dedicated columns for hot attributes** — promote the highest-volume attribute keys
  into first-class Parquet columns (configurable per signal). Measured on real data:
  −8% (logs) / −21% (traces) file size and order-of-magnitude faster filters on
  promoted keys.
- **Cold-open round-trip elimination** — file geometry carried in partition metadata so
  planned reads start with zero metadata round trips.
- **Storage-class tiering** — aged, compacted data transitions to cheaper S3 storage
  classes; outputs are already shaped for it (large immutable files).
- **Trace dependency pre-aggregation** — service-graph queries from precomputed edges.
- **Full-text acceleration** — n-gram pruning for message search.
- **Parallel compaction with resource caps** — higher compaction throughput under the
  same memory governors.
- **Optional low-latency metadata tier** — profile-integrated choice for single-digit-ms
  metadata reads where the deployment supports it.

Items appear here only after the decision is final and, where applicable, the
expected benefit has been measured. The benchmark gate applies to every
performance item: claims ship with reproducible numbers or they do not ship.
