# Soak + Performance Bench — 2026-06-05

Captures the soak-test stability run and the LH-vs-VT/VL-vs-ClickHouse
comparative benchmark executed against the e2e compose immediately
after PR #121's PB-scale + security fixes landed.

## 30-min soak with 200ms ± 50ms injected S3 latency

Driver: `scripts/soak-test.sh` issuing 8 endpoint hits per iteration,
~145 iterations completed in 1800s. Toxiproxy injected 200ms mean
50ms jitter latency between LH/lakehouse-traces/clickhouse and MinIO
for the duration.

| Endpoint | Total | ok | empty | parse_err | p50 | p99 | max |
|---|---|---|---|---|---|---|---|
| jaeger_dependencies | 145 | 145 | 0 | 0 | 1476 | 2389 | 2667 |
| jaeger_services | 145 | 145 | 0 | 0 | 5943 | 7620 | 7849 |
| jaeger_traces | 145 | 145 | 0 | 0 | 1016 | 4708 | 4873 |
| logsql_count_1h | 145 | 145 | 0 | 0 | 246 | 3108 | 3360 |
| logsql_overview | 145 | 145 | 0 | 0 | 11 | 119 | 185 |
| logsql_tenants | 145 | 145 | 0 | 0 | 11 | 40 | 41 |
| tempo_search_tags | 145 | 145 | 0 | 0 | 26 | 1881 | 3054 |
| tempo_traceql | 145 | 145 | 0 | 0 | 760 | 3096 | 3529 |

**Key findings:**

- **Zero failures across 1160 requests.** No parse errors, no empty
  responses, no connection drops. Every container stayed healthy
  for the full 30 minutes.
- **In-memory caches insulate from S3 latency.** `logsql_overview`
  and `logsql_tenants` p50=11ms even under 200ms S3 latency —
  they serve from `tenantAggregates` (PB-scale fix #2) without
  hitting S3 at all. Exactly the design goal.
- **`jaeger_services` p50=5.9s is high.** This endpoint enumerates
  services from the label index. With 200ms S3 latency each S3
  read contributes; if the label index has many S3 round-trips
  on cold startup, total latency compounds. Worth tracking; not
  a regression from this PR (predates).

## Comparative benchmark — LH vs VT/VL vs ClickHouse (no latency)

Driver: `scripts/perf-bench-lh-vs-others.sh 30` (5 warmup + 30 measured).
S3 latency cleared before the run. Each engine queried for the same
1h-window count and trace-by-service search.

| Scenario | Engine | n | p50 | p95 | p99 |
|---|---|---|---|---|---|
| deps | lh_cold_traces | 30 | 46 | 68 | 70 |
| deps | vt_hot | 30 | 52 | 63 | 66 |
| log_count_1h | lh_cold_logs | 35 | **30** | 41 | 48 |
| log_count_1h | vl_hot | 35 | 69 | 89 | 111 |
| log_count_1h | clickhouse | 35 | 79 | 90 | 94 |
| span_count_1h | lh_cold_traces | 30 | 177 | 277 | 347 |
| span_count_1h | vt_hot | 30 | **56** | 66 | 68 |
| span_count_1h | clickhouse | 30 | 77 | 195 | 270 |
| trace_search | lh_cold_traces | 35 | 161 | 242 | 277 |
| trace_search | vt_hot | 35 | **56** | 89 | 104 |

**Relative ratios (p50, cold-tier baseline):**
- `log_count_1h`: LH cold **2.30× faster than VL hot**, **2.63× faster than ClickHouse**
- `deps`: LH cold and VT hot near parity (LH 13% faster)
- `span_count_1h`: VT hot **3.16× faster** than LH cold (in-memory wins for hot data)
- `trace_search`: VT hot **2.88× faster** than LH cold (same reason)
- ClickHouse over S3 parquet: slower than VT hot, slower than LH cold-logs

**Interpretation:**

- LH cold-logs winning `log_count_1h` is a direct result of the
  incremental TenantSummaries cache landing in this PR: count()
  over the recent window is served from memory, not from S3 reads.
  VL hot has to scan its in-memory storage; ClickHouse has to read
  Parquet from S3.
- LH cold-traces losing `trace_search` and `span_count_1h` is
  expected: hot has all data in memory; cold has to fetch row
  groups from S3-backed Parquet. The ~3× penalty is normal for
  cold-tier vs hot-tier on data-bound workloads.
- ClickHouse-over-S3-parquet trace queries land between LH cold
  and VT hot — confirming our Parquet layout is comparable to
  what ClickHouse can do over the same files.

## What's exercised vs not

Both scripts hit single-process LH binaries against a single MinIO
bucket. Multi-region S3, real S3 latency profile (with the
occasional 5+ second tail), per-tenant cardinality limits, and
extreme manifest sizes (>1M files) are NOT exercised by this run.
The soak verifies stability + correctness under modest stress; the
bench gives a head-to-head on parity-relevant workloads.

For PB-scale validation, separately exercise:
1. The seed scripts (`datagen-seed-*`) at larger N to build
   a multi-million-file manifest.
2. A multi-region toxiproxy chain with the "real" tail latency
   profile (40 ms p50, 600 ms p99, occasional 5s outliers).
3. The bucket-isolation migration tool against >100 tenants.
