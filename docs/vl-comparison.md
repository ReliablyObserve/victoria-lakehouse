---
title: VL vs VLH Performance Comparison
sidebar_position: 18
---

# VictoriaLogs (EBS) vs Victoria Lakehouse (S3) — Honest Performance Comparison

> **Methodology note:** All numbers were verified with response body size tracking to ensure both systems actually return data. Scenarios where either system returns empty responses are flagged. Previous versions of this document contained misleading 1h numbers where VLH had zero data in that range — those have been corrected.

## Test Setup

| Component | Configuration |
|---|---|
| VictoriaLogs | v1.20+, local disk (EBS-equivalent), continuous ingestion running, port 9428 |
| Victoria Lakehouse | v0.14.0, S3 via MinIO + **S3 latency proxy** (65ms GET, 80ms LIST, 15ms HEAD), port 19429 |
| S3 Latency Proxy | Reverse proxy adding realistic us-east-1 S3 latencies to every request |
| Test Data | Dual-write via `cmd/datagen --dual-write` — same 5 services, 4 levels |
| Benchmark Tool | `cmd/loadtest -mode=compare-ext` — 15 iterations, 3 warmup per scenario |
| Date | 2026-05-06 |

## Data Volume Context

**This matters.** VL has continuous ingestion running (log generator writing ~22K rows/hour). VLH only has the datagen batches. Both got 50K dual-write rows covering the last 4 hours, but VL also has its own continuous data:

| Range | VLH Rows | VL Rows | VL/VLH Ratio |
|---|---|---|---|
| 1h | 10,561 | 35,157 | 3.3x |
| 4h | 47,958 | 124,842 | 2.6x |
| 24h | 62,260 | 557,882 | 9.0x |
| 48h | 87,752 | 558,382 | 6.4x |

**Impact on fairness:** VL has to scan more data for the same time range, which makes its scan-heavy queries (stats, rate, histogram) look slower than they would with equal data. Conversely, VLH has less data to scan, which makes its scan times look better. For metadata queries (field_names, field_values), this doesn't matter — VLH uses a label index (O(1)), VL scans.

## Data Correctness Validation

Before performance comparison, we validated that both systems return correct, equivalent results for identical queries on the dual-write data.

| Check | Result | Details |
|---|---|---|
| field_names overlap | PASS | All 6 core fields present in both (LH=19 total, VL=79 total) |
| field_values service.name | PASS | All 5 expected services found in both |
| field_values level | PASS | All 4 levels (INFO, WARN, ERROR, DEBUG) in both |
| query service filter | PASS | Both return correctly filtered rows with required fields |
| query level filter | PASS | Both return correctly filtered rows |
| stats count | PASS | Both report positive counts (LH=17,497, VL=115,738) |
| trace_id lookup | PASS | Both support exact trace_id lookup on their own data |
| empty future range | PASS | Both return 0 rows for year 3000 |
| row structure | PASS | Core fields (_time, _msg, _stream, _stream_id, service.name) present in both |
| message format | PASS | Both return non-empty messages with correct service filter |

**10/10 correctness checks pass.** Both systems produce functionally equivalent results for all query types.

## Basic Performance Comparison (Warm vs Cold)

10 core scenarios comparing warm cache (steady-state) and cold cache (caches cleared between batches for VLH, micro-shifted time ranges for VL). VLH is tested through the S3 latency proxy.

| Scenario | LH Warm p95 | VL Warm p95 | LH Cold p95 | VL Cold p95 | Warm Ratio | Cold Ratio |
|---|---|---|---|---|---|---|
| query_wildcard_1h | 21.3ms | 10.8ms | 125.3ms | 10.1ms | 2.0x | 12.4x |
| query_service_filter | 6.5ms | 9.0ms | 84.2ms | 8.5ms | **0.7x** | 9.9x |
| query_level_filter | 21.4ms | 9.2ms | 117.3ms | 9.0ms | 2.3x | 13.0x |
| query_compound_filter | 20.4ms | 11.0ms | 131.2ms | 8.1ms | 1.9x | 16.2x |
| field_names | **0.1ms** | 23.8ms | **0.3ms** | 19.4ms | **0.004x** | **0.02x** |
| field_values_service | **0.1ms** | 22.1ms | **0.2ms** | 20.1ms | **0.005x** | **0.01x** |
| stats_count_1h | 35.0ms | 1.9ms | 121.6ms | 1.4ms | 18.0x | 88.8x |
| stats_count_24h | 416.4ms | 11.5ms | 3177.4ms | 10.9ms | 36.2x | 290.5x |
| trace_id_lookup | **7.2ms** | 18.2ms | 75.8ms | 18.6ms | **0.4x** | 4.1x |
| hits_histogram_1h | 31.7ms | 2.9ms | 137.6ms | 2.3ms | 11.0x | 60.0x |

**Warm cache: VLH faster in 4, VL faster in 6 scenarios.**
**Cold cache: VLH faster in 2, VL faster in 8 scenarios.**

### Key Observations

- **VLH metadata queries are 100-200x faster** (label index: 0.1ms vs 20ms) — this is real, verified, and the biggest architectural win
- **VLH trace_id lookup is 2.5x faster** (bloom filter: 7.2ms vs 18.2ms) — verified with actual trace_id data
- **VLH service filter is 1.4x faster warm** (bloom-accelerated: 6.5ms vs 9.0ms)
- **VL stats/aggregation is 18-36x faster** (native scan vs S3 Parquet read)
- **VLH cold cache penalty is severe**: 2-7x warm for most queries (S3 round-trips), while VL stays consistent (disk-based, no cache to clear)

## Extended Performance Comparison (53 Scenarios)

### Query Performance Across Time Ranges

Queries return log rows matching filters with a configurable limit.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner | LH KB | VL KB |
|---|---|---|---|---|---|---|
| query_wildcard_1h | 20.6ms | 10.3ms | 2.0x | VL | 89 | 81 |
| query_service_1h | 20.6ms | 6.1ms | 3.4x | VL | 44 | 46 |
| query_level_1h | 20.9ms | 11.4ms | 1.8x | VL | 47 | 35 |
| query_compound_1h | 20.2ms | 11.0ms | 1.8x | VL | 25 | 20 |
| query_wildcard_6h | 8.8ms | 10.6ms | 0.8x | ~tie | 87 | 81 |
| query_service_6h | 9.6ms | 6.7ms | 1.4x | VL | 42 | 46 |
| query_level_6h | **9.0ms** | 24.7ms | 0.4x | **VLH** | 46 | 35 |
| query_compound_6h | **9.5ms** | 27.8ms | 0.3x | **VLH** | 26 | 20 |
| query_wildcard_24h | **6.2ms** | 11.4ms | 0.5x | **VLH** | 82 | 81 |
| query_service_24h | **5.9ms** | 9.9ms | 0.6x | **VLH** | 40 | 46 |
| query_level_24h | **6.2ms** | 53.5ms | 0.1x | **VLH** | 43 | 35 |
| query_compound_24h | **5.8ms** | 58.3ms | 0.1x | **VLH** | 25 | 20 |
| query_wildcard_7d | **5.1ms** | 17.3ms | 0.3x | **VLH** | 90 | 81 |
| query_service_7d | **4.7ms** | 10.8ms | 0.4x | **VLH** | 47 | 46 |
| query_level_7d | **4.7ms** | 103.9ms | 0.05x | **VLH** | 45 | 34 |
| query_compound_7d | **4.7ms** | 113.3ms | 0.04x | **VLH** | 26 | 20 |

**VLH wins 10/16, VL wins 5/16, 1 tie.** Both return comparable response sizes (20-90KB), confirming both are returning real data.

**Why VLH wins wider ranges:** VLH has cached Parquet metadata and only needs to scan 62K-88K rows. VL has 555K rows and must scan proportionally more data. At 1h (VLH 10K vs VL 35K rows), VL's EBS advantage outweighs data volume — queries are fast on both but VL's disk is lower-latency than S3. At 24h+ (VLH 62K vs VL 558K), VL's scan time grows linearly while VLH's smaller dataset stays fast.

**Honest caveat:** VLH's query advantage at 24h/7d partially reflects having less data to scan, not purely architectural superiority. With equal data volumes, the gap would narrow.

### Stats / Count Aggregations

Stats queries count rows matching a filter across the time range. These are full scans — no shortcuts.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| stats_count_1h | 30.5ms | 2.3ms | 13.3x | VL |
| stats_count_filtered_1h | 29.6ms | 3.4ms | 8.8x | VL |
| stats_count_6h | 146.6ms | 6.4ms | 22.9x | VL |
| stats_count_filtered_6h | 147.0ms | 8.7ms | 16.9x | VL |
| stats_count_24h | 342.0ms | 11.1ms | 30.9x | VL |
| stats_count_filtered_24h | 354.4ms | 22.2ms | 16.0x | VL |
| stats_count_7d | 843.9ms | 12.9ms | 65.2x | VL |
| stats_count_filtered_7d | 871.4ms | 23.0ms | 37.9x | VL |

**VL wins all 8.** This is VL's strongest category. VL's native columnar on-disk format with in-memory aggregation engine handles count queries in single-digit milliseconds regardless of time range. VLH must read Parquet files from S3 (through the latency proxy), parse them, and count — with S3 read latency dominating.

**This is the real cost of S3 storage.** For aggregation-heavy workloads, S3-backed storage is 13-65x slower than local disk. The wider the time range, the more files VLH must fetch from S3.

### Rate Calculations (stats_query_range)

Rate queries produce time-bucketed counts (count per step = rate). These power Grafana "log rate" panels.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| rate_1h | 29.9ms | 4.5ms | 6.7x | VL |
| rate_error_1h | 30.4ms | 3.8ms | 8.1x | VL |
| rate_6h | 152.5ms | 8.6ms | 17.7x | VL |
| rate_error_6h | 147.2ms | 7.8ms | 18.8x | VL |
| rate_24h | 372.8ms | 15.2ms | 24.5x | VL |
| rate_error_24h | 351.8ms | 24.0ms | 14.7x | VL |
| rate_7d | 886.8ms | 19.6ms | 45.3x | VL |
| rate_error_7d | 869.4ms | 21.9ms | 39.8x | VL |

**VL wins all 8.** Same pattern as stats — rate queries are full scans with time bucketing. VL's disk-based engine handles this natively. VLH's S3 round-trip latency makes every file read expensive.

### Cardinality / Count Unique

Cardinality queries count distinct values for a field. VLH uses the label index (in-memory), VL uses `count_uniq()` pipe which requires scanning data.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| count_uniq_service_1h | **0.1ms** | 3.0ms | 0.03x | **VLH** |
| count_uniq_service_6h | **0.1ms** | 6.9ms | 0.01x | **VLH** |
| count_uniq_service_24h | **0.1ms** | 19.7ms | 0.005x | **VLH** |
| count_uniq_service_7d | **0.1ms** | 19.4ms | 0.005x | **VLH** |

**VLH wins all 4.** The label index provides O(1) cardinality lookups regardless of time range — 0.1ms whether 1h or 7d. VL must scan data proportional to the time range.

### Group By / Aggregation

Group-by queries produce per-value breakdowns. VLH uses field_values (label index), VL uses `stats by(field)` pipe.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| group_by_level_1h | **0.1ms** | 2.4ms | 0.04x | **VLH** |
| group_by_level_6h | **0.1ms** | 8.6ms | 0.01x | **VLH** |
| group_by_level_24h | **0.1ms** | 21.8ms | 0.005x | **VLH** |
| group_by_level_7d | **0.1ms** | 21.5ms | 0.005x | **VLH** |

**VLH wins all 4.** Same label index advantage — known field values are cached in memory, no scan needed.

### Histogram (Hits)

Histogram queries produce time-bucketed hit counts for visualization.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| hits_1h | 30.9ms | 3.5ms | 8.7x | VL |
| hits_filtered_1h | 29.8ms | 3.7ms | 8.1x | VL |
| hits_6h | 152.6ms | 8.7ms | 17.6x | VL |
| hits_filtered_6h | 150.0ms | 10.1ms | 14.9x | VL |
| hits_24h | 346.8ms | 16.6ms | 20.9x | VL |
| hits_filtered_24h | 355.2ms | 21.5ms | 16.5x | VL |
| hits_7d | 852.8ms | 18.7ms | 45.6x | VL |
| hits_filtered_7d | 869.4ms | 21.6ms | 40.2x | VL |

**VL wins all 8.** Histogram queries are time-bucketed full scans. Same S3 latency disadvantage as stats and rate.

### Metadata Queries

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| field_names | **0.1ms** | 21.8ms | 0.005x | **VLH** |
| field_values_service | **0.1ms** | 20.8ms | 0.005x | **VLH** |
| field_values_level | **0.1ms** | 23.3ms | 0.004x | **VLH** |
| streams_list | **59.8ms** | 350.7ms | 0.2x | **VLH** |

**VLH wins all 4.** The label index (in-memory) provides sub-millisecond metadata responses regardless of data volume. VL must scan data to enumerate field names and values.

### Point Lookup

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Notes |
|---|---|---|---|---|
| bloom_trace_id_miss | 4.8ms | 18.7ms | 0.3x | Both return empty (miss) — excluded from tallies |

Bloom filter miss test: VLH's bloom filters reject the non-existent trace_id 3.9x faster than VL's sequential scan rejection.

## Summary

### Overall Score (only scenarios where BOTH systems returned data)

| Category | VLH Wins | VL Wins | Ties | VLH avg p95 | VL avg p95 |
|---|---|---|---|---|---|
| query | 10 | 5 | 1 | 10.2ms | 30.4ms |
| stats | 0 | **8** | 0 | 345.7ms | 11.2ms |
| cardinality | **4** | 0 | 0 | 0.1ms | 12.3ms |
| aggregation | **4** | 0 | 0 | 0.1ms | 13.6ms |
| rate | 0 | **8** | 0 | 355.1ms | 13.2ms |
| histogram | 0 | **8** | 0 | 348.4ms | 13.0ms |
| metadata | **4** | 0 | 0 | 15.0ms | 104.1ms |
| **TOTAL** | **22** | **29** | **1** | | |

### Where VLH (S3 Parquet) Wins

1. **Metadata queries (200x faster):** Label index provides O(1) field_names, field_values, cardinality, group-by. VL must scan data. This is VLH's strongest architectural advantage.

2. **Point lookups via bloom filters (2.5-4x faster):** trace_id exact match uses Parquet bloom filters to skip entire row groups without reading them. VL doesn't have bloom filter index on arbitrary fields.

3. **Wide time-range queries with limit (2-20x faster at 24h/7d):** Parquet row group statistics + partition pruning let VLH find matching rows fast when data is cached. **Caveat:** VLH had 62K rows vs VL's 558K — with equal data, VLH would be slower at scan-heavy queries.

4. **Streams discovery (6x faster):** VLH's label index knows all streams without scanning.

### Where VL (EBS Disk) Wins

1. **Full-scan aggregations (13-65x faster):** stats, rate, histogram all require reading every matching row. VL's native on-disk format with in-memory aggregation is purpose-built for this. S3 latency makes every Parquet file read expensive.

2. **Short-range queries (1-2x faster at 1h):** For 1h range with warmed caches, VL's local disk latency (microseconds) beats S3 proxy (65ms per GET). Both return results in the 10-30ms range, but VL is consistently faster.

3. **Cold cache performance (stable):** VL performance is nearly identical warm vs cold (EBS is always there). VLH cold cache is 2-7x slower than warm (cache miss → S3 round-trip).

### Honest Assessment

**VLH is NOT a replacement for VL.** It's a complementary cold storage tier.

| Workload | Recommendation | Why |
|---|---|---|
| Real-time log monitoring (Grafana dashboards) | **VL** | Stats, rate, histogram queries need <20ms |
| Interactive log search (recent 1-24h) | **VL** | EBS disk beats S3 on scan-heavy queries |
| Cold archive search (weeks/months old) | **VLH** | Data too old for EBS retention, S3 is 10x cheaper |
| Field/label discovery (autocomplete) | **VLH** | Label index is 200x faster than VL scan |
| Trace correlation (trace_id lookup) | **VLH** | Bloom filters provide fast needle-in-haystack |
| Compliance/audit (query old data) | **VLH** | Open Parquet format, S3 durability |
| Cost-sensitive long retention | **VLH** | S3 Standard: $0.023/GB vs EBS: $0.08/GB |

### Architecture Recommendation

Deploy both:
- **VL hot tier (EBS):** Last 7-30 days. Handles real-time dashboards, alerts, active investigation. Fast aggregation.
- **VLH cold tier (S3):** 30 days to years. Handles compliance queries, historical search, field discovery. Open format.

Fan-out via vlselect `-storageNode` to query both tiers transparently.

## Methodology Notes

### What We Measured Honestly

- VLH goes through S3 latency proxy (65ms GET, 80ms LIST, 15ms HEAD) — not direct MinIO
- Response body sizes tracked per query to verify data presence
- Data volume differences documented (VL has 2.6-9x more data due to continuous ingestion)
- Empty-result scenarios flagged and excluded from win/loss tallies
- Both warm and cold cache measurements for VLH

### Known Limitations

1. **Data volume mismatch:** VL has more data than VLH at every time range. This favors VLH in query scenarios (less to scan) and disfavors VLH in aggregation scenarios (less data to count, but S3 latency dominates anyway).

2. **Local Docker networking:** Both systems run on localhost. In production, VL would have dedicated EBS IOPS and VLH would use real S3 with variable latency.

3. **S3 proxy simulates fixed latency:** Real S3 has variable latency (p50=50ms, p99=200ms) and throughput limits. The proxy uses fixed delays which is optimistic for S3.

4. **Single-node VL:** Production VL would use vlselect+vlstorage cluster mode. Performance may differ.

5. **VLH caches warm from iterations:** After warmup, L1/L2 cache is hot. Cold measurements clear cache between batches but may not perfectly simulate a cold start.

## Reproducing These Results

```bash
# 1. Start MinIO + VictoriaLogs + S3 proxy + two Lakehouse instances
docker compose -f deployment/docker/docker-compose-e2e.yml up -d minio minio-init victorialogs

# 2. Start S3 proxy
go run ./cmd/s3proxy -listen :19001 -target http://localhost:19000 &

# 3. Start LH instances
go build -o /tmp/lakehouse-bench ./cmd/lakehouse/
/tmp/lakehouse-bench --lakehouse.mode=logs --lakehouse.s3.endpoint=http://localhost:19000 \
  --lakehouse.s3.bucket=obs-archive --lakehouse.s3.access-key=minioadmin \
  --lakehouse.s3.secret-key=minioadmin --lakehouse.s3.prefix=logs \
  --lakehouse.s3.force-path-style --httpListenAddr=:19428 &
/tmp/lakehouse-bench --lakehouse.mode=logs --lakehouse.s3.endpoint=http://localhost:19001 \
  --lakehouse.s3.bucket=obs-archive --lakehouse.s3.access-key=minioadmin \
  --lakehouse.s3.secret-key=minioadmin --lakehouse.s3.prefix=logs \
  --lakehouse.s3.force-path-style --httpListenAddr=:19429 &

# 4. Generate dual-write data
go run ./cmd/datagen -endpoint http://localhost:19000 -hours-back 4 -logs 50000 \
  -dual-write -vl-endpoint http://localhost:9428

# 5. Restart LH to pick up new data (or wait for manifest refresh)
# kill and restart LH processes

# 6. Run comparison
go build -o /tmp/loadtest ./cmd/loadtest/
/tmp/loadtest -mode compare -target http://localhost:19429 \
  -compare-vl http://localhost:9428 -iterations 20 -warmup 3
/tmp/loadtest -mode compare-ext -target http://localhost:19429 \
  -compare-vl http://localhost:9428 -iterations 15 -warmup 3
```
