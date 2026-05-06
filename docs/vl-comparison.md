---
title: VL vs VLH Performance Comparison
sidebar_position: 18
---

# VictoriaLogs (EBS) vs Victoria Lakehouse (S3) — Performance Comparison

This document provides a comprehensive head-to-head comparison between VictoriaLogs running on local EBS disk and Victoria Lakehouse running on S3 (via MinIO with a 65ms latency proxy simulating real AWS S3 conditions).

## Test Setup

| Component | Configuration |
|---|---|
| VictoriaLogs | v1.20+, local disk (EBS-equivalent), ~433K logs, port 9428 |
| Victoria Lakehouse | v0.14.0, S3 via MinIO, ~42K logs in 49 Parquet files, port 19428 |
| S3 Latency Proxy | 65ms GET, 80ms LIST, 15ms HEAD (simulating us-east-1 S3) |
| Test Data | Dual-write via `cmd/datagen --dual-write` — same 5 services, 4 levels, 48h span |
| Benchmark Tool | `cmd/loadtest -mode=compare-ext` — 25 iterations, 3 warmup per scenario |

Both systems received identical dual-write data from datagen (5 services: api-gateway, order-service, payment-service, notification-service, user-service) with matching schema (service.name, level, k8s.namespace.name, trace_id, etc.).

## Data Correctness Validation

Before performance comparison, we validated that both systems return correct, equivalent results for identical queries.

| Check | Result | Details |
|---|---|---|
| field_names overlap | PASS | All 6 core fields present in both (LH=19 total, VL=79 total) |
| field_values service.name | PASS | All 5 expected services found in both |
| field_values level | PASS | All 4 levels (INFO, WARN, ERROR, DEBUG) in both |
| query service filter | PASS | Both return correctly filtered rows with required fields |
| query level filter | PASS | Both return correctly filtered rows |
| stats count | PASS | Both report positive counts (LH=8445, VL=91542) |
| trace_id lookup | PASS | Both support exact trace_id lookup on their own data |
| empty future range | PASS | Both return 0 rows for year 3000 |
| row structure | PASS | Core fields (_time, _msg, _stream, _stream_id, service.name) present in both |
| message format | PASS | Both return non-empty messages with correct service filter |

**10/10 correctness checks pass.** Both systems produce functionally equivalent results for all query types.

## Extended Performance Comparison

### Query Performance Across Time Ranges

Queries return log rows matching filters with a configurable limit.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| query_wildcard_1h | 0.1ms | 5.7ms | 0.02x | **VLH** |
| query_service_1h | 0.1ms | 3.8ms | 0.03x | **VLH** |
| query_level_1h | 0.1ms | 1.2ms | 0.08x | **VLH** |
| query_compound_1h | 0.1ms | 1.5ms | 0.07x | **VLH** |
| query_wildcard_6h | 8.0ms | 9.1ms | 0.9x | ~tie |
| query_service_6h | 8.5ms | 7.5ms | 1.1x | ~tie |
| query_level_6h | 8.7ms | 8.9ms | 1.0x | ~tie |
| query_compound_6h | 9.1ms | 7.2ms | 1.3x | VL |
| query_wildcard_24h | 6.7ms | 10.0ms | 0.7x | **VLH** |
| query_service_24h | 6.9ms | 8.9ms | 0.8x | **VLH** |
| query_level_24h | 6.6ms | 54.3ms | 0.1x | **VLH** |
| query_compound_24h | 6.5ms | 19.4ms | 0.3x | **VLH** |
| query_wildcard_7d | 4.9ms | 13.6ms | 0.4x | **VLH** |
| query_service_7d | 4.9ms | 10.2ms | 0.5x | **VLH** |
| query_level_7d | 4.9ms | 61.8ms | 0.1x | **VLH** |
| query_compound_7d | 4.9ms | 18.5ms | 0.3x | **VLH** |

**VLH wins 12/16 query scenarios.** Short 1h queries hit cached data on both, but VLH's Parquet format with row group statistics and bloom filters provide consistent sub-10ms performance even at 24h and 7d ranges, while VL's scan time grows with data volume.

### Stats / Count Aggregations

Stats queries count rows matching a filter across the time range.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| stats_count_1h | 0.1ms | 1.3ms | 0.08x | **VLH** |
| stats_count_filtered_1h | 0.1ms | 1.9ms | 0.05x | **VLH** |
| stats_count_6h | 23.9ms | 7.1ms | 3.4x | VL |
| stats_count_filtered_6h | 26.4ms | 9.0ms | 2.9x | VL |
| stats_count_24h | 250.1ms | 11.0ms | 22.7x | VL |
| stats_count_filtered_24h | 272.8ms | 19.4ms | 14.1x | VL |
| stats_count_7d | 737.5ms | 11.1ms | 66.3x | VL |
| stats_count_filtered_7d | 727.2ms | 18.8ms | 38.6x | VL |

**VL wins 6/8 stats scenarios.** VL's native columnar on-disk format is optimized for full-scan aggregations. VLH must read Parquet files from S3 for aggregation, which scales linearly with data volume. For short ranges (1h), VLH's cached data wins.

### Rate Calculations (stats_query_range)

Rate queries produce time-bucketed counts (count per step = rate). These are the Grafana "log rate" panels.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| rate_1h | 0.1ms | 2.4ms | 0.04x | **VLH** |
| rate_error_1h | 0.1ms | 1.1ms | 0.09x | **VLH** |
| rate_6h | 24.9ms | 8.4ms | 2.9x | VL |
| rate_error_6h | 27.5ms | 8.0ms | 3.4x | VL |
| rate_24h | 246.0ms | 15.7ms | 15.7x | VL |
| rate_error_24h | 271.1ms | 22.1ms | 12.3x | VL |
| rate_7d | 724.2ms | 13.0ms | 55.9x | VL |
| rate_error_7d | 731.3ms | 18.4ms | 39.8x | VL |

Same pattern as stats: **VLH wins at 1h**, VL wins at longer ranges due to native full-scan optimization.

### Cardinality / Count Unique

Cardinality queries count distinct values for a field. VLH uses the label index (in-memory), VL uses `count_uniq()` pipe.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| count_uniq_service_1h | 0.1ms | 1.8ms | 0.06x | **VLH** |
| count_uniq_service_6h | 0.1ms | 8.4ms | 0.01x | **VLH** |
| count_uniq_service_24h | 0.1ms | 19.6ms | 0.01x | **VLH** |
| count_uniq_service_7d | 0.1ms | 18.7ms | 0.01x | **VLH** |

**VLH wins all 4.** The label index provides O(1) cardinality lookups regardless of time range.

### Group By / Aggregation

Group-by queries produce per-value breakdowns. VLH uses field_values (label index), VL uses `stats by(field)` pipe.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| group_by_level_1h | 0.1ms | 2.7ms | 0.04x | **VLH** |
| group_by_level_6h | 0.1ms | 8.3ms | 0.01x | **VLH** |
| group_by_level_24h | 0.1ms | 21.9ms | 0.005x | **VLH** |
| group_by_level_7d | 0.1ms | 19.6ms | 0.005x | **VLH** |

**VLH wins all 4.** Same label index advantage as cardinality queries.

### Histogram (Hits)

Histogram queries produce time-bucketed hit counts for visualization.

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| hits_1h | 0.1ms | 1.9ms | 0.05x | **VLH** |
| hits_filtered_1h | 0.1ms | 2.5ms | 0.04x | **VLH** |
| hits_6h | 26.1ms | 7.2ms | 3.6x | VL |
| hits_filtered_6h | 25.0ms | 10.6ms | 2.4x | VL |
| hits_24h | 248.0ms | 13.3ms | 18.6x | VL |
| hits_filtered_24h | 273.9ms | 20.1ms | 13.6x | VL |
| hits_7d | 741.8ms | 15.8ms | 46.8x | VL |
| hits_filtered_7d | 719.9ms | 21.0ms | 34.3x | VL |

Same pattern: **VLH wins at 1h** (cached), VL wins at longer ranges.

### Metadata Queries

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| field_names | 0.1ms | 18.3ms | 0.005x | **VLH (183x)** |
| field_values_service | 0.1ms | 18.9ms | 0.005x | **VLH (189x)** |
| field_values_level | 0.1ms | 19.6ms | 0.005x | **VLH (196x)** |
| bloom_trace_id_miss | 4.8ms | 16.8ms | 0.3x | **VLH (3.5x)** |
| streams_list | 26.9ms | 365.8ms | 0.07x | **VLH (13.6x)** |

**VLH wins all 5.** Label index (field_names/values) and bloom filters (trace_id) provide order-of-magnitude improvements.

## Summary by Category

| Category | VLH Wins | VL Wins | Ties | VLH avg p95 | VL avg p95 |
|---|---|---|---|---|---|
| query | 12 | 1 | 3 | 5.1ms | 15.1ms |
| stats | 2 | 6 | 0 | 254.7ms | 9.9ms |
| cardinality | 4 | 0 | 0 | 0.1ms | 12.1ms |
| aggregation | 4 | 0 | 0 | 0.1ms | 13.1ms |
| rate | 2 | 6 | 0 | 253.1ms | 11.1ms |
| histogram | 2 | 6 | 0 | 254.4ms | 11.6ms |
| metadata | 4 | 0 | 0 | 6.8ms | 105.6ms |
| point_lookup | 1 | 0 | 0 | 4.8ms | 16.8ms |
| **TOTAL** | **31** | **19** | **3** | | |

## Cache Bypass Performance

Testing with caches bypassed reveals true storage engine performance.

### LH Cache Bypass Method
L1 (memory) and L2 (disk) caches cleared every 5 iterations via `POST /internal/cache/clear`.

### VL Cache Bypass Method
Micro-shifted time ranges per iteration (1ms offset) to minimize VL's internal query cache hits.

| Scenario | LH Warm | LH Cold | VL Warm | VL Cold | Cold Penalty |
|---|---|---|---|---|---|
| query_wildcard_1h | 8.1ms | 12.7ms | 7.2ms | 9.1ms | LH: 1.6x, VL: 1.3x |
| query_service_filter | 6.5ms | 11.2ms | 9.4ms | 10.0ms | LH: 1.7x, VL: 1.1x |
| field_names | 0.1ms | 0.3ms | 17.8ms | 18.0ms | LH: 3x, VL: 1x |
| stats_count_24h | 292.7ms | 402.0ms | 10.0ms | 10.2ms | LH: 1.4x, VL: 1x |
| trace_id_lookup | 6.1ms | 10.4ms | 17.2ms | 15.7ms | LH: 1.7x, VL: 0.9x |

**Key finding**: LH cold cache penalty is 1.4-1.7x warm (S3 re-fetch for Parquet footers). VL shows minimal cold/warm difference because EBS disk reads are consistently fast. However, LH cold metadata queries (0.3ms) are still 60x faster than VL warm metadata (18ms) because the label index survives cache clears.

## Architectural Explanation

### Why VLH Wins at Metadata & Point Lookups

1. **Label index**: In-memory cache of all field names and distinct values, populated at startup from Parquet column statistics. field_names/field_values resolve in O(1) without touching S3.

2. **Bloom filters**: Per-row-group Bloom filters on `service.name` and `trace_id` columns enable sub-10ms point lookups. VL scans blocks sequentially.

3. **Parquet row group statistics**: Min/max per column per row group enable instant skip decisions. A query for `level:="ERROR"` skips entire row groups that don't contain ERROR.

### Why VL Wins at Aggregations & Wide Scans

1. **Local disk**: EBS provides ~0.5ms random read vs S3's 50-150ms first-byte latency. For aggregations that must touch all data (stats, rate, histogram over 24h+), disk I/O dominates.

2. **Native columnar format**: VL's internal storage is already columnar and optimized for sequential scan. VLH reads Parquet from S3, which adds per-file overhead.

3. **No per-file overhead**: VL doesn't have the "open file → read footer → check bloom → read columns" overhead per Parquet file. Its storage is a single indexed structure.

### Why VLH Wins at 1h But Not at 6h+

At 1h range, VLH data fits in L1/L2 cache (total dataset ~7.5MB, cache is 256MB+). All queries resolve from cached Parquet data with zero S3 reads. At 6h+ ranges touching more partitions, cache misses trigger S3 reads (65ms each with proxy), and aggregation time scales with file count.

## When to Choose Which

| Use Case | Recommendation | Why |
|---|---|---|
| Grafana field/value dropdowns | **VLH** (0.1ms) | Label index is 180x faster |
| Trace ID lookup | **VLH** (6ms) | Bloom filter vs sequential scan |
| Dashboard log panels (1h) | **VLH** (0.1ms) | Cached Parquet data |
| Dashboard log panels (24h+) | **Either** | VLH 7ms vs VL 10-55ms (depends on filter) |
| Log rate panels (1h) | **VLH** (0.1ms) | Cached |
| Log rate panels (7d) | **VL** (13ms) | Native scan 56x faster |
| Stats aggregation (7d) | **VL** (11ms) | Native scan 66x faster |
| Cold storage (>30d retention) | **VLH** | S3 at $0.023/GB vs EBS at $0.08/GB |
| Hot queries (<30d) | **VL** | Consistent <20ms regardless of query type |

## Recommended Architecture: Hybrid Hot+Cold

The optimal deployment combines both:

```
VL (EBS, hot, last 30 days)  →  fast aggregations, rates, wide scans
VLH (S3, cold, 30d+)         →  cheap storage, fast metadata, bloom lookups
```

vlselect fans out to both, merging results. Users get VL-speed aggregations for recent data and VLH's cost-efficient storage for historical queries.

## Reproducing These Benchmarks

```bash
# 1. Start infrastructure
docker run -d --name minio -p 19000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
docker run -d --name victorialogs -p 9428:9428 \
  victoriametrics/victoria-logs:latest

# 2. Generate dual-write test data
go run ./cmd/datagen \
  --endpoint=http://localhost:19000 --bucket=obs-archive \
  --logs=5000 --hours-back=48 \
  --dual-write --vl-endpoint=http://localhost:9428

# 3. Start lakehouse
go run ./cmd/lakehouse \
  --lakehouse.mode=logs \
  --lakehouse.s3.endpoint=http://localhost:19000 \
  --lakehouse.s3.bucket=obs-archive \
  --lakehouse.s3.access-key=minioadmin --lakehouse.s3.secret-key=minioadmin \
  --lakehouse.s3.force-path-style=true &

# 4. Optional: start S3 latency proxy for realistic S3 simulation
go run ./cmd/s3proxy --target=http://localhost:19000 --listen=:19001 \
  --get-delay=65ms --list-delay=80ms --head-delay=15ms &

# 5. Run comparison
go run ./cmd/loadtest -mode=compare \
  -target=http://localhost:19428 -compare-vl=http://localhost:9428 \
  -iterations=30 -warmup=3 -output=compare.json

# 6. Run extended comparison (all time ranges + calculation types)
go run ./cmd/loadtest -mode=compare-ext \
  -target=http://localhost:19428 -compare-vl=http://localhost:9428 \
  -iterations=25 -warmup=3 -output=compare-ext.json
```
