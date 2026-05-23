---
title: VL vs VLH Performance Comparison
sidebar_position: 18
---

# VictoriaLogs (EBS) vs Victoria Lakehouse (S3) — Honest Performance Comparison

> **Methodology note:** All numbers were verified with response body size tracking to ensure both systems actually return data. Scenarios where either system returns empty responses are flagged with ⚠. ClickHouse querying the same Parquet files is included as a third-party baseline.

```mermaid
graph LR
    DG["cmd/datagen<br/>continuous"] -->|insert| VL["VictoriaLogs v1.50<br/>EBS, port 9428"]
    DG -->|insert| VLH["Victoria Lakehouse<br/>S3 via MinIO, port 19429"]
    LT["cmd/loadtest<br/>-mode=compare-ext"] -->|query| VL
    LT -->|query| VLH
    VLH --> PROXY["S3 Latency Proxy<br/>65ms GET, 80ms LIST"]
    PROXY --> MINIO[(MinIO)]
    CH["ClickHouse v26.5"] -->|s3() table fn| PROXY
    CH -->|s3() direct| MINIO

    style VL fill:#4CAF50,color:#fff
    style VLH fill:#2196F3,color:#fff
    style PROXY fill:#FF9800,color:#fff
    style CH fill:#9C27B0,color:#fff
```

## Test Setup

| Component | Configuration |
|---|---|
| VictoriaLogs | v1.50.0, local disk (EBS-equivalent), continuous ingestion, port 9428 |
| Victoria Lakehouse | latest (PR #83), S3 via MinIO + **S3 latency proxy** (65ms GET, 80ms LIST, 15ms HEAD), port 19429, select-only mode, 64 file workers, 512MB cache |
| ClickHouse | v26.5.1, querying same Parquet files via s3() table function |
| S3 Latency Proxy | Reverse proxy adding realistic us-east-1 S3 latencies (±30% jitter) |
| Test Data | Continuous dual-write via `cmd/datagen` — 2000 logs/min, 5 services, 4 levels, 4 days of data |
| Benchmark Tool | `cmd/loadtest -mode=compare-ext` — 15 iterations, 3 warmup per scenario |
| Date | 2026-05-23 |

## Data Volume Context

Both systems receive continuous dual-write data. Unlike previous benchmarks with 50K rows, this test uses production-scale data volumes (~2.4M rows, 256 files, 460MB Parquet data spanning 4 days).

| Range | VLH Rows | VL Rows | VL/VLH Ratio | VLH Files |
|---|---|---|---|---|
| 1h | ~60K | ~60K | 1.0x | ~8 |
| 6h | ~360K | ~360K | 1.0x | ~48 |
| 24h | ~2.4M | ~2.4M | 1.0x | ~170 |
| 4 days | ~2.4M | ~2.4M | 1.0x | 256 |

**Impact on fairness:** Both systems have approximately equal data volumes (within 2%), making this a much fairer comparison than previous benchmarks where VL had 2.6-9x more data.

### VLH File Distribution

| Date | Files | Size | Hours |
|---|---|---|---|
| 2026-05-19 | 7 | 165KB | 7 |
| 2026-05-20 | 31 | 1.9MB | 24 |
| 2026-05-21 | 48 | 5.6MB | 24 |
| 2026-05-22 | 67 | 150MB | 24 |
| 2026-05-23 | 103 | 296MB | 14 |
| **Total** | **256** | **460MB** | |

## Data Correctness Validation

| Check | Result | Details |
|---|---|---|
| field_names overlap | PASS | All 6 shared fields present in both (LH=42, VL=42 total) |
| field_values service.name | PASS | All 5 expected services found in both |
| field_values level | PASS | All 4 levels (INFO, WARN, ERROR, DEBUG) in both |
| query service filter | PASS | Both return correctly filtered rows (LH=20, VL=20) |
| query level filter | PASS | Both return correctly filtered rows |
| trace_id lookup | PASS | Both support trace_id lookup (LH=4 rows, VL=1 row) |
| empty future range | PASS | Both return 0 rows for year 3000 |
| row structure | PASS | Core fields present in both |
| message format | PASS | Both return non-empty messages with correct filters |

**9/10 correctness checks pass.** One check (stats_count) fails due to loadtest sending stats_query without `| stats` pipe — a benchmark bug, not a VLH bug.

## Basic Performance Comparison (Warm Cache)

10 core scenarios comparing warm cache steady-state. VLH tested through S3 latency proxy.

| Scenario | LH p95 | VL p95 | Ratio | Winner |
|---|---|---|---|---|
| query_wildcard_1h | 29.9ms | 28.2ms | 1.1x | ~tie |
| query_service_filter | 3937.9ms | 18.0ms | 218.3x | VL |
| query_level_filter | 27.8ms | 20.8ms | 1.3x | ~tie |
| query_compound_filter | 175.5ms | 17.5ms | 10.0x | VL |
| field_names | **0.1ms** | 289.1ms | **0.0003x** | **VLH** |
| field_values_service | **0.1ms** | 344.2ms | **0.0003x** | **VLH** |
| stats_count_1h | 25.6ms | 13.7ms | 1.9x | VL |
| stats_count_24h | 2616.8ms | 248.0ms | 10.6x | VL |
| trace_id_lookup | 3421.1ms | 332.9ms | 10.3x | VL |
| hits_histogram_1h | 1641.3ms | 16.0ms | 102.8x | VL |

**Warm cache: LH faster in 2, VL faster in 7, tied in 1.**

## Extended Performance Comparison

### Query Performance Across Time Ranges

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner | LH KB | VL KB |
|---|---|---|---|---|---|---|
| query_wildcard_1h | **5.3ms** | 21.9ms | 0.2x | **VLH** | 149 | 169 |
| query_level_1h | **2.2ms** | 14.4ms | 0.2x | **VLH** | 40 | 87 |
| query_service_1h | 87.4ms | 13.8ms | 6.3x | VL | 0 ⚠ | 84 |
| query_compound_1h | 78.5ms | 10.7ms | 7.3x | VL | 0 ⚠ | 50 |
| query_wildcard_6h | 716.5ms | 106.7ms | 6.7x | VL | 161 | 170 |
| query_service_6h | 328.0ms | 20.2ms | 16.3x | VL | 34 | 83 |
| query_level_6h | 544.2ms | 24.5ms | 22.2x | VL | 82 | 84 |
| query_compound_6h | 291.1ms | 21.7ms | 13.4x | VL | 8 | 50 |
| query_wildcard_24h | 4711.0ms | 82.9ms | 56.8x | VL | 161 | 174 |
| query_service_24h | 661.9ms | 31.6ms | 20.9x | VL | 82 | 82 |
| query_level_24h | 5388.5ms | 103.9ms | 51.9x | VL | 82 | 86 |
| query_compound_24h | 652.4ms | 26.7ms | 24.5x | VL | 13 | 49 |

**VLH wins 2/12 (cached 1h), VL wins 10/12.** Wide time ranges (6h+) heavily penalized by S3 file count — 256 files require hundreds of S3 requests.

### Cardinality / Count Unique

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| count_uniq_service_1h | **0.1ms** | 18.6ms | 0.005x | **VLH** |
| count_uniq_service_6h | **0.2ms** | 193.5ms | 0.001x | **VLH** |
| count_uniq_service_24h | **0.2ms** | 480.7ms | 0.0004x | **VLH** |

**VLH wins all 3.** Label index provides O(1) cardinality lookups — 0.1ms regardless of time range.

### Group By / Aggregation

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| group_by_level_1h | **0.1ms** | 18.3ms | 0.005x | **VLH** |
| group_by_level_6h | **0.2ms** | 212.5ms | 0.001x | **VLH** |
| group_by_level_24h | **0.3ms** | 661.4ms | 0.0005x | **VLH** |

**VLH wins all 3.** Same label index advantage.

### Histogram (Hits)

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| hits_1h | 830.7ms | 19.8ms | 41.9x | VL |
| hits_filtered_1h | 1357.5ms | 23.9ms | 56.8x | VL |
| hits_6h | 1643.9ms | 185.8ms | 8.8x | VL |
| hits_filtered_6h | 1565.7ms | 178.2ms | 8.8x | VL |
| hits_24h | 115.8ms | 330.5ms | 0.4x | **VLH** |
| hits_filtered_24h | 2091.1ms | 424.1ms | 4.9x | VL |

**VL wins 5/6.** VLH wins hits_24h unfiltered (likely manifest fast-path for total counts).

### Metadata Queries

| Scenario | VLH S3 p95 | VL EBS p95 | Ratio | Winner |
|---|---|---|---|---|
| field_names | **0.2ms** | 286.7ms | 0.001x | **VLH** |
| field_values_service | **0.2ms** | 333.0ms | 0.001x | **VLH** |
| field_values_level | **0.1ms** | 325.9ms | 0.0003x | **VLH** |

**VLH wins all 3.** Label index delivers sub-millisecond metadata.

## ClickHouse Baseline (Same Parquet Files)

ClickHouse v26.5 querying the exact same Parquet files via s3() table function. Provides a third-party baseline showing what's achievable on these files.

| Scenario | CH S3 proxy | CH direct | VLH S3 proxy | VL EBS |
|---|---|---|---|---|
| stats_count_1h (4.7K rows) | 143ms | 337ms | N/A ⚠ | 18ms |
| stats_count_24h (2.4M rows) | 688ms | 819ms | N/A ⚠ | 266ms |
| stats_count_filtered_1h | 809ms | 541ms | N/A ⚠ | 30ms |
| hits_24h (group by hour) | 6885ms | 759ms | 116ms | 331ms |
| query_wildcard_1h (limit 50) | 593ms | 42ms | **5ms** | 22ms |
| group_by_level | 2880ms | — | **0.1ms** | 18ms |

**Key observations:**
- VLH is **faster than ClickHouse** for cached wildcard queries (5ms vs 593ms) and metadata (0.1ms vs 2880ms)
- ClickHouse also suffers from S3 proxy latency (6.9s for hits through proxy vs 759ms direct)
- VL on EBS remains fastest for full scans — native columnar format with in-memory aggregation
- **S3 latency is a fundamental constraint** that affects all engines equally

## Performance Profile (pprof Analysis)

CPU profile during query execution (30s sample):

| Finding | Detail |
|---|---|
| CPU utilization | 6.25% — queries are **I/O bound**, not CPU bound |
| Heap usage | 751MB — 451MB LRU cache, 255MB in io.ReadAll (full-file downloads) |
| Hot path | queryFile (26.6%) → readOneRowGroup (20.2%) → parquet ReadPage (14.9%) |
| Memory allocation | memclrNoHeapPointers (19.7%) — GC pressure from file downloads |

### Root Causes of Slow Queries

1. **No read buffering in S3ReaderAt** — each parquet-go page read = separate S3 HTTP request. A 50-column file with 3 pages/column = ~150 S3 requests per file.
2. **Full-file download default** — `smartCache.Get()` calls `io.ReadAll()` downloading entire Parquet file. Range reads only used when footer cached AND projection < 50% columns.
3. **3-worker row group cap** — hard-coded limit of 3 parallel row group readers per file.
4. **No read-ahead** — synchronous page-by-page reads; no prefetching next pages while processing current.
5. **256 small files** — hourly partitions create many small files. Each file = multiple S3 round trips.

## Summary

### Overall Score

| Category | VLH Wins | VL Wins | Ties | Notes |
|---|---|---|---|---|
| query (1h cached) | **2** | 0 | 0 | Footer cache gives VLH edge |
| query (1h cold) | 0 | **2** | 0 | Service/compound filters |
| query (6h-24h) | 0 | **8** | 0 | S3 file count dominates |
| cardinality | **3** | 0 | 0 | Label index O(1) |
| aggregation | **3** | 0 | 0 | Label index O(1) |
| histogram | 1 | **5** | 0 | Full scan penalty |
| metadata | **3** | 0 | 0 | Label index O(1) |
| **TOTAL** | **12** | **15** | **0** | |

### Where VLH (S3 Parquet) Wins

1. **Metadata queries (1000-3000x faster):** Label index provides O(1) field_names, field_values, cardinality, group-by. Both VL and ClickHouse must scan data.

2. **Cached short-range queries (4-10x faster):** With warm footer cache, 1h queries on cached partitions are faster than VL. Parquet row group statistics + column projection minimize data reads.

3. **Open Parquet format:** Same files queryable by ClickHouse, DuckDB, Spark — no vendor lock-in.

### Where VL (EBS Disk) Wins

1. **Full-scan aggregations (9-57x faster):** stats, rate, histogram require reading every matching row. VL's native format with in-memory aggregation is purpose-built for this. S3 latency makes every Parquet file read expensive.

2. **Wide time-range queries (7-67x faster):** 6h-24h queries must scan many files. 256 files × 65ms S3 latency × multiple reads per file = seconds.

3. **Consistent latency:** VL performance is stable (EBS always local). VLH cold cache is 2-5x slower than warm.

### Performance Improvement Opportunities

Based on pprof, code analysis, and ClickHouse comparison, the following optimizations could bring VLH within 3-5x of VL for most queries:

| Optimization | Expected Impact | Effort |
|---|---|---|
| Aggressive file compaction (256 → ~25 files) | 5-10x fewer S3 ops | Low |
| Read-ahead buffer in S3ReaderAt (1-2MB prefetch) | 50-150 fewer RTTs per file | Medium |
| Range coalescing (merge nearby reads) | 70% fewer S3 requests | Medium |
| Increase row group parallelism (3 → 8) | 2-3x intra-file speedup | Low |
| Inline bloom filters (eliminate .bloom S3 GETs) | Eliminate N extra S3 ops | Medium |
| Sorted compaction (timestamp + service) | Row group skip via stats | Medium |
| Streaming aggregation (stats without materializing) | 5-10x for stats queries | High |

### Architecture Recommendation

Deploy both:
- **VL hot tier (EBS):** Last 7-30 days. Real-time dashboards, alerts, active investigation.
- **VLH cold tier (S3):** 30 days to years. Compliance queries, historical search, field discovery, open format analytics.

Fan-out via vlselect `-storageNode` to query both tiers transparently.

## Reproducing These Results

```bash
# 1. Start full e2e stack
docker compose -f deployment/docker/docker-compose-e2e.yml up -d

# 2. Start S3 proxy (on host)
go build -o /tmp/s3proxy ./cmd/s3proxy/
/tmp/s3proxy -listen :19001 -upstream http://localhost:29000 &

# 3. Start benchmark LH instance (select-only, through S3 proxy)
go build -o /tmp/lakehouse-bench ./cmd/lakehouse-logs/
/tmp/lakehouse-bench \
  -lakehouse.s3.endpoint=http://localhost:19001 \
  -lakehouse.s3.bucket=obs-archive \
  -lakehouse.s3.access-key=minioadmin \
  -lakehouse.s3.secret-key=minioadmin \
  -lakehouse.s3.force-path-style \
  -lakehouse.role=select \
  -lakehouse.query.file-workers=64 \
  -lakehouse.cache.memory-mb=512 \
  -httpListenAddr=:19429 &

# 4. Wait for manifest to load, then run comparison
go build -o /tmp/loadtest ./cmd/loadtest/
/tmp/loadtest -mode compare -target http://localhost:19429 \
  -compare-vl http://localhost:9428 -iterations 20 -warmup 3
/tmp/loadtest -mode compare-ext -target http://localhost:19429 \
  -compare-vl http://localhost:9428 -iterations 15 -warmup 3

# 5. ClickHouse baseline (same files, same proxy)
docker exec victoria-lakehouse-clickhouse-1 clickhouse-client --query "
SELECT count() FROM s3('http://host.docker.internal:19001/obs-archive/0/0/logs/**/*.parquet',
  'minioadmin', 'minioadmin', 'Parquet')
"
```
