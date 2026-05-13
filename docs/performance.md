---
title: Performance
sidebar_position: 14
---

# Performance

## Performance Targets

| Operation | Target p95 | Notes |
|---|---|---|
| Manifest "nothing here" fast path | <1ms | Query range outside cold data |
| Point query (trace_id, bloom filter) | <100ms | Single bloom filter lookup |
| Time-range scan (1h) | <500ms | Row group stats pruning |
| stats_query (aggregation) | <300ms | Aggregation over matched data |
| field_names / field_values | <1ms | Label index lookup |

## Running Benchmarks

### Quick start

```bash
# Start MinIO
docker run -d -p 9000:9000 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data

# Generate test data
go run ./cmd/datagen --endpoint=http://localhost:9000 --logs=50000 --hours-back=48

# Run latency benchmarks
go run ./cmd/loadtest -mode=latency -target=http://localhost:9428

# Run file size / compression benchmarks
go run ./cmd/loadtest -mode=benchmark -output=benchmark-results.json
```

### Benchmark modes

| Mode | Description |
|---|---|
| `latency` | Measures p50/p95/p99 for each query type against targets |
| `throughput` | Stress tests insert and query concurrency |
| `benchmark` | File size × row group × compression matrix |
| `all` | Runs latency + throughput |

## File Size Optimization

The benchmark suite tests these combinations:

| Target Size | Row Count | Row Group Sizes Tested |
|---|---|---|
| 1 MB | ~500 rows | 1K |
| 5 MB | ~2,500 rows | 1K, 5K |
| 10 MB | ~5,000 rows | 1K, 5K, 10K |
| 50 MB | ~25,000 rows | 1K, 5K, 10K, 50K |
| 100 MB | ~50,000 rows | 1K, 5K, 10K, 50K |

**Recommendation:** 10-50MB files with 10K row groups provide the best balance of:
- S3 GET efficiency (fewer requests per query)
- Row group stats pruning (skip irrelevant groups)
- Write throughput (reasonable flush frequency)

## Compression Ratios

Measured on **real E2E production data** (377K log rows, 159K trace rows from Docker compose).

| ZSTD Level | Write Speed | Logs Ratio | Traces Ratio | Use Case |
|---|---|---|---|---|
| 1 (Fastest) | ~340 MB/s | 4.43x | 6.90x | High ingest rate (>500 MB/s) |
| 3 | ~320 MB/s | 4.60x | 7.93x | Maximum write speed |
| **7 (Default)** | **~260 MB/s** | **6.13x** | **9.39x** | **Best cost/performance** |
| 11+ (Best) | ~63 MB/s | 6.23x | 9.67x | Never recommended (<2% gain, 5x slower) |

Read latency is nearly flat across all levels (1.3x variation). Level 7 saves
**$50/month per 2 TB/day** vs level 3. See [ZSTD Benchmark](zstd-compression-benchmark.md).

**Column breakdown** (typical log data):
- `body` (text): 2-4x compression (high entropy)
- `service.name`: 50-200x (low cardinality, dictionary encoding)
- `timestamp_unix_nano`: 10-50x (delta encoding)
- `trace_id`: 1.5-3x (random, high entropy)
- `k8s.*` fields: 20-100x (low cardinality)

## MinIO vs S3 Latency

MinIO provides a local baseline. Real S3 adds network overhead:

```
Estimated S3 latency = MinIO latency + S3 first-byte overhead

Where:
  MinIO first-byte: 1-5ms (local network)
  S3 first-byte:    50-150ms (us-east-1, same region)
  S3 cross-region:  100-300ms
```

**Extrapolation formula:**

```
s3_p95 = minio_p95 + 80ms  (same-region estimate)
```

For multi-GET queries (scanning multiple row groups):

```
s3_scan_p95 = minio_scan_p95 + (num_gets × 80ms / concurrency)
```

With default concurrency=128, a query touching 10 row groups:
```
s3_scan_p95 ≈ minio_scan_p95 + (10 × 80 / 128) ≈ minio + 6ms
```

## Cost Projections

### Storage cost

```
Monthly storage = ingestion_gb_day × retention_days × (1/compression_ratio) × $0.023/GB

Example (500 GB/day, 365 day retention, 6x compression):
  = 500 × 365 × (1/6) × $0.023 = $699/month
```

### Request cost

```
Monthly requests = queries_per_day × 30 × avg_gets_per_query × $0.0004/1000

Example (10K queries/day, 5 GETs each):
  = 10,000 × 30 × 5 × $0.0004/1000 = $0.60/month
```

### Total cost comparison

| Scenario | Hot (EBS) | Cold (S3) | Total |
|---|---|---|---|
| 100 GB/day, 30d hot + 335d cold | $1,080/mo | $168/mo | $1,248/mo |
| 500 GB/day, 30d hot + 335d cold | $5,400/mo | $839/mo | $6,239/mo |
| 1 TB/day, 30d hot + 335d cold | $10,800/mo | $1,678/mo | $12,478/mo |

**EBS cost:** $0.08/GB/month × uncompressed hot data (VL handles its own compression)
**S3 cost:** $0.023/GB/month × compressed cold data

## CI Integration

The nightly workflow (`.github/workflows/nightly-loadtest.yaml`) runs:
1. Latency benchmarks against performance targets
2. Throughput stress tests
3. File size / compression matrix

Results are uploaded as workflow artifacts. The latency benchmarks fail the workflow if targets are not met.
