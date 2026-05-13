---
title: Benchmarks
sidebar_position: 17
---

# Benchmarks

Victoria Lakehouse includes two benchmark tools: `cmd/loadtest` for latency and throughput testing against a running instance, and `cmd/datagen` for generating realistic test data. This document describes how to run each benchmark mode, interpret results, and integrate with CI.

## Performance Targets

The benchmark suite validates these p95 latency targets:

| Benchmark | Target p95 | Description |
|---|---|---|
| `manifest_fast_path` | <1ms | Query time range with no cold data |
| `bloom_point_query` | <100ms | Exact trace_id lookup via bloom filter |
| `time_range_scan_1h` | <500ms | Scan one hour of data with limit |
| `stats_aggregation` | <300ms | stats_query aggregation over one hour |
| `field_names` | <1ms | List available field/column names |
| `field_values` | <1ms | List values for `service.name` |

## Generating Test Data

The `cmd/datagen` tool writes Parquet files directly to S3/MinIO and optionally dual-writes to a VictoriaLogs instance:

```bash
# Generate 5000 logs and 1000 trace spans over 48 hours
go run ./cmd/datagen \
  --endpoint=http://localhost:9000 \
  --bucket=obs-archive \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --logs=5000 \
  --traces=1000 \
  --hours-back=48

# With dual-write to VictoriaLogs (for hot+cold comparison)
go run ./cmd/datagen \
  --endpoint=http://localhost:9000 \
  --bucket=obs-archive \
  --access-key=minioadmin \
  --secret-key=minioadmin \
  --logs=5000 \
  --traces=1000 \
  --hours-back=48 \
  --dual-write \
  --vl-endpoint=http://localhost:9428

# Continuous mode: generate fresh data every 30 seconds
go run ./cmd/datagen \
  --endpoint=http://localhost:9000 \
  --bucket=obs-archive \
  --logs=500 \
  --traces=200 \
  --hours-back=1 \
  --interval=30s
```

### Generated Data Characteristics

The datagen tool produces realistic observability data across five services (`api-gateway`, `user-service`, `order-service`, `payment-service`, `notification-service`) with:

- **Five log patterns**: JSON access logs, logfmt, nginx combined format, Java stack traces, and OTEL-formatted logs
- **Multi-span traces**: 2-5 spans per trace with parent-child relationships, realistic span names (HTTP endpoints, DB queries, gRPC calls, Redis operations)
- **Full OTEL attributes**: `service.name`, `k8s.namespace.name`, `k8s.pod.name`, `k8s.deployment.name`, `k8s.node.name`, `deployment.environment`, `cloud.region`, `host.name`
- **Log attributes**: `http.method`, `http.status_code`, `request_id`, `exception.type`, `db.system`
- **Span attributes**: `http.method`, `http.status_code`, `http.url`, `db.system`, `db.statement`
- **Hive partitioning**: files written to `logs/dt=YYYY-MM-DD/hour=HH/` and `traces/dt=YYYY-MM-DD/hour=HH/` paths

## Running Latency Benchmarks

The `cmd/loadtest` tool measures p50/p95/p99 latency for each query type:

```bash
# Default: 100 iterations per test
go run ./cmd/loadtest -mode=latency -target=http://localhost:9428

# More iterations for stable results
go run ./cmd/loadtest -mode=latency -target=http://localhost:9428 -iterations=500

# Save results as JSON
go run ./cmd/loadtest -mode=latency -target=http://localhost:9428 -output=latency-results.json
```

The latency suite runs six benchmarks:

| Test | Query | What It Measures |
|---|---|---|
| `manifest_fast_path` | `query=*` with future time range | Manifest "nothing here" fast path |
| `bloom_point_query` | `trace_id:="0000000000000001"` | Bloom filter point lookup |
| `time_range_scan_1h` | `query=*` over last hour, limit 100 | Row group stats pruning + scan |
| `stats_aggregation` | `stats_query` over last hour | Aggregation query |
| `field_names` | `/select/logsql/field_names` | Label index lookup |
| `field_values` | `/select/logsql/field_values?field=service.name` | Label value enumeration |

The benchmark **passes** if all tests meet their p95 targets. The exit code is non-zero on failure, making it suitable for CI gates.

## Running Throughput Tests

Throughput mode stress-tests insert and query concurrency:

```bash
# Default: 60 second duration
go run ./cmd/loadtest -mode=throughput -target=http://localhost:9428

# Longer test
go run ./cmd/loadtest -mode=throughput -target=http://localhost:9428 -duration=300s
```

The throughput suite measures:

- **`max_insert_rate`** -- Maximum rows/second across concurrency levels (1, 2, 4, 8, 16, 32). Each worker sends batches of 100 NDJSON rows to `/insert/jsonline`. Stops escalating concurrency when throughput drops below 80% of the peak.
- **`max_query_qps`** -- Maximum queries/second across the same concurrency levels. Each worker runs `query=*&limit=10` queries.

## Running the File Size / Compression Matrix

Benchmark mode tests all combinations of file size, row group size, and ZSTD compression level:

```bash
go run ./cmd/loadtest -mode=benchmark -output=benchmark-results.json
```

### Benchmark Matrix

| Target File Size | Row Count | Row Group Sizes Tested |
|---|---|---|
| 1 MB | ~500 rows | 1K |
| 5 MB | ~2,500 rows | 1K, 5K |
| 10 MB | ~5,000 rows | 1K, 5K, 10K |
| 50 MB | ~25,000 rows | 1K, 5K, 10K, 50K |
| 100 MB | ~50,000 rows | 1K, 5K, 10K, 50K |

Each combination is tested with ZSTD compression levels 1, 3, 7 (default), and 11.

For each combination, the benchmark measures:
- **Write time** (ms) -- time to write the Parquet file in memory
- **Read time** (ms) -- time to read and deserialize the file
- **Compressed size** (bytes) -- actual Parquet file size
- **Compression ratio** -- raw size / compressed size

### Interpreting Results

The optimal configuration for most workloads is **10-50 MB files with 10K row groups and ZSTD level 7**:

- Larger files reduce the number of S3 GET requests per query
- 10K row groups provide enough granularity for row group stats pruning
- ZSTD level 7 compresses 25% better than level 3 on real data (~260 MB/s, 6.1x logs / 9.4x traces)
- Read performance is nearly flat across levels (decompression is not the bottleneck)

Extreme settings to avoid:
- Files under 5 MB create excessive S3 request overhead
- Row groups over 50K reduce the effectiveness of statistics-based skipping
- ZSTD level 11+ compresses <2% better than level 7 at 5x the CPU cost

See [ZSTD Compression Benchmark](zstd-compression-benchmark.md) for detailed real-data results.

## Running All Benchmarks

```bash
# Latency + throughput
go run ./cmd/loadtest -mode=all -target=http://localhost:9428 -output=results.json

# Mixed workload (70% insert / 30% query concurrently)
go run ./cmd/loadtest -mode=mixed -target=http://localhost:9428 -duration=120s
```

The mixed workload runs 7 insert workers and 3 query workers simultaneously, measuring combined operations/second.

## Full Local Benchmark Workflow

```bash
# 1. Start MinIO
docker run -d --name minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

# 2. Create bucket
docker run --rm --network host minio/mc \
  sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb local/obs-archive"

# 3. Generate test data
go run ./cmd/datagen \
  --endpoint=http://localhost:9000 \
  --bucket=obs-archive \
  --logs=50000 \
  --traces=10000 \
  --hours-back=72

# 4. Start lakehouse
go run ./cmd/lakehouse \
  --lakehouse.mode=logs \
  --lakehouse.s3.bucket=obs-archive \
  --lakehouse.s3.endpoint=http://localhost:9000 \
  --lakehouse.s3.access-key=minioadmin \
  --lakehouse.s3.secret-key=minioadmin \
  --lakehouse.s3.force-path-style=true &

# 5. Wait for ready
until curl -s http://localhost:9428/ready; do sleep 1; done

# 6. Run benchmarks
go run ./cmd/loadtest -mode=all -target=http://localhost:9428 -output=benchmark.json

# 7. Run file size matrix
go run ./cmd/loadtest -mode=benchmark -output=matrix.json
```

## CI Integration

The nightly workflow (`.github/workflows/nightly-loadtest.yaml`) runs the full benchmark suite:

1. Starts MinIO in a service container
2. Generates test data with `cmd/datagen`
3. Starts lakehouse and waits for readiness
4. Runs `cmd/loadtest -mode=all` with p95 target validation
5. Runs `cmd/loadtest -mode=benchmark` for the compression matrix
6. Uploads `benchmark.json` and `matrix.json` as workflow artifacts

The workflow fails if any latency benchmark exceeds its p95 target, preventing performance regressions from merging.
