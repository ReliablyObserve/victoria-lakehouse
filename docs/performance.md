# Performance

## Latency Targets

| Operation | Target p95 | Notes |
|---|---|---|
| Manifest "nothing here" | <1ms | In-memory, no I/O |
| Point query (trace_id, bloom) | <100ms | Bloom filter + row group skip |
| Time-range scan (1h) | <500ms | Parquet column reads from cache/S3 |
| Time-range scan (24h) | <1200ms | Multiple partition reads |
| stats_query (aggregation) | <300ms | COUNT/SUM via pipe processors |
| field_names discovery | <1ms | Label index (pre-computed) |
| field_values | <3ms | Label index |

## Latency by Cache Tier

### L1 Memory Cache Hit

| Query Type | p50 | p95 |
|---|---|---|
| Point query (trace_id) | 3-10ms | 20-40ms |
| Time-range scan (1h) | 10-30ms | 50-100ms |
| stats_query | 15-50ms | 80-200ms |
| field_names | <1ms | <2ms |

### L2 Disk Cache Hit (EBS gp3)

| Query Type | p50 | p95 |
|---|---|---|
| Point query | 5-20ms | 30-60ms |
| Time-range scan (1h) | 15-50ms | 80-150ms |
| Full-text search (1h) | 30-100ms | 200-500ms |

### L3 Peer Cache Hit

| Query Type | p50 | p95 |
|---|---|---|
| Point query | 10-30ms | 50-80ms |
| Time-range scan (1h) | 25-70ms | 100-200ms |

### L4 S3 Direct Read (Cold)

| Query Type | p50 | p95 |
|---|---|---|
| Point query (bloom) | 50-100ms | 150-250ms |
| Time-range scan (1h) | 100-300ms | 300-600ms |
| Time-range scan (24h) | 200-600ms | 500-1200ms |
| Full-text search (1h) | 200-500ms | 500-1500ms |

## Optimization Techniques

### Partition Manifest Fast Path

For queries entirely within the hot tier's range, the partition manifest returns empty in <1ms with zero S3 I/O. This is the critical optimization for cluster mode where every query fans out to all storage nodes.

### Bloom Filter + Row Group Statistics

- Row group statistics (min/max on timestamp, service_name) skip 80-95% of row groups
- Bloom filters on `trace_id` and `service_name` skip 95%+ for point lookups
- Combined: most queries read <5% of total data

### Column Projection

Parquet column projection reads only the columns referenced by the query. A 3-column query on a 20-column file reads 70%+ fewer bytes.

### Cache Coalescence

`singleflight.Group` ensures concurrent queries for the same Parquet file generate only one S3 fetch. Subsequent queries wait for the first fetch to complete.

### Correlated Prefetch

After a log query, Victoria Lakehouse warms trace Parquet files for the same time+service in the background. The user's next trace query hits warm cache.

### Read-Ahead

Sequential time-range scans (scrolling in Grafana Explore) prefetch the next N partitions. Configurable via `--lakehouse.prefetch.read-ahead-depth`.

## Throughput

| Metric | L1/L2 Cache | S3 Cold |
|---|---|---|
| QPS per instance | 200-500 | 50-150 |
| Data scan rate | 500 MB/s | 50-100 MB/s |
| Max S3 connections | N/A | 100-200 |
| Fleet QPS (12 instances) | 2,400-6,000 | 600-1,800 |

## Load Testing (M9)

Victoria Lakehouse ships a load test binary at `cmd/loadtest` that measures both latency targets and maximum throughput.

### Building and Running

```bash
# Build
make build-loadtest
# or
go build -o bin/loadtest ./cmd/loadtest

# Run all tests (latency + throughput)
./bin/loadtest --target http://localhost:9428 --mode all --duration 60s

# Latency benchmarks only
./bin/loadtest --target http://localhost:9428 --mode latency --iterations 100

# Throughput stress tests only
./bin/loadtest --target http://localhost:9428 --mode throughput --duration 60s

# Save results to JSON
./bin/loadtest --target http://localhost:9428 --mode all --output results.json
```

Exits 0 if all p95 targets pass, exits 1 if any target is missed.

### Latency Benchmarks

Six benchmarks, each measured at p50/p95/p99 across the configured number of iterations:

| Benchmark | Target p95 | What it measures |
|---|---|---|
| `manifest_fast_path` | 1ms | Empty response for queries in the future (manifest short-circuit) |
| `field_names` | 1ms | `GET /select/logsql/field_names` from label index |
| `field_values` | 1ms | `GET /select/logsql/field_values?field=service.name` from label index |
| `bloom_point_query` | 100ms | `trace_id:="..."` bloom filter lookup |
| `stats_aggregation` | 300ms | `stats_query` over 1h window |
| `time_range_scan_1h` | 500ms | Full scan over last 1h, 100-row limit |

The `manifest_fast_path` and label index benchmarks (1ms target) exercise the pure in-memory path with no S3 I/O.

### Throughput Tests

Two throughput tests find the maximum sustainable rate by sweeping concurrency levels (1, 2, 4, 8, 16, 32):

| Test | Unit | What it measures |
|---|---|---|
| `max_insert_rate` | rows/s | Maximum insert throughput (100-row NDJSON batches) |
| `max_query_qps` | qps | Maximum query throughput (`SELECT * LIMIT 10`) |

Additionally, a `mixed` mode runs 7 insert workers and 3 query workers concurrently for a blended ops/s measurement.

### Running in CI

The nightly GitHub Actions workflow (`.github/workflows/loadtest.yml`) runs the load test against a live MinIO + Lakehouse stack. Results are saved as a JSON artifact and the job fails if any latency target is missed.

## User Experience

| Scenario | Behavior |
|---|---|
| Recent logs (within hot range) | Instant — hot VL handles, Lakehouse returns empty <1ms |
| 30-day query (hot+cold) | Hot portion fast, cold 100-300ms |
| Field autocomplete | <1ms from label index (faster than EBS VL/VT) |
| Scrolling through time | Smooth with read-ahead prefetch |
| First query after deploy | 1-5s from persisted cache |
