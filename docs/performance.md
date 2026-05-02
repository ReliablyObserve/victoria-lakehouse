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

## User Experience

| Scenario | Behavior |
|---|---|
| Recent logs (within hot range) | Instant — hot VL handles, Lakehouse returns empty <1ms |
| 30-day query (hot+cold) | Hot portion fast, cold 100-300ms |
| Field autocomplete | <1ms from label index (faster than EBS VL/VT) |
| Scrolling through time | Smooth with read-ahead prefetch |
| First query after deploy | 1-5s from persisted cache |
