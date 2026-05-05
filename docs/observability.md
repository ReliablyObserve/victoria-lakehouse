---
title: Observability
sidebar_position: 11
---

# Observability

## Metrics

Victoria Lakehouse exposes ~80 Prometheus metrics at `/metrics` using the `lakehouse_` prefix. Metrics use the same library as VL/VT: `github.com/VictoriaMetrics/metrics`.

### RED Metrics (Client-Facing)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lakehouse_http_requests_total` | Counter | `path`, `code` | Requests per endpoint per status |
| `lakehouse_http_request_duration_seconds` | Summary | `path` | Latency (0.5/0.9/0.95/0.99 quantiles) |
| `lakehouse_http_errors_total` | Counter | `path`, `code` | Failed requests |
| `lakehouse_concurrent_select_current` | Gauge | | Active queries |
| `lakehouse_concurrent_select_capacity` | Gauge | | Max query slots |
| `lakehouse_slow_queries_total` | Counter | | Queries exceeding threshold |

### S3 Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lakehouse_s3_requests_total` | Counter | `op` | S3 API calls (GET/HEAD/LIST) |
| `lakehouse_s3_request_duration_seconds` | Summary | `op` | S3 latency |
| `lakehouse_s3_errors_total` | Counter | `op`, `code` | S3 errors |
| `lakehouse_s3_bytes_read_total` | Counter | | Bytes from S3 |
| `lakehouse_s3_throttle_total` | Counter | | 429/503 throttles |
| `lakehouse_s3_circuit_breaker_state` | Gauge | | 0=closed, 1=half, 2=open |

### Cache Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lakehouse_cache_hits_total` | Counter | `tier` | Hits per tier (L1-L4) |
| `lakehouse_cache_misses_total` | Counter | `tier` | Misses per tier |
| `lakehouse_cache_hit_ratio` | Gauge | `tier` | Rolling hit ratio |
| `lakehouse_cache_memory_bytes` | Gauge | | L1 current size |
| `lakehouse_cache_disk_bytes` | Gauge | | L2 current size |
| `lakehouse_cache_singleflight_dedup_total` | Counter | | Coalesced fetches |

### Peer Cache Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lakehouse_peer_requests_total` | Counter | `op`, `peer` | Requests to peers |
| `lakehouse_peer_hits_total` | Counter | `peer` | Peer cache hits |
| `lakehouse_peer_ring_members` | Gauge | | Fleet size |
| `lakehouse_peer_bytes_transferred_total` | Counter | `direction` | Network bytes |

### Manifest & Discovery Metrics

| Metric | Type | Description |
|---|---|---|
| `lakehouse_manifest_files` | Gauge | Parquet files tracked |
| `lakehouse_manifest_fast_path_total` | Counter | Queries short-circuited |
| `lakehouse_discovery_hot_boundary_seconds` | Gauge | Auto-discovered boundary |
| `lakehouse_discovery_hot_boundary_gap_days` | Gauge | Gap between cold and hot |

### Parquet Engine Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lakehouse_parquet_row_groups_skipped_total` | Counter | `reason` | Skipped by stats/bloom |
| `lakehouse_parquet_bloom_checks_total` | Counter | `result` | Bloom lookups |
| `lakehouse_parquet_column_bytes_read_total` | Counter | | Parquet I/O |

### Compaction Metrics (M9)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lakehouse_compaction_runs_total` | Counter | | Compaction cycles started |
| `lakehouse_compaction_files_input_total` | Counter | | Source files read across all compactions |
| `lakehouse_compaction_files_output_total` | Counter | | Output files written |
| `lakehouse_compaction_bytes_read_total` | Counter | | Bytes downloaded from S3 for compaction |
| `lakehouse_compaction_bytes_written_total` | Counter | | Bytes uploaded to S3 after compaction |
| `lakehouse_compaction_rows_merged_total` | Counter | | Total rows processed by compaction |
| `lakehouse_compaction_duration_seconds` | Histogram | | Per-partition compaction time |
| `lakehouse_compaction_errors_total` | Counter | | Failed compaction attempts |
| `lakehouse_compaction_level_files` | Gauge | `level` | Current file count at each compaction level |
| `lakehouse_compaction_skipped_total` | Counter | `reason` | Skipped partitions (`locked`, `not_leader`, `below_threshold`, `too_recent`, `schema_mismatch`) |

### Leader Election Metrics (M9)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lakehouse_election_leader` | Gauge | | 1 if this instance is the current leader, 0 otherwise |
| `lakehouse_election_transitions_total` | Counter | | Total leadership transitions |
| `lakehouse_election_health_checks_total` | Counter | `result` | Liveness check outcomes (`alive`, `dead`, `timeout`) — S3 election mode |

### Manifest Push Metrics (M9)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lakehouse_manifest_push_total` | Counter | | Push notifications sent to peers after flush/compaction |
| `lakehouse_manifest_push_errors_total` | Counter | | Failed push attempts |
| `lakehouse_manifest_push_peers` | Gauge | | Number of peers currently notified |
| `lakehouse_manifest_update_received_total` | Counter | | Manifest update notifications received from peers |

### Startup Metrics

| Metric | Type | Description |
|---|---|---|
| `lakehouse_startup_phase` | Gauge | Current phase (0-3) |
| `lakehouse_startup_total_seconds` | Gauge | Total startup time |
| `lakehouse_ready` | Gauge | 1=ready, 0=warming |
| `lakehouse_info` | Gauge | Build info (version, mode, topology) |

## Dashboards

Victoria Lakehouse ships Grafana dashboards in `dashboards/`:

| Dashboard | Description |
|---|---|
| `victoria-lakehouse.json` | Single-instance overview (7 rows) |
| `victoria-lakehouse-cluster.json` | Fleet monitoring (adds peer cache, per-instance) |
| `vm/victoria-lakehouse.json` | VictoriaMetrics datasource variant |
| `vm/victoria-lakehouse-cluster.json` | VM datasource cluster variant |

Dashboard rows: Stats -> RED -> S3 -> Cache -> Parquet Engine -> Manifest -> Prefetch.

Supplementary panels are available for adding a "Cold Storage" row to existing VL/VT community dashboards.

## Alerting Rules

Shipped in `alerts/alerts-lakehouse.yml`:

| Alert | Severity | Condition |
|---|---|---|
| `LakehouseHighErrorRate` | warning | Error rate >5% for 5m |
| `LakehouseS3CircuitBreakerOpen` | critical | Circuit breaker open for 1m |
| `LakehouseHotBoundaryGap` | warning | Gap >1 day between cold/hot for 10m |
| `LakehouseCacheDiskFull` | warning | L2 disk >95% for 5m |
| `LakehouseNotReady` | critical | Not ready for >5m |
| `LakehouseSlowQueries` | warning | Sustained slow queries for 10m |
| `LakehouseManifestStale` | warning | Not refreshed in >2h for 15m |
| `LakehouseDiscoveryFailed` | critical | No storage nodes found for 10m |
| `LakehouseS3ThrottleSustained` | warning | Sustained S3 throttling for 5m |
| `LakehousePeerDown` | warning | High peer error rate for 5m |

## Structured Logging

Victoria Lakehouse uses `slog` with JSON output:

```json
{
  "time": "2026-05-02T14:30:00Z",
  "level": "INFO",
  "msg": "starting victoria-lakehouse",
  "version": "0.1.0",
  "mode": "logs",
  "topology": "auto",
  "listen": ":9428",
  "s3_bucket": "obs-archive"
}
```

Log level controlled by `--loggerLevel` (DEBUG/INFO/WARN/ERROR).
