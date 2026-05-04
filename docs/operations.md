# Operations

## Health Endpoints

| Endpoint | Purpose | Returns 200 |
|---|---|---|
| `/health` | Liveness probe | Always (once HTTP binds) |
| `/ready` | Readiness probe | After startup warmup completes |
| `/lakehouse/info` | Build/config info | Always |
| `/manifest/range` | Data range served | Always |
| `/metrics` | Prometheus metrics | Always |
| `/internal/buffer/query` | Buffer query (insert pods) | When insert role enabled |

## Startup Behavior

Victoria Lakehouse goes through 4 phases on startup:

1. **INIT** — parse config, bind HTTP port. `/health` returns 200.
2. **DISK_RECOVERY** — load persisted manifest, label index, and footers from disk (~1-3s warm, N/A cold).
3. **S3_REFRESH** — incremental S3 ListObjects for new files since last persist (~2-10s warm, 30-60s cold).
4. **READY** — `/ready` returns 200. Kubernetes routes traffic.

Monitor: `lakehouse_startup_phase` gauge, `lakehouse_startup_total_seconds` gauge.

### Early Serving

Set `--lakehouse.startup.serve-stale=true` to serve from persisted cache (Phase 1) before S3 refresh completes. Queries may return slightly stale results until refresh finishes.

### Warmup Safety Valve

`--lakehouse.startup.max-warmup-time=5m` aborts warmup and goes ready with whatever was loaded. Background refresh continues.

## Write Path Operations

### WAL Management

The WAL is automatically managed but operators should monitor:
- **WAL size**: `lakehouse_insert_wal_bytes` — if approaching `--lakehouse.insert.wal-max-bytes`, flush pipeline may be stalled
- **WAL replay on startup**: check logs for `"WAL recovery complete"` with recovered log/trace counts
- **WAL location**: `--lakehouse.insert.wal-dir` must be on durable storage (not tmpfs)

### Buffer Query Bridge

When running separate insert and select pods:
- Select pods discover insert pods via `--lakehouse.select.insert-headless-service`
- Buffer query timeout is configurable via `--lakehouse.select.buffer-query-timeout` (default 2s)
- Endpoint errors are silently ignored — degraded to S3-only results rather than failing the query

### Flush Pipeline

- **Periodic flush**: every `--lakehouse.insert.flush-interval` (default 10s)
- **Adaptive flush**: when per-partition estimate reaches `--lakehouse.insert.target-file-size` (default 128MB)
- **Graceful shutdown flush**: all buffers flushed before process exit (preStop hook)

## Graceful Shutdown

On SIGTERM:
1. Stop accepting new queries (readiness -> false)
2. Flush all pending write buffers to S3
3. Drain in-flight queries (30s timeout)
4. Truncate WAL (all data flushed)
5. Persist manifest, label index, peer ring to disk
6. Close S3 and peer connections
7. Exit

Set `terminationGracePeriodSeconds: 60` in Kubernetes (30s drain + 30s persist).

## Cache Management

### L1 Memory Cache

- Stores footers (~1KB), bloom filters (~10KB), hot pages
- LRU eviction at `--lakehouse.cache.memory-limit`
- Monitor: `lakehouse_cache_memory_bytes` vs `lakehouse_cache_memory_limit_bytes`

### L2 Disk Cache

- Stores full Parquet files from S3
- LRU eviction at `--lakehouse.cache.eviction-watermark` (default 80%) of disk limit
- Monitor: `lakehouse_cache_disk_bytes` vs `lakehouse_cache_disk_limit_bytes`
- Alert: `LakehouseCacheDiskFull` if >95% full

### L3 Peer Cache

- Consistent hash routes keys to peer instances
- Requires headless service and `--lakehouse.peer-auth-key`
- Monitor: `lakehouse_peer_ring_members`, `lakehouse_peer_hits_total`

### Cache Coalescence

`singleflight.Group` ensures only one S3 fetch per cache key, even under concurrent queries for the same data. Monitor: `lakehouse_cache_singleflight_dedup_total`.

## Manifest Refresh

The partition manifest tracks all Parquet files in S3. It refreshes:
- **Polling**: every `--lakehouse.manifest.refresh-interval` (default 5m) via S3 ListObjects
- **SQS** (optional): near-real-time updates from S3 event notifications

Monitor: `lakehouse_manifest_files`, `lakehouse_manifest_refresh_total`, `lakehouse_manifest_sqs_events_total`.

## Hot Boundary Discovery

Victoria Lakehouse polls vlstorage/vtstorage nodes to learn the hot tier's data range:
- Refreshes every `--lakehouse.discovery.refresh-interval` (default 5m)
- Monitor: `lakehouse_discovery_hot_boundary_seconds`, `lakehouse_discovery_hot_boundary_gap_days`
- Alert: `LakehouseHotBoundaryGap` if gap > 1 day between cold and hot data

## Circuit Breaker

S3 failures trigger a circuit breaker:
- **Closed** (normal): requests flow through
- **Open** (after N failures): requests fail fast for `--lakehouse.circuit-breaker.timeout`
- **Half-open**: probe requests; N successes close the breaker

Monitor: `lakehouse_s3_circuit_breaker_state` (0=closed, 1=half-open, 2=open).
Alert: `LakehouseS3CircuitBreakerOpen`.

## Scaling

### Vertical

- **CPU**: driven by Parquet decompression and filter evaluation. 0.5-2 vCPU per instance typical.
- **Memory**: L1 cache + query working set. 512MB-2GB per instance typical.
- **Disk**: L2 cache. Size to hold 2-4 weeks of frequently queried data.

### Horizontal

- Add replicas to increase query throughput
- Peer cache distributes L2 across fleet (3x effective cache)
- Manifest and label index replicated on each instance (lightweight)
- No coordination required between instances

### Sizing Guide

| Dataset | Replicas | CPU/instance | Memory/instance | L2 Disk |
|---|---|---|---|---|
| 100GB S3 | 3 (1/AZ) | 0.5 vCPU | 512MB | 10GB |
| 1TB S3 | 6 (2/AZ) | 1 vCPU | 1GB | 50GB |
| 10TB S3 | 12 (4/AZ) | 2 vCPU | 2GB | 100GB |
| 100TB S3 | 24 (8/AZ) | 2 vCPU | 4GB | 200GB |

## Compaction

### Enabling Compaction

Compaction is disabled by default. Enable it for production deployments:

```bash
lakehouse \
  --lakehouse.compaction.enabled=true \
  --lakehouse.compaction.leader-election=auto \
  --lakehouse.compaction.min-files-l0=10 \
  --lakehouse.compaction.min-files-l1=10
```

Or in YAML:

```yaml
lakehouse:
  compaction:
    enabled: true
    leader_election: auto
    min_files_l0: 10
    min_files_l1: 10
    interval: 5m
    min_age: 1h
```

Compaction is only meaningful when inserts are active. For read-only (select-only) instances, leave compaction disabled.

### Monitoring Compaction

Key metrics to watch:

| Metric | Alert condition |
|---|---|
| `lakehouse_compaction_errors_total` (rate) | Any sustained errors |
| `lakehouse_compaction_level_files{level="0"}` | Should trend down over time |
| `lakehouse_compaction_duration_seconds` (p95) | >60s may indicate S3 saturation |
| `lakehouse_election_leader` | Should be 1 on exactly one instance in the fleet |

### Leader Election Troubleshooting

**K8s mode — "not becoming leader"**

1. Check that the Helm chart RBAC was applied: the ServiceAccount needs `get/create/update` on `leases.coordination.k8s.io`.
2. Check `lakehouse_election_transitions_total` — transitions should occur when pods restart.
3. Increase `--lakehouse.compaction.lease-duration` if instances are losing leadership due to transient API server latency.

**S3 mode — lock not being released after crash**

The lock TTL (`--lakehouse.compaction.s3-lock-ttl`, default 60s) controls when a stale lock may be stolen. After a crash, the next instance will take over within one TTL. To recover faster, reduce the TTL or manually delete the lock file `{prefix}.election-lock`.

**`none` mode — multiple instances all compact**

This is expected for `none` mode. Only use `none` for single-instance deployments. For fleets, use `auto`, `k8s`, or `s3`.

## Troubleshooting

### Queries return empty when data exists

1. Check manifest: `curl /manifest/range` — does the time range overlap?
2. Check hot boundary: `lakehouse_discovery_hot_boundary_seconds` — is it suppressing your data?
3. Check S3 access: `lakehouse_s3_errors_total` for permission/connectivity issues
4. Check circuit breaker: `lakehouse_s3_circuit_breaker_state` — is it open?

### High query latency

1. Check cache hit rates: `lakehouse_cache_hit_ratio` by tier
2. Check S3 latency: `lakehouse_s3_request_duration_seconds`
3. Check row group skip rate: `lakehouse_parquet_row_groups_skipped_total` — low skip rate means queries scan too much data
4. Check query concurrency: `lakehouse_concurrent_select_current` vs `_capacity`

### Startup takes too long

1. Check startup phase: `lakehouse_startup_phase`
2. For cold start: full S3 ListObjects can take 30-60s with large datasets
3. Enable `--lakehouse.startup.serve-stale=true` for faster readiness
4. Reduce `--lakehouse.startup.warmup-window` to warm fewer partitions
5. Set `--lakehouse.startup.max-warmup-time` as safety valve

### Insert returns 503

1. Check `CanWriteData()` — S3 connectivity issue or WAL full
2. If WAL full: flush pipeline may be stalled (check S3 write errors)
3. Increase `--lakehouse.insert.wal-max-bytes` or investigate S3 permissions

### Recently ingested data not visible in queries

1. Check flush interval: data is visible in S3 after `--lakehouse.insert.flush-interval`
2. If buffer query bridge is enabled, data should be visible immediately via insert pod buffers
3. Check `--lakehouse.select.buffer-query-enabled` is `true`
4. Check `--lakehouse.select.insert-headless-service` resolves to insert pods
5. Check buffer query timeout: `--lakehouse.select.buffer-query-timeout` (default 2s)

### WAL replay on startup reports entries but data is missing

1. WAL replay re-adds entries to partition buffers — they will be flushed on next flush cycle
2. If process crashes before first flush after replay, the WAL still has the entries (not truncated until successful flush)
3. Check logs for `"WAL recovery complete"` with correct counts
