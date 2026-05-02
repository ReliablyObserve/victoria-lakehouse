# Configuration

Victoria Lakehouse uses a `--lakehouse.*` flag prefix for all settings. Flags can also be set via YAML config file (`--lakehouse.config=path`). CLI flags override YAML values.

All flags have production-ready defaults. A minimal config requires only `--lakehouse.mode` and `--lakehouse.s3.bucket`.

## Core Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.mode` | **(required)** | `logs` or `traces` — determines schema, port, field mapping |
| `--lakehouse.config` | `""` | Path to YAML config file |
| `--lakehouse.topology` | `auto` | `auto`, `storage-node`, `direct`, `loki-proxy` |

## S3 Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.s3.bucket` | **(required)** | S3 bucket name |
| `--lakehouse.s3.region` | `us-east-1` | AWS region |
| `--lakehouse.s3.prefix` | `""` | Key prefix (auto-set from mode: `logs/` or `traces/`) |
| `--lakehouse.s3.endpoint` | `""` | Custom S3 endpoint (MinIO, R2) |
| `--lakehouse.s3.access-key` | `""` | Static access key (prefer IAM role/IRSA) |
| `--lakehouse.s3.secret-key` | `""` | Static secret key (prefer IAM role/IRSA) |
| `--lakehouse.s3.force-path-style` | `false` | Use path-style S3 URLs (required for MinIO) |
| `--lakehouse.s3.max-connections` | `128` | Max concurrent S3 HTTP connections |
| `--lakehouse.s3.timeout` | `30s` | Per-request S3 timeout |
| `--lakehouse.s3.retry-max` | `3` | Max retries on S3 transient errors |
| `--lakehouse.s3.retry-base-delay` | `200ms` | Initial retry backoff (doubles each retry) |

## Cache Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.cache.memory-limit` | `512MB` | L1 in-memory cache max size |
| `--lakehouse.cache.disk-path` | `/data/lakehouse/cache` | L2 disk cache directory |
| `--lakehouse.cache.disk-limit` | `50GB` | L2 disk cache max size |
| `--lakehouse.cache.eviction-watermark` | `0.8` | Start evicting at 80% of disk limit |
| `--lakehouse.cache.footer-ttl` | `1h` | L1 footer cache TTL |
| `--lakehouse.cache.bloom-ttl` | `1h` | L1 bloom filter cache TTL |
| `--lakehouse.cache.page-ttl` | `10m` | L1 hot page cache TTL |

## Discovery Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.discovery.headless-service` | `""` | K8s headless service for vlstorage/vtstorage |
| `--lakehouse.discovery.storage-nodes` | `""` | Comma-separated static storage node addresses |
| `--lakehouse.discovery.partition-auth-key` | `""` | Auth key for `/internal/partition/list` |
| `--lakehouse.discovery.refresh-interval` | `5m` | How often to poll storage nodes |
| `--lakehouse.discovery.timeout` | `10s` | Timeout per storage node poll |
| `--lakehouse.discovery.peer-headless-service` | `""` | K8s headless service for peer cache fleet |
| `--lakehouse.discovery.peer-refresh-interval` | `30s` | Peer DNS refresh interval |

## Hot Boundary

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.hot-boundary` | `""` (auto-discover) | Manual hot boundary override (e.g., `7d`, `168h`) |

When empty, Victoria Lakehouse auto-discovers the hot boundary by polling vlstorage/vtstorage nodes. Set this to skip auto-discovery.

## Manifest Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.manifest.refresh-interval` | `5m` | S3 ListObjects polling interval |
| `--lakehouse.manifest.sqs-queue-url` | `""` | Optional SQS queue for S3 event notifications |
| `--lakehouse.manifest.sqs-region` | (from `s3.region`) | SQS queue region |
| `--lakehouse.manifest.sqs-wait-time` | `20s` | SQS long-poll wait time |
| `--lakehouse.manifest.persist-path` | `/data/lakehouse` | Directory for persisted manifest + index |
| `--lakehouse.manifest.persist-interval` | `5m` | How often to write manifest to disk |

## Prefetch Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.prefetch.correlated` | `true` | Enable cross-signal prefetch (logs/traces) |
| `--lakehouse.prefetch.read-ahead-depth` | `2` | Partitions to prefetch for sequential scans |
| `--lakehouse.prefetch.max-concurrent` | `4` | Max concurrent prefetch downloads |
| `--lakehouse.prefetch.max-queue` | `64` | Max pending prefetch tasks |

## Peer Cache Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.peer-auth-key` | `""` | Shared secret for peer cache HTTP |
| `--lakehouse.peer.timeout` | `5s` | Timeout for peer cache requests |
| `--lakehouse.peer.max-connections` | `32` | Max HTTP connections per peer |

## Startup Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.startup.serve-stale` | `false` | Serve from disk cache before S3 refresh |
| `--lakehouse.startup.warmup-window` | `24h` | Pre-cache footers/blooms for recent data |
| `--lakehouse.startup.max-warmup-time` | `5m` | Abort warmup safety valve |

## Query Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.query.max-concurrent` | `32` | Max concurrent queries |
| `--lakehouse.query.timeout` | `60s` | Per-query timeout |
| `--lakehouse.query.max-rows` | `10000000` | Max rows scanned per query (safety limit) |
| `--lakehouse.query.slow-threshold` | `5s` | Queries slower than this are logged |

## Circuit Breaker Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.circuit-breaker.threshold` | `5` | Consecutive S3 failures to open breaker |
| `--lakehouse.circuit-breaker.timeout` | `30s` | Time in open state before half-open probe |
| `--lakehouse.circuit-breaker.success-threshold` | `2` | Successful probes to close breaker |

## Tenant Settings

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.tenant.default-prefix` | `""` | S3 prefix for default (no tenant) queries |
| `--lakehouse.tenant.prefix-template` | `{AccountID}/{ProjectID}/` | S3 prefix template per tenant |

## Inherited VL/VT Flags

| Flag | Default | Description |
|---|---|---|
| `--httpListenAddr` | `:9428` / `:10428` (auto) | HTTP listen address |
| `--loggerLevel` | `INFO` | Log level (DEBUG, INFO, WARN, ERROR) |

## Timeout Summary

| Operation | Timeout | Retry | Notes |
|---|---|---|---|
| S3 single request | 30s | 3x exponential (200ms base) | Range read, HEAD, ListObjects |
| Query execution | 60s | No retry | Client gets 504 if exceeded |
| Storage node discovery poll | 10s | Next refresh cycle (5m) | Background |
| Peer cache request | 5s | Falls back to S3 | Must be < query timeout |
| SQS long poll | 20s | Immediate re-poll | AWS max |
| Circuit breaker open | 30s | Half-open probe after timeout | 2 successes to close |
| Startup max warmup | 5m | Goes ready with partial state | Background continues |
| Graceful shutdown drain | 30s | Force exit after 60s | In-flight queries drain |

## YAML Config Example

```yaml
lakehouse:
  mode: logs
  topology: auto

  s3:
    bucket: obs-archive
    region: us-east-1
    prefix: ""
    max_connections: 128
    timeout: 30s
    retry_max: 3
    retry_base_delay: 200ms

  cache:
    memory_limit: 512MB
    disk_path: /data/lakehouse/cache
    disk_limit: 50GB
    eviction_watermark: 0.8

  discovery:
    headless_service: vlstorage.monitoring.svc.cluster.local
    partition_auth_key: "${PARTITION_AUTH_KEY}"
    refresh_interval: 5m
    peer_headless_service: lakehouse-logs.monitoring.svc.cluster.local

  manifest:
    refresh_interval: 5m
    persist_path: /data/lakehouse

  prefetch:
    correlated: true
    read_ahead_depth: 2

  startup:
    serve_stale: false
    warmup_window: 24h
    max_warmup_time: 5m

  query:
    max_concurrent: 32
    timeout: 60s
    slow_threshold: 5s
```
