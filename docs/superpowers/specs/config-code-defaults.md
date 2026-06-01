# Configuration Code Defaults - Victoria Lakehouse

**Source File:** `internal/config/config.go`  
**Last Updated:** Based on code extraction  
**Phase:** 1 - Code Defaults Extraction

This document captures all configuration settings defined in the Go code as source of truth. Settings are extracted from the `Default()` function (lines 490-729) and supporting struct definitions.

---

## 1. Storage/S3 Configuration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `s3.bucket` | (no default) | string | struct field (195) | S3 bucket for storage | YAML only |
| `s3.region` | `us-east-1` | string | Default() L496 | AWS region for S3 | YAML only |
| `s3.prefix` | (empty) | string | struct field (197) | Path prefix in bucket | YAML only |
| `s3.endpoint` | (empty) | string | struct field (198) | Custom S3 endpoint URL | YAML only |
| `s3.access_key` | (empty) | string | struct field (199) | AWS access key ID | YAML only |
| `s3.secret_key` | (empty) | string | struct field (200) | AWS secret access key | YAML only |
| `s3.force_path_style` | false | bool | struct field (201) | Use path-style S3 URLs | YAML only |
| `s3.max_connections` | 128 | int | Default() L497 | Max concurrent S3 connections | YAML only |
| `s3.timeout` | 30s | time.Duration | Default() L498 | S3 operation timeout | YAML only |
| `s3.retry_max` | 3 | int | Default() L499 | Max S3 request retries | YAML only |
| `s3.retry_base_delay` | 200ms | time.Duration | Default() L500 | Base delay for S3 retries | YAML only |
| `s3.max_concurrent_downloads` | 16 | int | Default() L501 | Max parallel S3 downloads (deprecated) | YAML only |
| `s3.concurrent_downloads_request` | 0 | int | struct field (214) | K8s-style request baseline | YAML only |
| `s3.concurrent_downloads_limit` | 0 | int | struct field (215) | K8s-style hard ceiling | YAML only |
| `s3.concurrent_downloads_scaling` | (empty) | string | struct field (216) | K8s-style ramp policy | YAML only |
| `s3.read_ahead_bytes` | 2MB | int | Default() L502 | Read-ahead buffer size | YAML only |
| `s3.coalesce_gap_bytes` | 64KB | int | Default() L503 | Gap threshold for read coalescing | YAML only |

---

## 2. Cache Configuration (L1 Memory + L2 Disk)

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `cache.memory_limit` | `512MB` | string | Default() L507 | L1 in-memory cache size | YAML only |
| `cache.disk_path` | `/data/lakehouse/cache` | string | Default() L508 | L2 disk cache directory | YAML only |
| `cache.disk_limit` | `50GB` | string | Default() L509 | L2 disk cache size limit | YAML only |
| `cache.eviction_watermark` | 0.8 | float64 | Default() L510 | Trigger eviction at 80% full | YAML only |
| `cache.footer_ttl` | 1h | time.Duration | Default() L511 | Footer (metadata) cache TTL | YAML only |
| `cache.bloom_ttl` | 1h | time.Duration | Default() L512 | Bloom filter cache TTL | YAML only |
| `cache.page_ttl` | 10m | time.Duration | Default() L513 | Data page cache TTL | YAML only |
| `cache.warmup_partitions` | 0 | int | struct field (229) | Partitions to warm on startup | YAML only |
| `cache.warmup_max_files` | 0 | int | struct field (230) | Max files per warmup partition | YAML only |
| `cache.warmup_concurrency` | 0 | int | struct field (231) | Parallel warmup workers | YAML only |
| `cache.partition_mode` | `az-local` | string | Default() L514 | Cache partitioning strategy | YAML only |
| `cache.memory_request` | (empty) | string | struct field (239) | K8s-style request baseline | YAML only |
| `cache.memory_limit_v2` | (empty) | string | struct field (240) | K8s-style memory limit (v2) | YAML only |
| `cache.memory_scaling` | (empty) | string | struct field (241) | K8s-style scaling policy | YAML only |

---

## 3. Ingestion Configuration (WAL, Buffers, Compression)

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `insert.flush_interval` | 60s | time.Duration | Default() L582 | Buffer flush interval | YAML only |
| `insert.max_buffer_rows` | 50000 | int | Default() L583 | Max rows before flush | YAML only |
| `insert.max_buffer_bytes` | `256MB` | string | Default() L584 | Max buffer size before flush | YAML only |
| `insert.target_file_size` | `128MB` | string | Default() L585 | Target Parquet file size | YAML only |
| `insert.row_group_size` | 10000 | int | Default() L586 | Rows per Parquet row group | YAML only |
| `insert.bloom_columns` | `["service.name", "trace_id"]` | []string | Default() L587 | Columns with bloom filters | YAML only |
| `insert.compression_level` | 7 | int | Default() L588 | Zstandard compression level (1-22) | YAML only |
| `insert.wal_enabled` | true | bool | Default() L589 | Write-ahead log enabled | YAML only |
| `insert.wal_dir` | `/data/lakehouse/wal` | string | Default() L590 | WAL directory | YAML only |
| `insert.wal_max_bytes` | `512MB` | string | Default() L591 | WAL size limit | YAML only |
| `insert.ack_mode` | `buffer` | string | Default() L593 | Acknowledgment mode (buffer/wal/flush-sync) | YAML only |
| `insert.flush_linger` | 200ms | time.Duration | Default() L594 | Linger time before flush | YAML only |
| `insert.flush_max_rows` | 5000 | int | Default() L595 | Force flush at this row count | YAML only |
| `insert.peer_replicate` | false | bool | Default() L596 | Replicate to peer nodes | YAML only |
| `insert.peer_replicate_timeout` | 5ms | time.Duration | Default() L597 | Peer replication timeout | YAML only |
| `insert.peer_replicate_ttl` | 30s | time.Duration | Default() L598 | Replication result TTL | YAML only |
| `insert.async_wal_enabled` | false | bool | Default() L599 | Async WAL writes | YAML only |
| `insert.async_wal_batch_linger` | 50ms | time.Duration | Default() L600 | Batch linger for async WAL | YAML only |

---

## 4. Query Configuration (Concurrency, Timeouts, Performance)

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `query.max_concurrent` | 32 | int | Default() L570 | Max concurrent queries | YAML only |
| `query.file_workers` | 64 | int | Default() L571 | Max file reader goroutines (deprecated) | YAML only |
| `query.timeout` | 60s | time.Duration | Default() L572 | Query timeout | YAML only |
| `query.max_rows` | 10,000,000 | int64 | Default() L573 | Legacy hard row limit per query | YAML only |
| `query.max_files_per_query` | 0 | int | Default() L574 | Max files per query (0=unlimited) | YAML only |
| `query.max_live_bytes` | 512MB | int64 | Default() L577 | Max in-flight data block budget | YAML only |
| `query.slow_threshold` | 5s | time.Duration | Default() L578 | Threshold for slow query logging | YAML only |
| `query.file_workers_request` | 0 | int | struct field (323) | K8s-style request baseline | YAML only |
| `query.file_workers_limit` | 0 | int | struct field (324) | K8s-style hard ceiling | YAML only |
| `query.file_workers_scaling` | (empty) | string | struct field (325) | K8s-style scaling policy | YAML only |
| `query.max_rows_request` | 0 | int64 | struct field (332) | K8s-style request baseline | YAML only |
| `query.max_rows_limit` | 0 | int64 | struct field (333) | K8s-style hard ceiling | YAML only |
| `query.max_rows_scaling` | (empty) | string | struct field (334) | K8s-style scaling policy | YAML only |

---

## 5. Replication/HA Configuration

### Peer Discovery & Connectivity

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `peer.auth_key` | (empty) | string | struct field (273) | Auth key for peer communication | YAML only |
| `peer.timeout` | 5s | time.Duration | Default() L540 | Peer request timeout | YAML only |
| `peer.max_connections` | 32 | int | Default() L541 | Max peer connections | YAML only |
| `peer.az_aware` | true | bool | Default() L542 | Enable availability zone awareness | YAML only |
| `peer.az_mode` | `preferred` | string | Default() L543 | AZ preference mode (preferred/strict) | YAML only |
| `peer.cross_az_fallback` | true | bool | Default() L544 | Allow cross-AZ fallback on failures | YAML only |
| `peer.az_env_var` | `LAKEHOUSE_AZ` | string | Default() L545 | Environment variable for AZ name | YAML only |
| `peer.az_min_peers_per_az` | 2 | int | Default() L546 | Minimum peers required per AZ | YAML only |

### Discovery Service

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `discovery.headless_service` | (empty) | string | struct field (245) | K8s headless service for nodes | YAML only |
| `discovery.storage_nodes` | (empty) | []string | struct field (246) | Static node addresses | YAML only |
| `discovery.partition_auth_key` | (empty) | string | struct field (247) | Auth key for partition discovery | YAML only |
| `discovery.refresh_interval` | 5m | time.Duration | Default() L518 | Node discovery refresh interval | YAML only |
| `discovery.timeout` | 10s | time.Duration | Default() L519 | Discovery request timeout | YAML only |
| `discovery.peer_headless_service` | (empty) | string | struct field (250) | K8s service for peer discovery | YAML only |
| `discovery.peer_refresh_interval` | 30s | time.Duration | Default() L520 | Peer discovery refresh interval | YAML only |
| `discovery.ring_stabilize_duration` | 60s | time.Duration | Default() L521 | Ring stabilization window | YAML only |
| `discovery.ring_change_notify` | true | bool | Default() L522 | Notify on topology changes | YAML only |

### Select/Query High Availability

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `select.buffer_query_enabled` | true | bool | Default() L604 | Query insert buffer while waiting | YAML only |
| `select.insert_headless_service` | (empty) | string | struct field (188) | K8s service for insert nodes | YAML only |
| `select.buffer_query_timeout` | 2s | time.Duration | Default() L605 | Timeout for buffer queries | YAML only |
| `select.az_aware` | true | bool | Default() L606 | Prefer same-AZ replicas | YAML only |
| `select.cross_az_fallback` | true | bool | Default() L607 | Fallback to cross-AZ on failure | YAML only |

---

## 6. Startup & Warmup Configuration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `startup.serve_stale` | false | bool | Default() L550 | Serve stale data during warmup | YAML only |
| `startup.warmup_window` | 24h | time.Duration | Default() L551 | Data age window for cache warmup | YAML only |
| `startup.max_warmup_time` | 5m | time.Duration | Default() L552 | Max time to wait for warmup | YAML only |
| `startup.peer_sync_timeout` | 30s | time.Duration | Default() L553 | Peer state sync timeout | YAML only |
| `startup.require_manifest_sync` | true | bool | Default() L554 | Block start until manifest synced | YAML only |
| `startup.stale_threshold` | 1h | time.Duration | Default() L555 | Max age of "stale" data | YAML only |
| `startup.wal_reconciliation` | true | bool | Default() L556 | Reconcile WAL on startup | YAML only |
| `startup.cache_revalidation` | true | bool | Default() L557 | Revalidate cache entries | YAML only |
| `startup.max_resync_time` | 10m | time.Duration | Default() L558 | Max time for full resync | YAML only |

---

## 7. Shutdown Configuration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `shutdown.delay` | 5s | time.Duration | Default() L562 | Initial shutdown delay | YAML only |
| `shutdown.max_graceful_duration` | 7s | time.Duration | Default() L563 | Grace period for drain | YAML only |
| `shutdown.flush_timeout` | 30s | time.Duration | Default() L564 | Time to flush buffers | YAML only |
| `shutdown.persist_timeout` | 10s | time.Duration | Default() L565 | Time to persist state | YAML only |
| `shutdown.release_timeout` | 5s | time.Duration | Default() L566 | Time to release resources | YAML only |

---

## 8. Observability Configuration

### Manifest & SQS Integration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `manifest.refresh_interval` | 5m | time.Duration | Default() L526 | Manifest metadata refresh rate | YAML only |
| `manifest.sqs_queue_url` | (empty) | string | struct field (258) | SQS queue for manifest events | YAML only |
| `manifest.sqs_region` | (empty) | string | struct field (259) | AWS region for SQS | YAML only |
| `manifest.sqs_wait_time` | 20s | time.Duration | Default() L527 | SQS long-poll wait time | YAML only |
| `manifest.persist_path` | `/data/lakehouse` | string | Default() L528 | Path for manifest persistence | YAML only |
| `manifest.persist_interval` | 5m | time.Duration | Default() L529 | Manifest persist interval | YAML only |

### Stats & Metrics

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `stats.enabled` | true | bool | Default() L676 | Enable stats collection | YAML only |
| `stats.push_interval` | 30s | time.Duration | Default() L677 | Push metrics interval | YAML only |
| `stats.push_compression` | true | bool | Default() L678 | Compress metrics before push | YAML only |
| `stats.snapshot_interval` | 5m | time.Duration | Default() L679 | Stats snapshot frequency | YAML only |
| `stats.snapshot_prefix` | `_meta/tenant-stats` | string | Default() L680 | S3 prefix for snapshots | YAML only |
| `stats.meta_bucket` | (empty) | string | struct field (375) | S3 bucket for metadata | YAML only |
| `stats.max_delta_count` | 1000 | int | Default() L681 | Max stats deltas before snapshot | YAML only |
| `stats.metrics_cardinality_limit` | 100 | int | Default() L682 | Metrics cardinality hard limit | YAML only |
| `stats.cardinality_warning_threshold` | 10000 | int | Default() L683 | Warning threshold for cardinality | YAML only |
| `stats.breakdown_labels` | `["service.name", "deployment.environment", "k8s.namespace.name", "k8s.cluster.name"]` | []string | Default() L684 | Labels for metric breakdown | YAML only |
| `stats.s3_lifecycle_rules` | (empty) | []LifecycleRuleConfig | struct field (380) | S3 lifecycle policies | YAML only |
| `stats.s3_price_per_gb` | STANDARD: $0.023, STANDARD_IA: $0.0125, GLACIER_IR: $0.004, GLACIER: $0.0036, DEEP_ARCHIVE: $0.00099 | map[string]float64 | Default() L685-690 | S3 storage pricing | YAML only |
| `stats.s3_request_prices` | PUT: $0.005, GET: $0.0004, LIST: $0.005 | map[string]float64 | Default() L692-696 | S3 API pricing | YAML only |
| `stats.s3_inventory_bucket` | (empty) | string | struct field (383) | S3 inventory bucket | YAML only |
| `stats.headobject_sample_interval` | 6h | time.Duration | Default() L697 | HeadObject sampling interval | YAML only |
| `stats.headobject_max_per_refresh` | 50 | int | Default() L698 | Max objects sampled per interval | YAML only |

### Telemetry

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `telemetry.enabled` | false | bool | Default() L709 | Enable distributed tracing | YAML only |
| `telemetry.endpoint` | (empty) | string | struct field (from TelemetryConfig) | OTLP exporter endpoint | YAML only |
| `telemetry.sample_rate` | 0.1 | float64 | Default() L710 | Trace sample rate (0.0-1.0) | YAML only |
| `telemetry.always_sample_slow` | true | bool | Default() L711 | Always sample slow traces | YAML only |
| `telemetry.service_name` | (empty) | string | struct field (from TelemetryConfig) | Service name for traces | YAML only |
| `telemetry.batch_timeout` | 5s | time.Duration | Default() L712 | Trace batch timeout | YAML only |

---

## 9. Data Management Configuration

### Compaction

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `compaction.enabled` | true | bool | Default() L624 | Enable background compaction | YAML only |
| `compaction.interval` | 5m | time.Duration | Default() L625 | Compaction check interval | YAML only |
| `compaction.max_concurrent` | 1 | int | Default() L626 | Max concurrent compactions | YAML only |
| `compaction.min_files_l0` | 10 | int | Default() L627 | Min files to trigger L0 compaction | YAML only |
| `compaction.min_files_l1` | 10 | int | Default() L628 | Min files to trigger L1+ compaction | YAML only |
| `compaction.min_age` | 1h | time.Duration | Default() L629 | Min file age before compaction | YAML only |
| `compaction.daily_rollup_age` | 24h | time.Duration | Default() L630 | Age at which to create daily rollup | YAML only |

### Deletion & Lifecycle

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `delete.enabled` | true | bool | Default() L634 | Enable deletion workflows | YAML only |
| `delete.default_mode` | `auto` | string | Default() L635 | Default deletion mode | YAML only |
| `delete.auto_rewrite_classes` | `["STANDARD"]` | []string | Default() L636 | Storage classes to auto-rewrite for deletion | YAML only |
| `delete.rewrite_delay` | 1h | time.Duration | Default() L637 | Delay before rewriting deleted objects | YAML only |
| `delete.rewrite_batch_size` | 50 | int | Default() L638 | Batch size for rewrite operations | YAML only |
| `delete.rewrite_max_concurrent` | 2 | int | Default() L639 | Max concurrent rewrites | YAML only |
| `delete.persist_path` | `/data/lakehouse/tombstones` | string | Default() L640 | Tombstone storage directory | YAML only |
| `delete.cost_warning_threshold` | 10.0 | float64 | Default() L641 | Cost threshold for deletion warnings | YAML only |
| `delete.force_glacier_header` | `X-Force-Glacier-Delete` | string | Default() L642 | Header to force Glacier transition | YAML only |
| `delete.verify_interval` | 6h | time.Duration | Default() L643 | Verify deletion completion interval | YAML only |
| `delete.lifecycle_rules` | (empty) | []LifecycleRuleConfig | struct field (427) | Storage class transition rules | YAML only |

### Garbage Collection

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `gc.enabled` | true | bool | Default() L647 | Enable garbage collection | YAML only |
| `gc.interval` | 6h | time.Duration | Default() L648 | GC scan interval | YAML only |
| `gc.orphan_grace_period` | 1h | time.Duration | Default() L649 | Grace period before orphan cleanup | YAML only |

### Retention

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `retention.enabled` | false | bool | Default() L670 | Enable retention policies | YAML only |
| `retention.default` | `90d` | string | Default() L671 | Default retention duration | YAML only |
| `retention.check_interval` | `1h` | string | Default() L672 | Retention check interval | YAML only |
| `retention.rules` | (empty) | []RetentionRule | struct field (468) | Label-based retention rules | YAML only |

---

## 10. Smart Cache Configuration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `smart_cache.max_age` | 24h | time.Duration | Default() L653 | Max age for smart cache entries | YAML only |
| `smart_cache.snapshot_interval` | 60s | time.Duration | Default() L654 | Snapshot/persist interval | YAML only |
| `smart_cache.query_grace_period` | 5m | time.Duration | Default() L655 | Grace period for query tracking | YAML only |
| `smart_cache.hot_access_threshold` | 3 | int | Default() L656 | Accesses to mark as "hot" | YAML only |
| `smart_cache.hot_window` | 10m | time.Duration | Default() L657 | Time window for hot tracking | YAML only |
| `smart_cache.target_hours` | 24 | int | Default() L658 | Target cache lifetime in hours | YAML only |
| `smart_cache.disk_limit_max` | `100GB` | string | Default() L659 | Max disk cache size (deprecated) | YAML only |
| `smart_cache.ingestion_rate_hint` | (empty) | string | struct field (443) | Ingestion rate hint for sizing | YAML only |
| `smart_cache.disk_request` | (empty) | string | struct field (449) | K8s-style request baseline | YAML only |
| `smart_cache.disk_limit` | (empty) | string | struct field (450) | K8s-style hard ceiling | YAML only |
| `smart_cache.disk_scaling` | (empty) | string | struct field (451) | K8s-style scaling policy | YAML only |

---

## 11. Cross-Signal Configuration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `cross_signal.enabled` | false | bool | Default() L663 | Enable cross-signal integration | YAML only |
| `cross_signal.endpoint` | (empty) | string | struct field (456) | Remote endpoint URL | YAML only |
| `cross_signal.headless_service` | (empty) | string | struct field (457) | K8s service for cross-signal | YAML only |
| `cross_signal.auth_key` | (empty) | string | struct field (458) | Auth key for cross-signal | YAML only |
| `cross_signal.timeout` | 2s | time.Duration | Default() L664 | Cross-signal request timeout | YAML only |
| `cross_signal.max_batch` | 100 | int | Default() L665 | Max events per batch | YAML only |
| `cross_signal.batch_interval` | 500ms | time.Duration | Default() L666 | Batch send interval | YAML only |

---

## 12. Prefetch Configuration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `prefetch.correlated` | true | bool | Default() L533 | Enable correlated prefetching | YAML only |
| `prefetch.read_ahead_depth` | 2 | int | Default() L534 | Read-ahead queue depth | YAML only |
| `prefetch.max_concurrent` | 8 | int | Default() L535 | Max prefetch workers | YAML only |
| `prefetch.max_queue` | 128 | int | Default() L536 | Max items in prefetch queue | YAML only |

---

## 13. Tenant Isolation Configuration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `tenant.default_prefix` | (empty) | string | struct field (338) | Default S3 prefix for tenants | YAML only |
| `tenant.prefix_template` | `{AccountID}/{ProjectID}/` | string | Default() L611 | Template for tenant prefixes | YAML only |
| `tenant.isolation` | `prefix` | string | Default() L612 | Isolation strategy (prefix/bucket) | YAML only |
| `tenant.bucket_template` | (empty) | string | struct field (341) | Template for per-tenant buckets | YAML only |
| `tenant.default_account` | `0` | string | Default() L613 | Default account ID | YAML only |
| `tenant.default_project` | `0` | string | Default() L614 | Default project ID | YAML only |
| `tenant.header_account` | `X-Scope-AccountID` | string | Default() L615 | HTTP header for account ID | YAML only |
| `tenant.header_project` | `X-Scope-ProjectID` | string | Default() L616 | HTTP header for project ID | YAML only |
| `tenant.global_read_header` | (empty) | string | struct field (346) | Header for global read access | YAML only |
| `tenant.global_read_value` | (empty) | string | struct field (347) | Value for global read access | YAML only |
| `tenant.global_read_token` | (empty) | string | struct field (348) | Token for global read access | YAML only |
| `tenant.known_tenants` | (empty) | []KnownTenant | struct field (349) | Static tenant configurations | YAML only |
| `tenant.orgid_header` | `X-Scope-OrgID` | string | Default() L617 | HTTP header for org ID | YAML only |
| `tenant.metrics_format` | `id` | string | Default() L618 | Metrics format (id/path) | YAML only |
| `tenant.auto_register` | false | bool | Default() L619 | Auto-register new tenants | YAML only |
| `tenant.alias_sync_interval` | 30s | time.Duration | Default() L620 | Sync interval for tenant aliases | YAML only |
| `tenant.aliases` | (empty) | map[string]AliasTarget | struct field (354) | Tenant alias mappings | YAML only |

---

## 14. Schema & Mode Configuration

### Schema Configuration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `schema.extra_promoted` | (empty) | []ExtraPromotedColumn | struct field (483) | Extra promoted columns | YAML only |

### Logs Mode

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `logs.bloom_columns` | `["service.name", "trace_id"]` | []string | Default() L716 | Bloom filter columns for logs | YAML only |
| `logs.delete_prefix` | `/delete/logsql` | string | Default() L717 | Delete API prefix for logs | YAML only |
| `logs.compat_version` | (empty) | string | Default() L718 | Compatibility version override | YAML only |
| `logs.profile` | (empty) | Profile | struct field (77) | Profile for logs mode | YAML only |
| `logs.insert.profile` | (empty) | Profile | struct field (78) | Insert role profile for logs | YAML only |
| `logs.select.profile` | (empty) | Profile | struct field (79) | Select role profile for logs | YAML only |

### Traces Mode

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `traces.bloom_columns` | `["trace_id", "service.name"]` | []string | Default() L722 | Bloom filter columns for traces | YAML only |
| `traces.delete_prefix` | `/delete/tracessql` | string | Default() L723 | Delete API prefix for traces | YAML only |
| `traces.compat_version` | (empty) | string | Default() L724 | Compatibility version override | YAML only |
| `traces.jaeger_enabled` | true | bool | Default() L725 | Enable Jaeger protocol support | YAML only |
| `traces.jaeger_grpc_addr` | `:16685` | string | Default() L726 | Jaeger gRPC listen address | YAML only |
| `traces.profile` | (empty) | Profile | struct field (88) | Profile for traces mode | YAML only |
| `traces.insert.profile` | (empty) | Profile | struct field (89) | Insert role profile for traces | YAML only |
| `traces.select.profile` | (empty) | Profile | struct field (90) | Select role profile for traces | YAML only |

---

## 15. UI & Top-Level Configuration

### UI Configuration

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `ui.enabled` | true | bool | Default() L702 | Enable web UI | YAML only |
| `ui.vmui_tab` | true | bool | Default() L703 | Show VictoriaMetrics UI tab | YAML only |
| `ui.refresh_default` | 0 | int | Default() L704 | Default UI refresh interval (ms) | YAML only |
| `ui.theme` | `auto` | string | Default() L705 | UI theme (auto/dark/light) | YAML only |

### Top-Level Settings

| Setting Name | Code Default | Data Type | Source | Purpose | Runtime Override |
|---|---|---|---|---|---|
| `mode` | (required) | Mode | struct field (40) | Operation mode (logs/traces) | YAML only |
| `role` | `all` | Role | Default() L492 | Node role (all/insert/select) | YAML only |
| `topology` | `auto` | Topology | Default() L493 | Topology detection mode | YAML only |
| `hot_boundary` | (empty) | string | struct field (48) | Hot/cold data boundary duration | YAML only |
| `profile` | (empty) | Profile | struct field (43) | Performance profile (balanced/max-performance/max-durability/max-cost-savings/dev) | YAML only |

---

## 16. Computed Defaults (from helper functions)

These values are derived from code when string values are not set:

| Setting Name | Computed Default | Condition | Source |
|---|---|---|---|
| `MaxBufferBytesN()` | 256 MB | when `insert.max_buffer_bytes` is empty/invalid | L151 |
| `TargetFileSizeN()` | 128 MB | when `insert.target_file_size` is empty/invalid | L159 |
| `WALMaxBytesN()` | 512 MB | when `insert.wal_max_bytes` is empty/invalid | L167 |
| `CacheMemoryBytes()` | 512 MB | when `cache.memory_limit` is empty/invalid | L1067 |
| `CacheDiskBytes()` | 50 GB | when `cache.disk_limit` is empty/invalid | L1075 |
| `ListenAddr()` | `:10428` (traces) or `:9428` (logs) | based on `mode` | L1082-1084 |
| `DefaultPort()` | `10428` (traces) or `9428` (logs) | based on `mode` | L1089-1091 |
| `ActiveDeletePrefix()` | `/delete/tracessql` or `/delete/logsql` | based on `mode` or mode-specific override | L103-114 |
| `ActiveBloomColumns()` | mode or insert defaults | based on mode hierarchy | L93-101 |

---

## Notes on Source Traceability

- **Default() function (lines 490-729)**: Primary source for hardcoded defaults returned by `Default()` constructor
- **Struct field definitions (lines 39-91)**: Field definitions with YAML tags
- **Helper functions**: Computed values and fallbacks
- **Profile-based defaults**: Additional defaults applied based on selected profile (see `profiles.go`)

---

## Validation Constraints

Important constraints enforced by validation (lines 781-1000):

- `s3.bucket` is **required**
- `s3.endpoint` must be http/https (SSRF protected)
- `insert.compression_level` must be 1-22
- `insert.ack_mode` must be one of: buffer, wal, flush-sync
- `cache.eviction_watermark` must be in (0, 1]
- `cache.partition_mode` must be: az-local, global, or distributed
- `peer.az_mode` must be: preferred or strict
- `topology` must be: auto, storage-node, direct, or loki-proxy
- `ui.theme` must be: auto, dark, or light
- When `isolation=bucket`, `bucket_template` is required
- Compaction/GC interval/concurrency must be positive when enabled
- Stats cardinality limits must be >= 0

