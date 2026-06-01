# Configuration Documentation Defaults - Victoria Lakehouse

**Source File:** `docs/configuration.md`  
**Last Updated:** Based on documentation extraction  
**Phase:** 1 - Documentation Defaults Extraction

This document captures all configuration settings documented in `docs/configuration.md` as the user-facing source of truth. Settings are extracted from configuration tables, profile tuning matrix, and descriptive sections.

---

## 1. Storage/S3 Configuration

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `s3.bucket` | **(required)** | N/A | Must be specified before startup | S3 Settings (line 80) |
| `s3.region` | `us-east-1` | AWS region names | Default AWS region; override for non-US buckets | S3 Settings (line 81) |
| `s3.prefix` | `""` (auto-set from mode) | N/A | Auto-set to `logs/` or `traces/` based on mode | S3 Settings (line 82) |
| `s3.endpoint` | `""` (empty) | Must be http/https if set | Custom endpoint for MinIO, R2, or S3-compatible | S3 Settings (line 83) |
| `s3.access_key` | `""` (empty) | N/A | Prefer IAM role/IRSA over static keys | S3 Settings (line 84) |
| `s3.secret_key` | `""` (empty) | N/A | Prefer IAM role/IRSA over static keys | S3 Settings (line 85) |
| `s3.force_path_style` | `false` | bool | Required for MinIO; not needed for AWS S3 | S3 Settings (line 86) |
| `s3.max_connections` | `128` | Positive integer | Scales with expected S3 concurrency | S3 Settings (line 87) |
| `s3.timeout` | `30s` | Duration | Per-request timeout for S3 operations (Range Read, HEAD, ListObjects) | S3 Settings (line 88) |
| `s3.retry_max` | `3` | Positive integer | 3x exponential backoff with 200ms base | S3 Settings (line 89) |
| `s3.retry_base_delay` | `200ms` | Duration | Base delay that doubles each retry | S3 Settings (line 90) |

---

## 2. Cache Configuration (L1 Memory + L2 Disk)

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `cache.memory_limit` | `512MB` | Profile varies: 64MB (dev) - 2GB (max-performance) | L1 in-memory cache; trade memory for fewer S3 requests | Cache Settings (line 96) & Profile Tuning (line 61) |
| `cache.disk_path` | `/data/lakehouse/cache` | Local filesystem path | Must have sufficient disk space and I/O; persistent across restarts | Cache Settings (line 97) |
| `cache.disk_limit` | `50GB` | Profile varies: 1GB (dev) - 100GB (max-performance) | L2 disk cache; trade disk for fewer S3 requests | Cache Settings (line 98) & Profile Tuning (line 61) |
| `cache.eviction_watermark` | `0.8` | Range (0, 1] | Start evicting entries when disk is 80% full | Cache Settings (line 99) |
| `cache.footer_ttl` | `1h` | Duration | TTL for Parquet footer metadata cache | Cache Settings (line 100) |
| `cache.bloom_ttl` | `1h` | Duration | TTL for bloom filter cache | Cache Settings (line 101) |
| `cache.page_ttl` | `10m` | Duration | TTL for hot data page cache | Cache Settings (line 102) |

---

## 3. Ingestion Configuration (WAL, Buffers, Compression)

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `insert.flush_interval` | `10s` | Range: 1s-60s | Periodic flush interval; shorter = lower latency, higher S3 requests | Insert Settings (line 128) |
| `insert.flush_linger` | `200ms` | Duration | Delay before flushing to coalesce small writes; reduces request count | Insert Settings (line 129) |
| `insert.flush_max_rows` | `5000` | Positive integer | Force flush when batch reaches this size | Insert Settings (line 130) |
| `insert.max_buffer_rows` | `50000` | Per-partition row limit | Memory/latency trade-off; larger = fewer flushes | Insert Settings (line 131) |
| `insert.max_buffer_bytes` | `256MB` | Size limit | Total buffer memory limit across all partitions | Insert Settings (line 132) |
| `insert.target_file_size` | `128MB` | Size target | Adaptive flush trigger for Parquet file optimization | Insert Settings (line 133) |
| `insert.row_group_size` | `10000` | Rows per group | Parquet row group size; trade memory for compression ratio | Insert Settings (line 134) |
| `insert.bloom_columns` | `service.name,trace_id` | CSV of column names | Columns indexed with bloom filters for fast filtering | Insert Settings (line 135) |
| `insert.compression_level` | `7` | Range: 1-22 | ZSTD compression: 1=fastest, 22=best ratio. Profile varies: 1 (dev) - 11 (max-cost-savings) | Insert Settings (line 136) & Profile Tuning (line 60) |
| `insert.ack_mode` | `buffer` | `buffer` or `flush-sync` | **buffer**: HTTP 200 after buffering (~0ms latency); **flush-sync**: after S3 confirmed (+200-400ms, zero data loss) | Acknowledgement modes (lines 144-149) |
| `insert.peer_replicate` | `false` | bool | Replicate writes to peer nodes for HA | Insert Settings (line 138) |
| `insert.peer_replicate_timeout` | `5ms` | Duration | Timeout for peer replication | Insert Settings (line 139) |
| `insert.peer_replicate_ttl` | `30s` | Duration | TTL for replicated data on peers | Insert Settings (line 140) |
| `insert.async_wal_enabled` | `false` | bool | Async WAL writes (faster but less durable) | Insert Settings (line 141) |
| `insert.async_wal_batch_linger` | `50ms` | Duration | Batch coalesce delay for async WAL | Insert Settings (line 142) |

### WAL Configuration

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `insert.wal_enabled` | `true` | bool | Enable for crash recovery; disable for performance. Profile varies: on/off by profile | WAL Settings (line 155) & Profile Tuning (line 58) |
| `insert.wal_dir` | `/data/lakehouse/wal` | Local filesystem path | WAL file directory; must have stable storage | WAL Settings (line 156) |
| `insert.wal_max_bytes` | `512MB` | Size limit | Maximum WAL file size before rotation | WAL Settings (line 157) |

---

## 4. Query Configuration (Concurrency, Timeouts, Performance)

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `query.max_concurrent` | `32` | Positive integer | Max concurrent queries; limit for resource fairness | Query Settings (line 253) |
| `query.file_workers` | `8` | Positive integer | Concurrent Parquet files processed per query | Query Settings (line 257) |
| `query.timeout` | `60s` | Duration | Per-query timeout; client gets 504 if exceeded | Query Settings (line 254) |
| `query.max_rows` | `10000000` (10M) | Positive integer | Max rows scanned per query (safety limit) | Query Settings (line 255) |
| `query.slow_threshold` | `5s` | Duration | Queries slower than this are logged | Query Settings (line 256) |

---

## 5. Replication/HA Configuration

### Discovery & Storage Node Connection

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `discovery.headless_service` | `""` (empty) | K8s service name | K8s headless service for vlstorage/vtstorage nodes | Discovery Settings (line 108) |
| `discovery.storage_nodes` | `""` (empty) | Comma-separated addresses | Static node addresses (alternative to headless service) | Discovery Settings (line 109) |
| `discovery.partition_auth_key` | `""` (empty) | Auth token | Auth key for `/internal/partition/list` endpoint | Discovery Settings (line 110) |
| `discovery.refresh_interval` | `5m` | Duration | How often to poll storage nodes | Discovery Settings (line 111) |
| `discovery.timeout` | `10s` | Duration | Timeout per storage node poll | Discovery Settings (line 112) |
| `discovery.peer_headless_service` | `""` (empty) | K8s service name | K8s headless service for peer cache fleet | Discovery Settings (line 113) |
| `discovery.peer_refresh_interval` | `30s` | Duration | Peer DNS refresh interval | Discovery Settings (line 114) |

### Select/Query High Availability

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `select.buffer_query_enabled` | `true` | bool | Query insert pods for unflushed data during selects | Select Settings (line 163) |
| `select.insert_headless_service` | `""` (empty) | K8s service name | K8s headless service for insert pod discovery | Select Settings (line 164) |
| `select.buffer_query_timeout` | `2s` | Duration | Timeout for buffer query fan-out | Select Settings (line 165) |
| `select.az_aware` | `true` | bool | Prefer same-AZ insert pods for buffer queries | Select Settings (line 166) |
| `select.cross_az_fallback` | `true` | bool | Fall back to other AZs if same-AZ unavailable | Select Settings (line 167) |

### Peer Cache Settings

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `peer.timeout` | `5s` | Duration | Timeout for peer cache requests; must be < query timeout | Peer Cache Settings (line 238) |
| `peer.max_connections` | `32` | Positive integer | Max HTTP connections per peer | Peer Cache Settings (line 239) |

---

## 6. Startup & Warmup Configuration

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `startup.serve_stale` | `false` | bool | Serve from disk cache before S3 refresh; lower latency but stale data | Startup Settings (line 245) |
| `startup.warmup_window` | `24h` | Duration | Pre-cache footers/blooms for recent data | Startup Settings (line 246) |
| `startup.max_warmup_time` | `5m` | Duration | Abort warmup safety valve; goes ready with partial state | Startup Settings (line 247) |

---

## 7. Data Management Configuration

### Compaction

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `compaction.enabled` | `false` | bool | Disable by default; enable for > few hours of data. Enabled by max-performance and max-durability profiles | Compaction Settings (line 278) & Guidance (line 289) |
| `compaction.interval` | `5m` | Duration | How often the scheduler scans for eligible partitions | Compaction Settings (line 279) |
| `compaction.max_concurrent` | `1` | Positive integer | Max partitions compacted per scan | Compaction Settings (line 280) |
| `compaction.min_files_l0` | `10` | Positive integer | L0→L1 compaction threshold (number of L0 files per partition) | Compaction Settings (line 281) |
| `compaction.min_files_l1` | `10` | Positive integer | L1→L2 compaction threshold (number of L1 files per partition) | Compaction Settings (line 282) |
| `compaction.min_age` | `1h` | Duration | Minimum partition age before it is eligible for compaction | Compaction Settings (line 283) |
| `compaction.leader_election` | `auto` | `auto`, `k8s`, `s3`, `none` | Election mode: **auto** uses K8s if available, else S3, else none; **k8s** requires RBAC; **s3** uses lock with liveness; **none** for single-instance | Leader election modes (lines 291-298) |
| `compaction.lease_duration` | `15s` | Duration | K8s Lease duration (k8s election mode) | Compaction Settings (line 285) |
| `compaction.s3_lock_ttl` | `60s` | Duration | S3 lock TTL before another instance may steal it (s3 election mode) | Compaction Settings (line 286) |
| `compaction.s3_heartbeat` | `15s` | Duration | S3 lock heartbeat interval (s3 election mode) | Compaction Settings (line 287) |

### Delete/Lifecycle

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `delete.enabled` | `true` | bool | Enable delete API endpoints | Delete Settings (line 304) |
| `delete.default_mode` | `auto` | `hide`, `permanent`, `auto` | Default deletion mode | Delete Settings (line 305) |
| `delete.auto_rewrite_classes` | `STANDARD` | Storage class names | Storage classes eligible for rewrite | Delete Settings (line 306) |
| `delete.rewrite_delay` | `1h` | Duration | Wait before rewriting files after tombstone | Delete Settings (line 307) |
| `delete.rewrite_batch_size` | `50` | Positive integer | Max files per rewrite batch | Delete Settings (line 308) |
| `delete.rewrite_max_concurrent` | `2` | Positive integer | Max concurrent rewrite workers | Delete Settings (line 309) |
| `delete.persist_path` | `/data/lakehouse/tombstones` | Local filesystem path | Tombstone persistence directory | Delete Settings (line 310) |
| `delete.cost_warning_threshold` | `10.0` | Dollar amount | Cost ($) that triggers a warning | Delete Settings (line 311) |
| `delete.verify_interval` | `6h` | Duration | Continuous verification interval | Delete Settings (line 312) |

### Garbage Collection

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `gc.enabled` | `true` | bool | Enable orphan file garbage collection. Disabled by max-cost-savings and dev profiles | GC Settings (line 318) & Guidance (line 322) |
| `gc.interval` | `6h` | Duration | How often to scan for orphan files | GC Settings (line 319) |
| `gc.orphan_grace_period` | `1h` | Duration | Grace period before deleting orphans | GC Settings (line 320) |

### Retention

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `retention.enabled` | `false` | bool | Enable automatic data retention. Enabled by max-durability and max-cost-savings profiles | Retention Settings (line 328) & Guidance (line 332) |
| `retention.default` | `90d` | Duration | Default retention period | Retention Settings (line 329) |
| `retention.check_interval` | `1h` | Duration | How often to check for expired data | Retention Settings (line 330) |

---

## 8. Observability Configuration

### Manifest & S3 Integration

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `manifest.refresh_interval` | `5m` | Duration | S3 ListObjects polling interval | Manifest Settings (line 192) |
| `manifest.sqs_queue_url` | `""` (empty) | SQS queue URL | Optional SQS queue for S3 event notifications | Manifest Settings (line 193) |
| `manifest.sqs_region` | (inherited from `s3.region`) | AWS region | SQS queue region (defaults to S3 region) | Manifest Settings (line 194) |
| `manifest.sqs_wait_time` | `20s` | Duration (AWS max) | SQS long-poll wait time | Manifest Settings (line 195) |
| `manifest.persist_path` | `/data/lakehouse` | Local filesystem path | Directory for persisted manifest + index | Manifest Settings (line 196) |
| `manifest.persist_interval` | `5m` | Duration | How often to write manifest to disk | Manifest Settings (line 197) |

### Stats & Metrics

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `stats.enabled` | `true` | bool | Enable tenant statistics collection. Disabled by max-cost-savings and dev profiles | Stats Settings (line 338) & Guidance (line 346) |
| `stats.push_interval` | `30s` | Duration | Delta broadcast interval to peers | Stats Settings (line 339) |
| `stats.push_compression` | `true` | bool | ZSTD-compress delta broadcasts | Stats Settings (line 340) |
| `stats.snapshot_interval` | `5m` | Duration | Full registry snapshot to S3 | Stats Settings (line 341) |
| `stats.snapshot_prefix` | `_meta/tenant-stats` | S3 key prefix | S3 key prefix for snapshots | Stats Settings (line 342) |
| `stats.max_delta_count` | `1000` | Positive integer | Force full sync after N deltas | Stats Settings (line 343) |
| `stats.metrics_cardinality_limit` | `100` | Positive integer | Max tenant label values in metrics | Stats Settings (line 344) |

---

## 9. Smart Cache Configuration

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `smart_cache.max_age` | `24h` | Duration | Maximum TTL for cached entries | Smart Cache Settings (line 212) |
| `smart_cache.snapshot_interval` | `60s` | Duration | How often to persist cache metadata to disk | Smart Cache Settings (line 213) |
| `smart_cache.query_grace_period` | `5m` | Duration | Keep pinned entries after query completes | Smart Cache Settings (line 214) |
| `smart_cache.hot_access_threshold` | `3` | Positive integer | Accesses within hot window to mark entry as "hot" | Smart Cache Settings (line 215) |
| `smart_cache.hot_window` | `10m` | Duration | Window for counting hot accesses | Smart Cache Settings (line 216) |
| `smart_cache.target_hours` | `24` | Positive integer | Target hours of query coverage for cache sizing | Smart Cache Settings (line 217) |
| `smart_cache.disk_limit_max` | `100GB` | Size limit | Hard cap on disk cache size | Smart Cache Settings (line 218) |
| `smart_cache.ingestion_rate_hint` | `""` (empty) | Size hint (e.g., `500MB`) | Optional hint for ingestion rate to bootstrap cache sizing | Smart Cache Settings (line 219) |

---

## 10. Cross-Signal Settings

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `cross_signal.enabled` | `false` | bool | Enable cross-signal prefetch hints | Cross-Signal Settings (line 225) |
| `cross_signal.endpoint` | `""` (empty) | HTTP URL | URL of the other signal's lakehouse (e.g., `http://lakehouse-traces:10428`) | Cross-Signal Settings (line 226) |
| `cross_signal.headless_service` | `""` (empty) | K8s service name | Headless service for peer discovery (alternative to endpoint) | Cross-Signal Settings (line 227) |
| `cross_signal.auth_key` | `""` (empty) | Auth token | Shared secret for cross-signal HTTP | Cross-Signal Settings (line 228) |
| `cross_signal.timeout` | `2s` | Duration | Timeout for cross-signal HTTP requests | Cross-Signal Settings (line 229) |
| `cross_signal.max_batch` | `100` | Positive integer | Max trace IDs per hint batch | Cross-Signal Settings (line 230) |
| `cross_signal.batch_interval` | `500ms` | Duration | Flush interval for hint batching | Cross-Signal Settings (line 231) |

---

## 11. Prefetch Configuration

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `prefetch.correlated` | `true` | bool | Enable cross-signal prefetch | Prefetch Settings (line 203) |
| `prefetch.read_ahead_depth` | `2` | Partitions | Partitions to prefetch for sequential scans | Prefetch Settings (line 204) |
| `prefetch.max_concurrent` | `4` | Positive integer | Max concurrent prefetch downloads | Prefetch Settings (line 205) |
| `prefetch.max_queue` | `64` | Positive integer | Max pending prefetch tasks | Prefetch Settings (line 206) |

---

## 12. Tenant Isolation Configuration

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `tenant.default_prefix` | `""` (empty) | S3 prefix path | S3 prefix for default (no tenant) queries | Tenant Settings (line 271) |
| `tenant.prefix_template` | `{AccountID}/{ProjectID}/` | Template string | S3 prefix template per tenant | Tenant Settings (line 272) |

---

## 13. Schema Extensibility

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `schema.extra_promoted` | `""` (empty) | YAML list of columns | User-defined promoted Parquet columns with optional bloom filters | Schema Extensibility (lines 169-186) |

---

## 14. Hot Boundary & Core Settings

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `hot_boundary` | `""` (auto-discover) | Duration (e.g., `7d`, `168h`) | Manual hot boundary override; empty = auto-discovery from vlstorage/vtstorage | Hot Boundary (lines 120-122) |
| `config` | `""` (none) | File path | Path to YAML config file | Core Settings (line 72) |
| `role` | `all` | `all`, `insert`, `select` | Component role | Core Settings (line 73) |
| `topology` | `auto` | `auto`, `storage-node`, `direct`, `loki-proxy` | Topology detection mode | Core Settings (line 74) |

---

## 15. UI Settings

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `ui.enabled` | `true` | bool | Serve Lakehouse Explorer at `/lakehouse/ui/`. Disabled by max-cost-savings profile | UI Settings (line 352) & Guidance (line 356) |
| `ui.vmui_tab` | `true` | bool | Inject tab into VL/VT VMUI navigation | UI Settings (line 353) |
| `ui.theme` | `auto` | `auto`, `dark`, `light` | Color theme | UI Settings (line 354) |

---

## 16. Inherited VL/VT Flags

| Setting Name | Documented Default | Documented Range/Constraint | Documented Trade-off | Doc Section/Line Reference |
|---|---|---|---|---|
| `httpListenAddr` | `:9428` / `:10428` (auto) | Address:port | HTTP listen address (auto-set based on mode) | Inherited VL/VT Flags (line 362) |
| `loggerLevel` | `INFO` | `DEBUG`, `INFO`, `WARN`, `ERROR` | Log level | Inherited VL/VT Flags (line 363) |

---

## 17. Profile Configuration

| Flag | Default | Description | Profile Settings |
|---|---|---|---|
| `profile` | `""` (balanced) | Configuration profile preset | Configuration Profiles (line 36) |

**Available profiles:** `balanced`, `max-performance`, `max-durability`, `max-cost-savings`, `dev` (line 39)

**Precedence:** explicit flag > config file > profile defaults > built-in defaults (line 41)

**Profile Tuning Summary** (lines 55-66):

| Setting Area | balanced | max-performance | max-durability | max-cost-savings | dev |
|---|---|---|---|---|---|
| **ack_mode** | buffer | buffer | flush-sync | buffer | buffer |
| **WAL** | on | off | on (1GB) | off | off |
| **flush_linger** | 200ms | 100ms | 0 (immediate) | 1s | 0 |
| **Compression** | ZSTD-7 | ZSTD-3 | ZSTD-7 | ZSTD-11 | ZSTD-1 |
| **Cache (mem/disk)** | 512MB/50GB | 2GB/100GB | 512MB/50GB | 128MB/10GB | 64MB/1GB |
| **Compaction** | off | on (aggressive) | on | off | off |
| **GC** | on (6h) | on (3h) | on (1h) | off | off |
| **Retention** | off | off | on | on | off |
| **Stats** | on | on | on | off | off |
| **Cross-signal** | off | on | off | off | off |

---

## 18. Timeout Summary

Documented timeout specifications and their implications (lines 365-376):

| Operation | Documented Timeout | Retry Strategy | Notes |
|---|---|---|---|
| S3 single request | 30s | 3x exponential (200ms base) | Range read, HEAD, ListObjects |
| Query execution | 60s | No retry | Client gets 504 if exceeded |
| Storage node discovery poll | 10s | Next refresh cycle (5m) | Background; non-blocking |
| Peer cache request | 5s | Falls back to S3 | Must be < query timeout (60s) |
| SQS long poll | 20s | Immediate re-poll | AWS max wait time |
| Circuit breaker open | 30s | Half-open probe after timeout | 2 successes to close |
| Startup max warmup | 5m | Goes ready with partial state | Background continues |
| Graceful shutdown drain | 30s | Force exit after 60s | In-flight queries drain |

---

## Gaps & Notable Observations

### Settings Documented but Not Yet Mapped
These settings appear in documentation but may need cross-reference with code defaults:
- **Hot Boundary override** (`hot_boundary`): Explicitly shows it's documented as auto-discover when empty, with manual override option. Code default comparison needed.

### Settings in Code (Task 1) Not Yet in Documentation
These are documented in Task 1 code defaults but appear to be missing from `docs/configuration.md`:
- S3: `concurrent_downloads_request`, `concurrent_downloads_limit`, `concurrent_downloads_scaling`, `read_ahead_bytes`, `coalesce_gap_bytes`
- Cache: `warmup_partitions`, `warmup_max_files`, `warmup_concurrency`, `partition_mode`, `memory_request`, `memory_limit_v2`, `memory_scaling`
- Query: `max_files_per_query`, `max_live_bytes`, `file_workers_request`, `file_workers_limit`, `file_workers_scaling`, `max_rows_request`, `max_rows_limit`, `max_rows_scaling`
- Peer: `auth_key`, `az_aware`, `az_mode`, `cross_az_fallback`, `az_env_var`, `az_min_peers_per_az`
- Startup: `peer_sync_timeout`, `require_manifest_sync`, `stale_threshold`, `wal_reconciliation`, `cache_revalidation`, `max_resync_time`
- Shutdown: `delay`, `max_graceful_duration`, `flush_timeout`, `persist_timeout`, `release_timeout`
- Stats: `meta_bucket`, `cardinality_warning_threshold`, `breakdown_labels`, `s3_lifecycle_rules`, `s3_price_per_gb`, `s3_request_prices`, `s3_inventory_bucket`, `headobject_sample_interval`, `headobject_max_per_refresh`
- Telemetry: All settings (`enabled`, `endpoint`, `sample_rate`, `always_sample_slow`, `service_name`, `batch_timeout`)
- Delete: `lifecycle_rules`, `force_glacier_header`
- Tenant: `bucket_template`, `default_account`, `default_project`, `header_account`, `header_project`, `orgid_header`, `metrics_format`, `auto_register`, `alias_sync_interval`, `aliases`, `global_read_header`, `global_read_value`, `global_read_token`, `known_tenants`
- Mode-specific: `logs.bloom_columns`, `logs.delete_prefix`, `logs.compat_version`, `logs.profile`, `logs.insert.profile`, `logs.select.profile`, `traces.bloom_columns`, `traces.delete_prefix`, `traces.compat_version`, `traces.jaeger_enabled`, `traces.jaeger_grpc_addr`, `traces.profile`, `traces.insert.profile`, `traces.select.profile`
- Compaction: `daily_rollup_age` (code has this, docs don't)
- Logging: `circuit_breaker.threshold`, `circuit_breaker.timeout`, `circuit_breaker.success_threshold`

### Key Trade-offs Documented
- **ACK Mode**: Documented trade-off between latency (~0ms for `buffer` vs +200-400ms for `flush-sync`) and durability (data-at-risk vs zero data loss)
- **Cache Sizes**: Profile-based trade-offs across all profiles; balances memory/disk usage vs S3 requests
- **Compression Levels**: Documented as ZSTD 1-22 scale; profiles show explicit per-level choices
- **Compaction**: Disabled by default; guidance to enable for > few hours of data; leader election modes trade coordination complexity

### Documentation Completeness
- Documentation is comprehensive for user-facing settings
- Several K8s-style resource scaling settings are in code but not documented
- Profile tuning is well-documented with explicit per-profile values
- Timeout summary provides good operational context
- YAML config example (lines 378-475) provides reference implementation

---

## Next Steps (Phase 2)

Compare this documentation extraction against:
1. Code defaults (Task 1: `config-code-defaults.md`)
2. Helm chart defaults (Task 3)
3. Identify gaps, misalignments, and drift between layers

