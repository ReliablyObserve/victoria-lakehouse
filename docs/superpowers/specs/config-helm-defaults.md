# Helm Chart Configuration Defaults

**Source:** `charts/victoria-lakehouse/values.yaml`

**Purpose:** Complete extraction of all Helm chart default values for comparison with code defaults (Task 1) and documentation defaults (Task 2).

**Format Notes:**
- All values are listed exactly as they appear in YAML (e.g., `50GB`, `512MB`, `2s`, `30s`)
- Full Helm key paths provided (e.g., `lakehouseConfig.s3.bucket` not just `bucket`)
- Schema constraints from `values.schema.json` are noted where defined
- Kubernetes resource specs (requests, limits, replicas) included
- Comments from YAML preserved as "Description"

---

## 1. Global & Helm Overrides

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `nameOverride` | `""` | string | | Chart name override |
| `fullnameOverride` | `""` | string | | Full chart name override |
| `global.imagePullSecrets` | `[]` | array | | Image pull secrets |
| `global.commonLabels` | `{}` | object | | Common labels for all resources |
| `global.commonAnnotations` | `{}` | object | | Common annotations for all resources |

---

## 2. Container Image Configuration

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `image.logs.repository` | `ghcr.io/reliablyobserve/lakehouse-logs` | string | | Logs container image repository |
| `image.traces.repository` | `ghcr.io/reliablyobserve/lakehouse-traces` | string | | Traces container image repository |
| `image.tag` | `""` | string | | Container image tag (empty = use chart AppVersion) |
| `image.pullPolicy` | `IfNotPresent` | string | | Image pull policy for containers |

---

## 3. Common Pod/Container Defaults

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `common.nodeSelector` | `{}` | object | | Node selector for pod placement |
| `common.tolerations` | `[]` | array | | Tolerations for pod scheduling |
| `common.affinity` | `{}` | object | | Pod affinity rules |
| `common.resources` | `{}` | object | | Container resource requests/limits |
| `common.podSecurityContext.runAsNonRoot` | `true` | boolean | | Run container as non-root user |
| `common.podSecurityContext.runAsUser` | `65534` | integer | | Pod runs as UID 65534 (nobody) |
| `common.podSecurityContext.runAsGroup` | `65534` | integer | | Pod runs as GID 65534 (nobody) |
| `common.podSecurityContext.fsGroup` | `65534` | integer | | Pod filesystem group |
| `common.podSecurityContext.seccompProfile.type` | `RuntimeDefault` | string | | Seccomp profile type |
| `common.securityContext.readOnlyRootFilesystem` | `true` | boolean | | Root filesystem read-only |
| `common.securityContext.allowPrivilegeEscalation` | `false` | boolean | | Prevent privilege escalation |
| `common.securityContext.capabilities.drop` | `["ALL"]` | array | | Drop all Linux capabilities |
| `common.securityContext.runAsNonRoot` | `true` | boolean | | Container runs as non-root |
| `common.securityContext.runAsUser` | `65534` | integer | | Container UID |
| `common.securityContext.runAsGroup` | `65534` | integer | | Container GID |
| `common.securityContext.seccompProfile.type` | `RuntimeDefault` | string | | Container seccomp profile |

---

## 4. Lakehouse Configuration - Shared (Applied to Logs & Traces)

### 4.1 Profiles & Topology

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.profile` | `""` | string | enum: `["", "balanced", "max-performance", "max-durability", "max-cost-savings", "dev"]` | Configuration profile preset. Sets defaults for all settings below. Any explicit setting overrides profile. |
| `lakehouseConfig.topology` | `auto` | string | | Deployment topology detection mode |
| `lakehouseConfig.hot_boundary` | `""` | string | | Static hot boundary override (e.g., "30d"). Empty = auto-detect from vlstorage. |

### 4.2 S3 — Object Storage Backend

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.s3.bucket` | `""` | string | minLength: 1 (schema requires non-empty) | S3 bucket name (required) |
| `lakehouseConfig.s3.region` | `us-east-1` | string | minLength: 1 (schema requires non-empty) | AWS region |
| `lakehouseConfig.s3.prefix` | `""` | string | | S3 key prefix. Empty = auto-prefix from mode+tenant. |
| `lakehouseConfig.s3.endpoint` | `""` | string | | Custom S3 endpoint (e.g., "http://minio:9000" for MinIO) |
| `lakehouseConfig.s3.access_key` | `""` | string | | S3 access key. Prefer IRSA/IAM roles in production. |
| `lakehouseConfig.s3.secret_key` | `""` | string | | S3 secret key |
| `lakehouseConfig.s3.force_path_style` | `false` | boolean | | Force path-style S3 URLs (required for MinIO) |
| `lakehouseConfig.s3.max_connections` | `128` | integer | | Maximum HTTP connections to S3 |
| `lakehouseConfig.s3.timeout` | `30s` | duration | | S3 request timeout |
| `lakehouseConfig.s3.retry_max` | `3` | integer | | Maximum retry attempts for failed S3 requests |
| `lakehouseConfig.s3.retry_base_delay` | `200ms` | duration | | Base delay between retries (exponential backoff) |
| `lakehouseConfig.s3.max_concurrent_downloads` | `16` | integer | | DEPRECATED: use concurrent_downloads_request/limit. Removed in v1.0. |
| `lakehouseConfig.s3.concurrent_downloads_request` | `4` | integer | | K8s-style request: always-reserved baseline S3 download concurrency. Defaults to limit/4 when only legacy max_concurrent_downloads is set. |
| `lakehouseConfig.s3.concurrent_downloads_limit` | `16` | integer | | K8s-style limit: hard ceiling on concurrent S3 downloads |
| `lakehouseConfig.s3.concurrent_downloads_scaling` | `fixed` | string | | K8s-style scaling policy: fixed (default) \| linear \| expbackoff. Linear and expbackoff reserved for future signal-driven scaling. |
| `lakehouseConfig.s3.read_ahead_bytes` | `0` | integer | | S3 read-ahead buffer size in bytes (0 = use default 2MB) |
| `lakehouseConfig.s3.coalesce_gap_bytes` | `0` | integer | | Merge S3 range reads with gaps smaller than this in bytes (0 = use default 64KB) |

### 4.3 Cache — Multi-tier L1 (memory) + L2 (disk) Cache

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.cache.memory_limit` | `512MB` | quantity | | DEPRECATED: use memory_request/memory_limit_v2. Honored as request=limit=alias when triple unset. Removed in v1.0. |
| `lakehouseConfig.cache.memory_request` | `""` | quantity | | K8s-style request: baseline L1 memory cache size. Defaults to limit/4 when only legacy memory_limit is set. |
| `lakehouseConfig.cache.memory_limit_v2` | `""` | quantity | | K8s-style limit: hard ceiling on L1 memory cache |
| `lakehouseConfig.cache.memory_scaling` | `fixed` | string | | K8s-style scaling policy: fixed \| linear \| expbackoff |
| `lakehouseConfig.cache.disk_path` | `/data/lakehouse/cache` | string | | L2 disk cache directory path |
| `lakehouseConfig.cache.disk_limit` | `50GB` | quantity | | L2 disk cache size limit |
| `lakehouseConfig.cache.eviction_watermark` | `0.8` | float | | Disk cache eviction watermark (0.0-1.0) |
| `lakehouseConfig.cache.footer_ttl` | `1h` | duration | | TTL for cached Parquet file footers |
| `lakehouseConfig.cache.bloom_ttl` | `1h` | duration | | TTL for cached bloom filter data |
| `lakehouseConfig.cache.page_ttl` | `10m` | duration | | TTL for cached Parquet data pages |
| `lakehouseConfig.cache.warmup_partitions` | `0` | integer | | Number of recent hourly partitions to warm on startup (0=disabled) |
| `lakehouseConfig.cache.warmup_max_files` | `500` | integer | | Maximum files to warm on startup |
| `lakehouseConfig.cache.warmup_concurrency` | `4` | integer | | Maximum concurrent warmup downloads |
| `lakehouseConfig.cache.partition_mode` | `az-local` | string | | Cache partition mode: az-local (AZ-scoped ring), global (full ring), distributed |

### 4.4 Insert — Write Path (Flush to Parquet + S3)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.insert.flush_interval` | `30s` | duration | | Periodic flush interval. Lower = less read delay, more S3 PUTs |
| `lakehouseConfig.insert.max_buffer_rows` | `50000` | integer | | Max rows per partition buffer before triggering flush |
| `lakehouseConfig.insert.max_buffer_bytes` | `256MB` | quantity | | Total buffer memory limit across all partitions |
| `lakehouseConfig.insert.target_file_size` | `128MB` | quantity | | Target Parquet file size before starting new file |
| `lakehouseConfig.insert.row_group_size` | `10000` | integer | | Rows per Parquet row group |
| `lakehouseConfig.insert.bloom_columns` | `["service.name", "trace_id"]` | array | | Columns to create bloom filters for (point lookup acceleration) |
| `lakehouseConfig.insert.compression_level` | `7` | integer | | ZSTD compression level (1=fast, 22=max compression) |
| `lakehouseConfig.insert.wal_enabled` | `true` | boolean | | Enable write-ahead log for crash recovery |
| `lakehouseConfig.insert.wal_dir` | `/data/lakehouse/wal` | string | | WAL directory path |
| `lakehouseConfig.insert.wal_max_bytes` | `512MB` | quantity | | Maximum WAL size before blocking writes |

### 4.5 Select — Read Path (Buffer Query Bridge)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.select.buffer_query_enabled` | `true` | boolean | | Query insert pods for unflushed data (zero-delay reads) |
| `lakehouseConfig.select.insert_headless_service` | `""` | string | | Headless service for discovering insert pods |
| `lakehouseConfig.select.buffer_query_timeout` | `2s` | duration | | Timeout for buffer query requests to insert pods |
| `lakehouseConfig.select.az_aware` | `true` | boolean | | Enable AZ-aware buffer bridge routing |
| `lakehouseConfig.select.cross_az_fallback` | `true` | boolean | | Allow cross-AZ fallback for buffer queries |

### 4.6 Discovery — Hot Tier Auto-detection & Peer Fleet Discovery

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.discovery.headless_service` | `""` | string | | Headless service for VL/VT storage node discovery |
| `lakehouseConfig.discovery.storage_nodes` | `[]` | array | | Static list of VL/VT storage node addresses |
| `lakehouseConfig.discovery.partition_auth_key` | `""` | string | | Auth key for VL/VT /internal/partition/list endpoint |
| `lakehouseConfig.discovery.peer_headless_service` | `""` | string | | Headless service for peer cache fleet discovery |
| `lakehouseConfig.discovery.refresh_interval` | `5m` | duration | | Interval for refreshing storage node list |
| `lakehouseConfig.discovery.peer_refresh_interval` | `30s` | duration | | Interval for refreshing peer cache ring membership |
| `lakehouseConfig.discovery.timeout` | `10s` | duration | | Timeout for discovery HTTP requests |
| `lakehouseConfig.discovery.ring_stabilize_duration` | `60s` | duration | | Duration to keep old ring members as shadow set during scaling events |
| `lakehouseConfig.discovery.ring_change_notify` | `true` | boolean | | Enable subscriber notifications on ring membership changes |

### 4.7 Peer — Distributed Peer Cache (L3)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.peer.auth_key` | `""` | string | | Bearer auth key for peer cache HTTP endpoints |
| `lakehouseConfig.peer.timeout` | `5s` | duration | | Timeout for peer cache fetch requests |
| `lakehouseConfig.peer.max_connections` | `32` | integer | | Maximum HTTP connections per peer node |
| `lakehouseConfig.peer.az_aware` | `true` | boolean | | Enable AZ-aware routing to prefer same-AZ peers |
| `lakehouseConfig.peer.az_mode` | `preferred` | string | | AZ routing mode: "preferred" or "strict" |
| `lakehouseConfig.peer.cross_az_fallback` | `true` | boolean | | Allow cross-AZ fallback when same-AZ peers unavailable |
| `lakehouseConfig.peer.az_env_var` | `LAKEHOUSE_AZ` | string | | Environment variable name for AZ override |
| `lakehouseConfig.peer.az_min_peers_per_az` | `2` | integer | | Minimum same-AZ peers required in strict mode |

### 4.8 Manifest — Partition Manifest & Persistence

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.manifest.refresh_interval` | `5m` | duration | | Interval for full S3 manifest refresh |
| `lakehouseConfig.manifest.persist_path` | `/data/lakehouse` | string | | Directory for manifest and metadata persistence |
| `lakehouseConfig.manifest.persist_interval` | `5m` | duration | | Interval for persisting manifest to disk |
| `lakehouseConfig.manifest.sqs_queue_url` | `""` | string | | SQS queue URL for near-real-time S3 event manifest updates |
| `lakehouseConfig.manifest.sqs_region` | `""` | string | | AWS region for SQS queue |
| `lakehouseConfig.manifest.sqs_wait_time` | `20s` | duration | | Long-poll wait time for SQS receive |

### 4.9 Prefetch — Proactive Cache Warming

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.prefetch.correlated` | `true` | boolean | | Enable correlated prefetch (logs↔traces via trace_id) |
| `lakehouseConfig.prefetch.read_ahead_depth` | `2` | integer | | Number of partitions to read ahead during sequential scans |
| `lakehouseConfig.prefetch.max_concurrent` | `4` | integer | | Maximum concurrent prefetch downloads |
| `lakehouseConfig.prefetch.max_queue` | `64` | integer | | Maximum pending prefetch tasks in queue |

### 4.10 Smart Cache — Unified Cache Controller (L1→L4 Orchestration)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.smart_cache.max_age` | `24h` | duration | | Maximum age for cached entries before TTL eviction |
| `lakehouseConfig.smart_cache.snapshot_interval` | `60s` | duration | | Interval for persisting cache metadata snapshots to disk |
| `lakehouseConfig.smart_cache.query_grace_period` | `5m` | duration | | Grace period after query completes before unpinning entries |
| `lakehouseConfig.smart_cache.hot_access_threshold` | `3` | integer | | Access count threshold within hot_window to classify entry as "hot" |
| `lakehouseConfig.smart_cache.hot_window` | `10m` | duration | | Time window for hot access counting |
| `lakehouseConfig.smart_cache.target_hours` | `24` | integer | | Target hours of query data to keep cached |
| `lakehouseConfig.smart_cache.disk_limit_max` | `100GB` | quantity | | DEPRECATED: use disk_request/disk_limit. Honored as request=limit=alias when triple unset. Removed in v1.0. |
| `lakehouseConfig.smart_cache.disk_request` | `""` | quantity | | K8s-style request: baseline smart-cache disk reservation |
| `lakehouseConfig.smart_cache.disk_limit` | `""` | quantity | | K8s-style limit: hard ceiling on smart-cache disk |
| `lakehouseConfig.smart_cache.disk_scaling` | `fixed` | string | | K8s-style scaling policy: fixed \| linear \| expbackoff |
| `lakehouseConfig.smart_cache.ingestion_rate_hint` | `""` | quantity | | Manual ingestion rate hint for sizing (e.g., "100MB"). Empty = auto-detect. |

### 4.11 Cross-Signal — Bidirectional Logs↔Traces Prefetch & Eviction

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.cross_signal.enabled` | `false` | boolean | | Enable cross-signal prefetch/eviction between logs and traces instances |
| `lakehouseConfig.cross_signal.endpoint` | `""` | string | | Direct endpoint of the other signal's lakehouse instance |
| `lakehouseConfig.cross_signal.headless_service` | `""` | string | | Headless service for discovering other signal's instances |
| `lakehouseConfig.cross_signal.auth_key` | `""` | string | | Bearer auth key for cross-signal HTTP endpoints |
| `lakehouseConfig.cross_signal.timeout` | `2s` | duration | | Timeout for cross-signal HTTP requests |
| `lakehouseConfig.cross_signal.max_batch` | `100` | integer | | Maximum trace IDs per cross-signal batch |
| `lakehouseConfig.cross_signal.batch_interval` | `500ms` | duration | | Batching interval for cross-signal hints |

### 4.12 Shutdown — Graceful Shutdown Behavior

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.shutdown.delay` | `5s` | duration | | Delay before starting HTTP server shutdown (allows LB to drain). Must be < terminationGracePeriodSeconds - 40s. |
| `lakehouseConfig.shutdown.max_graceful_duration` | `7s` | duration | | Maximum time for in-flight HTTP requests to complete |
| `lakehouseConfig.shutdown.flush_timeout` | `15s` | duration | | Timeout for flushing buffered data to S3 during shutdown |
| `lakehouseConfig.shutdown.persist_timeout` | `10s` | duration | | Timeout for persisting manifest/cache/stats snapshots during shutdown |
| `lakehouseConfig.shutdown.release_timeout` | `5s` | duration | | Timeout for releasing leader lease and notifying peers during shutdown |

### 4.13 Query — Read Path Limits & Parallelism

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.query.max_concurrent` | `32` | integer | | Maximum concurrent select queries (HTTP 429 when exceeded) |
| `lakehouseConfig.query.timeout` | `60s` | duration | | Query execution timeout |
| `lakehouseConfig.query.max_rows` | `10000000` | integer | | DEPRECATED: use max_rows_request/limit. Honored as request=limit=alias when triple unset. Removed in v1.0. |
| `lakehouseConfig.query.max_rows_request` | `0` | integer | | K8s-style request: operator-visible baseline (informational) |
| `lakehouseConfig.query.max_rows_limit` | `0` | integer | | K8s-style limit: hard ceiling on rows per query |
| `lakehouseConfig.query.max_rows_scaling` | `fixed` | string | | K8s-style scaling policy: fixed \| linear \| expbackoff |
| `lakehouseConfig.query.slow_threshold` | `5s` | duration | | Slow query logging threshold |
| `lakehouseConfig.query.file_workers` | `8` | integer | | DEPRECATED: use file_workers_request/limit. Honored as request=limit=alias when triple unset. Removed in v1.0. |
| `lakehouseConfig.query.file_workers_request` | `0` | integer | | K8s-style request: always-reserved baseline file workers |
| `lakehouseConfig.query.file_workers_limit` | `0` | integer | | K8s-style limit: hard ceiling on file workers |
| `lakehouseConfig.query.file_workers_scaling` | `fixed` | string | | K8s-style scaling policy: fixed \| linear \| expbackoff |
| `lakehouseConfig.query.max_files_per_query` | `500` | integer | | Maximum S3 files a single query may scan (0 = use default 500) |

### 4.14 Startup — Warmup & Readiness Behavior

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.startup.serve_stale` | `false` | boolean | | Serve stale data from disk cache while refreshing from S3 |
| `lakehouseConfig.startup.warmup_window` | `24h` | duration | | Time window of recent partitions to warm on startup |
| `lakehouseConfig.startup.max_warmup_time` | `5m` | duration | | Maximum time to spend on startup warmup before marking ready |
| `lakehouseConfig.startup.peer_sync_timeout` | `30s` | duration | | Timeout for peer sync during startup |
| `lakehouseConfig.startup.stale_threshold` | `1h` | duration | | Duration threshold to consider persisted data stale |
| `lakehouseConfig.startup.wal_reconciliation` | `true` | boolean | | Reconcile WAL entries against manifest on stale startup |
| `lakehouseConfig.startup.cache_revalidation` | `true` | boolean | | Revalidate cache entries on stale startup |
| `lakehouseConfig.startup.max_resync_time` | `2m` | duration | | Maximum time for stale PV resync before marking ready anyway |

### 4.15 Schema — Parquet Column Extensibility

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.schema.extra_promoted` | `[]` | array | | Additional promoted columns beyond defaults |

### 4.16 Circuit Breaker — S3 Failure Protection

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.circuit_breaker.threshold` | `5` | integer | | Number of consecutive failures to open circuit |
| `lakehouseConfig.circuit_breaker.timeout` | `30s` | duration | | Duration circuit stays open before half-open probe |
| `lakehouseConfig.circuit_breaker.success_threshold` | `2` | integer | | Successful probes required to close circuit |

### 4.17 Tenant — Multi-tenancy Routing & Isolation

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.tenant.default_prefix` | `""` | string | | Static S3 prefix override |
| `lakehouseConfig.tenant.prefix_template` | `{AccountID}/{ProjectID}/` | string | | S3 prefix template. {AccountID} and {ProjectID} replaced from headers. |
| `lakehouseConfig.tenant.isolation` | `prefix` | string | | Tenant isolation mode: "prefix" or "bucket" |
| `lakehouseConfig.tenant.bucket_template` | `""` | string | | Bucket name template for bucket isolation mode |
| `lakehouseConfig.tenant.default_account` | `0` | string | | Default AccountID when no tenant header present |
| `lakehouseConfig.tenant.default_project` | `0` | string | | Default ProjectID when no tenant header present |
| `lakehouseConfig.tenant.header_account` | `X-Scope-AccountID` | string | | HTTP header name for AccountID extraction |
| `lakehouseConfig.tenant.header_project` | `X-Scope-ProjectID` | string | | HTTP header name for ProjectID extraction |
| `lakehouseConfig.tenant.global_read_header` | `""` | string | | HTTP header name for global (cross-tenant) read access |
| `lakehouseConfig.tenant.global_read_value` | `""` | string | | Required header value for global read access |
| `lakehouseConfig.tenant.global_read_token` | `""` | string | | Bearer token for global read access via Authorization header |
| `lakehouseConfig.tenant.known_tenants` | `[]` | array | | Known tenants for bucket-isolation mode |
| `lakehouseConfig.tenant.orgid_header` | `X-Scope-OrgID` | string | | HTTP header name for string-based tenant identification (Loki/Tempo compatible). When present, header value resolved to AccountID/ProjectID via aliases. |
| `lakehouseConfig.tenant.metrics_format` | `id` | string | enum: `["id", "name", "both"]` | Prometheus tenant label format: "id" (42:3), "name" (prod-team-eu_staging), "both" |
| `lakehouseConfig.tenant.auto_register` | `false` | boolean | | Auto-register unknown X-Scope-OrgID values as new tenant aliases |
| `lakehouseConfig.tenant.alias_sync_interval` | `30s` | duration | | Fleet sync interval for runtime-added aliases |
| `lakehouseConfig.tenant.aliases` | `{}` | object | | Static alias mappings: string alias → {account_id, project_id} |

### 4.18 Compaction — Background Parquet File Merging (Election-Free, HRW-Owned)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.compaction.enabled` | `true` | boolean | | Enable background compaction. Every pod runs scheduler; HRW ownership assigns partitions to exactly one pod. |
| `lakehouseConfig.compaction.interval` | `5m` | duration | | Compaction scan interval |
| `lakehouseConfig.compaction.max_concurrent` | `1` | integer | | Maximum concurrent compaction workers per pod |
| `lakehouseConfig.compaction.min_files_l0` | `10` | integer | | Minimum L0 files in partition to trigger compaction |
| `lakehouseConfig.compaction.min_files_l1` | `10` | integer | | Minimum L1 files in partition to trigger compaction |
| `lakehouseConfig.compaction.min_age` | `1h` | duration | | Minimum file age before eligible for compaction |
| `lakehouseConfig.compaction.daily_rollup_age` | `24h` | duration | | Minimum partition age for daily rollup compaction (merges ≥2 L1 files in old partitions) |

### 4.19 Delete — Cost-Aware Deletion (Tombstone + Selective Rewrite)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.delete.enabled` | `true` | boolean | | Enable delete API endpoints |
| `lakehouseConfig.delete.default_mode` | `auto` | string | | Default delete mode: "hide", "permanent", "auto" |
| `lakehouseConfig.delete.auto_rewrite_classes` | `["STANDARD"]` | array | | Storage classes eligible for active rewrite |
| `lakehouseConfig.delete.rewrite_delay` | `1h` | duration | | Delay before rewriting files after tombstone creation |
| `lakehouseConfig.delete.rewrite_batch_size` | `50` | integer | | Maximum files per rewrite batch |
| `lakehouseConfig.delete.rewrite_max_concurrent` | `2` | integer | | Maximum concurrent rewrite workers |
| `lakehouseConfig.delete.persist_path` | `/data/lakehouse/tombstones` | string | | Directory for tombstone persistence |
| `lakehouseConfig.delete.cost_warning_threshold` | `10.0` | float | | Cost threshold ($) that triggers a warning |
| `lakehouseConfig.delete.force_glacier_header` | `X-Force-Glacier-Delete` | string | | HTTP header to force Glacier file deletion |
| `lakehouseConfig.delete.verify_interval` | `6h` | duration | | Interval for continuous deletion verification |
| `lakehouseConfig.delete.lifecycle_rules` | `[]` | array | | S3 lifecycle rules for delete cost estimation |

### 4.20 Stats — Tenant Statistics, Storage Class Tracking, Cost Estimation

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.stats.enabled` | `true` | boolean | | Enable tenant stats collection and fleet sync |
| `lakehouseConfig.stats.push_interval` | `30s` | duration | | Interval for broadcasting tenant stat deltas to peer nodes |
| `lakehouseConfig.stats.push_compression` | `true` | boolean | | ZSTD-compress delta broadcasts |
| `lakehouseConfig.stats.snapshot_interval` | `5m` | duration | | Interval for persisting full registry snapshot to S3 |
| `lakehouseConfig.stats.snapshot_prefix` | `_meta/tenant-stats` | string | | S3 key prefix for stat snapshots |
| `lakehouseConfig.stats.meta_bucket` | `""` | string | | Dedicated S3 bucket for metadata (bucket-isolation mode) |
| `lakehouseConfig.stats.max_delta_count` | `1000` | integer | | Force full sync after N incremental deltas |
| `lakehouseConfig.stats.metrics_cardinality_limit` | `100` | integer | | Maximum unique tenant label values in Prometheus metrics |
| `lakehouseConfig.stats.cardinality_warning_threshold` | `10000` | integer | | Field cardinality threshold for high-cardinality warnings |
| `lakehouseConfig.stats.s3_lifecycle_rules` | `[]` | array | | S3 lifecycle rules for zero-cost storage class prediction |
| `lakehouseConfig.stats.s3_price_per_gb.STANDARD` | `0.023` | float | | Per-GB/month pricing for STANDARD storage class |
| `lakehouseConfig.stats.s3_price_per_gb.STANDARD_IA` | `0.0125` | float | | Per-GB/month pricing for STANDARD_IA storage class |
| `lakehouseConfig.stats.s3_price_per_gb.GLACIER_IR` | `0.004` | float | | Per-GB/month pricing for GLACIER_IR storage class |
| `lakehouseConfig.stats.s3_price_per_gb.GLACIER` | `0.0036` | float | | Per-GB/month pricing for GLACIER storage class |
| `lakehouseConfig.stats.s3_price_per_gb.DEEP_ARCHIVE` | `0.00099` | float | | Per-GB/month pricing for DEEP_ARCHIVE storage class |
| `lakehouseConfig.stats.s3_request_prices.PUT` | `0.005` | float | | Per-1000-requests pricing for PUT operations |
| `lakehouseConfig.stats.s3_request_prices.GET` | `0.0004` | float | | Per-1000-requests pricing for GET operations |
| `lakehouseConfig.stats.s3_request_prices.LIST` | `0.005` | float | | Per-1000-requests pricing for LIST operations |
| `lakehouseConfig.stats.s3_inventory_bucket` | `""` | string | | S3 Inventory source bucket for exact storage class verification |
| `lakehouseConfig.stats.headobject_sample_interval` | `6h` | duration | | Interval for HeadObject spot-checks near lifecycle transitions |
| `lakehouseConfig.stats.headobject_max_per_refresh` | `50` | integer | | Maximum HeadObject API calls per refresh cycle |

### 4.21 UI — Lakehouse Explorer Web Interface

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `lakehouseConfig.ui.enabled` | `true` | boolean | | Serve Lakehouse Explorer at /lakehouse/ui/ |
| `lakehouseConfig.ui.vmui_tab` | `true` | boolean | | Inject "Lakehouse" tab into VL/VT VMUI navigation |
| `lakehouseConfig.ui.refresh_default` | `0` | integer | | Default auto-refresh interval in seconds (0 = off) |
| `lakehouseConfig.ui.theme` | `auto` | string | | UI color theme: "auto", "dark", "light" |

---

## 5. Logs Signal — VictoriaLogs-Compatible Lakehouse (Port 9428)

### 5.1 Logs Global Configuration

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `logs.enabled` | `true` | boolean | | Enable logs signal. Deploys logs-select and logs-insert components. |
| `logs.profile` | `""` | string | enum: `["", "balanced", "max-performance", "max-durability", "max-cost-savings", "dev"]` | Per-signal profile override for logs. Empty = inherit lakehouseConfig.profile. |
| `logs.config.bloom_columns` | `["service.name"]` | array | | Bloom filter columns for logs mode |
| `logs.config.delete_prefix` | `/delete/logsql` | string | | Delete API endpoint prefix for logs mode |
| `logs.config.compat_version` | `""` | string | | VL compatibility version override (empty = latest) |

### 5.2 Logs Select (Read Path)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `logs.select.enabled` | `true` | boolean | | Enable logs select component |
| `logs.select.profile` | `""` | string | enum: `["", "balanced", "max-performance", "max-durability", "max-cost-savings", "dev"]` | Per-role profile override for logs select |
| `logs.select.replicaCount` | `2` | integer | minimum: 0 (schema) | Number of logs-select replicas |
| `logs.select.image` | `{}` | object | | Image overrides (per-component) |
| `logs.select.extraArgs` | `{}` | object | | Extra CLI arguments |
| `logs.select.extraEnv` | `[]` | array | | Extra environment variables |
| `logs.select.extraEnvFrom` | `[]` | array | | Extra envFrom references |
| `logs.select.extraVolumes` | `[]` | array | | Extra volumes |
| `logs.select.extraVolumeMounts` | `[]` | array | | Extra volume mounts |
| `logs.select.extraContainers` | `[]` | array | | Extra sidecar containers |
| `logs.select.initContainers` | `[]` | array | | Init containers |
| `logs.select.resources` | `{}` | object | | Container resource requests/limits |
| `logs.select.podSecurityContext` | `{}` | object | | Pod security context overrides |
| `logs.select.securityContext` | `{}` | object | | Container security context overrides |
| `logs.select.service.type` | `ClusterIP` | string | | Kubernetes service type |
| `logs.select.service.port` | `""` | string | | Service port (empty = use container port) |
| `logs.select.service.annotations` | `{}` | object | | Service annotations |
| `logs.select.service.labels` | `{}` | object | | Service labels |
| `logs.select.headlessService.enabled` | `true` | boolean | | Enable headless service for discovery |
| `logs.select.headlessService.annotations` | `{}` | object | | Headless service annotations |
| `logs.select.nodeSelector` | `{}` | object | | Node selector for pod placement |
| `logs.select.tolerations` | `[]` | array | | Tolerations for pod scheduling |
| `logs.select.affinity` | `{}` | object | | Pod affinity rules |
| `logs.select.podDisruptionBudget.enabled` | `false` | boolean | | Enable PDB. Recommended: true with minAvailable ≥ 7 for select (query path). |
| `logs.select.podDisruptionBudget.minAvailable` | `1` | integer | | Minimum pods available during disruptions |
| `logs.select.serviceAccount.create` | `true` | boolean | | Create service account |
| `logs.select.serviceAccount.name` | `""` | string | | Service account name |
| `logs.select.serviceAccount.annotations` | `{}` | object | | Service account annotations |
| `logs.select.horizontalPodAutoscaler.enabled` | `false` | boolean | | Enable HPA |
| `logs.select.horizontalPodAutoscaler.minReplicas` | `2` | integer | | Minimum replicas for HPA |
| `logs.select.horizontalPodAutoscaler.maxReplicas` | `10` | integer | | Maximum replicas for HPA |
| `logs.select.horizontalPodAutoscaler.targetCPUUtilizationPercentage` | `80` | integer | | Target CPU utilization percentage |
| `logs.select.horizontalPodAutoscaler.behavior.scaleDown.stabilizationWindowSeconds` | `300` | integer | | Scale-down stabilization window to prevent ring churn during transient load drops |
| `logs.select.horizontalPodAutoscaler.behavior.scaleDown.policies[0].type` | `Pods` | string | | Scale-down policy type |
| `logs.select.horizontalPodAutoscaler.behavior.scaleDown.policies[0].value` | `1` | integer | | Scale-down pod count per period |
| `logs.select.horizontalPodAutoscaler.behavior.scaleDown.policies[0].periodSeconds` | `120` | integer | | Scale-down period in seconds |
| `logs.select.verticalPodAutoscaler.enabled` | `false` | boolean | | Enable VPA |
| `logs.select.verticalPodAutoscaler.updateMode` | `Off` | string | | VPA update mode |
| `logs.select.ingress.enabled` | `false` | boolean | | Enable ingress |
| `logs.select.ingress.className` | `""` | string | | Ingress class name |
| `logs.select.ingress.annotations` | `{}` | object | | Ingress annotations |
| `logs.select.ingress.hosts` | `[]` | array | | Ingress hosts |
| `logs.select.ingress.tls` | `[]` | array | | Ingress TLS config |
| `logs.select.serviceMonitor.enabled` | `false` | boolean | | Enable ServiceMonitor for Prometheus |
| `logs.select.serviceMonitor.interval` | `30s` | duration | | Scrape interval |
| `logs.select.serviceMonitor.labels` | `{}` | object | | ServiceMonitor labels |
| `logs.select.persistence.enabled` | `true` | boolean | | Enable persistent volume |
| `logs.select.persistence.size` | `50Gi` | quantity | | PVC size |
| `logs.select.persistence.storageClass` | `""` | string | | Storage class name |
| `logs.select.persistence.accessModes` | `["ReadWriteOnce"]` | array | | PVC access modes |
| `logs.select.probe.liveness.initialDelaySeconds` | `5` | integer | | Liveness probe initial delay |
| `logs.select.probe.liveness.periodSeconds` | `10` | integer | | Liveness probe period |
| `logs.select.probe.liveness.failureThreshold` | `3` | integer | | Liveness probe failure threshold |
| `logs.select.probe.readiness.initialDelaySeconds` | `2` | integer | | Readiness probe initial delay |
| `logs.select.probe.readiness.periodSeconds` | `5` | integer | | Readiness probe period |
| `logs.select.probe.readiness.failureThreshold` | `60` | integer | | Readiness probe failure threshold |
| `logs.select.probe.startup.periodSeconds` | `5` | integer | | Startup probe period |
| `logs.select.probe.startup.failureThreshold` | `120` | integer | | Startup probe failure threshold |
| `logs.select.podAnnotations` | `{}` | object | | Pod annotations |
| `logs.select.podLabels` | `{}` | object | | Pod labels |
| `logs.select.terminationGracePeriodSeconds` | `60` | integer | | Graceful termination window |
| `logs.select.priorityClassName` | `""` | string | | Priority class name |
| `logs.select.topologySpreadConstraints[0].maxSkew` | `1` | integer | | Max skew for topology spread |
| `logs.select.topologySpreadConstraints[0].topologyKey` | `topology.kubernetes.io/zone` | string | | Topology key for AZ spread |
| `logs.select.topologySpreadConstraints[0].whenUnsatisfiable` | `ScheduleAnyway` | string | | Fallback when constraint unsatisfiable |
| `logs.select.topologySpreadConstraints[0].labelSelector.matchLabels.app.kubernetes.io/component` | `logs-select` | string | | Component label for topology spread |

### 5.3 Logs Insert (Write Path)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `logs.insert.enabled` | `true` | boolean | | Enable logs insert component |
| `logs.insert.profile` | `""` | string | enum: `["", "balanced", "max-performance", "max-durability", "max-cost-savings", "dev"]` | Per-role profile override for logs insert |
| `logs.insert.replicaCount` | `2` | integer | minimum: 0 (schema) | Number of logs-insert replicas |
| `logs.insert.image` | `{}` | object | | Image overrides |
| `logs.insert.extraArgs` | `{}` | object | | Extra CLI arguments |
| `logs.insert.extraEnv` | `[]` | array | | Extra environment variables |
| `logs.insert.extraEnvFrom` | `[]` | array | | Extra envFrom references |
| `logs.insert.extraVolumes` | `[]` | array | | Extra volumes |
| `logs.insert.extraVolumeMounts` | `[]` | array | | Extra volume mounts |
| `logs.insert.extraContainers` | `[]` | array | | Extra sidecar containers |
| `logs.insert.initContainers` | `[]` | array | | Init containers |
| `logs.insert.resources` | `{}` | object | | Container resource requests/limits |
| `logs.insert.podSecurityContext` | `{}` | object | | Pod security context overrides |
| `logs.insert.securityContext` | `{}` | object | | Container security context overrides |
| `logs.insert.service.type` | `ClusterIP` | string | | Kubernetes service type |
| `logs.insert.service.port` | `""` | string | | Service port (empty = use container port) |
| `logs.insert.service.annotations` | `{}` | object | | Service annotations |
| `logs.insert.service.labels` | `{}` | object | | Service labels |
| `logs.insert.headlessService.enabled` | `true` | boolean | | Enable headless service for discovery |
| `logs.insert.headlessService.annotations` | `{}` | object | | Headless service annotations |
| `logs.insert.nodeSelector` | `{}` | object | | Node selector for pod placement |
| `logs.insert.tolerations` | `[]` | array | | Tolerations for pod scheduling |
| `logs.insert.affinity` | `{}` | object | | Pod affinity rules |
| `logs.insert.podDisruptionBudget.enabled` | `false` | boolean | | Enable PDB. Recommended: true with minAvailable ≥ 2 for insert (write path). |
| `logs.insert.podDisruptionBudget.minAvailable` | `1` | integer | | Minimum pods available during disruptions |
| `logs.insert.serviceAccount.create` | `true` | boolean | | Create service account |
| `logs.insert.serviceAccount.name` | `""` | string | | Service account name |
| `logs.insert.serviceAccount.annotations` | `{}` | object | | Service account annotations |
| `logs.insert.horizontalPodAutoscaler.enabled` | `false` | boolean | | Enable HPA |
| `logs.insert.horizontalPodAutoscaler.minReplicas` | `2` | integer | | Minimum replicas for HPA |
| `logs.insert.horizontalPodAutoscaler.maxReplicas` | `10` | integer | | Maximum replicas for HPA |
| `logs.insert.horizontalPodAutoscaler.targetCPUUtilizationPercentage` | `80` | integer | | Target CPU utilization percentage |
| `logs.insert.horizontalPodAutoscaler.behavior.scaleDown.stabilizationWindowSeconds` | `300` | integer | | Scale-down stabilization window for safe scale-down (prevents oscillation that defers compaction) |
| `logs.insert.horizontalPodAutoscaler.behavior.scaleDown.policies[0].type` | `Pods` | string | | Scale-down policy type |
| `logs.insert.horizontalPodAutoscaler.behavior.scaleDown.policies[0].value` | `1` | integer | | Scale-down pod count per period |
| `logs.insert.horizontalPodAutoscaler.behavior.scaleDown.policies[0].periodSeconds` | `120` | integer | | Scale-down period in seconds |
| `logs.insert.horizontalPodAutoscaler.behavior.scaleUp.stabilizationWindowSeconds` | `30` | integer | | Scale-up stabilization window for rapid scale-up (1→5 pods in 30s) |
| `logs.insert.horizontalPodAutoscaler.behavior.scaleUp.policies[0].type` | `Pods` | string | | Scale-up policy type |
| `logs.insert.horizontalPodAutoscaler.behavior.scaleUp.policies[0].value` | `2` | integer | | Scale-up pod count per period |
| `logs.insert.horizontalPodAutoscaler.behavior.scaleUp.policies[0].periodSeconds` | `60` | integer | | Scale-up period in seconds |
| `logs.insert.verticalPodAutoscaler.enabled` | `false` | boolean | | Enable VPA |
| `logs.insert.verticalPodAutoscaler.updateMode` | `Off` | string | | VPA update mode |
| `logs.insert.ingress.enabled` | `false` | boolean | | Enable ingress |
| `logs.insert.ingress.className` | `""` | string | | Ingress class name |
| `logs.insert.ingress.annotations` | `{}` | object | | Ingress annotations |
| `logs.insert.ingress.hosts` | `[]` | array | | Ingress hosts |
| `logs.insert.ingress.tls` | `[]` | array | | Ingress TLS config |
| `logs.insert.serviceMonitor.enabled` | `false` | boolean | | Enable ServiceMonitor for Prometheus |
| `logs.insert.serviceMonitor.interval` | `30s` | duration | | Scrape interval |
| `logs.insert.serviceMonitor.labels` | `{}` | object | | ServiceMonitor labels |
| `logs.insert.persistence.enabled` | `true` | boolean | | Enable persistent volume |
| `logs.insert.persistence.size` | `50Gi` | quantity | | PVC size |
| `logs.insert.persistence.storageClass` | `""` | string | | Storage class name |
| `logs.insert.persistence.accessModes` | `["ReadWriteOnce"]` | array | | PVC access modes |
| `logs.insert.probe.liveness.initialDelaySeconds` | `5` | integer | | Liveness probe initial delay |
| `logs.insert.probe.liveness.periodSeconds` | `10` | integer | | Liveness probe period |
| `logs.insert.probe.liveness.failureThreshold` | `3` | integer | | Liveness probe failure threshold |
| `logs.insert.probe.readiness.initialDelaySeconds` | `2` | integer | | Readiness probe initial delay |
| `logs.insert.probe.readiness.periodSeconds` | `5` | integer | | Readiness probe period |
| `logs.insert.probe.readiness.failureThreshold` | `60` | integer | | Readiness probe failure threshold |
| `logs.insert.probe.startup.periodSeconds` | `5` | integer | | Startup probe period |
| `logs.insert.probe.startup.failureThreshold` | `120` | integer | | Startup probe failure threshold |
| `logs.insert.podAnnotations` | `{}` | object | | Pod annotations |
| `logs.insert.podLabels` | `{}` | object | | Pod labels |
| `logs.insert.terminationGracePeriodSeconds` | `120` | integer | | Graceful termination window (accommodates compaction + drain, p99 headroom before SIGKILL) |
| `logs.insert.priorityClassName` | `""` | string | | Priority class name |
| `logs.insert.topologySpreadConstraints[0].maxSkew` | `1` | integer | | Max skew for topology spread |
| `logs.insert.topologySpreadConstraints[0].topologyKey` | `topology.kubernetes.io/zone` | string | | Topology key for AZ spread |
| `logs.insert.topologySpreadConstraints[0].whenUnsatisfiable` | `ScheduleAnyway` | string | | Fallback when constraint unsatisfiable |
| `logs.insert.topologySpreadConstraints[0].labelSelector.matchLabels.app.kubernetes.io/component` | `logs-insert` | string | | Component label for topology spread |

---

## 6. Traces Signal — VictoriaTraces-Compatible Lakehouse (Port 10428)

### 6.1 Traces Global Configuration

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `traces.enabled` | `false` | boolean | | Enable traces signal. Deploys traces-select and traces-insert components. |
| `traces.profile` | `""` | string | enum: `["", "balanced", "max-performance", "max-durability", "max-cost-savings", "dev"]` | Per-signal profile override for traces. Empty = inherit lakehouseConfig.profile. |
| `traces.config.bloom_columns` | `["trace_id", "service.name"]` | array | | Bloom filter columns for traces mode |
| `traces.config.delete_prefix` | `/delete/tracessql` | string | | Delete API endpoint prefix for traces mode |
| `traces.config.compat_version` | `""` | string | | VT compatibility version override (empty = latest) |
| `traces.config.jaeger_enabled` | `true` | boolean | | Enable Jaeger gRPC API for traces |
| `traces.config.jaeger_grpc_addr` | `:16685` | string | | Jaeger gRPC listen address |
| `traces.config.jaeger_grpc_port` | `16685` | integer | | Jaeger gRPC port for K8s service/container (matches jaeger_grpc_addr) |

### 6.2 Traces Select (Read Path)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `traces.select.enabled` | `true` | boolean | | Enable traces select component |
| `traces.select.profile` | `""` | string | enum: `["", "balanced", "max-performance", "max-durability", "max-cost-savings", "dev"]` | Per-role profile override for traces select |
| `traces.select.replicaCount` | `2` | integer | minimum: 0 (schema) | Number of traces-select replicas |
| `traces.select.image` | `{}` | object | | Image overrides |
| `traces.select.extraArgs` | `{}` | object | | Extra CLI arguments |
| `traces.select.extraEnv` | `[]` | array | | Extra environment variables |
| `traces.select.extraEnvFrom` | `[]` | array | | Extra envFrom references |
| `traces.select.extraVolumes` | `[]` | array | | Extra volumes |
| `traces.select.extraVolumeMounts` | `[]` | array | | Extra volume mounts |
| `traces.select.extraContainers` | `[]` | array | | Extra sidecar containers |
| `traces.select.initContainers` | `[]` | array | | Init containers |
| `traces.select.resources` | `{}` | object | | Container resource requests/limits |
| `traces.select.podSecurityContext` | `{}` | object | | Pod security context overrides |
| `traces.select.securityContext` | `{}` | object | | Container security context overrides |
| `traces.select.service.type` | `ClusterIP` | string | | Kubernetes service type |
| `traces.select.service.port` | `""` | string | | Service port (empty = use container port) |
| `traces.select.service.annotations` | `{}` | object | | Service annotations |
| `traces.select.service.labels` | `{}` | object | | Service labels |
| `traces.select.headlessService.enabled` | `true` | boolean | | Enable headless service for discovery |
| `traces.select.headlessService.annotations` | `{}` | object | | Headless service annotations |
| `traces.select.nodeSelector` | `{}` | object | | Node selector for pod placement |
| `traces.select.tolerations` | `[]` | array | | Tolerations for pod scheduling |
| `traces.select.affinity` | `{}` | object | | Pod affinity rules |
| `traces.select.podDisruptionBudget.enabled` | `false` | boolean | | Enable PDB. Recommended: true with minAvailable ≥ 7 for select (query path). |
| `traces.select.podDisruptionBudget.minAvailable` | `1` | integer | | Minimum pods available during disruptions |
| `traces.select.serviceAccount.create` | `true` | boolean | | Create service account |
| `traces.select.serviceAccount.name` | `""` | string | | Service account name |
| `traces.select.serviceAccount.annotations` | `{}` | object | | Service account annotations |
| `traces.select.horizontalPodAutoscaler.enabled` | `false` | boolean | | Enable HPA |
| `traces.select.horizontalPodAutoscaler.minReplicas` | `2` | integer | | Minimum replicas for HPA |
| `traces.select.horizontalPodAutoscaler.maxReplicas` | `10` | integer | | Maximum replicas for HPA |
| `traces.select.horizontalPodAutoscaler.targetCPUUtilizationPercentage` | `80` | integer | | Target CPU utilization percentage |
| `traces.select.horizontalPodAutoscaler.behavior.scaleDown.stabilizationWindowSeconds` | `300` | integer | | Scale-down stabilization window (prevents ring churn) |
| `traces.select.horizontalPodAutoscaler.behavior.scaleDown.policies[0].type` | `Pods` | string | | Scale-down policy type |
| `traces.select.horizontalPodAutoscaler.behavior.scaleDown.policies[0].value` | `1` | integer | | Scale-down pod count per period |
| `traces.select.horizontalPodAutoscaler.behavior.scaleDown.policies[0].periodSeconds` | `120` | integer | | Scale-down period in seconds |
| `traces.select.horizontalPodAutoscaler.behavior.scaleUp.stabilizationWindowSeconds` | `30` | integer | | Scale-up stabilization window (rapid scale-up 1→5 pods in 30s) |
| `traces.select.horizontalPodAutoscaler.behavior.scaleUp.policies[0].type` | `Pods` | string | | Scale-up policy type |
| `traces.select.horizontalPodAutoscaler.behavior.scaleUp.policies[0].value` | `2` | integer | | Scale-up pod count per period |
| `traces.select.horizontalPodAutoscaler.behavior.scaleUp.policies[0].periodSeconds` | `60` | integer | | Scale-up period in seconds |
| `traces.select.verticalPodAutoscaler.enabled` | `false` | boolean | | Enable VPA |
| `traces.select.verticalPodAutoscaler.updateMode` | `Off` | string | | VPA update mode |
| `traces.select.ingress.enabled` | `false` | boolean | | Enable ingress |
| `traces.select.ingress.className` | `""` | string | | Ingress class name |
| `traces.select.ingress.annotations` | `{}` | object | | Ingress annotations |
| `traces.select.ingress.hosts` | `[]` | array | | Ingress hosts |
| `traces.select.ingress.tls` | `[]` | array | | Ingress TLS config |
| `traces.select.serviceMonitor.enabled` | `false` | boolean | | Enable ServiceMonitor for Prometheus |
| `traces.select.serviceMonitor.interval` | `30s` | duration | | Scrape interval |
| `traces.select.serviceMonitor.labels` | `{}` | object | | ServiceMonitor labels |
| `traces.select.persistence.enabled` | `true` | boolean | | Enable persistent volume |
| `traces.select.persistence.size` | `50Gi` | quantity | | PVC size |
| `traces.select.persistence.storageClass` | `""` | string | | Storage class name |
| `traces.select.persistence.accessModes` | `["ReadWriteOnce"]` | array | | PVC access modes |
| `traces.select.probe.liveness.initialDelaySeconds` | `5` | integer | | Liveness probe initial delay |
| `traces.select.probe.liveness.periodSeconds` | `10` | integer | | Liveness probe period |
| `traces.select.probe.liveness.failureThreshold` | `3` | integer | | Liveness probe failure threshold |
| `traces.select.probe.readiness.initialDelaySeconds` | `2` | integer | | Readiness probe initial delay |
| `traces.select.probe.readiness.periodSeconds` | `5` | integer | | Readiness probe period |
| `traces.select.probe.readiness.failureThreshold` | `60` | integer | | Readiness probe failure threshold |
| `traces.select.probe.startup.periodSeconds` | `5` | integer | | Startup probe period |
| `traces.select.probe.startup.failureThreshold` | `120` | integer | | Startup probe failure threshold |
| `traces.select.podAnnotations` | `{}` | object | | Pod annotations |
| `traces.select.podLabels` | `{}` | object | | Pod labels |
| `traces.select.terminationGracePeriodSeconds` | `60` | integer | | Graceful termination window |
| `traces.select.priorityClassName` | `""` | string | | Priority class name |
| `traces.select.topologySpreadConstraints[0].maxSkew` | `1` | integer | | Max skew for topology spread |
| `traces.select.topologySpreadConstraints[0].topologyKey` | `topology.kubernetes.io/zone` | string | | Topology key for AZ spread |
| `traces.select.topologySpreadConstraints[0].whenUnsatisfiable` | `ScheduleAnyway` | string | | Fallback when constraint unsatisfiable |
| `traces.select.topologySpreadConstraints[0].labelSelector.matchLabels.app.kubernetes.io/component` | `traces-select` | string | | Component label for topology spread |

### 6.3 Traces Insert (Write Path)

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `traces.insert.enabled` | `true` | boolean | | Enable traces insert component |
| `traces.insert.profile` | `""` | string | enum: `["", "balanced", "max-performance", "max-durability", "max-cost-savings", "dev"]` | Per-role profile override for traces insert |
| `traces.insert.replicaCount` | `2` | integer | minimum: 0 (schema) | Number of traces-insert replicas |
| `traces.insert.image` | `{}` | object | | Image overrides |
| `traces.insert.extraArgs` | `{}` | object | | Extra CLI arguments |
| `traces.insert.extraEnv` | `[]` | array | | Extra environment variables |
| `traces.insert.extraEnvFrom` | `[]` | array | | Extra envFrom references |
| `traces.insert.extraVolumes` | `[]` | array | | Extra volumes |
| `traces.insert.extraVolumeMounts` | `[]` | array | | Extra volume mounts |
| `traces.insert.extraContainers` | `[]` | array | | Extra sidecar containers |
| `traces.insert.initContainers` | `[]` | array | | Init containers |
| `traces.insert.resources` | `{}` | object | | Container resource requests/limits |
| `traces.insert.podSecurityContext` | `{}` | object | | Pod security context overrides |
| `traces.insert.securityContext` | `{}` | object | | Container security context overrides |
| `traces.insert.service.type` | `ClusterIP` | string | | Kubernetes service type |
| `traces.insert.service.port` | `""` | string | | Service port (empty = use container port) |
| `traces.insert.service.annotations` | `{}` | object | | Service annotations |
| `traces.insert.service.labels` | `{}` | object | | Service labels |
| `traces.insert.headlessService.enabled` | `true` | boolean | | Enable headless service for discovery |
| `traces.insert.headlessService.annotations` | `{}` | object | | Headless service annotations |
| `traces.insert.nodeSelector` | `{}` | object | | Node selector for pod placement |
| `traces.insert.tolerations` | `[]` | array | | Tolerations for pod scheduling |
| `traces.insert.affinity` | `{}` | object | | Pod affinity rules |
| `traces.insert.podDisruptionBudget.enabled` | `false` | boolean | | Enable PDB. Recommended: true with minAvailable ≥ 2 for insert (write path). |
| `traces.insert.podDisruptionBudget.minAvailable` | `1` | integer | | Minimum pods available during disruptions |
| `traces.insert.serviceAccount.create` | `true` | boolean | | Create service account |
| `traces.insert.serviceAccount.name` | `""` | string | | Service account name |
| `traces.insert.serviceAccount.annotations` | `{}` | object | | Service account annotations |
| `traces.insert.horizontalPodAutoscaler.enabled` | `false` | boolean | | Enable HPA |
| `traces.insert.horizontalPodAutoscaler.minReplicas` | `2` | integer | | Minimum replicas for HPA |
| `traces.insert.horizontalPodAutoscaler.maxReplicas` | `10` | integer | | Maximum replicas for HPA |
| `traces.insert.horizontalPodAutoscaler.targetCPUUtilizationPercentage` | `80` | integer | | Target CPU utilization percentage |
| `traces.insert.horizontalPodAutoscaler.behavior.scaleDown.stabilizationWindowSeconds` | `300` | integer | | Scale-down stabilization window (prevents oscillation) |
| `traces.insert.horizontalPodAutoscaler.behavior.scaleDown.policies[0].type` | `Pods` | string | | Scale-down policy type |
| `traces.insert.horizontalPodAutoscaler.behavior.scaleDown.policies[0].value` | `1` | integer | | Scale-down pod count per period |
| `traces.insert.horizontalPodAutoscaler.behavior.scaleDown.policies[0].periodSeconds` | `120` | integer | | Scale-down period in seconds |
| `traces.insert.horizontalPodAutoscaler.behavior.scaleUp.stabilizationWindowSeconds` | `30` | integer | | Scale-up stabilization window |
| `traces.insert.horizontalPodAutoscaler.behavior.scaleUp.policies[0].type` | `Pods` | string | | Scale-up policy type |
| `traces.insert.horizontalPodAutoscaler.behavior.scaleUp.policies[0].value` | `2` | integer | | Scale-up pod count per period |
| `traces.insert.horizontalPodAutoscaler.behavior.scaleUp.policies[0].periodSeconds` | `60` | integer | | Scale-up period in seconds |
| `traces.insert.verticalPodAutoscaler.enabled` | `false` | boolean | | Enable VPA |
| `traces.insert.verticalPodAutoscaler.updateMode` | `Off` | string | | VPA update mode |
| `traces.insert.ingress.enabled` | `false` | boolean | | Enable ingress |
| `traces.insert.ingress.className` | `""` | string | | Ingress class name |
| `traces.insert.ingress.annotations` | `{}` | object | | Ingress annotations |
| `traces.insert.ingress.hosts` | `[]` | array | | Ingress hosts |
| `traces.insert.ingress.tls` | `[]` | array | | Ingress TLS config |
| `traces.insert.serviceMonitor.enabled` | `false` | boolean | | Enable ServiceMonitor for Prometheus |
| `traces.insert.serviceMonitor.interval` | `30s` | duration | | Scrape interval |
| `traces.insert.serviceMonitor.labels` | `{}` | object | | ServiceMonitor labels |
| `traces.insert.persistence.enabled` | `true` | boolean | | Enable persistent volume |
| `traces.insert.persistence.size` | `50Gi` | quantity | | PVC size |
| `traces.insert.persistence.storageClass` | `""` | string | | Storage class name |
| `traces.insert.persistence.accessModes` | `["ReadWriteOnce"]` | array | | PVC access modes |
| `traces.insert.probe.liveness.initialDelaySeconds` | `5` | integer | | Liveness probe initial delay |
| `traces.insert.probe.liveness.periodSeconds` | `10` | integer | | Liveness probe period |
| `traces.insert.probe.liveness.failureThreshold` | `3` | integer | | Liveness probe failure threshold |
| `traces.insert.probe.readiness.initialDelaySeconds` | `2` | integer | | Readiness probe initial delay |
| `traces.insert.probe.readiness.periodSeconds` | `5` | integer | | Readiness probe period |
| `traces.insert.probe.readiness.failureThreshold` | `60` | integer | | Readiness probe failure threshold |
| `traces.insert.probe.startup.periodSeconds` | `5` | integer | | Startup probe period |
| `traces.insert.probe.startup.failureThreshold` | `120` | integer | | Startup probe failure threshold |
| `traces.insert.podAnnotations` | `{}` | object | | Pod annotations |
| `traces.insert.podLabels` | `{}` | object | | Pod labels |
| `traces.insert.terminationGracePeriodSeconds` | `120` | integer | | Graceful termination window (accommodates compaction + drain) |
| `traces.insert.priorityClassName` | `""` | string | | Priority class name |
| `traces.insert.topologySpreadConstraints[0].maxSkew` | `1` | integer | | Max skew for topology spread |
| `traces.insert.topologySpreadConstraints[0].topologyKey` | `topology.kubernetes.io/zone` | string | | Topology key for AZ spread |
| `traces.insert.topologySpreadConstraints[0].whenUnsatisfiable` | `ScheduleAnyway` | string | | Fallback when constraint unsatisfiable |
| `traces.insert.topologySpreadConstraints[0].labelSelector.matchLabels.app.kubernetes.io/component` | `traces-insert` | string | | Component label for topology spread |

---

## 7. VMAuth — Shared Request Router

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `vmauth.enabled` | `false` | boolean | | Enable vmauth component |
| `vmauth.image.repository` | `victoriametrics/vmauth` | string | | VMAuth container image repository |
| `vmauth.image.tag` | `v1.106.1` | string | | VMAuth container image tag |
| `vmauth.image.pullPolicy` | `IfNotPresent` | string | | Image pull policy |
| `vmauth.replicaCount` | `1` | integer | | Number of vmauth replicas |
| `vmauth.resources` | `{}` | object | | Container resource requests/limits |
| `vmauth.service.type` | `ClusterIP` | string | | Kubernetes service type |
| `vmauth.service.port` | `8427` | integer | | Service port |
| `vmauth.service.annotations` | `{}` | object | | Service annotations |
| `vmauth.service.labels` | `{}` | object | | Service labels |
| `vmauth.config` | `""` | string | | VMAuth configuration (YAML string) |
| `vmauth.extraArgs` | `{}` | object | | Extra CLI arguments |
| `vmauth.podSecurityContext` | `{}` | object | | Pod security context overrides |
| `vmauth.securityContext` | `{}` | object | | Container security context overrides |
| `vmauth.nodeSelector` | `{}` | object | | Node selector for pod placement |
| `vmauth.tolerations` | `[]` | array | | Tolerations for pod scheduling |
| `vmauth.affinity` | `{}` | object | | Pod affinity rules |
| `vmauth.serviceMonitor.enabled` | `false` | boolean | | Enable ServiceMonitor for Prometheus |
| `vmauth.serviceMonitor.interval` | `30s` | duration | | Scrape interval |
| `vmauth.serviceMonitor.labels` | `{}` | object | | ServiceMonitor labels |
| `vmauth.ingress.enabled` | `false` | boolean | | Enable ingress |
| `vmauth.ingress.className` | `""` | string | | Ingress class name |
| `vmauth.ingress.annotations` | `{}` | object | | Ingress annotations |
| `vmauth.ingress.hosts` | `[]` | array | | Ingress hosts |
| `vmauth.ingress.tls` | `[]` | array | | Ingress TLS config |
| `vmauth.podAnnotations` | `{}` | object | | Pod annotations |
| `vmauth.podLabels` | `{}` | object | | Pod labels |
| `vmauth.terminationGracePeriodSeconds` | `30` | integer | | Graceful termination window |

---

## 8. Network Policy

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `networkPolicy.enabled` | `false` | boolean | | Enable network policy |
| `networkPolicy.ingressFrom` | `[]` | array | | Ingress rules (list of selectors) |
| `networkPolicy.extraEgress` | `[]` | array | | Extra egress rules |

---

## 9. Extra Manifests

| Helm Key Path | Helm Default Value | Data Type | Schema Constraint | Description |
|---|---|---|---|---|
| `extraManifests` | `[]` | array | | Extra Kubernetes manifests to deploy alongside the chart. Each entry is a complete YAML manifest. Supports Go template syntax. |

---

## Summary of Key Findings

### Unit Conventions
- **Memory/Disk Quantities:** Helm uses Kubernetes notation (`Mi`, `Gi`, `MB`, `GB`) for storage
  - Example: `cache.memory_limit = 512MB`, `cache.disk_limit = 50GB`
  - Example: Persistence: `50Gi` (consistent with K8s PVC sizing)
- **Durations:** ISO 8601 notation (`30s`, `5m`, `1h`, `24h`, `200ms`)
- **Numeric:** Plain integers/floats without units

### Deprecated Keys (To Be Removed in v1.0)
These Helm keys have K8s-style replacements and will be removed:
- `lakehouseConfig.s3.max_concurrent_downloads` → use `concurrent_downloads_request` + `concurrent_downloads_limit`
- `lakehouseConfig.cache.memory_limit` → use `memory_request` + `memory_limit_v2`
- `lakehouseConfig.smart_cache.disk_limit_max` → use `disk_request` + `disk_limit`
- `lakehouseConfig.query.max_rows` → use `max_rows_request` + `max_rows_limit`
- `lakehouseConfig.query.file_workers` → use `file_workers_request` + `file_workers_limit`

### K8s-Style Scaling Policies
Multiple settings use standardized scaling policies:
- `fixed` (default, hardcoded ceiling)
- `linear` (reserved for future signal-driven scaling)
- `expbackoff` (reserved for future signal-driven scaling)

### Profile Enum (Schema Validated)
Valid profile values: `["", "balanced", "max-performance", "max-durability", "max-cost-savings", "dev"]`

### Helm-Only Configuration
- Image repositories, pull policies, and tags
- Pod/deployment topology spread constraints
- Persistent volume claims (size, storage class)
- Service types and ports
- Probes (liveness, readiness, startup)
- Pod security contexts and security policies
- HPA/VPA configurations
- Service monitors for Prometheus
- Network policy

---

## Notes for Task 4 (Compare with Code & Doc Defaults)

1. **Unit Mismatches:** Helm uses K8s quantities (`Mi`/`Gi`) while application config may use different units (`MB`/`GB`). Task 4 will flag these.
2. **Deprecated Keys:** These Helm values should be compared against their K8s-style replacements (where `memory_limit` should map to `memory_request` + `memory_limit_v2`).
3. **Profile Expansion:** The `profile` key (shared and per-component/signal) is a preset that expands to multiple underlying settings. Task 4 should verify code handles all profile cases.
4. **Helm-Specific Features:** Network policy, service monitors, HPA/VPA, and topology constraints are Helm-deployment features that may not exist in application code. Task 4 will flag these as "Helm-only".
5. **Cross-Signal Configuration:** `cross_signal` settings are Helm-enabled features; Task 4 should verify application support.

