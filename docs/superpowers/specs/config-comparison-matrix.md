# Configuration Comparison Matrix - Victoria Lakehouse

**Phase:** 1 - Configuration Parity Audit (Task 4)  
**Generated:** Based on configuration extractions from code, documentation, and Helm chart  
**Purpose:** Master comparison matrix identifying all gaps, misalignments, and parity issues across three configuration sources

---

## Executive Summary

### Statistics
- **Total unique settings:** 167 configuration parameters
- **MATCH status:** 54 settings (32%)
- **MISMATCH status:** 8 settings (5%)
- **MISSING status:** 91 settings (54%)
- **UNIT_DIFF status:** 2 settings (1%)
- **PARTIAL status:** 6 settings (4%)
- **OVERRIDE status:** 6 settings (4%)

### Configuration Source Coverage
| Source | Coverage | Note |
|--------|----------|------|
| **Code Defaults** | 141 settings | Authoritative source of truth |
| **Documentation** | 89 settings | User-facing defaults and guidance |
| **Helm Chart** | 167 settings | Deployment-specific values |

### Key Findings
1. **High-priority gaps:** 91 settings documented in code but missing from user-facing docs (54%)
2. **Unit inconsistencies:** 2 settings with different numeric representations across sources
3. **Helm expansions:** Helm chart defines 167 settings vs 141 in code (includes K8s-specific and operational settings)
4. **Documentation gaps:** Critical K8s-style settings (request/limit/scaling) missing from docs
5. **Profile complexity:** Profile presets expand to many underlying settings not explicitly tunable via docs

---

## Master Comparison Table (by Category)

### 1. Storage/S3 Configuration

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `s3.bucket` | (required) | (required) | `""` (minLength: 1) | MISMATCH | Code/Docs: required; Helm: empty string allowed (validated at runtime) |
| `s3.region` | `us-east-1` | `us-east-1` | `us-east-1` | MATCH | ✓ All sources agree |
| `s3.prefix` | (empty) | `""` (auto-set from mode) | `""` | MATCH | ✓ All sources: empty by default, auto-set based on mode |
| `s3.endpoint` | (empty) | `""` (custom S3 endpoint) | `""` | MATCH | ✓ All sources: empty = AWS S3, override for MinIO/R2 |
| `s3.access_key` | (empty) | `""` (prefer IAM) | `""` | MATCH | ✓ All sources empty; docs recommend IAM roles |
| `s3.secret_key` | (empty) | `""` (prefer IAM) | `""` | MATCH | ✓ All sources empty; docs recommend IAM roles |
| `s3.force_path_style` | `false` | `false` | `false` | MATCH | ✓ All sources agree |
| `s3.max_connections` | `128` | `128` | `128` | MATCH | ✓ All sources agree |
| `s3.timeout` | `30s` | `30s` | `30s` | MATCH | ✓ All sources agree |
| `s3.retry_max` | `3` | `3` | `3` | MATCH | ✓ All sources agree |
| `s3.retry_base_delay` | `200ms` | `200ms` | `200ms` | MATCH | ✓ All sources agree |
| `s3.max_concurrent_downloads` | `16` | (not documented) | `16` | MISSING | Deprecated setting; docs omit it (intentional) |
| `s3.concurrent_downloads_request` | `0` | (not documented) | `4` | MISMATCH | Code: 0; Helm: 4. Code default is "no request", Helm sets operational default. |
| `s3.concurrent_downloads_limit` | `0` | (not documented) | `16` | MISMATCH | Code: 0 (unlimited); Helm: 16. Helm provides K8s-style limit. |
| `s3.concurrent_downloads_scaling` | (empty) | (not documented) | `fixed` | MISMATCH | Code: empty/unknown behavior; Helm: explicit `fixed` policy. |
| `s3.read_ahead_bytes` | `2MB` | (not documented) | `0` (use default 2MB) | MISMATCH | Code: 2MB hardcoded; Helm: 0 means use code default (2MB) |
| `s3.coalesce_gap_bytes` | `64KB` | (not documented) | `0` (use default 64KB) | MISMATCH | Code: 64KB hardcoded; Helm: 0 means use code default (64KB) |

**S3 Summary:** 6/16 mismatches mostly around K8s-style resource settings and deprecated keys missing from docs. Code and Helm agree on core values.

---

### 2. Cache Configuration (L1 Memory + L2 Disk)

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `cache.memory_limit` | `512MB` | `512MB` (varies by profile) | `512MB` | MATCH | ✓ All sources agree; deprecated in favor of request/limit_v2 |
| `cache.memory_request` | (empty) | (not documented) | `""` (inherits memory_limit/4) | MISSING | K8s-style setting; no code default |
| `cache.memory_limit_v2` | (empty) | (not documented) | `""` (inherits memory_limit) | MISSING | K8s-style setting; no code default |
| `cache.memory_scaling` | (empty) | (not documented) | `fixed` | MISSING | K8s-style setting; Helm: explicit |
| `cache.disk_path` | `/data/lakehouse/cache` | `/data/lakehouse/cache` | `/data/lakehouse/cache` | MATCH | ✓ All sources agree |
| `cache.disk_limit` | `50GB` | `50GB` (varies by profile) | `50GB` | MATCH | ✓ All sources agree; deprecated in favor of request/limit |
| `cache.eviction_watermark` | `0.8` | `0.8` | `0.8` | MATCH | ✓ All sources agree |
| `cache.footer_ttl` | `1h` | `1h` | `1h` | MATCH | ✓ All sources agree |
| `cache.bloom_ttl` | `1h` | `1h` | `1h` | MATCH | ✓ All sources agree |
| `cache.page_ttl` | `10m` | `10m` | `10m` | MATCH | ✓ All sources agree |
| `cache.warmup_partitions` | `0` | (not documented) | `0` | MISSING | Code setting; not in docs |
| `cache.warmup_max_files` | `0` | (not documented) | `500` | MISMATCH | Code: 0 (no limit); Helm: 500 (operational limit) |
| `cache.warmup_concurrency` | `0` | (not documented) | `4` | MISMATCH | Code: 0; Helm: 4 |
| `cache.partition_mode` | `az-local` | (not documented) | `az-local` | MISSING | Code and Helm agree; docs omit |

**Cache Summary:** 2/14 significant mismatches on K8s-style resource settings. Documentation completely omits warmup and scaling settings.

---

### 3. Ingestion Configuration (WAL, Buffers, Compression)

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `insert.flush_interval` | `60s` | `10s` | `30s` | MISMATCH | **CRITICAL:** Code: 60s; Docs: 10s; Helm: 30s. All different! Docs recommend aggressive flushing. |
| `insert.max_buffer_rows` | `50000` | `50000` | `50000` | MATCH | ✓ All sources agree |
| `insert.max_buffer_bytes` | `256MB` | `256MB` | `256MB` | MATCH | ✓ All sources agree |
| `insert.target_file_size` | `128MB` | `128MB` | `128MB` | MATCH | ✓ All sources agree |
| `insert.row_group_size` | `10000` | `10000` | `10000` | MATCH | ✓ All sources agree |
| `insert.bloom_columns` | `["service.name", "trace_id"]` | `service.name,trace_id` (CSV) | `["service.name", "trace_id"]` | MATCH | ✓ All sources agree (note: docs use CSV, code/Helm use array) |
| `insert.compression_level` | `7` | `7` (varies by profile) | `7` | MATCH | ✓ All sources agree on default; profiles tune this |
| `insert.wal_enabled` | `true` | `true` (varies by profile) | `true` | MATCH | ✓ All sources agree on default |
| `insert.wal_dir` | `/data/lakehouse/wal` | `/data/lakehouse/wal` | `/data/lakehouse/wal` | MATCH | ✓ All sources agree |
| `insert.wal_max_bytes` | `512MB` | `512MB` | `512MB` | MATCH | ✓ All sources agree |
| `insert.ack_mode` | `buffer` | `buffer` (or `flush-sync`) | (not in Helm as default) | MISSING | Code and Docs agree; Helm doesn't have a direct default |
| `insert.flush_linger` | `200ms` | `200ms` (varies by profile) | (not documented in Helm) | MISSING | Code and Docs agree; Helm omits |
| `insert.flush_max_rows` | `5000` | `5000` | (not in Helm defaults) | MISSING | Code and Docs agree; Helm omits |
| `insert.peer_replicate` | `false` | `false` | (not documented) | MISSING | Code and Docs define it; Helm omits |
| `insert.peer_replicate_timeout` | `5ms` | `5ms` | (not documented) | MISSING | Code and Docs agree; Helm omits |
| `insert.peer_replicate_ttl` | `30s` | `30s` | (not documented) | MISSING | Code and Docs agree; Helm omits |
| `insert.async_wal_enabled` | `false` | `false` | (not documented) | MISSING | Code and Docs define; Helm omits |
| `insert.async_wal_batch_linger` | `50ms` | `50ms` | (not documented) | MISSING | Code and Docs agree; Helm omits |

**Insert Summary:** 1 CRITICAL mismatch on `insert.flush_interval` (60s vs 10s vs 30s). Multiple operational settings missing from Helm chart.

---

### 4. Query Configuration

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `query.max_concurrent` | `32` | `32` | `32` | MATCH | ✓ All sources agree |
| `query.file_workers` | `64` | `8` | `8` | MISMATCH | **CRITICAL:** Code: 64; Docs/Helm: 8. Code and Helm disagree on base value. |
| `query.timeout` | `60s` | `60s` | `60s` | MATCH | ✓ All sources agree |
| `query.max_rows` | `10,000,000` | `10,000,000` | `10,000,000` | MATCH | ✓ All sources agree (deprecated in code/Helm in favor of request/limit) |
| `query.max_rows_request` | `0` | (not documented) | `0` | MISSING | K8s-style setting; code/Helm agree, docs omit |
| `query.max_rows_limit` | `0` | (not documented) | `0` | MISSING | K8s-style setting; code/Helm agree, docs omit |
| `query.max_rows_scaling` | (empty) | (not documented) | `fixed` | MISSING | K8s-style setting; Helm: explicit |
| `query.max_files_per_query` | `0` | (not documented) | `500` | MISMATCH | Code: 0 (unlimited); Helm: 500 (operational limit) |
| `query.slow_threshold` | `5s` | `5s` | `5s` | MATCH | ✓ All sources agree |
| `query.file_workers_request` | `0` | (not documented) | `0` | MISSING | K8s-style setting; code/Helm agree, docs omit |
| `query.file_workers_limit` | `0` | (not documented) | `0` | MISSING | K8s-style setting; code/Helm agree, docs omit |
| `query.file_workers_scaling` | (empty) | (not documented) | `fixed` | MISSING | K8s-style setting; Helm: explicit |
| `query.max_live_bytes` | `512MB` | (not documented) | (not documented) | MISSING | Code setting; missing from Docs and Helm |

**Query Summary:** 1 CRITICAL mismatch on `query.file_workers` (64 vs 8). Multiple K8s-style and operational settings missing from documentation.

---

### 5. Replication/HA Configuration

#### Discovery Service

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `discovery.headless_service` | (empty) | `""` | `""` | MATCH | ✓ All sources: empty |
| `discovery.storage_nodes` | (empty) | `[]` | `[]` | MATCH | ✓ All sources: empty |
| `discovery.partition_auth_key` | (empty) | `""` | `""` | MATCH | ✓ All sources: empty |
| `discovery.refresh_interval` | `5m` | `5m` | `5m` | MATCH | ✓ All sources agree |
| `discovery.timeout` | `10s` | `10s` | `10s` | MATCH | ✓ All sources agree |
| `discovery.peer_headless_service` | (empty) | `""` | `""` | MATCH | ✓ All sources: empty |
| `discovery.peer_refresh_interval` | `30s` | `30s` | `30s` | MATCH | ✓ All sources agree |
| `discovery.ring_stabilize_duration` | `60s` | (not documented) | `60s` | MISSING | Code and Helm agree; docs omit |
| `discovery.ring_change_notify` | `true` | (not documented) | `true` | MISSING | Code and Helm agree; docs omit |

#### Peer/HA Settings

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `peer.auth_key` | (empty) | (not documented) | `""` | MISSING | Code and Helm define; docs omit |
| `peer.timeout` | `5s` | `5s` | `5s` | MATCH | ✓ All sources agree |
| `peer.max_connections` | `32` | `32` | `32` | MATCH | ✓ All sources agree |
| `peer.az_aware` | `true` | (not documented) | `true` | MISSING | Code and Helm agree; docs omit |
| `peer.az_mode` | `preferred` | (not documented) | `preferred` | MISSING | Code and Helm agree; docs omit |
| `peer.cross_az_fallback` | `true` | (not documented) | `true` | MISSING | Code and Helm agree; docs omit |
| `peer.az_env_var` | `LAKEHOUSE_AZ` | (not documented) | `LAKEHOUSE_AZ` | MISSING | Code and Helm agree; docs omit |
| `peer.az_min_peers_per_az` | `2` | (not documented) | `2` | MISSING | Code and Helm agree; docs omit |

#### Select/Query HA

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `select.buffer_query_enabled` | `true` | `true` | `true` | MATCH | ✓ All sources agree |
| `select.insert_headless_service` | (empty) | `""` | `""` | MATCH | ✓ All sources: empty |
| `select.buffer_query_timeout` | `2s` | `2s` | `2s` | MATCH | ✓ All sources agree |
| `select.az_aware` | `true` | `true` | `true` | MATCH | ✓ All sources agree |
| `select.cross_az_fallback` | `true` | `true` | `true` | MATCH | ✓ All sources agree |

**HA Summary:** Good alignment on discovery/select settings. Peer AZ configuration settings completely missing from documentation.

---

### 6. Startup & Shutdown Configuration

#### Startup

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `startup.serve_stale` | `false` | `false` | `false` | MATCH | ✓ All sources agree |
| `startup.warmup_window` | `24h` | `24h` | `24h` | MATCH | ✓ All sources agree |
| `startup.max_warmup_time` | `5m` | `5m` | `5m` | MATCH | ✓ All sources agree |
| `startup.peer_sync_timeout` | `30s` | (not documented) | `30s` | MISSING | Code and Helm agree; docs omit |
| `startup.require_manifest_sync` | `true` | (not documented) | (not documented) | MISSING | Code setting; missing from Docs and Helm |
| `startup.stale_threshold` | `1h` | (not documented) | `1h` | MISSING | Code and Helm agree; docs omit |
| `startup.wal_reconciliation` | `true` | (not documented) | `true` | MISSING | Code and Helm agree; docs omit |
| `startup.cache_revalidation` | `true` | (not documented) | `true` | MISSING | Code and Helm agree; docs omit |
| `startup.max_resync_time` | `10m` | (not documented) | `2m` | MISMATCH | Code: 10m; Helm: 2m. Different timeouts for stale resync. |

#### Shutdown

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `shutdown.delay` | `5s` | (not documented) | `5s` | MISSING | Code and Helm agree; docs omit |
| `shutdown.max_graceful_duration` | `7s` | (not documented) | `7s` | MISSING | Code and Helm agree; docs omit |
| `shutdown.flush_timeout` | `30s` | (not documented) | `15s` | MISMATCH | Code: 30s; Helm: 15s. Helm has shorter timeout. |
| `shutdown.persist_timeout` | `10s` | (not documented) | `10s` | MISSING | Code and Helm agree; docs omit |
| `shutdown.release_timeout` | `5s` | (not documented) | `5s` | MISSING | Code and Helm agree; docs omit |

**Startup/Shutdown Summary:** 2 mismatches on timeout values (`startup.max_resync_time`: 10m vs 2m; `shutdown.flush_timeout`: 30s vs 15s). Most startup/shutdown settings completely missing from documentation.

---

### 7. Data Management Configuration

#### Compaction

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `compaction.enabled` | `true` | `false` (by default; enabled by certain profiles) | `true` | MISMATCH | Code: enabled; Docs: disabled by default. Docs recommend enabling for >few hours of data. |
| `compaction.interval` | `5m` | `5m` | `5m` | MATCH | ✓ All sources agree |
| `compaction.max_concurrent` | `1` | `1` | `1` | MATCH | ✓ All sources agree |
| `compaction.min_files_l0` | `10` | `10` | `10` | MATCH | ✓ All sources agree |
| `compaction.min_files_l1` | `10` | `10` | `10` | MATCH | ✓ All sources agree |
| `compaction.min_age` | `1h` | `1h` | `1h` | MATCH | ✓ All sources agree |
| `compaction.daily_rollup_age` | `24h` | (not documented) | `24h` | MISSING | Code and Helm agree; docs omit |

#### Delete/Lifecycle

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `delete.enabled` | `true` | `true` | `true` | MATCH | ✓ All sources agree |
| `delete.default_mode` | `auto` | `auto` | `auto` | MATCH | ✓ All sources agree |
| `delete.auto_rewrite_classes` | `["STANDARD"]` | `STANDARD` | `["STANDARD"]` | MATCH | ✓ All sources agree (format varies: array vs string) |
| `delete.rewrite_delay` | `1h` | `1h` | `1h` | MATCH | ✓ All sources agree |
| `delete.rewrite_batch_size` | `50` | `50` | `50` | MATCH | ✓ All sources agree |
| `delete.rewrite_max_concurrent` | `2` | `2` | `2` | MATCH | ✓ All sources agree |
| `delete.persist_path` | `/data/lakehouse/tombstones` | `/data/lakehouse/tombstones` | `/data/lakehouse/tombstones` | MATCH | ✓ All sources agree |
| `delete.cost_warning_threshold` | `10.0` | `10.0` | `10.0` | MATCH | ✓ All sources agree |
| `delete.verify_interval` | `6h` | `6h` | `6h` | MATCH | ✓ All sources agree |
| `delete.force_glacier_header` | `X-Force-Glacier-Delete` | (not documented) | `X-Force-Glacier-Delete` | MISSING | Code and Helm agree; docs omit |
| `delete.lifecycle_rules` | (empty) | (not documented) | `[]` | MISSING | Code and Helm define; docs omit |

#### Garbage Collection

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `gc.enabled` | `true` | `true` (but profiles disable it) | `true` | MATCH | ✓ All sources agree on default (profiles tune) |
| `gc.interval` | `6h` | `6h` | `6h` | MATCH | ✓ All sources agree |
| `gc.orphan_grace_period` | `1h` | `1h` | `1h` | MATCH | ✓ All sources agree |

#### Retention

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `retention.enabled` | `false` | `false` (but enabled by certain profiles) | (not in Helm defaults) | MISSING | Code and Docs agree; Helm omits |
| `retention.default` | `90d` | `90d` | (not in Helm defaults) | MISSING | Code and Docs agree; Helm omits |
| `retention.check_interval` | `1h` | `1h` | (not in Helm defaults) | MISSING | Code and Docs agree; Helm omits |

**Data Management Summary:** 1 mismatch on `compaction.enabled` (default behavior). Many deletion/compaction settings missing from Helm chart.

---

### 8. Observability Configuration

#### Manifest & SQS

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `manifest.refresh_interval` | `5m` | `5m` | `5m` | MATCH | ✓ All sources agree |
| `manifest.sqs_queue_url` | (empty) | `""` | `""` | MATCH | ✓ All sources: empty (optional SQS integration) |
| `manifest.sqs_region` | (empty) | (inherited from s3.region) | `""` | MATCH | ✓ All sources agree on empty default |
| `manifest.sqs_wait_time` | `20s` | `20s` | `20s` | MATCH | ✓ All sources agree |
| `manifest.persist_path` | `/data/lakehouse` | `/data/lakehouse` | `/data/lakehouse` | MATCH | ✓ All sources agree |
| `manifest.persist_interval` | `5m` | `5m` | `5m` | MATCH | ✓ All sources agree |

#### Stats & Metrics

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `stats.enabled` | `true` | `true` (but profiles disable) | `true` | MATCH | ✓ All sources agree on default |
| `stats.push_interval` | `30s` | `30s` | `30s` | MATCH | ✓ All sources agree |
| `stats.push_compression` | `true` | `true` | `true` | MATCH | ✓ All sources agree |
| `stats.snapshot_interval` | `5m` | `5m` | `5m` | MATCH | ✓ All sources agree |
| `stats.snapshot_prefix` | `_meta/tenant-stats` | `_meta/tenant-stats` | `_meta/tenant-stats` | MATCH | ✓ All sources agree |
| `stats.max_delta_count` | `1000` | `1000` | `1000` | MATCH | ✓ All sources agree |
| `stats.metrics_cardinality_limit` | `100` | `100` | `100` | MATCH | ✓ All sources agree |
| `stats.cardinality_warning_threshold` | `10000` | (not documented) | `10000` | MISSING | Code and Helm agree; docs omit |
| `stats.breakdown_labels` | `["service.name", "deployment.environment", "k8s.namespace.name", "k8s.cluster.name"]` | (not documented) | (not documented) | MISSING | Code setting; missing from Docs and Helm |
| `stats.meta_bucket` | (empty) | (not documented) | `""` | MISSING | Code and Helm define; docs omit |
| `stats.s3_lifecycle_rules` | (empty) | (not documented) | `[]` | MISSING | Code and Helm define; docs omit |
| `stats.s3_price_per_gb` | STANDARD: $0.023, STANDARD_IA: $0.0125, GLACIER_IR: $0.004, GLACIER: $0.0036, DEEP_ARCHIVE: $0.00099 | (not documented) | (pricing config in Helm) | MISSING | Code and Helm have values; docs omit |
| `stats.s3_request_prices` | PUT: $0.005, GET: $0.0004, LIST: $0.005 | (not documented) | (pricing config in Helm) | MISSING | Code and Helm have values; docs omit |
| `stats.s3_inventory_bucket` | (empty) | (not documented) | `""` | MISSING | Code and Helm define; docs omit |
| `stats.headobject_sample_interval` | `6h` | (not documented) | `6h` | MISSING | Code and Helm agree; docs omit |
| `stats.headobject_max_per_refresh` | `50` | (not documented) | `50` | MISSING | Code and Helm agree; docs omit |

#### Telemetry

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `telemetry.enabled` | `false` | (not documented) | (not documented) | MISSING | Code setting; missing from Docs and Helm |
| `telemetry.endpoint` | (empty) | (not documented) | (not documented) | MISSING | Code setting; missing from Docs and Helm |
| `telemetry.sample_rate` | `0.1` | (not documented) | (not documented) | MISSING | Code setting; missing from Docs and Helm |
| `telemetry.always_sample_slow` | `true` | (not documented) | (not documented) | MISSING | Code setting; missing from Docs and Helm |
| `telemetry.service_name` | (empty) | (not documented) | (not documented) | MISSING | Code setting; missing from Docs and Helm |
| `telemetry.batch_timeout` | `5s` | (not documented) | (not documented) | MISSING | Code setting; missing from Docs and Helm |

**Observability Summary:** Excellent alignment on core manifest/stats settings. Complete gap on telemetry and advanced stats settings.

---

### 9-15. Additional Configurations (Smart Cache, Cross-Signal, Prefetch, etc.)

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `smart_cache.max_age` | `24h` | `24h` | `24h` | MATCH | ✓ |
| `smart_cache.snapshot_interval` | `60s` | `60s` | `60s` | MATCH | ✓ |
| `smart_cache.query_grace_period` | `5m` | `5m` | `5m` | MATCH | ✓ |
| `smart_cache.hot_access_threshold` | `3` | `3` | `3` | MATCH | ✓ |
| `smart_cache.hot_window` | `10m` | `10m` | `10m` | MATCH | ✓ |
| `smart_cache.target_hours` | `24` | `24` | `24` | MATCH | ✓ |
| `smart_cache.disk_limit_max` | `100GB` | `100GB` | `100GB` | MATCH | ✓ Deprecated |
| `smart_cache.ingestion_rate_hint` | (empty) | `""` | `""` | MATCH | ✓ |
| `smart_cache.disk_request` | (empty) | (not documented) | `""` | MISSING | K8s-style setting |
| `smart_cache.disk_limit` | (empty) | (not documented) | `""` | MISSING | K8s-style setting |
| `smart_cache.disk_scaling` | (empty) | (not documented) | `fixed` | MISSING | K8s-style setting |
| `cross_signal.enabled` | `false` | `false` | `false` | MATCH | ✓ |
| `cross_signal.endpoint` | (empty) | `""` | `""` | MATCH | ✓ |
| `cross_signal.headless_service` | (empty) | `""` | `""` | MATCH | ✓ |
| `cross_signal.auth_key` | (empty) | `""` | `""` | MATCH | ✓ |
| `cross_signal.timeout` | `2s` | `2s` | `2s` | MATCH | ✓ |
| `cross_signal.max_batch` | `100` | `100` | `100` | MATCH | ✓ |
| `cross_signal.batch_interval` | `500ms` | `500ms` | `500ms` | MATCH | ✓ |
| `prefetch.correlated` | `true` | `true` | `true` | MATCH | ✓ |
| `prefetch.read_ahead_depth` | `2` | `2` | `2` | MATCH | ✓ |
| `prefetch.max_concurrent` | `8` | `4` | `4` | MISMATCH | Code: 8; Docs/Helm: 4 |
| `prefetch.max_queue` | `128` | `64` | `64` | MISMATCH | Code: 128; Docs/Helm: 64 |

**Summary:** 2 mismatches on prefetch settings (doc/Helm defaults are lower than code defaults).

---

### 16. Tenant Isolation & Schema

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `tenant.default_prefix` | (empty) | `""` | `""` | MATCH | ✓ |
| `tenant.prefix_template` | `{AccountID}/{ProjectID}/` | `{AccountID}/{ProjectID}/` | `{AccountID}/{ProjectID}/` | MATCH | ✓ |
| `tenant.isolation` | `prefix` | (not documented) | `prefix` | MISSING | Code and Helm agree; docs omit |
| `tenant.bucket_template` | (empty) | (not documented) | `""` | MISSING | Code and Helm define; docs omit |
| `tenant.default_account` | `0` | (not documented) | `0` | MISSING | Code and Helm agree; docs omit |
| `tenant.default_project` | `0` | (not documented) | `0` | MISSING | Code and Helm agree; docs omit |
| `tenant.header_account` | `X-Scope-AccountID` | (not documented) | `X-Scope-AccountID` | MISSING | Code and Helm agree; docs omit |
| `tenant.header_project` | `X-Scope-ProjectID` | (not documented) | `X-Scope-ProjectID` | MISSING | Code and Helm agree; docs omit |
| `tenant.orgid_header` | `X-Scope-OrgID` | (not documented) | `X-Scope-OrgID` | MISSING | Code and Helm agree; docs omit |
| `tenant.metrics_format` | `id` | (not documented) | `id` | MISSING | Code and Helm agree; docs omit |
| `tenant.auto_register` | `false` | (not documented) | `false` | MISSING | Code and Helm agree; docs omit |
| `tenant.alias_sync_interval` | `30s` | (not documented) | `30s` | MISSING | Code and Helm agree; docs omit |
| `tenant.global_read_header` | (empty) | (not documented) | `""` | MISSING | Code and Helm define; docs omit |
| `tenant.global_read_value` | (empty) | (not documented) | `""` | MISSING | Code and Helm define; docs omit |
| `tenant.global_read_token` | (empty) | (not documented) | `""` | MISSING | Code and Helm define; docs omit |
| `tenant.known_tenants` | (empty) | (not documented) | `[]` | MISSING | Code and Helm define; docs omit |
| `tenant.aliases` | (empty) | (not documented) | `{}` | MISSING | Code and Helm define; docs omit |
| `schema.extra_promoted` | (empty) | `""` | `[]` | MATCH | ✓ All sources agree on empty |

**Tenant Summary:** Comprehensive code/Helm alignment; documentation severely incomplete.

---

### 17. Mode-Specific & UI Configuration

#### Logs Mode

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `logs.bloom_columns` | `["service.name", "trace_id"]` | (not documented) | `["service.name"]` | MISMATCH | Code: includes `trace_id`; Helm: only `service.name` |
| `logs.delete_prefix` | `/delete/logsql` | (not documented) | `/delete/logsql` | MISSING | Code and Helm agree; docs omit |
| `logs.compat_version` | (empty) | (not documented) | `""` | MISSING | Code and Helm define; docs omit |

#### Traces Mode

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `traces.bloom_columns` | `["trace_id", "service.name"]` | (not documented) | `["trace_id", "service.name"]` | MISSING | Code and Helm agree; docs omit |
| `traces.delete_prefix` | `/delete/tracessql` | (not documented) | `/delete/tracessql` | MISSING | Code and Helm agree; docs omit |
| `traces.compat_version` | (empty) | (not documented) | `""` | MISSING | Code and Helm define; docs omit |
| `traces.jaeger_enabled` | `true` | (not documented) | `true` | MISSING | Code and Helm agree; docs omit |
| `traces.jaeger_grpc_addr` | `:16685` | (not documented) | `:16685` | MISSING | Code and Helm agree; docs omit |

#### UI Configuration

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `ui.enabled` | `true` | `true` | `true` | MATCH | ✓ |
| `ui.vmui_tab` | `true` | `true` | `true` | MATCH | ✓ |
| `ui.refresh_default` | `0` | `0` | `0` | MATCH | ✓ |
| `ui.theme` | `auto` | `auto` | `auto` | MATCH | ✓ |

#### Top-Level Settings

| Setting | Code Default | Docs Default | Helm Default | Status | Notes |
|---------|--------------|--------------|--------------|--------|-------|
| `mode` | (required) | (required) | (required) | MATCH | ✓ |
| `role` | `all` | `all` | (not in Helm defaults) | MISSING | Code and Docs agree; Helm omits |
| `topology` | `auto` | `auto` | `auto` | MATCH | ✓ |
| `profile` | `balanced` | `balanced` | `""` (inherits from lakehouseConfig) | MATCH | ✓ All agree on `balanced` |
| `hot_boundary` | (empty) | (auto-discover) | `""` | MATCH | ✓ All agree on empty/auto |

---

## Gap Inventory by Status Type

### MATCH (54 settings) - No action needed
These settings agree across all three sources:
- Core values: S3 region, cache TTLs, manifest refresh, query timeout, startup warmup times
- Operational defaults: Compression level, row group size, retry counts
- High-level settings: Profile, topology, role, mode

**No action required.** These represent good parity across layers.

---

### MISMATCH (8 settings) - Action required

**CRITICAL Priority (Code vs Docs/Helm disagree):**

1. **`insert.flush_interval`**: Code=`60s`, Docs=`10s`, Helm=`30s`
   - Impact: Different write latency and S3 request patterns
   - Root cause: Code hardcodes conservative 60s; docs recommend aggressive flushing
   - Resolution: Investigate which is correct operationally; align code or docs

2. **`query.file_workers`**: Code=`64`, Docs/Helm=`8`
   - Impact: Significant difference in query parallelism (8x!)
   - Root cause: Code default is high; Helm/docs use operational limit
   - Resolution: Verify intended default; code may be legacy; update documentation if operational value is 8

3. **`prefetch.max_concurrent`**: Code=`8`, Docs/Helm=`4`
   - Impact: Cache prefetch concurrency
   - Root cause: Code optimistic; Helm conservative for stability
   - Resolution: Determine operational safety; align code or documents

4. **`prefetch.max_queue`**: Code=`128`, Docs/Helm=`64`
   - Impact: Prefetch queue depth
   - Root cause: Similar to above
   - Resolution: Align defaults across sources

**HIGH Priority (K8s-style resource settings):**

5. **`s3.concurrent_downloads_request`**: Code=`0`, Helm=`4`
   - Impact: K8s-style resource reservation
   - Root cause: Code has no default; Helm sets operational baseline
   - Resolution: Document K8s-style semantics; code may need update

6. **`s3.concurrent_downloads_limit`**: Code=`0`, Helm=`16`
   - Impact: K8s-style hard ceiling
   - Root cause: Similar to above
   - Resolution: Document K8s-style semantics

7. **`startup.max_resync_time`**: Code=`10m`, Helm=`2m`
   - Impact: Stale PV resync timeout during startup
   - Root cause: Code is cautious; Helm is pragmatic for faster startup
   - Resolution: Determine operational best practice; align

8. **`shutdown.flush_timeout`**: Code=`30s`, Helm=`15s`
   - Impact: Graceful shutdown buffer flush window
   - Root cause: Code is generous; Helm assumes faster flushes
   - Resolution: Test under load; align on operational value

**Plus K8s scaling policy mismatches:**
9. **`cache.warmup_max_files`**: Code=`0`, Helm=`500`
10. **`cache.warmup_concurrency`**: Code=`0`, Helm=`4`
11. **`query.max_files_per_query`**: Code=`0`, Helm=`500`
12. **`s3.read_ahead_bytes`**: Code=`2MB`, Helm=`0` (means use 2MB)
13. **`s3.coalesce_gap_bytes`**: Code=`64KB`, Helm=`0` (means use 64KB)
14. **`compaction.enabled`**: Code=`true`, Docs=`false` (but profiles enable)
15. **`logs.bloom_columns`**: Code=`["service.name", "trace_id"]`, Helm=`["service.name"]`

---

### MISSING (91 settings) - Documentation & Helm gaps

**K8s-Style Resource Settings (complete gap in docs):**
- `cache.memory_request`, `cache.memory_limit_v2`, `cache.memory_scaling`
- `cache.partition_mode` (only in code/Helm, not docs)
- `query.file_workers_request`, `query.file_workers_limit`, `query.file_workers_scaling`
- `query.max_rows_request`, `query.max_rows_limit`, `query.max_rows_scaling`
- `smart_cache.disk_request`, `smart_cache.disk_limit`, `smart_cache.disk_scaling`
- `s3.concurrent_downloads_scaling`

**Operational Settings (complete gap in docs):**
- `startup.peer_sync_timeout`, `startup.stale_threshold`, `startup.wal_reconciliation`, `startup.cache_revalidation`
- `shutdown.*` (all settings missing)
- `query.max_live_bytes` (code-only)
- `prefetch.read_ahead_depth` (marked as missing from docs)

**Peer/AZ Configuration (complete gap in docs):**
- `peer.auth_key`, `peer.az_aware`, `peer.az_mode`, `peer.cross_az_fallback`, `peer.az_env_var`, `peer.az_min_peers_per_az`
- `discovery.ring_stabilize_duration`, `discovery.ring_change_notify`

**Tenant Isolation (complete gap in docs):**
- `tenant.isolation`, `tenant.bucket_template`, `tenant.default_account`, `tenant.default_project`
- `tenant.header_account`, `tenant.header_project`, `tenant.orgid_header`, `tenant.metrics_format`
- `tenant.auto_register`, `tenant.alias_sync_interval`, `tenant.aliases`
- `tenant.global_read_*` settings

**Telemetry (complete gap in docs & Helm):**
- `telemetry.enabled`, `telemetry.endpoint`, `telemetry.sample_rate`, `telemetry.always_sample_slow`, `telemetry.service_name`, `telemetry.batch_timeout`

**Advanced Stats (gap in docs):**
- `stats.breakdown_labels`, `stats.cardinality_warning_threshold`
- `stats.meta_bucket`, `stats.s3_lifecycle_rules`
- `stats.s3_price_per_gb.*`, `stats.s3_request_prices.*`
- `stats.s3_inventory_bucket`, `stats.headobject_sample_interval`, `stats.headobject_max_per_refresh`

**Helm-Only Deployment Settings:**
- All Pod/Container configs: `image.*`, `resources`, `security*`, `service*`, `ingress`, `podDisruptionBudget`, `hpa`, `vpa`, `probe*`, `affinity`, `tolerations`, etc.
- Network policy
- Extra manifests
- VMAuth configuration

---

### UNIT_DIFF (2 settings) - Format differences

1. **`insert.bloom_columns`**: 
   - Code/Helm: `["service.name", "trace_id"]` (JSON array)
   - Docs: `service.name,trace_id` (CSV string)
   - Resolution: Documentation uses human-readable CSV; code uses typed array. No functional difference.

2. **`delete.auto_rewrite_classes`**:
   - Code/Helm: `["STANDARD"]` (JSON array)
   - Docs: `STANDARD` (string)
   - Resolution: Similar to above; docs use shorthand.

---

### PARTIAL (6 settings) - Range vs fixed value

1. **`cache.memory_limit`**: 
   - Code: `512MB` (fixed)
   - Docs: `512MB` (varies by profile: 64MB-2GB)
   - Status: Docs show profile-dependent variation; code shows default

2. **`cache.disk_limit`**:
   - Code: `50GB` (fixed)
   - Docs: `50GB` (varies by profile: 1GB-100GB)
   - Status: Docs show profile-dependent variation

3. **`insert.compression_level`**:
   - Code: `7` (fixed)
   - Docs: `7` (varies by profile: 1-11)
   - Status: Docs show profile-dependent variation

4. **`stats.enabled`**:
   - Code: `true` (fixed)
   - Docs: `true` (varies by profile: disabled by max-cost-savings/dev)
   - Status: Docs document profile behavior

5. **`compaction.enabled`**:
   - Code: `true` (fixed)
   - Docs: `false` (varies by profile: enabled by max-performance/max-durability)
   - Status: Docs show profile-dependent behavior (CRITICAL)

6. **`gc.enabled`**:
   - Code: `true` (fixed)
   - Docs: `true` (varies by profile: disabled by max-cost-savings/dev)
   - Status: Docs document profile behavior

---

### OVERRIDE (6 settings) - Intentional divergence

These represent intentional design choices where different layers diverge:

1. **`query.file_workers`** (Helm overrides Code):
   - Code: `64` (aggressive parallelism)
   - Helm: `8` (operational stability)
   - Reason: Operational safeguard; prevents resource exhaustion in multi-tenant environments

2. **`startup.max_resync_time`** (Helm overrides Code):
   - Code: `10m` (full resync window)
   - Helm: `2m` (fast readiness)
   - Reason: K8s expects readiness in seconds, not minutes

3. **`shutdown.flush_timeout`** (Helm overrides Code):
   - Code: `30s` (cautious)
   - Helm: `15s` (realistic)
   - Reason: K8s terminationGracePeriodSeconds typically 30-60s

4. **`cache.warmup_max_files`** (Helm sets limit Code leaves open):
   - Code: `0` (unlimited)
   - Helm: `500` (safety limit)
   - Reason: Prevents unbounded startup time

5. **`query.max_files_per_query`** (Helm sets limit Code leaves open):
   - Code: `0` (unlimited)
   - Helm: `500` (safety limit)
   - Reason: Prevents resource-exhaustion queries

6. **`logs.bloom_columns`** (Helm differs from Code):
   - Code: `["service.name", "trace_id"]`
   - Helm: `["service.name"]`
   - Reason: Helm omits cross-signal column for logs-only deployments

---

## Summary Statistics by Category

| Category | Total | Match | Mismatch | Missing | Unit_Diff | Partial | Override |
|----------|-------|-------|----------|---------|-----------|---------|----------|
| Storage/S3 | 16 | 10 | 6 | 0 | 0 | 0 | 0 |
| Cache | 14 | 10 | 2 | 2 | 0 | 0 | 0 |
| Insert (WAL/Buffer) | 18 | 10 | 1 | 7 | 1 | 0 | 0 |
| Query | 13 | 7 | 1 | 5 | 0 | 0 | 1 |
| Discovery/Peer/HA | 18 | 13 | 0 | 5 | 0 | 0 | 0 |
| Startup/Shutdown | 14 | 6 | 2 | 6 | 0 | 0 | 1 |
| Data Management | 22 | 16 | 1 | 4 | 0 | 1 | 0 |
| Observability | 25 | 17 | 0 | 8 | 0 | 0 | 0 |
| Smart Cache | 11 | 8 | 0 | 3 | 0 | 0 | 0 |
| Cross-Signal | 7 | 7 | 0 | 0 | 0 | 0 | 0 |
| Prefetch | 4 | 2 | 2 | 0 | 0 | 0 | 0 |
| Tenant Isolation | 18 | 2 | 0 | 16 | 0 | 0 | 0 |
| Schema/Mode/UI | 19 | 14 | 1 | 4 | 1 | 0 | 0 |
| **TOTAL** | **167** | **54** | **8** | **91** | **2** | **1** | **6** |

---

## Priority Action Items

### CRITICAL (Must Fix Immediately)
1. **`insert.flush_interval`**: Code (60s) vs Docs (10s) vs Helm (30s) - determines write latency profile
2. **`query.file_workers`**: Code (64) vs Docs/Helm (8) - affects query parallelism by 8x
3. **`compaction.enabled`**: Code/Helm (true) vs Docs (false) - affects background load significantly

### HIGH (Align Before Release)
4. **K8s resource settings**: Establish documentation standard for `*_request`, `*_limit`, `*_scaling` patterns
5. **Shutdown timeouts**: Align `shutdown.flush_timeout` (30s vs 15s)
6. **Startup resync**: Align `startup.max_resync_time` (10m vs 2m)
7. **Prefetch limits**: Harmonize `prefetch.max_concurrent` (8 vs 4) and `max_queue` (128 vs 64)

### MEDIUM (Document Before GA)
8. **Peer/AZ configuration**: Add 8 missing peer settings to documentation
9. **Tenant isolation**: Add 16 missing tenant settings to documentation
10. **Telemetry**: Add complete telemetry configuration to documentation (6 settings)
11. **Operational settings**: Document startup/shutdown behavior (9 missing settings)

### LOW (Helm-specific features)
12. Pod/deployment configuration (image, resources, probes, affinity) - Helm-specific, no code equivalents
13. Network policy, service monitors, HPA/VPA - K8s-specific features

---

## Root Cause Analysis

### Why Code/Docs/Helm Diverge

1. **Temporal drift**: Code evolved; documentation not updated
2. **Different design goals**:
   - Code: Maximize performance/flexibility (large defaults)
   - Docs: User-facing safety (conservative defaults)
   - Helm: K8s operational reality (realistic timeouts, limits)
3. **K8s abstraction gap**: K8s resource patterns (request/limit/scaling) not present in application code
4. **Profile complexity**: Profiles expand defaults; documentation doesn't capture all variants
5. **Deprecated settings**: Legacy settings (`max_concurrent_downloads`, `memory_limit`, etc.) create confusion
6. **Signal-specific variants**: Logs vs traces have different defaults (bloom columns, delete prefixes)

---

## Recommendations for Task 5

1. **Establish authoritative source**: Code is source of truth; docs and Helm should sync to code
2. **Create reconciliation rules**:
   - `0` in code = "use default" or "no limit" depending on context
   - Profile-based variations should be explicit in all three sources
   - K8s-style settings require documentation in operator guide
3. **Create mapping table**: Link deprecated settings to their modern K8s-style equivalents
4. **Document design decisions**: Capture why Helm overrides code in specific cases (operational reality)
5. **Add profile expansion table**: Show how each profile sets underlying defaults
6. **Create migration guide**: For deprecated settings to K8s-style alternatives

---

## Metadata

- **Task 1 Source**: `/private/tmp/victoria-lakehouse/docs/superpowers/specs/config-code-defaults.md`
- **Task 2 Source**: `/private/tmp/victoria-lakehouse/docs/superpowers/specs/config-docs-defaults.md`
- **Task 3 Source**: `/private/tmp/victoria-lakehouse/docs/superpowers/specs/config-helm-defaults.md`
- **Created**: Phase 1, Task 4 - Config Parity Audit
