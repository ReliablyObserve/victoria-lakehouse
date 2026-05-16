# Settings Profiles — Design Spec

## Goal

Named configuration presets (`balanced`, `max-performance`, `max-durability`, `max-cost-savings`, `dev`) that set default values for all config flags. Profiles are base defaults — any explicit flag or config file setting overrides them. Profiles can be set at three levels: global, per-signal (logs/traces), and per-role (insert/select within a signal).

Additionally: fix the hybrid cost model to include doubled network ingestion cost from mirroring data to both VL/VT insert and Lakehouse insert.

## Scope

1. **Profile system**: 5 named profiles in Go config + Helm chart
2. **Precedence chain**: profile → config file → flags, with per-signal and per-role granularity
3. **Documentation**: getting-started.md profile guide with cost/performance impact tables
4. **Cost model fix**: add doubled delivery network cost to hybrid calculations in cost-estimates.md and cost-comparison.md

## Non-Goals

- Runtime profile switching (restart required)
- Custom user-defined profile names (use explicit overrides instead)
- Profile API endpoint (profiles are build-time config, not runtime state)

---

## Profile Definitions

### Overview

| Profile | Default | Target Use Case |
|---|---|---|
| `balanced` | Yes | Production general-purpose — good trade-offs everywhere |
| `max-performance` | | Lowest query + ingest latency, accepts higher resource usage and cost |
| `max-durability` | | Zero data loss priority — aggressive persistence, verification, redundancy |
| `max-cost-savings` | | Minimize S3 PUTs, network, compute, memory — archive/cold-only workloads |
| `dev` | | Local development — fast iteration, small footprint, MinIO-friendly |

### Complete Settings Matrix

Every setting that differs from `balanced` is shown. Settings not listed here are identical across all profiles (e.g., `s3.region`, `tenant.*`, `schema.*`).

#### Insert Path

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `insert.flush_interval` | 10s | 5s | 10s | 30s | 1s |
| `insert.wal_enabled` | true | false | true | false | false |
| `insert.wal_max_bytes` | 512MB | 512MB | 1GB | 256MB | 32MB |
| `insert.compression_level` | 7 | 3 | 7 | 11 | 1 |
| `insert.max_buffer_rows` | 50000 | 100000 | 50000 | 25000 | 1000 |
| `insert.max_buffer_bytes` | 256MB | 512MB | 256MB | 128MB | 32MB |
| `insert.target_file_size` | 128MB | 64MB | 128MB | 256MB | 8MB |
| `insert.row_group_size` | 10000 | 5000 | 10000 | 50000 | 1000 |

**Rationale:**

- **max-performance**: WAL off eliminates fsync overhead. Smaller flush interval + smaller target files = faster read-after-write. ZSTD-3 trades 25% more storage for 20% faster writes. Larger buffers absorb burst ingest.
- **max-durability**: WAL on with larger max bytes ensures crash recovery. Default compression preserves storage efficiency. Standard buffer sizes.
- **max-cost-savings**: WAL off saves disk I/O. Longer flush = fewer S3 PUTs ($0.005/1K PUTs). ZSTD-11 squeezes ~8% more compression (saves $50/mo per 2TB/day vs level 7). Larger target files = fewer S3 objects = cheaper storage and listing. Larger row groups = better compression within groups.
- **dev**: Everything minimal. ZSTD-1 for fastest possible flushes. 1s flush for instant read-after-write during development. Tiny buffers to avoid memory pressure on laptops.

#### Select / Query Path

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `select.buffer_query_enabled` | true | true | true | false | true |
| `select.buffer_query_timeout` | 2s | 1s | 2s | 2s | 2s |
| `query.file_workers` | 8 | 16 | 8 | 4 | 2 |
| `query.max_concurrent` | 32 | 64 | 32 | 16 | 4 |
| `query.timeout` | 60s | 120s | 60s | 30s | 60s |
| `query.max_rows` | 10M | 50M | 10M | 1M | 100K |
| `query.slow_threshold` | 5s | 10s | 5s | 3s | 1s |

**Rationale:**

- **max-performance**: More workers + higher concurrency + higher row limit = handle larger queries faster. Shorter buffer query timeout fails fast.
- **max-cost-savings**: Buffer query disabled saves cross-AZ network for unflushed data queries. Fewer workers = less CPU/memory. Lower row cap prevents runaway cold scans.
- **dev**: Minimal workers (laptop-friendly). Low row cap catches accidental full-table scans during development.

#### Cache

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `cache.memory_limit` | 512MB | 2GB | 512MB | 128MB | 64MB |
| `cache.disk_limit` | 50GB | 100GB | 50GB | 10GB | 1GB |
| `cache.footer_ttl` | 1h | 4h | 1h | 30m | 1m |
| `cache.bloom_ttl` | 1h | 4h | 1h | 30m | 1m |
| `cache.page_ttl` | 10m | 1h | 10m | 5m | 1m |

**Rationale:**

- **max-performance**: Large memory + disk cache with long TTLs keeps most data hot. 4h footer/bloom TTL means repeated queries within a shift never re-fetch metadata from S3.
- **max-cost-savings**: Small cache footprint. Shorter TTLs free memory sooner. Reduces infrastructure requirements (smaller instances).
- **dev**: Tiny cache — development data is small, no need to cache aggressively.

#### Smart Cache

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `smart_cache.target_hours` | 24 | 72 | 24 | 6 | 1 |
| `smart_cache.max_age` | 24h | 72h | 24h | 6h | 1h |
| `smart_cache.snapshot_interval` | 60s | 30s | 30s | 5m | 30s |
| `smart_cache.hot_access_threshold` | 3 | 2 | 3 | 5 | 2 |
| `smart_cache.hot_window` | 10m | 15m | 10m | 5m | 5m |
| `smart_cache.disk_limit_max` | 100GB | 200GB | 100GB | 20GB | 2GB |

**Rationale:**

- **max-performance**: 72h cache window means 3 days of data stays hot. Lower hot threshold promotes entries to hot tier faster. More aggressive snapshots for fast restart recovery.
- **max-cost-savings**: 6h window — only very recent data cached. Higher hot threshold means only truly frequent queries get hot treatment.

#### Prefetch & Cross-Signal

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `prefetch.correlated` | true | true | true | false | false |
| `prefetch.read_ahead_depth` | 2 | 4 | 2 | 0 | 0 |
| `prefetch.max_concurrent` | 8 | 16 | 8 | 2 | 1 |
| `prefetch.max_queue` | 128 | 256 | 128 | 32 | 8 |
| `cross_signal.enabled` | false | true | false | false | false |

**Rationale:**

- **max-performance**: Aggressive prefetch + read-ahead + cross-signal ensures subsequent queries hit cache. 4-partition read-ahead for sequential scans.
- **max-cost-savings**: All prefetch disabled — only fetch data when explicitly queried. Zero speculative S3 GETs.

#### S3 & Network

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `s3.max_connections` | 128 | 256 | 128 | 64 | 16 |
| `s3.max_concurrent_downloads` | 16 | 32 | 16 | 8 | 4 |
| `s3.timeout` | 30s | 15s | 30s | 60s | 30s |
| `s3.retry_max` | 3 | 5 | 5 | 3 | 1 |
| `s3.force_path_style` | false | false | false | false | true |

**Rationale:**

- **max-performance**: More connections = higher parallel S3 throughput. Shorter timeout + more retries = fail fast then retry.
- **max-durability**: More retries to handle transient S3 failures.
- **max-cost-savings**: Fewer connections = less TCP overhead. Longer timeout = fewer retries = fewer wasted S3 requests.
- **dev**: `force_path_style: true` for MinIO compatibility out of the box. Single retry (local MinIO doesn't need retries).

#### Compaction

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `compaction.enabled` | false | true | true | false | false |
| `compaction.interval` | 5m | 2m | 5m | 15m | 5m |
| `compaction.max_concurrent` | 1 | 2 | 1 | 1 | 1 |
| `compaction.min_files_l0` | 10 | 5 | 10 | 20 | 10 |

**Rationale:**

- **max-performance**: Compaction enabled + aggressive interval produces optimally-sized files for faster queries (fewer files to scan, better row group stats).
- **max-durability**: Compaction enabled ensures tombstoned data is physically removed during merges (mode=permanent/auto).
- **max-cost-savings**: Compaction disabled — avoids S3 read+write I/O cost. Files may be suboptimal but storage is cheap.

#### Persistence & Discovery

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `manifest.persist_interval` | 5m | 1m | 1m | 15m | 5s |
| `manifest.refresh_interval` | 5m | 1m | 5m | 15m | 5s |
| `startup.serve_stale` | false | true | false | false | true |
| `startup.warmup_window` | 24h | 72h | 24h | 6h | 1h |
| `startup.max_warmup_time` | 5m | 10m | 5m | 2m | 10s |

**Rationale:**

- **max-performance**: Frequent manifest persistence = fast restart. `serve_stale` = serve immediately from disk cache on restart while refreshing from S3 in background. 72h warmup window matches cache target.
- **max-durability**: Frequent manifest persistence = minimal data loss window on crash.
- **dev**: `serve_stale` for fast restart during development. 5s intervals for instant visibility.

#### Delete

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `delete.enabled` | true | true | true | true | true |
| `delete.default_mode` | auto | auto | permanent | hide | auto |
| `delete.verify_interval` | 6h | 6h | 1h | 24h | 1m |
| `delete.rewrite_delay` | 1h | 30m | 1h | 6h | 10s |
| `delete.rewrite_batch_size` | 50 | 100 | 50 | 25 | 5 |

**Rationale:**

- **max-durability**: `permanent` mode = physically remove data. 1h verify interval catches any leaks quickly.
- **max-cost-savings**: `hide` mode = tombstone only, never rewrites files (zero S3 I/O for deletions). 24h verify to minimize S3 reads.
- **dev**: Fast rewrite for testing delete functionality.

#### Stats & UI

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `stats.enabled` | true | true | true | false | true |
| `stats.push_interval` | 30s | 15s | 30s | 5m | 30s |
| `stats.snapshot_interval` | 5m | 5m | 1m | 30m | 30s |
| `ui.enabled` | true | true | true | false | true |

**Rationale:**

- **max-cost-savings**: Stats and UI disabled — fewer S3 writes for snapshots, less compute for aggregation. Enable explicitly if needed.

#### Peer Cache

| Setting | `balanced` | `max-performance` | `max-durability` | `max-cost-savings` | `dev` |
|---|---|---|---|---|---|
| `peer.max_connections` | 32 | 64 | 32 | 16 | 8 |
| `peer.timeout` | 5s | 2s | 5s | 10s | 5s |
| `peer.az_aware` | true | true | true | true | false |
| `discovery.peer_refresh_interval` | 30s | 10s | 30s | 60s | 30s |

**Rationale:**

- **max-performance**: More connections + faster timeout + faster peer refresh = aggressive L3 cache usage.
- **dev**: AZ-aware off — no AZ concept in local development.

---

## Precedence & Resolution

### Precedence Chain (later wins)

```
built-in Default() → profile defaults → config file → per-signal config → per-role config → explicit CLI flags
```

### Level Hierarchy

Profiles can be set at three levels. More specific levels inherit from less specific, and override where set:

| Level | Scope | Example |
|---|---|---|
| **Global** | All signals, all roles | `lakehouseConfig.profile: balanced` |
| **Per-signal** | One signal (logs or traces), all roles | `logs.profile: max-durability` |
| **Per-role** | One signal + one role | `logs.insert.profile: max-performance` |

### Resolution Algorithm

For any config setting, resolve by walking from most specific to least specific:

```
1. Explicit override (flag or config file value for this signal+role)?  → use it
2. Per-role profile set (e.g., logs.insert.profile)?                   → use profile value
3. Per-signal profile set (e.g., logs.profile)?                        → use profile value
4. Global profile set (e.g., lakehouseConfig.profile)?                 → use profile value
5. None of the above                                                   → use built-in Default()
```

### Helm Chart Examples

#### Example 1: Global profile with one override

```yaml
lakehouseConfig:
  profile: balanced
  insert:
    compression_level: 11    # override: want better compression
```

All settings from `balanced`, except compression is ZSTD-11.

#### Example 2: Different profiles per signal

```yaml
lakehouseConfig:
  profile: balanced          # global fallback

logs:
  profile: max-durability    # logs are business-critical
  config:
    insert:
      wal_max_bytes: 2GB     # override: larger WAL for high-volume logs

traces:
  profile: max-cost-savings  # traces are best-effort archive
```

Logs get `max-durability` defaults (WAL on, 1h verify, permanent delete mode) with a larger WAL. Traces get `max-cost-savings` (no WAL, no buffer query, no prefetch).

#### Example 3: Different profiles per role within a signal

```yaml
lakehouseConfig:
  profile: balanced

logs:
  profile: max-durability    # signal-level default for logs
  insert:
    profile: max-durability  # inherits from logs.profile (same here)
  select:
    profile: max-performance # query speed matters most for reads
  config:
    select:
      query:
        max_rows: 100000000  # override: allow very large cold scans
```

Logs insert uses `max-durability` (WAL, conservative flush). Logs select uses `max-performance` (big caches, aggressive prefetch, more workers) with an explicit max_rows override.

#### Example 4: Dev profile for local docker-compose

```yaml
lakehouseConfig:
  profile: dev
  s3:
    bucket: lakehouse-data
    endpoint: http://minio:9000
    access_key: minioadmin
    secret_key: minioadmin
```

Everything from `dev` profile (tiny buffers, 1s flush, no WAL, MinIO-compatible). Only S3 connection needs explicit config.

#### Example 5: Minimal production (profile does the work)

```yaml
lakehouseConfig:
  profile: max-durability
  s3:
    bucket: obs-archive
    region: us-east-1
  discovery:
    headless_service: vlstorage.monitoring.svc.cluster.local
```

Three lines of actual config. Profile handles the other 40+ settings.

### CLI Flag Examples

CLI flags only support a global profile (`--lakehouse.profile`). Per-signal and per-role profiles are available via YAML config files (loaded with `--lakehouse.config`) or Helm values — they don't have dedicated CLI flags because Helm manages separate insert/select pods anyway.

```bash
# Global profile via CLI flag + one override
lakehouse-logs \
  --lakehouse.profile=max-performance \
  --lakehouse.insert.wal-enabled=true \
  --lakehouse.s3.bucket=obs-archive

# Dev mode with MinIO
lakehouse-logs \
  --lakehouse.profile=dev \
  --lakehouse.s3.bucket=lakehouse-data \
  --lakehouse.s3.endpoint=http://localhost:9000 \
  --lakehouse.s3.access-key=minioadmin \
  --lakehouse.s3.secret-key=minioadmin

# Per-role profiles via YAML config file (not CLI flags)
# config.yaml:
#   lakehouse:
#     profile: balanced
#     logs:
#       insert:
#         profile: max-durability
#       select:
#         profile: max-performance
lakehouse-logs --lakehouse.config=config.yaml --lakehouse.s3.bucket=obs-archive
```

---

## Go Implementation

### Profile Type and Registry

New file: `internal/config/profile.go`

```go
type Profile string

const (
    ProfileBalanced       Profile = "balanced"
    ProfileMaxPerformance Profile = "max-performance"
    ProfileMaxDurability  Profile = "max-durability"
    ProfileMaxCostSavings Profile = "max-cost-savings"
    ProfileDev            Profile = "dev"
)

func ProfileConfig(p Profile) *Config { ... }
func ValidProfiles() []Profile { ... }
```

`ProfileConfig()` returns a full `*Config` with all fields set for that profile. This is a pure function — no side effects, no I/O. Each profile is a complete config, not a diff/patch.

### Load Order Change

Current:
```go
cfg := Default()
merged := mergeConfig(cfg, &fileConfig)
```

New:
```go
cfg := Default()
profileCfg := ProfileConfig(resolvedProfile)  // profile on top of defaults
merged := mergeConfig(cfg, profileCfg)
merged = mergeConfig(merged, &fileConfig)      // file overrides profile
// CLI flags override via existing flag parsing
```

The existing `mergeConfig()` function already does field-by-field "overlay non-zero values" — profiles use the same mechanism. No changes to mergeConfig needed.

### Config Struct Addition

```go
type Config struct {
    Profile  Profile  `yaml:"profile"`  // NEW — global profile name
    // ... existing fields unchanged
}
```

### Validation

```go
func (c *Config) Validate() error {
    // Existing validations...
    
    switch c.Profile {
    case ProfileBalanced, ProfileMaxPerformance, ProfileMaxDurability,
         ProfileMaxCostSavings, ProfileDev, "":
        // valid (empty = balanced)
    default:
        return fmt.Errorf("--lakehouse.profile must be one of: balanced, max-performance, max-durability, max-cost-savings, dev; got %q", c.Profile)
    }
}
```

### Helm Chart Integration

In `values.yaml`:

```yaml
lakehouseConfig:
  # -- Configuration profile preset. Sets defaults for all settings below.
  # Any explicit setting overrides the profile value.
  # Options: balanced (default), max-performance, max-durability, max-cost-savings, dev
  profile: balanced

logs:
  # -- Per-signal profile override for logs. Empty = inherit lakehouseConfig.profile.
  profile: ""
  insert:
    # -- Per-role profile override for logs insert. Empty = inherit logs.profile.
    profile: ""
  select:
    # -- Per-role profile override for logs select. Empty = inherit logs.profile.
    profile: ""

traces:
  # -- Per-signal profile override for traces. Empty = inherit lakehouseConfig.profile.
  profile: ""
  insert:
    profile: ""
  select:
    profile: ""
```

The chart template passes the resolved profile as `--lakehouse.profile=<value>` to the container args. Per-role profiles are resolved in the template: if `logs.insert.profile` is set, use it; else if `logs.profile` is set, use it; else use `lakehouseConfig.profile`.

---

## Hybrid Cost Model — Network Doubling Fix

### Problem

Current cost tables show identical data transfer costs for Hybrid and VL/VT EBS Only. But Hybrid mirrors ingested data to **both** VL/VT insert (hot tier) **and** Lakehouse insert (cold tier). This doubles the cross-AZ ingestion delivery network cost.

### Fix

In `docs/cost-comparison.md`, the Data Transfer Costs table currently shows:

```
Lakehouse Hybrid cross-AZ ingest: $0.01/GB × 500GB/day × 30 = $150/mo
VL/VT EBS Only cross-AZ ingest:   $0.01/GB × 500GB/day × 30 = $150/mo
```

Should be:

```
Lakehouse Hybrid cross-AZ ingest: $0.01/GB × 500GB/day × 2 destinations × 30 = $300/mo
VL/VT EBS Only cross-AZ ingest:   $0.01/GB × 500GB/day × 30 = $150/mo
```

This adds $150/mo to Hybrid at 500 GB/day. Total monthly changes:
- Hybrid 1yr: $2,859 → $3,009/mo (+$150)
- Hybrid 2yr: $3,547 → $3,697/mo (+$150)

Updated totals in `docs/cost-estimates.md` and README must also reflect this.

### Affected Files

| File | Change |
|---|---|
| `docs/cost-comparison.md` | Fix Data Transfer table: Hybrid cross-AZ ingest = 2× destinations. Update Total Monthly Cost table. Update Key Insights with new percentages. |
| `docs/cost-estimates.md` | Update any tables referencing hybrid monthly costs if they derive from cost-comparison numbers. |
| `README.md` | Update The Cost Case table if hybrid monthly cost changes. |

### Impact on Recommendations

The $150/mo increase doesn't change any recommendation thresholds:
- At 500 GB/day: VL/VT EBS is still cheapest ($2,679 vs $3,009 hybrid). The gap widens slightly from 7% to 12%.
- At PB/mo: the $150/mo is negligible vs $15K-100K/mo totals. Break-even retention shifts by less than 1 month.
- Loki+Tempo comparison: Hybrid is still 48% cheaper ($3,009 vs $5,763).

---

## Documentation — Getting Started Profiles Section

Add a new section to `docs/getting-started.md` after the Installation section. This is the primary place users discover profiles. Must explain:

1. What profiles are and why they exist
2. Quick reference table with one-line descriptions
3. When to use each profile
4. How to override settings on top of a profile
5. Per-signal and per-role profile examples
6. Cost impact comparison

### Content Structure

```markdown
## Configuration Profiles

Victoria Lakehouse ships with five configuration profiles — named presets that
tune 40+ settings for a specific operational goal. Profiles are base defaults:
any explicit flag or config file setting overrides the profile value.

### Quick Reference

| Profile | Description | WAL | Flush | Cache | Prefetch | Compaction |
|---|---|---|---|---|---|---|
| `balanced` (default) | Production general-purpose | On | 10s | 512MB mem, 50GB disk | On | Off |
| `max-performance` | Lowest latency, higher resources | Off | 5s | 2GB mem, 100GB disk | Aggressive | On |
| `max-durability` | Zero data loss priority | On | 10s | 512MB mem, 50GB disk | On | On |
| `max-cost-savings` | Minimize S3/compute/network cost | Off | 30s | 128MB mem, 10GB disk | Off | Off |
| `dev` | Local development, MinIO-friendly | Off | 1s | 64MB mem, 1GB disk | Off | Off |

### When to Use Each Profile

[Decision flowchart and per-profile explanation]

### Overriding Profile Settings

[Examples of flag/config overrides on top of profiles]

### Per-Signal and Per-Role Profiles

[Helm examples showing different profiles for logs vs traces, insert vs select]

### Cost Impact

[Table showing estimated monthly cost difference between profiles at reference scale]
```

---

## Testing

### Unit Tests

| Test | Description |
|---|---|
| `TestProfileConfig_AllProfiles` | Every valid profile name returns a non-nil Config that passes Validate() |
| `TestProfileConfig_InvalidProfile` | Unknown profile name fails Validate() |
| `TestProfileConfig_EmptyIsBalanced` | Empty string profile resolves to balanced defaults |
| `TestProfileConfig_SettingsMatch` | Spot-check key settings per profile match the spec matrix |
| `TestProfilePrecedence_FlagOverridesProfile` | Explicit flag beats profile value |
| `TestProfilePrecedence_ConfigFileOverridesProfile` | Config file value beats profile value |
| `TestProfilePrecedence_ProfileOverridesDefault` | Profile value beats built-in default |

### Integration Tests

| Test | Description |
|---|---|
| `TestProfileLoad_DevWithMinIO` | Load dev profile + MinIO S3 config, verify force_path_style=true |
| `TestProfileLoad_PerSignalOverride` | Load balanced global + max-durability for logs, verify logs gets WAL on and traces gets balanced WAL |
| `TestProfileLoad_ExplicitOverride` | Load max-performance + wal_enabled=true override, verify WAL is on despite profile saying off |

### Helm Tests

| Test | Description |
|---|---|
| `helm template` with `profile: dev` | Verify container args include `--lakehouse.profile=dev` |
| `helm template` with per-signal profiles | Verify logs and traces pods get different `--lakehouse.profile` args |
| `helm template` with explicit override on top of profile | Verify explicit value appears in args after profile flag |
