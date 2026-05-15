# Smart Cache & Cross-Signal Prefetch — Design Spec

## Goal

Add intelligent caching with TTL, cross-signal prefetch between separate lakehouse-logs and lakehouse-traces deployments, parallel query execution, hash-routed cache ownership, active query prioritization, and cache sizing metrics — all encapsulated inside the storage layer, invisible to VL/VT select handlers.

## Constraints

- ZERO changes to VL/VT handler code — all machinery behind Storage interface
- Must work across separate binaries/deployments (lakehouse-logs and lakehouse-traces)
- Must work with single binary (logs+traces co-located) equally well
- Disk I/O efficient — no per-entry metadata files, single snapshot persistence
- Cache state survives instance restarts
- All new config under `--lakehouse.*` namespace

---

## 1. Smart Cache Controller

### Package: `internal/smartcache/`

Wraps existing L1 (memory LRU), L2 (disk LRU), L3 (peer consistent hash) with unified intelligence. Existing cache backends stay dumb storage — the controller adds all decision logic.

### Entry Metadata

Each cached item tracks (in-memory only, persisted via snapshot):

| Field | Type | Purpose |
|-------|------|---------|
| `createdAt` | `time.Time` | When first cached |
| `lastAccess` | `time.Time` | Last read time |
| `accessCount` | `int` | Hits in current hot window |
| `accessWindowStart` | `time.Time` | Start of current hot tracking window |
| `pinnedBy` | `map[string]time.Time` | Query IDs → pin expiry (query end + grace) |
| `signal` | `string` | `"logs"` or `"traces"` |
| `traceIDs` | `[]string` | Extracted trace_ids (for cross-signal eviction) |

### Two-Knob Config

Replaces current multi-TTL config (`footer_ttl`, `bloom_ttl`, `page_ttl`) with unified:

```yaml
cache:
  disk_limit: "50GB"          # size limit per node
  max_age: "24h"              # TTL — data older than this gets evicted
  memory_limit: "512MB"       # L1 stays separate (always fast, hot working set)
  snapshot_interval: 60s      # metadata persistence frequency
  query_grace_period: 5m      # keep data pinned after query completes
  hot_access_threshold: 3     # accesses within hot_window to become "hot"
  hot_window: 10m             # sliding window for hot detection
```

### Eviction Priority (lowest priority evicted first)

1. **Expired entries** — `time.Since(createdAt) > max_age` AND not pinned AND not hot — always evict
2. **Unpinned cold entries** — below hot threshold, no active query — LRU order
3. **Unpinned hot entries** — above hot threshold — TTL resets on access, so `time.Since(lastAccess) > max_age`
4. **Pinned entries** — active query in-flight or within grace period — never evicted

### Hot Detection

An entry is "hot" if `accessCount >= hot_access_threshold` within the `hot_window` sliding window. Hot entries get their TTL clock reset on each access. When the window rolls over and access count drops below threshold, the entry reverts to cold and its original `createdAt`-based TTL applies.

### TTL Enforcement

Background goroutine runs every 30 seconds:
1. Iterate all L2 entries via in-memory metadata map (zero disk IOPS)
2. Check expiry based on entry state (cold: createdAt, hot: lastAccess, pinned: skip)
3. Evict expired entries
4. If `curSize > watermark * disk_limit` after TTL pass, fall back to LRU eviction

---

## 2. Hash-Routed Cache Ownership

Each select node owns a shard of the cache keyspace via the existing consistent hash ring (from peer cache). A node only stores data it owns in L2 disk cache. Data owned by peers is fetched via L3 and stored in L1 memory only.

### Routing in `getFileData`

```
getFileData(key):
  → L1 memory check (always local, small hot working set)
  → hash ring lookup: who owns this key?
    → owner == self:
      → L2 disk cache check
      → miss: S3 download → store in L2 disk + L1 memory
    → owner == peer:
      → L3 peer fetch from owner
      → store in L1 only (don't duplicate in L2)
```

### Benefits

- **Disk usage**: each node stores ~1/N of the total dataset (N = fleet size). 3 nodes × 50GB = 150GB effective cache, zero duplication
- **TTL**: only the owner manages TTL for its entries. No conflicting eviction decisions
- **Connected eviction**: cross-signal hints route to the owning node
- **Sizing**: `recommended_bytes` divides by fleet size automatically

### Ring Rebalancing

When a node joins or leaves:
- Hash ring updates via existing peer discovery
- Keys that moved to a new owner: old owner evicts lazily via normal TTL/LRU, new owner fetches from S3 on first access
- No bulk migration — data rebalances naturally through access patterns
- During transition: stale L2 hits on old owner still valid (data is correct, just suboptimal location)

### Single-Instance Behavior

With 1 node in the ring, all keys hash to self. Everything goes to local L2 disk cache, L3 peer fetch is never used. The system degrades gracefully to a simple local cache — no special-casing needed.

---

## 3. Query Parallelism

### Parallel File Processing

`RunQuery` currently processes files sequentially. New behavior:

```
files := manifest.GetFilesForRange(startNs, endNs)
→ dispatch to worker pool (bounded by query.file_workers)
→ each worker: getFileData → open parquet → scan row groups → writeBlock
→ workerID assigned per file worker for thread-safe writeBlock calls
```

### Concurrency Layers

| Control | Default | Scope | Purpose |
|---------|---------|-------|---------|
| `query.max_concurrent` | 32 | global | Query admission semaphore (existing, returns 429) |
| `query.file_workers` | 8 | per-query | Concurrent files processed per query |
| `s3.max_concurrent_downloads` | 16 | global | S3 download semaphore, shared across queries + prefetch |
| `prefetch.max_concurrent` | 8 | global | Prefetch worker pool (up from 4) |
| `prefetch.max_queue` | 128 | global | Prefetch task queue depth (up from 64) |

The S3 download semaphore prevents hammering S3 when multiple parallel queries each fan out to 8 files simultaneously.

### Config

```yaml
query:
  max_concurrent: 32
  file_workers: 8

s3:
  max_concurrent_downloads: 16

prefetch:
  max_concurrent: 8
  max_queue: 128
```

---

## 4. Cross-Signal Prefetch

### Discovery

Both methods, tried in priority order:

1. **Explicit config**: `cross_signal.endpoint` — direct URL to peer signal deployment
2. **Headless DNS**: `cross_signal.headless_service` — K8s service, auto-detect signal mode via `/lakehouse/info` endpoint

### Config

```yaml
cross_signal:
  enabled: true
  endpoint: ""                    # explicit URL (takes priority)
  headless_service: ""            # K8s DNS discovery fallback
  auth_key: ""                    # shared secret (X-Cross-Signal-Key header)
  timeout: 2s
  max_batch: 100                  # max trace_ids per hint request
  batch_interval: 500ms           # collect hints then send in batch
```

### Internal API

New endpoint on both binaries:

```
POST /internal/prefetch/hint
X-Cross-Signal-Key: <auth_key>

{
  "trace_ids": ["abc123", "def456"],
  "start_ns": 1715000000000000000,
  "end_ns":   1715003600000000000,
  "source_signal": "logs"
}
```

Receiving side re-hashes trace_ids against its own peer ring, routes each to the correct owner node within its fleet, and enqueues as `TypeCrossSignal` into the local prefetch engine. Avoids duplicate S3 downloads across replicas.

### Bidirectional Flow

Every query triggers prefetch on **both** sides:

**Log query:**
```
lakehouse-logs RunQuery
  → queryFile extracts trace_id values from result rows
  → batches into cross-signal hint buffer (per-query, in-memory)
  → buffer flushes async every 500ms or at max_batch (100)
  → for each trace_id: hash to owning select node via peer ring
  → self-owned trace_ids: enqueue TypeCorrelated locally (prefetch more log files with these trace_ids)
  → peer-owned trace_ids: POST /internal/prefetch/hint to owning peer
  → POST /internal/prefetch/hint to lakehouse-traces deployment (prefetch trace files)
    → traces deployment re-hashes against its own ring, routes to correct owner
```

**Trace query:**
```
lakehouse-traces RunQuery
  → queryFile extracts trace_id + service.name from results
  → sends hint to lakehouse-logs (prefetch logs for these trace_ids)
  → sends hint to self (prefetch more trace files nearby)
  → same hash-routing logic within each fleet
```

### Trace ID Extraction

During `readRowGroup`:
- Find `trace_id` column index via SchemaRegistry
- Collect unique non-empty values from rows that passed all filters
- Cap at 200 unique trace_ids per file (prevent hint explosion)
- Only extract from result rows, not filtered-out data

### Prefetch Task Types

| Type | Priority | Source |
|------|----------|--------|
| `TypeCrossSignal` | 1 | Cross-deployment hints (highest priority) |
| `TypeCorrelated` | 1 | Self-signal prefetch (same trace_ids, adjacent files) |
| `TypeReadAhead` | 2 | Speculative adjacent partition prefetch |
| `TypeWarmup` | 3 | Startup warmup |

### Read-Ahead Prefetch

When RunQuery processes files from partition hour=0 through hour=3, as files complete the engine speculatively prefetches hour=4 files from manifest (TypeReadAhead, lower priority than correlated).

---

## 5. Connected Data Eviction

When entries are evicted from cache, the controller extracts their trace_ids from metadata and sends eviction hints to the peer signal deployment.

### Eviction Hint API

```
POST /internal/cache/evict-hint
X-Cross-Signal-Key: <auth_key>

{
  "trace_ids": ["abc123", "def456"],
  "source_signal": "logs"
}
```

Receiving side looks up cache entries whose trace_ids overlap, and marks them for **priority eviction** (moved to back of LRU). Does NOT force-delete — if entries are pinned or hot, they survive.

### Delivery Semantics

Best-effort with tracking:
- Hints sent async, fire-and-forget
- If peer is unreachable: retry with backoff for up to 5 minutes, then drop
- Metrics track pending/delivered/applied

### Metrics

| Metric | Type | Purpose |
|--------|------|---------|
| `lakehouse_cache_cross_eviction_sent` | Counter | Hints sent to peer signal |
| `lakehouse_cache_cross_eviction_received` | Counter | Hints received from peer |
| `lakehouse_cache_cross_eviction_pending` | Gauge | Hints awaiting delivery |
| `lakehouse_cache_cross_eviction_applied` | Counter | Entries deprioritized |

---

## 6. Metadata Persistence

### In-Memory Map + Periodic Snapshot

All entry metadata lives in memory during runtime. Zero disk IOPS for access tracking or eviction scans.

**Snapshot file:** `{cache_dir}/smartcache.meta.json`
- Written atomically every `snapshot_interval` (default 60s)
- Written on graceful shutdown
- Single file regardless of cache entry count

**On startup:**
1. Load `smartcache.meta.json`
2. Scan actual cache directory files
3. Reconcile: metadata with no matching file → drop. Files with no metadata → fresh metadata using file mtime as createdAt
4. Rebuild in-memory LRU order sorted by lastAccess

**Crash safety:** snapshot is at most 60s stale. Worst case: entries accessed in the last 60s lose their latest access count bump. Re-warm naturally.

### IOPS Comparison vs Sidecar Files

| Operation | Sidecar approach | Snapshot approach |
|-----------|-----------------|-------------------|
| Cache put | 2 writes (data + meta) | 1 write (data only) |
| Cache get | 1 read + 0 meta | 1 read (data only) |
| Eviction scan | N stat() calls | 0 (in-memory) |
| Background persist | 0 | 1 write per minute |
| Startup | N reads | 1 read + N stat() |

Net: identical IOPS to current implementation + 1 write/minute.

---

## 7. Cache Sizing Calculator

### Two Estimation Methods

**Ingestion-based** (available immediately on select nodes):
- Select nodes compute from manifest: `sum(file sizes added in last N hours) / N`
- No insert pod communication needed — purely local manifest data
- Conservative upper bound (assumes all ingested data gets queried)

**Query-based** (needs warmup):
- Tracks unique bytes read by queries in a rolling 24h window
- Deduplicates by file_key (same file queried 10 times = counted once)
- More accurate — reflects actual access patterns

**Gradual blend:**
- Hour 0-1: 100% ingestion-based
- Hour 1-12: weighted blend → `weight = min(1.0, query_hours / 12)`
- Hour 12+: 100% query-based
- Formula: `estimate = (1 - weight) * ingestion_estimate + weight * query_estimate`

**Fallback chain for ingestion rate (select-only nodes):**
1. Manifest-based (always available, zero network cost)
2. Insert pod API: `GET /internal/stats/ingestion-rate` → `{"bytes_per_hour": N}`
3. Explicit config: `cache.ingestion_rate_hint: "2GB/h"`

### Auto-Sizing

```yaml
cache:
  disk_limit: "auto"          # auto-size from blended estimate
  disk_limit_max: "100GB"     # hard cap for auto mode
  target_hours: 24            # target coverage window
  ingestion_rate_hint: ""     # manual fallback (e.g., "2GB/h")
```

When `disk_limit=auto`: uses blended estimate, capped at `disk_limit_max`. Re-evaluates every 5 minutes. If the estimate shrinks below current usage, the cache does NOT actively delete data — it simply stops accepting new entries until usage drops below the new limit via normal TTL/LRU eviction. This prevents thrashing where the cache repeatedly fills and empties.

### Metrics

| Metric | Type | Purpose |
|--------|------|---------|
| `lakehouse_cache_hit_ratio` | Gauge | L2 hit/(hit+miss) over 5min window |
| `lakehouse_cache_entries_total` | Gauge | Current entry count |
| `lakehouse_cache_bytes_used` | Gauge | Current disk usage |
| `lakehouse_cache_bytes_limit` | Gauge | Configured or auto-calculated limit |
| `lakehouse_cache_evictions_total{reason}` | Counter | By reason: `ttl`, `size`, `cross_signal`, `manual` |
| `lakehouse_cache_hot_entries` | Gauge | Entries above hot threshold |
| `lakehouse_cache_pinned_entries` | Gauge | Entries pinned by active queries |
| `lakehouse_cache_recommended_bytes{method}` | Gauge | `ingestion` and `query` estimates |
| `lakehouse_cache_coverage_hours` | Gauge | Estimated hours of query data currently cached |
| `lakehouse_cache_prefetch_hit_ratio` | Gauge | How often prefetched data was actually used |
| `lakehouse_cache_owned_entries` | Gauge | Entries this node owns in hash ring |
| `lakehouse_cache_owned_bytes` | Gauge | Disk usage for owned entries |
| `lakehouse_cache_peer_served_total` | Counter | Times this node served data to peers |
| `lakehouse_cache_effective_bytes` | Gauge | Fleet-wide estimated total (owned_bytes * fleet_size) |

---

## 8. Storage Layer Encapsulation

All machinery is inside ParquetS3Storage, invisible to VL/VT:

```
VL/VT select handlers (UNCHANGED, no awareness)
  │
  ▼
Storage interface: RunQuery, GetFieldNames, GetFieldValues, ...
  │
  ▼
ParquetS3Storage (our code)
  ├── SmartCacheController    ← NEW, wraps L1/L2/L3
  │     ├── hash ring routing (own vs peer)
  │     ├── TTL + hot detection + pin tracking
  │     ├── metadata snapshot persistence
  │     └── cache sizing calculator
  ├── CrossSignalClient       ← NEW, sends/receives hints
  │     ├── prefetch hints (POST /internal/prefetch/hint)
  │     ├── eviction hints (POST /internal/cache/evict-hint)
  │     └── discovery (explicit config + headless DNS)
  ├── PrefetchEngine          ← EXISTING, now wired into query path
  │     ├── TypeCrossSignal (from peer deployment)
  │     ├── TypeCorrelated (self-signal, same trace_ids)
  │     ├── TypeReadAhead (adjacent partitions)
  │     └── TypeWarmup (startup)
  ├── S3 ClientPool           ← EXISTING, + global download semaphore
  ├── Manifest                ← EXISTING, unchanged
  └── SchemaRegistry          ← EXISTING, unchanged
```

### What Changes

| Component | Change |
|-----------|--------|
| `ParquetS3Storage.getFileData` | Routes through SmartCacheController instead of calling L1/L2/L3 directly |
| `ParquetS3Storage.RunQuery` | Parallel file workers, pin/unpin via controller, trace_id extraction |
| `ParquetS3Storage.queryFile` | Extracts trace_ids, feeds to prefetch engine |
| `internal/config/config.go` | New config structs for cache, cross_signal, query.file_workers, s3.max_concurrent_downloads |
| `internal/metrics/lakehouse.go` | ~15 new metrics |
| `cmd/lakehouse-logs/main.go` | Wire SmartCacheController + CrossSignalClient |
| `lakehouse-traces/main.go` | Same wiring |

### What Does NOT Change

- Any VL/VT handler code
- Storage interface method signatures
- vlstorage adapter layer
- HTTP handler registration
- Any external API surface (`/select/*`, `/insert/*`, `/delete/*`)
- Helm chart structure (new config flags only)

---

## 9. File Structure

| File | Responsibility |
|------|---------------|
| `internal/smartcache/controller.go` | SmartCacheController: wraps L1/L2/L3, hash routing, pin/unpin, eviction |
| `internal/smartcache/controller_test.go` | Controller unit tests |
| `internal/smartcache/metadata.go` | EntryMeta struct, in-memory map, snapshot persistence |
| `internal/smartcache/metadata_test.go` | Metadata + snapshot tests |
| `internal/smartcache/sizing.go` | Cache sizing calculator (ingestion + query blend) |
| `internal/smartcache/sizing_test.go` | Sizing calculation tests |
| `internal/smartcache/eviction.go` | TTL enforcement goroutine, hot detection, eviction priority |
| `internal/smartcache/eviction_test.go` | Eviction logic tests |
| `internal/crosssignal/client.go` | CrossSignalClient: discovery, hint batching, HTTP send |
| `internal/crosssignal/client_test.go` | Client tests |
| `internal/crosssignal/handler.go` | HTTP handlers: /internal/prefetch/hint, /internal/cache/evict-hint |
| `internal/crosssignal/handler_test.go` | Handler tests |
| `internal/config/config.go` | Add SmartCacheConfig, CrossSignalConfig, update defaults |
| `internal/metrics/lakehouse.go` | Add ~15 new metrics |
| `internal/storage/parquets3/storage.go` | Replace getFileData to use SmartCacheController |
| `internal/storage/parquets3/storage_query.go` | Parallel file workers, trace_id extraction, prefetch wiring |
| `internal/prefetch/prefetch.go` | Add TypeCrossSignal, S3 semaphore integration |

---

## 10. Complete Config Reference

```yaml
cache:
  memory_limit: "512MB"           # L1 memory cache size
  disk_limit: "50GB"              # L2 disk cache size per node ("auto" for auto-sizing)
  disk_limit_max: "100GB"         # hard cap when disk_limit=auto
  disk_path: "/data/lakehouse/cache"
  max_age: "24h"                  # unified TTL for all cached data
  snapshot_interval: 60s          # metadata persistence frequency
  query_grace_period: 5m          # keep data pinned after query completes
  hot_access_threshold: 3         # accesses within hot_window to become "hot"
  hot_window: 10m                 # sliding window for hot detection
  target_hours: 24                # target coverage for sizing calculation
  ingestion_rate_hint: ""         # manual fallback for select-only nodes (e.g., "2GB/h")

cross_signal:
  enabled: true
  endpoint: ""                    # explicit URL to peer signal deployment
  headless_service: ""            # K8s headless service for DNS discovery
  auth_key: ""                    # shared secret (X-Cross-Signal-Key header)
  timeout: 2s
  max_batch: 100                  # max trace_ids per hint request
  batch_interval: 500ms           # hint batching interval

query:
  max_concurrent: 32              # existing — query admission semaphore
  file_workers: 8                 # concurrent files per query
  timeout: 60s                    # existing
  slow_threshold: 5s              # existing

s3:
  max_concurrent_downloads: 16    # global S3 download semaphore
  max_connections: 128            # existing
  retry_max: 3                    # existing
  retry_base_delay: 200ms         # existing

prefetch:
  correlated: true                # existing
  read_ahead_depth: 2             # existing
  max_concurrent: 8               # up from 4
  max_queue: 128                  # up from 64
```
