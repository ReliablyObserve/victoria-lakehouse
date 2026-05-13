# Tenant Storage Statistics, Detailed Metrics & Lakehouse Explorer

## Goal

Add per-tenant storage statistics, detailed storage metrics with S3 storage class awareness, cost estimation, and a Lakehouse Explorer UI tab integrated into VMUI — giving operators full visibility into tenant usage, storage costs, field cardinality, and system health across the entire lakehouse fleet.

## Architecture

Three layers: a distributed TenantRegistry (CRDT-style peer-synced, S3-durable), JSON API endpoints consumed by both Prometheus metrics and the Explorer UI, and a standalone UI page injected as a tab into the VMUI we already serve.

**Tech Stack:** Go (backend), Preact + uPlot (UI, no build step), ZSTD (sync compression), S3 (snapshot persistence)

---

## Requirements

| # | Requirement | Acceptance Criteria |
|---|---|---|
| R1 | Per-tenant storage stats (files, bytes, rows, time range, compression, cost) | TenantRegistry tracks all tenants with live stats |
| R2 | Fleet-consistent registry across all select/insert nodes | Peer broadcast + CRDT merge, no flipping between nodes |
| R3 | Cold tenant discovery | S3 prefix scan discovers tenants with no recent activity |
| R4 | S3 storage class awareness | Lifecycle prediction + write-time tagging + HeadObject sampling |
| R5 | Cost estimation by storage class and tenant | Configurable per-class pricing, lifecycle transition projections |
| R6 | Temporal stats (ingestion, compression, cost over time) | Hour/day/month granularity derived from partition-level manifest data |
| R7 | Per-tenant Prometheus metrics with cardinality cap | Configurable `metrics_cardinality_limit`, overflow tracked |
| R8 | Global storage Prometheus metrics (class, cost, compression) | Always emitted, no tenant label |
| R9 | Field/label cardinality explorer API | Per-field unique value counts, type, bloom status |
| R10 | Lakehouse Explorer UI (3 tabs) | Standalone at `/lakehouse/ui/`, injected into VMUI |
| R11 | VMUI tab injection (zero VL/VT modifications) | Wrap VL's VMUI handler, inject script on HTML responses |
| R12 | Honor all VL/VT VMUI flags | Pass through `-search.*` flags unchanged |
| R13 | Peer sync with compression and batching | ZSTD-compressed delta broadcasts, configurable interval |
| R14 | S3 snapshot persistence for registry | Survives full fleet restart |
| R15 | Both Go modules (root + lakehouse-traces) | Changes mirrored to both modules |

---

## Component 1: TenantRegistry

**File:** `internal/stats/registry.go`

### TenantStats

```go
type TenantStats struct {
    AccountID       string
    ProjectID       string
    Isolation       string             // "prefix" or "bucket"
    Bucket          string             // S3 bucket (differs per tenant in bucket isolation)
    Prefix          string             // S3 key prefix (e.g., "100/1/logs/")
    TotalFiles      int64
    TotalBytes      int64
    RawBytes        int64
    TotalRows       int64
    Partitions      int
    MinTimeNs       int64
    MaxTimeNs       int64
    LastWriteAt     time.Time
    LastQueryAt     time.Time
    Labels          map[string]int     // field name → unique value count
    BytesByClass    map[string]int64   // storage class → bytes
    FilesByClass    map[string]int64   // storage class → file count
    NodeContribs    map[string]int64   // nodeID → bytes contributed (for CRDT sum)
}
```

### TenantRegistry

```go
type TenantRegistry struct {
    mu          sync.RWMutex
    tenants     map[string]*TenantStats  // key: "{AccountID}/{ProjectID}"
    nodeID      string                   // this node's unique ID
    generation  uint64                   // incremented on each local change
    lastPushGen uint64                   // generation at last peer push
    lifecycle   []LifecycleRule          // S3 lifecycle rules from config
    pricing     map[string]float64       // storage class → $/GB/month
}
```

### Tenant Isolation Modes

The registry supports both tenant isolation modes configured via `--lakehouse.tenant.isolation`:

**Prefix isolation** (default): All tenants share one S3 bucket, separated by key prefix (`{AccountID}/{ProjectID}/`). Discovery scans the shared bucket root with delimiter `/` to find all tenant prefixes. S3 snapshots and lifecycle rules apply to the single bucket.

**Bucket isolation**: Each tenant gets a dedicated S3 bucket (via `--lakehouse.tenant.bucket-template`, e.g., `obs-{AccountID}-{ProjectID}`). Discovery requires a configured tenant list or S3 API calls per known bucket. Each bucket may have different lifecycle rules and IAM policies.

**Differences by mode:**

| Aspect | Prefix Isolation | Bucket Isolation |
|---|---|---|
| Discovery | ListObjectsV2 with `/` delimiter on shared bucket | Iterate configured tenant list, HeadBucket per tenant |
| S3 snapshots | `s3://{bucket}/_meta/tenant-stats/` (shared) | `s3://{tenant-bucket}/_meta/tenant-stats/` (per-bucket) |
| Lifecycle rules | One set for shared bucket | Per-tenant rules (may differ per bucket) |
| IAM | Shared bucket policy | Per-bucket IAM policy (full isolation) |
| Cost calculation | Single pricing config | Per-tenant pricing possible (different regions/classes) |
| Manifest refresh | One ListObjects call covers all tenants | One ListObjects per tenant bucket |

**Bucket isolation tenant list** — since we can't discover buckets by scanning, tenants must be declared:
```yaml
lakehouse:
  tenant:
    isolation: bucket
    bucket_template: "obs-{AccountID}-{ProjectID}"
    known_tenants:              # required for bucket isolation discovery
      - account_id: "100"
        project_id: "1"
      - account_id: "200"
        project_id: "5"
```

Alternatively, if the lakehouse serves requests from multiple tenants (via header routing), the registry auto-discovers tenants from incoming traffic — no static list needed for active tenants. The `known_tenants` list is only needed to discover cold/dormant tenants in bucket isolation mode.

### Feed Points

**Write path** — on every `manifest.AddFile()`:
- Extract tenant from S3 key prefix (prefix mode) or from configured bucket mapping (bucket mode)
- Record `Isolation`, `Bucket`, `Prefix` on TenantStats
- Update TenantStats: increment files, bytes, rawBytes, rows
- Set storage class to STANDARD, record CreatedAt
- Update labels from FileInfo.Labels
- Increment generation

**Query path** — on every `RunQuery()`:
- Extract tenant from prefix or bucket context
- Update LastQueryAt
- Increment queries counter

**Manifest refresh** — on periodic S3 scan:
- **Prefix mode**: scan shared bucket, group files by tenant prefix, recalculate per-tenant aggregates
- **Bucket mode**: for each known tenant, scan their bucket, recalculate aggregates
- Recalculate predicted storage classes based on file age + lifecycle rules (per-tenant rules in bucket mode)
- Discover cold tenants: prefix mode via prefix scan, bucket mode via `known_tenants` + HeadBucket

### CRDT Merge Strategy

```go
func (r *TenantRegistry) Merge(remote *TenantDelta) {
    // Counters: per-node tracking, sum across nodes
    // files, bytes, rows: local.NodeContribs[remoteNodeID] = remote value
    //   total = sum(NodeContribs)
    
    // Timestamps: extrema wins
    // MinTimeNs: min(local, remote)
    // MaxTimeNs: max(local, remote)
    // LastWriteAt: max(local, remote)
    // LastQueryAt: max(local, remote)
    
    // Labels: max cardinality wins per field
    // Labels[field] = max(local[field], remote[field])
    
    // Storage class: sum per class across nodes
    // BytesByClass[class]: per-node tracking like counters
}
```

No vector clocks needed — monotonic counters + extrema are naturally convergent.

---

## Component 2: Peer Sync

**File:** `internal/stats/sync.go`

### TenantDelta

```go
type TenantDelta struct {
    NodeID     string                  // source node
    Generation uint64                  // source generation
    Tenants    map[string]*TenantStats // only changed tenants
    Timestamp  time.Time
}
```

### Sync Protocol

**Push path** (periodic, configurable interval):
1. Collect tenants changed since `lastPushGen` (delta)
2. Marshal to JSON, compress with ZSTD
3. POST to each peer: `POST /internal/stats/sync`
4. Header: `Authorization: Bearer {peer.auth_key}`, `Content-Encoding: zstd`
5. On success: update `lastPushGen = generation`

**Receive path:**
1. Decompress ZSTD body
2. Unmarshal TenantDelta
3. Call `registry.Merge(delta)`

**Full sync:** After `max_delta_count` pushes (default 1000), or on startup, send full registry instead of delta. Peers detect via `Generation` field.

**S3 snapshots:**
- **Prefix isolation**: `s3://{shared-bucket}/_meta/tenant-stats/{nodeID}.json.zst`
  - Single snapshot file covers all tenants (they share the bucket)
  - On startup: load all node snapshots, merge into registry
- **Bucket isolation**: `s3://{primary-bucket}/_meta/tenant-stats/{nodeID}.json.zst`
  - Primary bucket is the first tenant's bucket or a dedicated meta bucket (`--lakehouse.stats.meta-bucket`)
  - Contains stats for ALL tenants (cross-bucket aggregation) — avoids needing read access to every tenant bucket just for stats
  - On startup: load from primary bucket, merge into registry
- Written every `snapshot_interval` (default 5m), ZSTD compressed
- Startup then triggers peer sync to fill any gap since last snapshot

### Config

```yaml
lakehouse:
  stats:
    enabled: true                    # master switch
    push_interval: 30s              # delta broadcast interval
    push_compression: true          # ZSTD compress deltas
    snapshot_interval: 5m           # S3 snapshot interval
    max_delta_count: 1000           # force full sync after N deltas
    snapshot_prefix: "_meta/tenant-stats"  # S3 prefix for snapshots
```

---

## Component 3: Storage Class Tracker

**File:** `internal/stats/storageclass.go`

### Detection Layers (cheapest first)

1. **Write-time tagging** — files we write are STANDARD. FileInfo gets `StorageClass: "STANDARD"` and `ClassSource: "write"`.

2. **Lifecycle prediction** — on manifest refresh, for each file:
   ```
   age = now - file.CreatedAt (or inferred from partition date)
   for each rule in lifecycle_rules (sorted by transition_days desc):
       if age >= rule.transition_days:
           file.StorageClass = rule.storage_class
           file.ClassSource = "lifecycle"
           break
   ```
   Zero S3 API cost.

3. **HeadObject sampling** — for files within ±2 days of a lifecycle transition boundary, verify actual class via HeadObject. Cache result in FileInfo with `ClassCheckedAt` timestamp. Cost: $0.0000004 per check.

4. **S3 Inventory** (optional) — if `s3_inventory_bucket` configured, periodically import CSV manifest for exact storage class of every object. Zero per-object API cost.

### FileInfo Extension

```go
type FileInfo struct {
    // ...existing fields...
    StorageClass   string    `json:"storage_class,omitempty"`    // STANDARD, STANDARD_IA, GLACIER, DEEP_ARCHIVE
    ClassCheckedAt time.Time `json:"class_checked_at,omitempty"` // last HeadObject verification
    ClassSource    string    `json:"class_source,omitempty"`     // write, lifecycle, headobject, inventory
    CreatedAt      time.Time `json:"created_at,omitempty"`       // for lifecycle age calculation
}
```

### Lifecycle Config

**Default rules** apply to all tenants (prefix isolation) or as fallback (bucket isolation):

```yaml
lakehouse:
  stats:
    s3_lifecycle_rules:                 # default rules (shared bucket or fallback)
      - transition_days: 30
        storage_class: STANDARD_IA
      - transition_days: 90
        storage_class: GLACIER
      - transition_days: 365
        storage_class: DEEP_ARCHIVE
    s3_price_per_gb:                    # default pricing
      STANDARD: 0.023
      STANDARD_IA: 0.0125
      GLACIER_IR: 0.004
      GLACIER: 0.0036
      DEEP_ARCHIVE: 0.00099
    s3_inventory_bucket: ""             # optional inventory source
    headobject_sample_interval: 6h      # how often to spot-check near boundaries
    headobject_max_per_refresh: 50      # cap HeadObject calls per refresh cycle
```

**Per-tenant overrides** (bucket isolation only — each bucket may have different lifecycle policies):

```yaml
lakehouse:
  tenant:
    isolation: bucket
    known_tenants:
      - account_id: "100"
        project_id: "1"
        lifecycle_rules:                # override for this tenant's bucket
          - transition_days: 14
            storage_class: STANDARD_IA
          - transition_days: 60
            storage_class: GLACIER
        price_per_gb:                   # override if different region/class
          STANDARD: 0.025               # e.g., eu-west-1 pricing
      - account_id: "200"
        project_id: "5"
        # no overrides — uses default rules
```

The StorageClassTracker resolves rules per-tenant: check tenant-specific rules first, fall back to defaults.

### Compaction Integration

When compactor replaces files:
- New merged file: `StorageClass: STANDARD`, `CreatedAt: now`
- Old source files: removed from registry
- Registry recalculates tenant totals

---

## Component 4: JSON API

**File:** `internal/stats/api.go`

All endpoints under `/lakehouse/api/v1/`. Responses are `application/json`.

### GET /lakehouse/api/v1/tenants

Tenant summary list.

```json
{
  "tenants": [
    {
      "account_id": "100",
      "project_id": "1",
      "isolation": "prefix",
      "bucket": "obs-archive",
      "total_files": 1247,
      "total_bytes": 52428800000,
      "raw_bytes": 104857600000,
      "compression_ratio": 2.0,
      "total_rows": 5000000,
      "partitions": 48,
      "min_time": "2026-04-01T00:00:00Z",
      "max_time": "2026-05-13T11:00:00Z",
      "last_write_at": "2026-05-13T10:55:00Z",
      "last_query_at": "2026-05-13T11:02:00Z",
      "storage_by_class": {
        "STANDARD": 40000000000,
        "STANDARD_IA": 12428800000
      },
      "monthly_cost_usd": 1.07,
      "top_labels": {"service.name": 12, "k8s.namespace.name": 4}
    }
  ],
  "total_tenants": 15,
  "total_bytes": 280000000000,
  "total_files": 18500
}
```

Query params: `?sort=bytes|files|cost|rows` (default: bytes desc)

### GET /lakehouse/api/v1/tenants/{accountID}/{projectID}

Tenant drill-down with partition breakdown.

```json
{
  "account_id": "100",
  "project_id": "1",
  "summary": { "...same as list entry..." },
  "partitions": [
    {"date": "2026-05-13", "hours": [0,1,2,10,11], "files": 25, "bytes": 1048576000},
    {"date": "2026-05-12", "hours": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23], "files": 120, "bytes": 5242880000}
  ],
  "file_size_histogram": {
    "buckets": ["<1MB", "1-10MB", "10-50MB", "50-128MB", ">128MB"],
    "counts": [5, 120, 800, 300, 22]
  },
  "storage_by_class": [
    {"class": "STANDARD", "bytes": 40000000000, "files": 800, "monthly_usd": 0.92},
    {"class": "STANDARD_IA", "bytes": 12428800000, "files": 447, "monthly_usd": 0.15}
  ],
  "avg_compression_ratio": 2.1,
  "avg_rows_per_file": 4010
}
```

### GET /lakehouse/api/v1/stats/overview

Global storage summary.

```json
{
  "bucket": "obs-archive",
  "mode": "logs",
  "total_files": 18500,
  "total_bytes": 280000000000,
  "total_raw_bytes": 560000000000,
  "avg_compression_ratio": 2.0,
  "total_rows": 75000000,
  "partition_count": 720,
  "oldest_data": "2026-01-15T00:00:00Z",
  "newest_data": "2026-05-13T11:00:00Z",
  "ingestion_rate_bytes_sec": 125000,
  "file_size_p50": 45000000,
  "file_size_p99": 128000000,
  "tenant_count": 15,
  "tenant_isolation": "prefix",
  "storage_by_class": [
    {"class": "STANDARD", "bytes": 180000000000, "files": 12000},
    {"class": "STANDARD_IA", "bytes": 80000000000, "files": 5000},
    {"class": "GLACIER", "bytes": 20000000000, "files": 1500}
  ],
  "fleet_nodes": 3,
  "registry_generation": 45821
}
```

### GET /lakehouse/api/v1/stats/ingestion

Temporal ingestion stats.

Query params: `?period=hour|day|month`, `&range=24h|7d|30d|90d|1y`, `&tenant=100/1` (optional)

```json
{
  "period": "day",
  "buckets": [
    {
      "timestamp": "2026-05-13",
      "rows": 2500000,
      "bytes": 12000000000,
      "raw_bytes": 24000000000,
      "compression_ratio": 2.0,
      "files_created": 120,
      "avg_file_size": 100000000
    }
  ],
  "totals": {
    "rows": 75000000,
    "bytes": 280000000000,
    "cost_estimate_usd": 5.21
  }
}
```

Source: partition-level aggregation from manifest. Partitions keyed by `dt=YYYY-MM-DD/hour=HH` provide natural hour/day/month bucketing.

### GET /lakehouse/api/v1/stats/cost

Cost breakdown with projections.

Query params: `?range=30d`, `&tenant=100/1` (optional)

```json
{
  "range": "30d",
  "storage_by_class": [
    {"class": "STANDARD", "bytes": 180000000000, "files": 12000, "monthly_usd": 4.14},
    {"class": "STANDARD_IA", "bytes": 80000000000, "files": 5000, "monthly_usd": 1.00},
    {"class": "GLACIER", "bytes": 20000000000, "files": 1500, "monthly_usd": 0.07}
  ],
  "total_storage_usd": 5.21,
  "requests": {
    "puts": 3600,
    "gets": 125000,
    "lists": 8640,
    "put_cost_usd": 0.018,
    "get_cost_usd": 0.050,
    "list_cost_usd": 0.043
  },
  "total_monthly_usd": 5.32,
  "vs_all_standard_usd": 6.44,
  "lifecycle_savings_usd": 1.12,
  "lifecycle_savings_pct": 17.4,
  "per_tenant": [
    {
      "tenant": "100/1",
      "by_class": [
        {"class": "STANDARD", "bytes": 120000000000, "monthly_usd": 2.76},
        {"class": "STANDARD_IA", "bytes": 60000000000, "monthly_usd": 0.75}
      ],
      "total_usd": 3.51,
      "pct": 66.0
    }
  ],
  "projections": {
    "growth_rate_bytes_per_day": 12000000000,
    "projected_30d_usd": 7.10,
    "projected_90d_usd": 12.80,
    "note": "Projections account for lifecycle transitions"
  }
}
```

Growth projections: linear regression over the last N days of ingestion, adjusted for lifecycle transitions (data moving to cheaper tiers over time).

### GET /lakehouse/api/v1/stats/compression

Compression trends.

Query params: `?period=day|month`, `&range=30d`

```json
{
  "period": "day",
  "buckets": [
    {
      "timestamp": "2026-05-13",
      "avg_ratio": 2.1,
      "p50_ratio": 2.0,
      "p99_ratio": 3.5,
      "best_tenant": {"tenant": "100/1", "ratio": 2.8},
      "worst_tenant": {"tenant": "200/5", "ratio": 1.3}
    }
  ],
  "global_avg_ratio": 2.0
}
```

### GET /lakehouse/api/v1/cardinality/fields

Field cardinality explorer.

Query params: `?tenant=100/1` (optional — omit for global), `&sort=cardinality|name`, `&limit=100`

```json
{
  "fields": [
    {"name": "custom.user_id", "cardinality": 50231, "type": "map", "has_bloom": false, "column": "log.attributes"},
    {"name": "k8s.pod.name", "cardinality": 847, "type": "promoted", "has_bloom": false},
    {"name": "service.name", "cardinality": 12, "type": "promoted", "has_bloom": true}
  ],
  "total_fields": 42,
  "total_promoted": 17,
  "total_map": 25,
  "high_cardinality_warning": ["custom.user_id", "request.id"],
  "cardinality_threshold": 10000
}
```

Source: LabelIndex already tracks per-field values. Extend to track per-tenant cardinality.

---

## Component 5: Prometheus Metrics

**File:** `internal/metrics/lakehouse.go` (extend existing)

### Per-Tenant Metrics (guarded by cardinality cap)

```
lakehouse_tenant_files{tenant="100/1"}                  Gauge
lakehouse_tenant_bytes{tenant="100/1"}                  Gauge
lakehouse_tenant_raw_bytes{tenant="100/1"}              Gauge
lakehouse_tenant_rows_total{tenant="100/1"}             Counter
lakehouse_tenant_ingestion_bytes_total{tenant="100/1"}  Counter
lakehouse_tenant_queries_total{tenant="100/1"}          Counter
lakehouse_tenant_last_write_timestamp{tenant="100/1"}   Gauge (unix seconds)
lakehouse_tenant_last_query_timestamp{tenant="100/1"}   Gauge (unix seconds)
```

### Global Storage Metrics (always emitted)

```
lakehouse_storage_files_total                           Gauge
lakehouse_storage_bytes_total                           Gauge
lakehouse_storage_raw_bytes_total                       Gauge
lakehouse_storage_compression_ratio                     Gauge (float)
lakehouse_storage_rows_total                            Gauge
lakehouse_storage_partitions_total                      Gauge
lakehouse_storage_oldest_data_seconds                   Gauge (unix timestamp)
lakehouse_storage_newest_data_seconds                   Gauge (unix timestamp)
lakehouse_storage_tenants_total                         Gauge
lakehouse_storage_bytes_by_class{class="STANDARD"}      Gauge
lakehouse_storage_files_by_class{class="STANDARD"}      Gauge
lakehouse_storage_cost_monthly_usd                      Gauge (float)
lakehouse_storage_cost_by_class_usd{class="STANDARD"}   Gauge (float)
lakehouse_storage_ingestion_rate_bytes                   Gauge (bytes/sec rolling avg)
```

### Cardinality Limiter

**File:** `internal/stats/cardinality_limiter.go`

```go
type CardinalityLimiter struct {
    mu         sync.RWMutex
    maxTenants int              // configurable cap
    tracked    map[string]bool  // tenants with active Prometheus series
    overflow   atomic.Int64     // tenants dropped due to cap
}

func (cl *CardinalityLimiter) Allow(tenant string) bool
// Returns true if tenant already tracked OR under cap.
// Returns false if cap reached and tenant is new.

func (cl *CardinalityLimiter) TrackedCount() int
func (cl *CardinalityLimiter) OverflowCount() int64
```

Meta-metrics:
```
lakehouse_metrics_cardinality_limit                     Gauge (configured cap)
lakehouse_metrics_cardinality_tracked                   Gauge (current unique tenants)
lakehouse_metrics_cardinality_overflow_total             Counter (tenants API-only)
```

### Config

```yaml
lakehouse:
  stats:
    metrics_cardinality_limit: 100  # max unique tenant label values (0 = disable per-tenant metrics)
```

Default: `100`. Overflow tenants still fully visible via JSON API — only Prometheus labels are capped.

---

## Component 6: Lakehouse Explorer UI

**Files:** `internal/ui/ui.go` (handler), `internal/ui/static/index.html` (single-file app)

### Tech Stack

- **Preact** (3KB) — lightweight React-compatible, no build step
- **uPlot** (35KB) — same charting library VMUI uses
- **HTM** (1KB) — JSX-like syntax without build step
- All loaded from CDN with SRI hashes, or bundled inline for air-gapped deployments

### Served At

`/lakehouse/ui/` — always available when `lakehouse.ui.enabled: true` (default).

### Three Tabs

**Tab 1: Storage Overview**
- Big number cards: total files, bytes, rows, compression ratio, monthly cost, tenant count
- Donut chart: storage cost by S3 class
- Line chart: ingestion rate over time (period selector: hour/day/month)
- Line chart: compression ratio trend
- Histogram: file size distribution
- Forecast cards: 30d/90d cost projections
- Data source: `/lakehouse/api/v1/stats/overview`, `/stats/ingestion`, `/stats/compression`, `/stats/cost`

**Tab 2: Tenants**
- Sortable table: tenant, files, bytes, rows, compression, last write, last query, monthly cost
- Filter by account/project
- Storage share pie chart (top 10 + "other")
- Click tenant → drill-down panel:
  - Partition heatmap (date × hour grid, color intensity = bytes)
  - File size histogram
  - Storage class breakdown
  - Top labels by cardinality
- Data source: `/lakehouse/api/v1/tenants`, `/tenants/{account}/{project}`

**Tab 3: Cardinality Explorer** (VM cardinality explorer style)
- Field table: name, cardinality, type (promoted/map), bloom status, source MAP column
- Sortable by cardinality (highest first)
- Per-tenant filter dropdown
- High-cardinality warnings (red badge for fields above threshold)
- Click field → expand: top 20 sample values by frequency
- Bar chart: top 20 fields by cardinality
- Data source: `/lakehouse/api/v1/cardinality/fields`

### Auto-Refresh

Toggle in header bar. Options: off (default), 10s, 30s, 60s. Uses `setInterval` + fetch, cancels on tab switch.

### Theme

Matches VMUI: dark/light toggle. Uses same CSS variable patterns.

### VMUI Tab Injection

**File:** `internal/ui/vmui_inject.go`

Since our binary already serves VMUI (imported via `app/vlselect`), we wrap VL's VMUI handler:

```go
func InjectLakehouseTab(upstream http.Handler) http.Handler
```

Middleware behavior:
1. Intercepts requests to `/vmui/*`
2. Calls VL's original VMUI handler
3. On HTML responses (Content-Type: `text/html`):
   - Buffers response body
   - Inserts `<script src="/lakehouse/ui/vmui-tab.js"></script>` before `</body>`
   - Adjusts Content-Length
4. Non-HTML responses (JS, CSS, images, fonts) pass through untouched

**vmui-tab.js** (~50 lines):
- Waits for VMUI nav to render (MutationObserver)
- Adds "Lakehouse" nav item matching VMUI's existing style
- On click: replaces main content area with `<iframe src="/lakehouse/ui/">` (full height)
- Preserves VMUI's existing navigation and state
- Detects VMUI framework (React router) and integrates without breaking SPA navigation

All VL/VT VMUI flags (`-search.maxQueryDuration`, `-search.maxLookback`, etc.) pass through unchanged — we use VL's actual handler, just wrapping the response.

### Config

```yaml
lakehouse:
  ui:
    enabled: true       # serve /lakehouse/ui/ (default true)
    vmui_tab: true      # inject Lakehouse tab into VMUI (default true)
    refresh_default: 0  # auto-refresh interval in seconds (0 = off)
    theme: "auto"       # auto|dark|light
```

---

## Component 7: LabelIndex Extension

**File:** `internal/cache/persist.go` (extend existing `LabelIndex`)

Current LabelIndex tracks field names and sample values globally. Extend to support per-tenant cardinality:

```go
type LabelInfo struct {
    Name         string            // field name
    Cardinality  int               // global unique value count
    Values       []string          // sample values (capped at 10K)
    SeenInFiles  int               // files containing this field
    Origin       string            // "promoted" or "map"
    MapColumn    string            // parent MAP column if origin=map
    HasBloom     bool              // bloom filter enabled
    PerTenant    map[string]int    // tenant → unique value count
}
```

Updated on:
- `updateLabelIndex()` during query file scan (existing path)
- `extractLogLabels()` / `extractTraceLabels()` during flush (existing path)

Both paths now receive tenant context and update `PerTenant` cardinality.

---

## Wiring

**File:** `cmd/lakehouse-logs/main.go` (and mirrored in `lakehouse-traces/`)

### Startup

```
1. Create TenantRegistry (load S3 snapshots if available)
2. Create CardinalityLimiter (from config)
3. Create StorageClassTracker (with lifecycle rules)
4. Wire into Storage: registry.RecordWrite() on AddFile, registry.RecordQuery() on RunQuery
5. Register API handlers on mux:
   /lakehouse/api/v1/tenants          → stats.TenantsHandler(registry)
   /lakehouse/api/v1/tenants/{a}/{p}  → stats.TenantDetailHandler(registry)
   /lakehouse/api/v1/stats/overview   → stats.OverviewHandler(registry, manifest)
   /lakehouse/api/v1/stats/ingestion  → stats.IngestionHandler(registry, manifest)
   /lakehouse/api/v1/stats/cost       → stats.CostHandler(registry, tracker)
   /lakehouse/api/v1/stats/compression → stats.CompressionHandler(registry, manifest)
   /lakehouse/api/v1/cardinality/fields → stats.CardinalityHandler(labelIndex)
   /internal/stats/sync               → stats.SyncHandler(registry)
6. Register UI handler: /lakehouse/ui/ → ui.Handler()
7. Wrap VMUI handler: /vmui/ → ui.InjectLakehouseTab(vlselect.VMUIHandler())
8. Start background loops:
   - Registry push loop (push_interval)
   - Registry S3 snapshot loop (snapshot_interval)
   - Prometheus metrics update loop (15s)
   - StorageClass refresh (on manifest refresh)
```

### Shutdown

```
1. Stop background loops
2. Final S3 snapshot write
3. Final peer push (full sync)
```

---

## Configuration Summary

```yaml
lakehouse:
  tenant:
    isolation: prefix                   # "prefix" (shared bucket) or "bucket" (per-tenant bucket)
    bucket_template: "obs-{AccountID}-{ProjectID}"  # bucket name template (bucket mode)
    known_tenants:                      # required for bucket-mode cold tenant discovery
      - account_id: "100"
        project_id: "1"
        lifecycle_rules:                # optional per-tenant lifecycle override
          - transition_days: 14
            storage_class: STANDARD_IA
        price_per_gb:                   # optional per-tenant pricing override
          STANDARD: 0.025
      - account_id: "200"
        project_id: "5"                 # uses default rules
  stats:
    enabled: true                       # master switch for all stats features
    push_interval: 30s                  # peer sync delta broadcast interval
    push_compression: true              # ZSTD compress peer deltas
    snapshot_interval: 5m               # S3 registry snapshot interval
    snapshot_prefix: "_meta/tenant-stats"  # S3 prefix for snapshots
    meta_bucket: ""                     # dedicated meta bucket for stats (bucket mode, optional)
    max_delta_count: 1000               # force full peer sync after N deltas
    metrics_cardinality_limit: 100      # max tenant label values in Prometheus (0=disable)
    cardinality_warning_threshold: 10000  # high-cardinality field warning
    s3_lifecycle_rules:                 # default lifecycle rules (all tenants in prefix mode)
      - transition_days: 30
        storage_class: STANDARD_IA
      - transition_days: 90
        storage_class: GLACIER
      - transition_days: 365
        storage_class: DEEP_ARCHIVE
    s3_price_per_gb:                    # default per-class storage pricing
      STANDARD: 0.023
      STANDARD_IA: 0.0125
      GLACIER_IR: 0.004
      GLACIER: 0.0036
      DEEP_ARCHIVE: 0.00099
    s3_request_prices:                  # per-1000-requests pricing
      PUT: 0.005
      GET: 0.0004
      LIST: 0.005
    s3_inventory_bucket: ""             # optional S3 Inventory source
    headobject_sample_interval: 6h      # spot-check interval near transition boundaries
    headobject_max_per_refresh: 50      # cap HeadObject calls per cycle
  ui:
    enabled: true                       # serve /lakehouse/ui/
    vmui_tab: true                      # inject tab into VMUI
    refresh_default: 0                  # auto-refresh seconds (0=off)
    theme: "auto"                       # auto|dark|light
```

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/stats/registry.go` | TenantRegistry: in-memory tenant stats, CRDT merge |
| `internal/stats/registry_test.go` | Registry CRUD, merge, concurrent access tests |
| `internal/stats/sync.go` | Peer broadcast: delta/full push, receive, S3 snapshots |
| `internal/stats/sync_test.go` | Sync protocol, compression, merge convergence tests |
| `internal/stats/storageclass.go` | Storage class tracker: lifecycle prediction, HeadObject |
| `internal/stats/storageclass_test.go` | Class prediction, boundary detection tests |
| `internal/stats/api.go` | HTTP handlers for all `/lakehouse/api/v1/*` endpoints |
| `internal/stats/api_test.go` | API response format, query param, sort/filter tests |
| `internal/stats/cardinality_limiter.go` | Prometheus cardinality cap |
| `internal/stats/cardinality_limiter_test.go` | Cap enforcement, overflow tracking tests |
| `internal/stats/cost.go` | Cost calculation: per-class pricing, projections |
| `internal/stats/cost_test.go` | Cost math, lifecycle projection tests |
| `internal/metrics/lakehouse.go` | Extend with tenant + global storage metrics |
| `internal/ui/ui.go` | HTTP handler serving static UI files |
| `internal/ui/vmui_inject.go` | VMUI response wrapper, script injection |
| `internal/ui/vmui_inject_test.go` | Injection tests (HTML vs non-HTML, Content-Length) |
| `internal/ui/static/index.html` | Single-file Preact + uPlot app (3 tabs) |
| `internal/ui/static/vmui-tab.js` | VMUI nav injection script (~50 lines) |
| `internal/config/config.go` | Add StatsConfig, UIConfig structs |
| `internal/manifest/manifest.go` | Extend FileInfo with StorageClass fields |
| `internal/cache/persist.go` | Extend LabelIndex with per-tenant cardinality |
| `cmd/lakehouse-logs/main.go` | Wire registry, API, UI, sync, metrics |

All changes mirrored to `lakehouse-traces/` module.

---

## Non-Goals

- Real-time streaming stats (WebSocket) — polling via auto-refresh is sufficient
- Historical time-series storage in-process — Prometheus scrapes provide history
- S3 Inventory automated setup — user configures Inventory externally, we only consume
- Billing/chargeback integration — we expose cost estimates, not invoicing
- Authentication on UI — relies on existing network/proxy auth (same as VMUI)

---

## Implementation Notes (2026-05-13)

### Manifest Fallback Pattern

The TenantRegistry is only populated when data flows through the write path (RecordWrite on AddFile). In read-only deployments (datagen, external ETL, or when lakehouse only serves cold data from S3), the registry is empty. Every API handler implements a **manifest fallback**: when registry data is empty, derive stats from the partition manifest which always has accurate S3 file metadata.

Affected endpoints:
- **GET /tenants**: falls back to `Manifest.TenantSummaries()` — extracts `AccountID/ProjectID` from S3 keys
- **GET /tenants/{a}/{p}**: falls back to manifest, adds drill-down (partitions, file_size_histogram)
- **GET /stats/overview**: `tenant_count` falls back to manifest count; `storage_by_class` defaults to STANDARD when class tracker is empty
- **GET /stats/cost**: derives per-tenant cost from manifest bytes at STANDARD class pricing
- **GET /stats/compression**: shows per-tenant byte totals from manifest (compression ratio unavailable without raw bytes)

### VMUI Tab: Inline Rendering (Not Iframe)

The spec described iframe-based injection. Implementation uses **inline rendering** instead:
- `vmui-tab.js` (~412 lines) fetches Lakehouse API directly and renders cards/tables/charts inside VMUI's content area
- Saves/restores VMUI's original content when switching between Lakehouse tab and other VMUI tabs
- Uses VMUI CSS variables (`--color-primary`, `--color-background-body`, `--color-text`, etc.) for visual consistency
- Has 3 sub-tabs matching the spec: Storage Overview, Tenants, Cardinality Explorer

### Cardinality: Schema-Based Type Classification

Field type (promoted vs map) is determined by `SchemaRegistry.IsPromoted()` — checking whether a field has a top-level Parquet column mapping. This replaces the original cardinality-based heuristic which was unreliable. The `SchemaRegistry` is now wired into `stats.APIConfig` from both main binaries.

### Bloom Filter Detection

Bloom filter status uses the configured `BloomColumns` list (from `config.ActiveBloomColumns()`) with suffix matching. This handles traces where label index discovers `resource_attr:service.name` but bloom config has `service.name`.

```go
hasBloomFilter := func(name string) bool {
    if _, ok := bloomSet[name]; ok { return true }
    if idx := strings.LastIndex(name, ":"); idx >= 0 {
        if _, ok := bloomSet[name[idx+1:]]; ok { return true }
    }
    return false
}
```

### Default Bloom Columns

- **Logs mode**: `service.name`, `trace_id` (both promoted, bloom-enabled in Parquet)
- **Traces mode**: `trace_id`, `service.name` (both promoted, bloom-enabled in Parquet)

### Tenant Discovery from S3 Keys

S3 key structure: `{AccountID}/{ProjectID}/{signal}/dt=YYYY-MM-DD/hour=HH/{hash}.parquet`

`Manifest.TenantSummaries()` parses the first two path components from all file keys to extract tenant identity. This provides tenant discovery for read-only deployments without requiring the write path.

---

## UI Safeguards

### Data Availability Guards

All UI components must handle the case where API data is incomplete or empty:

1. **Empty state**: Every card, table, and chart shows a meaningful empty state (e.g., "No data available", "No tenants found") instead of blank/broken rendering
2. **Zero values**: Fields like `compression_ratio`, `total_rows`, `raw_bytes` may be 0 in read-only deployments. UI should display "N/A" or "—" for zero compression ratios rather than "0.00x"
3. **Missing fields**: API responses may omit optional fields (`omitempty`). UI must check for existence before accessing nested properties

### Error Handling

1. **Fetch failures**: All API fetches use try/catch. On failure, display an inline error message in the affected component, not a global error
2. **Partial failures**: If one endpoint fails (e.g., cost), other tabs continue working independently
3. **Timeout**: Fetch calls should use AbortController with a 10s timeout to prevent hanging UI

### VMUI Integration Safety

1. **MutationObserver cleanup**: Always disconnect the observer once the nav is found to avoid memory leaks
2. **Content preservation**: Save/restore VMUI's original main content when toggling between Lakehouse tab and native VMUI tabs
3. **No global state pollution**: All Lakehouse JS is scoped — no global variables that could conflict with VMUI's React app
4. **Graceful degradation**: If VMUI DOM structure changes (upstream VL update), the injection script silently fails without breaking VMUI functionality

### Refresh Safety

1. **Cancel on tab switch**: Auto-refresh intervals are cleared when switching tabs or navigating away
2. **Debounce**: Rapid tab switches don't trigger concurrent API calls — previous pending fetches are aborted
3. **Stale data indicator**: If auto-refresh is off and data is older than 5 minutes, show a "stale data" badge

### Tenant Detail Drill-Down

1. **Large partition lists**: If a tenant has >365 partitions, paginate or truncate the partition list with a "show more" control
2. **File histogram**: Always show all buckets even if count is 0 (consistent axes)
3. **Time range formatting**: Display human-readable relative time (e.g., "4 days ago") alongside absolute timestamps
