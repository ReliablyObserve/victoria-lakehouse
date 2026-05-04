# Victoria Lakehouse — Storage Parity Design Spec

**Date**: 2026-05-04
**Status**: Draft
**Scope**: Four PRs bringing S3-Parquet storage to feature parity with EBS-based VL/VT

## Context

Victoria Lakehouse M1-M6 delivers a read-only cold storage tier (18.5K LOC, 738 tests). The write path (M8-Phase 1-3) added insert APIs, batch writer, schema extensibility, and MAP columns. This spec covers the remaining gaps to reach parity with VL/VT's native EBS storage.

### Current State

- Write path: insert APIs → in-memory buffer → Parquet → S3 → manifest
- Read path: manifest lookup → multi-tier cache → Parquet scan → DataBlock stream
- No WAL — unflushed rows lost on crash
- No buffer query bridge — 10s write-to-read gap
- No retention or compaction
- No row-group filter push-down beyond time + bloom
- No multi-tenant isolation
- No full-text token indexing

### Constraints

- **ZERO modifications to VL/VT source code** — import as dependency only
- **No separate storage nodes** — insert + select roles, S3 is storage
- **No sidecar files on S3** — only Parquet files; metadata lives in manifest (in-memory + EBS persistence)
- **Minimize complexity** — fewer moving parts, maximum compression efficiency

## Query Cost Model — Five-Level Prune Cascade

Every query passes through five pruning levels. Each level is cheaper than the next. The goal: answer "nothing here" at the cheapest possible level.

```
Level 0: Tenant prune     — manifest scoped by tenant prefix           — O(1), zero I/O
Level 1: Time prune       — partition key dt=/hour=                    — sub-ms, zero I/O
Level 2: Label prune      — FileInfo.Labels match against query fields — sub-ms, zero I/O
Level 3: Row-group prune  — Parquet column stats + bloom filters       — <10ms, footer read (cached)
Level 4: Row scan         — read matching columns, apply filters       — 50-500ms, data read
```

Levels 0-2 are **manifest-only** (in-memory, no S3 I/O). Level 3 reads Parquet footer (cached after first access). Level 4 reads actual data columns. For "nothing here" queries, processing stops at Level 0, 1, or 2 — zero cost.

### S3 Path Structure

```
{bucket}/{tenant_prefix}/{signal}/dt=YYYY-MM-DD/hour=HH/{batch_id}.parquet
```

- `tenant_prefix`: derived from AccountID/ProjectID (configurable template)
- `signal`: `logs` or `traces` (from mode)
- `dt=`/`hour=`: Hive partition for time pruning
- `batch_id`: random hex ID

### Enhanced FileInfo (Manifest Entry)

Each file in the manifest carries metadata that enables Levels 0-2 pruning without touching S3:

```go
type FileInfo struct {
    Key               string              `json:"key"`
    Size              int64               `json:"size"`
    RowCount          int64               `json:"row_count,omitempty"`
    MinTimeNs         int64               `json:"min_time_ns,omitempty"`
    MaxTimeNs         int64               `json:"max_time_ns,omitempty"`
    RawBytes          int64               `json:"raw_bytes,omitempty"`
    SchemaFingerprint string              `json:"schema_fp,omitempty"`
    CompactionLevel   int                 `json:"compaction_level,omitempty"`
    Labels            map[string][]string `json:"labels,omitempty"`
}
```

`Labels` contains distinct values of promoted fields present in the file (capped at 100 per field). Populated during flush at zero extra cost (writer already iterates all rows). Enables:
- **Retention**: pattern rules match against Labels to determine per-file TTL
- **Query pruning**: skip files that can't contain matching rows (Level 2)

### Manifest Structure (Multi-Tenant)

```go
// Single-tenant (current):  map[partition][]FileInfo
// Multi-tenant (Phase D):   map[tenant]map[partition][]FileInfo
```

Tenant scoping is a prefix filter on the existing map — no structural redesign needed. Single-tenant deployments use empty string as tenant key.

---

## Phase A: Write Durability & Adaptive Sizing (PR 1)

### A1: Write-Ahead Log (WAL)

Single append-only binary file per instance. Ensures zero data loss on crash.

**Format:**

```
[4-byte little-endian length][1-byte mode: 'L'=log, 'T'=trace][gob-encoded row bytes]
```

**Lifecycle:**

1. `MustAddLogRows(rows)` → append each row to WAL → buffer in memory
2. `FlushAll()` succeeds → truncate WAL (atomic: write empty file to `.tmp`, rename over WAL)
3. Startup: if WAL file exists and non-empty → replay: decode rows → reconstruct partition buffers → resume flush loop

**Config:**

| Flag | Default | Description |
|---|---|---|
| `insert.wal-enabled` | `true` | Enable WAL for crash recovery |
| `insert.wal-dir` | `/data/lakehouse/wal` | WAL file directory |
| `insert.wal-max-bytes` | `512MB` | Max WAL size; backpressure when full |

**Backpressure**: When WAL exceeds `wal-max-bytes`, `CanWriteData()` returns error, insert handlers respond 503 Service Unavailable. This prevents unbounded disk growth while signaling upstream to retry.

**Corrupt entry handling**: Each WAL entry is length-prefixed. On replay, if a length prefix reads beyond EOF or gob decode fails, stop replay at that point — the partial entry is from a crash mid-write. All complete entries before it are valid and replayed. Log a warning with the number of bytes discarded.

**Encoding**: `encoding/gob` — zero new dependencies, adequate throughput for WAL (not on hot read path). If VL's `lib/encoding` exports VarInt/string encoding, use that instead for compactness.

**Implementation:**

New file: `internal/wal/wal.go`

```go
type WAL struct {
    mu   sync.Mutex
    file *os.File
    size int64
    max  int64
}

func (w *WAL) Append(mode byte, row any) error    // encode + write + sync size counter
func (w *WAL) Truncate() error                     // atomic rename of empty file
func (w *WAL) Replay() ([]schema.LogRow, []schema.TraceRow, error) // decode all entries
func (w *WAL) Size() int64                         // current WAL size
func (w *WAL) IsFull() bool                        // size >= max
```

**Integration with BatchWriter:**

```go
func (w *BatchWriter) AddLogRows(rows []schema.LogRow) {
    if w.wal != nil {
        for i := range rows {
            if err := w.wal.Append('L', &rows[i]); err != nil {
                w.logger.Error("WAL append failed", "error", err)
            }
        }
    }
    // ... existing partition buffering ...
}

func (w *BatchWriter) FlushAll(ctx context.Context) error {
    // ... existing flush logic ...
    if err == nil && w.wal != nil {
        w.wal.Truncate()
    }
    return err
}
```

**Startup recovery** in `storage.New()`:

```go
if cfg.Insert.WALEnabled {
    wal, _ := wal.Open(cfg.Insert.WALDir, cfg.Insert.WALMaxBytesN())
    logs, traces, err := wal.Replay()
    if err == nil && (len(logs) > 0 || len(traces) > 0) {
        writer.AddLogRows(logs)
        writer.AddTraceRows(traces)
        logger.Info("WAL replayed", "logs", len(logs), "traces", len(traces))
    }
}
```

### A2: Adaptive File Sizing

Four flush triggers — **first one wins**:

| Trigger | Config Flag | Default | Purpose |
|---|---|---|---|
| Time | `insert.flush-interval` | `10s` | Latency bound — never wait longer than this |
| Partition size | `insert.target-file-size` | `128MB` | File quality — produces well-sized Parquet files |
| Row count | `insert.max-buffer-rows` | `50000` | Memory safety per partition |
| Total memory | `insert.max-buffer-bytes` | `256MB` | System memory safety |

**Adaptive behavior**: At high throughput (>10MB/s per partition), buffers hit `target-file-size` before `flush-interval` — producing large, well-compressed files with optimal row groups. At low throughput (<1MB/s), `flush-interval` fires first — producing smaller files but respecting latency. No tuning needed — the system self-adjusts to workload.

**New config field**: `--lakehouse.insert.target-file-size` (default `128MB`). Parsed via existing `ParseSizeBytes()`.

**Implementation**: Extend `checkSizeThreshold()` to also check per-partition byte estimate:

```go
func (w *BatchWriter) checkSizeThreshold() {
    total := int(w.totalRows.Load())
    if total >= w.cfg.MaxBufferRows {
        w.triggerFlush()
        return
    }
    // Per-partition size check on each add
    w.mu.Lock()
    for _, rows := range w.logBufs {
        if estimatePartitionBytes(rows) >= w.cfg.TargetFileSizeN() {
            w.mu.Unlock()
            w.triggerFlush()
            return
        }
    }
    w.mu.Unlock()
}
```

**File size guidance** (for documentation):

| Throughput | Flush Trigger | Typical File Size | Compression Ratio |
|---|---|---|---|
| <1 MB/s | Time (10s) | 5-10 MB | 3-5x |
| 1-10 MB/s | Time or Size | 10-128 MB | 5-8x |
| >10 MB/s | Size (128MB) | ~128 MB | 8-12x |
| Burst (100+ MB/s) | Size (128MB) | ~128 MB | 8-12x |

### A3: Buffer Query Bridge

Insert pods expose unflushed data over HTTP so select pods can query it.

**Endpoint (insert pods):**

```
GET /internal/buffer/query?start=<ns>&end=<ns>&mode=logs|traces
Response: NDJSON — one JSON object per row
```

Returns rows from in-memory buffers matching the time range. Reuses existing `BufferedLogRows()` / `BufferedTraceRows()` methods.

**Select-side integration:**

On query, select pod executes in parallel:
1. **S3 path**: manifest lookup → Parquet scan (existing)
2. **Buffer path**: HTTP call to insert pods → parse NDJSON → merge

Insert pod discovery:
- `role=all` (single binary): local function call, zero network
- `role=select`: discover insert pods via `--lakehouse.select.insert-headless-service` (headless DNS, reuses existing discovery package)

**Merge**: results combined by timestamp. Deduplication by `(timestamp, _stream, body)` tuple to handle the edge case where a flush completes between the S3 query and buffer query.

**Config:**

| Flag | Default | Description |
|---|---|---|
| `select.buffer-query-enabled` | `true` | Enable buffer query bridge |
| `select.insert-headless-service` | `""` | Headless service for insert pod discovery |
| `select.buffer-query-timeout` | `2s` | Timeout for buffer queries to insert pods |

**Implementation:**

New handler registered in insert role:
```go
mux.HandleFunc("/internal/buffer/query", h.handleBufferQuery)
```

Select-side in `RunQuery()`:
```go
func (s *Storage) RunQuery(ctx context.Context, qctx *QueryContext, writeBlock WriteDataBlockFunc) error {
    // Existing S3/manifest path
    err := s.runParquetQuery(ctx, qctx, writeBlock)

    // Buffer bridge (parallel in production, sequential here for clarity)
    if s.bufferBridge != nil {
        bufRows, _ := s.bufferBridge.Query(ctx, qctx.StartNs, qctx.EndNs)
        if len(bufRows) > 0 {
            db := s.rowsToDataBlock(bufRows, qctx)
            writeBlock(db)
        }
    }
    return err
}
```

### A4: FileInfo Labels Population

During flush, extract distinct values of promoted fields for manifest-level query pruning:

```go
func extractLabels(rows []schema.LogRow) map[string][]string {
    sets := map[string]map[string]bool{}
    for i := range rows {
        addIfNonEmpty(sets, "service.name", rows[i].ServiceName)
        addIfNonEmpty(sets, "severity_text", rows[i].SeverityText)
        addIfNonEmpty(sets, "k8s.namespace.name", rows[i].K8sNamespaceName)
        // ... other promoted fields
    }
    labels := make(map[string][]string, len(sets))
    for k, vs := range sets {
        vals := make([]string, 0, len(vs))
        for v := range vs {
            if len(vals) >= 100 { break } // cap per field
            vals = append(vals, v)
        }
        labels[k] = vals
    }
    return labels
}
```

Called in `flushLogPartition()` / `flushTracePartition()`, stored in `FileInfo.Labels`.

### Phase A Deliverables

- `internal/wal/wal.go` + `wal_test.go` — WAL implementation
- Updated `internal/storage/parquets3/writer.go` — WAL integration, adaptive sizing, label extraction
- New `internal/insertapi/buffer_handler.go` + test — buffer query endpoint
- Updated `internal/storage/parquets3/storage.go` — buffer bridge in RunQuery
- Updated `internal/config/config.go` — new config fields
- Updated `internal/manifest/manifest.go` — Labels field, label-aware file matching

---

## Phase B: Read Performance (PR 2)

### B1: Row-Group Filter Push-Down

Current state: row groups are skipped only by time range stats and bloom filter (service.name, trace_id). Extend to use Parquet column statistics (min/max per row group) for all filter types.

**Push-down rules:**

| Filter | Push-Down Strategy |
|---|---|
| `field:="exact"` | Skip row group if value < column min OR value > column max |
| `field:>"N"` (greater than) | Skip if column max < N |
| `field:<"N"` (less than) | Skip if column min > N |
| `field:="prefix*"` (prefix match) | Skip if prefix > column max OR prefix+`\xff` < column min |
| `field:value` (substring) | No stats skip possible — must scan |
| `field:~"regex"` | Extract literal prefix if present → prefix skip; else scan |
| `NOT filter` | Skip only if child filter guarantees ALL rows match (conservative) |
| `AND(a, b)` | Skip if EITHER child skips (short-circuit) |
| `OR(a, b)` | Skip only if BOTH children skip |

**Implementation:**

New function in `storage.go`:

```go
func rowGroupMatchesFilter(rg parquet.RowGroup, filter *selectapi.Filter, schema *parquet.Schema) bool {
    switch filter.Op {
    case selectapi.OpExact:
        col := findColumnChunk(rg, filter.Field)
        if col == nil { return true } // unknown column, can't skip
        idx := col.ColumnIndex()
        for i := 0; i < idx.NumPages(); i++ {
            min, max := idx.MinValue(i), idx.MaxValue(i)
            if filter.Value >= min.String() && filter.Value <= max.String() {
                return true // page could contain match
            }
        }
        return false // no page can contain this value
    case selectapi.OpGreaterThan:
        // skip if max < threshold
    case selectapi.OpAnd:
        return rowGroupMatchesFilter(rg, filter.Left, schema) &&
               rowGroupMatchesFilter(rg, filter.Right, schema)
    // ... other operators
    }
}
```

Compose in `queryFile()`:
```go
for i := 0; i < file.NumRowGroups(); i++ {
    rg := file.RowGroups()[i]
    if !rowGroupMatchesTimeRange(rg, startNs, endNs) { continue }
    if bloomFilterSkip(rg, bloomChecks)              { continue }
    if !rowGroupMatchesFilter(rg, filter, schema)    { continue } // NEW
    readRowGroup(rg, ...)
}
```

**Cost**: reading column index is part of the Parquet footer (already cached at L1/L2). Zero additional S3 I/O.

### B2: Full-Text Token Bloom

Accelerate body text search by storing per-row-group token bloom filters in Parquet metadata.

**On write** (in `writeLogsParquet`):

1. For each row group, collect all body text
2. Tokenize: split on whitespace and punctuation, lowercase, deduplicate
3. Build bloom filter (~1KB for 10K tokens at 1% FPR)
4. Serialize bloom filter bytes
5. Store in Parquet file key-value metadata: key `_bloom_body_rg_{N}`, value = base64-encoded bloom bytes

```go
func buildTokenBloom(bodies []string) []byte {
    bf := bloom.NewWithEstimates(10000, 0.01) // 10K tokens, 1% FPR
    for _, body := range bodies {
        for _, token := range tokenize(body) {
            bf.AddString(token)
        }
    }
    buf, _ := bf.MarshalBinary()
    return buf
}

func tokenize(s string) []string {
    // Split on non-alphanumeric, lowercase, deduplicate
    // Reuse VL's tokenization algorithm if lib/logstorage exports it
}
```

**On read** (in `queryFile`):

1. Extract search terms from body filter (substring or word match)
2. For each row group, load token bloom from file metadata
3. Check if ALL search tokens exist in bloom → if any is definitely absent, skip row group
4. Fall back to row scan if bloom says "maybe present"

```go
func tokenBloomSkip(file *parquet.File, rgIndex int, searchTokens []string) bool {
    key := fmt.Sprintf("_bloom_body_rg_%d", rgIndex)
    meta := file.Metadata().KeyValueMetadata()
    bloomBytes := findMetaValue(meta, key)
    if bloomBytes == nil { return false } // no bloom, can't skip

    bf := &bloom.BloomFilter{}
    bf.UnmarshalBinary(bloomBytes)
    for _, token := range searchTokens {
        if !bf.TestString(token) {
            return true // token definitely not in this row group
        }
    }
    return false // all tokens might be present
}
```

**Backward compatibility**: Old files without `_bloom_body_rg_*` metadata are never skipped — they just fall through to row scan. New files are readable by any Parquet reader (metadata is ignored by readers that don't know about it).

**Bloom dependency**: Use `github.com/bits-and-blooms/bloom/v3` if available in module graph, otherwise implement minimal bloom (hash + bitset — ~50 LOC).

### Phase B Deliverables

- Updated `internal/storage/parquets3/storage.go` — `rowGroupMatchesFilter()`, token bloom skip
- New `internal/storage/parquets3/filter_pushdown.go` + test — filter push-down logic
- New `internal/storage/parquets3/token_bloom.go` + test — tokenization, bloom build/check
- Updated `internal/storage/parquets3/writer.go` — token bloom generation during write
- Updated `internal/selectapi/filter.go` — expose filter AST for push-down consumption

---

## Phase C: Data Lifecycle (PR 3)

### C1: Retention with Pattern Rules

Automatic deletion of old data based on configurable time-based rules with pattern matching.

**Config:**

```yaml
lakehouse:
  retention:
    enabled: true
    default: 90d                    # default TTL for all files
    check_interval: 1h              # how often to run retention check
    rules:
      - match:
          severity_text: "DEBUG"
        keep: 7d
      - match:
          service.name: "critical-*"   # glob pattern
        keep: 365d
      - match:
          k8s.namespace.name: "staging"
        keep: 30d
```

**Config types:**

```go
type RetentionConfig struct {
    Enabled       bool            `yaml:"enabled"`
    Default       time.Duration   `yaml:"default"`
    CheckInterval time.Duration   `yaml:"check_interval"`
    Rules         []RetentionRule `yaml:"rules"`
}

type RetentionRule struct {
    Match map[string]string `yaml:"match"` // field → value or glob pattern
    Keep  time.Duration     `yaml:"keep"`
}
```

**Rule matching:**

A file matches a rule if ALL fields in `match` are satisfied:
- Exact match: `"DEBUG"` matches if `"DEBUG"` is in `FileInfo.Labels["severity_text"]`
- Glob match: `"critical-*"` matches if any value in `FileInfo.Labels["service.name"]` matches the glob

When multiple rules match, the **longest retention wins** (most conservative — never delete data that any rule says to keep).

When no rules match, `default` TTL applies.

**File age**: determined by `FileInfo.MaxTimeNs` (the newest row in the file). A file is eligible for deletion only when `now - MaxTimeNs > TTL`. This is conservative — some rows may be older than TTL, but we don't delete until ALL rows are past retention.

**Deletion process:**

```go
func (r *RetentionManager) Run(ctx context.Context) {
    ticker := time.NewTicker(r.cfg.CheckInterval)
    for {
        select {
        case <-ticker.C:
            r.runOnce(ctx)
        case <-ctx.Done():
            return
        }
    }
}

func (r *RetentionManager) runOnce(ctx context.Context) {
    now := time.Now()
    files := r.manifest.AllFiles() // snapshot

    for partition, fileList := range files {
        for _, fi := range fileList {
            ttl := r.resolveTTL(fi)
            fileAge := now.Sub(time.Unix(0, fi.MaxTimeNs))
            if fileAge > ttl {
                r.deleteFile(ctx, partition, fi)
            }
        }
    }
}
```

**Implementation:**

New file: `internal/retention/retention.go`

```go
type Manager struct {
    cfg      *config.RetentionConfig
    manifest *manifest.Manifest
    s3pool   *s3reader.ClientPool
    logger   *slog.Logger
}

func (m *Manager) Start(ctx context.Context)           // background goroutine
func (m *Manager) resolveTTL(fi manifest.FileInfo) time.Duration
func (m *Manager) matchRule(fi manifest.FileInfo, rule config.RetentionRule) bool
func (m *Manager) deleteFile(ctx context.Context, partition string, fi manifest.FileInfo) error
```

Manifest gets new methods:
```go
func (m *Manifest) AllFiles() map[string][]FileInfo           // snapshot of all partitions
func (m *Manifest) RemoveFile(partition string, key string)   // remove from index after S3 delete
```

### C2: Compaction

Background goroutine merges small Parquet files within a partition into larger, optimally-sized files.

**Config:**

```yaml
lakehouse:
  compaction:
    enabled: true
    interval: 1h                    # how often to scan for compaction candidates
    min_files_per_partition: 10     # trigger: partition has more than this many files
    min_file_size_ratio: 0.25      # trigger: file smaller than target-file-size * this ratio
    max_files_per_run: 100         # safety: max files to compact in one run
    max_compaction_level: 2        # don't re-compact files at this level
```

**Trigger conditions** (either triggers compaction for a partition):
1. Partition has more than `min_files_per_partition` files
2. Any file in partition is smaller than `target-file-size * min_file_size_ratio`

Files at `CompactionLevel >= max_compaction_level` are excluded from compaction.

**Process:**

```
1. Scan manifest for compaction candidates
2. For each candidate partition:
   a. Lock partition in manifest (prevent concurrent flush to same partition key prefix)
   b. Download all candidate files (via cache — likely already cached)
   c. Read all rows, merge, sort by timestamp
   d. Write new target-sized file(s) with optimal row groups and bloom filters
   e. Upload new file(s) to S3
   f. Add new FileInfo(s) to manifest (CompactionLevel incremented)
   g. Delete old files from S3
   h. Remove old FileInfo(s) from manifest
   i. Unlock partition
```

**Crash safety**: New files are written and added to manifest BEFORE old files are deleted. If crash occurs mid-compaction:
- Both old and new files exist → query returns duplicates (acceptable, deduplicated at query layer)
- On next compaction run, orphaned new files are discovered via S3 refresh
- Old files that weren't deleted yet are cleaned up on next run

**Implementation:**

New file: `internal/compaction/compaction.go`

```go
type Compactor struct {
    cfg      *config.CompactionConfig
    manifest *manifest.Manifest
    writer   *parquets3.BatchWriter
    s3pool   *s3reader.ClientPool
    mode     config.Mode
    logger   *slog.Logger
}

func (c *Compactor) Start(ctx context.Context)                      // background goroutine
func (c *Compactor) findCandidates() map[string][]manifest.FileInfo // partitions needing compaction
func (c *Compactor) compactPartition(ctx context.Context, partition string, files []manifest.FileInfo) error
```

Manifest gets new method:
```go
func (m *Manifest) LockPartition(partition string)    // prevent concurrent writes during compaction
func (m *Manifest) UnlockPartition(partition string)
```

### Phase C Deliverables

- New `internal/retention/retention.go` + test — retention manager with pattern rules
- New `internal/compaction/compaction.go` + test — background compactor
- Updated `internal/config/config.go` — RetentionConfig, CompactionConfig types with defaults and validation
- Updated `internal/manifest/manifest.go` — AllFiles(), RemoveFile(), LockPartition()/UnlockPartition()
- Updated `internal/storage/parquets3/storage.go` — start retention + compaction goroutines
- Updated `internal/storage/parquets3/writer.go` — populate FileInfo.Labels during flush

---

## Phase D: Multi-Tenancy (PR 4)

### D1: Tenant Routing

S3 path structure with tenant prefix:

```
{bucket}/{tenant_prefix}/{signal}/dt=YYYY-MM-DD/hour=HH/{batch_id}.parquet
```

**Tenant ID extraction:**

VL convention: `AccountID:ProjectID` from HTTP header or URL parameter. Default: empty (single-tenant).

```go
type TenantID struct {
    AccountID uint32
    ProjectID uint32
}

func (t TenantID) Prefix(template string) string {
    // e.g., template="{AccountID}/{ProjectID}/" → "12345/67890/"
}
```

**On write:**
1. Insert handler extracts TenantID from request headers
2. Partition key becomes `{tenant_prefix}/{signal}/dt=.../hour=.../`
3. Manifest stores files under tenant scope

**On read:**
1. `QueryContext.TenantID` scopes manifest lookup to tenant prefix
2. Only files under matching tenant prefix are considered
3. Zero cross-tenant data access possible at storage layer

### D2: Manifest Multi-Tenant Structure

```go
type Manifest struct {
    mu    sync.RWMutex
    files map[string]map[string][]FileInfo // tenant → partition → files
    // ... other fields per tenant
}
```

For single-tenant deployments, tenant key is `""` (empty string). No behavioral change.

**Migration**: existing single-tenant manifests load into `""` tenant key. Zero-downtime upgrade.

### D3: GetTenantIDs Implementation

Currently a stub returning nil. Implement:

```go
func (s *Storage) GetTenantIDs(ctx context.Context, qctx *QueryContext) ([]TenantID, error) {
    return s.manifest.TenantIDs(), nil
}

func (m *Manifest) TenantIDs() []TenantID {
    m.mu.RLock()
    defer m.mu.RUnlock()
    ids := make([]TenantID, 0, len(m.files))
    for tenant := range m.files {
        ids = append(ids, parseTenantPrefix(tenant))
    }
    return ids
}
```

### D4: Tenant-Aware Retention and Compaction

Retention rules and compaction both respect tenant boundaries:
- Retention: rules can include tenant match (`match: {_tenant: "12345/*"}`)
- Compaction: only merges files within same tenant prefix (never cross-tenant)
- Per-tenant retention overrides global default if configured

### Phase D Deliverables

- Updated `internal/manifest/manifest.go` — multi-tenant map, TenantIDs()
- Updated `internal/storage/parquets3/storage.go` — tenant-scoped queries
- Updated `internal/storage/parquets3/writer.go` — tenant-prefixed S3 paths
- Updated `internal/insertapi/handler.go` — tenant ID extraction from headers
- Updated `internal/retention/retention.go` — tenant-aware rules
- Updated `internal/compaction/compaction.go` — tenant-scoped compaction
- Updated `internal/config/config.go` — tenant config fields

---

## Cross-Phase: VL Code Reuse Strategy

Constraint: **import VL packages as Go module dependency, NEVER modify VL source**.

| Component | VL Package | What We Reuse | Fallback |
|---|---|---|---|
| WAL encoding | `lib/encoding` | VarInt, string encoding for compact WAL | `encoding/gob` |
| Tokenization | `lib/logstorage` | `tokenizeValue()` for body token bloom | Implement same algorithm (~30 LOC) |
| Bloom filter | `lib/bloomfilter` | Bloom filter construction | `bits-and-blooms/bloom` or ~50 LOC custom |
| Tenant parsing | `lib/logstorage` | `TenantID` type and parsing | Already have our own in `storage.TenantID` |

Strategy: attempt `go get` of VL packages. If exported (uppercase functions), import directly. If not exported (lowercase/internal), implement the same algorithm in our code. Same behavior, zero VL modifications, clean `go get -u` upgrade path.

---

## Configuration Summary

### New Config Fields (all phases)

```yaml
lakehouse:
  insert:
    wal_enabled: true                 # Phase A
    wal_dir: /data/lakehouse/wal      # Phase A
    wal_max_bytes: 512MB              # Phase A
    target_file_size: 128MB           # Phase A

  select:
    buffer_query_enabled: true        # Phase A
    insert_headless_service: ""       # Phase A
    buffer_query_timeout: 2s          # Phase A

  retention:
    enabled: false                    # Phase C
    default: 90d                      # Phase C
    check_interval: 1h               # Phase C
    rules: []                         # Phase C

  compaction:
    enabled: false                    # Phase C
    interval: 1h                     # Phase C
    min_files_per_partition: 10       # Phase C
    min_file_size_ratio: 0.25         # Phase C
    max_files_per_run: 100            # Phase C
    max_compaction_level: 2           # Phase C
```

### Defaults Rationale

- WAL enabled by default: crash recovery is critical for production, minimal overhead
- Buffer query enabled by default: zero-delay reads expected in single-binary mode
- Retention disabled by default: destructive operation, must be explicitly enabled
- Compaction disabled by default: optional optimization, explicit opt-in
- Target file size 128MB: good balance of compression ratio, query performance, and S3 multipart efficiency

---

## Testing Strategy

Each phase includes unit and integration tests:

**Phase A:**
- WAL: append → replay round-trip, truncate, max-size backpressure, corrupt recovery
- Adaptive sizing: flush triggers for each condition, partition size estimation
- Buffer bridge: HTTP endpoint test, select-side merge, deduplication
- Labels: extraction from log/trace rows, cap enforcement

**Phase B:**
- Filter push-down: each operator type, composite AND/OR, unknown columns
- Token bloom: tokenization, bloom build/check, metadata round-trip, backward compat (files without bloom)

**Phase C:**
- Retention: TTL calculation, pattern matching, glob patterns, most-conservative-wins, S3 deletion
- Compaction: candidate detection, merge correctness, crash safety (write-before-delete), level tracking

**Phase D:**
- Tenant routing: prefix generation, tenant-scoped manifest, cross-tenant isolation
- GetTenantIDs: returns correct tenants from manifest

---

## PR Dependency Chain

```
Phase A (PR 1) ← Phase B (PR 2)    [B uses FileInfo.Labels from A]
Phase A (PR 1) ← Phase C (PR 3)    [C uses FileInfo.Labels from A, retention needs manifest methods]
Phase A (PR 1) ← Phase D (PR 4)    [D restructures manifest, needs A's foundation]
Phase B (PR 2) ── independent of ── Phase C (PR 3)
Phase C (PR 3) ← Phase D (PR 4)    [D adds tenant-aware retention/compaction]
```

Ship order: **A → B and C in parallel → D**

---

## Metrics (all phases)

| Metric | Type | Phase |
|---|---|---|
| `lakehouse_wal_bytes` | Gauge | A |
| `lakehouse_wal_replayed_rows_total` | Counter | A |
| `lakehouse_buffer_query_rows_total` | Counter | A |
| `lakehouse_buffer_query_duration_seconds` | Histogram | A |
| `lakehouse_flush_trigger` | Counter (label: trigger_type) | A |
| `lakehouse_file_size_bytes` | Histogram | A |
| `lakehouse_rowgroup_skipped_total` | Counter (label: reason) | B |
| `lakehouse_token_bloom_skip_total` | Counter | B |
| `lakehouse_retention_deleted_files_total` | Counter | C |
| `lakehouse_retention_deleted_bytes_total` | Counter | C |
| `lakehouse_compaction_runs_total` | Counter | C |
| `lakehouse_compaction_files_merged_total` | Counter | C |
| `lakehouse_compaction_duration_seconds` | Histogram | C |
| `lakehouse_tenants_active` | Gauge | D |
