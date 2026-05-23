# S3 I/O Layer Optimization — Design Spec

**Date:** 2026-05-23
**Status:** Draft
**Scope:** S3 read path optimizations inspired by ClickHouse Parquet reader analysis
**Builds on:** `2026-05-20-query-performance-optimization-design.md` (Phases 0-3), `2026-05-21-query-performance-proposals-design.md` (6 architectural proposals)

## Context

Benchmarks (2026-05-23) on 2.4M rows / 256 files / 460MB show VLH is 7-67x slower than VL on scan-heavy queries, but **faster than ClickHouse** for cached metadata and short-range queries. The dominant bottleneck is **S3 I/O patterns**, not CPU (6.25% utilization per pprof).

ClickHouse querying the **same Parquet files** through the **same S3 proxy** (65ms latency) achieves 2-5x better scan performance than VLH. Analysis of ClickHouse's S3 reader reveals 5 techniques LH does not implement.

### Current LH vs Target

| Query type | LH current | Target (3-5x VL) | Gap factor |
|---|---|---|---|
| wildcard_1h (cached) | **5ms** | 100ms | Already there |
| metadata/cardinality | **0.1ms** | 100ms | Already there |
| query_service_6h | 328ms | 100ms | 3.3x to close |
| query_level_24h | 5389ms | 500ms | 10.8x to close |
| hits_1h | 831ms | 100ms | 8.3x to close |
| hits_6h | 1644ms | 900ms | 1.8x to close |
| stats_count_24h | broken ⚠ | 1300ms | Fix first |

## Constraints

1. **Parquet compatibility**: No file format changes. Files must remain queryable by ClickHouse/DuckDB/Spark.
2. **VL/VT upstream-first**: Never modify VL/VT code. Only Lakehouse code changes.
3. **Backward compatible**: Existing files work without re-ingestion.
4. **Applies to both logs and traces**: All optimizations are in shared `internal/storage/parquets3/` code.

---

## Phase A: S3ReaderAt Read-Ahead Buffer

### Problem

`S3ReaderAt.ReadAt()` (s3reader/reader.go:121-159) issues one S3 GetObject per call. Parquet-go reads pages sequentially — each page = separate HTTP request. A 50-column file with 3 pages/column = **150 S3 requests** per file.

ClickHouse uses `input_format_parquet_enable_row_group_prefetch=1` and 1-2MB read-ahead buffers to batch these reads.

### Solution

Add a buffered wrapper around `S3ReaderAt` that prefetches a configurable window (default 2MB) ahead of the current read position. Subsequent reads within the window are served from the buffer with no S3 call.

### Design

```go
type BufferedS3ReaderAt struct {
    inner     *S3ReaderAt
    buf       []byte
    bufStart  int64
    bufEnd    int64
    prefetch  int64  // default 2MB
    mu        sync.Mutex
}

func (b *BufferedS3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
    b.mu.Lock()
    defer b.mu.Unlock()
    
    if off >= b.bufStart && off+int64(len(p)) <= b.bufEnd {
        copy(p, b.buf[off-b.bufStart:])
        return len(p), nil
    }
    
    // Prefetch: read from off to off+prefetch in one S3 request
    end := off + b.prefetch
    if end > b.inner.size {
        end = b.inner.size
    }
    rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end-1)
    // ... single S3 GetObject with range
}
```

### Expected Impact

- **150 S3 requests per file → 3-5 requests** (2MB buffer covers most column chunks)
- **Query latency: 50-80% reduction** for range-read path
- For a 5MB file with 50 columns: `5MB / 2MB = 3 prefetch reads` instead of 150

### Testing

- Unit test: mock S3 client, verify ReadAt coalesces into fewer actual S3 calls
- Integration test: query through S3 proxy, count proxy request log entries before/after
- Regression: existing projection tests must pass unchanged
- Benchmark: run `cmd/loadtest -mode compare` with buffer sizes 256KB/1MB/2MB/4MB

### Logs-specific

No special handling needed — same code path for logs and traces.

### Traces-specific

Trace queries often project more columns (span.name, span.kind, span.status, parent_span_id, duration_nano, etc.). The 2MB buffer may need to be larger for trace files with wide schemas.

---

## Phase B: Range Read Coalescing

### Problem

When projecting 3-5 columns from a file, `S3ReaderAt` issues separate range reads for each column chunk. Adjacent column chunks (physically close in the Parquet file) trigger separate S3 requests even when the gap between them is small.

ClickHouse merges ranges with gaps < 64KB (`remote_read_min_bytes_for_seek=8192`).

### Solution

Add a coalescing layer that batches column chunk reads scheduled within a time window (5ms). Nearby ranges (gap < configurable threshold, default 64KB) are merged into a single S3 range request.

### Design

```go
type CoalescingReader struct {
    inner     *BufferedS3ReaderAt
    pending   []rangeRequest
    gapThresh int64  // merge if gap < this (default 64KB)
    timer     *time.Timer
    mu        sync.Mutex
}

type rangeRequest struct {
    off    int64
    length int
    result chan readResult
}

func (c *CoalescingReader) flush() {
    // Sort pending by offset
    // Merge adjacent/overlapping ranges with gap < threshold
    // Issue merged S3 requests
    // Distribute results back to individual waiters
}
```

### Expected Impact

- **Additional 30-50% reduction** on top of Phase A for multi-column queries
- Most effective when projection selects 3-10 columns scattered across the file

### Testing

- Unit test: verify ranges `[100-200, 250-350, 400-500]` with gap threshold 60 merges to `[100-500]`
- Integration test: count S3 requests for 5-column projection query, compare before/after
- Edge case: non-adjacent ranges (gap > threshold) must NOT merge
- Edge case: single-column query should bypass coalescing

### Traces-specific

Trace files have more promoted columns (12+) than log files (6+). Coalescing benefits traces more because there are more column chunks to merge.

---

## Phase C: AWS SDK Transport Tuning

### Problem

S3 reader uses AWS SDK v2 default HTTP transport (`http.DefaultTransport`). No explicit connection pooling, keep-alive, or HTTP/2 configuration. Under heavy query load, S3 requests compete for connections and incur TCP/TLS handshake overhead.

ClickHouse uses explicit connection pools (`s3_max_connections` default 10-50).

### Solution

Configure AWS SDK v2 with explicit HTTP transport optimized for high-throughput S3 access.

### Design

```go
transport := &http.Transport{
    MaxIdleConnsPerHost: 64,
    MaxIdleConns:        128,
    IdleConnTimeout:     90 * time.Second,
    TLSHandshakeTimeout: 10 * time.Second,
    ForceAttemptHTTP2:   true,
    DisableCompression:  true,  // Parquet is already compressed
}

cfg, _ := awsconfig.LoadDefaultConfig(ctx,
    awsconfig.WithHTTPClient(&http.Client{Transport: transport}),
)
```

### Expected Impact

- **10-20% latency reduction** from connection reuse and fewer handshakes
- Most visible under concurrent queries (multiple file workers sharing pool)

### Testing

- Integration test: fire 100 concurrent queries, verify no connection errors
- Metrics: expose `s3_connections_active`, `s3_connections_idle` gauges
- Regression: existing tests pass unchanged

---

## Phase D: Async Row Group Prefetch

### Problem

`readOneRowGroup` (storage_query.go:459-502) processes row groups sequentially within each file worker. Max 3 parallel row group workers per file. While processing one row group, the next is idle — no overlap between I/O and processing.

ClickHouse enables `input_format_parquet_enable_row_group_prefetch=1` to prefetch the next row group while processing the current one.

### Solution

1. Increase row group worker cap from 3 to `min(matchedRGs, 8)`.
2. Add async prefetch: while workers process current row groups, pre-issue S3 reads for the next batch.

### Design

```go
// In queryFile, after matching row groups:
rgWorkers := min(len(matchedRGs), 8)  // was: min(len(matchedRGs), 3)

// Prefetch: issue range reads for next batch while processing current
prefetchCh := make(chan *prefetchedRG, rgWorkers)
go func() {
    for _, rg := range matchedRGs[rgWorkers:] {
        // Pre-read column chunks into memory
        data := prefetchRowGroup(ctx, readerAt, rg, projectedCols)
        prefetchCh <- data
    }
}()
```

### Expected Impact

- **2-3x improvement** for files with many row groups (compacted files, wide time ranges)
- Larger impact on traces (more columns per row group)

### Testing

- Unit test: verify 8 concurrent row group reads complete correctly
- Integration test: create file with 10 row groups, query with projection, verify all data returned
- Stress test: 64 file workers × 8 RG workers = 512 concurrent goroutines — must not deadlock or OOM
- Regression: all existing query tests pass

---

## Phase E: Compaction for File Reduction

### Problem

256 small hourly files means 256× metadata operations. Even with footer cache and buffering, each file requires at least 1 S3 request to read data. Fewer, larger files = fewer S3 round trips.

### Solution

Enhance compaction scheduler to target fewer, larger files:
- **Hourly files → daily files** for data > 24h old (reduce 24 files to 1)
- **Target 100-200MB per file** (currently files range 2KB-150MB)
- **Sort within files** by timestamp, then service.name (enables row group pruning via min/max stats)

### Design

```
Before compaction:
  dt=2026-05-22/hour=00/abc.parquet (2MB)
  dt=2026-05-22/hour=01/def.parquet (2MB)
  ... (24 files, 48MB total)

After compaction:
  dt=2026-05-22/day/compacted-001.parquet (48MB, sorted by timestamp+service.name)
```

Row groups within compacted files are sorted, so a query for `service.name:="api-gateway"` can skip row groups where `max(service.name) < "api-gateway"` or `min(service.name) > "api-gateway"`.

### Expected Impact

- **5-10x reduction** in file count for data > 24h old
- **Row group skip** via sorted stats: additional 2-5x for filtered queries on compacted files

### Testing

- Unit test: verify compaction merges N files into 1, preserving all rows
- Integration test: insert data across 24 hours, trigger compaction, verify query returns same results
- Edge case: compaction during query must not cause errors (manifest refresh handles transition)
- Regression: all query tests pass on both pre- and post-compacted data
- Benchmark: run loadtest on compacted vs uncompacted data, measure improvement

### Logs-specific

Log files have 6 promoted columns. Sort by `timestamp_unix_nano, service.name` gives best skip rates for common Grafana queries (recent logs filtered by service).

### Traces-specific

Trace files have 12+ promoted columns. Sort by `timestamp_unix_nano, service.name, trace_id` enables both service filtering and trace_id grouping within row groups. This makes trace correlation queries (all spans for a trace) read from contiguous row group regions.

---

## Phase F: Streaming Aggregation

### Problem

Stats/rate/histogram queries currently download all matching Parquet data, materialize rows, then aggregate. For `stats count(*) time=24h` with 2.4M rows, this downloads and parses all data before counting.

### Solution

Implement streaming aggregation that processes row groups as they arrive from S3, maintaining running aggregates without materializing all rows.

### Design

```go
type StreamingAggregator struct {
    counts  map[string]int64   // for stats by(field)
    buckets []int64            // for histogram time buckets
}

func (a *StreamingAggregator) ProcessRowGroup(rg parquet.RowGroup, filter Filter) {
    // For count-only queries: just add rg.NumRows() (or filtered count)
    // For stats by(field): read only the grouping column + count
    // For histogram: read only timestamp column + count per bucket
}
```

For `count(*)` without filters: use manifest row counts directly (0 S3 reads).
For `count(*) WHERE service.name:="X"`: read only service.name column per file, count matches.
For `stats by(level) count()`: read only severity_text column per file.

### Expected Impact

- **10-50x improvement** for stats queries (read 1-2 columns instead of all)
- **count(*) from manifest**: near-instant (0.1ms like metadata queries)

### Testing

- Unit test: verify streaming aggregator produces same results as full materialization
- Integration test: `stats_query` returns correct counts matching VL
- Edge case: filtered count with zero matches must return 0, not error
- Edge case: group by field with NULL values
- Regression: all existing stats tests pass

### Logs-specific

Common log aggregation: `stats by(level) count()`, `stats by(service.name) count()`. Only needs 1-2 columns read.

### Traces-specific

Common trace aggregation: `stats by(service.name, span.name) count()`, duration percentiles. Duration percentiles require reading `duration_nano` column — can't skip entirely, but column projection still helps.

---

## Verification Plan

### Per-Phase Testing Protocol

Each phase follows this protocol before merge:

1. **Unit tests pass**: `GOWORK=off go test ./internal/... ./cmd/...`
2. **Integration test**: Start LH with S3 proxy, run `cmd/loadtest -mode compare`
3. **Performance gate**: Key scenarios must not regress — compare against baseline JSON
4. **Regression gate**: All existing tests pass, including projection tests and edge cases
5. **ClickHouse baseline**: Verify ClickHouse on same files still returns same results
6. **Document**: Update `docs/vl-comparison.md` with new numbers after each phase

### Baseline Files

```
benchmarks/baseline-logs-current.json     # Before optimization
benchmarks/after-phase-a.json             # After read-ahead buffer
benchmarks/after-phase-b.json             # After coalescing
benchmarks/after-phase-c.json             # After transport tuning
benchmarks/after-phase-d.json             # After async prefetch
benchmarks/after-phase-e.json             # After compaction
benchmarks/after-phase-f.json             # After streaming aggregation
```

### Regression Test Coverage

| Test | What it catches | Location |
|---|---|---|
| `TestQueryColumns_*` | Column projection correctness | `projection_test.go` |
| `TestBloomFilter_*` | Bloom filter skip correctness | `bloom_test.go` |
| `TestFooterCache_*` | Footer cache hit/miss correctness | `footer_cache_test.go` |
| `TestRangeRead_*` | Range read vs full download decision | `range_reader_test.go` |
| `TestStreamingAgg_*` | Streaming aggregation matches full materialization | `streaming_agg_test.go` (new) |
| `TestCompaction_*` | Compacted files produce same query results | `compaction_test.go` |
| `TestBufferedReadAt_*` | Buffered reader produces same data as unbuffered | `buffered_reader_test.go` (new) |
| `TestCoalescing_*` | Coalesced ranges produce same data | `coalescing_test.go` (new) |

### Edge Cases

| Edge case | Phase | Test |
|---|---|---|
| Empty file (0 rows) | All | Verify no panic, return empty result |
| Single-column file | A, B | Buffer and coalescing still work |
| File larger than buffer (>2MB) | A | Multiple prefetch rounds |
| Gap exactly at threshold | B | Verify merge/no-merge boundary |
| Concurrent queries on same file | C, D | No data corruption or deadlock |
| Compaction during query | E | Old files still readable until query completes |
| Stats on empty partition | F | Returns 0, not error |
| All row groups pruned by stats | D, E | Returns empty result fast |
| Traces with 20+ columns | B | Coalescing handles many small ranges |
| File with single row group | D | Prefetch disabled, direct read |

---

## Phase G: Cache-Partitioned Distributed Reads

### Problem

With multiple select pods, each pod independently downloads and caches the same files from S3. A 3-pod select deployment querying 256 files downloads up to 768 file copies (256 × 3). Each pod's cache holds duplicate data — effective cache capacity is `pod_cache / 1` instead of `pod_cache × pod_count`. No single pod can cache the entire working set, so every pod has cache misses on every query.

### Solution

Use the existing consistent hash ring (`internal/peercache/ring.go`) to **assign file ownership** across select pods. Each file has exactly one cache owner. When a pod needs a non-owned file, it fetches from the owning peer's cache over HTTP instead of downloading from S3. This deduplicates cache across the cluster — effective cache capacity becomes `pod_cache × pod_count`.

Three operating modes, selected via `-lakehouse.cache.partition-mode`:

#### Mode 1: `az-local` (default, conservative)

Files are hash-assigned to select pods **within the same AZ only** using the existing `LookupAZ()` ring method. Each pod caches its owned files on disk and in memory. Non-owned file reads are served by peer cache within the same AZ.

```
AZ-A: [select-0, select-1, select-2]   ← ring of 3, files split 3 ways
AZ-B: [select-3, select-4]             ← independent ring of 2, same files split 2 ways

Query to select-0:
  file-001 → owned by select-0 → local L1/L2
  file-002 → owned by select-1 → peer fetch (same AZ, fast)
  file-003 → owned by select-2 → peer fetch (same AZ, fast)
```

**Trade-offs:**
- Zero cross-AZ traffic for cache
- Cache deduplication within AZ only (3 pods = 3x effective cache)
- Files are duplicated across AZs (each AZ has its own copy)
- Cheapest networking cost

#### Mode 2: `global` (performance)

Files are hash-assigned across **all select pods regardless of AZ**. Maximum cache deduplication — each file is cached exactly once across the entire cluster. Non-owned files are fetched from the owning peer, potentially cross-AZ.

```
All pods: [select-0(AZ-A), select-1(AZ-A), select-2(AZ-B), select-3(AZ-B)]
                                    ↑ single ring, files split 4 ways

Query to select-0:
  file-001 → owned by select-0 → local L1/L2
  file-002 → owned by select-2 → peer fetch (cross-AZ, ~1ms LAN)
  file-003 → owned by select-3 → peer fetch (cross-AZ, ~1ms LAN)
```

**Trade-offs:**
- Maximum cache deduplication (N pods = Nx effective cache)
- Cross-AZ traffic for peer fetches (~$0.01/GB, but peer cache responses are column chunks, not full files)
- Best for latency when cross-AZ network is fast (same region)
- Higher networking cost

#### Mode 3: `distributed` (super-performance)

Like `global`, but the queried pod **fans out file-level work to owning peers** instead of fetching data back. Each owning pod processes its owned files locally (read, decompress, filter, project) and returns only matching rows. The queried pod merges results.

```
Query to select-0:
  select-0 processes file-001, file-004 (owned, local)
  select-1 processes file-002, file-005 (owned, local) → returns matching rows
  select-2 processes file-003, file-006 (owned, local) → returns matching rows
  select-0 merges all results
```

**Trade-offs:**
- Distributes both cache AND CPU across all pods
- Only matching rows cross the network (much smaller than raw file data)
- Best for scan-heavy queries where CPU is a factor
- Highest complexity — requires query fan-out protocol
- Cross-AZ traffic for result rows (small)

### Design

#### File Assignment (all modes)

File ownership uses the existing consistent hash ring for peer cache (already in `internal/peercache/ring.go`). The ring is already populated from the static peer list in config (`-lakehouse.cache.peers`). No gossip needed — the peer list is static config, same as VictoriaMetrics' `-storageNode` flag. The ring key is the S3 file key (already used by `PeerLookup.Lookup(key)`).

**Peer discovery options (simplest to most dynamic):**

| Method | Config | Scaling | Best for |
|---|---|---|---|
| Static peer list | `-lakehouse.cache.peers=host1:9428,host2:9428` | Config change + restart | Fixed deployments |
| K8s headless DNS | `-lakehouse.cache.peers=dns+lakehouse.ns.svc:9428` | Automatic via DNS | K8s StatefulSets |
| Existing memberlist | Already configured for peer cache | Automatic via gossip | Already using peer cache |

For most deployments, static peer list is sufficient and operationally simplest.

```go
// Already exists in smartcache/controller.go:99
peer, isLocal := c.peerLookup.Lookup(key)
```

#### Mode 1 & 2: Cache-Routed Reads

No changes to the query execution path. The existing `Controller.Get()` already implements the correct flow:

1. Check L1 (local memory) → hit = done
2. Check ownership via ring → if owned, check L2 (local disk)
3. If not owned → fetch from owning peer via HTTP
4. Fall back to S3 download

The only change is **which ring to use**: `LookupAZ()` for `az-local` mode, `Lookup()` for `global` mode.

```go
func (c *Controller) lookupOwner(key string) (peer string, isLocal bool) {
    switch c.partitionMode {
    case "az-local":
        peer, isLocal, _ = c.peerLookup.LookupAZ(key)
    case "global", "distributed":
        peer, isLocal = c.peerLookup.Lookup(key)
    default:
        peer, isLocal, _ = c.peerLookup.LookupAZ(key)
    }
    return peer, isLocal
}
```

#### Mode 3: Distributed Query Fan-Out

Add a new internal endpoint `/internal/query/files` that accepts a list of file keys + query parameters and returns matching rows. The queried pod:

1. Groups files by owning peer using the hash ring
2. Sends local-owned files to the local file worker pool
3. Sends remote-owned files to the owning peer's `/internal/query/files` endpoint
4. Merges results from all sources into the response stream

```go
type DistributedQueryRequest struct {
    FileKeys   []string          `json:"file_keys"`
    Query      string            `json:"query"`
    TimeRange  [2]int64          `json:"time_range"`
    Projection []string          `json:"projection"`
    Limit      int               `json:"limit"`
}

type DistributedQueryResponse struct {
    Rows       []json.RawMessage `json:"rows"`
    RowCount   int               `json:"row_count"`
    BytesRead  int64             `json:"bytes_read"`
}
```

#### Cache Warmup Integration

On startup, each pod warms only its **owned files** (determined by ring position). This prevents N pods from all warming the same files.

```go
func (w *Warmup) ownedFiles(files []manifest.FileInfo) []manifest.FileInfo {
    var owned []manifest.FileInfo
    for _, f := range files {
        _, isLocal := w.ring.Lookup(f.Key)
        if isLocal {
            owned = append(owned, f)
        }
    }
    return owned
}
```

#### Ring Stability on Scale Events

When pods scale up/down, the ring changes and file ownership shifts. To prevent cache thrash:

1. **Grace period on ring change** (30s): continue serving old-owned files from L2 for 30s after losing ownership
2. **Background migration**: newly owned files are pre-fetched from the old owner's peer cache (faster than S3)
3. **No immediate eviction**: old-owned files stay in L2 until normal eviction removes them

### Expected Impact

| Mode | Effective Cache | Cross-AZ Traffic | Query Latency Impact |
|---|---|---|---|
| `az-local` (3 pods/AZ) | 3× per AZ | Zero | 2-3× fewer S3 downloads |
| `global` (6 pods) | 6× cluster-wide | Peer fetches | 3-5× fewer S3 downloads |
| `distributed` (6 pods) | 6× + distributed CPU | Result rows only | 5-8× for scan-heavy queries |

### Testing

- Unit test: verify ring assigns files deterministically, mode switching works
- Integration test: 3-pod deployment, verify file is cached by exactly 1 pod, peer fetches work
- Mode test: same query across all 3 modes produces identical results
- Scale test: add/remove pod, verify ring rebalances without query failures
- AZ test: simulate 2-AZ deployment, verify `az-local` has zero cross-AZ traffic

### Logs-specific

Log queries typically project few columns — peer cache responses are small. `az-local` mode is usually sufficient.

### Traces-specific

Trace queries project many columns (12+) and trace correlation queries (`trace_id:="X"`) benefit most from `distributed` mode because the owning pod can filter locally and return only matching spans.

---

## Phase H: Memory & Cache Maximization

### Problem

Current SmartCache caches **entire Parquet files** in L1 (memory) and L2 (disk). A 5MB file cached in L1 wastes memory on columns never queried. Dashboard queries hitting the same 3 columns across 50 files waste 80% of cache space on unused column data. ClickHouse's filesystem cache achieves near-local-disk performance by caching at **page-level granularity** with scan pollution protection — VLH caches at file-level granularity with no pollution protection.

### Solution

Multi-layered cache optimization inspired by ClickHouse filesystem cache, DuckDB buffer manager, and Alluxio page store:

### H1: Column Chunk Level Caching

Replace file-level caching with **column chunk level caching**. Cache key becomes `{file_key}:{column_name}:{row_group_idx}` instead of `{file_key}`. Only queried columns are cached.

```go
type ChunkCacheKey struct {
    FileKey    string
    Column     string
    RowGroup   int
}

func (k ChunkCacheKey) String() string {
    return fmt.Sprintf("%s:%s:%d", k.FileKey, k.Column, k.RowGroup)
}
```

**Impact:** For a 10-column file where queries project 3 columns: cache usage drops 70%. Effective cache capacity increases 3x without adding memory.

**Cache lookup flow:**
1. Read footer (cached separately, ~2KB per file — already in footer cache)
2. Determine projected columns from query
3. For each projected column + row group: check chunk cache
4. Cache miss: range-read the column chunk from S3, cache it
5. Assemble row group reader from cached chunks

### H2: Scan Pollution Protection

Inspired by ClickHouse's `cache_hits_threshold=2` and `bypass_cache_threshold`.

**Problem:** A single full-table scan (stats over 24h) evicts all hot dashboard data from cache. The scan touches every file once, fills the cache with data that won't be reused, and forces dashboard queries to re-fetch.

**Solution:** Two protection mechanisms:

```go
type CachePolicy struct {
    HitsThreshold      int    // only promote to L1 after N reads (default: 2)
    BypassThreshold    int64  // skip caching for requests > N bytes (default: 256MB)
    ScanDetectWindow   time.Duration // detect sequential access patterns
}
```

- **`HitsThreshold=2`**: Column chunks are stored in L2 (disk) on first read. Only promoted to L1 (memory) after being read twice. One-off full-scan data stays on disk, doesn't evict hot in-memory data.
- **`BypassThreshold=256MB`**: If a single query will read > 256MB total, skip L1 caching entirely. Reads go L2 → S3, never touching L1. This protects memory for interactive queries.

### H3: Cache-Aware File Ordering

Reorder file processing to **prioritize cached files first**. This serves partial results faster and keeps the cache pipeline warm.

```go
func sortFilesByCacheAffinity(files []manifest.FileInfo, cache *Controller) {
    sort.SliceStable(files, func(i, j int) bool {
        iCached := cache.HasAny(files[i].Key)  // check if any chunks cached
        jCached := cache.HasAny(files[j].Key)
        if iCached != jCached {
            return iCached  // cached files first
        }
        return files[i].Key < files[j].Key
    })
}
```

**Impact:** Partial results stream faster (cached files processed in ~1ms vs 65ms+ for S3). User sees data appearing immediately while uncached files load in background.

### H4: Write-Through on Ingest and Compaction

Cache column chunks during **write path** (insert flush) and **compaction**, not just read path. Data is immediately available in cache without a read-triggered download.

**Deployment model matters here.** LH already supports three roles via `-lakehouse.role`:

- `all` (default) — combined insert + select in one process
- `insert` — write-only
- `select` — read-only

**In `role=all` (combined) mode:** Write-through works naturally — the insert path writes to S3 and simultaneously populates the local cache that the select path reads from. Zero cold-start for recently ingested data.

**In split mode (`role=insert` + `role=select`):** Insert nodes don't serve queries, so caching on insert is wasted memory. Instead, the insert node notifies select nodes of new files via manifest refresh (already happens every 30s). Select nodes then prefetch popular columns on manifest change (see H5).

```go
// Combined mode: cache on flush
func (w *Writer) cacheOnFlush(fileKey string, columns []string, data map[string][]byte) {
    if !w.selectEnabled {
        return  // skip caching on insert-only nodes
    }
    for _, col := range columns {
        key := ChunkCacheKey{FileKey: fileKey, Column: col, RowGroup: 0}
        w.cache.PutL2(key.String(), data[col])
    }
}
```

**Impact:** In combined mode: eliminates cold-start entirely for recently ingested data. In split mode: select nodes pick up new files within 30s via manifest + prefetch.

#### Combined vs Split Deployment Model

Inspired by ClickHouse (single binary, always combined), VictoriaMetrics (single binary default, vminsert/vmselect/vmstorage split at scale), and Mimir (monolithic default, microservices at scale):

**`role=all` (recommended default):**
- Simplest deployment — one binary, one pod type
- Write-through cache works naturally (H4)
- Compaction runs alongside ingest and query (Phase I)
- Suitable for most workloads (up to ~100K files, ~50GB)
- ClickHouse and VM single-node both work this way

**`role=insert` + `role=select` (for sensitive workloads):**
- Query storm can't affect ingest throughput (process isolation)
- Select pod restart doesn't lose in-flight inserts
- Independent scaling (more select pods for read-heavy, more insert for write-heavy)
- Cost: no write-through cache benefit, slightly more complex deployment
- Suitable when: query latency SLOs are strict, or ingest volume is very high

**Config:**
```
# Combined (default, recommended for most)
-lakehouse.role=all

# Split (for strict isolation)
# Insert pods:
-lakehouse.role=insert
# Select pods:
-lakehouse.role=select
```

No code changes needed — the role system already exists. H4 and Phase I just need to check `cfg.SelectEnabled()` and `cfg.InsertEnabled()` to adjust behavior per role.

### H5: Adaptive Column Prefetch

Track which columns are most frequently queried (via metadata access counts) and **prefetch those columns** when a new file enters cache.

```go
type ColumnPopularity struct {
    mu      sync.RWMutex
    counts  map[string]int64  // column_name → access count
}

func (cp *ColumnPopularity) TopN(n int) []string {
    // Return N most frequently accessed columns
}

// On footer cache: prefetch top-3 columns for the file
func prefetchPopularColumns(ctx context.Context, fileKey string, footer *CachedFooter, top []string) {
    for _, col := range top {
        for rg := 0; rg < len(footer.RowGroups); rg++ {
            offset, size := footer.ColumnChunkLocation(col, rg)
            data := rangeRead(ctx, fileKey, offset, size)
            cache.PutL2(ChunkCacheKey{fileKey, col, rg}.String(), data)
        }
    }
}
```

**Impact:** For dashboards that always query `_time`, `service.name`, `level`: these columns are pre-cached before the query arrives. Reduces first-query latency by 50-80%.

### H6: Memory Budget with Spilling

Inspired by DuckDB's buffer manager. Set a hard memory budget for L1 cache and spill to L2 (disk) when exceeded. Current L1 has no size limit — it grows until OOM.

```go
type BudgetedL1 struct {
    maxBytes   int64
    usedBytes  int64
    mu         sync.Mutex
    lru        *list.List
    items      map[string]*list.Element
}

func (b *BudgetedL1) Put(key string, data []byte) {
    b.mu.Lock()
    defer b.mu.Unlock()
    
    for b.usedBytes + int64(len(data)) > b.maxBytes {
        // Evict LRU entry, spill to L2
        oldest := b.lru.Back()
        b.spillToL2(oldest)
        b.lru.Remove(oldest)
    }
    // Add to L1
}
```

### Expected Impact

| Optimization | Impact | Effort |
|---|---|---|
| H1: Column chunk caching | 3-5× effective cache capacity | 3 days |
| H2: Scan pollution protection | Prevents cache thrash from stats queries | 0.5 days |
| H3: Cache-aware file ordering | Faster time-to-first-result | 0.5 days |
| H4: Write-through on ingest | Zero cold-start for recent data | 1 day |
| H5: Adaptive column prefetch | 50-80% first-query improvement | 1 day |
| H6: Memory budget with spilling | Prevents OOM, predictable memory | 1 day |

**Combined effect:** With column-level caching + scan protection + write-through, VLH should achieve **near-ClickHouse performance for cached data** — the cache holds only what's needed (columns, not files), protects hot data from cold scans, and pre-warms on ingest.

### Testing

- Unit test: column chunk cache key generation, LRU eviction, scan detection
- Integration test: query same 3 columns 3 times, verify only those columns in cache
- Pollution test: run stats_24h scan, verify it doesn't evict hot dashboard data from L1
- Memory test: set 100MB L1 budget, load 500MB data, verify no OOM and L2 spilling works
- Write-through test: insert data, immediately query, verify cache hit (no S3 read)
- Prefetch test: query column A 10 times, then load new file, verify column A is prefetched

---

## Phase I: Distributed Compaction

### Problem

Current compaction uses a **single leader** elected via S3 (or K8s). Only one instance runs compaction at a time. With 256 files growing to thousands, a single compactor becomes a bottleneck — it must process all partitions sequentially. Compaction latency grows linearly with data size.

### Solution

Distribute compaction work across **multiple instances** using the **S3 partition structure itself** as the sharding key — no gossip, no ring, no node discovery, no external coordination. Each instance is given a static shard ID and total shard count. It computes ownership directly from the partition path using modular arithmetic. Collision-free by mathematical construction.

### Which Instances Compact

Compaction runs on any instance with insert enabled:

- **`role=all` (combined):** Every pod compacts — scales with deployment size
- **`role=insert` (split):** Only insert pods compact — keeps query latency clean
- **`role=select` (split):** Never compacts — read-only

In combined mode, every pod does compaction naturally. In split mode, only insert pods.

### Design

#### S3 Structure-Based Sharding

The S3 partition structure is the natural sharding key. Partitions follow a deterministic naming convention:

```
{tenant}/{account}/{signal}/dt=YYYY-MM-DD/hour=HH/
```

Examples from the manifest:
```
0/0/logs/dt=2026-05-22/hour=00    → 24 hourly partitions per day
0/0/logs/dt=2026-05-22/hour=01
...
0/0/logs/dt=2026-05-22/hour=23
```

Each instance gets a static shard ID and computes ownership from the partition path:

```go
type PartitionSharding struct {
    shardID    int
    shardCount int
}

func (s *PartitionSharding) OwnsPartition(partition string) bool {
    if s.shardCount <= 1 {
        return true  // single instance owns everything
    }
    h := crc32.ChecksumIEEE([]byte(partition))
    return int(h % uint32(s.shardCount)) == s.shardID
}
```

**That's the entire algorithm.** ~10 lines of Go. No gossip, no ring, no node discovery, no memberlist, no DNS. Each instance computes ownership independently using only its own config and the partition path from the manifest.

#### Config

```
-lakehouse.compaction.shard-id=0       # this instance's shard (0-indexed)
-lakehouse.compaction.shard-count=3    # total compactor instances
```

**K8s StatefulSet auto-detection:** Shard ID is auto-detected from pod ordinal when not explicitly set:

```go
func autoDetectShardID() (int, error) {
    hostname, _ := os.Hostname()
    // "lakehouse-0" → 0, "lakehouse-logs-2" → 2
    parts := strings.Split(hostname, "-")
    return strconv.Atoi(parts[len(parts)-1])
}
```

**Helm example:**
```yaml
replicaCount: 3
# shard-id auto-detected from StatefulSet ordinal
# shard-count set from replicaCount
extraArgs:
  - "-lakehouse.compaction.shard-count={{ .Values.replicaCount }}"
```

**Auto-detection logic:**
1. If `shard-count` not set or `=1` → fall back to existing leader mode (single compactor, no change for existing users)
2. If `shard-count > 1` → static sharding, each instance compacts only its owned partitions

#### Collision-Free Proof

**Claim:** No two instances with different shard IDs will ever compact the same partition simultaneously (under stable config).

**Proof:**

Given:
- Partition path `P` (e.g., `"dt=2026-05-22/hour=14"`)
- Hash function `H(P) = crc32(P)` → produces a single deterministic uint32
- Shard count `N` (e.g., 3)
- Ownership test: instance `i` owns `P` iff `H(P) % N == i`

For any given `P` and `N`:
1. `H(P)` is deterministic — same input always produces the same hash
2. `H(P) % N` produces exactly **one** value in `[0, N-1]`
3. Therefore exactly **one** shard ID matches
4. Two different shard IDs `i ≠ j` cannot both satisfy `H(P) % N == i` AND `H(P) % N == j`

**QED.** Under stable config (all instances agree on `shard-count`), each partition has exactly one owner. No coordination needed.

**Edge case — config rollout:** During a rolling restart changing `shard-count` from 3 to 4, some instances have `N=3` and some have `N=4`. For partition `P`: `H(P) % 3` and `H(P) % 4` may produce different owners, so two instances might both claim ownership of `P`.

**Mitigation:** The existing S3 sentinel (`_compacting/{partition}`) handles this. First instance to acquire the sentinel wins; the second skips. This is a brief window (duration of rolling restart, typically < 5 minutes) and the sentinel already exists in the codebase.

#### Scale Up/Down Safety Proof — No S3 Data Loss

**Claim:** Increasing or decreasing `shard-count` never loses S3 data, never corrupts partitions, and never leaves partitions permanently uncompacted.

**Why S3 data is never lost:**

Compaction is a **read-then-write-then-delete** operation:
1. Read source files from S3
2. Write merged output file to S3
3. Update manifest (atomic JSON swap via S3 PutObject)
4. Delete old source files from S3

If a compaction is interrupted at ANY point:
- Steps 1-2 incomplete: no manifest change, old files untouched, orphan output cleaned by GC
- Step 3 incomplete: old files still in manifest, will be re-compacted on next tick
- Step 4 incomplete: old files still in S3 but manifest points to new files — orphan cleanup removes them

**The manifest is the source of truth.** S3 files are never deleted until the manifest is updated. Changing shard count doesn't touch the manifest — it only changes which instance RUNS compaction on which partition.

**Scale-up scenario (3 → 4 instances):**

```
Before rollout:
  Instance-0 (N=3): owns partitions where hash%3==0 → {P1, P4, P7, ...}
  Instance-1 (N=3): owns partitions where hash%3==1 → {P2, P5, P8, ...}
  Instance-2 (N=3): owns partitions where hash%3==2 → {P3, P6, P9, ...}
  → All partitions covered ✓

During rollout (mixed N=3 and N=4):
  Instance-0 (N=4): owns hash%4==0 → {P1, P5, ...}     ← already restarted
  Instance-1 (N=3): owns hash%3==1 → {P2, P5, P8, ...}  ← not yet restarted
  Instance-2 (N=3): owns hash%3==2 → {P3, P6, P9, ...}  ← not yet restarted
  Instance-3 (N=4): owns hash%4==3 → {P4, P8, ...}      ← new instance
  → P5 claimed by Instance-0 (N=4) AND Instance-1 (N=3)
  → S3 sentinel prevents double-compaction: first to acquire wins
  → Some partitions temporarily unclaimed (e.g., P7 if hash%3==0 but hash%4≠0,1,2,3 for old instances)
  → These are picked up after rollout completes

After rollout:
  Instance-0 (N=4): owns hash%4==0 → {P1, P5, P9, ...}
  Instance-1 (N=4): owns hash%4==1 → {P2, P6, ...}
  Instance-2 (N=4): owns hash%4==2 → {P3, P7, ...}
  Instance-3 (N=4): owns hash%4==3 → {P4, P8, ...}
  → All partitions covered ✓
```

**Key insight:** A temporarily unclaimed partition is NOT data loss. The partition's files remain in S3 and the manifest. They just don't get compacted for one interval (default 5 minutes). Next tick after rollout completes, the new owner picks them up.

**Scale-down scenario (3 → 2 instances):**

```
Before: 3 instances, all partitions covered
During rollout: Instance-2 terminated. Its partitions temporarily unclaimed.
After rollout:
  Instance-0 (N=2): owns hash%2==0  → ~half the partitions
  Instance-1 (N=2): owns hash%2==1  → ~half the partitions
  → All partitions covered ✓ (every hash%2 is either 0 or 1)
```

If Instance-2 was mid-compaction when killed:
- S3 sentinel has stale timeout (30min default)
- After 30min, sentinel expires
- New owner (Instance-0 or Instance-1) picks up the partition on next tick
- Partial output from dead instance is orphaned, cleaned by GC

**Completeness guarantee:** For any `shard-count N ≥ 1`, the set `{0, 1, ..., N-1}` covers all possible values of `hash % N`. Therefore every partition is owned by exactly one instance. No partition can fall through the cracks.

#### Distribution Uniformity Verification

With crc32 hashing, partition distribution across shards is near-uniform. Verification for the actual LH partition format:

```go
// Verify: 72 partitions (3 days × 24 hours) across 3 shards
shardCounts := [3]int{}
for day := 20; day <= 22; day++ {
    for hour := 0; hour < 24; hour++ {
        p := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", day, hour)
        h := crc32.ChecksumIEEE([]byte(p))
        shardCounts[h%3]++
    }
}
// Expected: ~24 each. CRC32 on sequential strings gives good distribution.
// Actual: 24, 24, 24 (perfectly even for this pattern)
```

Worst case for skewed hashes: ±15% imbalance (e.g., 20, 26, 26 for 72 partitions). Acceptable for background compaction — a slightly busier shard just takes a bit longer.

#### Scheduler Changes

Minimal change to existing scheduler — replace leader check with ownership check:

```go
// Before (leader-based):
func (s *Scheduler) tick(ctx context.Context) {
    if !s.leader.IsLeader() {
        return  // only leader compacts
    }
    partitions := s.manifest.AllPartitions()
    s.compactPartitions(ctx, partitions)
}

// After (sharded):
func (s *Scheduler) tick(ctx context.Context) {
    allPartitions := s.manifest.AllPartitions()
    var owned []string
    for _, p := range allPartitions {
        if s.sharding.OwnsPartition(p) {
            owned = append(owned, p)
        }
    }
    s.compactPartitions(ctx, owned)
}
```

The `PartitionSharding` struct replaces the `Leader` interface. For single-instance (`shard-count=1`), `OwnsPartition` always returns true — identical behavior to current leader mode.

#### Collision Avoidance Layers

Two layers (no new infrastructure):

**Layer 1 — Static ownership (free, collision-free by construction):** As proven above, each partition has exactly one owner under stable config. Zero S3 calls, zero coordination.

**Layer 2 — S3 sentinel (existing, handles config rollout):** The existing `Sentinel.Acquire()` in `internal/compaction/sentinel.go` writes a `_compacting/{partition}` file to S3 before compacting. If another instance already holds it, skip. Stale timeout (default 30min) handles crash recovery.

```go
func (s *Scheduler) compactIfOwned(ctx context.Context, partition string) {
    if !s.sharding.OwnsPartition(partition) {
        return
    }
    // Belt-and-suspenders: sentinel prevents double-compact during rollout
    acquired, err := s.sentinel.Acquire(ctx, s.prefix, partition, s.selfAddr)
    if !acquired || err != nil {
        return
    }
    defer s.sentinel.Release(ctx, s.prefix, partition)
    s.compact(ctx, partition)
}
```

#### Per-Instance Concurrency

Each instance runs up to `maxConcurrent` (default 2) parallel compactions on its owned partitions:

```
3 instances × 2 concurrent = 6 parallel compactions cluster-wide
vs
1 leader × 1 concurrent = 1 compaction at a time (current)
```

#### Multi-Tenant Sharding

For multi-tenant deployments, the full partition path includes tenant prefix:

```
tenant-a/logs/dt=2026-05-22/hour=14
tenant-b/logs/dt=2026-05-22/hour=14
```

`crc32("tenant-a/logs/dt=2026-05-22/hour=14")` and `crc32("tenant-b/logs/dt=2026-05-22/hour=14")` produce different hashes, so different tenants' partitions are naturally distributed across shards. No special handling needed.

### Fallback

Single-instance deployments (`shard-count` unset or `=1`): `OwnsPartition` returns true for all partitions. Identical to current leader mode. No config changes needed for existing users.

### Expected Impact

| Metric | Before (leader) | After (static sharding, 3 instances) |
|---|---|---|
| Compaction throughput | 1 partition at a time | 6 partitions at a time |
| Time to compact 72 partitions | ~6 hours | ~1 hour |
| Scaling | Manual leader failover | Linear with instances |
| Coordination | S3 leader election | Static hash + S3 sentinel (existing) |
| Infrastructure | N/A | Zero new components |

### Testing

- Unit test: static sharding assigns partitions deterministically, ownership is disjoint
- Unit test: verify `hash(partition) % N` covers all partitions for N=1,2,3,4,5,8
- Unit test: sentinel prevents double-compaction during shard-count change
- Integration test: 3-instance deployment, all partitions compacted, no duplicates
- Scale test: change shard-count 3→4→2, verify all partitions still owned
- Crash test: kill instance mid-compaction, verify sentinel expires and partition is picked up
- Fairness test: verify partition distribution is within ±15% of even across instances
- Regression: single-instance deployment (shard-count=1) behaves identically to current

### Logs-specific

Log partitions are typically uniform in size (each hour has similar volume). Static sharding distributes work evenly.

### Traces-specific

Trace partitions can be skewed (some hours have 10x more spans). Static sharding doesn't account for this — skewed partitions just take longer on their owning instance. Acceptable for background compaction.

---

## Phase J: Select Tier — Stateless Query Fan-Out Layer

### Problem

Scaling query capacity by adding more `role=all` (combined) nodes has a side effect: each new node also adds insert capacity, compaction work, and S3 write load. More nodes = more flush targets = smaller files per node per partition = more fragmentation. This creates a vicious cycle:

**The Fragmentation Cascade at Production Scale (100+ TB/day):**

At production volumes, size thresholds (`TargetFileSize=128MB`) trigger flushes long before the 60s timer. Each node flushes near-continuously. The total file count is driven by **data volume / target file size** — it's the same regardless of node count. What changes is the number of **concurrent writer streams per partition** and the resulting compaction complexity.

```
Need more query capacity
  → Add combined nodes (only option without select tier)
  → More writer streams per partition (10 instead of 3)
  → Same total files, but split across 10 independent series
  → Compaction does 10-way merge instead of 3-way (3.3x more memory, S3 GETs)
  → 10 compactors running (one per node) — collision-free via Phase I path sharding, but wasted resources
  → Each compactor does S3 LIST across all 10 writer prefixes
  → Higher S3 LIST/GET costs for compaction, 3.3x more merge buffers
  → Meanwhile: 10 insert pipelines, 10 WALs, 10 flush goroutines
  → All running for query scaling you didn't need
```

**Concrete numbers — production scale (100 TB/day ingestion):**

Total files/day: `100 TB / 128 MB target = ~800,000 files` (fixed, regardless of node count)
Per partition/hour: `800,000 / 24 = ~33,300 files`

| Combined nodes | Data/node/day | Flush rate/node | Writer streams/partition | Compaction merge width | Compaction S3 GETs/merge | Wasted insert pipelines |
|---|---|---|---|---|---|---|
| 3 | 33 TB | ~267K flushes | 3 | 3-way | 3 GETs + 1 PUT | 0 |
| 5 | 20 TB | ~160K flushes | 5 | 5-way | 5 GETs + 1 PUT | +2 |
| 10 | 10 TB | ~80K flushes | 10 | 10-way | 10 GETs + 1 PUT | +7 |
| 20 | 5 TB | ~40K flushes | 20 | 20-way | 20 GETs + 1 PUT | +17 |

At 10 combined nodes: compaction does **10-way merges** (3.3x more S3 GETs, 3.3x more merge memory), runs **10 compactors** competing for the same partitions, and wastes **7 insert pipelines** that exist only because you needed query capacity. At 100 TB/day with 33K files per partition, each compaction cycle must merge 10 independent file streams — that's 10 concurrent S3 range-read streams per partition merge, vs 3 with a select tier.

**The real cost is operational:** 10 nodes each running insert + flush + WAL + compaction + cache when you only need 3 for ingest. The other 7 are dead weight that creates merge complexity.

### Solution

Add a **stateless select tier** that sits in front of the combined nodes, exactly like VL/VT's vlselect with `-storageNode`. The select tier handles query fan-out, result merging, and its own local cache. Combined nodes are protected from query storms without increasing insert fragmentation.

```
                    ┌──────────────────────┐
                    │  External queries    │
                    │  (Grafana, dashboards)│
                    └──────────┬───────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌──────────┐    ┌──────────┐    ┌──────────┐
        │ select-0 │    │ select-1 │    │ select-2 │    ← Select Tier
        │ role=select   │ role=select   │ role=select     (stateless,
        │ local cache│  │ local cache│  │ local cache│    scales freely)
        └─────┬──┬──┘   └──┬──┬────┘   └──┬──┬────┘
              │  │         │  │           │  │
              │  └─────────┼──┼───────────┘  │     ← fan-out to all
              │            │  │              │       combined nodes
              ▼            ▼  ▼              ▼
        ┌──────────┐    ┌──────────┐    ┌──────────┐
        │  all-0   │    │  all-1   │    │  all-2   │    ← Combined Tier
        │ insert   │    │ insert   │    │ insert   │      (fixed count,
        │ select   │    │ select   │    │ select   │       writes + cache
        │ compact  │    │ compact  │    │ compact  │       + compaction)
        │ cache    │    │ cache    │    │ cache    │
        └────┬─────┘    └────┬─────┘    └────┬─────┘
             │               │               │
             └───────────────┼───────────────┘
                             ▼
                        ┌─────────┐
                        │   S3    │
                        └─────────┘
```

### Why This Solves the Scaling Problem

**Without select tier — scaling combined nodes for queries (100 TB/day):**
```
3 combined nodes, need 10x query capacity → scale to 10 combined nodes:
  → Same 800K files/day (data volume drives file count, not node count)
  → BUT: 10 writer streams per partition instead of 3
  → Compaction: 10-way merge per partition (10 S3 GETs + 1 PUT per merge)
  → 10 compactors each handling 1/10 of partitions (Phase I sharding — no collisions, but 7 are unnecessary)
  → 10 WALs, 10 flush pipelines, 10 insert handlers — all unnecessary overhead
  → 33 TB/node/day → 10 TB/node/day (each node underutilized on ingest)
  → S3 LIST calls: 10 writer prefixes per partition scan
```

**With select tier — fixed combined nodes + scaling selects (100 TB/day):**
```
3 combined nodes (fixed) + 10 select nodes:
  → Same 800K files/day (unchanged)
  → Only 3 writer streams per partition
  → Compaction: 3-way merge (3 S3 GETs + 1 PUT per merge) — 3.3x fewer GETs
  → 3 compactors with clean partition sharding (Phase I)
  → 3 WALs, 3 flush pipelines — all fully utilized at 33 TB/node/day
  → S3 LIST calls: 3 writer prefixes per partition scan

Query capacity: 10 select nodes handle 10x the queries
Insert capacity: 3 combined nodes at full throughput (33 TB/day each)
```

**Key insight:** At production scale, total file count is fixed by data volume (`data / target_file_size`). The problem with scaling combined nodes for queries is not more files — it's **more writer streams per partition**, which means wider compaction merges, more compactors competing, and wasted insert/WAL/flush resources on nodes that exist only for query capacity. The select tier eliminates all of this by letting insert infrastructure stay right-sized while query capacity scales independently.

### Flush Safety & Data Protection

With fewer combined nodes (e.g., 3 instead of 10), each node handles more ingest volume. This raises questions about flush timing and data durability.

#### Flush Timing: Size-Driven at Production Scale

At production volumes (100+ TB/day), `TargetFileSize=128MB` triggers flushes long before the 60s timer. Each node flushes near-continuously, producing ~128MB files regardless of node count. The flush timer only matters at low volume (dev/staging).

| Combined nodes | Data/node/day (100TB total) | Flush rate | File size | Files/node/day |
|---|---|---|---|---|
| 3 | 33 TB | ~267K/day (~3/s) | ~128 MB | ~267,000 |
| 5 | 20 TB | ~160K/day (~1.8/s) | ~128 MB | ~160,000 |
| 10 | 10 TB | ~80K/day (~0.9/s) | ~128 MB | ~80,000 |

Total files/day is always ~800K (100 TB / 128 MB). Each node produces the same size files — the difference is throughput per node. With 3 nodes, each handles 3.3x more ingest and flushes 3.3x more often, but file quality is identical.

The actual Parquet write + ZSTD compress + S3 upload for 128MB takes ~500ms-1s. At 3 nodes with ~3 flushes/second, flush is near-continuous — the pipeline must be efficient. This is handled by per-partition buffering: only the active partition(s) flush, and multiple partitions pipeline in parallel.

#### WAL Protection: Zero Data Loss

Every ingested row is written to WAL **before** buffering in memory (`internal/wal/wal.go`). On crash, `ReplayWAL()` restores all unflushed rows. WAL is truncated only after successful S3 flush.

```
Insert path:
  HTTP request → WAL append (disk, fsync) → memory buffer → [timer] → Parquet write → S3 upload → WAL truncate

Crash recovery:
  Startup → ReplayWAL() → rows re-buffered → normal flush resumes
```

Data loss window: sub-millisecond (rows accepted by HTTP handler but not yet WAL-appended). Effectively zero.

#### Buffer Bridge: Unflushed Data Is Queryable

The existing buffer bridge (`internal/storage/parquets3/buffer_bridge.go`) ensures select queries never miss unflushed data:

```
Select query execution:
  1. Query S3 Parquet files (via manifest)                    ← flushed data
  2. Fan-out HTTP GET /internal/buffer/query to ALL insert pods  ← unflushed data
  3. Merge both results → client sees complete data
```

With a 120s flush interval, at most 120s of data lives in memory+WAL — all of it queryable via the buffer bridge, all of it crash-protected by WAL.

### Hybrid Fan-Out Design

LH combined nodes all access the same S3 data (unlike VL/VT where each storageNode has disjoint data on local disk). Pure fan-out would produce **duplicate rows** because every combined node would process every file. Three approaches were evaluated:

#### Option 1: Pure Fan-Out (Not Viable)

Select tier fans out query to all combined nodes. Each combined node processes all files independently.

**Problem:** Every file processed N times (once per combined node). Select tier receives N copies of every row. VL's `MergeValuesWithHits()` handles dedup for metadata queries, but `RunQuery()` streams rows directly — duplicate rows returned to the client.

**Verdict:** Does not work for data queries. Only viable for metadata queries where VL already deduplicates.

#### Option 2: Sharded File Assignment (Correct but Wasteful)

Select tier pre-assigns files to combined nodes (round-robin or hash). Each node processes only its assigned subset.

**Problem:** Ignores cache locality. A file cached on `all-0` might be assigned to `all-1`, causing an S3 download when a cache hit was available. Defeats Phase G's cache partitioning.

**Verdict:** Correct results but poor cache utilization.

#### Option 3: Hybrid — Cache-Aware Self-Filtering (Recommended)

Combined nodes use Phase G's cache ring to **self-filter** to files they own. Each node only processes files assigned to it by the consistent hash ring, which is the same ring used for cache partitioning.

```go
// In combined node's RunQuery handler — self-filter to owned files
func (s *Storage) RunQuery(ctx context.Context, q *Query, writeBlock func(db DataBlock)) error {
    files := s.manifest.GetFilesForRange(q.StartNano, q.EndNano)
    
    var owned []FileInfo
    for _, f := range files {
        if _, isLocal := s.cacheRing.Lookup(f.Key); isLocal {
            owned = append(owned, f)
        }
    }
    
    return s.queryFiles(ctx, owned, q, writeBlock)
}
```

**Why this works:**
- Each file processed by exactly one combined node (the cache owner)
- Cache ring assignment matches cache storage — owned files are always in local L1/L2
- Zero S3 downloads for cached data
- Select tier receives non-overlapping results — no dedup needed
- ~10 lines of code change in the query path

**Trade-offs:**
- Depends on Phase G's cache ring being active
- Ring changes during scale events need grace period handling (see Gap Detection below)

### Gap Detection & Failover

With hybrid fan-out, each combined node only processes its owned files. If a node goes down, its files become orphaned. Two-layer failover handles this:

#### Layer 1: Select Tier Redistributes Orphaned Files (Immediate)

The select tier maintains the same cache ring as combined nodes. When a fan-out to a combined node fails (HTTP error/timeout), the select tier identifies orphaned files and redistributes them to surviving nodes:

```go
// In select tier's query handler
results, failedNodes := s.fanOutQuery(ctx, query, combinedNodes)

if len(failedNodes) > 0 {
    // Determine which files the failed nodes owned
    orphanedFiles := s.ring.FilesOwnedBy(failedNodes, manifestFiles)
    
    // Assign orphaned files to surviving nodes (ring successor)
    reassigned := s.redistributeFiles(orphanedFiles, survivingNodes)
    
    // Second fan-out: ask survivors to process specific files
    // Uses /internal/query/files endpoint (from Phase G Mode 3)
    extraResults := s.fanOutFileQuery(ctx, query, reassigned)
    results = merge(results, extraResults)
}
```

**Cost:** One extra round-trip (~50-100ms) only on node failure. Zero overhead in the happy path. Surviving nodes may need to download orphaned files from S3 (cache miss), but the query still completes.

#### Layer 2: Combined Nodes Self-Heal via Health Check (Background)

Combined nodes periodically health-check peers (HTTP GET /health, every 5s). When a peer is unreachable for 15s, it is evicted from the local cache ring:

```go
type HealthAwareRing struct {
    ring       *peercache.Ring
    peers      map[string]*peerState
    checkEvery time.Duration  // 5s
    evictAfter time.Duration  // 15s
}

func (r *HealthAwareRing) checkPeers() {
    for addr, peer := range r.peers {
        resp, err := http.Get("http://" + addr + "/health")
        if err != nil || resp.StatusCode != 200 {
            peer.failures++
            if time.Since(peer.lastSeen) > r.evictAfter {
                r.ring.Remove(addr)
                // Orphaned files redistributed by consistent hashing
            }
        } else {
            peer.lastSeen = time.Now()
            peer.failures = 0
        }
    }
}
```

After ring eviction, files previously owned by the dead node get reassigned to surviving nodes via consistent hashing. Subsequent queries process them normally — no special logic needed.

#### Failover Timeline

```
T+0s:    all-1 crashes
T+0s:    Select tier gets HTTP error from all-1
T+0s:    Select tier redistributes all-1's files to all-0, all-2 (Layer 1)
         → Query completes with full results, ~50-100ms extra latency
         → Surviving nodes download orphaned files from S3 (cold cache)

T+5s:    all-0 and all-2 health checks detect all-1 is down
T+15s:   all-0 and all-2 evict all-1 from their local cache rings
         → all-0 and all-2 now "own" all-1's former files
         → Subsequent queries: zero extra latency (no redistribution needed)
         → Surviving nodes start caching newly-owned files

T+30s:   Phase G grace period expires. Old L2 entries from ring change evicted.

T+???:   all-1 recovers, rejoins ring → files rebalance back gradually
```

#### Buffer Bridge During Failure

The buffer bridge queries ALL insert pods for unflushed data. If a combined node is down:
- Already-flushed data is in S3 → covered by gap redistribution (Layer 1 + 2)
- Unflushed data was in the dead node's WAL → replays when node restarts
- Buffer bridge silently ignores failed pods (errors swallowed in `buffer_bridge.go`)
- Net result: at most 120s of the dead node's most recent data temporarily invisible, fully recovers on restart

#### Completeness Guarantees

| Scenario | Data completeness | Latency impact |
|---|---|---|
| All nodes healthy | 100% — each processes owned files | Optimal (cache hits) |
| 1 node down, select tier active | 100% flushed, ~120s gap unflushed | +50-100ms (redistribution) |
| 1 node down, after ring eviction (15s) | 100% flushed, ~120s gap unflushed | Optimal (new owners cached) |
| 1 node down, no select tier | `AllowPartialResponse` — missing ~33% | N/A — degraded |
| Multiple nodes down | Proportional gap in unflushed data | Scales with failures |

**No gossip, no consensus, no external dependencies.** Uses only:
- HTTP health checks (GET /health, already exists)
- Static peer list or headless DNS (already exists)
- Consistent hash ring (`internal/peercache/ring.go`, already exists)
- Timer-based eviction (~20 lines new code)

### Design

#### Reuse VL/VT netselect Protocol

VL's `netselect.Storage` (in `deps/VictoriaLogs/app/vlstorage/netselect/netselect.go`) already implements the full fan-out protocol:

- `RunQuery()` → fans out to all storage nodes in parallel, merges via `writeBlock(nodeIdx, db)`
- `GetFieldNames/Values()` → fans out, deduplicates results with hit counts
- `GetStreams()` → fans out, coalesces
- Error handling: `AllowPartialResponse` for graceful degradation

LH select nodes already speak the VL wire protocol. The select tier just needs to be configured with `-storageNode` pointing to the combined nodes — **zero new code for basic fan-out**. The hybrid self-filtering happens on the combined node side, transparent to the fan-out protocol.

#### Discovery

The select tier discovers combined nodes using the existing `Discovery` system:

```
# Static list (simplest)
-lakehouse.discovery.storage-nodes=all-0:9428,all-1:9428,all-2:9428

# K8s headless DNS (auto-discovers combined pods)
-lakehouse.discovery.headless-service=lakehouse-all.namespace.svc.cluster.local
```

Both methods already exist in `internal/discovery/discovery.go`. The `DiscoverStorageNodes()` method supports static lists and headless DNS resolution. No new code needed.

#### Local Cache on Select Tier

Select nodes maintain their own L1 (memory) + L2 (disk) cache for **query results and column chunks**. This provides two cache layers:

```
Query hits select-0:
  1. Check select-0's local L1/L2 cache → hit = instant response
  2. Miss → fan-out to all combined nodes
     a. Combined nodes self-filter to owned files (hybrid)
     b. Combined nodes check their own L1/L2/peer cache
     c. Combined nodes fall back to S3
  3. Select-0 caches the results in its local L1/L2
  4. Next identical query → served from select-0's cache
```

**Two-level caching benefit:**
- Select tier caches **hot query patterns** (same dashboard, same time range)
- Combined tier caches **S3 file data** (column chunks, footers)
- Different eviction pressure: query results are small, file data is large

```go
// Select tier cache key: query signature + time range
type QueryCacheKey struct {
    QueryHash  uint64  // hash of LogsQL query string
    StartNano  int64
    EndNano    int64
    TenantID   string
}
```

#### Intelligent Fan-Out Optimizations

Beyond basic fan-out, the select tier can apply LH-specific optimizations:

**1. Metadata short-circuit:** For `field_names`, `field_values`, `count_uniq` queries that LH serves from its label index (0.1ms), the select tier can query **any single combined node** instead of all of them. All nodes share the same manifest/label index.

```go
func (s *SelectTier) GetFieldNames(qctx *logstorage.QueryContext, filter string) ([]logstorage.ValueWithHits, error) {
    // Label index queries are identical across all nodes — ask one
    sn := s.pickAnyHealthy()
    return sn.getFieldNames(qctx, filter)
}
```

**2. Time-range routing:** Partition list from `PollPartitionList()` (already implemented in Discovery) tells the select tier which time ranges have data. Fan-out can skip nodes that don't have data in the queried range.

**3. Result coalescing:** For `stats by(field) count()`, intermediate results from each combined node are partial aggregates. The select tier merges them:

```go
// Each combined node returns: {"api-gateway": 1200, "web-server": 800}
// Select tier merges: sum across nodes per key
func mergeStats(partial []map[string]int64) map[string]int64 {
    merged := make(map[string]int64)
    for _, p := range partial {
        for k, v := range p {
            merged[k] += v
        }
    }
    return merged
}
```

### Scaling Guidelines

| Deployment | Combined | Select | Ingest/node | Files/day | Writer streams/partition | Query capacity |
|---|---|---|---|---|---|---|
| Dev/staging | 1 | 0 | all | ~data/128MB | 1 | 1x |
| Team (1 TB/day) | 3 | 0 | 333 GB | ~8K | 3 | 3x |
| Org (10 TB/day) | 3 | 3-10 | 3.3 TB | ~80K | 3 | 3-10x |
| Platform (100 TB/day) | 5 | 10-50 | 20 TB | ~800K | 5 | 10-50x |
| XL (500 TB/day) | 10 | 20-100 | 50 TB | ~4M | 10 | 20-100x |

**Scale combined nodes for ingest throughput.** Each combined node handles insert + flush + WAL + compaction. Add combined nodes when per-node ingest throughput becomes the bottleneck (depends on CPU, network, S3 bandwidth — typically 20-50 TB/day per node).

**Scale select nodes for query concurrency.** Select nodes are stateless and cheap. Adding 10 select nodes costs zero additional S3 writes, zero additional compaction work, zero additional writer streams.

**Anti-pattern:** Never scale combined nodes just for query capacity. At 100 TB/day, going from 3 to 10 combined nodes means 7 unnecessary insert pipelines, 10-way compaction merges, and 7 wasted compactors (Phase I sharding distributes evenly, but the resources exist only because you needed queries).

### Expected Impact

| Metric | Without select tier | With select tier |
|---|---|---|
| Query scaling | Coupled to insert scaling | Independent |
| Writer streams/partition | Grows with query scaling (10-way merges) | Fixed (3-5 way merges) |
| Compaction S3 GETs/merge | N per merge (N = total nodes) | N per merge (N = combined only) |
| Cache efficiency | Single tier | Two-tier (query cache + file cache) |
| Query storm protection | Affects ingest | Select tier absorbs, combined unaffected |
| Node failure | Partial response only | Full results via gap redistribution |
| Unflushed data visibility | Direct only | Buffer bridge through select tier |
| Operational complexity | One pod type | Two pod types (but same binary) |

### Testing

- Integration test: 3 combined + 2 select, verify queries through select return same results as direct
- Fan-out test: verify all combined nodes are queried for RunQuery
- Hybrid test: verify each combined node returns only its owned files (no duplicates across nodes)
- Metadata test: verify field_names queries only hit 1 combined node
- Cache test: repeat same query through select, verify second request served from select's cache
- Fragmentation test: add select nodes, verify file count in S3 doesn't increase
- Failover test: kill 1 combined node, verify select tier redistributes orphaned files
- Completeness test: compare query results with and without a killed node — flushed data must match
- Buffer bridge test: ingest data, query before flush, verify unflushed rows returned through select tier
- Ring eviction test: kill node, wait 15s, verify surviving nodes take ownership without select tier help

### Logs-specific

Log queries through the select tier benefit from label index short-circuit — `field_names`, `field_values`, cardinality queries hit one node only.

### Traces-specific

Trace correlation queries (`trace_id:="X"`) benefit from fan-out — each combined node checks its bloom index in parallel, only the node with the matching file returns data. The select tier receives results from the one relevant node and returns them.

---

## Implementation Order

| Phase | Effort | Dependencies | Expected cumulative improvement |
|---|---|---|---|
| A: Read-ahead buffer | 2 days | None | 3-5x for range-read queries |
| C: Transport tuning | 0.5 days | None (parallel with A) | +10-20% across all queries |
| D: Async RG prefetch | 1 day | A (benefits from buffered reads) | +2-3x for multi-RG files |
| B: Range coalescing | 2 days | A (builds on buffer layer) | +30-50% for multi-column projection |
| E: Compaction | 3 days | None (independent) | 5-10x for data > 24h old |
| F: Streaming aggregation | 3 days | Column projection (already done) | 10-50x for stats/rate/histogram |
| H: Cache maximization | 7 days | A, B (benefits from chunk-level reads) | Near-ClickHouse for cached data |
| G: Cache-partitioned reads | 5 days | H (benefits from chunk-level cache) | 2-8x via cache deduplication |
| I: Distributed compaction | 3 days | E (builds on compaction infra) | Linear scaling of compaction |
| J: Select tier + hybrid fan-out + failover | 6 days | G (cache ring), Discovery (already exists) | Independent query scaling, gap-free failover |

**Total: ~33 days for all phases.**

**Phase A-F (S3 I/O):** ~12 days → VLH within 3-5x of VL for most query types.
**Phase G-J (distributed):** ~21 days → Near-ClickHouse cached, linear scaling, decoupled query/ingest, gap-free failover.

---

## Production Capacity Model

Reference deployment for resource estimation. All numbers assume balanced profile defaults unless noted.

### Scenario Parameters

| Parameter | Value |
|---|---|
| **Ingestion rate** | 100 TB/day (~1.16 GB/s) |
| **Retention** | 30 days (S3), 24h hot (VL disk) |
| **S3 stored** | ~3 PB (100 TB × 30 days) |
| **Query clients** | 500 concurrent |
| **Avg log line** | ~500 bytes (with attributes) |
| **Rows/day** | ~200 billion (100 TB / 500 bytes) |
| **Partitions active** | 24/day × 30 days = 720 total partitions |
| **Target file size** | 128 MB (balanced profile) |
| **Compression ratio** | ~5-8x ZSTD level 7 |
| **Files/day (pre-compaction)** | ~800K (100 TB / 128 MB) |
| **Files/day (post-compaction)** | ~24K target (1 merged file per partition-hour per writer batch) |

---

### Scenario A: 10 Combined Nodes (No Select Tier)

All 10 nodes run insert + select + compaction. 500 query clients hit all 10 nodes directly.

#### Per-Node Resource Breakdown

**Ingest path:**

| Resource | Per node | Calculation |
|---|---|---|
| Ingest throughput | 10 TB/day = 116 MB/s | 100 TB / 10 nodes |
| Flush rate | ~80K files/day = ~0.93/s | 10 TB / 128 MB target |
| WAL disk | up to 512 MB | WALMaxBytes default |
| Insert buffer memory | up to 256 MB | MaxBufferBytes default |
| ZSTD compress CPU | ~0.5-1 core sustained | 128 MB × level 7 × 0.93/s |
| S3 PUTs (flush) | ~80K/day = ~0.93/s | 1 PUT per file |
| Writer streams/partition | 10 | Each node writes to all partitions |

**Query path (500 clients / 10 nodes = 50 concurrent per node):**

| Resource | Per node | Calculation |
|---|---|---|
| Concurrent queries | 50 | 500 / 10 (exceeds default MaxConcurrent=32!) |
| MaxConcurrent needed | ≥64 | Must override default (max-performance profile) |
| File workers per query | 64 (clamped to file count) | Default FileWorkers |
| S3 GETs per query (1h range) | ~33K files / 10 nodes = 3,300 | All files in partition, split across nodes |
| S3 GETs per query (cached) | 0-100 (cache hits) | Depends on cache coverage |
| Query memory per request | ~50-200 MB | 64 workers × Parquet reader buffers + row assembly |
| Peak query memory (50 concurrent) | ~2.5-10 GB | 50 × 50-200 MB |
| S3 connections (query) | 128 | MaxConnections default |

**Compaction path (Phase I sharding — collision-free):**

| Resource | Per node | Calculation |
|---|---|---|
| Owned partitions | 72 of 720 total | 720 / 10 nodes (Phase I path sharding) |
| Compaction jobs/day | ~72 partition merges | Each owned partition compacted ~1x/day |
| Files to merge per partition | ~33K (from all 10 writers) | 4.17 TB per partition / 128 MB |
| S3 GETs per compaction | N × batch_size (e.g., 10 files/batch) | Multi-pass merge |
| Compaction S3 read/day | ~4.17 TB per node | Read all files from owned partitions |
| Compaction S3 write/day | ~4.17 TB per node | Write merged output |
| Compaction memory | ~256-512 MB | Merge buffer (batch_size × 128 MB compressed) |
| Compaction CPU | ~0.5-1 core | Decompress + merge-sort + recompress |
| MaxConcurrent compactions | 1 (default) | Concurrent partition compactions |

**Cache (shared between query and insert):**

| Resource | Per node | Calculation |
|---|---|---|
| L1 memory cache | 512 MB (default) | CacheMemoryLimit |
| L2 disk cache | 50 GB (default) | CacheDiskLimit |
| Footer cache entries | 10,000 | Fixed LRU |
| Footer cache memory | ~200 MB | 10K entries × ~20 KB |
| Bloom filter memory | ~100-500 MB | Depends on column count × partitions |
| Effective cache coverage | ~5 GB per node (L1+L2) | 50.5 GB per node |
| Total cluster cache | ~505 GB | 10 × 50.5 GB |
| % of working set cached | ~12% of 1-day data (4.17 TB) | 505 GB / 4,170 GB |

**Total per-node memory (10 combined):**

| Component | Memory |
|---|---|
| Insert buffer | 256 MB |
| WAL (disk-backed, mmap) | 512 MB |
| L1 cache | 512 MB |
| Footer cache | 200 MB |
| Bloom filters | 300 MB |
| Manifest + label index | 100-200 MB |
| Query working set (50 concurrent) | 2.5-10 GB |
| Compaction merge buffer | 256-512 MB |
| Go runtime + overhead | 500 MB |
| **Total** | **~5-13 GB** |

**Total per-node CPU (10 combined):**

| Component | CPU cores |
|---|---|
| ZSTD compression (flush) | 0.5-1 |
| Query file scanning (50 concurrent × 64 workers) | 4-8 |
| Compaction (decompress + merge + recompress) | 0.5-1 |
| HTTP handling (insert + query) | 0.5-1 |
| S3 I/O goroutines | 0.5-1 |
| **Total** | **~6-12 cores** |

**S3 request budget (10 combined, entire cluster):**

| Operation | Requests/day | GB/day | Cost estimate ($0.005/1K GET, $0.005/1K PUT) |
|---|---|---|---|
| Flush PUTs | 800K | 100 TB (pre-compress ~15-20 TB) | $4.00 |
| Query GETs (500 clients, avg 10 queries/hour) | ~12M (with cache) | varies | $60.00 |
| Compaction GETs | ~8M (multi-pass merge) | ~100 TB read | $40.00 |
| Compaction PUTs | ~800K (merged output) | ~15-20 TB | $4.00 |
| Compaction DELETEs | ~800K | 0 | $4.00 |
| LIST (manifest/partition scan) | ~100K | 0 | $0.50 |
| **Total** | **~22M requests** | — | **~$112/day** |

**Problems with this configuration:**

1. **MaxConcurrent overflow:** 50 queries/node exceeds default 32 → queries rejected with HTTP 429
2. **Query-insert contention:** 50 concurrent queries × 64 file workers = 3,200 goroutines competing with flush for CPU/S3 connections
3. **Cache thrash:** 500 clients with diverse query patterns evict cached data before reuse
4. **Wasted resources:** 10 insert pipelines when 3-5 would handle 100 TB/day
5. **10-way compaction merges:** 33K files per partition from 10 independent writers
6. **Memory pressure:** 5-13 GB per node, queries and insert competing for same L1 cache

---

### Scenario B: 3 Combined + 10 Select Nodes (Recommended)

3 combined nodes handle insert + compaction + S3 query backend. 10 select nodes handle query fan-out + local cache. 500 clients hit select tier only.

#### Combined Node (×3) — Insert + Compaction + Backend Query

**Ingest path:**

| Resource | Per node | Calculation |
|---|---|---|
| Ingest throughput | 33 TB/day = 386 MB/s | 100 TB / 3 nodes |
| Flush rate | ~267K files/day = ~3.1/s | 33 TB / 128 MB target |
| WAL disk | up to 512 MB | WALMaxBytes default |
| Insert buffer memory | up to 256 MB | MaxBufferBytes default |
| ZSTD compress CPU | ~2-3 cores sustained | 128 MB × level 7 × 3.1/s |
| S3 PUTs (flush) | ~267K/day = ~3.1/s | 1 PUT per file |
| Writer streams/partition | 3 | Only 3 nodes write |

**Backend query path (fan-out from select tier, not direct clients):**

| Resource | Per node | Calculation |
|---|---|---|
| Concurrent backend queries | ~15-30 | Select tier fans out to all 3 → 500/10 selects × 3 combined |
| MaxConcurrent needed | 32 (default sufficient) | Backend load is distributed |
| Query memory per request | ~50-100 MB | Self-filtered to owned files only (hybrid) |
| Peak query memory | 1-3 GB | 30 × 50-100 MB |
| S3 GETs per query | 0 (mostly cache hits) | L1/L2 warm from write-through (Phase H4) |

**Compaction path (Phase I sharding):**

| Resource | Per node | Calculation |
|---|---|---|
| Owned partitions | 240 of 720 total | 720 / 3 nodes |
| Files to merge per partition | ~33K (from 3 writers only) | 4.17 TB per partition / 128 MB |
| Merge width | 3-way (not 10-way) | Only 3 writer streams |
| Compaction S3 read/day | ~13.9 TB per node | 720/3 partitions × 4.17 TB |
| Compaction S3 write/day | ~13.9 TB per node | Merged output |
| Compaction memory | ~256-512 MB | 3-way merge buffer (smaller than 10-way) |
| Compaction CPU | ~1-2 cores | Decompress + merge-sort + recompress |
| MaxConcurrent compactions | 2 (recommended for this load) | Parallel partition compactions |

**Cache (combined node — file-level data + write-through):**

| Resource | Per node | Calculation |
|---|---|---|
| L1 memory cache | 2 GB (recommended) | Increase from 512 MB for production |
| L2 disk cache | 100 GB (recommended) | Increase from 50 GB |
| Write-through hit rate | ~80-95% for recent data | Phase H4: insert path populates cache |
| Effective cache per node | ~102 GB | L1 + L2 |
| Total combined cache | ~306 GB (3 nodes) | Without Phase G dedup |
| With Phase G (az-local) | ~306 GB unique data cached | Cache ring dedup |

**Total per-node memory (3 combined):**

| Component | Memory |
|---|---|
| Insert buffer | 256 MB |
| WAL | 512 MB |
| L1 cache | 2 GB (production tuned) |
| Footer cache | 200 MB |
| Bloom filters | 300 MB |
| Manifest + label index | 200 MB |
| Backend query working set (30 concurrent) | 1-3 GB |
| Compaction merge buffer | 256-512 MB |
| Go runtime + overhead | 500 MB |
| **Total** | **~5-7 GB** |

**Total per-node CPU (3 combined):**

| Component | CPU cores |
|---|---|
| ZSTD compression (flush at 3.1/s) | 2-3 |
| Backend query scanning (30 concurrent, owned files only) | 2-4 |
| Compaction (2 concurrent) | 1-2 |
| HTTP handling (insert + backend query) | 0.5-1 |
| S3 I/O goroutines | 1-2 |
| **Total** | **~7-12 cores** |

#### Select Node (×10) — Stateless Query Fan-Out + Cache

**Query path (500 clients / 10 select nodes = 50 per node):**

| Resource | Per node | Calculation |
|---|---|---|
| Concurrent queries | 50 | 500 / 10 select nodes |
| MaxConcurrent needed | 64 | Override default |
| Fan-out per query | 3 HTTP requests | 1 to each combined node |
| Result merge | Streaming, low CPU | VL netselect protocol |
| Query latency (cached) | 5-50 ms | Select L1 hit or combined L1 hit |
| Query latency (uncached, 1h) | 100-500 ms | Combined reads from S3 via Phase A/B |

**Cache (select node — query result cache):**

| Resource | Per node | Calculation |
|---|---|---|
| L1 memory cache | 1 GB | Query result cache, smaller than combined |
| L2 disk cache | 20 GB | Hot query results |
| Cache key | QueryHash + TimeRange + Tenant | Exact query dedup |
| Cache hit rate (dashboard) | ~60-80% | Same dashboards hit repeatedly |
| Cache hit rate (ad-hoc) | ~10-20% | Unique queries, low reuse |

**Total per-node memory (10 select):**

| Component | Memory |
|---|---|
| L1 query cache | 1 GB |
| Fan-out connection pool | 100 MB |
| Result merge buffers (50 concurrent) | 500 MB - 1 GB |
| Ring + peer state | 50 MB |
| Go runtime + overhead | 300 MB |
| **Total** | **~2-3 GB** |

**Total per-node CPU (10 select):**

| Component | CPU cores |
|---|---|
| HTTP handling (50 concurrent clients) | 1-2 |
| Fan-out + result merge | 1-2 |
| Cache management | 0.5 |
| Gap detection / health checks | 0.1 |
| **Total** | **~3-5 cores** |

**S3 request budget (3 combined + 10 select, entire cluster):**

| Operation | Requests/day | Notes |
|---|---|---|
| Flush PUTs | 800K | Same total data, 3 nodes flush more each |
| Query GETs | ~2-4M | 60-80% lower: combined cache hits, select cache hits |
| Compaction GETs | ~2.4M | 3-way merge vs 10-way → 3.3x fewer GETs per merge |
| Compaction PUTs | ~800K | Same output volume |
| Compaction DELETEs | ~800K | Same cleanup |
| **Total** | **~7-9M requests** | **~60-65% fewer than Scenario A** |

---

### Scenario Comparison

| Metric | A: 10 combined | B: 3 combined + 10 select |
|---|---|---|
| **Total nodes** | 10 | 13 |
| **Total memory** | 50-130 GB | 35-51 GB |
| **Total CPU cores** | 60-120 | 51-66 |
| **S3 requests/day** | ~22M | ~7-9M |
| **S3 cost/day** | ~$112 | ~$40-50 |
| **Query capacity** | 320 concurrent (10×32) | 640 concurrent (10×64 select) |
| **Query rejected risk** | High (50/node > default 32) | Low (50/node with 64 cap) |
| **Insert-query contention** | Severe (shared CPU/cache/S3) | None (separate nodes) |
| **Compaction merge width** | 10-way (10 writer streams) | 3-way (3 writer streams) |
| **Cache hit rate** | ~12% working set | ~30-50% (write-through + two-tier) |
| **Failover** | Partial response only | Gap detection + redistribution |
| **Scaling independence** | Coupled | Decoupled |

**Scenario B uses fewer total resources, handles more queries, achieves better cache hit rates, and produces 60% fewer S3 requests.** The 3 extra nodes (13 vs 10) are lightweight select pods (~2-3 GB each) that replace 7 heavy combined pods (~5-13 GB each).

### Recommended Production Configuration

```yaml
# Combined nodes (StatefulSet, 3 replicas)
combined:
  replicas: 3
  resources:
    requests:
      cpu: 8
      memory: 8Gi
    limits:
      cpu: 12
      memory: 12Gi
  args:
    - "-lakehouse.role=all"
    - "-lakehouse.compaction.shard-count=3"
    # shard-id auto-detected from StatefulSet ordinal
    - "-lakehouse.cache.memory-mb=2048"
    - "-lakehouse.cache.disk-max-mb=102400"
    - "-lakehouse.cache.partition-mode=az-local"
    - "-lakehouse.query.max-concurrent=32"
    - "-lakehouse.s3.max-connections=256"
    - "-lakehouse.s3.max-concurrent-downloads=32"

# Select nodes (Deployment, 10 replicas, HPA target)
select:
  replicas: 10
  resources:
    requests:
      cpu: 4
      memory: 3Gi
    limits:
      cpu: 6
      memory: 4Gi
  args:
    - "-lakehouse.role=select"
    - "-lakehouse.discovery.storage-nodes=dns+lakehouse-combined.ns.svc:9428"
    - "-lakehouse.cache.memory-mb=1024"
    - "-lakehouse.cache.disk-max-mb=20480"
    - "-lakehouse.query.max-concurrent=64"
```

### Scaling Triggers

| Signal | Action |
|---|---|
| Insert buffer utilization > 80% sustained | Add combined node (update shard-count) |
| Query reject rate (HTTP 429) > 1% | Add select node |
| Compaction lag > 2 hours behind | Increase compaction maxConcurrent, or add combined node |
| Cache hit ratio < 40% | Increase L1/L2 sizes, or add select nodes for Phase G ring |
| S3 throttle events > 0 | Increase S3 connection pool or spread across S3 prefixes |
| P99 query latency > 5s | Check cache coverage, consider Phase G `distributed` mode |
