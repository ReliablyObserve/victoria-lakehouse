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

File ownership uses the existing consistent hash ring. The ring key is the S3 file key (already used by `PeerLookup.Lookup(key)`). No new infrastructure needed.

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

```go
// During flush: cache the columns we just wrote
func (w *Writer) cacheOnFlush(fileKey string, columns []string, data map[string][]byte) {
    for _, col := range columns {
        key := ChunkCacheKey{FileKey: fileKey, Column: col, RowGroup: 0}
        w.cache.PutL2(key.String(), data[col])
    }
}
```

**Impact:** Eliminates cold-start entirely for recently ingested data. Dashboard queries for "last 1h" are always cache-hot.

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

Distribute compaction work across **all insert instances** using the existing consistent hash ring for partition-based ownership. Each instance compacts only the partitions it owns. No leader election needed — work is distributed by default. Collision-free by construction.

### Why Insert Nodes (Not Select)

| Factor | Insert nodes | Select nodes |
|---|---|---|
| Data locality | Just wrote the data, may have it in memory | Must download from S3 |
| CPU competition | Ingest is bursty, compaction fills gaps | Compaction competes with queries |
| Write path | Already have S3 write credentials and writer pool | Read-only in some deployments |
| Scaling signal | More ingest = more data = more compaction needed | More queries ≠ more compaction |

Insert nodes are the natural home for compaction. They scale with data volume, have the data warm, and compaction fills idle CPU between flush cycles.

### Design

#### Partition Ownership via Hash Ring

Each insert node joins a **compaction ring** (separate from the peer cache ring, using the same memberlist gossip). Partition keys (e.g., `0/0/logs/dt=2026-05-22/hour=14`) are hashed to determine the owning compactor.

```go
type CompactionRing struct {
    ring        *peercache.Ring
    selfAddr    string
}

func (cr *CompactionRing) OwnsPartition(partition string) bool {
    _, isLocal := cr.ring.Lookup(partition)
    return isLocal
}

func (cr *CompactionRing) OwnedPartitions(all []string) []string {
    var owned []string
    for _, p := range all {
        if cr.OwnsPartition(p) {
            owned = append(owned, p)
        }
    }
    return owned
}
```

#### Scheduler Changes

Replace leader-based scheduling with ring-based ownership:

```go
// Before (leader-based):
func (s *Scheduler) tick(ctx context.Context) {
    if !s.leader.IsLeader() {
        return  // only leader compacts
    }
    partitions := s.manifest.AllPartitions()
    s.compactPartitions(ctx, partitions)
}

// After (ring-based):
func (s *Scheduler) tick(ctx context.Context) {
    allPartitions := s.manifest.AllPartitions()
    owned := s.compactionRing.OwnedPartitions(allPartitions)
    s.compactPartitions(ctx, owned)
}
```

#### Collision Avoidance

Three layers, each cheaper than the last:

**Layer 1 — Ring ownership (free):** Each partition has exactly one owner. No collision possible under stable ring.

**Layer 2 — S3 sentinel (existing):** Before compacting, acquire the existing S3 sentinel file (`_compacting/{partition}`). This handles the brief window during ring rebalancing when two nodes might temporarily claim the same partition.

```go
func (s *Scheduler) compactIfOwned(ctx context.Context, partition string) {
    if !s.compactionRing.OwnsPartition(partition) {
        return
    }
    acquired, err := s.sentinel.Acquire(ctx, s.prefix, partition, s.selfAddr)
    if !acquired || err != nil {
        return  // another node got it during rebalance
    }
    defer s.sentinel.Release(ctx, s.prefix, partition)
    s.compact(ctx, partition)
}
```

**Layer 3 — Stale timeout (existing):** Sentinel has `staleTimeout` (default 30min). If a compactor crashes mid-compaction, the sentinel auto-expires and another node picks up the work.

#### Ring Rebalancing

When insert nodes scale up/down:

1. **New node joins:** Memberlist gossip propagates within seconds. New node's compaction ring recalculates ownership. Some partitions shift to the new node.
2. **Transition window (30s):** Both old and new owner might try to compact the same partition. S3 sentinel prevents collision — first to acquire wins, second skips.
3. **Node leaves:** Remaining nodes recalculate ownership. Orphaned partitions are picked up within one compaction interval (default 5min).

No external coordination needed. Ring + sentinel provides the same guarantees as Mimir's compactor ring but using only gossip + S3 — no etcd/Consul required.

#### Work Distribution Fairness

The hash ring naturally distributes partitions evenly. With 3 insert nodes and 72 partitions (3 days × 24 hours), each node owns ~24 partitions. As data grows, new partitions are automatically distributed.

```
Insert-0: owns dt=05-20/hour=00, dt=05-20/hour=03, dt=05-20/hour=06, ...  (~24 partitions)
Insert-1: owns dt=05-20/hour=01, dt=05-20/hour=04, dt=05-20/hour=07, ...  (~24 partitions)
Insert-2: owns dt=05-20/hour=02, dt=05-20/hour=05, dt=05-20/hour=08, ...  (~24 partitions)
```

#### Per-Instance Concurrency

Each insert node runs up to `maxConcurrent` (default 2) parallel compactions on its owned partitions. Total cluster compaction throughput = `nodes × maxConcurrent`.

```
3 insert nodes × 2 concurrent = 6 parallel compactions cluster-wide
vs
1 leader × 1 concurrent = 1 compaction at a time (current)
```

### Fallback to Leader Mode

For single-instance deployments (no ring), fall back to the existing leader-based scheduler. Config:

```
-lakehouse.compaction.mode=ring       # distributed (default when peers > 1)
-lakehouse.compaction.mode=leader     # single-leader (default when peers = 0)
```

Auto-detection: if the compaction ring has > 1 member, use ring mode. Otherwise, use leader mode.

### Expected Impact

| Metric | Before (leader) | After (ring, 3 nodes) |
|---|---|---|
| Compaction throughput | 1 partition at a time | 6 partitions at a time |
| Time to compact 72 partitions | ~6 hours | ~1 hour |
| Scaling | Manual leader failover | Linear with insert nodes |
| Coordination | S3 leader election | Gossip + S3 sentinel (existing) |

### Testing

- Unit test: ring assigns partitions deterministically, ownership is disjoint
- Unit test: sentinel prevents double-compaction during rebalance
- Integration test: 3-instance deployment, all partitions compacted, no duplicates
- Scale test: add/remove instance, verify repartitioning within 1 compaction interval
- Crash test: kill instance mid-compaction, verify sentinel expires and partition is picked up
- Fairness test: verify partition distribution is within ±10% of even across nodes
- Regression: single-instance deployment auto-falls back to leader mode

### Logs-specific

Log partitions are typically uniform in size (each hour has similar volume). Ring distributes work evenly.

### Traces-specific

Trace partitions can be skewed (some hours have 10x more spans). Consider weighted ring (future): nodes with more CPU/memory get more vnodes. For now, even distribution is acceptable — skewed partitions just take longer on their owning node.

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

**Total: ~27 days for all phases.**

**Phase A-F (S3 I/O):** ~12 days → VLH within 3-5x of VL for most query types.
**Phase G-I (distributed):** ~15 days → Near-ClickHouse for cached, linear compaction scaling.
