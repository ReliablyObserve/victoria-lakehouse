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

## Implementation Order

| Phase | Effort | Dependencies | Expected cumulative improvement |
|---|---|---|---|
| A: Read-ahead buffer | 2 days | None | 3-5x for range-read queries |
| C: Transport tuning | 0.5 days | None (parallel with A) | +10-20% across all queries |
| D: Async RG prefetch | 1 day | A (benefits from buffered reads) | +2-3x for multi-RG files |
| B: Range coalescing | 2 days | A (builds on buffer layer) | +30-50% for multi-column projection |
| E: Compaction | 3 days | None (independent) | 5-10x for data > 24h old |
| F: Streaming aggregation | 3 days | Column projection (already done) | 10-50x for stats/rate/histogram |

**Total: ~12 days for all phases. Expected result: VLH within 3-5x of VL for most query types.**
