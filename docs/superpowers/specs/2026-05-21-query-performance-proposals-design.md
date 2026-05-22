# Query Performance Architectural Optimizations — Design Spec

**Date:** 2026-05-21
**Status:** Approved
**Scope:** 6 architectural optimizations for query latency, maintaining parquet compatibility and durability

## Context

Phase 0-3 of query performance optimization is complete (correctness gate, instrumentation, column projection, bloom coverage, concurrency testing). Benchmarks show 5/21 scenarios passing — the dominant bottleneck is **full S3 file downloads per query** (50-500ms each × 200+ files).

Root cause analysis identified 6 architectural proposals, all approved for implementation.

## Constraints

1. **Parquet compatibility**: All parquet files must remain directly queryable by analytics tools (ClickHouse, DuckDB, Spark). No proprietary formats, no required sidecar files for basic queries.
2. **Durability**: All changes must preserve data durability guarantees. No in-memory-only state that hasn't been flushed to S3.
3. **VL/VT upstream-first**: Never modify VL/VT code. Only Lakehouse code changes.

---

## Proposal 1: Parquet Footer + Column Index Caching

### Problem

`queryFile()` (storage_query.go:198-262) downloads the entire parquet file via `getFileData()`, then calls `parquet.OpenFile(bytes.NewReader(data))` which parses the footer. For a 5MB file, this downloads 5MB just to read ~2KB of metadata.

### Solution

Cache parquet footer metadata (schema, row group stats, column chunk offsets) in an LRU cache keyed by file key. On subsequent queries to the same file, skip the footer download and use cached metadata to determine which row groups need reading.

### Design

```
type FooterCache struct {
    mu    sync.RWMutex
    items map[string]*CachedFooter  // key → footer
    lru   *list.List
    maxMB int
}

type CachedFooter struct {
    Schema      *parquet.Schema
    RowGroups   []RowGroupMeta     // min/max stats, offsets, sizes
    FileSize    int64
    ColumnIndex map[int]parquet.ColumnIndex
}

type RowGroupMeta struct {
    NumRows    int64
    Columns    []ColumnChunkMeta
}

type ColumnChunkMeta struct {
    Offset     int64
    Size       int64
    MinValue   parquet.Value
    MaxValue   parquet.Value
    BloomOffset int64
    BloomSize   int64
}
```

Footer cache is populated on first file access. Cache is memory-only (footers are small — ~2KB per file × 10K files = ~20MB).

**Parquet compatibility**: No file format changes. Footer cache is read-only in-memory optimization.

**Durability**: No impact — read path only.

---

## Proposal 2: In-Memory Partition Summary Index

### Problem

`bloomFilterFiles()` (storage_query.go:634-706) loads per-partition bloom indexes from S3 on first query. For partitions not yet cached, this adds an S3 round-trip per partition.

### Solution

Build a lightweight in-memory summary index from manifest metadata on startup. The summary contains min/max timestamps and a hash sketch per promoted field per partition, enabling instant partition skip without S3 access.

### Design

```
type PartitionSummary struct {
    Partition     string
    MinTimestamp   int64
    MaxTimestamp   int64
    FileCount     int
    TotalBytes    int64
    FieldSketches map[string]*HyperLogLog  // field → cardinality sketch
    LabelUnion    map[string][]string       // field → union of all label values
}

type SummaryIndex struct {
    mu         sync.RWMutex
    partitions []PartitionSummary  // sorted by MinTimestamp
}
```

Rebuilt on every manifest refresh (30s interval). Uses existing manifest FileInfo.Labels data — no additional S3 reads.

**Parquet compatibility**: No file format changes.

**Durability**: Read-only in-memory structure derived from manifest.

---

## Proposal 3: Pre-Aggregated Rollups for Stats/Hits

### Problem

`stats_query` and `hits` endpoints read all rows in all matching files to compute counts and histograms. For `hits_24h_step_1h`, this scans 24 hours of data reading every row just to count timestamps per bucket.

### Solution

During compaction, compute and store sidecar rollup files alongside the main parquet files. Rollup files contain pre-aggregated counts by time bucket and field value combinations.

### Design

Rollup file format: standard parquet with a fixed schema:

```
rollup_{partition}_{interval}.parquet:
  bucket_start: int64 (unix_nano, start of time bucket)
  bucket_end:   int64 (unix_nano, end of time bucket)
  field_name:   string (e.g., "service.name", "level", "*")
  field_value:  string (e.g., "api-gateway", "ERROR", "")
  count:        int64
  min_timestamp: int64
  max_timestamp: int64
```

Intervals: 1m, 5m, 1h, 1d. Compaction produces rollups for each interval.

Query path: `RunHitsQuery` and `RunStatsQuery` check for rollup files first. If a rollup exists for the requested step size, read the rollup instead of scanning raw data. Fall back to full scan for queries with complex filters that rollups don't cover.

**Parquet compatibility**: Rollup files are standard parquet — queryable by any analytics tool. They're additive (extra files), not modifications to existing data files.

**Durability**: Rollup files are written to S3 during compaction. If missing, queries fall back to full scan — no data loss.

---

## Proposal 4: Persistent File-Level Cache with Warmup

### Problem

First query after server restart has cold cache — every file requires an S3 download. SmartCache L1 (memory) and L2 (disk) are empty.

### Solution

On startup, pre-warm the disk cache (L2) by downloading files from the most recent partitions. This ensures common "last N hours" queries hit cache immediately.

### Design

```
type WarmupConfig struct {
    Enabled        bool
    PartitionsBack int           // how many partitions to warm (default: 6 = 6 hours)
    MaxFiles       int           // cap total files to warm (default: 500)
    MaxBytes       int64         // cap total bytes (default: 1GB)
    Concurrency    int           // parallel S3 downloads (default: 16)
}
```

Warmup runs as a background goroutine after manifest first loads. Prioritizes most recent partitions. Uses the same `getFileData` path so files land in both L2 disk cache and L1 memory cache.

Progress logged every 100 files. Warmup is cancelled if server receives shutdown signal.

**Parquet compatibility**: No file format changes.

**Durability**: Read-only operation.

---

## Proposal 5: Range-Based S3 Reads

### Problem

`Download()` (s3reader/reader.go:194-220) does a full `GetObject` for every file. When we only need the footer (last 8 bytes + footer length), or a specific row group, we download the entire file.

### Solution

Add `DownloadRange(ctx, key, offset, length)` to the S3 reader. Use HTTP Range header for targeted reads:
- Footer read: last 8 bytes → parse footer length → last N bytes
- Row group read: specific byte range from column chunk metadata
- Full file read: fallback for small files or when all row groups needed

### Design

```
func (p *ClientPool) DownloadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
    rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
    out, err := p.client.GetObject(ctx, &s3.GetObjectInput{
        Bucket: aws.String(p.bucket),
        Key:    aws.String(key),
        Range:  aws.String(rangeHeader),
    })
    // ...
}
```

Decision logic in `queryFile()`:
1. If file < 256KB: full download (range overhead not worth it)
2. If footer cached and only 1-2 row groups needed: range read those row groups
3. If footer not cached: range read footer first, cache it, then decide

**Parquet compatibility**: No file format changes. Standard S3 range reads.

**Durability**: Read-only operation.

---

## Proposal 6: Remove Write Lock Serialization

### Problem

`serializedWriteBlock` (storage_query.go:127-131) wraps `filteredWriteBlock` with `wbMu sync.Mutex`. All file workers serialize through this single lock when emitting results. This creates contention under concurrent queries.

### Solution

Remove the mutex. VL's `WriteDataBlockFunc` already handles concurrent calls from multiple workers — it's designed for the VL storage engine's parallel architecture. The `workerID` parameter exists specifically for this purpose.

### Design

Replace:
```go
var wbMu sync.Mutex
serializedWriteBlock := func(workerID uint, db *logstorage.DataBlock) {
    wbMu.Lock()
    filteredWriteBlock(workerID, db)
    wbMu.Unlock()
}
```

With:
```go
// VL's writeBlock is safe for concurrent calls with distinct workerIDs
```

Each file worker already gets a distinct implicit workerID (goroutine index). Pass the worker index as the workerID parameter directly.

**Verification needed**: Confirm VL's WriteDataBlockFunc contract allows concurrent calls. Check `logstorage.WriteDataBlockFunc` type and its callers in VL source.

**Parquet compatibility**: No file format changes.

**Durability**: No impact — read path only.

---

## Implementation Order

1. **Proposal 6** (write lock removal) — 30 min, immediate concurrency improvement
2. **Proposal 5** (range reads) — 2 hours, foundation for proposals 1 and 3
3. **Proposal 1** (footer caching) — 2 hours, depends on range reads
4. **Proposal 4** (cache warmup) — 1 hour, independent
5. **Proposal 2** (partition summary) — 2 hours, depends on manifest
6. **Proposal 3** (rollups) — 4 hours, most complex, depends on compaction

## Verification Plan

After each proposal:
1. All 1717 existing tests pass
2. Benchmark comparison (before/after)
3. Parquet files remain readable by `parquet-go`, DuckDB, and the existing ClickHouse integration
4. No new S3 error modes introduced
5. Cache behavior validated under concurrent queries
