# Long-Range Query Optimization Plan (Phase 5)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce long-range query latency from 3.8-9.7s to <500ms by eliminating unnecessary S3 file downloads through manifest-level metadata, footer pre-fetch, and file-level bloom indexes.

**Architecture:** Five layered optimizations, each reducing the set of files that need full S3 download. Ordered by impact/effort ratio. All changes are Lakehouse-only — no VL/VT upstream or parquet library modifications.

**Tech Stack:** Go, Parquet (parquet-go v0.29.0), AWS SDK v2 (S3 range reads), existing manifest/cache infrastructure.

**Constraint:** HARD RULE — only Lakehouse code changes. No modifications to VL/VT upstream packages or the parquet-go library.

---

## Current bottleneck

```
24h query: 3.8s total
  └─ 8 file workers × ~5 files each × 150ms/file
       └─ S3 GET (60-150ms) + footer parse (5-10ms) + RG filter (5-20ms) + deser (20-100ms)

Root cause: full S3 download before any per-file filtering.
```

## Optimization layers (cumulative)

```
Layer 0: Manifest partition pruning (existing) ← already done
Layer 1: Pre-aggregated partition counts ← hits/stats skip file reads entirely
Layer 2: Manifest column stats ← skip files by min/max without S3
Layer 3: Footer pre-fetch ← download 8KB footer, skip full file if no match
Layer 4: File-level bloom sidecar ← per-file bloom checked before download
Layer 5: S3 range column reads ← fetch only needed column chunks
```

---

### Task 1: Pre-aggregated partition counts for hits/stats queries

**Files:**
- Modify: `internal/manifest/manifest.go:59-65` (PartitionMeta struct)
- Modify: `internal/manifest/manifest.go:286-310` (rebuildIndex)
- Modify: `internal/manifest/manifest.go:466-488` (AddFile)
- Create: `internal/manifest/partition_stats.go`
- Modify: `internal/storage/parquets3/storage_query.go:105-148` (RunQuery — add fast path before file workers)
- Modify: `lakehouse-traces/internal/storage/parquets3/storage_query.go` (same)
- Test: `internal/manifest/manifest_test.go`
- Test: `internal/storage/parquets3/storage_query_test.go`

**Rationale:** Hits queries (`/select/logsql/hits`) and stats count queries only need row counts per time bucket. The manifest already has `RowCount` per file and partitions are hourly. By pre-aggregating counts per partition in the manifest, these queries can skip all S3 file reads.

- [ ] **Step 1: Write failing test for partition stats**

```go
// internal/manifest/manifest_test.go
func TestManifest_PartitionStats(t *testing.T) {
	m := newTestManifest()

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:      "logs/dt=2026-05-01/hour=10/a.parquet",
		Size:     1000,
		RowCount: 500,
	})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:      "logs/dt=2026-05-01/hour=10/b.parquet",
		Size:     2000,
		RowCount: 300,
	})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{
		Key:      "logs/dt=2026-05-01/hour=11/c.parquet",
		Size:     1500,
		RowCount: 400,
	})

	stats := m.GetPartitionStats()
	if len(stats) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(stats))
	}

	h10 := stats["dt=2026-05-01/hour=10"]
	if h10.TotalRows != 800 {
		t.Errorf("hour=10 rows = %d, want 800", h10.TotalRows)
	}
	if h10.FileCount != 2 {
		t.Errorf("hour=10 files = %d, want 2", h10.FileCount)
	}

	h11 := stats["dt=2026-05-01/hour=11"]
	if h11.TotalRows != 400 {
		t.Errorf("hour=11 rows = %d, want 400", h11.TotalRows)
	}
}

func TestManifest_GetRowCountsForRange(t *testing.T) {
	m := newTestManifest()

	may1h10 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	may1h11 := time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC)
	may1h12 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key: "a.parquet", Size: 1000, RowCount: 500,
		MinTimeNs: may1h10.UnixNano(), MaxTimeNs: may1h10.Add(30 * time.Minute).UnixNano(),
	})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{
		Key: "b.parquet", Size: 2000, RowCount: 300,
		MinTimeNs: may1h11.UnixNano(), MaxTimeNs: may1h11.Add(30 * time.Minute).UnixNano(),
	})
	m.AddFile("dt=2026-05-01/hour=12", FileInfo{
		Key: "c.parquet", Size: 1500, RowCount: 400,
		MinTimeNs: may1h12.UnixNano(), MaxTimeNs: may1h12.Add(30 * time.Minute).UnixNano(),
	})

	// Range covering hours 10-11 should return 800 rows
	total := m.GetRowCountForRange(may1h10.UnixNano(), may1h12.UnixNano())
	if total != 800 {
		t.Errorf("row count for 10-12 range = %d, want 800", total)
	}

	// Full range should return 1200
	total = m.GetRowCountForRange(may1h10.UnixNano(), may1h12.Add(time.Hour).UnixNano())
	if total != 1200 {
		t.Errorf("row count for full range = %d, want 1200", total)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test -count=1 -run "TestManifest_PartitionStats|TestManifest_GetRowCountsForRange" github.com/ReliablyObserve/victoria-lakehouse/internal/manifest -v`
Expected: FAIL — methods don't exist

- [ ] **Step 3: Implement PartitionStats in manifest**

Create `internal/manifest/partition_stats.go`:

```go
package manifest

// PartitionStats holds pre-aggregated counts per partition.
type PartitionStats struct {
	TotalRows int64
	FileCount int
	TotalBytes int64
}

// GetPartitionStats returns pre-aggregated stats for all partitions.
func (m *Manifest) GetPartitionStats() map[string]PartitionStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]PartitionStats, len(m.files))
	for partition, files := range m.files {
		var ps PartitionStats
		for _, fi := range files {
			ps.TotalRows += fi.RowCount
			ps.FileCount++
			ps.TotalBytes += fi.Size
		}
		result[partition] = ps
	}
	return result
}

// GetRowCountForRange returns the total row count across all partitions
// that overlap with [startNs, endNs). Uses partition time bounds only —
// does not open any files.
func (m *Manifest) GetRowCountForRange(startNs, endNs int64) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	start := time.Unix(0, startNs)
	end := time.Unix(0, endNs)

	var total int64
	idx := sort.Search(len(m.sortedPartitions), func(i int) bool {
		return m.sortedPartitions[i].end.After(start)
	})

	for i := idx; i < len(m.sortedPartitions); i++ {
		p := &m.sortedPartitions[i]
		if !p.start.Before(end) {
			break
		}
		for _, fi := range m.files[p.key] {
			total += fi.RowCount
		}
	}
	return total
}

// GetRowCountsByPartition returns per-partition row counts for partitions
// overlapping [startNs, endNs). Each entry maps partition key to its row count.
// Used by hits/histogram queries to avoid opening files.
func (m *Manifest) GetRowCountsByPartition(startNs, endNs int64) []PartitionRowCount {
	m.mu.RLock()
	defer m.mu.RUnlock()

	start := time.Unix(0, startNs)
	end := time.Unix(0, endNs)

	idx := sort.Search(len(m.sortedPartitions), func(i int) bool {
		return m.sortedPartitions[i].end.After(start)
	})

	var result []PartitionRowCount
	for i := idx; i < len(m.sortedPartitions); i++ {
		p := &m.sortedPartitions[i]
		if !p.start.Before(end) {
			break
		}
		var rows int64
		for _, fi := range m.files[p.key] {
			rows += fi.RowCount
		}
		result = append(result, PartitionRowCount{
			StartNs:  p.start.UnixNano(),
			EndNs:    p.end.UnixNano(),
			RowCount: rows,
		})
	}
	return result
}

// PartitionRowCount holds time bounds and row count for a single partition.
type PartitionRowCount struct {
	StartNs  int64
	EndNs    int64
	RowCount int64
}
```

Add required imports (`sort`, `time`) to the file.

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOWORK=off go test -count=1 -run "TestManifest_PartitionStats|TestManifest_GetRowCountsForRange" github.com/ReliablyObserve/victoria-lakehouse/internal/manifest -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/manifest/partition_stats.go internal/manifest/manifest_test.go
git commit -m "feat: pre-aggregated partition stats for manifest-level row counts"
```

---

### Task 2: Footer pre-fetch — download 8KB footer before full file

**Files:**
- Create: `internal/storage/parquets3/footer_prefetch.go`
- Modify: `internal/storage/parquets3/storage_query.go:236-240` (queryFile — add footer pre-check)
- Modify: `lakehouse-traces/internal/storage/parquets3/storage_query.go` (same)
- Test: `internal/storage/parquets3/footer_prefetch_test.go`

**Rationale:** Currently `queryFile` downloads the full file (60-150ms) before checking any row group metadata. Parquet footers are typically 1-8KB. By downloading just the last 8KB via S3 range read (already supported: `DownloadRange` at s3reader/reader.go:222), we can parse column statistics and dictionary pages, then skip the full download if the query filter doesn't match any row group. This turns a 60-150ms full GET into a 5-15ms range read for files that can be eliminated.

**Existing building blocks:**
- `ClientPool.DownloadRange(ctx, key, offset, length)` — s3reader/reader.go:222
- `FooterLength(tail8)` — footer_cache.go:130 — reads 4-byte LE footer length from last 8 bytes
- `ParseFooterFromBytes(key, footerBytes, fileSize)` — footer_cache.go:116 — parses footer-only bytes
- `rowGroupMatchesFilter(f, rg, pdf)` — filter_pushdown.go:139 — checks column stats + dictionary

- [ ] **Step 1: Write failing test for footer pre-fetch skip**

```go
// internal/storage/parquets3/footer_prefetch_test.go
package parquets3

import (
	"context"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestShouldSkipByFooter_NoFilter(t *testing.T) {
	// No pushdown filter — should never skip
	reg := schema.NewRegistry(schema.ModeLog)
	skip, err := shouldSkipByFooter(context.Background(), nil, manifest.FileInfo{
		Key: "test.parquet", Size: 10000,
	}, "", reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if skip {
		t.Error("should not skip when no filter applies")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test -count=1 -run "TestShouldSkipByFooter" github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3 -v`
Expected: FAIL — function doesn't exist

- [ ] **Step 3: Implement footer pre-fetch logic**

Create `internal/storage/parquets3/footer_prefetch.go`:

```go
package parquets3

import (
	"context"
	"fmt"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const minFileSizeForPrefetch = 32 * 1024 // only prefetch for files > 32KB
const maxFooterPrefetchBytes = 16 * 1024  // fetch last 16KB (covers footer + column indexes)

// shouldSkipByFooter downloads only the Parquet footer via S3 range read,
// parses column statistics and dictionary pages, and returns true if the
// file can be skipped because no row group matches the query filter.
//
// Returns (false, nil) if:
//   - no pushdown filter applies (wildcard query)
//   - file is too small to benefit from prefetch
//   - footer already cached
//   - range read fails (fall back to full download)
func shouldSkipByFooter(
	ctx context.Context,
	pool *s3reader.ClientPool,
	fi manifest.FileInfo,
	queryStr string,
	registry *schema.Registry,
	footerCache *FooterCache,
) (bool, error) {
	// No pool — can't do range reads
	if pool == nil {
		return false, nil
	}

	// Build pushdown filter — skip prefetch for wildcard queries
	pdf := buildPushDownFilter(queryStr, registry)
	if pdf == nil || len(pdf.Checks) == 0 {
		return false, nil
	}

	// Small files are faster to download fully
	if fi.Size < minFileSizeForPrefetch {
		return false, nil
	}

	// Footer already cached — no need for prefetch, queryFile will use cache
	if footerCache != nil {
		if _, ok := footerCache.Get(fi.Key); ok {
			return false, nil
		}
	}

	// Download last N bytes (footer + column index pages)
	prefetchLen := int64(maxFooterPrefetchBytes)
	if prefetchLen > fi.Size {
		prefetchLen = fi.Size
	}
	offset := fi.Size - prefetchLen

	footerBytes, err := pool.DownloadRange(ctx, fi.Key, offset, prefetchLen)
	if err != nil {
		metrics.S3ErrorsTotal.Inc("RANGE_GET")
		return false, nil // fall back to full download
	}
	metrics.S3RequestsTotal.Inc("RANGE_GET")

	// Parse footer from the tail bytes
	cached, f, parseErr := ParseFooterFromBytes(fi.Key, footerBytes, fi.Size)
	if parseErr != nil {
		return false, nil // can't parse, fall back
	}

	// Cache the parsed footer for queryFile to reuse
	if footerCache != nil && cached != nil {
		footerCache.Put(fi.Key, cached)
		metrics.FooterCacheHits.Inc() // prefetch-populated
	}

	// Resolve filter column indices against this file's schema
	resolvedPDF := resolvePushDownIndices(f, pdf)
	if resolvedPDF == nil {
		return false, nil
	}

	// Check every row group — if none match, skip the file
	for _, rg := range f.RowGroups() {
		if rowGroupMatchesFilter(f, rg, resolvedPDF) {
			return false, nil // at least one RG matches, need full download
		}
	}

	metrics.ParquetRowGroupsSkipped.Inc("footer_prefetch")
	return true, nil
}
```

- [ ] **Step 4: Integrate footer pre-fetch into queryFile**

In `internal/storage/parquets3/storage_query.go`, inside the file worker loop (around line 188), add a pre-check before `queryFile`:

```go
// In the worker goroutine, before calling s.queryFile:
// Add after: for fi := range taskCh {
//   Add before: if err := s.queryFile(...)

// Footer pre-fetch: for filtered queries on large files, download
// just the footer via range read to check if file can be skipped.
if skip, _ := shouldSkipByFooter(ctx, s.pool, fi, queryStr, s.registry, s.footerCache); skip {
	continue
}
```

Apply the same change to `lakehouse-traces/internal/storage/parquets3/storage_query.go`.

- [ ] **Step 5: Run tests**

Run: `GOWORK=off go test -count=1 github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3 -v -timeout=120s`
Expected: PASS (all existing + new tests)

- [ ] **Step 6: Commit**

```bash
git add internal/storage/parquets3/footer_prefetch.go internal/storage/parquets3/footer_prefetch_test.go \
  internal/storage/parquets3/storage_query.go lakehouse-traces/internal/storage/parquets3/storage_query.go
git commit -m "feat: footer pre-fetch — S3 range read to skip files without full download"
```

---

### Task 3: Manifest column stats — min/max per column per file

**Files:**
- Modify: `internal/manifest/manifest.go:24-38` (FileInfo struct — add ColumnStats)
- Modify: `internal/storage/parquets3/storage_query.go:268` (updateLabelIndex — also capture column stats from footer)
- Create: `internal/manifest/column_stats.go`
- Modify: `internal/storage/parquets3/storage_query.go:992-1029` (filterFilesByLabels — add stats-based pre-filter)
- Modify: `lakehouse-traces/internal/storage/parquets3/storage_query.go` (same)
- Test: `internal/manifest/column_stats_test.go`

**Rationale:** Apache Iceberg stores per-file column min/max in its manifest. We can do the same: after parsing a file's footer (during first query or warmup), extract min/max values for key columns (`service.name`, `trace_id`, `level`, `status_code`) and store them in `FileInfo.ColumnStats`. On subsequent queries, `filterFilesByLabels` can skip files where the query value falls outside [min, max] — without any S3 access.

- [ ] **Step 1: Write failing test for column stats**

```go
// internal/manifest/column_stats_test.go
package manifest

import "testing"

func TestFileInfo_ColumnStatsMatch(t *testing.T) {
	fi := FileInfo{
		Key:  "test.parquet",
		Size: 1000,
		ColumnStats: map[string]ColumnMinMax{
			"service.name": {Min: "api-gateway", Max: "worker-service"},
			"level":        {Min: "DEBUG", Max: "WARN"},
		},
	}

	// Value in range
	if !fi.ColumnStatsContains("service.name", "order-service") {
		t.Error("order-service should be in range [api-gateway, worker-service]")
	}

	// Value below range
	if fi.ColumnStatsContains("service.name", "aaa-service") {
		t.Error("aaa-service should be below range")
	}

	// Value above range
	if fi.ColumnStatsContains("service.name", "zzz-service") {
		t.Error("zzz-service should be above range")
	}

	// Unknown column — no stats, assume match
	if !fi.ColumnStatsContains("unknown_col", "anything") {
		t.Error("unknown column should assume match")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test -count=1 -run "TestFileInfo_ColumnStatsMatch" github.com/ReliablyObserve/victoria-lakehouse/internal/manifest -v`
Expected: FAIL

- [ ] **Step 3: Implement ColumnStats on FileInfo**

Create `internal/manifest/column_stats.go`:

```go
package manifest

// ColumnMinMax stores the min and max values for a column across all
// row groups in a file. Extracted from Parquet column index metadata.
type ColumnMinMax struct {
	Min string `json:"min"`
	Max string `json:"max"`
}

// ColumnStatsContains returns true if the given value falls within
// [Min, Max] for the named column. Returns true (assume match) if
// no stats exist for the column.
func (fi FileInfo) ColumnStatsContains(column, value string) bool {
	if fi.ColumnStats == nil {
		return true
	}
	stats, ok := fi.ColumnStats[column]
	if !ok {
		return true
	}
	return value >= stats.Min && value <= stats.Max
}
```

Add `ColumnStats` field to `FileInfo` in `internal/manifest/manifest.go`:

```go
type FileInfo struct {
	// ... existing fields ...
	ColumnStats  map[string]ColumnMinMax `json:"column_stats,omitempty"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOWORK=off go test -count=1 -run "TestFileInfo_ColumnStatsMatch" github.com/ReliablyObserve/victoria-lakehouse/internal/manifest -v`
Expected: PASS

- [ ] **Step 5: Extract column stats during footer parse**

In `internal/storage/parquets3/storage_query.go`, the `updateLabelIndex` function (line 268) already parses the Parquet file after footer cache. Add column stats extraction alongside it:

```go
// Add to storage_query.go after updateLabelIndex(f) call:
s.updateColumnStats(fi.Key, f)
```

Create the extraction function (in a new file or in storage_query.go):

```go
func (s *Storage) updateColumnStats(fileKey string, f *parquet.File) {
	statsColumns := []string{"service.name", "trace_id", "level", "status_code", "service_name"}

	stats := make(map[string]manifest.ColumnMinMax)
	for _, col := range statsColumns {
		colIdx := findColumnIndex(f.Root(), col)
		if colIdx < 0 {
			continue
		}
		var globalMin, globalMax string
		for _, rg := range f.RowGroups() {
			cols := rg.ColumnChunks()
			if colIdx >= len(cols) {
				continue
			}
			cidx, err := cols[colIdx].ColumnIndex()
			if err != nil || cidx == nil {
				continue
			}
			for p := 0; p < cidx.NumPages(); p++ {
				pageMin := valueToString(cidx.MinValue(p))
				pageMax := valueToString(cidx.MaxValue(p))
				if globalMin == "" || pageMin < globalMin {
					globalMin = pageMin
				}
				if globalMax == "" || pageMax > globalMax {
					globalMax = pageMax
				}
			}
		}
		if globalMin != "" {
			stats[col] = manifest.ColumnMinMax{Min: globalMin, Max: globalMax}
		}
	}

	if len(stats) > 0 {
		s.manifest.UpdateFileColumnStats(fileKey, stats)
	}
}
```

Add `UpdateFileColumnStats` to manifest.go:

```go
func (m *Manifest) UpdateFileColumnStats(key string, stats map[string]ColumnMinMax) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, files := range m.files {
		for i := range files {
			if files[i].Key == key {
				files[i].ColumnStats = stats
				return
			}
		}
	}
}
```

- [ ] **Step 6: Use column stats in filterFilesByLabels**

In `filterByLabelIndex` (storage_query.go), add a column-stats pre-check for exact-match filters that miss the inverted index:

```go
// In filterFilesByLabels, before the O(N) scan loop, add:
// Column stats pre-filter — skip files where value is outside [min, max]
if pdf != nil {
	preFiltered := files[:0]
	statsSkipped := 0
	for _, fi := range files {
		skip := false
		for _, check := range pdf.Checks {
			if check.Op == PushDownExact && !fi.ColumnStatsContains(check.Column, check.Value) {
				skip = true
				break
			}
		}
		if skip {
			statsSkipped++
		} else {
			preFiltered = append(preFiltered, fi)
		}
	}
	if statsSkipped > 0 {
		metrics.ParquetRowGroupsSkipped.Inc("column_stats")
		files = preFiltered
	}
}
```

Apply the same change to `lakehouse-traces/internal/storage/parquets3/storage_query.go`.

- [ ] **Step 7: Run all tests**

Run: `GOWORK=off go test -count=1 -timeout=120s github.com/ReliablyObserve/victoria-lakehouse/internal/manifest github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add internal/manifest/column_stats.go internal/manifest/column_stats_test.go \
  internal/manifest/manifest.go internal/storage/parquets3/storage_query.go \
  lakehouse-traces/internal/storage/parquets3/storage_query.go
git commit -m "feat: manifest-level column stats — skip files by min/max without S3 download"
```

---

### Task 4: File-level bloom sidecar index

**Files:**
- Create: `internal/bloomindex/file_bloom.go`
- Create: `internal/bloomindex/file_bloom_test.go`
- Modify: `internal/storage/parquets3/bloom_build.go` (extend OnFileFlush to write file-level bloom)
- Modify: `internal/storage/parquets3/storage_query.go` (add file-level bloom check before download)
- Modify: `lakehouse-traces/internal/storage/parquets3/storage_query.go` (same)

**Rationale:** The current bloom index is partition-level (`{partition}/_bloom.bin`), meaning it can only skip entire partitions. For `trace_id` lookups, a partition bloom hit still requires downloading all files in that partition. A file-level bloom sidecar (stored alongside each Parquet file as `{key}.bloom`) enables skipping individual files. This is analogous to ClickHouse's SET data-skipping index.

- [ ] **Step 1: Write failing test for file-level bloom**

```go
// internal/bloomindex/file_bloom_test.go
package bloomindex

import (
	"testing"
)

func TestFileBloom_Contains(t *testing.T) {
	fb := NewFileBloom(0.01)
	fb.Add("trace_id", "abc123")
	fb.Add("trace_id", "def456")
	fb.Add("service.name", "api-gateway")

	data, err := fb.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	fb2, err := UnmarshalFileBloom(data)
	if err != nil {
		t.Fatal(err)
	}

	if !fb2.MayContain("trace_id", "abc123") {
		t.Error("should contain abc123")
	}
	if !fb2.MayContain("trace_id", "def456") {
		t.Error("should contain def456")
	}
	if fb2.MayContain("trace_id", "nonexistent") {
		t.Error("should not contain nonexistent (false positive unlikely at n=2)")
	}
	if !fb2.MayContain("service.name", "api-gateway") {
		t.Error("should contain api-gateway")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test -count=1 -run "TestFileBloom_Contains" github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex -v`
Expected: FAIL

- [ ] **Step 3: Implement file-level bloom**

Create `internal/bloomindex/file_bloom.go`:

```go
package bloomindex

import (
	"bytes"
	"encoding/gob"
	"hash/fnv"
	"math"
)

// FileBloom is a per-file bloom filter for key columns.
// Stored as a sidecar file ({parquet_key}.bloom) in S3.
type FileBloom struct {
	Columns map[string]*bitset // column -> bloom filter
	FPRate  float64
}

type bitset struct {
	Bits []uint64
	K    int // number of hash functions
	N    int // number of items added
}

func NewFileBloom(fpRate float64) *FileBloom {
	return &FileBloom{
		Columns: make(map[string]*bitset),
		FPRate:  fpRate,
	}
}

func (fb *FileBloom) Add(column, value string) {
	bs, ok := fb.Columns[column]
	if !ok {
		// Start with capacity for 1000 items, will grow if needed
		bs = newBitset(1000, fb.FPRate)
		fb.Columns[column] = bs
	}
	bs.add(value)
}

func (fb *FileBloom) MayContain(column, value string) bool {
	bs, ok := fb.Columns[column]
	if !ok {
		return true // no bloom for this column, assume present
	}
	return bs.mayContain(value)
}

func (fb *FileBloom) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(fb); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalFileBloom(data []byte) (*FileBloom, error) {
	var fb FileBloom
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&fb); err != nil {
		return nil, err
	}
	return &fb, nil
}

func newBitset(expectedItems int, fpRate float64) *bitset {
	m := int(math.Ceil(-float64(expectedItems) * math.Log(fpRate) / (math.Log(2) * math.Log(2))))
	k := int(math.Ceil(float64(m) / float64(expectedItems) * math.Log(2)))
	if k < 1 {
		k = 1
	}
	words := (m + 63) / 64
	return &bitset{Bits: make([]uint64, words), K: k}
}

func (bs *bitset) add(value string) {
	bs.N++
	for i := 0; i < bs.K; i++ {
		idx := bs.hash(value, i) % uint64(len(bs.Bits)*64)
		bs.Bits[idx/64] |= 1 << (idx % 64)
	}
}

func (bs *bitset) mayContain(value string) bool {
	for i := 0; i < bs.K; i++ {
		idx := bs.hash(value, i) % uint64(len(bs.Bits)*64)
		if bs.Bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

func (bs *bitset) hash(value string, seed int) uint64 {
	h := fnv.New64a()
	h.Write([]byte{byte(seed)})
	h.Write([]byte(value))
	return h.Sum64()
}

func init() {
	gob.Register(&FileBloom{})
	gob.Register(&bitset{})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOWORK=off go test -count=1 -run "TestFileBloom_Contains" github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex -v`
Expected: PASS

- [ ] **Step 5: Write file-level bloom on flush**

In `internal/storage/parquets3/bloom_build.go`, extend `OnFileFlush` to also write a file-level bloom sidecar:

```go
// In storageBloomObserver.OnFileFlush, after existing partition bloom logic,
// add file-level bloom write:
func (o *storageBloomObserver) writeFileBloom(ctx context.Context, fileKey string, values map[string][]string) {
	fb := bloomindex.NewFileBloom(0.01)
	for col, vals := range values {
		for _, v := range vals {
			fb.Add(col, v)
		}
	}
	data, err := fb.Marshal()
	if err != nil {
		logger.Warnf("file bloom marshal failed: %s; key=%s", err, fileKey)
		return
	}
	bloomKey := fileKey + ".bloom"
	if err := o.pool.Upload(ctx, bloomKey, data); err != nil {
		logger.Warnf("file bloom upload failed: %s; key=%s", err, bloomKey)
		return
	}
	metrics.S3RequestsTotal.Inc("PUT")
}
```

- [ ] **Step 6: Check file bloom before download in query path**

In `storage_query.go`, add file-level bloom check in the worker loop, after footer pre-fetch and before `queryFile`:

```go
// File-level bloom check: skip file if bloom sidecar indicates
// the queried value is definitely not present.
if skip := s.checkFileBloom(ctx, fi, queryStr); skip {
	continue
}
```

Implement `checkFileBloom`:

```go
func (s *Storage) checkFileBloom(ctx context.Context, fi manifest.FileInfo, queryStr string) bool {
	checks := s.buildBloomChecks(queryStr)
	if len(checks) == 0 {
		return false
	}

	bloomKey := fi.Key + ".bloom"
	data, err := s.pool.Download(ctx, bloomKey)
	if err != nil {
		return false // no bloom sidecar, can't skip
	}

	fb, err := bloomindex.UnmarshalFileBloom(data)
	if err != nil {
		return false
	}

	for _, check := range checks {
		if !fb.MayContain(check.Column, check.Value) {
			metrics.ParquetBloomChecks.Inc("file_bloom_skip")
			return true
		}
	}
	return false
}
```

Apply the same to traces module.

- [ ] **Step 7: Run all tests**

Run: `GOWORK=off go test -count=1 -timeout=120s github.com/ReliablyObserve/victoria-lakehouse/...`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add internal/bloomindex/file_bloom.go internal/bloomindex/file_bloom_test.go \
  internal/storage/parquets3/bloom_build.go internal/storage/parquets3/storage_query.go \
  lakehouse-traces/internal/storage/parquets3/storage_query.go
git commit -m "feat: file-level bloom sidecar — per-file bloom filter for trace_id/service skipping"
```

---

### Task 5: S3 range column reads — fetch only needed column chunks

**Files:**
- Create: `internal/storage/parquets3/range_reader.go`
- Create: `internal/storage/parquets3/range_reader_test.go`
- Modify: `internal/storage/parquets3/storage_query.go:236-240` (queryFile — use range reader for projected queries)
- Modify: `lakehouse-traces/internal/storage/parquets3/storage_query.go` (same)

**Rationale:** When a query projects only specific columns (e.g., timestamp-only for hits, or service.name for label lookups), downloading the entire Parquet file wastes bandwidth. Parquet stores column chunks at known offsets (from footer metadata). By using the already-cached footer + S3 range reads, we can fetch only the needed column chunks. For timestamp-only queries this could reduce S3 transfer by 80-90%.

**Existing building blocks:**
- `S3ReaderAt` implements `io.ReaderAt` — s3reader/reader.go:23-29
- `parquet.OpenFile` accepts `io.ReaderAt` — can use S3ReaderAt directly
- Footer cache provides file metadata without S3 read
- `queryColumns` returns set of needed columns

- [ ] **Step 1: Write test for range-based column reader**

```go
// internal/storage/parquets3/range_reader_test.go
package parquets3

import (
	"testing"
)

func TestEstimateColumnChunkBytes(t *testing.T) {
	// With a 100KB file, 10 columns, and requesting 2 columns:
	// estimated bytes = (2/10) * 100KB + footer overhead
	est := estimateColumnChunkBytes(100*1024, 10, 2, 4096)
	if est >= 100*1024 {
		t.Errorf("estimate %d should be less than full file %d", est, 100*1024)
	}
	if est < 20*1024 {
		t.Errorf("estimate %d too small for 2/10 columns", est)
	}
}

func TestShouldUseRangeRead(t *testing.T) {
	// Small file — not worth range reads
	if shouldUseRangeRead(10*1024, 2, 10) {
		t.Error("small files should use full download")
	}

	// Large file with few projected columns — use range reads
	if !shouldUseRangeRead(1024*1024, 1, 10) {
		t.Error("large file with 1/10 columns should use range reads")
	}

	// All columns projected — not worth it
	if shouldUseRangeRead(1024*1024, 10, 10) {
		t.Error("all columns projected, should use full download")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test -count=1 -run "TestEstimateColumnChunkBytes|TestShouldUseRangeRead" github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3 -v`
Expected: FAIL

- [ ] **Step 3: Implement range reader decision logic**

Create `internal/storage/parquets3/range_reader.go`:

```go
package parquets3

const minFileSizeForRangeRead = 64 * 1024 // 64KB minimum
const rangeReadThreshold = 0.5            // use range reads when reading < 50% of columns

// shouldUseRangeRead returns true when S3 range reads would be more
// efficient than downloading the full file.
func shouldUseRangeRead(fileSize int64, projectedCols, totalCols int) bool {
	if fileSize < minFileSizeForRangeRead {
		return false
	}
	if totalCols == 0 || projectedCols == 0 {
		return false
	}
	ratio := float64(projectedCols) / float64(totalCols)
	return ratio < rangeReadThreshold
}

// estimateColumnChunkBytes estimates how many bytes we'd fetch with
// range reads for the projected columns.
func estimateColumnChunkBytes(fileSize int64, totalCols, projectedCols int, footerSize int) int64 {
	if totalCols == 0 {
		return fileSize
	}
	dataBytes := fileSize - int64(footerSize)
	if dataBytes < 0 {
		dataBytes = fileSize
	}
	estimated := (dataBytes * int64(projectedCols)) / int64(totalCols)
	return estimated + int64(footerSize)
}
```

- [ ] **Step 4: Integrate into queryFile — use S3ReaderAt for projected queries**

In `queryFile`, after footer cache check, if we have a cached footer AND projected columns AND should use range reads:

```go
// In queryFile, after footer cache hit check (around line 247-250):
// If footer is cached and we're projecting few columns, use S3ReaderAt
// instead of downloading full file.
if f != nil && projectedCols != nil && shouldUseRangeRead(fi.Size, len(projectedCols), len(f.Root().Fields())) {
	readerAt := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
	// Use the cached footer's file handle — parquet-go will use
	// ReaderAt for on-demand column chunk reads
	// This avoids s.getFileData entirely
	metrics.S3RequestsTotal.Inc("RANGE_READ")
	// ... process row groups using readerAt ...
}
```

Note: Full integration depends on parquet-go's ability to read column chunks lazily via `io.ReaderAt`. The `S3ReaderAt` (s3reader/reader.go:23) already implements this interface. The key is passing it to `parquet.OpenFile` instead of a `bytes.Reader` wrapping the full file data.

- [ ] **Step 5: Run tests**

Run: `GOWORK=off go test -count=1 -timeout=120s github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/storage/parquets3/range_reader.go internal/storage/parquets3/range_reader_test.go \
  internal/storage/parquets3/storage_query.go lakehouse-traces/internal/storage/parquets3/storage_query.go
git commit -m "feat: S3 range column reads — fetch only needed column chunks for projected queries"
```

---

### Task 6: Increase trace benchmark volume to 50K

**Files:**
- Modify: `deployment/docker/docker-compose-benchmark.yml` (datagen-seed command)
- Modify: `scripts/comparative-benchmark-traces.sh` (add throughput measurement)
- Modify: `scripts/comparative-benchmark.sh` (add throughput measurement)

- [ ] **Step 1: Update datagen to 50K traces**

In `deployment/docker/docker-compose-benchmark.yml`, change datagen-seed command:

```yaml
command:
  - "--logs=500000"
  - "--traces=50000"   # was 5000
  - "--hours-back=168"
```

- [ ] **Step 2: Add throughput measurement to benchmark scripts**

In both `comparative-benchmark.sh` and `comparative-benchmark-traces.sh`, update the `measure_query` function to capture response body size and compute throughput:

```bash
# In measure_query function, change curl to also capture size:
local size_bytes
size_bytes=$(curl -sf -o /dev/null -w "%{size_download}" "$url" 2>/dev/null) || size_bytes="0"
# Add to JSON output:
echo "{\"name\":\"$name\",\"system\":\"$system\",\"p50_ms\":${sorted[$p50_idx]},\"p95_ms\":${sorted[$p95_idx]},\"p99_ms\":${sorted[$p99_idx]},\"min_ms\":${sorted[0]},\"max_ms\":${sorted[$((n-1))]},\"iterations\":$n,\"errors\":$errors,\"avg_bytes\":$size_bytes}"
```

- [ ] **Step 3: Rebuild, re-seed, re-run**

```bash
docker compose -f deployment/docker/docker-compose-benchmark.yml build datagen-seed
docker compose -f deployment/docker/docker-compose-benchmark.yml down -v
docker compose -f deployment/docker/docker-compose-benchmark.yml up -d
# Wait for datagen to complete, then run both benchmarks
```

- [ ] **Step 4: Commit**

```bash
git add deployment/docker/docker-compose-benchmark.yml scripts/comparative-benchmark.sh \
  scripts/comparative-benchmark-traces.sh
git commit -m "feat: increase trace benchmark to 50K spans, add throughput measurement"
```

---

## Expected results after all optimizations

| Scenario | Before | After | Improvement |
|---|---|---|---|
| hits 24h | 3796ms | <50ms | 76x (partition counts, no file reads) |
| stats 24h count | 4013ms | <50ms | 80x (partition counts) |
| trace_id hit (cold) | 8550ms | <500ms | 17x (file bloom + footer prefetch) |
| trace_id miss | 8614ms | <100ms | 86x (file bloom immediate rejection) |
| 24h wildcard | 3878ms | ~2000ms | 2x (footer prefetch skips some, range reads reduce transfer) |
| 48h service | 9695ms | <1000ms | 10x (column stats + label index + footer prefetch) |
| 48h errors | 8973ms | <1000ms | 9x (column stats + file bloom) |

**Note:** Wildcard queries (24h `*`) cannot benefit from bloom/stats filtering — they must read all files. Range column reads (Task 5) provide the main improvement there by reducing transfer size.
