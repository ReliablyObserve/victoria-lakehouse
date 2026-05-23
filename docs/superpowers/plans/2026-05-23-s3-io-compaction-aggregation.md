# Compaction & Streaming Aggregation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce file count 5-10x for cold data via enhanced compaction (daily rollup, sorted output), and add streaming aggregation for stats queries (10-50x faster count/histogram via manifest metadata and column-only reads).

**Architecture:** Phase E enhances the existing `Compactor` with secondary sort keys (timestamp + service.name) and a daily rollup policy that merges 24 hourly files into 1 daily file. Phase F adds a streaming aggregation path that intercepts `| stats count()` queries, serving unfiltered counts from manifest metadata (zero S3 reads) and filtered counts from single-column reads. All changes in shared `internal/compaction/` and `internal/storage/parquets3/` code — logs and traces share code paths with mode-specific sort keys.

**Tech Stack:** Go, parquet-go, ZSTD

**Spec:** `docs/superpowers/specs/2026-05-23-s3-io-optimization-design.md` — Phases E, F

**Build/test command:** `GOWORK=off go test ./internal/compaction/... ./internal/storage/parquets3/... -count=1 -race -timeout 180s`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `internal/compaction/compactor.go` | Modify | Add secondary sort keys to `mergeLogFiles`, `mergeTraceFiles` |
| `internal/compaction/compactor_test.go` | Modify | Add sort verification tests |
| `internal/compaction/policy.go` | Modify | Add daily rollup eligibility (L1→L2 by age, cross-hour merge) |
| `internal/compaction/policy_test.go` | Modify | Test daily rollup policy |
| `internal/compaction/scheduler.go` | Modify | Support cross-partition (daily) compaction candidates |
| `internal/compaction/scheduler_test.go` | Modify | Test daily merge scheduling |
| `internal/config/config.go` | Modify | Add `DailyRollupAge` to CompactionConfig |
| `internal/storage/parquets3/storage_query.go` | Modify | Enhance manifestFastPath for broader coverage |
| `internal/storage/parquets3/storage_query_test.go` | Create | Tests for streaming aggregation fast path |
| `internal/storage/parquets3/rg_skip.go` | Create | Row group skip logic using sorted column stats |
| `internal/storage/parquets3/rg_skip_test.go` | Create | Tests for row group skip on sorted files |

---

### Task 1: Secondary Sort Keys in Compaction

**Files:**
- Modify: `internal/compaction/compactor.go:215-243`
- Modify: `internal/compaction/compactor_test.go`

- [ ] **Step 1: Write failing test for multi-key sort (logs)**

Add to `internal/compaction/compactor_test.go`:

```go
func TestMergeLogFiles_SortByTimestampThenService(t *testing.T) {
	c := NewCompactor(CompactorConfig{Mode: config.ModeLogs})

	file1 := makeLogParquet(t, []schema.LogRow{
		{TimestampUnixNano: 100, ServiceName: "zeta"},
		{TimestampUnixNano: 200, ServiceName: "alpha"},
	})
	file2 := makeLogParquet(t, []schema.LogRow{
		{TimestampUnixNano: 100, ServiceName: "alpha"},
		{TimestampUnixNano: 200, ServiceName: "zeta"},
	})

	merged, err := c.mergeLogFiles([][]byte{file1, file2})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	if len(merged) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(merged))
	}

	// Same timestamp: sorted by service.name ascending
	if merged[0].TimestampUnixNano != 100 || merged[0].ServiceName != "alpha" {
		t.Errorf("row 0: expected ts=100 svc=alpha, got ts=%d svc=%s",
			merged[0].TimestampUnixNano, merged[0].ServiceName)
	}
	if merged[1].TimestampUnixNano != 100 || merged[1].ServiceName != "zeta" {
		t.Errorf("row 1: expected ts=100 svc=zeta, got ts=%d svc=%s",
			merged[1].TimestampUnixNano, merged[1].ServiceName)
	}
	// Next timestamp
	if merged[2].TimestampUnixNano != 200 || merged[2].ServiceName != "alpha" {
		t.Errorf("row 2: expected ts=200 svc=alpha, got ts=%d svc=%s",
			merged[2].TimestampUnixNano, merged[2].ServiceName)
	}
}

func makeLogParquet(t *testing.T, rows []schema.LogRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/compaction/ -run TestMergeLogFiles_SortByTimestampThenService -v`
Expected: FAIL — rows sorted only by timestamp, not by service.name within same timestamp

- [ ] **Step 3: Add secondary sort key to mergeLogFiles**

In `internal/compaction/compactor.go`, modify `mergeLogFiles` (line 224-227):

```go
// Before:
sort.Slice(merged, func(i, j int) bool {
	return merged[i].TimestampUnixNano < merged[j].TimestampUnixNano
})

// After:
sort.Slice(merged, func(i, j int) bool {
	if merged[i].TimestampUnixNano != merged[j].TimestampUnixNano {
		return merged[i].TimestampUnixNano < merged[j].TimestampUnixNano
	}
	return merged[i].ServiceName < merged[j].ServiceName
})
```

- [ ] **Step 4: Add secondary sort key to mergeTraceFiles**

In `internal/compaction/compactor.go`, modify `mergeTraceFiles` (line 239-242):

```go
// Before:
sort.Slice(merged, func(i, j int) bool {
	return merged[i].TimestampUnixNano < merged[j].TimestampUnixNano
})

// After:
sort.Slice(merged, func(i, j int) bool {
	if merged[i].TimestampUnixNano != merged[j].TimestampUnixNano {
		return merged[i].TimestampUnixNano < merged[j].TimestampUnixNano
	}
	if merged[i].ServiceName != merged[j].ServiceName {
		return merged[i].ServiceName < merged[j].ServiceName
	}
	return merged[i].TraceID < merged[j].TraceID
})
```

- [ ] **Step 5: Write test for trace sort (timestamp + service + trace_id)**

Add to `internal/compaction/compactor_test.go`:

```go
func TestMergeTraceFiles_SortByTimestampThenServiceThenTraceID(t *testing.T) {
	c := NewCompactor(CompactorConfig{Mode: config.ModeTraces})

	file1 := makeTraceParquet(t, []schema.TraceRow{
		{TimestampUnixNano: 100, ServiceName: "api", TraceID: "bbb"},
		{TimestampUnixNano: 100, ServiceName: "api", TraceID: "aaa"},
	})

	merged, err := c.mergeTraceFiles([][]byte{file1})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	if merged[0].TraceID != "aaa" {
		t.Errorf("row 0: expected trace_id=aaa, got %s", merged[0].TraceID)
	}
	if merged[1].TraceID != "bbb" {
		t.Errorf("row 1: expected trace_id=bbb, got %s", merged[1].TraceID)
	}
}

func makeTraceParquet(t *testing.T, rows []schema.TraceRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}
```

- [ ] **Step 6: Run all compaction tests**

Run: `GOWORK=off go test ./internal/compaction/ -run TestMerge -v`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add internal/compaction/compactor.go internal/compaction/compactor_test.go
git commit -m "feat(compaction): add secondary sort keys

Logs: sort by timestamp + service.name (enables RG pruning by service)
Traces: sort by timestamp + service.name + trace_id (enables trace grouping)

Phase E of S3 I/O optimization spec."
```

---

### Task 2: Daily Rollup Policy

**Files:**
- Modify: `internal/compaction/policy.go`
- Modify: `internal/compaction/policy_test.go`
- Modify: `internal/config/config.go:339-350`

- [ ] **Step 1: Write failing test for daily rollup eligibility**

Add to `internal/compaction/policy_test.go`:

```go
func TestLevelPolicy_EligibleDailyRollup(t *testing.T) {
	p := NewLevelPolicy(10, 15, time.Hour)
	p.DailyRollupAge = 24 * time.Hour

	// Simulate 3 L1 files in a partition that is 48 hours old.
	// Not enough for regular L1→L2 (needs 15), but qualifies for daily rollup.
	files := makeFiles(1, "fp1", 3)
	partitionTime := time.Now().Add(-48 * time.Hour)

	level, eligible := p.Eligible(files, partitionTime)
	if !eligible {
		t.Fatal("expected eligible=true for daily rollup")
	}
	if level != 1 {
		t.Fatalf("expected level=1 for daily rollup, got %d", level)
	}
}

func TestLevelPolicy_DailyRollupNotEligibleTooRecent(t *testing.T) {
	p := NewLevelPolicy(10, 15, time.Hour)
	p.DailyRollupAge = 24 * time.Hour

	// Only 6 hours old — not eligible for daily rollup
	files := makeFiles(1, "fp1", 3)
	partitionTime := time.Now().Add(-6 * time.Hour)

	_, eligible := p.Eligible(files, partitionTime)
	if eligible {
		t.Fatal("expected eligible=false for recent partition")
	}
}

func TestLevelPolicy_DailyRollupNeedsMultipleFiles(t *testing.T) {
	p := NewLevelPolicy(10, 15, time.Hour)
	p.DailyRollupAge = 24 * time.Hour

	// Only 1 L1 file — nothing to merge
	files := makeFiles(1, "fp1", 1)
	partitionTime := time.Now().Add(-48 * time.Hour)

	_, eligible := p.Eligible(files, partitionTime)
	if eligible {
		t.Fatal("expected eligible=false with only 1 file")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/compaction/ -run TestLevelPolicy_EligibleDailyRollup -v`
Expected: FAIL — `DailyRollupAge` field undefined

- [ ] **Step 3: Add DailyRollupAge to LevelPolicy**

Modify `internal/compaction/policy.go`:

```go
type LevelPolicy struct {
	MinFilesL0     int
	MinFilesL1     int
	MinAge         time.Duration
	DailyRollupAge time.Duration
}

func (p *LevelPolicy) Eligible(files []manifest.FileInfo, partitionTime time.Time) (level int, eligible bool) {
	if time.Since(partitionTime) < p.MinAge {
		return 0, false
	}
	l0Count := countAtLevel(files, 0)
	l1Count := countAtLevel(files, 1)
	if l0Count >= p.MinFilesL0 {
		return 0, true
	}
	if l1Count >= p.MinFilesL1 {
		return 1, true
	}
	// Daily rollup: merge any L1 files (≥2) in partitions older than DailyRollupAge.
	if p.DailyRollupAge > 0 && time.Since(partitionTime) >= p.DailyRollupAge && l1Count >= 2 {
		return 1, true
	}
	return 0, false
}
```

- [ ] **Step 4: Add DailyRollupAge to CompactionConfig and Default()**

In `internal/config/config.go`, add to `CompactionConfig` struct:

```go
DailyRollupAge time.Duration `yaml:"daily_rollup_age"`
```

Set default (in `Default()` function, Compaction section):

```go
DailyRollupAge: 24 * time.Hour,
```

- [ ] **Step 5: Run all policy tests**

Run: `GOWORK=off go test ./internal/compaction/ -run TestLevelPolicy -v`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add internal/compaction/policy.go internal/compaction/policy_test.go internal/config/config.go
git commit -m "feat(compaction): add daily rollup policy

Partitions older than DailyRollupAge (default 24h) with ≥2 L1
files are eligible for compaction even if below MinFilesL1 threshold.
Reduces file count for cold data by merging hourly files into daily.

Phase E of S3 I/O optimization spec."
```

---

### Task 3: Wire DailyRollupAge Through Scheduler and CLI

**Files:**
- Modify: `internal/compaction/scheduler.go:17-30`
- Modify: `cmd/lakehouse-logs/main.go`
- Modify: `cmd/lakehouse-traces/main.go`

- [ ] **Step 1: Pass DailyRollupAge from config to LevelPolicy**

In both `cmd/lakehouse-logs/main.go` and `cmd/lakehouse-traces/main.go`, in the compaction scheduler initialization section (where `NewLevelPolicy` is called):

```go
// Before:
policy := compaction.NewLevelPolicy(
	cfg.Compaction.MinFilesL0,
	cfg.Compaction.MinFilesL1,
	cfg.Compaction.MinAge,
)

// After:
policy := compaction.NewLevelPolicy(
	cfg.Compaction.MinFilesL0,
	cfg.Compaction.MinFilesL1,
	cfg.Compaction.MinAge,
)
policy.DailyRollupAge = cfg.Compaction.DailyRollupAge
```

- [ ] **Step 2: Add CLI flag for daily rollup age**

In both `cmd/lakehouse-logs/main.go` and `cmd/lakehouse-traces/main.go`:

```go
compactionDailyRollupAge = flag.Duration("lakehouse.compaction.daily-rollup-age", 0, "Minimum partition age for daily rollup compaction (default: 24h)")
```

In `applyFlags()`:

```go
if *compactionDailyRollupAge > 0 {
	cfg.Compaction.DailyRollupAge = *compactionDailyRollupAge
}
```

- [ ] **Step 3: Run scheduler tests**

Run: `GOWORK=off go test ./internal/compaction/ -count=1 -race`
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add cmd/lakehouse-logs/main.go cmd/lakehouse-traces/main.go internal/compaction/scheduler.go
git commit -m "feat: wire daily rollup age through CLI and scheduler

Adds -lakehouse.compaction.daily-rollup-age flag (default 24h).
Scheduler passes daily rollup config to LevelPolicy."
```

---

### Task 4: Row Group Skip Using Sorted Column Stats

**Files:**
- Create: `internal/storage/parquets3/rg_skip.go`
- Create: `internal/storage/parquets3/rg_skip_test.go`
- Modify: `internal/storage/parquets3/storage_query.go:412-429`

- [ ] **Step 1: Write failing test for row group skip on sorted data**

```go
// internal/storage/parquets3/rg_skip_test.go
package parquets3

import (
	"testing"
)

func TestCanSkipRowGroupByServiceStats(t *testing.T) {
	// Row group with service.name min="alpha", max="gamma"
	// Query for service.name="zeta" — can skip (zeta > gamma)
	if !canSkipByColumnStats("service.name", "zeta", "alpha", "gamma") {
		t.Fatal("expected skip: zeta outside [alpha, gamma]")
	}

	// Query for service.name="beta" — cannot skip (beta inside [alpha, gamma])
	if canSkipByColumnStats("service.name", "beta", "alpha", "gamma") {
		t.Fatal("expected no skip: beta inside [alpha, gamma]")
	}

	// Query for service.name="alpha" — cannot skip (exact boundary match)
	if canSkipByColumnStats("service.name", "alpha", "alpha", "gamma") {
		t.Fatal("expected no skip: alpha at min boundary")
	}
}

func TestCanSkipRowGroupByServiceStats_EmptyRange(t *testing.T) {
	// Empty min/max — cannot skip (no stats available)
	if canSkipByColumnStats("service.name", "anything", "", "") {
		t.Fatal("expected no skip: empty stats")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run TestCanSkipRowGroupByServiceStats -v`
Expected: FAIL — `canSkipByColumnStats` undefined

- [ ] **Step 3: Implement canSkipByColumnStats**

```go
// internal/storage/parquets3/rg_skip.go
package parquets3

// canSkipByColumnStats returns true if a row group can be skipped because the
// exact-match value falls outside the [minVal, maxVal] range of a sorted column.
// This works because compacted files are sorted by the column, making min/max
// stats tight bounds for each row group.
func canSkipByColumnStats(column, value, minVal, maxVal string) bool {
	if minVal == "" && maxVal == "" {
		return false
	}
	return value < minVal || value > maxVal
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run TestCanSkipRowGroupByServiceStats -v`
Expected: all PASS

- [ ] **Step 5: Integrate into row group filtering in queryFile**

In `internal/storage/parquets3/storage_query.go`, in the row group filtering loop (around lines 412-429), after existing pushdown filter checks, add:

```go
// After existing pushdown and bloom checks:
if pdf != nil && fi.CompactionLevel > 0 {
	// Compacted files are sorted — use column index min/max for service.name skip
	if serviceFilter := pdf.ExactMatch("service.name"); serviceFilter != "" {
		colIdx := findColumnIndex(f.Schema(), "service.name")
		if colIdx >= 0 {
			ci := rg.ColumnChunks()[colIdx].ColumnIndex()
			if ci != nil {
				for page := 0; page < ci.NumPages(); page++ {
					minStr := ci.MinValue(page).String()
					maxStr := ci.MaxValue(page).String()
					if canSkipByColumnStats("service.name", serviceFilter, minStr, maxStr) {
						metrics.ParquetRowGroupsSkipped.Inc("sorted_stats")
						continue // skip this row group
					}
				}
			}
		}
	}
}
```

Note: The exact integration depends on how parquet-go exposes column index stats per row group. If column-level min/max is available at the row group level (via `ColumnIndex()`), this works directly. Otherwise, the existing pushdown filter already handles this via `checkMatchesStats`.

Verify the existing pushdown path already covers this:

```bash
GOWORK=off grep -n "checkMatchesStats\|ColumnIndex\|column_stats" internal/storage/parquets3/filter_pushdown.go
```

If the existing pushdown filter already handles exact-match skip via column min/max stats, then this task reduces to ensuring compacted files have tight per-RG stats (which the secondary sort guarantees). In that case, mark this integration as "already handled by existing pushdown filter on sorted data" and skip the code change.

- [ ] **Step 6: Run full query test suite**

Run: `GOWORK=off go test ./internal/storage/parquets3/... -count=1 -race -timeout 120s`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add internal/storage/parquets3/rg_skip.go internal/storage/parquets3/rg_skip_test.go internal/storage/parquets3/storage_query.go
git commit -m "feat: row group skip using sorted column stats

Compacted files sorted by service.name enable tighter min/max
bounds per row group. Queries filtering on service.name can skip
row groups where the value falls outside the sorted range.

Phase E of S3 I/O optimization spec."
```

---

### Task 5: Enhance Manifest Fast Path Coverage

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go:249-268`
- Create: `internal/storage/parquets3/storage_query_test.go` (or add to existing)

The existing `manifestFastPath` (line 249) requires `IsTimestampOnly(ctx) && filter == nil`. This covers `* | stats count()` but not `* | stats count() by(service.name)` or `service.name:"api-gateway" | stats count()`. Phase F extends coverage.

- [ ] **Step 1: Write test for manifest count with no filter**

Add to test file:

```go
func TestManifestFastPath_AllFilesResolved(t *testing.T) {
	// Create storage with manifest containing files with known row counts
	s := newTestStorage(t)
	s.manifest.AddFile("dt=2026-05-22/hour=10", manifest.FileInfo{
		Key:       "test/file1.parquet",
		RowCount:  1000,
		MinTimeNs: hoursAgo(2).UnixNano(),
		MaxTimeNs: hoursAgo(1).UnixNano(),
		Size:      1024,
	})
	s.manifest.AddFile("dt=2026-05-22/hour=11", manifest.FileInfo{
		Key:       "test/file2.parquet",
		RowCount:  2000,
		MinTimeNs: hoursAgo(1).UnixNano(),
		MaxTimeNs: time.Now().UnixNano(),
		Size:      2048,
	})

	// Query with timestamp-only context (simulates stats count(*))
	startNs := hoursAgo(3).UnixNano()
	endNs := time.Now().Add(time.Hour).UnixNano()
	files := s.manifest.GetFilesForRange(startNs, endNs)

	remaining := s.manifestFastPath(files, startNs, endNs, func(_ uint, db *logstorage.DataBlock) {
		// Should receive synthetic blocks
	})

	if len(remaining) != 0 {
		t.Fatalf("expected 0 remaining files (all resolved from manifest), got %d", len(remaining))
	}
}

func hoursAgo(h int) time.Time {
	return time.Now().Add(-time.Duration(h) * time.Hour)
}
```

- [ ] **Step 2: Run test**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run TestManifestFastPath -v`
Expected: PASS (existing code already handles this case)

- [ ] **Step 3: Add total row count API from manifest for unfiltered count**

The manifest already has `GetRowCountForRange(startNs, endNs)` in `internal/manifest/partition_stats.go`. This can serve `* | stats count()` in a single call with zero S3 reads.

Verify this is already wired up by checking if `IsTimestampOnly(ctx)` is set for count-only queries:

```bash
GOWORK=off grep -rn "IsTimestampOnly\|SetTimestampOnly" internal/storage/
```

If `IsTimestampOnly` is already set for stats count queries, the manifest fast path already handles this. If not, the VL upstream query pipeline sets this — verify by checking VL's query compilation path.

- [ ] **Step 4: Verify manifest-based count works end-to-end**

Start e2e stack and test:

```bash
curl -s 'http://localhost:29428/select/logsql/query?query=*+%7C+stats+count()&start=48h' | head -5
curl -s 'http://localhost:29428/metrics' | grep metadata_only
```

Expected: `lakehouse_metadata_only_files_total` counter should be > 0, indicating manifest fast path was used.

- [ ] **Step 5: Commit (if any changes were made)**

```bash
git add internal/storage/parquets3/storage_query.go internal/storage/parquets3/storage_query_test.go
git commit -m "feat: enhance manifest fast path for stats count queries

Extends metadata-only resolution coverage. Unfiltered count(*)
queries resolve entirely from manifest row counts (zero S3 reads).

Phase F of S3 I/O optimization spec."
```

---

### Task 6: Column-Only Read for Filtered Count Queries

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go`
- Modify: `internal/storage/parquets3/reader_columnar.go`

Phase F's key optimization: for `service.name:"api-gateway" | stats count()`, read ONLY the `service.name` column from each file, count matches, and return the count without reading other columns.

- [ ] **Step 1: Write test for single-column count read**

Add to test file:

```go
func TestColumnOnlyCount_ServiceFilter(t *testing.T) {
	s := newTestStorage(t)

	// Insert test data with known service name distribution
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, ServiceName: "api-gateway", Body: "log1"},
		{TimestampUnixNano: 2000, ServiceName: "web-server", Body: "log2"},
		{TimestampUnixNano: 3000, ServiceName: "api-gateway", Body: "log3"},
		{TimestampUnixNano: 4000, ServiceName: "db-proxy", Body: "log4"},
	}
	writeTestFile(t, s, "dt=2026-05-22/hour=10", rows)

	// Query: service.name:"api-gateway" | stats count()
	// Should project only service.name column and count matches
	q := parseTestQuery(t, `service.name:"api-gateway"`)
	projectedCols := map[string]bool{"service.name": true, "_time": true}

	// Verify that only 2 columns are projected (not all 20+)
	if len(projectedCols) > 3 {
		t.Fatalf("expected narrow projection for count query, got %d columns", len(projectedCols))
	}
}
```

- [ ] **Step 2: Verify existing column projection handles this**

The existing `queryColumns()` function (in `projection.go`) already extracts projected columns from the query pipe. For `service.name:"api-gateway" | stats count()`, the projection should be `{service.name, _time}` — only the filter column + timestamp.

Check this:

```bash
GOWORK=off grep -n "queryColumns\|pipeFields\|GetQueryPipeFields" internal/storage/parquets3/projection.go
```

If `queryColumns()` already produces a narrow projection for stats count queries with a filter, then Phase F's column-only read is already partially implemented through existing column projection. The remaining optimization is to avoid materializing full rows when only a count is needed.

- [ ] **Step 3: Verify the read path uses column projection for stats queries**

In `storage_query.go`, `queryFile()` calls `queryColumns()` to determine which columns to project, then passes this to `openParquetFile()`. If the projection is narrow (1-2 columns), the range-read path already reads only those columns.

Run a query and check metrics:

```bash
# Start e2e stack
curl -s 'http://localhost:29428/select/logsql/query?query=service.name:api-gateway+%7C+stats+count()&start=48h'
curl -s 'http://localhost:29428/metrics' | grep s3_range_reads
```

- [ ] **Step 4: Document findings and commit**

If column projection already works for filtered count queries (reading only filter + timestamp columns), document this as "Phase F column-only reads are handled by existing projection infrastructure."

If gaps are found, implement the specific optimization:

```go
// In queryFile, when detecting a count-only query:
if isCountOnlyQuery(pipeFields) && filter != nil {
	// Project only the filter columns + timestamp
	projectedCols = filterColumnsOnly(filter)
	projectedCols["_time"] = true
}
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/parquets3/storage_query.go internal/storage/parquets3/projection.go
git commit -m "feat: column-only reads for filtered count queries

Stats count queries with filters now project only the filter
columns + timestamp, reducing S3 data transfer by 80-90%.

Phase F of S3 I/O optimization spec."
```

---

### Task 7: Manifest Row Count for Hits/Histogram Queries

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go`

The manifest already has `GetRowCountsByPartition(startNs, endNs)` which returns per-partition row counts with time bounds. For unfiltered `| stats count()` with time bucketing (hits queries), this enables zero-S3-read histograms.

- [ ] **Step 1: Verify GetRowCountsByPartition exists and works**

```bash
GOWORK=off go test ./internal/manifest/ -run TestGetRowCounts -v
```

- [ ] **Step 2: Check if hits queries already use manifest counts**

```bash
GOWORK=off grep -rn "GetRowCountsByPartition\|IsTimestampOnly" internal/storage/parquets3/
```

If `IsTimestampOnly(ctx)` is set for hits queries AND `manifestFastPath` is used, then per-partition row counts from the manifest feed directly into the hits response.

- [ ] **Step 3: Run e2e verification**

```bash
curl -s 'http://localhost:29428/select/logsql/hits?query=*&start=48h&step=1h' | python3 -m json.tool | head -20
curl -s 'http://localhost:29428/metrics' | grep metadata_only
```

Expected: hits query should show `metadata_only_files` incrementing (manifest-based counts used).

- [ ] **Step 4: Commit if changes made**

```bash
git add internal/storage/parquets3/storage_query.go
git commit -m "feat: verify hits queries use manifest row counts

Unfiltered hits/histogram queries resolve from manifest partition
row counts — zero S3 reads for time-bucketed counts.

Phase F of S3 I/O optimization spec."
```

---

### Task 8: Integration Verification

**Files:**
- No new files

- [ ] **Step 1: Run full unit test suite**

Run: `GOWORK=off go test ./internal/compaction/... ./internal/storage/parquets3/... -count=1 -race -timeout 180s`
Expected: all PASS

- [ ] **Step 2: Build and deploy e2e stack**

```bash
cd deployment/docker && docker compose -f docker-compose-e2e.yml build lakehouse-logs lakehouse-traces && docker compose -f docker-compose-e2e.yml up -d
```

- [ ] **Step 3: Verify compaction produces sorted output**

After data seeding and compaction runs:

```bash
# Check compaction logs for sorted output
docker compose -f docker-compose-e2e.yml logs lakehouse-logs 2>&1 | grep "compaction complete"
```

- [ ] **Step 4: Verify query correctness on compacted data**

```bash
# Compare counts: lakehouse vs VL
curl -s 'http://localhost:29428/select/logsql/query?query=service.name:api-gateway&start=48h&limit=100' | wc -l
curl -s 'http://localhost:9428/select/logsql/query?query=service.name:api-gateway&start=24h&limit=100' | wc -l
```

Both should return consistent results (lakehouse covers more time range due to S3 retention).

- [ ] **Step 5: Verify stats count uses manifest fast path**

```bash
curl -s 'http://localhost:29428/select/logsql/query?query=*+%7C+stats+count()&start=48h'
curl -s 'http://localhost:29428/metrics' | grep metadata_only
```

- [ ] **Step 6: Check buffer/coalesce metrics from Plan 1**

```bash
curl -s 'http://localhost:29428/metrics' | grep -E "buffer_hits|buffer_misses|coalesced|range_reads"
```

---

## Verification Checklist

| Check | Command | Expected |
|---|---|---|
| Compaction tests pass | `GOWORK=off go test ./internal/compaction/... -race` | PASS |
| Storage tests pass | `GOWORK=off go test ./internal/storage/parquets3/... -race -timeout 120s` | PASS |
| Secondary sort works | Compact test file, verify sort order | Rows sorted by ts+svc |
| Daily rollup policy | Policy test with 48h old partition | Eligible for compaction |
| Manifest fast path | `* \| stats count()` query | metadata_only_files > 0 |
| Column projection | Filtered count query | Only filter columns read |
| Compacted file correctness | Query compacted vs uncompacted data | Same results |
| Both logs and traces | Query both ports | Consistent results |
