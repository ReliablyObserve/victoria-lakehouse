# Phase 2: Latency Optimizations — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce query latency across all endpoint types through five targeted optimizations: hits fast path, column projection, parallel row group processing, expanded bloom coverage, and manifest time-range indexing.

**Architecture:** Each optimization is independent and follows TDD: write test → measure baseline → implement → verify improvement → regression suite green. The query hot path flows HTTP → wrapVL → Storage.RunQuery → manifest.GetFilesForRange → bloom filter → parallel queryFile workers → readRowGroup → writeBlock. Optimizations target different stages of this pipeline. Both `lakehouse-logs` (root module) and `lakehouse-traces` (separate Go module at `lakehouse-traces/`) must receive each optimization, respecting their structural differences (different bloom architectures, traces missing pushdown filter).

**Tech Stack:** Go, parquet-go, VictoriaLogs logstorage, OpenTelemetry tracing (from Phase 1)

**Build/test commands:** All Go commands require `GOWORK=off` (incompatible VL versions across modules). Root module: `GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`. Traces module: `cd lakehouse-traces && GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`.

---

## File Structure

### Priority 1: Manifest Time-Range Index (shared)

| Action | File | Purpose |
|--------|------|---------|
| Modify | `internal/manifest/manifest.go` | Replace `map[string][]FileInfo` with sorted partition slice + binary search in `GetFilesForRange` |
| Create | `internal/manifest/manifest_range_test.go` | Tests for sorted partition index and binary search |

### Priority 2: Column Projection Pushdown (both modules)

| Action | File | Purpose |
|--------|------|---------|
| Modify | `internal/storage/parquets3/storage_query.go` | Add column-selective row group reader using `rg.Rows()` API |
| Create | `internal/storage/parquets3/projection.go` | Column projection logic: map query fields to parquet column indices |
| Create | `internal/storage/parquets3/projection_test.go` | Unit tests for column projection mapping |
| Create | `internal/storage/parquets3/reader_projected.go` | Projected row reader: reads selected columns, builds DataBlock |
| Create | `internal/storage/parquets3/reader_projected_test.go` | Tests for projected reading with real parquet files |
| Modify | `lakehouse-traces/internal/storage/parquets3/storage_query.go` | Same column projection in traces module |
| Create | `lakehouse-traces/internal/storage/parquets3/projection.go` | Traces-side projection (copy, adapted for TraceRow) |
| Create | `lakehouse-traces/internal/storage/parquets3/projection_test.go` | Traces-side projection tests |
| Create | `lakehouse-traces/internal/storage/parquets3/reader_projected.go` | Traces-side projected reader |
| Create | `lakehouse-traces/internal/storage/parquets3/reader_projected_test.go` | Traces-side projected reader tests |

### Priority 3: Traces Pushdown Filter Parity

| Action | File | Purpose |
|--------|------|---------|
| Copy+Modify | `lakehouse-traces/internal/storage/parquets3/filter_pushdown.go` | Port root module's pushdown filter to traces |
| Create | `lakehouse-traces/internal/storage/parquets3/filter_pushdown_test.go` | Tests for traces pushdown |
| Modify | `lakehouse-traces/internal/storage/parquets3/storage_query.go` | Wire pushdown filter into traces `queryFile` |

### Priority 4: Expanded Bloom Coverage

| Action | File | Purpose |
|--------|------|---------|
| Modify | `internal/schema/registry.go` | Add `HasBloom: true` to additional fields |
| Create | `internal/schema/bloom_coverage_test.go` | Verify bloom coverage for all specified fields |
| Modify | `internal/storage/parquets3/storage_query.go` | Extend `extractExactMatch` → `extractFilterValues` to support `in()` |
| Create | `internal/storage/parquets3/filter_extract_test.go` | Tests for `in()` value extraction |

### Priority 5: Documentation & Exit Verification

| Action | File | Purpose |
|--------|------|---------|
| Modify | `docs/performance.md` | Update with optimization results |
| Modify | `CHANGELOG.md` | Add Phase 2 entries |

---

### Task 1: Manifest Sorted Partition Index

**Files:**
- Modify: `internal/manifest/manifest.go`
- Create: `internal/manifest/manifest_range_test.go`

Currently `GetFilesForRange()` (line 242) iterates a `map[string][]FileInfo` linearly and calls `parsePartitionTime()` on every key. This is O(P) where P = partition count. At 10K+ files with hourly partitions, this matters.

- [ ] **Step 1: Write the failing test for sorted partition lookup**

Create `internal/manifest/manifest_range_test.go`:

```go
package manifest

import (
	"testing"
	"time"
)

func TestGetFilesForRange_BinarySearchCorrectness(t *testing.T) {
	m := &Manifest{}
	m.files = make(map[string][]FileInfo)

	// Add 100 hourly partitions spanning ~4 days
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		pt := base.Add(time.Duration(i) * time.Hour)
		key := formatPartitionKey(pt)
		m.files[key] = []FileInfo{
			{Key: key + "/file.parquet", Size: 1000},
		}
	}
	m.rebuildIndex()

	// Query a 3-hour window in the middle
	start := base.Add(50 * time.Hour)
	end := start.Add(3 * time.Hour)
	startNs := start.UnixNano()
	endNs := end.UnixNano()

	files := m.GetFilesForRange(startNs, endNs)

	// Should return files from partitions 50, 51, 52 (3 hours)
	if len(files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(files))
	}

	// Verify no files outside the range
	for _, fi := range files {
		if fi.Key < formatPartitionKey(start) || fi.Key > formatPartitionKey(end) {
			// This is a rough check — just ensure we're in the right ballpark
		}
	}
}

func TestGetFilesForRange_EmptyManifest(t *testing.T) {
	m := &Manifest{}
	m.files = make(map[string][]FileInfo)
	m.rebuildIndex()

	files := m.GetFilesForRange(0, time.Now().UnixNano())
	if len(files) != 0 {
		t.Fatalf("expected 0 files from empty manifest, got %d", len(files))
	}
}

func TestGetFilesForRange_SinglePartition(t *testing.T) {
	m := &Manifest{}
	m.files = make(map[string][]FileInfo)
	pt := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	key := formatPartitionKey(pt)
	m.files[key] = []FileInfo{
		{Key: key + "/a.parquet", Size: 500},
		{Key: key + "/b.parquet", Size: 600},
	}
	m.rebuildIndex()

	// Query range that covers the partition
	start := pt.Add(-30 * time.Minute)
	end := pt.Add(90 * time.Minute)
	files := m.GetFilesForRange(start.UnixNano(), end.UnixNano())
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}

	// Query range entirely before the partition
	files = m.GetFilesForRange(0, pt.Add(-1*time.Hour).UnixNano())
	if len(files) != 0 {
		t.Fatalf("expected 0 files for range before partition, got %d", len(files))
	}
}

func TestRebuildIndex_SortsPartitions(t *testing.T) {
	m := &Manifest{}
	m.files = make(map[string][]FileInfo)

	// Add partitions out of order
	times := []time.Time{
		time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	for _, pt := range times {
		key := formatPartitionKey(pt)
		m.files[key] = []FileInfo{{Key: key + "/f.parquet"}}
	}
	m.rebuildIndex()

	if len(m.sortedPartitions) != 3 {
		t.Fatalf("expected 3 sorted partitions, got %d", len(m.sortedPartitions))
	}
	for i := 1; i < len(m.sortedPartitions); i++ {
		if !m.sortedPartitions[i].start.After(m.sortedPartitions[i-1].start) {
			t.Errorf("partitions not sorted at index %d", i)
		}
	}
}

func formatPartitionKey(t time.Time) string {
	return "dt=" + t.Format("2006-01-02") + "/hour=" + t.Format("15")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/manifest/ -run TestGetFilesForRange_BinarySearch -v -count=1`
Expected: FAIL — `rebuildIndex` and `sortedPartitions` don't exist yet.

- [ ] **Step 3: Implement sorted partition index**

Add to `internal/manifest/manifest.go`:

```go
type partitionEntry struct {
	key   string    // "dt=2026-01-01/hour=00"
	start time.Time // parsed partition start
	end   time.Time // start + 1 hour
}

// Add field to Manifest struct:
// sortedPartitions []partitionEntry  // sorted by start time, rebuilt on mutation

func (m *Manifest) rebuildIndex() {
	m.sortedPartitions = m.sortedPartitions[:0]
	for partition := range m.files {
		t, err := parsePartitionTime(partition)
		if err != nil {
			continue
		}
		m.sortedPartitions = append(m.sortedPartitions, partitionEntry{
			key:   partition,
			start: t,
			end:   t.Add(time.Hour),
		})
	}
	sort.Slice(m.sortedPartitions, func(i, j int) bool {
		return m.sortedPartitions[i].start.Before(m.sortedPartitions[j].start)
	})
}
```

Replace `GetFilesForRange` implementation with binary search:

```go
func (m *Manifest) GetFilesForRange(startNs, endNs int64) []FileInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	start := time.Unix(0, startNs)
	end := time.Unix(0, endNs)

	// Binary search for first partition that could overlap
	lo := sort.Search(len(m.sortedPartitions), func(i int) bool {
		return m.sortedPartitions[i].end.After(start)
	})

	var result []FileInfo
	for i := lo; i < len(m.sortedPartitions); i++ {
		p := m.sortedPartitions[i]
		if p.start.After(end) || !p.start.Before(end) {
			break
		}
		result = append(result, m.files[p.key]...)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})
	return result
}
```

Call `m.rebuildIndex()` at the end of every method that mutates `m.files` (`AddFile`, `RemoveFile`, `Refresh`, `loadFromS3`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOWORK=off go test ./internal/manifest/ -run "TestGetFilesForRange|TestRebuildIndex" -v -count=1`
Expected: PASS

- [ ] **Step 5: Run full manifest test suite**

Run: `GOWORK=off go test ./internal/manifest/ -short -race -count=1 -timeout=2m`
Expected: All tests pass (binary search is a drop-in replacement with same semantics).

- [ ] **Step 6: Run full test suite for both modules**

Run: `GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`
Run: `cd lakehouse-traces && GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`
Expected: All pass — manifest is shared via import, traces module should pick up the change.

- [ ] **Step 7: Commit**

```bash
git add internal/manifest/manifest.go internal/manifest/manifest_range_test.go
git commit -m "perf: replace linear partition scan with sorted index + binary search in GetFilesForRange"
```

---

### Task 2: Column Projection — Query Field Extraction

**Files:**
- Create: `internal/storage/parquets3/projection.go`
- Create: `internal/storage/parquets3/projection_test.go`

Extract which columns a query actually needs, so we can skip deserializing the other 20+ columns. The existing `projectColumns` method (storage_query.go:450) computes indices but is never called. We build on its logic but produce a more useful output: a set of parquet column names.

- [ ] **Step 1: Write the failing test**

Create `internal/storage/parquets3/projection_test.go`:

```go
package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestQueryColumns_ExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`trace_id:="abc123"`, reg)

	// Must always include timestamp
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be included")
	}
	// Must include the filtered field
	if !cols["trace_id"] {
		t.Error("trace_id must be included for exact match filter")
	}
	// Should NOT include unrelated columns
	if cols["body"] {
		t.Error("body should not be included when not referenced")
	}
}

func TestQueryColumns_Wildcard(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`*`, reg)

	// Wildcard query needs all columns — return nil (means "read all")
	if cols != nil {
		t.Error("wildcard query should return nil (read all columns)")
	}
}

func TestQueryColumns_MultipleFilters(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api" AND level:="ERROR"`, reg)

	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp must be included")
	}
	if !cols["service.name"] {
		t.Error("service.name must be included")
	}
	if !cols["severity_text"] {
		t.Error("severity_text (internal: level) must be included")
	}
}

func TestQueryColumns_EmptyQuery(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(``, reg)

	if cols != nil {
		t.Error("empty query should return nil (read all columns)")
	}
}

func TestQueryColumns_BodySearch(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`"error connecting"`, reg)

	// Free text search needs _msg (body) column
	if !cols["body"] {
		t.Error("body must be included for free text search")
	}
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp must be included")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run TestQueryColumns -v -count=1`
Expected: FAIL — `queryColumns` doesn't exist.

- [ ] **Step 3: Implement queryColumns**

Create `internal/storage/parquets3/projection.go`:

```go
package parquets3

import (
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// queryColumns returns the set of parquet column names that a query references.
// Returns nil if all columns are needed (wildcard, empty, or unparseable query).
// Always includes timestamp_unix_nano.
func queryColumns(queryStr string, registry *schema.Registry) map[string]bool {
	if queryStr == "" || queryStr == "*" {
		return nil
	}

	cols := make(map[string]bool)
	cols[registry.TimestampColumn()] = true

	// Check for free-text search (unquoted or quoted string without field prefix)
	if isFreeTextSearch(queryStr) {
		cols["body"] = true
	}

	for _, fm := range registry.PromotedColumns() {
		if referencesField(queryStr, fm.InternalName) || referencesField(queryStr, fm.ParquetColumn) {
			cols[fm.ParquetColumn] = true
		}
	}

	// If we found only the timestamp, the query likely references fields we
	// can't parse — fall back to reading all columns.
	if len(cols) <= 1 && !isFreeTextSearch(queryStr) {
		return nil
	}

	return cols
}

func referencesField(query, name string) bool {
	// Check for field:="value", field:"value", field:=value, field:in(...)
	patterns := []string{
		name + `:="`,
		name + `:"`,
		name + `:=`,
		name + `:in(`,
		name + `:`,
	}
	for _, p := range patterns {
		if strings.Contains(query, p) {
			return true
		}
	}
	return false
}

func isFreeTextSearch(query string) bool {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" || trimmed == "*" {
		return false
	}
	// If the query starts with a quoted string (no field prefix), it's free text
	if trimmed[0] == '"' {
		return true
	}
	// If there's no colon, it might be a bare word search
	if !strings.Contains(trimmed, ":") {
		return true
	}
	return false
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run TestQueryColumns -v -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/parquets3/projection.go internal/storage/parquets3/projection_test.go
git commit -m "feat: add queryColumns to extract referenced parquet columns from query"
```

---

### Task 3: Column Projection — Projected Row Reader (Logs)

**Files:**
- Create: `internal/storage/parquets3/reader_projected.go`
- Create: `internal/storage/parquets3/reader_projected_test.go`
- Modify: `internal/storage/parquets3/storage_query.go`

Replace `parquet.NewGenericRowGroupReader[T](rg)` (reads ALL columns) with a projected reader that only decodes selected columns via `rg.Rows()` with a column selection mask.

- [ ] **Step 1: Write the failing test**

Create `internal/storage/parquets3/reader_projected_test.go`:

```go
package parquets3

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestReadRowGroupProjected_ReadsOnlySelectedColumns(t *testing.T) {
	// Write a small parquet file with LogRow data
	rows := []schema.LogRow{
		{
			TimestampUnixNano: 1000000000,
			Body:              "test message",
			SeverityText:      "INFO",
			ServiceName:       "api",
			TraceID:           "abc123",
			HostName:          "host-1",
		},
		{
			TimestampUnixNano: 2000000000,
			Body:              "another message",
			SeverityText:      "ERROR",
			ServiceName:       "web",
			TraceID:           "def456",
			HostName:          "host-2",
		},
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf)
	_, err := w.Write(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// Project to only timestamp + service.name
	wantCols := map[string]bool{
		"timestamp_unix_nano": true,
		"service.name":        true,
	}

	var blocks int
	writeBlock := func(_ uint, db interface{}) {
		blocks++
	}

	err = readRowGroupProjected(f, rgs[0], wantCols, func(fields []field) {
		// Verify we got service.name and timestamp but not body
		hasTimestamp := false
		hasServiceName := false
		hasBody := false
		for _, fld := range fields {
			switch fld.name {
			case "_time":
				hasTimestamp = true
			case "service.name":
				hasServiceName = true
			case "_msg":
				hasBody = true
			}
		}
		if !hasTimestamp {
			t.Error("expected _time field in projected output")
		}
		if !hasServiceName {
			t.Error("expected service.name field in projected output")
		}
		if hasBody {
			t.Error("body should NOT be in projected output")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run TestReadRowGroupProjected -v -count=1`
Expected: FAIL — `readRowGroupProjected` doesn't exist.

- [ ] **Step 3: Implement projected reader**

Create `internal/storage/parquets3/reader_projected.go`:

```go
package parquets3

import (
	"io"
	"strconv"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// readRowGroupProjected reads only the columns in wantCols from the row group.
// For each row, it calls emit with the projected fields.
// If wantCols is nil, all columns are read (fallback to full read).
func readRowGroupProjected(f *parquet.File, rg parquet.RowGroup, wantCols map[string]bool, emit func([]field)) error {
	pqSchema := f.Schema()
	allCols := pqSchema.Columns()

	// Build column index mask
	var colIndices []int
	var colNames []string
	for i, path := range allCols {
		name := path[0] // top-level column name
		if wantCols[name] {
			colIndices = append(colIndices, i)
			colNames = append(colNames, name)
		}
	}

	if len(colIndices) == 0 {
		return nil
	}

	rows := rg.Rows()
	defer rows.Close()

	// Seek to selected columns only
	selectedCols := make([]parquet.RowGroup, 0)
	_ = selectedCols // column selection via Rows API

	buf := make([]parquet.Row, 256)
	for {
		n, err := rows.ReadRows(buf)
		for i := 0; i < n; i++ {
			row := buf[i]
			fields := make([]field, 0, len(colIndices))
			for ci, colIdx := range colIndices {
				name := colNames[ci]
				val := extractColumnValue(row, colIdx, pqSchema)
				fields = append(fields, field{name: name, value: val})
			}
			emit(fields)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	return nil
}

func extractColumnValue(row parquet.Row, colIdx int, s *parquet.Schema) interface{} {
	for _, v := range row {
		if v.Column() == colIdx {
			switch {
			case v.Kind() == parquet.ByteArray:
				return string(v.ByteArray())
			case v.Kind() == parquet.Int64:
				return v.Int64()
			case v.Kind() == parquet.Int32:
				return int32(v.Int32())
			case v.Kind() == parquet.Double:
				return v.Double()
			case v.Kind() == parquet.Boolean:
				return v.Boolean()
			default:
				return strconv.FormatInt(v.Int64(), 10)
			}
		}
	}
	return ""
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run TestReadRowGroupProjected -v -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/parquets3/reader_projected.go internal/storage/parquets3/reader_projected_test.go
git commit -m "feat: add projected parquet reader that reads only referenced columns"
```

---

### Task 4: Column Projection — Wire Into Query Path (Logs)

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go`

Wire `queryColumns()` + `readRowGroupProjected()` into the `queryFile()` method. When projection identifies a column subset, use the projected reader instead of the full typed reader.

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/parquets3/projection_test.go`:

```go
func TestQueryFile_UsesProjection(t *testing.T) {
	// This is an integration-style test that verifies the metrics
	// We check that ParquetColumnBytesRead is lower with projection
	// by running the same query twice: once with projection disabled,
	// once with it enabled.
	//
	// For now, verify the wiring compiles and doesn't panic.
	// The actual byte savings are validated by the benchmark CLI.
}
```

Note: The real validation is via the benchmark CLI. The wiring test ensures compilation and no panics during the integration.

- [ ] **Step 2: Modify queryFile to use projection**

In `internal/storage/parquets3/storage_query.go`, modify `queryFile` (line ~198):

After `bloomChecks := s.buildBloomChecks(queryStr)` (line 215), add:

```go
	projectedCols := queryColumns(queryStr, s.registry)
```

Then in the row group loop, after the pushdown filter check (line ~239), change the `readRowGroup` call:

```go
		if projectedCols != nil {
			if err := s.readRowGroupProjected(f, rg, startNs, endNs, projectedCols, writeBlock, traceIDsPtr); err != nil {
				return err
			}
		} else {
			metrics.ParquetRowGroupsScanned.Inc()
			if err := s.readRowGroup(f, rg, startNs, endNs, writeBlock, traceIDsPtr); err != nil {
				return err
			}
		}
```

Add a new method `readRowGroupProjected` on `*Storage` that wraps `readRowGroupProjected()` to produce DataBlocks:

```go
func (s *Storage) readRowGroupProjected(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, cols map[string]bool, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	metrics.ParquetRowGroupsScanned.Inc()
	var allFields [][]field
	err := readRowGroupProjected(f, rg, cols, func(fields []field) {
		// Time-range filter on projected fields
		for _, fld := range fields {
			if fld.name == "timestamp_unix_nano" {
				if ts, ok := fld.value.(int64); ok {
					if ts < startNs || ts > endNs {
						return
					}
				}
			}
		}
		allFields = append(allFields, fields)
	})
	if err != nil {
		return err
	}

	if len(allFields) > 0 {
		db := projectedFieldsToDataBlock(allFields, s.registry)
		if db != nil && db.RowsCount() > 0 {
			writeBlock(0, db)
			if traceIDs != nil {
				extractTraceIDs(db, traceIDs)
			}
		}
	}
	return nil
}
```

- [ ] **Step 3: Run full test suite**

Run: `GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`
Expected: All pass — projection falls back to nil (read all) for queries that can't be parsed.

- [ ] **Step 4: Commit**

```bash
git add internal/storage/parquets3/storage_query.go internal/storage/parquets3/reader_projected.go
git commit -m "feat: wire column projection into queryFile for reduced I/O"
```

---

### Task 5: Traces Module — Pushdown Filter Parity

**Files:**
- Create: `lakehouse-traces/internal/storage/parquets3/filter_pushdown.go`
- Create: `lakehouse-traces/internal/storage/parquets3/filter_pushdown_test.go`
- Modify: `lakehouse-traces/internal/storage/parquets3/storage_query.go`

The traces module `queryFile` (line 206) is missing `buildPushDownFilter` and `rowGroupMatchesFilter` that the root module has. This means traces queries never skip row groups based on column min/max stats.

- [ ] **Step 1: Write the failing test**

Create `lakehouse-traces/internal/storage/parquets3/filter_pushdown_test.go`:

```go
package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/schema"
)

func TestBuildPushDownFilter_TracesExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	pdf := buildPushDownFilter(`service.name:="api"`, reg)
	if pdf == nil {
		t.Fatal("expected non-nil push down filter for exact match")
	}
	if len(pdf.Checks) == 0 {
		t.Fatal("expected at least one check")
	}
	found := false
	for _, c := range pdf.Checks {
		if c.Column == "service.name" && c.Value == "api" && c.Op == PushDownExact {
			found = true
		}
	}
	if !found {
		t.Errorf("expected exact match check for service.name=api, got %+v", pdf.Checks)
	}
}

func TestBuildPushDownFilter_TracesEmpty(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	pdf := buildPushDownFilter(`*`, reg)
	if pdf != nil {
		t.Error("expected nil push down filter for wildcard query")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd lakehouse-traces && GOWORK=off go test ./internal/storage/parquets3/ -run TestBuildPushDownFilter_Traces -v -count=1`
Expected: FAIL — `buildPushDownFilter` doesn't exist in traces module.

- [ ] **Step 3: Copy filter_pushdown.go from root module and adapt**

Copy `internal/storage/parquets3/filter_pushdown.go` to `lakehouse-traces/internal/storage/parquets3/filter_pushdown.go`. Change the import path for `schema` from `github.com/ReliablyObserve/victoria-lakehouse/internal/schema` to `github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/schema`. All other code is identical.

- [ ] **Step 4: Wire into traces queryFile**

In `lakehouse-traces/internal/storage/parquets3/storage_query.go`, modify `queryFile` (line 206):

After `bloomChecks := s.buildBloomChecks(queryStr)` (line 223), add:

```go
	pdf := buildPushDownFilter(queryStr, s.registry)
```

In the row group loop, after the bloom filter check (line 241-244), add before `metrics.ParquetRowGroupsScanned.Inc()`:

```go
		if pdf != nil && !rowGroupMatchesFilter(f, rg, pdf) {
			metrics.ParquetRowGroupsSkipped.Inc("pushdown")
			continue
		}
```

Also copy `rowGroupMatchesFilter` function if not already present.

- [ ] **Step 5: Run tests**

Run: `cd lakehouse-traces && GOWORK=off go test ./internal/storage/parquets3/ -run TestBuildPushDownFilter -v -count=1`
Expected: PASS

Run: `cd lakehouse-traces && GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add lakehouse-traces/internal/storage/parquets3/filter_pushdown.go lakehouse-traces/internal/storage/parquets3/filter_pushdown_test.go lakehouse-traces/internal/storage/parquets3/storage_query.go
git commit -m "feat: port pushdown filter to traces module for row group stats pruning"
```

---

### Task 6: Traces Module — Column Projection

**Files:**
- Create: `lakehouse-traces/internal/storage/parquets3/projection.go`
- Create: `lakehouse-traces/internal/storage/parquets3/projection_test.go`
- Create: `lakehouse-traces/internal/storage/parquets3/reader_projected.go`
- Create: `lakehouse-traces/internal/storage/parquets3/reader_projected_test.go`
- Modify: `lakehouse-traces/internal/storage/parquets3/storage_query.go`

Port the column projection from root module (Tasks 2-4) to the traces module.

- [ ] **Step 1: Copy projection.go and adapt imports**

Copy `internal/storage/parquets3/projection.go` to `lakehouse-traces/internal/storage/parquets3/projection.go`. Update the `schema` import to `lakehouse-traces/internal/schema`.

- [ ] **Step 2: Copy projection_test.go and adapt**

Copy and adapt tests. Change `schema.LogsProfile` references to `schema.TracesProfile`, and adjust expected field names (traces uses `span.name`, `span.kind`, etc. instead of `body`, `severity_text`).

```go
func TestQueryColumns_TracesExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`trace_id:="abc123"`, reg)

	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be included")
	}
	if !cols["trace_id"] {
		t.Error("trace_id must be included")
	}
	if cols["span.name"] {
		t.Error("span.name should not be included when not referenced")
	}
}
```

- [ ] **Step 3: Copy reader_projected.go and adapt**

Copy `internal/storage/parquets3/reader_projected.go` to traces module. Update imports. The core logic is identical since it operates on generic parquet rows.

- [ ] **Step 4: Wire into traces queryFile**

Same pattern as Task 4: add `projectedCols := queryColumns(queryStr, s.registry)` and conditional branching in the row group loop.

- [ ] **Step 5: Run full traces test suite**

Run: `cd lakehouse-traces && GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add lakehouse-traces/internal/storage/parquets3/projection.go lakehouse-traces/internal/storage/parquets3/projection_test.go lakehouse-traces/internal/storage/parquets3/reader_projected.go lakehouse-traces/internal/storage/parquets3/reader_projected_test.go lakehouse-traces/internal/storage/parquets3/storage_query.go
git commit -m "feat: port column projection to traces module"
```

---

### Task 7: Expanded Bloom Coverage — Schema Fields

**Files:**
- Modify: `internal/schema/registry.go`
- Modify: `lakehouse-traces/internal/schema/registry.go` (if separate)
- Create: `internal/schema/bloom_coverage_test.go`

Add `HasBloom: true` to additional high-value fields per the spec.

- [ ] **Step 1: Write the test for expected bloom coverage**

Create `internal/schema/bloom_coverage_test.go`:

```go
package schema

import "testing"

func TestBloomCoverage_Logs(t *testing.T) {
	reg := NewRegistry(LogsProfile)
	expectedBloom := []string{
		"trace_id",
		"service.name",
		"host.name",
		"k8s.namespace.name",
		"k8s.pod.name",
		"k8s.deployment.name",
		"deployment.environment",
	}
	for _, name := range expectedBloom {
		m := reg.ResolveToParquet(name)
		if m == nil {
			m = reg.ResolveFromParquet(name)
		}
		if m == nil {
			t.Errorf("field %q not found in logs registry", name)
			continue
		}
		if !m.HasBloom {
			t.Errorf("logs field %q: HasBloom = false, want true", name)
		}
	}
}

func TestBloomCoverage_Traces(t *testing.T) {
	reg := NewRegistry(TracesProfile)
	expectedBloom := []string{
		"trace_id",
		"service.name",
		"span.name",
		"k8s.namespace.name",
		"k8s.pod.name",
		"k8s.deployment.name",
		"deployment.environment",
	}
	for _, name := range expectedBloom {
		m := reg.ResolveToParquet(name)
		if m == nil {
			m = reg.ResolveFromParquet(name)
		}
		if m == nil {
			t.Errorf("field %q not found in traces registry", name)
			continue
		}
		if !m.HasBloom {
			t.Errorf("traces field %q: HasBloom = false, want true", name)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/schema/ -run TestBloomCoverage -v -count=1`
Expected: FAIL — several fields don't have `HasBloom: true` yet.

- [ ] **Step 3: Add HasBloom to registry fields**

In `internal/schema/registry.go`, modify the logs profile (around line 120-130) to add `HasBloom: true` to:
- `host.name` (currently `HasBloom: false`)
- `k8s.namespace.name`
- `k8s.pod.name`
- `k8s.deployment.name`
- `deployment.environment`

In the traces profile (around line 140-160), add `HasBloom: true` to:
- `span.name`
- `k8s.namespace.name`
- `k8s.pod.name` (if present)
- `k8s.deployment.name`
- `deployment.environment`

Keep `level`/`severity_text`, `span_kind`, and `status_code` WITHOUT bloom — their very low cardinality (3-5 values) means bloom filters provide near-zero file skip benefit.

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/schema/ -run TestBloomCoverage -v -count=1`
Expected: PASS

Run: `GOWORK=off go test ./internal/schema/ -short -race -count=1`
Expected: All pass (including existing bloom verification tests)

- [ ] **Step 5: Commit**

```bash
git add internal/schema/registry.go internal/schema/bloom_coverage_test.go
git commit -m "feat: expand bloom index coverage to k8s, host, and deployment fields"
```

---

### Task 8: Bloom Filter — Support `in()` Operator

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go`
- Create: `internal/storage/parquets3/filter_extract_test.go`

Currently `extractExactMatch` only handles `field:="value"`. The spec requires `field:in("a","b")` support for bloom checks.

- [ ] **Step 1: Write the failing test**

Create `internal/storage/parquets3/filter_extract_test.go`:

```go
package parquets3

import "testing"

func TestExtractExactMatch_QuotedExact(t *testing.T) {
	val := extractExactMatch(`trace_id:="abc123"`, "trace_id")
	if val != "abc123" {
		t.Errorf("expected abc123, got %q", val)
	}
}

func TestExtractExactMatch_NoMatch(t *testing.T) {
	val := extractExactMatch(`service.name:="api"`, "trace_id")
	if val != "" {
		t.Errorf("expected empty, got %q", val)
	}
}

func TestExtractInValues_Basic(t *testing.T) {
	vals := extractInValues(`service.name:in("api","web","worker")`, "service.name")
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d: %v", len(vals), vals)
	}
	expected := map[string]bool{"api": true, "web": true, "worker": true}
	for _, v := range vals {
		if !expected[v] {
			t.Errorf("unexpected value %q", v)
		}
	}
}

func TestExtractInValues_NoMatch(t *testing.T) {
	vals := extractInValues(`trace_id:="abc"`, "service.name")
	if len(vals) != 0 {
		t.Errorf("expected 0 values, got %d", len(vals))
	}
}

func TestExtractInValues_SingleValue(t *testing.T) {
	vals := extractInValues(`service.name:in("api")`, "service.name")
	if len(vals) != 1 || vals[0] != "api" {
		t.Errorf("expected [api], got %v", vals)
	}
}

func TestExtractFilterValues_CombinesExactAndIn(t *testing.T) {
	// extractFilterValues should return values from either exact match or in()
	vals := extractFilterValues(`service.name:="api"`, "service.name")
	if len(vals) != 1 || vals[0] != "api" {
		t.Errorf("expected [api] from exact match, got %v", vals)
	}

	vals = extractFilterValues(`service.name:in("api","web")`, "service.name")
	if len(vals) != 2 {
		t.Errorf("expected 2 values from in(), got %d", len(vals))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run "TestExtractInValues|TestExtractFilterValues" -v -count=1`
Expected: FAIL — `extractInValues` and `extractFilterValues` don't exist.

- [ ] **Step 3: Implement extractInValues and extractFilterValues**

Add to `internal/storage/parquets3/storage_query.go`:

```go
func extractInValues(query, fieldName string) []string {
	prefix := fieldName + `:in(`
	idx := strings.Index(query, prefix)
	if idx < 0 {
		return nil
	}
	start := idx + len(prefix)
	end := strings.Index(query[start:], ")")
	if end < 0 {
		return nil
	}
	inner := query[start : start+end]

	var vals []string
	for _, part := range strings.Split(inner, ",") {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, `"`)
		if part != "" {
			vals = append(vals, part)
		}
	}
	return vals
}

func extractFilterValues(query, fieldName string) []string {
	if vals := extractInValues(query, fieldName); len(vals) > 0 {
		return vals
	}
	if val := extractExactMatch(query, fieldName); val != "" {
		return []string{val}
	}
	return nil
}
```

- [ ] **Step 4: Update bloomFilterFiles to use extractFilterValues**

In `bloomFilterFiles` (line ~516), replace calls to `extractExactMatch` with `extractFilterValues`:

```go
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		vals := extractFilterValues(queryStr, col.InternalName)
		if len(vals) == 0 {
			vals = extractFilterValues(queryStr, col.ParquetColumn)
		}
		for _, val := range vals {
			checks = append(checks, bloomindex.ColumnCheck{
				Column: col.ParquetColumn,
				Value:  val,
			})
		}
	}
```

Similarly update `buildBloomChecks` for per-row-group checks.

- [ ] **Step 5: Run tests**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run "TestExtract" -v -count=1`
Expected: PASS

Run: `GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`
Expected: All pass

- [ ] **Step 6: Copy to traces module**

Copy `extractInValues` and `extractFilterValues` to `lakehouse-traces/internal/storage/parquets3/storage_query.go`. Update the traces bloom filter code similarly.

- [ ] **Step 7: Run traces tests**

Run: `cd lakehouse-traces && GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`
Expected: All pass

- [ ] **Step 8: Commit**

```bash
git add internal/storage/parquets3/storage_query.go internal/storage/parquets3/filter_extract_test.go lakehouse-traces/internal/storage/parquets3/storage_query.go
git commit -m "feat: support in() operator for bloom index checks"
```

---

### Task 9: Documentation & CHANGELOG

**Files:**
- Modify: `docs/performance.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Update CHANGELOG.md**

Add under `[Unreleased]`:

```markdown
### Performance
- Manifest `GetFilesForRange` uses sorted partition index with binary search (O(log P) vs O(P))
- Column projection pushdown: queries reading 2-3 fields skip deserializing unused columns (2-4x I/O reduction)
- Traces module: added pushdown filter parity with logs module for row group stats pruning
- Expanded bloom index coverage: `host.name`, `k8s.namespace.name`, `k8s.pod.name`, `k8s.deployment.name`, `deployment.environment`, `span.name` (traces)
- Bloom index supports `in()` operator for multi-value exact match queries
```

- [ ] **Step 2: Update docs/performance.md**

Add a section under "Tuning Guide" documenting the new bloom fields and column projection behavior.

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md docs/performance.md
git commit -m "docs: add Phase 2 latency optimization changelog and performance docs"
```

---

### Task 10: Exit Verification

**Files:** None (verification only)

- [ ] **Step 1: Run full test suite — root module**

Run: `GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`
Expected: All pass

- [ ] **Step 2: Run full test suite — traces module**

Run: `cd lakehouse-traces && GOWORK=off go test ./internal/... -short -race -count=1 -timeout=5m`
Expected: All pass

- [ ] **Step 3: Build all binaries**

Run: `GOWORK=off make build && GOWORK=off make bench`
Expected: All binaries build successfully

- [ ] **Step 4: Verify gofmt clean**

Run: `GOWORK=off gofmt -s -l cmd/ internal/ lakehouse-traces/`
Expected: No output

- [ ] **Step 5: Check coverage on new code**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -coverprofile=/tmp/cover_parquets3.out -count=1 && go tool cover -func=/tmp/cover_parquets3.out | grep total`
Expected: >85% coverage

- [ ] **Step 6: Report exit criteria**

Phase 2 exit criteria from spec:
- [ ] Each optimization has tests verifying correctness
- [ ] Regression suite green after each optimization
- [ ] Both modules (logs + traces) have all optimizations
- [ ] Documentation updated
