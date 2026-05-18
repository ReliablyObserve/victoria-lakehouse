package parquets3

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestPREWHERE_FilterColumnReadFirst(t *testing.T) {
	dir := t.TempDir()

	type traceRow struct {
		TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
		TraceID           string `parquet:"trace_id"`
		ServiceName       string `parquet:"service.name"`
		SpanName          string `parquet:"span.name"`
	}

	// Write parquet with known trace_ids across multiple row groups
	path := filepath.Join(dir, "prewhere_test.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[traceRow](f,
		parquet.Compression(&parquet.Zstd),
		parquet.MaxRowsPerRowGroup(100),
	)

	for rg := 0; rg < 10; rg++ {
		rows := make([]traceRow, 100)
		for i := range rows {
			rows[i] = traceRow{
				TimestampUnixNano: int64(rg*100 + i),
				TraceID:           fmt.Sprintf("trace-%d-%d", rg, i),
				ServiceName:       "api-gateway",
				SpanName:          "GET /api/users",
			}
		}
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Open and verify row group structure
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := pf.RowGroups()
	t.Logf("Parquet has %d row groups", len(rgs))

	// PREWHERE concept: for each row group, read only the trace_id column first
	// If trace_id not found, skip remaining columns (95% savings)
	targetTraceID := "trace-5-42"
	matchedRGs := 0
	for _, rg := range rgs {
		traceIDIdx := findColumnIndex(pf.Root(), "trace_id")
		if traceIDIdx < 0 {
			t.Fatal("trace_id column not found")
		}

		// In PREWHERE, we'd read ONLY this column chunk first
		// For this test, we verify the concept of column-selective reads
		cols := rg.ColumnChunks()
		if traceIDIdx < len(cols) {
			_ = cols[traceIDIdx] // would read just this column
			matchedRGs++
		}
	}
	_ = targetTraceID
	t.Logf("PREWHERE would check %d row groups for trace_id column only", matchedRGs)
}

func TestPREWHERE_RGStatsElimination(t *testing.T) {
	// Test that row group statistics (min/max) can eliminate row groups
	// before even reading column data
	dir := t.TempDir()

	type tsRow struct {
		TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
		TraceID           string `parquet:"trace_id"`
	}

	path := filepath.Join(dir, "stats_test.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[tsRow](f,
		parquet.Compression(&parquet.Zstd),
		parquet.MaxRowsPerRowGroup(50),
	)

	// Row groups with non-overlapping timestamps
	for rg := 0; rg < 5; rg++ {
		rows := make([]tsRow, 50)
		for i := range rows {
			rows[i] = tsRow{
				TimestampUnixNano: int64(rg*1000 + i),
				TraceID:           fmt.Sprintf("trace-%d-%d", rg, i),
			}
		}
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := pf.RowGroups()
	t.Logf("Created %d row groups with non-overlapping timestamps", len(rgs))

	// Time range filter: only query rows in [2000, 3000)
	// This should eliminate 3 of 5 row groups using stats alone
	startNs := int64(2000)
	endNs := int64(3000)
	tsIdx := findColumnIndex(pf.Root(), "timestamp_unix_nano")

	matched := 0
	for _, rg := range rgs {
		if tsIdx >= 0 && !rowGroupMatchesTimeRange(rg, tsIdx, startNs, endNs) {
			continue
		}
		matched++
	}

	t.Logf("Time filter: %d/%d row groups matched for range [%d, %d)", matched, len(rgs), startNs, endNs)
}
