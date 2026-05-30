package parquets3

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestRowGroupMatchesTimeRange_OutOfOrderPages mirrors the traces-module
// regression test: it locks the per-page min/max scan in
// rowGroupMatchesTimeRange. Even for logs the columnar parquet writer can
// reorder rows across pages for compression, so the bounds of a row group
// are not necessarily MinValue(page 0) and MaxValue(page N-1). The fix
// walks every page and aggregates.
//
// Mirror of lakehouse-traces/internal/storage/parquets3/row_group_time_range_test.go.
// To verify this test is a real regression lock: temporarily revert the
// per-page scan in rowGroupMatchesTimeRange (replace the for-loop with
// the old two-line MinValue(0)/MaxValue(numPages-1) form) — this test MUST
// fail on the "true max lives in middle page" subtests.
func TestRowGroupMatchesTimeRange_OutOfOrderPages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out_of_order_pages.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// Use a very small PageBufferSize without compression so the column
	// writer is forced to roll a new page after every few values. With many
	// rows it produces multiple pages per column; arranging the timestamps
	// so the true MAX lives in the first page (not the last) reproduces
	// the bug.
	w := parquet.NewGenericWriter[pushdownTestRow](f,
		parquet.PageBufferSize(64),
	)

	// Block A (first set of pages): timestamps in [4500, 5000]; the row-group MAX (5000) lives here.
	rows := make([]pushdownTestRow, 0, 256)
	for i := 0; i < 128; i++ {
		ts := int64(4500)
		if i == 5 {
			ts = 5000 // TRUE MAX, lives in first batch of pages
		} else if i%3 == 0 {
			ts = 4500 + int64(i%500)
		}
		rows = append(rows, pushdownTestRow{
			TimestampUnixNano: ts, Body: "a", SeverityText: "info", ServiceName: "svc",
		})
	}
	// Block B (later pages): timestamps in [1000, 2000]; the row-group MIN (1000) lives here.
	for i := 0; i < 128; i++ {
		ts := int64(1500)
		if i == 5 {
			ts = 1000 // TRUE MIN, lives in later pages
		} else if i%3 == 0 {
			ts = 1000 + int64(i%500)
		}
		rows = append(rows, pushdownTestRow{
			TimestampUnixNano: ts, Body: "b", SeverityText: "info", ServiceName: "svc",
		})
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rgs := pf.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups produced")
	}
	rg := rgs[0]
	tsIdx := findColumnIndex(pf.Root(), "timestamp_unix_nano")
	if tsIdx < 0 {
		t.Fatal("timestamp_unix_nano column not found")
	}

	idx, err := rg.ColumnChunks()[tsIdx].ColumnIndex()
	if err != nil {
		t.Fatalf("ColumnIndex: %v", err)
	}
	if idx == nil || idx.NumPages() < 2 {
		t.Skipf("parquet writer produced %d pages; need 2+ to exercise the bug",
			func() int { if idx == nil { return 0 }; return idx.NumPages() }())
	}

	tests := []struct {
		name    string
		startNs int64
		endNs   int64
		want    bool
	}{
		// True bounds are [1000, 5000]. The bug surfaced as "false" (skip)
		// for queries whose window was inside the true bounds but outside
		// the old [MinValue(0), MaxValue(N-1)] approximation.
		{"narrow window around true MAX (regression)", 4950, 5001, true},
		{"narrow window around true MIN (regression)", 999, 1001, true},
		{"narrow window in middle of true range", 2500, 3500, true}, // conservative — bounds overlap
		{"window encompassing true range", 500, 5500, true},
		{"window entirely below true MIN", 0, 999, false},
		{"window entirely above true MAX", 5001, 9999, false},
		{"window touching true MAX boundary", 5000, 6000, true},
		{"window touching true MIN boundary (exclusive end)", 0, 1001, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rowGroupMatchesTimeRange(rg, tsIdx, tt.startNs, tt.endNs)
			if got != tt.want {
				t.Errorf("rowGroupMatchesTimeRange(rg, %d, %d, %d) = %v, want %v (numPages=%d)",
					tsIdx, tt.startNs, tt.endNs, got, tt.want, idx.NumPages())
			}
		})
	}
}
