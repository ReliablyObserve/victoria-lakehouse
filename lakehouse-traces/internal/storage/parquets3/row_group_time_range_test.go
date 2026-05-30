package parquets3

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestRowGroupMatchesTimeRange_OutOfOrderPages locks the regression caught
// during the Jaeger 24h search investigation: when a parquet row group is
// flushed with pages whose min/max timestamps are NOT monotonic across the
// page index (the canonical case for traces, where a long-running parent
// span emits AFTER its children, or where the columnar writer reorders rows
// for compression), the previous implementation took MinValue(0) and
// MaxValue(numPages-1) as the row-group bounds and silently skipped row
// groups whose true MAX timestamp lived in a middle page. The fix walks
// every page's min and max.
//
// To exercise multi-page output reliably we use PageBufferSize(64) so each
// page holds ~3 ints; the rows are then written in non-monotonic order so
// page 0's MAX is well below page 1's MIN, and the last page's MAX is well
// below the global MAX (which lives in a middle page).
//
// To verify this test is a real regression lock: temporarily revert the
// per-page scan in rowGroupMatchesTimeRange (replace the for-loop with the
// old two-line MinValue(0)/MaxValue(numPages-1) form) — this test MUST fail
// on the "true max lives in middle page" subtests. The DIAG-RG-TIME log
// added during investigation captured exactly this shape in production.
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
	// the bug. We also write a lot of rows to ensure the buffer fills up
	// multiple times.
	w := parquet.NewGenericWriter[pushdownTestRow](f,
		parquet.PageBufferSize(64),
	)

	// Block A (first set of pages): timestamps in [4500, 5000]; the row-group MAX (5000) lives here.
	// Many rows so the page buffer fills repeatedly.
	rows := make([]pushdownTestRow, 0, 256)
	for i := 0; i < 128; i++ {
		ts := int64(4500)
		if i == 5 {
			ts = 5000 // TRUE MAX, lives in first batch of pages
		} else if i%3 == 0 {
			ts = 4500 + int64(i%500)
		}
		rows = append(rows, pushdownTestRow{
			TimestampUnixNano: ts, SpanName: "a", SeverityText: "info", ServiceName: "svc",
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
			TimestampUnixNano: ts, SpanName: "b", SeverityText: "info", ServiceName: "svc",
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

	// Skip the test gracefully if parquet-go decided to emit a single page
	// despite our PageBufferSize hint (e.g. heavy compression at very small
	// row counts). The aggregation-across-pages logic only matters when
	// there is more than one page; otherwise we'd be asserting on a single
	// page's min/max, which the old code already handled correctly.
	idx, err := rg.ColumnChunks()[tsIdx].ColumnIndex()
	if err != nil {
		t.Fatalf("ColumnIndex: %v", err)
	}
	if idx == nil || idx.NumPages() < 2 {
		t.Skipf("parquet writer produced %d pages; need 2+ to exercise the bug",
			func() int {
				if idx == nil {
					return 0
				}
				return idx.NumPages()
			}())
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
