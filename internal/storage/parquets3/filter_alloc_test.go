package parquets3

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func buildLargeDataBlock(rows int) *logstorage.DataBlock {
	bodies := make([]string, rows)
	services := make([]string, rows)
	levels := make([]string, rows)
	for i := 0; i < rows; i++ {
		bodies[i] = fmt.Sprintf("event %d completed", i)
		if i%5 == 0 {
			services[i] = "checkout"
		} else {
			services[i] = "payments"
		}
		if i%3 == 0 {
			levels[i] = "error"
		} else {
			levels[i] = "info"
		}
	}
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_msg", Values: bodies},
		{Name: "service.name", Values: services},
		{Name: "level", Values: levels},
	})
	return db
}

// BenchmarkFilterDataBlock measures per-row allocation cost. After
// hoisting the []logstorage.Field allocation outside the inner loop the
// per-op alloc count should be drastically lower than the row count.
func BenchmarkFilterDataBlock(b *testing.B) {
	db := buildLargeDataBlock(1024)
	filter, err := logstorage.ParseFilter(`service.name:="checkout"`)
	if err != nil {
		b.Fatalf("ParseFilter: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = filterDataBlock(db, filter)
	}
}

// TestFilterDataBlock_HoistedAllocs uses runtime.ReadMemStats-style
// allocation counting via b.AllocsPerRun to confirm that allocations
// per row are bounded by a small constant rather than scaling with row
// count. With per-row allocation, a 1024-row block would produce 1024+
// allocs; after hoisting we expect a constant baseline (≤ a few dozen)
// regardless of row count.
func TestFilterDataBlock_HoistedAllocs(t *testing.T) {
	const rows = 1024
	db := buildLargeDataBlock(rows)
	filter, err := logstorage.ParseFilter(`service.name:="checkout"`)
	if err != nil {
		t.Fatalf("ParseFilter: %v", err)
	}

	// AllocsPerRun runs the func b.N times after warmup.
	allocs := testing.AllocsPerRun(50, func() {
		_ = filterDataBlock(db, filter)
	})

	// Hard ceiling: per-row allocation would yield at least `rows`
	// allocs. The hoisted implementation must stay well below.
	const ceiling = 64.0
	if allocs > ceiling {
		t.Fatalf("filterDataBlock allocates %.0f times per call (rows=%d); expected <= %.0f after hoisting per-row Field slice",
			allocs, rows, ceiling)
	}
}
