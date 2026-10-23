//go:build !race

package parquets3

import "testing"

// The heap of a drain does not grow with the rows that share a second: the same
// rows in one second peak close to the same rows spread over seconds. It is not
// built under -race: the detector multiplies its time by 100 (88 s here), and the
// CI jobs that run this package with -race have a time budget; the benchmark
// BenchmarkDrainHeap and the cap on the objects (drain_crowded_test.go) cover it
// there. Plain `go test` (and CI's coverage run) builds it.
func TestDrainHeap_DoesNotGrowWithRowsPerSecond(t *testing.T) {
	const rows, maxRows = 30000, 5000
	spread, _, _ := drainHeap(t, rows, 1000, maxRows, 25)
	crowded, objects, _ := drainHeap(t, rows, rows, maxRows, 25)
	t.Logf("heap growth: spread %d MiB, one second %d MiB, %d objects (measured)", spread>>20, crowded>>20, objects)
	if objects < rows/maxRows {
		t.Errorf("%d objects for %d rows at %d per group", objects, rows, maxRows)
	}
	// A smoke check, not the guarantee (the cap on the objects, asserted above and
	// in drain_crowded_test.go, is): measured at this size, 0.5 to 1.5 over 18 runs
	// with and without -race, and 2.2 to 2.7 on main.
	if float64(crowded) > 3*float64(spread) {
		t.Errorf("the rows of one second peaked at %d MiB against %d MiB spread over seconds: more than three times", crowded>>20, spread>>20)
	}
}
