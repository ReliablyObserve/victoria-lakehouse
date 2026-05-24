package smartcache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func TestColumnPopularity_TopN(t *testing.T) {
	cp := NewColumnPopularity()

	// Record with different counts: host=5, path=3, method=1
	for i := 0; i < 5; i++ {
		cp.Record("host")
	}
	for i := 0; i < 3; i++ {
		cp.Record("path")
	}
	cp.Record("method")

	got := cp.TopN(3)
	if len(got) != 3 {
		t.Fatalf("expected 3 results, got %d", len(got))
	}
	if got[0] != "host" {
		t.Errorf("expected host at position 0, got %s", got[0])
	}
	if got[1] != "path" {
		t.Errorf("expected path at position 1, got %s", got[1])
	}
	if got[2] != "method" {
		t.Errorf("expected method at position 2, got %s", got[2])
	}
}

func TestColumnPopularity_TopN_LessThanN(t *testing.T) {
	cp := NewColumnPopularity()
	cp.Record("host")
	cp.Record("path")

	got := cp.TopN(10)
	if len(got) != 2 {
		t.Fatalf("expected 2 results when fewer columns than N, got %d", len(got))
	}
}

func TestColumnPopularity_TopN_Empty(t *testing.T) {
	cp := NewColumnPopularity()

	got := cp.TopN(5)
	if len(got) != 0 {
		t.Fatalf("expected 0 results for empty tracker, got %d", len(got))
	}
}

func TestColumnPopularity_TopN_TiesAreStable(t *testing.T) {
	cp := NewColumnPopularity()

	// All columns have the same count — tie-breaking should be deterministic.
	columns := []string{"zebra", "alpha", "middle", "beta"}
	for _, c := range columns {
		cp.Record(c)
	}

	// Run multiple times to verify stability.
	var prev []string
	for i := 0; i < 100; i++ {
		got := cp.TopN(4)
		if prev != nil {
			for j := range got {
				if got[j] != prev[j] {
					t.Fatalf("iteration %d: TopN not stable, position %d changed from %s to %s", i, j, prev[j], got[j])
				}
			}
		}
		prev = got
	}

	// With alphabetical tie-breaking, the order should be alpha, beta, middle, zebra.
	expected := []string{"alpha", "beta", "middle", "zebra"}
	for i, e := range expected {
		if prev[i] != e {
			t.Errorf("position %d: expected %s, got %s", i, e, prev[i])
		}
	}
}

func TestColumnPopularity_Record_Concurrent(t *testing.T) {
	cp := NewColumnPopularity()
	const goroutines = 10
	const recordsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			col := fmt.Sprintf("col_%d", id)
			for i := 0; i < recordsPerGoroutine; i++ {
				cp.Record(col)
			}
		}(g)
	}
	wg.Wait()

	// Each goroutine recorded 1000 times to its own column.
	top := cp.TopN(goroutines)
	if len(top) != goroutines {
		t.Fatalf("expected %d columns, got %d", goroutines, len(top))
	}

	// Verify total counts.
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	for g := 0; g < goroutines; g++ {
		col := fmt.Sprintf("col_%d", g)
		if cp.counts[col] != recordsPerGoroutine {
			t.Errorf("column %s: expected %d, got %d", col, recordsPerGoroutine, cp.counts[col])
		}
	}
}

func TestColumnPopularity_TopN_LargeN(t *testing.T) {
	cp := NewColumnPopularity()

	// Record 1000 columns with ascending counts.
	for i := 0; i < 1000; i++ {
		col := fmt.Sprintf("col_%04d", i)
		for j := 0; j <= i; j++ {
			cp.Record(col)
		}
	}

	got := cp.TopN(5)
	if len(got) != 5 {
		t.Fatalf("expected 5 results, got %d", len(got))
	}

	// Top 5 should be the columns with highest indices (most records).
	expected := []string{"col_0999", "col_0998", "col_0997", "col_0996", "col_0995"}
	for i, e := range expected {
		if got[i] != e {
			t.Errorf("position %d: expected %s, got %s", i, e, got[i])
		}
	}
}

func TestColumnPopularity_NoGoroutineLeak(t *testing.T) {
	// Force GC and get baseline goroutine count.
	runtime.GC()
	baseline := runtime.NumGoroutine()

	// Create, use, and discard a ColumnPopularity instance.
	cp := NewColumnPopularity()
	for i := 0; i < 10_000; i++ {
		cp.Record(fmt.Sprintf("col_%d", i%100))
	}
	cp.TopN(10)
	_ = cp

	runtime.GC()
	after := runtime.NumGoroutine()

	// Allow a margin of 2 goroutines for runtime background activity.
	if after > baseline+2 {
		t.Errorf("goroutine leak: before=%d, after=%d", baseline, after)
	}
}

func TestColumnPopularity_NoMemoryLeak(t *testing.T) {
	// Force GC and get baseline memory.
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Create and discard 100 instances, each with 1000 columns.
	for i := 0; i < 100; i++ {
		cp := NewColumnPopularity()
		for j := 0; j < 1000; j++ {
			cp.Record(fmt.Sprintf("col_%d", j))
		}
		_ = cp.TopN(5)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// All instances should be garbage collected. Allow up to 10MB growth.
	growthBytes := int64(after.HeapInuse) - int64(before.HeapInuse)
	const maxGrowth = 10 * 1024 * 1024 // 10 MB
	if growthBytes > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes (%.2f MB), limit %d bytes",
			growthBytes, float64(growthBytes)/(1024*1024), maxGrowth)
	}
}

func TestColumnPopularity_Record_SingleColumn(t *testing.T) {
	cp := NewColumnPopularity()

	const n = 1_000_000
	for i := 0; i < n; i++ {
		cp.Record("only_column")
	}

	cp.mu.RLock()
	defer cp.mu.RUnlock()
	if cp.counts["only_column"] != n {
		t.Errorf("expected count %d, got %d", n, cp.counts["only_column"])
	}
}
