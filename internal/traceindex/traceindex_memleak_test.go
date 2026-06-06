package traceindex

import (
	"fmt"
	"runtime"
	"testing"
)

func tiForceGC() {
	runtime.GC()
	runtime.GC()
}

func tiHeapInUse() uint64 {
	var m runtime.MemStats
	tiForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// TestMemLeak_PackUnpackChurn runs 50_000 Marshal/Unmarshal cycles on a
// 100-entry index and asserts heap growth stays under 10 MB. Catches the
// regression where the codec's underlying buffer pool would leak refs
// per cycle (would manifest as multi-hundred-MB growth here).
func TestMemLeak_PackUnpackChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memleak test under -short")
	}

	const (
		iterations = 50_000
		entryCount = 100
	)

	// Build a representative entries slice once; reuse across all iters
	// so the test measures codec churn, not input churn.
	entries := make([]Entry, entryCount)
	for i := 0; i < entryCount; i++ {
		entries[i] = Entry{
			TraceID:   fmt.Sprintf("trace-%032d", i),
			Partition: uint16(i % 1024),
			StartNs:   int64(i) * 1_000_000,
			EndNs:     int64(i)*1_000_000 + 5_000_000,
		}
	}

	// Warm-up: prime any internal caches / first-touch allocations.
	for i := 0; i < 100; i++ {
		buf := Marshal(entries)
		if _, err := Unmarshal(buf); err != nil {
			t.Fatalf("warmup unmarshal: %v", err)
		}
	}

	before := tiHeapInUse()

	for i := 0; i < iterations; i++ {
		buf := Marshal(entries)
		got, err := Unmarshal(buf)
		if err != nil {
			t.Fatalf("iter %d: unmarshal: %v", i, err)
		}
		if len(got) != entryCount {
			t.Fatalf("iter %d: got %d entries, want %d", i, len(got), entryCount)
		}
	}

	after := tiHeapInUse()
	growth := int64(after) - int64(before)
	const budget = int64(10 * 1024 * 1024)
	if growth > budget {
		t.Errorf("heap grew %d bytes over %d Marshal/Unmarshal cycles (budget %d)",
			growth, iterations, budget)
	}
}
