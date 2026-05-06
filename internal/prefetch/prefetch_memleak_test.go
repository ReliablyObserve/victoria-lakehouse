package prefetch

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

func forceGC() {
	runtime.GC()
	runtime.GC()
}

func heapInUse() uint64 {
	var m runtime.MemStats
	forceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestEngine_MemLeak_EnqueueCycles(t *testing.T) {
	e := NewEngine(4, 100, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	for i := 0; i < 1000; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i), Type: TypeCorrelated})
	}
	waitForDrain(e, 1000)
	forceGC()

	before := heapInUse()

	for i := 1000; i < 50_000; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i), Type: TypeCorrelated})
	}
	waitForDrain(e, 50000)
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 49K Enqueue cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestEngine_MemLeak_MarkUsefulCycles(t *testing.T) {
	e := NewEngine(4, 100, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	for i := 0; i < 1000; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i)})
	}
	waitForDrain(e, 1000)

	for i := 0; i < 1000; i++ {
		e.MarkUseful(fmt.Sprintf("k%d", i))
	}
	forceGC()

	before := heapInUse()

	for i := 1000; i < 50_000; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i)})
	}
	waitForDrain(e, 50000)

	for i := 1000; i < 50_000; i++ {
		e.MarkUseful(fmt.Sprintf("k%d", i))
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(20 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 49K MarkUseful cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestEngine_MemLeak_StatsCycles(t *testing.T) {
	e := NewEngine(2, 10, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	for i := 0; i < 100; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i)})
	}
	waitForDrain(e, 100)

	for i := 0; i < 1000; i++ {
		e.Stats()
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		e.Stats()
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 100K Stats cycles (max allowed %d)", growth, maxGrowth)
	}
}

func waitForDrain(e *Engine, expectedMin int64) {
	for i := 0; i < 500; i++ {
		triggered, completed, errors, _ := e.Stats()
		if completed+errors >= triggered && triggered >= expectedMin {
			return
		}
		runtime.Gosched()
	}
}
