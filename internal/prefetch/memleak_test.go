package prefetch

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

func prefetchGC() {
	runtime.GC()
	runtime.GC()
}

func prefetchHeap() uint64 {
	var m runtime.MemStats
	prefetchGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func prefetchWaitDrain(e *Engine, expectedMin int64) {
	for i := 0; i < 1000; i++ {
		triggered, completed, errors, _ := e.Stats()
		if completed+errors >= triggered && triggered >= expectedMin {
			return
		}
		runtime.Gosched()
	}
}

func TestMemLeak_Engine_EnqueueCycles(t *testing.T) {
	e := NewEngine(4, 100, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	// Warm up
	for i := 0; i < 500; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i), Type: TypeCorrelated})
	}
	prefetchWaitDrain(e, 500)
	prefetchGC()

	before := prefetchHeap()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i), Type: TypeCorrelated})
	}
	prefetchWaitDrain(e, int64(iterations))
	prefetchGC()

	after := prefetchHeap()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d Enqueue cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Engine_MarkUsefulCycles(t *testing.T) {
	e := NewEngine(2, 50, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	// Warm up: enqueue and mark useful
	for i := 0; i < 200; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i)})
	}
	prefetchWaitDrain(e, 200)
	for i := 0; i < 200; i++ {
		e.MarkUseful(fmt.Sprintf("k%d", i))
	}
	prefetchGC()

	before := prefetchHeap()

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("key-%d", i%500)
		e.Enqueue(Task{Key: key, Type: TypeCrossSignal})
		e.MarkUseful(key)
	}
	prefetchWaitDrain(e, int64(iterations))
	prefetchGC()

	after := prefetchHeap()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d Enqueue+MarkUseful cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Engine_StatsCycles(t *testing.T) {
	e := NewEngine(2, 10, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	// Warm up
	for i := 0; i < 100; i++ {
		e.Stats()
	}
	prefetchGC()

	before := prefetchHeap()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		triggered, completed, errors, useful := e.Stats()
		_, _, _, _ = triggered, completed, errors, useful
	}

	prefetchGC()
	after := prefetchHeap()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d Stats cycles (max %d)", growth, iterations, maxAllowed)
	}
}
