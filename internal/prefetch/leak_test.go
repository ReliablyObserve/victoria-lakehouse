package prefetch

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// --- Goroutine leak tests ---

func TestEngine_NoGoroutineLeak_StartStop(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 20; i++ {
		e := NewEngine(4, 100, func(_ context.Context, _ string) error {
			return nil
		})

		// Enqueue some tasks and let them complete.
		for j := 0; j < 50; j++ {
			e.Enqueue(Task{Key: fmt.Sprintf("key-%d-%d", i, j), Type: TypeCorrelated})
		}
		waitForDrainLeak(e, int64(50))

		e.Close()
	}

	time.Sleep(200 * time.Millisecond) // let goroutines settle
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after 20 Engine start/stop cycles: before=%d after=%d", before, after)
	}
}

func TestEngine_NoGoroutineLeak_CloseWithPendingTasks(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		e := NewEngine(1, 1000, func(ctx context.Context, _ string) error {
			// Slow task — will be interrupted by Close.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				return nil
			}
		})

		// Enqueue many tasks; Close should cancel them.
		for j := 0; j < 100; j++ {
			e.Enqueue(Task{Key: fmt.Sprintf("key-%d-%d", i, j), Type: TypeReadAhead})
		}

		// Close immediately without waiting for tasks.
		e.Close()
	}

	time.Sleep(200 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after closing engines with pending tasks: before=%d after=%d", before, after)
	}
}

func TestEngine_NoGoroutineLeak_EnqueueAfterClose(t *testing.T) {
	before := runtime.NumGoroutine()

	e := NewEngine(2, 50, func(_ context.Context, _ string) error {
		return nil
	})

	for j := 0; j < 20; j++ {
		e.Enqueue(Task{Key: fmt.Sprintf("key-%d", j), Type: TypeCorrelated})
	}
	waitForDrainLeak(e, 20)

	e.Close()

	// Enqueue after close should be a no-op; must not leak goroutines.
	for j := 0; j < 50; j++ {
		e.Enqueue(Task{Key: fmt.Sprintf("late-key-%d", j), Type: TypeWarmup})
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after enqueue-after-close: before=%d after=%d", before, after)
	}
}

// --- Memory leak test for high-throughput enqueue ---

func TestEngine_NoMemoryLeak_HighThroughput(t *testing.T) {
	e := NewEngine(4, 200, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	// Warm up.
	for i := 0; i < 500; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("warmup-%d", i), Type: TypeCorrelated})
	}
	waitForDrainLeak(e, 500)
	forceGC()

	before := heapInUse()

	for i := 500; i < 5000; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("key-%d", i), Type: TypeCorrelated})
		// Periodically let tasks drain to avoid queue overflow.
		if i%500 == 0 {
			waitForDrainLeak(e, int64(i+1))
		}
	}
	waitForDrainLeak(e, 5000)
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 4.5K enqueue cycles (max %d)", growth, maxGrowth)
	}
}

// waitForDrainLeak waits until at least expectedMin tasks have completed+errored.
func waitForDrainLeak(e *Engine, expectedMin int64) {
	for i := 0; i < 2000; i++ {
		triggered, completed, errors, _ := e.Stats()
		if completed+errors >= triggered && triggered >= expectedMin {
			return
		}
		runtime.Gosched()
		if i%100 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
}
