package cache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// forceGC and heapInUse are defined in cache_memleak_test.go

// --- Goroutine leak tests ---

func TestGroup_NoGoroutineLeak_DoCycles(t *testing.T) {
	g := NewGroup()

	before := runtime.NumGoroutine()

	for i := 0; i < 5000; i++ {
		_, _, _ = g.Do(fmt.Sprintf("key-%d", i), func() ([]byte, error) {
			return make([]byte, 64), nil
		})
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestGroup_NoGoroutineLeak_ConcurrentDo(t *testing.T) {
	g := NewGroup()

	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for round := 0; round < 100; round++ {
		key := fmt.Sprintf("key-%d", round)
		for worker := 0; worker < 5; worker++ {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				_, _, _ = g.Do(k, func() ([]byte, error) {
					return make([]byte, 128), nil
				})
			}(key)
		}
		wg.Wait()
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after concurrent Do: before=%d after=%d", before, after)
	}

	if g.Inflight() != 0 {
		t.Errorf("inflight = %d after all operations, want 0", g.Inflight())
	}
}

// --- LRU goroutine leak test (ensure no background goroutines) ---

func TestLRU_NoGoroutineLeak_Lifecycle(t *testing.T) {
	before := runtime.NumGoroutine()

	for cycle := 0; cycle < 50; cycle++ {
		c := NewLRU(1024 * 1024)
		for i := 0; i < 100; i++ {
			c.Put(fmt.Sprintf("k%d", i), make([]byte, 512))
		}
		for i := 0; i < 50; i++ {
			c.Get(fmt.Sprintf("k%d", i))
		}
		for i := 0; i < 30; i++ {
			c.Delete(fmt.Sprintf("k%d", i))
		}
		c.Clear()
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

// --- Memory leak tests for Get (copy allocation) ---

func TestLRU_NoMemoryLeak_GetCycles(t *testing.T) {
	c := NewLRU(10 * 1024 * 1024)
	for i := 0; i < 100; i++ {
		c.Put(fmt.Sprintf("k%d", i), make([]byte, 4096))
	}

	// Warm up.
	for i := 0; i < 1000; i++ {
		c.Get(fmt.Sprintf("k%d", i%100))
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 200_000; i++ {
		v, ok := c.Get(fmt.Sprintf("k%d", i%100))
		if !ok {
			t.Fatalf("key k%d missing", i%100)
		}
		_ = v
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 200K Get cycles (max %d)", growth, maxGrowth)
	}
}

// --- LabelIndex goroutine/memory combined ---

func TestLabelIndex_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for cycle := 0; cycle < 100; cycle++ {
		idx := NewLabelIndex()
		for i := 0; i < 50; i++ {
			idx.Add(fmt.Sprintf("field-%d", i), []string{"val-a", "val-b"})
		}
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}
