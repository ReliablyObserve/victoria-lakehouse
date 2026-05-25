package cache

import (
	"fmt"
	"runtime"
	"testing"
)

func TestMemLeak_LRU_PutEvict(t *testing.T) {
	c := NewLRU(1024)

	// Warm up
	for i := 0; i < 1000; i++ {
		c.Put(fmt.Sprintf("k%d", i), make([]byte, 100))
	}
	runtime.GC()
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		c.Put(fmt.Sprintf("k%d", i), make([]byte, 100))
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	if c.Size() > c.MaxSize() {
		t.Errorf("size %d exceeds max %d", c.Size(), c.MaxSize())
	}

	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap alloc grew %d bytes over %d put+evict cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_LRU_PutDelete(t *testing.T) {
	c := NewLRU(1024 * 1024)

	// Warm up
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("k%d", i)
		c.Put(key, make([]byte, 256))
		c.Delete(key)
	}
	runtime.GC()
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("k%d", i)
		c.Put(key, make([]byte, 256))
		c.Delete(key)
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap alloc grew %d bytes over %d put+delete cycles (max %d)", growth, iterations, maxAllowed)
	}
	if c.Len() != 0 {
		t.Errorf("len = %d after all deletes, want 0", c.Len())
	}
}

func TestMemLeak_LRU_ClearCycles(t *testing.T) {
	c := NewLRU(1024 * 1024)

	// Warm up
	for cycle := 0; cycle < 50; cycle++ {
		for i := 0; i < 100; i++ {
			c.Put(fmt.Sprintf("k%d", i), make([]byte, 128))
		}
		c.Clear()
	}
	runtime.GC()
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	const cycles = 500
	for cycle := 0; cycle < cycles; cycle++ {
		for i := 0; i < 100; i++ {
			c.Put(fmt.Sprintf("k%d", i), make([]byte, 128))
		}
		c.Clear()
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap alloc grew %d bytes over %d fill+clear cycles (max %d)", growth, cycles, maxAllowed)
	}
}

func TestMemLeak_Group_DoCycles(t *testing.T) {
	g := NewGroup()

	// Warm up
	for i := 0; i < 500; i++ {
		_, _, _ = g.Do(fmt.Sprintf("k%d", i), func() ([]byte, error) {
			return make([]byte, 32), nil
		})
	}
	runtime.GC()
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		_, _, _ = g.Do(fmt.Sprintf("k%d", i), func() ([]byte, error) {
			return make([]byte, 32), nil
		})
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap alloc grew %d bytes over %d Group.Do cycles (max %d)", growth, iterations, maxAllowed)
	}
	if g.Inflight() != 0 {
		t.Errorf("inflight = %d, want 0", g.Inflight())
	}
}

func TestMemLeak_LabelIndex_AddCycles(t *testing.T) {
	idx := NewLabelIndex()

	// Pre-seed fixed label names
	for i := 0; i < 20; i++ {
		idx.Add(fmt.Sprintf("field-%d", i), []string{"val"})
	}
	runtime.GC()
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	const iterations = 10000
	for cycle := 0; cycle < iterations; cycle++ {
		for i := 0; i < 20; i++ {
			// Add same field names — updates existing entries, no new keys
			idx.Add(fmt.Sprintf("field-%d", i), []string{fmt.Sprintf("v-%d", cycle%100)})
		}
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	maxAllowed := int64(20 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap alloc grew %d bytes over %d LabelIndex.Add cycles (max %d)", growth, iterations, maxAllowed)
	}
}
