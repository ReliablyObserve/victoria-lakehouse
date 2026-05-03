package cache

import (
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

func TestLRU_MemLeak_PutEvict(t *testing.T) {
	c := NewLRU(1024)

	for i := 0; i < 1000; i++ {
		c.Put(fmt.Sprintf("k%d", i), make([]byte, 100))
	}
	forceGC()

	before := heapInUse()

	for i := 1000; i < 100_000; i++ {
		c.Put(fmt.Sprintf("k%d", i), make([]byte, 100))
	}
	forceGC()

	after := heapInUse()

	if c.Size() > c.MaxSize() {
		t.Errorf("size %d exceeds max %d", c.Size(), c.MaxSize())
	}

	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 99K put+evict cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestLRU_MemLeak_PutDelete(t *testing.T) {
	c := NewLRU(1024 * 1024)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("k%d", i)
		c.Put(key, make([]byte, 512))
		c.Delete(key)
	}
	forceGC()

	before := heapInUse()

	for i := 1000; i < 50_000; i++ {
		key := fmt.Sprintf("k%d", i)
		c.Put(key, make([]byte, 512))
		c.Delete(key)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 49K put+delete cycles (max allowed %d)", growth, maxGrowth)
	}

	if c.Len() != 0 {
		t.Errorf("len = %d after all deletes, want 0", c.Len())
	}
}

func TestLRU_MemLeak_ClearCycles(t *testing.T) {
	c := NewLRU(1024 * 1024)

	for cycle := 0; cycle < 100; cycle++ {
		for i := 0; i < 200; i++ {
			c.Put(fmt.Sprintf("k%d", i), make([]byte, 256))
		}
		c.Clear()
	}
	forceGC()

	before := heapInUse()

	for cycle := 0; cycle < 1000; cycle++ {
		for i := 0; i < 200; i++ {
			c.Put(fmt.Sprintf("k%d", i), make([]byte, 256))
		}
		c.Clear()
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 1000 fill+clear cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestGroup_MemLeak_DoCycles(t *testing.T) {
	g := NewGroup()

	for i := 0; i < 1000; i++ {
		_, _, _ = g.Do(fmt.Sprintf("k%d", i), func() ([]byte, error) {
			return make([]byte, 64), nil
		})
	}
	forceGC()

	before := heapInUse()

	for i := 1000; i < 100_000; i++ {
		_, _, _ = g.Do(fmt.Sprintf("k%d", i), func() ([]byte, error) {
			return make([]byte, 64), nil
		})
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 99K Do cycles (max allowed %d)", growth, maxGrowth)
	}

	if g.Inflight() != 0 {
		t.Errorf("inflight = %d, want 0", g.Inflight())
	}
}

func TestLabelIndex_MemLeak_AddCycles(t *testing.T) {
	idx := NewLabelIndex()

	for i := 0; i < 20; i++ {
		idx.Add(fmt.Sprintf("field-%d", i), []string{"val"})
	}
	forceGC()

	before := heapInUse()

	for cycle := 0; cycle < 10_000; cycle++ {
		for i := 0; i < 20; i++ {
			idx.Add(fmt.Sprintf("field-%d", i), []string{fmt.Sprintf("val-%d", cycle)})
		}
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(20 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 200K add cycles (max allowed %d)", growth, maxGrowth)
	}
}
