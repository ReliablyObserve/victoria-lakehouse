package peercache

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

func TestRing_MemLeak_SetCycles(t *testing.T) {
	r := NewRing("self:9428", 150)

	peers := make([]string, 10)
	for i := range peers {
		peers[i] = fmt.Sprintf("peer%d:9428", i)
	}
	for i := 0; i < 100; i++ {
		r.Set(peers)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 10_000; i++ {
		r.Set(peers)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 10K Set cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestRing_MemLeak_LookupCycles(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "a:9428", "b:9428", "c:9428"})

	for i := 0; i < 1000; i++ {
		r.Lookup(fmt.Sprintf("k%d", i))
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		r.Lookup(fmt.Sprintf("k%d", i))
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 100K Lookup cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestHandler_MemLeak_PutGetCycles(t *testing.T) {
	h := NewHandler("test-key")

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("k%d", i)
		h.Put(key, make([]byte, 128))
		h.Get(key)
	}
	forceGC()

	before := heapInUse()

	for i := 1000; i < 50_000; i++ {
		key := fmt.Sprintf("k%d", i%100)
		h.Put(key, make([]byte, 128))
		h.Get(key)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 49K Put+Get cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestRing_MemLeak_MembersCycles(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "a:9428", "b:9428", "c:9428", "d:9428"})

	for i := 0; i < 1000; i++ {
		_ = r.Members()
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		_ = r.Members()
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 100K Members cycles (max allowed %d)", growth, maxGrowth)
	}
}
