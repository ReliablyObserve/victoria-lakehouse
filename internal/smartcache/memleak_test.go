package smartcache

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func scForceGC() {
	runtime.GC()
	runtime.GC()
}

func scHeapInUse() uint64 {
	var m runtime.MemStats
	scForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestMemLeak_MetadataMap_SetDeleteCycles(t *testing.T) {
	m := NewMetadataMap()

	// Warm up
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%d", i)
		m.Set(key, EntryMeta{
			CreatedAt:  time.Now(),
			LastAccess: time.Now(),
			Signal:     "logs",
			Size:       1024,
		})
		m.Delete(key)
	}
	scForceGC()

	before := scHeapInUse()

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("key-%d", i%100) // bounded key space
		m.Set(key, EntryMeta{
			CreatedAt:  time.Now(),
			LastAccess: time.Now(),
			Signal:     "logs",
			Size:       int64(i % 1024),
		})
		if i%3 == 0 {
			m.Delete(key)
		}
	}

	scForceGC()
	after := scHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d MetadataMap Set/Delete cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_MetadataMap_RecordAccessCycles(t *testing.T) {
	m := NewMetadataMap()

	// Pre-populate with bounded set
	const keyCount = 100
	for i := 0; i < keyCount; i++ {
		m.Set(fmt.Sprintf("key-%d", i), EntryMeta{
			CreatedAt:  time.Now(),
			LastAccess: time.Now(),
			Signal:     "traces",
			Size:       2048,
		})
	}
	scForceGC()

	before := scHeapInUse()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("key-%d", i%keyCount)
		m.RecordAccess(key)
	}

	scForceGC()
	after := scHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d MetadataMap.RecordAccess cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_MetadataMap_AllCycles(t *testing.T) {
	m := NewMetadataMap()

	// Pre-populate
	for i := 0; i < 50; i++ {
		m.Set(fmt.Sprintf("file-%d", i), EntryMeta{
			CreatedAt:  time.Now(),
			LastAccess: time.Now(),
			Signal:     "logs",
			Size:       4096,
			TraceIDs:   []string{fmt.Sprintf("trace-%d", i)},
		})
	}

	// Warm up
	for i := 0; i < 20; i++ {
		_ = m.All()
	}
	scForceGC()

	before := scHeapInUse()

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		all := m.All()
		_ = len(all) // prevent optimizer from eliminating the call
	}

	scForceGC()
	after := scHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d MetadataMap.All cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_MetadataMap_PinUnpinCycles(t *testing.T) {
	m := NewMetadataMap()

	const keyCount = 20
	for i := 0; i < keyCount; i++ {
		m.Set(fmt.Sprintf("file-%d", i), EntryMeta{
			CreatedAt:  time.Now(),
			LastAccess: time.Now(),
			Signal:     "logs",
			Size:       1024,
		})
	}
	scForceGC()

	before := scHeapInUse()

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("file-%d", i%keyCount)
		queryID := fmt.Sprintf("q-%d", i%10)
		m.Pin(key, queryID, 100*time.Millisecond)
		m.Unpin(key, queryID)
	}

	scForceGC()
	after := scHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d Pin/Unpin cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_MetadataMap_ReconcileCycles(t *testing.T) {
	m := NewMetadataMap()

	// Warm up
	for i := 0; i < 20; i++ {
		files := make(map[string]int64, 50)
		for j := 0; j < 50; j++ {
			files[fmt.Sprintf("file-%d", j)] = int64(j * 1024)
		}
		m.Reconcile(files)
	}
	scForceGC()

	before := scHeapInUse()

	const iterations = 2000
	for i := 0; i < iterations; i++ {
		// Reconcile with a fixed set of files — no net growth
		files := make(map[string]int64, 50)
		for j := 0; j < 50; j++ {
			files[fmt.Sprintf("file-%d", j)] = int64(j * 1024)
		}
		m.Reconcile(files)
	}

	scForceGC()
	after := scHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d Reconcile cycles (max %d)", growth, iterations, maxAllowed)
	}
}
