package bloomindex

import (
	"fmt"
	"runtime"
	"testing"
)

func bloomForceGC() {
	runtime.GC()
	runtime.GC()
}

func bloomHeapInUse() uint64 {
	var m runtime.MemStats
	bloomForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestMemLeak_Index_AddColumns(t *testing.T) {
	idx := New()

	// Warm up
	for i := 0; i < 100; i++ {
		f := NewFilter(10, 0.01)
		f.Add(fmt.Sprintf("val-%d", i))
		idx.AddColumns(fmt.Sprintf("file-%d", i), map[string]*Filter{"col": f})
	}
	bloomForceGC()

	before := bloomHeapInUse()

	// Add many entries then remove by replacing the index (bounded set)
	const iterations = 5000
	for i := 0; i < iterations; i++ {
		key := fmt.Sprintf("file-%d", i%100) // reuse 100 keys — no growth
		f := NewFilter(10, 0.01)
		f.Add(fmt.Sprintf("value-%d", i))
		idx.AddColumns(key, map[string]*Filter{"trace_id": f})
	}

	bloomForceGC()
	after := bloomHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d AddColumns cycles with bounded keys (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Index_MayContain(t *testing.T) {
	idx := New()
	keys := make([]string, 50)
	for i := 0; i < 50; i++ {
		keys[i] = fmt.Sprintf("file-%d", i)
		f := NewFilter(100, 0.01)
		for j := 0; j < 10; j++ {
			f.Add(fmt.Sprintf("trace-%d-%d", i, j))
		}
		idx.AddColumns(keys[i], map[string]*Filter{"trace_id": f})
	}
	bloomForceGC()

	before := bloomHeapInUse()

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		_ = idx.MayContain(keys, "trace_id", fmt.Sprintf("trace-%d-5", i%50))
	}

	bloomForceGC()
	after := bloomHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d MayContain cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Filter_AddAndMarshalUnmarshal(t *testing.T) {
	// Warm up
	for i := 0; i < 50; i++ {
		f := NewFilter(100, 0.01)
		f.Add("warmup")
		data := f.Marshal()
		_, _ = UnmarshalFilter(data)
	}
	bloomForceGC()

	before := bloomHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		f := NewFilter(100, 0.01)
		f.Add(fmt.Sprintf("value-%d", i))
		data := f.Marshal()
		f2, err := UnmarshalFilter(data)
		if err != nil {
			t.Fatalf("UnmarshalFilter failed: %v", err)
		}
		_ = f2.MayContain(fmt.Sprintf("value-%d", i))
	}

	bloomForceGC()
	after := bloomHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d marshal/unmarshal cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_PartitionedIndex_AddFile(t *testing.T) {
	pi := NewPartitionedIndex(GranularityHour, 0.01)

	// Warm up
	for i := 0; i < 100; i++ {
		pi.AddFile("dt=2024-01-01/hour=00", fmt.Sprintf("file-%d", i), map[string][]string{
			"trace_id": {"trace-abc", "trace-def"},
		})
	}
	bloomForceGC()

	before := bloomHeapInUse()

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		partition := fmt.Sprintf("dt=2024-01-01/hour=%02d", i%24)
		key := fmt.Sprintf("file-%d", i%50) // bounded key space per partition
		pi.AddFile(partition, key, map[string][]string{
			"trace_id":     {fmt.Sprintf("trace-%d", i)},
			"service.name": {"my-service"},
		})
	}

	bloomForceGC()
	after := bloomHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(15 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d PartitionedIndex.AddFile cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Index_MarshalUnmarshal(t *testing.T) {
	idx := New()
	for i := 0; i < 50; i++ {
		f := NewFilter(20, 0.01)
		for j := 0; j < 5; j++ {
			f.Add(fmt.Sprintf("v-%d-%d", i, j))
		}
		idx.AddColumns(fmt.Sprintf("file-%d", i), map[string]*Filter{"col1": f})
	}

	// Warm up
	for i := 0; i < 20; i++ {
		data := idx.Marshal()
		_, _ = Unmarshal(data)
	}
	bloomForceGC()

	before := bloomHeapInUse()

	const iterations = 2000
	for i := 0; i < iterations; i++ {
		data := idx.Marshal()
		idx2, err := Unmarshal(data)
		if err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		_ = idx2.Len()
	}

	bloomForceGC()
	after := bloomHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d index marshal/unmarshal cycles (max %d)", growth, iterations, maxAllowed)
	}
}
