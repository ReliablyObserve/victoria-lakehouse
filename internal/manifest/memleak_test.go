package manifest

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func manifestGC() {
	runtime.GC()
	runtime.GC()
}

func manifestHeap() uint64 {
	var m runtime.MemStats
	manifestGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestMemLeak_Manifest_AddFileCycles(t *testing.T) {
	m := New("bucket", "logs/")

	// Warm up — add a bounded set of files
	for i := 0; i < 100; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/file-%d.parquet", partition, i%10), Size: 1024})
	}
	manifestGC()

	before := manifestHeap()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		// Reuse bounded partition+key space — no net growth
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		key := fmt.Sprintf("%s/file-%d.parquet", partition, i%5)
		m.AddFile(partition, FileInfo{Key: key, Size: 1024})
	}

	manifestGC()
	after := manifestHeap()

	growth := int64(after) - int64(before)
	maxAllowed := int64(15 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d AddFile cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Manifest_GetFilesForRange(t *testing.T) {
	m := New("bucket", "logs/")

	for d := 0; d < 7; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/f.parquet", partition), Size: 2048})
		}
	}

	startNs := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC).UnixNano()

	// Warm up
	for i := 0; i < 100; i++ {
		_ = m.GetFilesForRange(startNs, endNs)
	}
	manifestGC()

	before := manifestHeap()

	const iterations = 20000
	for i := 0; i < iterations; i++ {
		files := m.GetFilesForRange(startNs, endNs)
		_ = len(files)
	}

	manifestGC()
	after := manifestHeap()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d GetFilesForRange cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Manifest_HasDataForRange(t *testing.T) {
	m := New("bucket", "traces/")

	for d := 0; d < 7; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/t.parquet", partition), Size: 512})
		}
	}

	startNs := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC).UnixNano()

	// Warm up
	for i := 0; i < 500; i++ {
		m.HasDataForRange(startNs, endNs)
	}
	manifestGC()

	before := manifestHeap()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		_ = m.HasDataForRange(startNs, endNs)
	}

	manifestGC()
	after := manifestHeap()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d HasDataForRange cycles (max %d)", growth, iterations, maxAllowed)
	}
}
