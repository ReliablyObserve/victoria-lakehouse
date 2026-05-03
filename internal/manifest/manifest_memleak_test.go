package manifest

import (
	"fmt"
	"runtime"
	"testing"
	"time"
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

func TestManifest_MemLeak_AddFileCycles(t *testing.T) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	for i := 0; i < 100; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/file-%d.parquet", partition, i), Size: 1024})
	}
	forceGC()

	before := heapInUse()

	for i := 100; i < 50_000; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/file-%d.parquet", partition, i), Size: 1024})
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(50 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 50K AddFile cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestManifest_MemLeak_HasDataForRangeCycles(t *testing.T) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	for d := 0; d < 30; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/f.parquet", partition), Size: 1024})
		}
	}

	startNs := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 15, 11, 0, 0, 0, time.UTC).UnixNano()

	for i := 0; i < 1000; i++ {
		m.HasDataForRange(startNs, endNs)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		m.HasDataForRange(startNs, endNs)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(5 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 100K HasDataForRange cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestManifest_MemLeak_GetFilesForRangeCycles(t *testing.T) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	for d := 0; d < 30; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/f.parquet", partition), Size: 1024})
		}
	}

	startNs := time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC).UnixNano()

	for i := 0; i < 1000; i++ {
		_ = m.GetFilesForRange(startNs, endNs)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 50_000; i++ {
		_ = m.GetFilesForRange(startNs, endNs)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 50K GetFilesForRange cycles (max allowed %d)", growth, maxGrowth)
	}
}
