package manifest

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// forceGC and heapInUse are defined in manifest_memleak_test.go

// --- Goroutine leak tests ---

// TestManifest_NoGoroutineLeak_AddFileRemoveFile tests that repeated
// AddFile/RemoveFile cycles don't leak goroutines.
func TestManifest_NoGoroutineLeak_AddFileRemoveFile(t *testing.T) {
	m := New("bucket", "logs/")

	before := runtime.NumGoroutine()

	for i := 0; i < 1000; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		key := fmt.Sprintf("%s/file-%d.parquet", partition, i)
		m.AddFile(partition, FileInfo{Key: key, Size: 1024})
		m.RemoveFile(partition, key)
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

// TestManifest_NoGoroutineLeak_QueryOperations tests that query-only
// operations (HasDataForRange, GetFilesForRange, AllFiles) don't leak goroutines.
func TestManifest_NoGoroutineLeak_QueryOperations(t *testing.T) {
	m := New("bucket", "logs/")

	// Populate with data.
	for d := 0; d < 10; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/f.parquet", partition), Size: 1024})
		}
	}

	startNs := time.Date(2026, 5, 5, 10, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC).UnixNano()

	before := runtime.NumGoroutine()

	for i := 0; i < 5000; i++ {
		m.HasDataForRange(startNs, endNs)
		_ = m.GetFilesForRange(startNs, endNs)
		_ = m.AllFiles()
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

// --- Memory leak test for AddFile/RemoveFile churn ---

func TestManifest_NoMemoryLeak_AddRemoveChurn(t *testing.T) {
	m := New("bucket", "logs/")

	// Warm up.
	for i := 0; i < 100; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		key := fmt.Sprintf("%s/file-%d.parquet", partition, i)
		m.AddFile(partition, FileInfo{Key: key, Size: 1024})
		m.RemoveFile(partition, key)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 50_000; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		key := fmt.Sprintf("%s/file-%d.parquet", partition, i)
		m.AddFile(partition, FileInfo{Key: key, Size: 1024})
		m.RemoveFile(partition, key)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 50K AddFile/RemoveFile cycles (max %d)", growth, maxGrowth)
	}
}

// TestManifest_NoMemoryLeak_FilesForPartition tests that repeated
// FilesForPartition queries don't accumulate memory.
func TestManifest_NoMemoryLeak_FilesForPartition(t *testing.T) {
	m := New("bucket", "logs/")

	// Populate.
	for i := 0; i < 50; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		for j := 0; j < 20; j++ {
			m.AddFile(partition, FileInfo{
				Key:  fmt.Sprintf("%s/file-%d.parquet", partition, j),
				Size: 1024,
			})
		}
	}

	// Warm up.
	for i := 0; i < 1000; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		_ = m.FilesForPartition(partition)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
		_ = m.FilesForPartition(partition)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 100K FilesForPartition queries (max %d)", growth, maxGrowth)
	}
}
