package compaction

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
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

// --- Scheduler goroutine leak tests ---

func TestScheduler_NoGoroutineLeak_StartStop(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)

	before := runtime.NumGoroutine()

	for i := 0; i < 20; i++ {
		sched := NewScheduler(SchedulerConfig{
			Manifest:         m,
			Pool:             pool,
			Ownership:        neverOwnsResolver(),
			Policy:           policy,
			Prefix:           "logs/",
			Mode:             config.ModeLogs,
			Interval:         50 * time.Millisecond,
			MaxConcurrent:    1,
			RowGroupSize:     1000,
			CompressionLevel: 7,
		})

		sched.Start()
		time.Sleep(10 * time.Millisecond) // let goroutine start
		sched.Stop()
	}

	time.Sleep(100 * time.Millisecond) // let goroutines settle
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after 20 Scheduler Start/Stop cycles: before=%d after=%d", before, after)
	}
}

func TestScheduler_NoGoroutineLeak_WithScans(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(100, 200, 0) // high thresholds so nothing compacts

	// Add some files to partitions so Scan has work to iterate.
	for i := 0; i < 10; i++ {
		partition := fmt.Sprintf("dt=2026-01-%02d/hour=%02d", (i%28)+1, i%24)
		for j := 0; j < 5; j++ {
			m.AddFile(partition, manifest.FileInfo{
				Key:  fmt.Sprintf("logs/%s/file-%d.parquet", partition, j),
				Size: 100,
			})
		}
	}

	before := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		sched := NewScheduler(SchedulerConfig{
			Manifest:         m,
			Pool:             pool,
			Ownership:        soleOwnerResolver(),
			Policy:           policy,
			Prefix:           "logs/",
			Mode:             config.ModeLogs,
			Interval:         20 * time.Millisecond,
			MaxConcurrent:    2,
			RowGroupSize:     1000,
			CompressionLevel: 7,
		})

		sched.Start()
		time.Sleep(50 * time.Millisecond) // let at least one tick fire
		sched.Stop()
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after 10 Scheduler Start/Stop with Scans: before=%d after=%d", before, after)
	}
}

// --- Policy memory leak tests ---

func TestLevelPolicy_NoMemoryLeak_EligibleCycles(t *testing.T) {
	policy := NewLevelPolicy(10, 20, 0)

	// Build a set of files.
	var files []manifest.FileInfo
	for i := 0; i < 15; i++ {
		files = append(files, manifest.FileInfo{
			Key:               fmt.Sprintf("file-%d.parquet", i),
			Size:              1024,
			CompactionLevel:   0,
			SchemaFingerprint: "fp-a",
		})
	}

	partitionTime := time.Now().Add(-24 * time.Hour)

	// Warm up.
	for i := 0; i < 1000; i++ {
		_, _ = policy.Eligible(files, partitionTime)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 500_000; i++ {
		_, _ = policy.Eligible(files, partitionTime)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 500K Eligible calls (max %d)", growth, maxGrowth)
	}
}

func TestLevelPolicy_NoMemoryLeak_SelectFilesCycles(t *testing.T) {
	policy := NewLevelPolicy(10, 20, 0)

	var files []manifest.FileInfo
	for i := 0; i < 50; i++ {
		files = append(files, manifest.FileInfo{
			Key:               fmt.Sprintf("file-%d.parquet", i),
			Size:              2048,
			CompactionLevel:   0,
			SchemaFingerprint: "fp-x",
		})
	}

	// Warm up.
	for i := 0; i < 1000; i++ {
		_ = policy.SelectFiles(files, 0, "fp-x")
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		_ = policy.SelectFiles(files, 0, "fp-x")
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 100K SelectFiles calls (max %d)", growth, maxGrowth)
	}
}

func TestMajoritySchemaFingerprint_NoMemoryLeak(t *testing.T) {
	var files []manifest.FileInfo
	for i := 0; i < 30; i++ {
		fp := "fp-a"
		if i%3 == 0 {
			fp = "fp-b"
		}
		files = append(files, manifest.FileInfo{
			Key:               fmt.Sprintf("file-%d.parquet", i),
			CompactionLevel:   0,
			SchemaFingerprint: fp,
		})
	}

	// Warm up.
	for i := 0; i < 1000; i++ {
		_ = MajoritySchemaFingerprint(files, 0)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 200_000; i++ {
		_ = MajoritySchemaFingerprint(files, 0)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after 200K MajoritySchemaFingerprint calls (max %d)", growth, maxGrowth)
	}
}

// Sentinel memory-leak tests were removed alongside sentinel.go in PR A —
// HRW ownership replaces the S3-sentinel approach (spec §2 + §7).
