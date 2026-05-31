package compaction

// forceGC and heapInUse are defined in leak_test.go.

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestMemLeak_LevelPolicy_Eligible verifies that repeated Eligible calls
// do not accumulate heap allocations.
func TestMemLeak_LevelPolicy_Eligible(t *testing.T) {
	policy := NewLevelPolicy(10, 20, 0)

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

	// Warm up
	for i := 0; i < 1000; i++ {
		_, _ = policy.Eligible(files, partitionTime)
	}
	runtime.GC()
	runtime.GC()

	var mBefore runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&mBefore)
	before := mBefore.HeapInuse

	const iterations = 500000
	for i := 0; i < iterations; i++ {
		_, _ = policy.Eligible(files, partitionTime)
	}
	runtime.GC()
	runtime.GC()

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)
	after := mAfter.HeapInuse

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("LevelPolicy.Eligible memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_LevelPolicy_SelectFiles verifies that SelectFiles does not
// accumulate heap allocations across repeated calls.
func TestMemLeak_LevelPolicy_SelectFiles(t *testing.T) {
	policy := NewLevelPolicy(10, 20, 0)

	var files []manifest.FileInfo
	for i := 0; i < 50; i++ {
		fp := "fp-x"
		if i%10 == 0 {
			fp = "fp-y"
		}
		files = append(files, manifest.FileInfo{
			Key:               fmt.Sprintf("file-%d.parquet", i),
			Size:              2048,
			CompactionLevel:   i % 3,
			SchemaFingerprint: fp,
		})
	}

	// Warm up
	for i := 0; i < 1000; i++ {
		_ = policy.SelectFiles(files, 0, "fp-x")
	}
	runtime.GC()
	runtime.GC()

	var mBefore runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&mBefore)
	before := mBefore.HeapInuse

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		selected := policy.SelectFiles(files, i%3, "fp-x")
		_ = len(selected)
	}
	runtime.GC()
	runtime.GC()

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)
	after := mAfter.HeapInuse

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("LevelPolicy.SelectFiles memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_MajoritySchemaFingerprint verifies that repeated fingerprint
// computation does not leak intermediate map allocations.
func TestMemLeak_MajoritySchemaFingerprint(t *testing.T) {
	var files []manifest.FileInfo
	for i := 0; i < 30; i++ {
		fp := "fp-a"
		if i%3 == 0 {
			fp = "fp-b"
		}
		files = append(files, manifest.FileInfo{
			Key:               fmt.Sprintf("file-%d.parquet", i),
			CompactionLevel:   i % 2,
			SchemaFingerprint: fp,
		})
	}

	// Warm up
	for i := 0; i < 1000; i++ {
		_ = MajoritySchemaFingerprint(files, 0)
	}
	runtime.GC()
	runtime.GC()

	var mBefore runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&mBefore)
	before := mBefore.HeapInuse

	const iterations = 200000
	for i := 0; i < iterations; i++ {
		fp := MajoritySchemaFingerprint(files, i%2)
		_ = fp
	}
	runtime.GC()
	runtime.GC()

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)
	after := mAfter.HeapInuse

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("MajoritySchemaFingerprint memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// (Sentinel acquire/release leak test removed alongside sentinel.go in PR A —
// HRW ownership replaces the S3-sentinel approach; see spec §2 + §7.)

// TestMemLeak_CompactionPlan_GenerationCycles verifies that generating
// compaction eligibility plans from manifest data is bounded.
func TestMemLeak_CompactionPlan_GenerationCycles(t *testing.T) {
	policy := NewLevelPolicy(5, 10, 0)

	// Build varied partition data
	filesByPartition := make(map[string][]manifest.FileInfo)
	for d := 1; d <= 10; d++ {
		partition := fmt.Sprintf("dt=2026-01-%02d/hour=00", d)
		for j := 0; j < 8; j++ {
			filesByPartition[partition] = append(filesByPartition[partition], manifest.FileInfo{
				Key:               fmt.Sprintf("logs/%s/file-%d.parquet", partition, j),
				Size:              4096,
				CompactionLevel:   j % 2,
				SchemaFingerprint: "fp-main",
			})
		}
	}

	partitionTime := time.Now().Add(-48 * time.Hour)

	// Warm up
	for i := 0; i < 500; i++ {
		for _, files := range filesByPartition {
			_, _ = policy.Eligible(files, partitionTime)
			_ = MajoritySchemaFingerprint(files, 0)
			_ = policy.SelectFiles(files, 0, "fp-main")
		}
	}
	runtime.GC()
	runtime.GC()

	var mBefore runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&mBefore)
	before := mBefore.HeapInuse

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		for _, files := range filesByPartition {
			level, ok := policy.Eligible(files, partitionTime)
			if ok {
				fp := MajoritySchemaFingerprint(files, level)
				selected := policy.SelectFiles(files, level, fp)
				_ = len(selected)
			}
		}
	}
	runtime.GC()
	runtime.GC()

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)
	after := mAfter.HeapInuse

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("compaction plan generation memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}
