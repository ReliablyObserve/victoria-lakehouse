package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestPolicy_LargeFileCount verifies Eligible() completes quickly for large file counts.
func TestPolicy_LargeFileCount(t *testing.T) {
	p := NewLevelPolicy(10, 15, time.Hour)
	partitionTime := time.Now().Add(-2 * time.Hour)

	// 1000 files at L0 — well above MinFilesL0=10
	files1000 := makeFiles(0, "fp1", 1000)
	level, eligible := p.Eligible(files1000, partitionTime)
	if !eligible {
		t.Fatal("expected eligible=true for 1000 L0 files")
	}
	if level != 0 {
		t.Fatalf("expected level=0, got %d", level)
	}

	// 5000 files at L1 — well above MinFilesL1=15
	files5000 := makeFiles(1, "fp1", 5000)
	level, eligible = p.Eligible(files5000, partitionTime)
	if !eligible {
		t.Fatal("expected eligible=true for 5000 L1 files")
	}
	if level != 1 {
		t.Fatalf("expected level=1, got %d", level)
	}

	// 10000 files — must complete in < 10ms
	files10000 := make([]manifest.FileInfo, 10000)
	for i := range files10000 {
		files10000[i] = manifest.FileInfo{
			CompactionLevel:   0,
			SchemaFingerprint: "fp1",
		}
	}
	start := time.Now()
	_, _ = p.Eligible(files10000, partitionTime)
	elapsed := time.Since(start)
	if elapsed >= 10*time.Millisecond {
		t.Fatalf("Eligible() for 10000 files took %v, expected < 10ms", elapsed)
	}
	t.Logf("Eligible(10000 files) completed in %v", elapsed)
}

// TestPolicy_SelectFiles_LargeCount verifies SelectFiles with mixed levels and fingerprints.
func TestPolicy_SelectFiles_LargeCount(t *testing.T) {
	p := NewLevelPolicy(10, 15, time.Hour)

	// 2000 files: mixed levels (0,1) and 3 fingerprints
	fps := []string{"fpA", "fpB", "fpC"}
	files := make([]manifest.FileInfo, 0, 2000)
	for i := 0; i < 2000; i++ {
		level := i % 2        // alternates 0 and 1
		fp := fps[i%len(fps)] // cycles through fpA, fpB, fpC
		files = append(files, manifest.FileInfo{
			Key:               fmt.Sprintf("file-%d", i),
			CompactionLevel:   level,
			SchemaFingerprint: fp,
		})
	}

	// Expected counts:
	// Total 2000 files. Level 0: indices 0,2,4,...(even) = 1000 files
	// Level 1: indices 1,3,5,...(odd) = 1000 files
	// Among level 0 (even indices 0,2,...,1998): fp = index%3
	//   even index i: fp = fps[i%3]
	//   We need to count per (level, fp) combination.
	expectedCounts := map[[2]string]int{}
	for i := 0; i < 2000; i++ {
		level := i % 2
		fp := fps[i%len(fps)]
		key := [2]string{fmt.Sprintf("%d", level), fp}
		expectedCounts[key]++
	}

	for level := 0; level <= 1; level++ {
		for _, fp := range fps {
			selected := p.SelectFiles(files, level, fp)
			expectedKey := [2]string{fmt.Sprintf("%d", level), fp}
			expected := expectedCounts[expectedKey]
			if len(selected) != expected {
				t.Errorf("SelectFiles(level=%d, fp=%s): got %d, want %d",
					level, fp, len(selected), expected)
			}
			// Output slice must be <= input size (no extra allocations beyond input)
			if len(selected) > len(files) {
				t.Errorf("SelectFiles returned %d files, larger than input %d", len(selected), len(files))
			}
			// Verify all returned files match the requested level and fingerprint
			for _, f := range selected {
				if f.CompactionLevel != level || f.SchemaFingerprint != fp {
					t.Errorf("unexpected file in selection: level=%d fp=%s, got level=%d fp=%s",
						level, fp, f.CompactionLevel, f.SchemaFingerprint)
				}
			}
		}
	}
}

// TestSharding_LargePartitionCount verifies ~equal distribution across 10 shards for 10000 partitions.
func TestSharding_LargePartitionCount(t *testing.T) {
	const totalPartitions = 10000
	const shardCount = 10
	const expectedPerShard = totalPartitions / shardCount // 1000
	const tolerance = 0.20                                // ±20%

	// Generate 10000 distinct partition names using the index directly for uniqueness.
	partitions := make([]string, totalPartitions)
	for i := range partitions {
		partitions[i] = fmt.Sprintf("dt=2026-01-01/hour=%05d", i)
	}

	// Build ownership map: partition → shardID that owns it.
	partitionOwner := make(map[string]int, totalPartitions)
	shardCounts := make([]int, shardCount)

	for shardID := 0; shardID < shardCount; shardID++ {
		s := NewPartitionSharding(shardID, shardCount)
		for _, p := range partitions {
			if s.OwnsPartition(p) {
				if existing, already := partitionOwner[p]; already {
					t.Fatalf("partition %q owned by both shard %d and shard %d", p, existing, shardID)
				}
				partitionOwner[p] = shardID
				shardCounts[shardID]++
			}
		}
	}

	// Verify all partitions are owned by exactly one shard (no gaps).
	for _, p := range partitions {
		if _, owned := partitionOwner[p]; !owned {
			t.Fatalf("partition %q not owned by any shard", p)
		}
	}

	// Verify distribution is within ±20% of expected.
	low := int(float64(expectedPerShard) * (1 - tolerance))
	high := int(float64(expectedPerShard) * (1 + tolerance))
	for id, count := range shardCounts {
		if count < low || count > high {
			t.Errorf("shard %d has %d partitions, expected %d±%d%% (%d–%d)",
				id, count, expectedPerShard, int(tolerance*100), low, high)
		}
	}
	t.Logf("10 shards, 10000 partitions: distribution=%v", shardCounts)
}

// TestSharding_PartitionOwnership_Deterministic verifies same shard always owns the same partition.
func TestSharding_PartitionOwnership_Deterministic(t *testing.T) {
	const partitionCount = 1000
	const shardCount = 5

	partitions := make([]string, partitionCount)
	for i := range partitions {
		partitions[i] = fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i/24)%28+1, i%24)
	}

	// For each shard, record first-pass ownership results.
	firstPass := make([][]bool, shardCount)
	for shardID := 0; shardID < shardCount; shardID++ {
		s := NewPartitionSharding(shardID, shardCount)
		firstPass[shardID] = make([]bool, partitionCount)
		for i, p := range partitions {
			firstPass[shardID][i] = s.OwnsPartition(p)
		}
	}

	// Second pass must produce identical results.
	for shardID := 0; shardID < shardCount; shardID++ {
		s := NewPartitionSharding(shardID, shardCount)
		for i, p := range partitions {
			got := s.OwnsPartition(p)
			if got != firstPass[shardID][i] {
				t.Errorf("non-deterministic: shard %d, partition %q: first=%v second=%v",
					shardID, p, firstPass[shardID][i], got)
			}
		}
	}
}

// TestScheduler_MaxConcurrentRespected verifies Scan() caps compactions at MaxConcurrent.
func TestScheduler_MaxConcurrentRespected(t *testing.T) {
	const maxConcurrent = 3
	const totalPartitions = 20
	const filesPerPartition = 15 // > MinFilesL0=10

	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)
	ctx := context.Background()
	const fp = "stress-fp"

	// Create 20 eligible partitions.
	// Use dates far in the past so MinAge=0 always passes.
	for partIdx := 0; partIdx < totalPartitions; partIdx++ {
		// Generate unique partition names over different days/hours.
		partition := fmt.Sprintf("dt=2026-01-%02d/hour=%02d", partIdx/24+1, partIdx%24)
		for i := 0; i < filesPerPartition; i++ {
			rows := []schema.LogRow{
				{
					TimestampUnixNano: int64(partIdx*10000 + i*1000 + 1),
					Body:              fmt.Sprintf("log-p%d-f%d", partIdx, i),
					ServiceName:       "stress-svc",
				},
			}
			data := makeTestParquet(t, rows)
			key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
			if err := pool.Upload(ctx, key, data); err != nil {
				t.Fatalf("Upload failed: %v", err)
			}
			m.AddFile(partition, manifest.FileInfo{
				Key:               key,
				Size:              int64(len(data)),
				RowCount:          1,
				MinTimeNs:         int64(partIdx*10000 + i*1000 + 1),
				MaxTimeNs:         int64(partIdx*10000 + i*1000 + 1),
				SchemaFingerprint: fp,
				CompactionLevel:   0,
			})
		}
	}

	sched := NewScheduler(SchedulerConfig{
		Manifest:         m,
		Pool:             pool,
		Ownership:        soleOwnerResolver(),
		Policy:           policy,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		Interval:         time.Minute,
		MaxConcurrent:    maxConcurrent,
		RowGroupSize:     1000,
		CompressionLevel: 1,
	})

	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if n > maxConcurrent {
		t.Fatalf("Scan ran %d compactions but MaxConcurrent=%d", n, maxConcurrent)
	}
	if n == 0 {
		t.Fatal("expected at least 1 compaction with 20 eligible partitions")
	}
	t.Logf("Scan ran %d compactions with MaxConcurrent=%d and %d eligible partitions",
		n, maxConcurrent, totalPartitions)
}

// TestMajoritySchemaFingerprint_LargeInput verifies correct majority selection with 5000 files and 100 fingerprints.
func TestMajoritySchemaFingerprint_LargeInput(t *testing.T) {
	const totalFiles = 5000
	const numFingerprints = 100
	const majorityFP = "fp-majority"
	const majorityCount = 2600 // > any other fingerprint count

	// Build 5000 files: majority fingerprint has 2600, rest 2400 spread across 99 fingerprints.
	files := make([]manifest.FileInfo, 0, totalFiles)

	// Add majority fingerprint files.
	for i := 0; i < majorityCount; i++ {
		files = append(files, manifest.FileInfo{
			Key:               fmt.Sprintf("majority-%d", i),
			CompactionLevel:   0,
			SchemaFingerprint: majorityFP,
		})
	}

	// Add remaining files with 99 different fingerprints.
	remaining := totalFiles - majorityCount
	for i := 0; i < remaining; i++ {
		fp := fmt.Sprintf("fp-%d", i%(numFingerprints-1)) // fp-0 through fp-98
		files = append(files, manifest.FileInfo{
			Key:               fmt.Sprintf("other-%d", i),
			CompactionLevel:   0,
			SchemaFingerprint: fp,
		})
	}

	if len(files) != totalFiles {
		t.Fatalf("expected %d files, got %d", totalFiles, len(files))
	}

	result := MajoritySchemaFingerprint(files, 0)
	if result != majorityFP {
		t.Fatalf("expected majority fingerprint %q, got %q", majorityFP, result)
	}
	t.Logf("MajoritySchemaFingerprint: correct majority=%q from %d files with %d distinct fingerprints",
		result, totalFiles, numFingerprints)
}
