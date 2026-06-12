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

// seedL2 writes n real L2 Parquet files into one partition under a tenant
// prefix, each carrying the given schema fingerprint, and returns their keys.
// Using makeTestParquet (real Parquet bytes through the mock pool) means the
// scheduler's full Scan → Compactor.Compact → mergeLogFiles path runs for
// real, not a stub — so a green test proves the merge actually happened.
func seedL2(t *testing.T, m *manifest.Manifest, pool *mockPool, partition, fp string, n int) []string {
	t.Helper()
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rows := []schema.LogRow{{
			TimestampUnixNano: int64((i + 1) * 1_000_000_000),
			Body:              fmt.Sprintf("stale line %d", i),
			ServiceName:       "svc",
		}}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("100/1/logs/%s/compacted-L2-old-%02d.parquet", partition, i)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatalf("upload: %v", err)
		}
		m.AddFile(partition, manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			MinTimeNs:         int64((i + 1) * 1_000_000_000),
			MaxTimeNs:         int64((i + 1) * 1_000_000_000),
			CompactionLevel:   2,
			SchemaFingerprint: fp,
		})
		keys = append(keys, key)
	}
	return keys
}

// newRecompactScheduler builds a single-pod scheduler (it owns every
// partition) with the given current fingerprint and a level policy whose
// thresholds are high enough that L0/L1 eligibility never fires — so any
// compaction observed is driven solely by the recompaction hint.
func newRecompactScheduler(m *manifest.Manifest, pool *mockPool, currentFP string) *Scheduler {
	return NewScheduler(SchedulerConfig{
		Manifest:                 m,
		Pool:                     pool,
		Ownership:                NewOwnershipResolver("self", staticPeers("self")),
		Policy:                   NewLevelPolicy(10, 10, 0), // L0/L1 thresholds unreachable here
		Prefix:                   "logs/",
		Mode:                     config.ModeLogs,
		Interval:                 time.Minute,
		RowGroupSize:             1000,
		CompressionLevel:         1,
		MaxConcurrent:            4,
		CurrentSchemaFingerprint: currentFP,
	})
}

// TestRecompact_E2E_StaleSchemaDrivesCompaction is the hint-driven
// recompaction end-to-end proof. Two L2 files carry an OLD fingerprint (v1) in
// a partition the level policy would never re-pick (L2 with the high
// L0/L1 thresholds). With the scheduler's CurrentSchemaFingerprint set to v2,
// recompactionLevel flags the partition stale and the Scan compacts it anyway:
// the two inputs merge into one L3 output, the inputs vanish from the manifest
// and the pool, and the merged row count is conserved.
func TestRecompact_E2E_StaleSchemaDrivesCompaction(t *testing.T) {
	const partition = "dt=2026-06-01/hour=00"

	m := manifest.New("test", "logs/")
	pool := newMockPool()
	inputKeys := seedL2(t, m, pool, partition, "v1", 2)

	// Sanity: the level policy ALONE must not pick this partition — proving the
	// recompaction hint is what drives the compaction below, not eligibility.
	pt, err := manifest.ParsePartitionTime(partition)
	if err != nil {
		t.Fatalf("parse partition time: %v", err)
	}
	if _, eligible := NewLevelPolicy(10, 10, 0).Eligible(m.FilesForPartition(partition), pt); eligible {
		t.Fatal("precondition failed: level policy considered the partition eligible; " +
			"the test can no longer attribute compaction to the recompaction hint")
	}

	sched := newRecompactScheduler(m, pool, "v2")

	compacted, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if compacted != 1 {
		t.Fatalf("compacted = %d, want 1 (stale-schema hint should drive recompaction)", compacted)
	}

	// Manifest: exactly one file now, at L3 (sourceLevel 2 + 1), 2 rows merged.
	files := m.FilesForPartition(partition)
	if len(files) != 1 {
		t.Fatalf("files after recompaction = %d, want 1", len(files))
	}
	out := files[0]
	if out.CompactionLevel != 3 {
		t.Errorf("output level = L%d, want L3", out.CompactionLevel)
	}
	if out.RowCount != 2 {
		t.Errorf("output row_count = %d, want 2 (rows conserved across merge)", out.RowCount)
	}

	// The two stale inputs must be gone from BOTH the manifest and the pool.
	for _, k := range inputKeys {
		if out.Key == k {
			t.Errorf("output key %q collides with an input key", k)
		}
		for _, f := range files {
			if f.Key == k {
				t.Errorf("stale input %q still present in manifest after recompaction", k)
			}
		}
		data, derr := pool.Download(context.Background(), k)
		if derr != nil {
			t.Fatalf("download %q: %v", k, derr)
		}
		if data != nil {
			t.Errorf("stale input %q still present in pool after recompaction", k)
		}
	}

	// The compacted output must actually exist in the pool.
	if data, _ := pool.Download(context.Background(), out.Key); data == nil {
		t.Errorf("compacted output %q missing from pool", out.Key)
	}

	// An attempt watermark must be stamped (Tier A relies on it).
	if m.LastAttempt(partition).IsZero() {
		t.Error("LastAttempt not stamped after recompaction Scan")
	}

	// Second pass must be a no-op: the output now carries the majority L3
	// fingerprint and is a lone top-level file, so neither the policy nor the
	// hint re-picks it — the partition has self-healed.
	again, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatalf("second Scan error: %v", err)
	}
	if again != 0 {
		t.Errorf("second Scan compacted = %d, want 0 (partition already healed)", again)
	}
}

// TestRecompact_Scheduler_CleanCurrentPartition_NoChurn is the negative
// control at the scheduler level: a single, current-fingerprint, top-level
// (L2) file must NOT be recompacted. The level policy skips it (no L0/L1
// churn) and the recompaction hint also declines (not stale, only one file at
// the top level), so a full Scan does nothing and the file is left untouched.
func TestRecompact_Scheduler_CleanCurrentPartition_NoChurn(t *testing.T) {
	const partition = "dt=2026-06-02/hour=00"

	m := manifest.New("test", "logs/")
	pool := newMockPool()

	rows := []schema.LogRow{{TimestampUnixNano: 1_000_000_000, Body: "clean", ServiceName: "svc"}}
	data := makeTestParquet(t, rows)
	key := "100/1/logs/" + partition + "/compacted-L2-clean.parquet"
	if err := pool.Upload(context.Background(), key, data); err != nil {
		t.Fatalf("upload: %v", err)
	}
	m.AddFile(partition, manifest.FileInfo{
		Key:               key,
		Size:              int64(len(data)),
		RowCount:          1,
		CompactionLevel:   2,
		SchemaFingerprint: "v2", // matches CurrentSchemaFingerprint → not stale
	})

	sched := newRecompactScheduler(m, pool, "v2")

	compacted, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if compacted != 0 {
		t.Fatalf("compacted = %d, want 0 (clean current single-top-level file must not churn)", compacted)
	}

	// The original file must still be present, untouched.
	files := m.FilesForPartition(partition)
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1 (untouched)", len(files))
	}
	if files[0].Key != key {
		t.Errorf("file key = %q, want %q (no rewrite)", files[0].Key, key)
	}
	if files[0].CompactionLevel != 2 {
		t.Errorf("file level = L%d, want L2 (no level bump)", files[0].CompactionLevel)
	}
	if d, _ := pool.Download(context.Background(), key); d == nil {
		t.Error("clean file deleted from pool; want untouched")
	}
}
