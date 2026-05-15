package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/election"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestIntegration_FullCompactionCycle(t *testing.T) {
	m := manifest.New("test", "logs/")
	pool := newMockPool()

	for i := 0; i < 12; i++ {
		rows := []schema.LogRow{
			{
				TimestampUnixNano: int64(i * 1_000_000_000),
				Body:              fmt.Sprintf("log line %d", i),
				ServiceName:       "test-svc",
			},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/dt=2026-05-02/hour=10/batch-%02d.parquet", i)
		_ = pool.Upload(context.Background(), key, data)
		m.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			MinTimeNs:         int64(i * 1_000_000_000),
			MaxTimeNs:         int64(i * 1_000_000_000),
			CompactionLevel:   0,
			SchemaFingerprint: "fp1",
		})
	}

	if m.TotalFiles() != 12 {
		t.Fatalf("expected 12 files, got %d", m.TotalFiles())
	}

	leader := election.NewNoopElector()
	sentinel := NewSentinel(pool, 10*time.Minute)
	policy := NewLevelPolicy(10, 10, 0)

	var notifiedAdded []manifest.FileInfo
	var notifiedRemoved []string

	sched := NewScheduler(SchedulerConfig{
		Leader:           leader,
		Manifest:         m,
		Pool:             pool,
		Sentinel:         sentinel,
		Policy:           policy,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     1000,
		CompressionLevel: 1,
		MaxConcurrent:    1,
		OnCompacted: func(added []manifest.FileInfo, removed []string) {
			notifiedAdded = added
			notifiedRemoved = removed
		},
	})

	compacted, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if compacted != 1 {
		t.Fatalf("expected 1 compaction, got %d", compacted)
	}

	files := m.FilesForPartition("dt=2026-05-02/hour=10")
	if len(files) != 1 {
		t.Fatalf("expected 1 file after compaction, got %d", len(files))
	}
	if files[0].CompactionLevel != 1 {
		t.Fatalf("expected L1, got L%d", files[0].CompactionLevel)
	}
	if files[0].RowCount != 12 {
		t.Fatalf("expected 12 rows merged, got %d", files[0].RowCount)
	}

	if len(notifiedRemoved) != 12 {
		t.Fatalf("expected 12 removed notifications, got %d", len(notifiedRemoved))
	}
	if len(notifiedAdded) == 0 {
		t.Fatal("expected added notification")
	}

	locked, err := sentinel.IsLocked(context.Background(), "logs/", "dt=2026-05-02/hour=10")
	if err != nil {
		t.Fatal(err)
	}
	if locked {
		t.Fatal("sentinel should be released after compaction")
	}
}

func TestIntegration_L1ToL2(t *testing.T) {
	m := manifest.New("test", "logs/")
	pool := newMockPool()

	for i := 0; i < 12; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i * 1_000_000_000), Body: fmt.Sprintf("line %d", i), ServiceName: "svc"},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/dt=2026-05-02/hour=10/compacted-L1-%02d.parquet", i)
		_ = pool.Upload(context.Background(), key, data)
		m.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			CompactionLevel:   1,
			SchemaFingerprint: "fp1",
		})
	}

	sched := NewScheduler(SchedulerConfig{
		Leader:           election.NewNoopElector(),
		Manifest:         m,
		Pool:             pool,
		Sentinel:         NewSentinel(pool, 10*time.Minute),
		Policy:           NewLevelPolicy(10, 10, 0),
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     1000,
		CompressionLevel: 1,
		MaxConcurrent:    1,
	})

	compacted, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if compacted != 1 {
		t.Fatalf("expected 1 L1->L2 compaction, got %d", compacted)
	}

	files := m.FilesForPartition("dt=2026-05-02/hour=10")
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].CompactionLevel != 2 {
		t.Fatalf("expected L2, got L%d", files[0].CompactionLevel)
	}
}
