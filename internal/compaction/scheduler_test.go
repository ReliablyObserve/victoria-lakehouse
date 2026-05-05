package compaction

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type staticLeader struct{ leader bool }

func (s *staticLeader) IsLeader() bool          { return s.leader }
func (s *staticLeader) Start(_ context.Context) {}
func (s *staticLeader) Stop()                   {}

func TestScheduler_SkipsWhenNotLeader(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/", logger)
	sentinel := NewSentinel(pool, time.Hour)
	policy := NewLevelPolicy(10, 20, 0)

	const partition = "dt=2026-01-01/hour=00"
	const fp = "abc123"

	// Add 15 L0 files — enough to be eligible.
	for i := 0; i < 15; i++ {
		m.AddFile(partition, manifest.FileInfo{
			Key:               fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i),
			Size:              100,
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

	sched := NewScheduler(SchedulerConfig{
		Leader:           &staticLeader{leader: false},
		Manifest:         m,
		Pool:             pool,
		Sentinel:         sentinel,
		Policy:           policy,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		Interval:         time.Minute,
		MaxConcurrent:    2,
		RowGroupSize:     1000,
		CompressionLevel: 3,
		Logger:           logger,
	})

	n, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 compactions when not leader, got %d", n)
	}

	// Verify files are untouched.
	files := m.FilesForPartition(partition)
	if len(files) != 15 {
		t.Fatalf("expected 15 files still in manifest, got %d", len(files))
	}
}

func TestScheduler_CompactsEligiblePartition(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/", logger)
	sentinel := NewSentinel(pool, time.Hour)
	// MinFilesL0=10, so 12 L0 files qualifies.
	policy := NewLevelPolicy(10, 20, 0)

	const partition = "dt=2026-01-01/hour=00"
	const fp = "test-fp"
	ctx := context.Background()

	// Create 12 real parquet files and upload them.
	for i := 0; i < 12; i++ {
		rows := []schema.LogRow{
			{
				TimestampUnixNano: int64(i*1000 + 1),
				Body:              fmt.Sprintf("log-%d", i),
				ServiceName:       "svc-test",
			},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := pool.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			MinTimeNs:         int64(i*1000 + 1),
			MaxTimeNs:         int64(i*1000 + 1),
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

	var callbackCalled bool
	sched := NewScheduler(SchedulerConfig{
		Leader:           &staticLeader{leader: true},
		Manifest:         m,
		Pool:             pool,
		Sentinel:         sentinel,
		Policy:           policy,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		Interval:         time.Minute,
		MaxConcurrent:    2,
		RowGroupSize:     1000,
		CompressionLevel: 3,
		Logger:           logger,
		OnCompacted: func(added []manifest.FileInfo, removed []string) {
			callbackCalled = true
		},
	})

	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 compaction, got %d", n)
	}

	// Verify manifest: should have exactly 1 file at L1.
	files := m.FilesForPartition(partition)
	if len(files) != 1 {
		t.Fatalf("expected 1 file in manifest after compaction, got %d", len(files))
	}
	if files[0].CompactionLevel != 1 {
		t.Errorf("expected compaction level 1, got %d", files[0].CompactionLevel)
	}

	// Verify callback was called.
	if !callbackCalled {
		t.Error("expected OnCompacted callback to be called")
	}

	// Verify sentinel is released.
	locked, err := sentinel.IsLocked(ctx, "logs/", partition)
	if err != nil {
		t.Fatalf("IsLocked error: %v", err)
	}
	if locked {
		t.Error("expected sentinel to be released after compaction")
	}
}
