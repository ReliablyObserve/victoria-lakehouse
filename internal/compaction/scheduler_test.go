package compaction

import (
	"context"
	"fmt"
	"strings"
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
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
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
		CompressionLevel: 7,
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
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
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
		CompressionLevel: 7,
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

// --- Start/Stop lifecycle test ---

func TestScheduler_StartStop(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	sentinel := NewSentinel(pool, time.Hour)
	policy := NewLevelPolicy(10, 20, 0)

	sched := NewScheduler(SchedulerConfig{
		Leader:           &staticLeader{leader: true},
		Manifest:         m,
		Pool:             pool,
		Sentinel:         sentinel,
		Policy:           policy,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		Interval:         50 * time.Millisecond,
		MaxConcurrent:    1,
		RowGroupSize:     1000,
		CompressionLevel: 1,
	})

	sched.Start()
	// Let at least one tick fire.
	time.Sleep(120 * time.Millisecond)
	sched.Stop()
	// Stop should not panic or hang; if we get here, the test passes.
}

func TestScheduler_DefaultsApplied(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	sentinel := NewSentinel(pool, time.Hour)
	policy := NewLevelPolicy(10, 20, 0)

	// Zero interval and zero maxConcurrent should get defaults.
	sched := NewScheduler(SchedulerConfig{
		Leader:   &staticLeader{leader: true},
		Manifest: m,
		Pool:     pool,
		Sentinel: sentinel,
		Policy:   policy,
		Prefix:   "logs/",
		Mode:     config.ModeLogs,
	})

	if sched.interval != 5*time.Minute {
		t.Errorf("expected default interval 5m, got %v", sched.interval)
	}
	if sched.maxConcurrent != 1 {
		t.Errorf("expected default maxConcurrent 1, got %d", sched.maxConcurrent)
	}
}

// --- Locked partition test ---

func TestScheduler_SkipsLockedPartition(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	sentinel := NewSentinel(pool, time.Hour)
	policy := NewLevelPolicy(10, 20, 0)

	const partition = "dt=2026-01-01/hour=00"
	const fp = "abc123"
	ctx := context.Background()

	// Add 15 L0 files with real parquet data.
	for i := 0; i < 15; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"},
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
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

	// Lock the partition via sentinel before scan.
	ok, err := sentinel.Acquire(ctx, "logs/", partition, "other-worker")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("failed to acquire sentinel lock")
	}

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
		CompressionLevel: 7,
	})

	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 compactions for locked partition, got %d", n)
	}

	// Files should be untouched.
	files := m.FilesForPartition(partition)
	if len(files) != 15 {
		t.Fatalf("expected 15 files still in manifest, got %d", len(files))
	}
}

// --- Sentinel 404 treated as not locked ---

type sentinelNotFoundPool struct {
	*mockPool
}

func (f *sentinelNotFoundPool) Download(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, "/_compacting") {
		return nil, fmt.Errorf("s3 GetObject: NoSuchKey")
	}
	return f.mockPool.Download(ctx, key)
}

func TestScheduler_SentinelNotFound_CompactionProceeds(t *testing.T) {
	base := newMockPool()
	pool := &sentinelNotFoundPool{mockPool: base}
	m := manifest.New("test-bucket", "logs/")
	sentinel := NewSentinel(pool, time.Hour)
	policy := NewLevelPolicy(10, 20, 0)

	const partition = "dt=2026-01-01/hour=00"
	const fp = "abc123"
	ctx := context.Background()

	for i := 0; i < 15; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := base.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

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
		CompressionLevel: 7,
	})

	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if n == 0 {
		t.Fatal("expected compaction to proceed when sentinel returns 404 (not locked)")
	}
}

// --- MaxConcurrent limit test ---

func TestScheduler_MaxConcurrentLimit(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	sentinel := NewSentinel(pool, time.Hour)
	policy := NewLevelPolicy(10, 20, 0)
	ctx := context.Background()
	const fp = "abc123"

	// Create 2 eligible partitions, each with 15 files.
	partitions := []string{"dt=2026-01-01/hour=00", "dt=2026-01-02/hour=00"}
	for _, partition := range partitions {
		for i := 0; i < 15; i++ {
			rows := []schema.LogRow{
				{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"},
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
				SchemaFingerprint: fp,
				CompactionLevel:   0,
			})
		}
	}

	sched := NewScheduler(SchedulerConfig{
		Leader:           &staticLeader{leader: true},
		Manifest:         m,
		Pool:             pool,
		Sentinel:         sentinel,
		Policy:           policy,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		Interval:         time.Minute,
		MaxConcurrent:    1, // Only allow 1 at a time.
		RowGroupSize:     1000,
		CompressionLevel: 7,
	})

	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 compaction due to maxConcurrent=1, got %d", n)
	}
}

// --- Unparseable partition test ---

func TestScheduler_SkipsUnparseablePartition(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	sentinel := NewSentinel(pool, time.Hour)
	policy := NewLevelPolicy(10, 20, 0)
	ctx := context.Background()
	const fp = "abc123"

	// Use an unparseable partition name.
	const badPartition = "not-a-valid-partition"
	for i := 0; i < 15; i++ {
		m.AddFile(badPartition, manifest.FileInfo{
			Key:               fmt.Sprintf("logs/%s/batch-%03d.parquet", badPartition, i),
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

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
		CompressionLevel: 7,
	})

	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 compactions for unparseable partition, got %d", n)
	}
}

// --- Less than 2 files after selection test ---

func TestScheduler_SkipsWhenLessThan2FilesSelected(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	sentinel := NewSentinel(pool, time.Hour)
	// Set threshold to 10, but we'll have 11 L0 files with mixed fingerprints.
	policy := NewLevelPolicy(10, 20, 0)
	ctx := context.Background()

	const partition = "dt=2026-01-01/hour=00"

	// 11 files total: 10 with fp1, 1 with fp2. The majority is fp1 (10 files).
	// After selection at level 0 with fp1, we get 10 files (>2) — so that actually compacts.
	// To test <2, use 11 files each with a unique fingerprint. Majority will be "fp-0" (1 file).
	// Actually we need a scenario where the majority fingerprint has only 1 file at the right level.
	// Use all unique fingerprints.
	for i := 0; i < 11; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"},
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
			SchemaFingerprint: fmt.Sprintf("fp-%d", i), // each unique
			CompactionLevel:   0,
		})
	}

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
		CompressionLevel: 7,
	})

	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	// MajoritySchemaFingerprint picks one fp with count=1, SelectFiles returns only 1 file.
	// Since 1 < 2, compaction is skipped.
	if n != 0 {
		t.Fatalf("expected 0 compactions when only 1 file per fingerprint, got %d", n)
	}
}

// --- Sentinel acquire returns error test ---
// Uses a pool where IsLocked (Download of sentinel key) succeeds with "not found" (nil,nil),
// but Upload (in Acquire) fails.

type acquireFailPool struct {
	*mockPool
	uploadFailOnSentinel bool
}

func (p *acquireFailPool) Upload(ctx context.Context, key string, data []byte) error {
	if p.uploadFailOnSentinel {
		// Fail only on sentinel keys.
		if len(key) > 12 && key[len(key)-12:] == "/_compacting" {
			return fmt.Errorf("simulated acquire upload failure")
		}
	}
	return p.mockPool.Upload(ctx, key, data)
}

func TestScheduler_SentinelAcquireError(t *testing.T) {
	base := newMockPool()
	pool := &acquireFailPool{mockPool: base, uploadFailOnSentinel: true}
	m := manifest.New("test-bucket", "logs/")
	sentinel := NewSentinel(pool, time.Hour)
	policy := NewLevelPolicy(10, 20, 0)
	ctx := context.Background()
	const fp = "abc123"
	const partition = "dt=2026-01-01/hour=00"

	for i := 0; i < 15; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := base.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

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
		CompressionLevel: 7,
	})

	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	// Sentinel acquire fails (Upload error), so no compaction.
	if n != 0 {
		t.Fatalf("expected 0 compactions when sentinel acquire fails, got %d", n)
	}
}

// --- Compaction failure within Scan ---
// Pool that fails on download of parquet files (not sentinel keys).

type compactionFailPool struct {
	*mockPool
	failParquetDownload bool
}

func (p *compactionFailPool) Download(ctx context.Context, key string) ([]byte, error) {
	if p.failParquetDownload {
		if len(key) > 8 && key[len(key)-8:] == ".parquet" {
			return nil, fmt.Errorf("simulated parquet download failure")
		}
	}
	return p.mockPool.Download(ctx, key)
}

func TestScheduler_CompactionFailure(t *testing.T) {
	base := newMockPool()
	// The pool will allow sentinel operations but fail on parquet downloads.
	pool := &compactionFailPool{mockPool: base, failParquetDownload: true}
	m := manifest.New("test-bucket", "logs/")
	sentinel := NewSentinel(pool, time.Hour)
	policy := NewLevelPolicy(10, 20, 0)
	ctx := context.Background()
	const fp = "abc123"
	const partition = "dt=2026-01-01/hour=00"

	for i := 0; i < 15; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := base.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

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
		CompressionLevel: 7,
	})

	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan error: %v", err)
	}
	// Compaction fails due to download error, but Scan itself doesn't return error.
	if n != 0 {
		t.Fatalf("expected 0 compactions when compaction fails, got %d", n)
	}

	// Verify sentinel was released after failed compaction.
	locked, err := sentinel.IsLocked(ctx, "logs/", partition)
	if err != nil {
		t.Fatalf("IsLocked error: %v", err)
	}
	if locked {
		t.Error("sentinel should be released after compaction failure")
	}
}
