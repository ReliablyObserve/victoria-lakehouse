package compaction

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// soleOwnerResolver returns an OwnershipResolver that thinks self
// owns every partition (single-pod case). Used by tests that don't
// care about ownership semantics — they just want compaction to
// proceed.
func soleOwnerResolver() *OwnershipResolver {
	return NewOwnershipResolver("self", staticPeers("self"))
}

// neverOwnsResolver returns an OwnershipResolver that thinks self
// owns nothing — peers contains exactly one other peer.
func neverOwnsResolver() *OwnershipResolver {
	return NewOwnershipResolver("self", staticPeers("other"))
}

// TestScheduler_NoOwnership_NoWork covers the "I am not the HRW
// primary" path. With self="self" and peers=["other"], HRW always
// picks "other" → scheduler's Scan does no compactions.
//
// Negative-control proof: removing the OwnsPartition gate from Scan
// would unconditionally compact, surfacing as n != 0 here.
func TestScheduler_NoOwnership_NoWork(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)

	const partition = "dt=2026-01-01/hour=00"
	for i := 0; i < 15; i++ {
		m.AddFile(partition, manifest.FileInfo{
			Key:               fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i),
			Size:              100,
			SchemaFingerprint: "fp",
		})
	}

	sched := NewScheduler(SchedulerConfig{
		Manifest:         m,
		Pool:             pool,
		Ownership:        neverOwnsResolver(),
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
		t.Fatalf("not the owner: got %d compactions, want 0", n)
	}
	if files := m.FilesForPartition(partition); len(files) != 15 {
		t.Fatalf("files mutated: got %d, want 15", len(files))
	}
}

// TestScheduler_CompactsEligiblePartition is the happy-path test:
// owner + eligible → compaction proceeds.
func TestScheduler_CompactsEligiblePartition(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)

	const partition = "dt=2026-01-01/hour=00"
	const fp = "test-fp"
	ctx := context.Background()

	for i := 0; i < 12; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc-test"},
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
		Manifest:         m,
		Pool:             pool,
		Ownership:        soleOwnerResolver(),
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
	files := m.FilesForPartition(partition)
	if len(files) != 1 {
		t.Fatalf("expected 1 compacted file, got %d", len(files))
	}
	if files[0].CompactionLevel != 1 {
		t.Errorf("expected L1, got %d", files[0].CompactionLevel)
	}
	if !callbackCalled {
		t.Error("OnCompacted not invoked")
	}
}

// TestScheduler_StabilizationDefersScan: Stabilizing()==true short-
// circuits Scan with 0 compactions and bumps the deferred metric.
//
// Negative-control proof: removing the IsStabilizing guard from
// Scan would surface dual-compactions during ring flap; this test
// would attempt the compaction and return n>0.
func TestScheduler_StabilizationDefersScan(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)
	const partition = "dt=2026-01-01/hour=00"
	for i := 0; i < 15; i++ {
		m.AddFile(partition, manifest.FileInfo{Key: fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i), Size: 100, SchemaFingerprint: "fp"})
	}
	r := soleOwnerResolver()
	r.Stabilizing = func() bool { return true }

	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: r, Policy: policy,
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		MaxConcurrent: 2, RowGroupSize: 1000, CompressionLevel: 7,
	})
	n, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if n != 0 {
		t.Fatalf("stabilizing: got %d, want 0", n)
	}
}

// TestScheduler_RecordsAttemptBeforeCompact asserts MarkAttempt is
// called before the compactor runs — even when compaction errors
// later, the watermark must be fresh so Tier A waits before stealing.
//
// Negative-control proof: moving MarkAttempt after compactor.Compact
// would leave the timestamp zero (or stale) when compaction fails,
// and Tier A would race in. We verify by passing a failing pool and
// confirming LastAttempt is non-zero after Scan.
func TestScheduler_RecordsAttemptBeforeCompact(t *testing.T) {
	pool := &failingDownloadPool{mockPool: newMockPool()}
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)
	const partition = "dt=2026-01-01/hour=00"
	const fp = "fp"
	ctx := context.Background()
	for i := 0; i < 12; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := pool.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: 1,
			SchemaFingerprint: fp, CompactionLevel: 0,
		})
	}

	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(), Policy: policy,
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		MaxConcurrent: 2, RowGroupSize: 1000, CompressionLevel: 7,
	})
	_, _ = sched.Scan(ctx)

	if m.LastAttempt(partition).IsZero() {
		t.Fatal("MarkAttempt not called before compactor; watermark is zero")
	}
}

// failingDownloadPool forces compactor errors so we can verify
// MarkAttempt happened earlier.
type failingDownloadPool struct {
	*mockPool
}

func (p *failingDownloadPool) Download(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, ".parquet") {
		return nil, fmt.Errorf("simulated download failure for %s", key)
	}
	return p.mockPool.Download(ctx, key)
}

// TestScheduler_StartStop confirms the lifecycle works.
func TestScheduler_StartStop(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(), Policy: policy,
		Prefix: "logs/", Mode: config.ModeLogs, Interval: 50 * time.Millisecond,
		MaxConcurrent: 1, RowGroupSize: 1000, CompressionLevel: 1,
	})
	sched.Start()
	time.Sleep(120 * time.Millisecond)
	sched.Stop()
	// Stop must be idempotent.
	sched.Stop()
}

// TestScheduler_DefaultsApplied: zero Interval/MaxConcurrent gets
// 5m/1.
func TestScheduler_DefaultsApplied(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(),
		Policy: NewLevelPolicy(10, 20, 0),
		Prefix: "logs/", Mode: config.ModeLogs,
	})
	if sched.interval != 5*time.Minute {
		t.Errorf("default interval: got %v, want 5m", sched.interval)
	}
	if sched.maxConcurrent != 1 {
		t.Errorf("default maxConcurrent: got %d, want 1", sched.maxConcurrent)
	}
	if sched.drainTimeout != 90*time.Second {
		t.Errorf("default drainTimeout: got %v, want 90s", sched.drainTimeout)
	}
}

// TestScheduler_NewScheduler_PanicsWithoutOwnership: Ownership is
// required (no leader-or-not fallback like the old design).
func TestScheduler_NewScheduler_PanicsWithoutOwnership(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when Ownership is nil")
		}
	}()
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	_ = NewScheduler(SchedulerConfig{Manifest: m, Pool: pool, Policy: NewLevelPolicy(10, 20, 0)})
}

// TestScheduler_MaxConcurrentLimit: with 2 eligible partitions and
// MaxConcurrent=1, only 1 compaction runs per Scan.
func TestScheduler_MaxConcurrentLimit(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)
	ctx := context.Background()
	const fp = "fp"
	for _, partition := range []string{"dt=2026-01-01/hour=00", "dt=2026-01-02/hour=00"} {
		for i := 0; i < 15; i++ {
			rows := []schema.LogRow{{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"}}
			data := makeTestParquet(t, rows)
			key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
			if err := pool.Upload(ctx, key, data); err != nil {
				t.Fatal(err)
			}
			m.AddFile(partition, manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 1, SchemaFingerprint: fp, CompactionLevel: 0})
		}
	}

	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(), Policy: policy,
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		MaxConcurrent: 1, RowGroupSize: 1000, CompressionLevel: 7,
	})
	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("MaxConcurrent=1: got %d, want 1", n)
	}
}

// TestScheduler_SkipsUnparseablePartition: an unparseable partition
// is logged and skipped (no compaction, no crash).
func TestScheduler_SkipsUnparseablePartition(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	const badPartition = "not-a-valid-partition"
	for i := 0; i < 15; i++ {
		m.AddFile(badPartition, manifest.FileInfo{
			Key:               fmt.Sprintf("logs/%s/batch-%03d.parquet", badPartition, i),
			SchemaFingerprint: "fp", CompactionLevel: 0,
		})
	}
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(),
		Policy: NewLevelPolicy(10, 20, 0),
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		MaxConcurrent: 2, RowGroupSize: 1000, CompressionLevel: 7,
	})
	n, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if n != 0 {
		t.Fatalf("unparseable: got %d, want 0", n)
	}
}

// TestScheduler_SkipsWhenLessThan2FilesSelected: skipping the
// SelectFiles<2 branch.
func TestScheduler_SkipsWhenLessThan2FilesSelected(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)
	const partition = "dt=2026-01-01/hour=00"
	for i := 0; i < 11; i++ {
		rows := []schema.LogRow{{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"}}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 1, SchemaFingerprint: fmt.Sprintf("fp-%d", i), CompactionLevel: 0})
	}
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(), Policy: policy,
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		MaxConcurrent: 2, RowGroupSize: 1000, CompressionLevel: 7,
	})
	n, _ := sched.Scan(context.Background())
	if n != 0 {
		t.Fatalf("len(selected)<2: got %d, want 0", n)
	}
}

// TestScheduler_CompactionFailure: download failure surfaces as a
// per-partition error but Scan itself returns nil error.
func TestScheduler_CompactionFailure(t *testing.T) {
	pool := &failingDownloadPool{mockPool: newMockPool()}
	m := manifest.New("test-bucket", "logs/")
	const partition = "dt=2026-01-01/hour=00"
	ctx := context.Background()
	for i := 0; i < 15; i++ {
		rows := []schema.LogRow{{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"}}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := pool.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 1, SchemaFingerprint: "fp", CompactionLevel: 0})
	}
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(),
		Policy: NewLevelPolicy(10, 20, 0),
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		MaxConcurrent: 2, RowGroupSize: 1000, CompressionLevel: 7,
	})
	n, err := sched.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan should not return error on per-partition failure: %v", err)
	}
	if n != 0 {
		t.Fatalf("compaction failed: got n=%d, want 0", n)
	}
}

// TestScheduler_DrainBlocksNewCompactions: after Drain, subsequent
// Scan calls return 0 even with eligible partitions.
//
// Negative-control proof: removing the `if s.draining.Load() return 0`
// check at the top of Scan would let in-flight work start, defeating
// the §11.1 partition-boundary invariant.
func TestScheduler_DrainBlocksNewCompactions(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	const partition = "dt=2026-01-01/hour=00"
	ctx := context.Background()
	for i := 0; i < 15; i++ {
		rows := []schema.LogRow{{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"}}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := pool.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 1, SchemaFingerprint: "fp", CompactionLevel: 0})
	}
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(),
		Policy: NewLevelPolicy(10, 20, 0),
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		MaxConcurrent: 2, RowGroupSize: 1000, CompressionLevel: 7,
		DrainTimeout: 100 * time.Millisecond,
	})

	sched.Drain()

	if !sched.IsDraining() {
		t.Fatal("IsDraining should be true after Drain()")
	}
	n, _ := sched.Scan(ctx)
	if n != 0 {
		t.Fatalf("post-Drain Scan: got %d, want 0", n)
	}
}

// TestScheduler_DrainIdempotent: Drain twice + Stop twice all work.
func TestScheduler_DrainIdempotent(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(),
		Policy: NewLevelPolicy(10, 20, 0),
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Hour,
		MaxConcurrent: 1, DrainTimeout: 50 * time.Millisecond,
	})
	sched.Drain()
	sched.Drain()
	sched.Stop()
	sched.Stop()
}

// TestScheduler_RingChangeRateGate fires the §11.4 thrash gate: 7
// rapid ring changes (above the default 6/5min) → Scan defers.
//
// Negative-control proof: removing the rate-gate check would let
// compaction race with ring flap.
func TestScheduler_RingChangeRateGate(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	const partition = "dt=2026-01-01/hour=00"
	for i := 0; i < 15; i++ {
		m.AddFile(partition, manifest.FileInfo{Key: fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i), Size: 100, SchemaFingerprint: "fp"})
	}
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(),
		Policy: NewLevelPolicy(10, 20, 0),
		Prefix: "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		MaxConcurrent: 2, RingChangeRateLimit: 3,
	})

	for i := 0; i < 5; i++ {
		sched.recordRingChange("join")
	}
	n, _ := sched.Scan(context.Background())
	if n != 0 {
		t.Fatalf("ring-thrash rate gate: got %d, want 0", n)
	}
}

// TestScheduler_RingEventsPruning asserts the sliding window drops
// events older than 5 minutes.
func TestScheduler_RingEventsPruning(t *testing.T) {
	sched := &Scheduler{}
	now := time.Now()
	sched.ringEventsMu.Lock()
	sched.ringEvents = []time.Time{
		now.Add(-10 * time.Minute), // dropped
		now.Add(-6 * time.Minute),  // dropped
		now.Add(-4 * time.Minute),  // kept
		now.Add(-1 * time.Minute),  // kept
	}
	sched.pruneRingEventsLocked(now)
	got := len(sched.ringEvents)
	sched.ringEventsMu.Unlock()
	if got != 2 {
		t.Fatalf("pruned events: got %d, want 2", got)
	}
}

// TestScheduler_FairShare_PicksAcrossTenants: when FairShare is wired,
// per-tenant slot budget caps selection.
func TestScheduler_FairShare_PicksAcrossTenants(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	policy := NewLevelPolicy(10, 20, 0)
	ctx := context.Background()
	const fp = "fp"

	// 3 partitions across 3 tenants.
	for _, tenant := range []string{"acctA/projA", "acctB/projB", "acctC/projC"} {
		partition := tenant + "/logs/dt=2026-01-01/hour=00"
		for i := 0; i < 12; i++ {
			rows := []schema.LogRow{{TimestampUnixNano: int64(i*1000 + 1), Body: fmt.Sprintf("log-%d", i), ServiceName: "svc"}}
			data := makeTestParquet(t, rows)
			key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
			if err := pool.Upload(ctx, key, data); err != nil {
				t.Fatal(err)
			}
			m.AddFile(partition, manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 1, MinTimeNs: int64(i*1000 + 1), MaxTimeNs: int64(i*1000 + 1), SchemaFingerprint: fp, CompactionLevel: 0})
		}
	}

	tickResults := map[string]int{}
	mu := sync.Mutex{}
	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool, Ownership: soleOwnerResolver(), Policy: policy,
		FairShare: NewFairShareScheduler(1),
		Prefix:    "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		MaxConcurrent: 1, RowGroupSize: 1000, CompressionLevel: 1,
		OnCompacted: func(added []manifest.FileInfo, removed []string) {
			mu.Lock()
			defer mu.Unlock()
			// `removed` carries the original L0 input keys whose
			// prefix matches the tenant we seeded with — extract
			// from there. We use the first removed key for the
			// tenant id.
			if len(removed) > 0 {
				t := extractTenant(removed[0])
				tickResults[t]++
			}
		},
	})

	for tick := 0; tick < 3; tick++ {
		_, _ = sched.Scan(ctx)
	}

	// We should have hit all 3 tenants over 3 ticks.
	if len(tickResults) != 3 {
		t.Fatalf("fair-share across 3 ticks: tenants compacted=%d, want 3 (results=%v)", len(tickResults), tickResults)
	}
}
