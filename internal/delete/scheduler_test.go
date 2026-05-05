package delete

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// buildSchedulerForTest creates a scheduler for testing with given parameters.
func buildSchedulerForTest(t *testing.T, store *TombstoneStore, detector *StorageClassDetector, pool *mockRewriterPool, allowedClasses []string) *RewriteScheduler {
	t.Helper()
	rewriter := NewRewriter(pool, "logs/", 10000)
	return NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       rewriter,
		Detector:       detector,
		RewriteDelay:   time.Hour,
		AllowedClasses: allowedClasses,
		MaxConcurrent:  2,
	})
}

func TestSchedulerRunOnce_EligibleTombstone(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil) // no rules = always STANDARD

	key := "logs/dt=2026-01-01/hour=10/file1.parquet"

	// Create a valid parquet file with rows that match the tombstone query.
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "delete me", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "keep this", SeverityText: "info", ServiceName: "web"},
	}
	pool.objects[key] = buildTestParquet(t, rows)

	ts := Tombstone{
		ID:           "ts-1",
		Query:        `severity_text:="error"`,
		StartNs:      0,
		EndNs:        5000,
		AffectedKeys: []string{key},
		CreatedAt:    time.Now().Add(-2 * time.Hour), // older than 1h delay
		Mode:         "permanent",
		Reaped:       make(map[string]bool),
	}
	store.Add(ts)

	sched := buildSchedulerForTest(t, store, detector, pool, []string{"STANDARD"})

	beforeTotal := metrics.DeleteRewriteTotal.Get()

	results := sched.RunOnce(context.Background())

	afterTotal := metrics.DeleteRewriteTotal.Get()

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].RowsRemoved != 1 {
		t.Errorf("expected 1 row removed, got %d", results[0].RowsRemoved)
	}
	if results[0].RowsKept != 1 {
		t.Errorf("expected 1 row kept, got %d", results[0].RowsKept)
	}
	if afterTotal <= beforeTotal {
		t.Error("expected DeleteRewriteTotal to be incremented")
	}

	// Key should be marked as reaped.
	got, _ := store.Get("ts-1")
	if !got.Reaped[key] {
		t.Error("key should be marked as reaped after successful rewrite")
	}
}

func TestSchedulerRunOnce_HideMode(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil)

	ts := Tombstone{
		ID:           "ts-hide",
		Query:        "*",
		StartNs:      0,
		EndNs:        time.Now().UnixNano(),
		AffectedKeys: []string{"logs/dt=2026-01-01/file1.parquet"},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "hide",
		Reaped:       make(map[string]bool),
	}
	store.Add(ts)

	sched := buildSchedulerForTest(t, store, detector, pool, []string{"STANDARD"})

	beforeErrors := metrics.DeleteRewriteErrors.Get()
	beforeTotal := metrics.DeleteRewriteTotal.Get()

	results := sched.RunOnce(context.Background())

	afterErrors := metrics.DeleteRewriteErrors.Get()
	afterTotal := metrics.DeleteRewriteTotal.Get()

	if len(results) != 0 {
		t.Errorf("expected 0 results for hide mode, got %d", len(results))
	}
	if afterErrors != beforeErrors {
		t.Error("hide mode should not trigger rewrite errors")
	}
	if afterTotal != beforeTotal {
		t.Error("hide mode should not trigger rewrite total")
	}
}

func TestSchedulerRunOnce_TooRecent(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil)

	ts := Tombstone{
		ID:           "ts-recent",
		Query:        "*",
		StartNs:      0,
		EndNs:        time.Now().UnixNano(),
		AffectedKeys: []string{"logs/dt=2026-01-01/file1.parquet"},
		CreatedAt:    time.Now().Add(-5 * time.Minute), // only 5 min ago, delay is 1h
		Mode:         "permanent",
		Reaped:       make(map[string]bool),
	}
	store.Add(ts)

	sched := buildSchedulerForTest(t, store, detector, pool, []string{"STANDARD"})

	beforeErrors := metrics.DeleteRewriteErrors.Get()
	beforeTotal := metrics.DeleteRewriteTotal.Get()

	results := sched.RunOnce(context.Background())

	afterErrors := metrics.DeleteRewriteErrors.Get()
	afterTotal := metrics.DeleteRewriteTotal.Get()

	if len(results) != 0 {
		t.Errorf("expected 0 results for too-recent tombstone, got %d", len(results))
	}
	if afterErrors != beforeErrors {
		t.Error("too-recent tombstone should not trigger rewrite errors")
	}
	if afterTotal != beforeTotal {
		t.Error("too-recent tombstone should not trigger rewrite total")
	}
}

func TestSchedulerRunOnce_GlacierClassSkipped(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()

	// Detector with a rule that transitions to GLACIER after 30 days.
	detector := NewStorageClassDetector([]LifecycleRule{
		{TransitionDays: 30, Class: ClassGlacier},
	})

	ts := Tombstone{
		ID:           "ts-glacier",
		Query:        "*",
		StartNs:      0,
		EndNs:        time.Now().UnixNano(),
		AffectedKeys: []string{"logs/dt=2025-01-01/file1.parquet"},
		CreatedAt:    time.Now().Add(-60 * 24 * time.Hour), // 60 days ago → Glacier
		Mode:         "permanent",
		Reaped:       make(map[string]bool),
	}
	store.Add(ts)

	sched := buildSchedulerForTest(t, store, detector, pool, []string{"STANDARD"})

	beforeSkipped := metrics.DeleteRewriteSkippedGlacier.Get()

	results := sched.RunOnce(context.Background())

	afterSkipped := metrics.DeleteRewriteSkippedGlacier.Get()

	if len(results) != 0 {
		t.Errorf("expected 0 results for glacier skip, got %d", len(results))
	}
	if afterSkipped <= beforeSkipped {
		t.Error("expected DeleteRewriteSkippedGlacier to be incremented")
	}
}

func TestSchedulerRunOnce_AlreadyReaped(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil)

	ts := Tombstone{
		ID:           "ts-reaped",
		Query:        "*",
		StartNs:      0,
		EndNs:        time.Now().UnixNano(),
		AffectedKeys: []string{"logs/dt=2026-01-01/file1.parquet"},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "permanent",
		Reaped:       map[string]bool{"logs/dt=2026-01-01/file1.parquet": true},
	}
	store.Add(ts)

	sched := buildSchedulerForTest(t, store, detector, pool, []string{"STANDARD"})

	beforeErrors := metrics.DeleteRewriteErrors.Get()
	beforeTotal := metrics.DeleteRewriteTotal.Get()

	results := sched.RunOnce(context.Background())

	afterErrors := metrics.DeleteRewriteErrors.Get()
	afterTotal := metrics.DeleteRewriteTotal.Get()

	if len(results) != 0 {
		t.Errorf("expected 0 results for already-reaped key, got %d", len(results))
	}
	if afterErrors != beforeErrors {
		t.Error("already-reaped key should not trigger rewrite errors")
	}
	if afterTotal != beforeTotal {
		t.Error("already-reaped key should not trigger rewrite total")
	}
}

func TestSchedulerStartStop(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil)

	sched := buildSchedulerForTest(t, store, detector, pool, []string{"STANDARD"})

	// Start with a short interval.
	sched.Start(50 * time.Millisecond)

	// Let it tick at least once.
	time.Sleep(100 * time.Millisecond)

	// Stop should not hang.
	done := make(chan struct{})
	go func() {
		sched.Stop()
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2 seconds")
	}
}

func TestSchedulerVerify(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil)

	// Add two tombstones.
	store.Add(Tombstone{ID: "v1", Mode: "permanent", CreatedAt: time.Now()})
	store.Add(Tombstone{ID: "v2", Mode: "hide", CreatedAt: time.Now()})

	sched := buildSchedulerForTest(t, store, detector, pool, []string{"STANDARD"})

	beforeVerify := metrics.DeleteVerifyTotal.Get()

	sched.Verify(context.Background())

	afterVerify := metrics.DeleteVerifyTotal.Get()

	// Should increment once per active tombstone (2 total).
	if afterVerify-beforeVerify != 2 {
		t.Errorf("expected DeleteVerifyTotal to increment by 2, got %d", afterVerify-beforeVerify)
	}
}

func TestSchedulerRunOnce_AutoMode(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil)

	key := "logs/dt=2026-01-01/hour=10/auto.parquet"

	// Create a valid parquet file.
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "remove", SeverityText: "error", ServiceName: "svc"},
		{TimestampUnixNano: 2000, Body: "keep", SeverityText: "info", ServiceName: "svc"},
	}
	pool.objects[key] = buildTestParquet(t, rows)

	ts := Tombstone{
		ID:           "ts-auto",
		Query:        `severity_text:="error"`,
		StartNs:      0,
		EndNs:        5000,
		AffectedKeys: []string{key},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "auto",
		Reaped:       make(map[string]bool),
	}
	store.Add(ts)

	sched := buildSchedulerForTest(t, store, detector, pool, []string{"STANDARD"})

	beforeTotal := metrics.DeleteRewriteTotal.Get()

	results := sched.RunOnce(context.Background())

	afterTotal := metrics.DeleteRewriteTotal.Get()

	// Auto mode should attempt and succeed at rewrite.
	if len(results) != 1 {
		t.Fatalf("expected 1 result for auto mode, got %d", len(results))
	}
	if afterTotal <= beforeTotal {
		t.Error("auto mode should have incremented DeleteRewriteTotal")
	}
}

func TestSchedulerNewDefaults(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil)
	rewriter := NewRewriter(pool, "logs/", 10000)

	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:    store,
		Rewriter: rewriter,
		Detector: detector,
	})

	// Defaults: AllowedClasses should be ["STANDARD"].
	if !sched.allowedClasses["STANDARD"] {
		t.Error("default AllowedClasses should include STANDARD")
	}
	if sched.maxConcurrent != 1 {
		t.Errorf("default MaxConcurrent should be 1, got %d", sched.maxConcurrent)
	}
}

func TestSchedulerRunOnce_RewriteError(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil)

	key := "logs/dt=2026-01-01/hour=10/missing.parquet"
	// Don't add data to pool — Download will fail.

	ts := Tombstone{
		ID:           "ts-err",
		Query:        "*",
		StartNs:      0,
		EndNs:        5000,
		AffectedKeys: []string{key},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "permanent",
		Reaped:       make(map[string]bool),
	}
	store.Add(ts)

	sched := buildSchedulerForTest(t, store, detector, pool, []string{"STANDARD"})

	beforeErrors := metrics.DeleteRewriteErrors.Get()

	results := sched.RunOnce(context.Background())

	afterErrors := metrics.DeleteRewriteErrors.Get()

	if len(results) != 0 {
		t.Errorf("expected 0 results on error, got %d", len(results))
	}
	if afterErrors <= beforeErrors {
		t.Error("expected DeleteRewriteErrors to be incremented on rewrite failure")
	}

	// Key should NOT be reaped.
	got, _ := store.Get("ts-err")
	if got.Reaped[key] {
		t.Error("key should not be reaped after rewrite error")
	}
}
