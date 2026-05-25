package lifecycle

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ---------------------------------------------------------------------------
// 1. Shutdown phase ordering — verify exact sequence, not just all-ran
// ---------------------------------------------------------------------------

func TestShutdownPhaseOrdering_StrictSequence(t *testing.T) {
	// Each phase records its index at call time via an atomic counter.
	// If phases overlap or reorder, indices will mismatch.
	var seq atomic.Int32
	type entry struct {
		name string
		idx  int32
	}
	var mu sync.Mutex
	var entries []entry

	record := func(name string) {
		idx := seq.Add(1)
		mu.Lock()
		entries = append(entries, entry{name, idx})
		mu.Unlock()
	}

	hooks := ShutdownHooks{
		OnDrain:   func(ctx context.Context) error { record("drain"); return nil },
		OnFlush:   func(ctx context.Context) (int64, error) { record("flush"); return 0, nil },
		OnPersist: func(ctx context.Context) error { record("persist"); return nil },
		OnRelease: func(ctx context.Context) error { record("release"); return nil },
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	if err := orch.Execute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"drain", "flush", "persist", "release"}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d", len(entries), len(want))
	}
	for i, w := range want {
		if entries[i].name != w {
			t.Errorf("phase[%d] = %q, want %q", i, entries[i].name, w)
		}
		if entries[i].idx != int32(i+1) {
			t.Errorf("phase %q ran at position %d, want %d", entries[i].name, entries[i].idx, i+1)
		}
	}
}

func TestShutdownPhaseOrdering_NoOverlap(t *testing.T) {
	// Verify that phases do not execute concurrently — each must finish
	// before the next starts.
	var running atomic.Int32
	var maxConcurrent atomic.Int32

	overlap := func(dur time.Duration) {
		cur := running.Add(1)
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(dur)
		running.Add(-1)
	}

	hooks := ShutdownHooks{
		OnDrain:   func(ctx context.Context) error { overlap(10 * time.Millisecond); return nil },
		OnFlush:   func(ctx context.Context) (int64, error) { overlap(10 * time.Millisecond); return 0, nil },
		OnPersist: func(ctx context.Context) error { overlap(10 * time.Millisecond); return nil },
		OnRelease: func(ctx context.Context) error { overlap(10 * time.Millisecond); return nil },
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	if err := orch.Execute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mc := maxConcurrent.Load(); mc > 1 {
		t.Errorf("max concurrent phases = %d, want 1 (no overlap)", mc)
	}
}

// ---------------------------------------------------------------------------
// 2. Shutdown cancellation — parent context cancelled mid-shutdown
// ---------------------------------------------------------------------------

func TestShutdownCancellation_PropagatesContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	hooks := ShutdownHooks{
		OnDrain: func(ctx context.Context) error {
			// Cancel the parent context during drain phase
			cancel()
			return nil
		},
		OnFlush: func(ctx context.Context) (int64, error) {
			// When parent is cancelled, the derived phaseCtx is already
			// done. runPhase may return the timeout error before this
			// goroutine even starts, but we verify the error propagates.
			return 0, ctx.Err()
		},
		OnPersist: func(ctx context.Context) error {
			return ctx.Err()
		},
		OnRelease: func(ctx context.Context) error {
			return ctx.Err()
		},
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	err := orch.Execute(ctx)

	// Should get an error because context was cancelled mid-shutdown
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	// The orchestrator should still be marked as draining
	if !orch.IsDraining() {
		t.Error("should still be draining after cancelled shutdown")
	}
}

func TestShutdownCancellation_AllPhasesStillAttempted(t *testing.T) {
	// Even when parent context is cancelled, orchestrator continues through
	// all phases (it records errors but does not short-circuit).
	// Note: with positive timeouts, runPhase launches hooks in goroutines
	// and may return the timeout error before the hook executes. Use zero
	// timeouts so hooks are called inline on the cancelled context.
	ctx, cancel := context.WithCancel(context.Background())

	var phasesRun atomic.Int32

	hooks := ShutdownHooks{
		OnDrain: func(ctx context.Context) error {
			phasesRun.Add(1)
			cancel()
			return nil
		},
		OnFlush: func(ctx context.Context) (int64, error) {
			phasesRun.Add(1)
			return 0, ctx.Err()
		},
		OnPersist: func(ctx context.Context) error {
			phasesRun.Add(1)
			return ctx.Err()
		},
		OnRelease: func(ctx context.Context) error {
			phasesRun.Add(1)
			return ctx.Err()
		},
	}

	// Zero timeouts so runPhase calls hooks inline (no goroutine race)
	cfg := config.ShutdownConfig{
		Delay:          0,
		FlushTimeout:   0,
		PersistTimeout: 0,
		ReleaseTimeout: 0,
	}

	orch := NewShutdownOrchestrator(cfg, hooks)
	_ = orch.Execute(ctx)

	if n := phasesRun.Load(); n != 4 {
		t.Errorf("phases run = %d, want 4 (all phases attempted even after cancel)", n)
	}
}

// ---------------------------------------------------------------------------
// 3. Config validation edge cases
// ---------------------------------------------------------------------------

func TestValidateShutdown_ZeroTimeouts(t *testing.T) {
	cfg := &config.Config{
		Shutdown: config.ShutdownConfig{
			Delay:          0,
			FlushTimeout:   0,
			PersistTimeout: 0,
			ReleaseTimeout: 0,
		},
	}
	// Zero total = 0, terminationGracePeriod 30s - 5s margin = 25s. 0 <= 25 -> valid.
	if err := cfg.ValidateShutdown(30 * time.Second); err != nil {
		t.Errorf("zero timeouts should be valid, got: %v", err)
	}
}

func TestValidateShutdown_NegativeValues(t *testing.T) {
	// Negative durations are nonsensical but should not cause panics.
	// Go's time.Duration is a signed int64, so negative values can
	// reduce the sum. The function should still evaluate arithmetically.
	cfg := &config.Config{
		Shutdown: config.ShutdownConfig{
			Delay:          -1 * time.Second,
			FlushTimeout:   10 * time.Second,
			PersistTimeout: 5 * time.Second,
			ReleaseTimeout: 5 * time.Second,
		},
	}
	// total = -1 + 10 + 5 + 5 = 19s. Budget = 30s - 5s = 25s. 19 <= 25 -> valid.
	if err := cfg.ValidateShutdown(30 * time.Second); err != nil {
		t.Errorf("negative delay should still compute correctly, got: %v", err)
	}
}

func TestValidateShutdown_ExactBudgetBoundary(t *testing.T) {
	// The function checks: total > terminationGracePeriod - margin
	// So total == budget - margin should be valid (not exceeding).
	margin := 5 * time.Second
	budget := 60 * time.Second
	maxAllowed := budget - margin // 55s

	cfg := &config.Config{
		Shutdown: config.ShutdownConfig{
			Delay:          10 * time.Second,
			FlushTimeout:   30 * time.Second,
			PersistTimeout: 10 * time.Second,
			ReleaseTimeout: 5 * time.Second,
			// total = 10+30+10+5 = 55s == maxAllowed
		},
	}

	if err := cfg.ValidateShutdown(budget); err != nil {
		t.Errorf("exact boundary (total=%s == budget-margin=%s) should be valid, got: %v",
			maxAllowed, maxAllowed, err)
	}

	// One second over the boundary should fail
	cfg.Shutdown.ReleaseTimeout = 6 * time.Second // total = 56s > 55s
	if err := cfg.ValidateShutdown(budget); err == nil {
		t.Error("1s over boundary should fail validation")
	}
}

func TestValidateShutdown_VerySmallBudget(t *testing.T) {
	// Budget smaller than the 5s margin itself
	cfg := &config.Config{
		Shutdown: config.ShutdownConfig{
			Delay:          0,
			FlushTimeout:   0,
			PersistTimeout: 0,
			ReleaseTimeout: 0,
		},
	}
	// total=0, budget=3s, budget-margin=3-5=-2s. 0 > -2s -> should fail
	if err := cfg.ValidateShutdown(3 * time.Second); err == nil {
		t.Error("zero phases with budget < margin should fail (budget too small)")
	}
}

// ---------------------------------------------------------------------------
// 4. Lifecycle ready endpoint draining header
// ---------------------------------------------------------------------------

func TestHandleLifecycleReady_DrainingHeader(t *testing.T) {
	h := HandleLifecycleReady(LifecycleInfo{
		GetPhase:   func() string { return "drain" },
		IsReady:    func() bool { return true },
		IsDraining: func() bool { return true },
	})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/ready", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Verify 503 status
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("draining should return 503, got %d", rec.Code)
	}

	// Verify X-Lakehouse-Draining header is present and correct
	drainingHeader := rec.Header().Get("X-Lakehouse-Draining")
	if drainingHeader != "true" {
		t.Errorf("X-Lakehouse-Draining header = %q, want %q", drainingHeader, "true")
	}

	// Verify Content-Type is still JSON
	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}

	// Verify body contains correct JSON
	var resp struct {
		Ready    bool   `json:"ready"`
		Phase    string `json:"phase"`
		Draining bool   `json:"draining"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Draining {
		t.Error("body should have draining=true")
	}
	if resp.Phase != "drain" {
		t.Errorf("body phase = %q, want %q", resp.Phase, "drain")
	}
}

func TestHandleLifecycleReady_NotDrainingNoHeader(t *testing.T) {
	h := HandleLifecycleReady(LifecycleInfo{
		GetPhase:   func() string { return "ready" },
		IsReady:    func() bool { return true },
		IsDraining: func() bool { return false },
	})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/ready", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("ready should return 200, got %d", rec.Code)
	}

	// X-Lakehouse-Draining header should NOT be present when not draining
	drainingHeader := rec.Header().Get("X-Lakehouse-Draining")
	if drainingHeader != "" {
		t.Errorf("X-Lakehouse-Draining should be absent when not draining, got %q", drainingHeader)
	}
}

func TestHandleLifecycleReady_NotReadyNotDraining_NoHeader(t *testing.T) {
	h := HandleLifecycleReady(LifecycleInfo{
		GetPhase:   func() string { return "init" },
		IsReady:    func() bool { return false },
		IsDraining: func() bool { return false },
	})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/ready", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("not-ready should return 503, got %d", rec.Code)
	}

	// Should be 503 but WITHOUT the draining header
	drainingHeader := rec.Header().Get("X-Lakehouse-Draining")
	if drainingHeader != "" {
		t.Errorf("X-Lakehouse-Draining should be absent when not draining, got %q", drainingHeader)
	}
}

// ---------------------------------------------------------------------------
// 5. Staleness detector with concurrent access
// ---------------------------------------------------------------------------

func TestStalenessDetector_ConcurrentInfoReads(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Set to 2 hours ago so it will be stale
	staleTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(manifestPath, staleTime, staleTime); err != nil {
		t.Fatal(err)
	}

	d := NewStalenessDetector(testStartupConfig(), manifestPath)

	// Run all mutations first (single-threaded), then verify concurrent reads.
	d.Check()
	d.ReconcileWAL([]WALEntry{{TimestampNs: 1000}}, &mockManifestChecker{covered: map[int64]bool{}})
	d.InvalidateCache(100)

	var wg sync.WaitGroup
	const goroutines = 50

	// Concurrent read-only Info() calls after mutation is complete.
	// No writes happen during this phase, so no data race.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				info := d.Info()
				if info == nil {
					t.Error("Info() returned nil")
					return
				}
				if !info.StaleDetected {
					t.Error("expected stale_detected=true")
					return
				}
				if !info.WALReconciled {
					t.Error("expected wal_reconciled=true")
					return
				}
				if !info.CacheRevalidated {
					t.Error("expected cache_revalidated=true")
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestStalenessDetector_SequentialReconcileThenInfo(t *testing.T) {
	// Verify that after ReconcileWAL completes, Info reflects the state.
	// NOTE: StalenessDetector fields are not protected by mutex/atomic,
	// so concurrent write+read would race. This test verifies sequential
	// correctness which is the current usage pattern.
	d := NewStalenessDetector(testStartupConfig(), "")
	d.staleDetected = true

	checker := &mockManifestChecker{covered: map[int64]bool{
		1000: true,
		2000: false,
		3000: false,
	}}

	entries := []WALEntry{
		{TimestampNs: 1000},
		{TimestampNs: 2000},
		{TimestampNs: 3000},
	}

	count := d.ReconcileWAL(entries, checker)
	if count != 2 {
		t.Errorf("needs reflush = %d, want 2", count)
	}

	info := d.Info()
	if !info.WALReconciled {
		t.Error("Info should reflect WAL reconciled after ReconcileWAL")
	}
}

func TestStalenessDetector_SequentialInvalidateThenInfo(t *testing.T) {
	// Verify Info reflects cache revalidation after InvalidateCache.
	d := NewStalenessDetector(testStartupConfig(), "")
	d.staleDetected = true

	d.InvalidateCache(100)

	info := d.Info()
	if !info.CacheRevalidated {
		t.Error("Info should reflect cache revalidated after InvalidateCache")
	}
}

func TestStalenessDetector_InfoSnapshotIsolation(t *testing.T) {
	// Verify that Info() returns a snapshot (copy), not a pointer to
	// mutable internal state.
	d := NewStalenessDetector(testStartupConfig(), "")

	info1 := d.Info()
	if info1.StaleDetected {
		t.Error("initial state should not be stale")
	}

	// Mutate internal state
	d.staleDetected = true

	// First snapshot should still show old value (it's a copy)
	if info1.StaleDetected {
		t.Error("snapshot should be isolated from subsequent mutations")
	}

	// New snapshot should show new value
	info2 := d.Info()
	if !info2.StaleDetected {
		t.Error("new snapshot should reflect mutation")
	}
}

// ---------------------------------------------------------------------------
// 6. Shutdown metrics — verify metrics are set/reset correctly
// ---------------------------------------------------------------------------

func TestShutdownMetrics_SuccessGaugeSet(t *testing.T) {
	hooks := ShutdownHooks{
		OnDrain:   func(ctx context.Context) error { return nil },
		OnFlush:   func(ctx context.Context) (int64, error) { return 0, nil },
		OnPersist: func(ctx context.Context) error { return nil },
		OnRelease: func(ctx context.Context) error { return nil },
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)

	// Before shutdown, reset gauge to known state
	metrics.ShutdownSuccess.Set(0)

	if err := orch.Execute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After successful shutdown, gauge should be 1
	if got := metrics.ShutdownSuccess.Get(); got != 1 {
		t.Errorf("ShutdownSuccess = %d, want 1 after successful shutdown", got)
	}
}

func TestShutdownMetrics_FailureGaugeStaysZero(t *testing.T) {
	hooks := ShutdownHooks{
		OnFlush: func(ctx context.Context) (int64, error) {
			return 0, context.DeadlineExceeded
		},
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	metrics.ShutdownSuccess.Set(0)

	_ = orch.Execute(context.Background())

	// After failed shutdown, gauge should remain 0
	if got := metrics.ShutdownSuccess.Get(); got != 0 {
		t.Errorf("ShutdownSuccess = %d, want 0 after failed shutdown", got)
	}
}

func TestShutdownMetrics_ResetBetweenCycles(t *testing.T) {
	// Simulate two shutdown cycles (e.g., test restart scenario).
	// First cycle succeeds, second fails — gauge should reflect the latest.

	successHooks := ShutdownHooks{
		OnDrain: func(ctx context.Context) error { return nil },
	}
	failHooks := ShutdownHooks{
		OnFlush: func(ctx context.Context) (int64, error) {
			return 0, context.DeadlineExceeded
		},
	}

	// Cycle 1: success
	orch1 := NewShutdownOrchestrator(testShutdownConfig(), successHooks)
	metrics.ShutdownSuccess.Set(0)
	_ = orch1.Execute(context.Background())
	if got := metrics.ShutdownSuccess.Get(); got != 1 {
		t.Fatalf("after cycle 1, ShutdownSuccess = %d, want 1", got)
	}

	// Cycle 2: failure
	orch2 := NewShutdownOrchestrator(testShutdownConfig(), failHooks)
	_ = orch2.Execute(context.Background())
	// Execute sets ShutdownSuccess to 0 at the start, then only sets to 1 on success.
	if got := metrics.ShutdownSuccess.Get(); got != 0 {
		t.Errorf("after cycle 2 (failure), ShutdownSuccess = %d, want 0", got)
	}
}

func TestShutdownMetrics_FlushRowsAccumulate(t *testing.T) {
	baseRows := metrics.ShutdownFlushRows.Get()

	hooks := ShutdownHooks{
		OnFlush: func(ctx context.Context) (int64, error) { return 500, nil },
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	_ = orch.Execute(context.Background())

	got := metrics.ShutdownFlushRows.Get()
	if got < baseRows+500 {
		t.Errorf("ShutdownFlushRows = %d, want >= %d", got, baseRows+500)
	}
}

func TestShutdownMetrics_PhaseActiveGaugeResets(t *testing.T) {
	hooks := ShutdownHooks{
		OnDrain: func(ctx context.Context) error {
			// During drain, the "drain" phase should be active
			if got := metrics.ShutdownPhaseActive.Get("drain"); got != 1 {
				t.Errorf("during drain, phase_active[drain] = %d, want 1", got)
			}
			return nil
		},
		OnFlush: func(ctx context.Context) (int64, error) {
			// During flush, drain should be inactive and flush should be active
			if got := metrics.ShutdownPhaseActive.Get("drain"); got != 0 {
				t.Errorf("during flush, phase_active[drain] = %d, want 0", got)
			}
			if got := metrics.ShutdownPhaseActive.Get("flush"); got != 1 {
				t.Errorf("during flush, phase_active[flush] = %d, want 1", got)
			}
			return 0, nil
		},
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	if err := orch.Execute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After shutdown, all phases should be inactive
	for _, phase := range []string{"drain", "flush", "persist", "release"} {
		if got := metrics.ShutdownPhaseActive.Get(phase); got != 0 {
			t.Errorf("after shutdown, phase_active[%s] = %d, want 0", phase, got)
		}
	}
}

func TestShutdownMetrics_ZeroFlushRowsNoIncrement(t *testing.T) {
	baseRows := metrics.ShutdownFlushRows.Get()

	hooks := ShutdownHooks{
		OnFlush: func(ctx context.Context) (int64, error) { return 0, nil },
	}

	orch := NewShutdownOrchestrator(testShutdownConfig(), hooks)
	_ = orch.Execute(context.Background())

	got := metrics.ShutdownFlushRows.Get()
	if got != baseRows {
		t.Errorf("ShutdownFlushRows changed from %d to %d with zero rows", baseRows, got)
	}
}

// ---------------------------------------------------------------------------
// Additional edge cases: shutdown with zero-timeout config
// ---------------------------------------------------------------------------

func TestShutdownWithZeroTimeouts_RunsPhasesWithoutTimeout(t *testing.T) {
	cfg := config.ShutdownConfig{
		Delay:          0,
		FlushTimeout:   0,
		PersistTimeout: 0,
		ReleaseTimeout: 0,
	}

	var order []string
	hooks := ShutdownHooks{
		OnDrain:   func(ctx context.Context) error { order = append(order, "drain"); return nil },
		OnFlush:   func(ctx context.Context) (int64, error) { order = append(order, "flush"); return 10, nil },
		OnPersist: func(ctx context.Context) error { order = append(order, "persist"); return nil },
		OnRelease: func(ctx context.Context) error { order = append(order, "release"); return nil },
	}

	orch := NewShutdownOrchestrator(cfg, hooks)
	if err := orch.Execute(context.Background()); err != nil {
		t.Fatalf("zero-timeout shutdown should succeed, got: %v", err)
	}

	expected := []string{"drain", "flush", "persist", "release"}
	if len(order) != len(expected) {
		t.Fatalf("phases = %v, want %v", order, expected)
	}
	for i, want := range expected {
		if order[i] != want {
			t.Errorf("phase[%d] = %s, want %s", i, order[i], want)
		}
	}
}
