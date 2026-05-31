package resourcebounds

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeMetrics is a Metrics implementation that records every event
// in atomic counters so tests can assert observability behaviour
// alongside the gate behaviour.
type fakeMetrics struct {
	acquired atomic.Int64
	rejected atomic.Int64
	outBytes atomic.Int64
	outCount atomic.Int64
}

func (m *fakeMetrics) AcquiredAdd(n int64)         { m.acquired.Add(n) }
func (m *fakeMetrics) RejectedAdd(n int64)         { m.rejected.Add(n) }
func (m *fakeMetrics) OutstandingBytesSet(v int64) { m.outBytes.Store(v) }
func (m *fakeMetrics) OutstandingCountSet(v int64) { m.outCount.Store(v) }

func TestNewBound_DefaultPolicy(t *testing.T) {
	b := NewBound(Config{Request: 1, Limit: 10, LimitCount: 2}, nil)
	if b == nil {
		t.Fatal("NewBound returned nil")
	}
	cfg := b.Config()
	if cfg.Request != 1 || cfg.Limit != 10 || cfg.LimitCount != 2 {
		t.Errorf("Config not preserved: %+v", cfg)
	}
	if cfg.Policy != Fixed {
		t.Errorf("default Policy = %v, want Fixed", cfg.Policy)
	}
}

func TestScalingPolicy_String(t *testing.T) {
	tests := []struct {
		p    ScalingPolicy
		want string
	}{
		{Fixed, "fixed"},
		{LinearGrowth, "linear"},
		{ExponentialBackoff, "expbackoff"},
		{ScalingPolicy(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.p.String(); got != tt.want {
			t.Errorf("%v.String() = %q, want %q", tt.p, got, tt.want)
		}
	}
}

func TestAcquire_WithinLimit(t *testing.T) {
	m := &fakeMetrics{}
	b := NewBound(Config{Limit: 100, LimitCount: 4}, m)

	rel, err := b.Acquire(context.Background(), 30)
	if err != nil {
		t.Fatalf("Acquire returned err: %v", err)
	}
	defer rel()

	out, cnt := b.Outstanding()
	if out != 30 || cnt != 1 {
		t.Errorf("Outstanding = (%d, %d), want (30, 1)", out, cnt)
	}
	if m.acquired.Load() != 1 {
		t.Errorf("acquired metric = %d, want 1", m.acquired.Load())
	}
	if m.outBytes.Load() != 30 {
		t.Errorf("outBytes metric = %d, want 30", m.outBytes.Load())
	}
}

func TestAcquire_ReleaseUpdatesOutstanding(t *testing.T) {
	m := &fakeMetrics{}
	b := NewBound(Config{Limit: 100, LimitCount: 4}, m)

	rel, err := b.Acquire(context.Background(), 30)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	rel()

	out, cnt := b.Outstanding()
	if out != 0 || cnt != 0 {
		t.Errorf("after release Outstanding = (%d, %d), want (0, 0)", out, cnt)
	}
	if m.outBytes.Load() != 0 {
		t.Errorf("outBytes metric after release = %d, want 0", m.outBytes.Load())
	}
}

func TestAcquire_DoubleReleaseIsSafe(t *testing.T) {
	b := NewBound(Config{Limit: 100, LimitCount: 4}, nil)
	rel, _ := b.Acquire(context.Background(), 30)
	rel()
	rel() // must not panic, must not double-decrement
	out, cnt := b.Outstanding()
	if out != 0 || cnt != 0 {
		t.Errorf("after double release Outstanding = (%d, %d), want (0, 0)", out, cnt)
	}
}

func TestAcquire_CountCapBlocksThenAdmits(t *testing.T) {
	b := NewBound(Config{Limit: 1000, LimitCount: 2}, nil)

	// Saturate the count cap.
	rel1, _ := b.Acquire(context.Background(), 10)
	rel2, _ := b.Acquire(context.Background(), 10)

	// Third must block until one releases.
	admitted := make(chan struct{})
	go func() {
		rel3, err := b.Acquire(context.Background(), 10)
		if err != nil {
			t.Errorf("third Acquire returned err: %v", err)
		}
		close(admitted)
		rel3()
	}()

	select {
	case <-admitted:
		t.Fatal("third Acquire admitted before any release")
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	rel1()

	select {
	case <-admitted:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Fatal("third Acquire never admitted after release")
	}
	rel2()
}

func TestAcquire_OutlierAdmittedAlone(t *testing.T) {
	// Outstanding=0, n > Limit: bound admits the outlier and
	// internally clamps n to Limit so outstanding never exceeds.
	b := NewBound(Config{Limit: 100, LimitCount: 4}, nil)
	rel, err := b.Acquire(context.Background(), 1000)
	if err != nil {
		t.Fatalf("Acquire outlier: %v", err)
	}
	defer rel()
	out, cnt := b.Outstanding()
	if out != 100 || cnt != 1 {
		t.Errorf("outlier Outstanding = (%d, %d), want (100, 1)", out, cnt)
	}
}

func TestAcquire_ContextCancelledWhileWaiting(t *testing.T) {
	m := &fakeMetrics{}
	b := NewBound(Config{Limit: 100, LimitCount: 1}, m)
	rel1, _ := b.Acquire(context.Background(), 10)
	defer rel1()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err := b.Acquire(ctx, 10)
	if err == nil {
		t.Fatal("Acquire returned nil err on cancelled ctx")
	}
	if err != context.Canceled {
		t.Errorf("Acquire err = %v, want context.Canceled", err)
	}
	if m.rejected.Load() != 1 {
		t.Errorf("rejected metric = %d, want 1", m.rejected.Load())
	}
}

func TestAcquire_NoCountCapByteOnly(t *testing.T) {
	b := NewBound(Config{Limit: 100, LimitCount: 0}, nil)
	rels := make([]func(), 5)
	for i := range rels {
		r, err := b.Acquire(context.Background(), 10)
		if err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
		rels[i] = r
	}
	out, cnt := b.Outstanding()
	if out != 50 || cnt != 5 {
		t.Errorf("Outstanding = (%d, %d), want (50, 5)", out, cnt)
	}
	for _, r := range rels {
		r()
	}
}

func TestAcquire_NoByteCapCountOnly(t *testing.T) {
	b := NewBound(Config{Limit: 0, LimitCount: 3}, nil)
	rels := make([]func(), 3)
	for i := range rels {
		r, err := b.Acquire(context.Background(), 1<<30)
		if err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
		rels[i] = r
	}
	_, cnt := b.Outstanding()
	if cnt != 3 {
		t.Errorf("Outstanding count = %d, want 3", cnt)
	}
	for _, r := range rels {
		r()
	}
}

func TestAcquire_ConcurrentNoPanic(t *testing.T) {
	b := NewBound(Config{Limit: 1000, LimitCount: 4}, nil)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := b.Acquire(context.Background(), 100)
			if err != nil {
				t.Errorf("concurrent Acquire: %v", err)
				return
			}
			time.Sleep(time.Microsecond)
			rel()
		}()
	}
	wg.Wait()
	out, cnt := b.Outstanding()
	if out != 0 || cnt != 0 {
		t.Errorf("after concurrent acquires Outstanding = (%d, %d), want (0, 0)", out, cnt)
	}
}

func TestStats_LifetimeCounters(t *testing.T) {
	b := NewBound(Config{Limit: 100, LimitCount: 1}, nil)
	rel1, _ := b.Acquire(context.Background(), 10)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_, _ = b.Acquire(ctx, 10) // rejected via cancellation

	acquired, rejected, _, _ := b.Stats()
	if acquired != 1 {
		t.Errorf("acquired = %d, want 1", acquired)
	}
	if rejected != 1 {
		t.Errorf("rejected = %d, want 1", rejected)
	}
	rel1()
}

func TestAcquire_ZeroOrNegativeNormalized(t *testing.T) {
	b := NewBound(Config{Limit: 100, LimitCount: 4}, nil)
	rel, err := b.Acquire(context.Background(), 0)
	if err != nil {
		t.Fatalf("Acquire(0): %v", err)
	}
	out, cnt := b.Outstanding()
	if out != 1 || cnt != 1 {
		t.Errorf("Acquire(0) Outstanding = (%d, %d), want (1, 1)", out, cnt)
	}
	rel()

	rel2, err := b.Acquire(context.Background(), -5)
	if err != nil {
		t.Fatalf("Acquire(-5): %v", err)
	}
	out, cnt = b.Outstanding()
	if out != 1 || cnt != 1 {
		t.Errorf("Acquire(-5) Outstanding = (%d, %d), want (1, 1)", out, cnt)
	}
	rel2()
}

func TestAcquire_PreservesLegacyFileBudgetSemantics(t *testing.T) {
	// Critical: small file + outlier file interleaved must match the
	// legacy fileBudget behaviour exactly. Outlier admitted alone
	// when pool empty, small files queue when outlier in flight.
	b := NewBound(Config{Limit: 100, LimitCount: 4}, nil)

	// Outlier admitted alone.
	relOutlier, err := b.Acquire(context.Background(), 500)
	if err != nil {
		t.Fatalf("outlier: %v", err)
	}
	out, _ := b.Outstanding()
	if out != 100 {
		t.Errorf("outlier clamped Outstanding = %d, want 100", out)
	}

	// Small acquire queues because byte cap exhausted (outlier is at
	// the ceiling) AND outCount != 0 (so the outlier-admit fast
	// path no longer applies).
	queued := make(chan error)
	go func() {
		_, e := b.Acquire(context.Background(), 10)
		queued <- e
	}()
	select {
	case <-queued:
		t.Fatal("small Acquire admitted while outlier in flight")
	case <-time.After(50 * time.Millisecond):
		// expected — queued behind outlier
	}

	relOutlier()
	select {
	case e := <-queued:
		if e != nil {
			t.Errorf("queued Acquire err: %v", e)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("queued small Acquire never admitted after outlier release")
	}
}

// TestTryAcquire_AdmitAndReject verifies the non-blocking TryAcquire
// API: admits while there's space, rejects with ErrBoundFull when at
// capacity, and increments the rejected_total metric on rejection.
//
// This is the API the cache and disk-cache hot paths use to avoid
// blocking the write — see internal/cache/lru.go SetBound docs.
func TestTryAcquire_AdmitAndReject(t *testing.T) {
	m := &fakeMetrics{}
	b := NewBound(Config{Request: 1, Limit: 100, LimitCount: 2}, m)

	rel1, err := b.TryAcquire(50)
	if err != nil {
		t.Fatalf("first TryAcquire: %v", err)
	}
	rel2, err := b.TryAcquire(40)
	if err != nil {
		t.Fatalf("second TryAcquire: %v", err)
	}
	// LimitCount=2 reached.
	_, err = b.TryAcquire(1)
	if err != ErrBoundFull {
		t.Errorf("third TryAcquire: got %v, want ErrBoundFull", err)
	}
	if m.rejected.Load() != 1 {
		t.Errorf("rejected metric = %d, want 1", m.rejected.Load())
	}

	rel1()
	rel2()
	// After release, TryAcquire admits again.
	rel3, err := b.TryAcquire(50)
	if err != nil {
		t.Errorf("post-release TryAcquire: %v", err)
	}
	rel3()
}

// TestTryAcquire_OutlierAdmission verifies that TryAcquire honours the
// outlier path — a single n > Limit reservation is admitted when the
// pool is empty (count==0), matching Acquire's documented semantics.
func TestTryAcquire_OutlierAdmission(t *testing.T) {
	b := NewBound(Config{Limit: 100, LimitCount: 0}, nil)
	rel, err := b.TryAcquire(1000)
	if err != nil {
		t.Fatalf("outlier TryAcquire: %v", err)
	}
	defer rel()
	_, _, outBytes, outCount := b.Stats()
	if outBytes != 100 {
		t.Errorf("outlier accounting clamps to Limit; outBytes=%d (want 100)", outBytes)
	}
	if outCount != 1 {
		t.Errorf("outlier counted once; outCount=%d (want 1)", outCount)
	}
}

// TestTryAcquire_DoubleReleaseIdempotent confirms double-release is
// safe (parity with Acquire's release contract).
func TestTryAcquire_DoubleReleaseIdempotent(t *testing.T) {
	b := NewBound(Config{Limit: 10, LimitCount: 1}, nil)
	rel, err := b.TryAcquire(1)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	rel()
	rel() // must not panic, must not double-decrement
	_, _, outBytes, outCount := b.Stats()
	if outBytes != 0 || outCount != 0 {
		t.Errorf("double-release corrupted accounting: outBytes=%d outCount=%d", outBytes, outCount)
	}
}
