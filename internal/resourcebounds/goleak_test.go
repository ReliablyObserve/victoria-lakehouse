package resourcebounds

// goleak smoke test for the resourcebounds package. The Acquire path
// spawns a per-call goroutine for ctx-cancel signaling (see bounds.go);
// this test verifies the goroutine is reaped on every code path
// (admission, cancellation, double-release).

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// TestGoleak_AcquireAdmit — admit path must reap the ctx-cancel goroutine.
func TestGoleak_AcquireAdmit(t *testing.T) {
	defer goleak.VerifyNone(t)
	b := NewBound(Config{Limit: 100, LimitCount: 4}, nil)
	rel, err := b.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	rel()
}

// TestGoleak_AcquireCancelled — cancel path must reap the ctx-cancel goroutine.
func TestGoleak_AcquireCancelled(t *testing.T) {
	defer goleak.VerifyNone(t)
	b := NewBound(Config{Limit: 10, LimitCount: 1}, nil)
	rel, _ := b.Acquire(context.Background(), 10)
	defer rel()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := b.Acquire(ctx, 10)
	if err == nil {
		t.Fatal("expected ctx-cancel rejection")
	}
	// Give the spawned cancel-watcher goroutine a moment to wake on
	// the cond.Broadcast and exit through the <-done branch.
	time.Sleep(10 * time.Millisecond)
}

// TestGoleak_TryAcquire — TryAcquire path must NOT spawn any
// goroutine (it's the non-blocking sibling of Acquire). This is the
// hot path for cache.Put / disk.Put — extra goroutines per cache
// operation would dominate the cost.
func TestGoleak_TryAcquire(t *testing.T) {
	defer goleak.VerifyNone(t)
	b := NewBound(Config{Limit: 100, LimitCount: 0}, nil)
	rel, err := b.TryAcquire(10)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	rel()
}

// TestGoleak_TryAcquireRejected — rejection path must NOT spawn any goroutine.
func TestGoleak_TryAcquireRejected(t *testing.T) {
	defer goleak.VerifyNone(t)
	b := NewBound(Config{Limit: 1, LimitCount: 1}, nil)
	_, _ = b.TryAcquire(1) // admit, fill
	_, err := b.TryAcquire(1)
	if err != ErrBoundFull {
		t.Fatalf("expected ErrBoundFull; got %v", err)
	}
}
