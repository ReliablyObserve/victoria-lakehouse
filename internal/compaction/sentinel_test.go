package compaction

import (
	"context"
	"sync"
	"testing"
	"time"
)

type mockPool struct {
	mu       sync.Mutex
	uploaded map[string][]byte
}

func newMockPool() *mockPool {
	return &mockPool{uploaded: make(map[string][]byte)}
}

func (m *mockPool) Upload(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploaded[key] = append([]byte(nil), data...)
	return nil
}

func (m *mockPool) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.uploaded[key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), d...), nil
}

func (m *mockPool) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.uploaded, key)
	return nil
}

func TestSentinel_AcquireAndRelease(t *testing.T) {
	pool := newMockPool()
	s := NewSentinel(pool, time.Hour)
	ctx := context.Background()

	// Acquire should succeed
	ok, err := s.Acquire(ctx, "prefix/", "dt=2026-05-04", "worker-1")
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if !ok {
		t.Fatal("expected Acquire to succeed")
	}

	// IsLocked should return true
	locked, err := s.IsLocked(ctx, "prefix/", "dt=2026-05-04")
	if err != nil {
		t.Fatalf("IsLocked error: %v", err)
	}
	if !locked {
		t.Fatal("expected IsLocked=true after acquire")
	}

	// Second acquire should fail (already locked)
	ok2, err := s.Acquire(ctx, "prefix/", "dt=2026-05-04", "worker-2")
	if err != nil {
		t.Fatalf("second Acquire error: %v", err)
	}
	if ok2 {
		t.Fatal("expected second Acquire to fail (already locked)")
	}

	// Release
	if err := s.Release(ctx, "prefix/", "dt=2026-05-04"); err != nil {
		t.Fatalf("Release error: %v", err)
	}

	// IsLocked should return false after release
	locked, err = s.IsLocked(ctx, "prefix/", "dt=2026-05-04")
	if err != nil {
		t.Fatalf("IsLocked after release error: %v", err)
	}
	if locked {
		t.Fatal("expected IsLocked=false after release")
	}
}

func TestSentinel_StaleIsUnlocked(t *testing.T) {
	pool := newMockPool()
	// Very short stale timeout: 1ms
	s := NewSentinel(pool, time.Millisecond)
	ctx := context.Background()

	ok, err := s.Acquire(ctx, "prefix/", "dt=2026-05-04", "worker-1")
	if err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if !ok {
		t.Fatal("expected Acquire to succeed")
	}

	// Wait for sentinel to go stale
	time.Sleep(10 * time.Millisecond)

	locked, err := s.IsLocked(ctx, "prefix/", "dt=2026-05-04")
	if err != nil {
		t.Fatalf("IsLocked error: %v", err)
	}
	if locked {
		t.Fatal("expected IsLocked=false for stale sentinel")
	}
}
