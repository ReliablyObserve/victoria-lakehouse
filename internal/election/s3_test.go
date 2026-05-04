// internal/election/s3_test.go
package election

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// mockS3Store is an in-memory S3Store for testing.
type mockS3Store struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMockS3Store() *mockS3Store {
	return &mockS3Store{data: make(map[string][]byte)}
}

func (m *mockS3Store) Upload(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	return nil
}

func (m *mockS3Store) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("s3 GetObject %s: NoSuchKey", key)
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (m *mockS3Store) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

// seedLock writes an S3Lock into the mock store at the given key.
func seedLock(t *testing.T, store *mockS3Store, key string, lock S3Lock) {
	t.Helper()
	data, err := json.Marshal(lock)
	if err != nil {
		t.Fatalf("marshal lock: %v", err)
	}
	if err := store.Upload(context.Background(), key, data); err != nil {
		t.Fatalf("seed lock: %v", err)
	}
}

// TestS3Elector_AcquiresLockWhenEmpty verifies that an elector acquires
// leadership when the lock file does not yet exist.
func TestS3Elector_AcquiresLockWhenEmpty(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-1",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  100 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	// Allow time for initial acquisition.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership on empty store")
	}

	e.Stop()
}

// TestS3Elector_DoesNotStealFromAliveHolder verifies that when another node
// holds the lock and is responding to health checks, this elector does NOT
// become leader.
func TestS3Elector_DoesNotStealFromAliveHolder(t *testing.T) {
	// Start a live httptest server that responds 200 to /health.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	// ts.Listener.Addr().String() gives "host:port".
	aliveAddress := ts.Listener.Addr().String()

	store := newMockS3Store()

	// Write an existing lock held by another node with a fresh heartbeat.
	seedLock(t, store, "election/leader.json", S3Lock{
		Holder:    "other-node",
		Address:   aliveAddress,
		Acquired:  time.Now().UTC().Add(-10 * time.Second),
		Heartbeat: time.Now().UTC(), // fresh
	})

	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-2",
		Address:            "127.0.0.1:9001",
		HeartbeatInterval:  100 * time.Millisecond,
		LockTTL:            5 * time.Second,
		HealthCheckTimeout: 500 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	// Wait long enough for at least two heartbeat cycles.
	time.Sleep(350 * time.Millisecond)

	if e.IsLeader() {
		t.Fatal("should NOT steal lock from alive holder")
	}

	e.Stop()
}

// TestS3Elector_TakesOverFromDeadHolder verifies that when the existing lock
// holder's heartbeat has expired and it does not respond to health checks,
// this elector takes over leadership.
func TestS3Elector_TakesOverFromDeadHolder(t *testing.T) {
	store := newMockS3Store()

	// Write a lock with an expired heartbeat and an unreachable address.
	seedLock(t, store, "election/leader.json", S3Lock{
		Holder:    "dead-node",
		Address:   "127.0.0.1:19999", // nothing listening here
		Acquired:  time.Now().UTC().Add(-60 * time.Second),
		Heartbeat: time.Now().UTC().Add(-60 * time.Second), // well past TTL
	})

	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-3",
		Address:            "127.0.0.1:9002",
		HeartbeatInterval:  100 * time.Millisecond,
		LockTTL:            500 * time.Millisecond,
		HealthCheckTimeout: 100 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	// Allow time for takeover (initial tryAcquire + health check timeout).
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !e.IsLeader() {
		t.Fatal("expected to take over from dead holder")
	}

	e.Stop()
}

// TestS3Elector_ImplementsLeader is a compile-time interface check.
func TestS3Elector_ImplementsLeader(t *testing.T) {
	var _ Leader = (*S3Elector)(nil)
}
