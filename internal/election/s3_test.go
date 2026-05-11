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

// TestS3Elector_CorruptedLockOverwrite verifies that a corrupted (non-JSON)
// lock file is overwritten and the elector acquires leadership.
func TestS3Elector_CorruptedLockOverwrite(t *testing.T) {
	store := newMockS3Store()
	// Write invalid JSON as the lock.
	if err := store.Upload(context.Background(), "election/leader.json", []byte("not-json{")); err != nil {
		t.Fatalf("seed corrupted lock: %v", err)
	}

	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-corrupt",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  100 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership after corrupted lock")
	}

	e.Stop()
}

// TestS3Elector_ReleasesOnStop verifies that Stop releases the lock and
// marks the elector as non-leader.
func TestS3Elector_ReleasesOnStop(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-release",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  100 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	// Wait for acquisition.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership")
	}

	e.Stop()

	if e.IsLeader() {
		t.Error("expected not to be leader after Stop")
	}

	// Verify lock file was deleted.
	store.mu.Lock()
	_, exists := store.data["election/leader.json"]
	store.mu.Unlock()
	if exists {
		t.Error("expected lock file to be deleted after Stop")
	}
}

// TestS3Elector_HeartbeatRenewsLock verifies that the heartbeat updates the
// lock's Heartbeat timestamp while the elector holds leadership.
func TestS3Elector_HeartbeatRenewsLock(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-hb",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            5 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	// Wait for initial acquisition.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership")
	}

	// Read initial heartbeat.
	data1, err := store.Download(context.Background(), "election/leader.json")
	if err != nil {
		t.Fatalf("download lock: %v", err)
	}
	var lock1 S3Lock
	if err := json.Unmarshal(data1, &lock1); err != nil {
		t.Fatalf("unmarshal lock: %v", err)
	}

	// Wait for at least one heartbeat cycle.
	time.Sleep(150 * time.Millisecond)

	// Read updated heartbeat.
	data2, err := store.Download(context.Background(), "election/leader.json")
	if err != nil {
		t.Fatalf("download lock: %v", err)
	}
	var lock2 S3Lock
	if err := json.Unmarshal(data2, &lock2); err != nil {
		t.Fatalf("unmarshal lock: %v", err)
	}

	if !lock2.Heartbeat.After(lock1.Heartbeat) {
		t.Errorf("expected heartbeat to advance; first=%v second=%v", lock1.Heartbeat, lock2.Heartbeat)
	}
	if lock2.Holder != "node-hb" {
		t.Errorf("holder = %q, want node-hb", lock2.Holder)
	}

	e.Stop()
}

// TestS3Elector_LockContention verifies that two electors compete correctly:
// the first acquires leadership and the second does not, then when the first
// stops, the second takes over.
func TestS3Elector_LockContention(t *testing.T) {
	store := newMockS3Store()

	cfg1 := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-a",
		Address:            "127.0.0.1:9001",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            300 * time.Millisecond,
		HealthCheckTimeout: 100 * time.Millisecond,
	}

	cfg2 := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-b",
		Address:            "127.0.0.1:9002",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            300 * time.Millisecond,
		HealthCheckTimeout: 100 * time.Millisecond,
	}

	// Start a health server for node-a.
	tsA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer tsA.Close()
	cfg1.Address = tsA.Listener.Addr().String()

	e1 := NewS3Elector(store, cfg1)
	e2 := NewS3Elector(store, cfg2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start node-a first.
	e1.Start(ctx)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e1.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e1.IsLeader() {
		t.Fatal("node-a should be leader")
	}

	// Start node-b — should not steal from alive node-a.
	e2.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	if e2.IsLeader() {
		t.Fatal("node-b should NOT be leader while node-a is alive")
	}

	// Stop node-a (releases lock and stops health server).
	e1.Stop()
	tsA.Close()

	// Wait for node-b to detect dead holder and take over.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e2.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !e2.IsLeader() {
		t.Fatal("node-b should have taken over after node-a stopped")
	}

	e2.Stop()
}

// TestS3Elector_DefaultConfig verifies that zero-value durations get
// sensible defaults applied.
func TestS3Elector_DefaultConfig(t *testing.T) {
	store := newMockS3Store()
	e := NewS3Elector(store, S3ElectorConfig{
		LockKey:  "test/lock.json",
		Identity: "n1",
	})

	if e.cfg.HeartbeatInterval != 5*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 5s", e.cfg.HeartbeatInterval)
	}
	if e.cfg.LockTTL != 30*time.Second {
		t.Errorf("LockTTL = %v, want 30s", e.cfg.LockTTL)
	}
	if e.cfg.HealthCheckTimeout != 3*time.Second {
		t.Errorf("HealthCheckTimeout = %v, want 3s", e.cfg.HealthCheckTimeout)
	}
}

// TestS3Elector_ContextCancellation verifies that cancelling the context
// causes the elector to release leadership and exit.
func TestS3Elector_ContextCancellation(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-ctx",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	e.Start(ctx)

	// Wait for acquisition.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership")
	}

	// Cancel context — should release.
	cancel()

	// Wait for the goroutine to finish via Stop (which waits on wg).
	e.Stop()

	if e.IsLeader() {
		t.Error("expected not to be leader after context cancellation")
	}
}

// TestS3Elector_HeartbeatExpiredNoHTTP verifies that a holder whose heartbeat
// expired and has no address is considered dead (no HTTP check needed).
func TestS3Elector_HeartbeatExpiredNoHTTP(t *testing.T) {
	store := newMockS3Store()

	// Write a lock with an expired heartbeat and empty address.
	seedLock(t, store, "election/leader.json", S3Lock{
		Holder:    "dead-node-no-addr",
		Address:   "", // no address
		Acquired:  time.Now().UTC().Add(-120 * time.Second),
		Heartbeat: time.Now().UTC().Add(-120 * time.Second),
	})

	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "takeover-node",
		Address:            "127.0.0.1:9099",
		HeartbeatInterval:  100 * time.Millisecond,
		LockTTL:            500 * time.Millisecond,
		HealthCheckTimeout: 100 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !e.IsLeader() {
		t.Fatal("expected to take over from holder with expired heartbeat and no address")
	}

	e.Stop()
}

// failingS3Store simulates S3 upload failures.
type failingS3Store struct {
	*mockS3Store
	uploadFail bool
}

func (f *failingS3Store) Upload(ctx context.Context, key string, data []byte) error {
	if f.uploadFail {
		return fmt.Errorf("simulated upload failure")
	}
	return f.mockS3Store.Upload(ctx, key, data)
}

// TestS3Elector_UploadFailureDoesNotBecomeLead verifies that if the lock
// write fails, the elector does not claim leadership.
func TestS3Elector_UploadFailureDoesNotBecomeLead(t *testing.T) {
	store := &failingS3Store{
		mockS3Store: newMockS3Store(),
		uploadFail:  true,
	}

	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-fail",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	if e.IsLeader() {
		t.Error("should not be leader when upload fails")
	}

	e.Stop()
}
