package election

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// mockLeakStore is a minimal in-memory S3Store for leak tests.
type mockLeakStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMockLeakStore() *mockLeakStore {
	return &mockLeakStore{data: make(map[string][]byte)}
}

func (m *mockLeakStore) Upload(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.data[key] = cp
	return nil
}

func (m *mockLeakStore) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (m *mockLeakStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func forceGC() {
	runtime.GC()
	runtime.GC()
}

func heapInUse() uint64 {
	var m runtime.MemStats
	forceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// --- S3Elector goroutine leak tests ---

func TestS3Elector_NoGoroutineLeak_StartStop(t *testing.T) {
	store := newMockLeakStore()

	before := runtime.NumGoroutine()

	for i := 0; i < 20; i++ {
		cfg := S3ElectorConfig{
			LockKey:            "election/leader.json",
			Identity:           fmt.Sprintf("node-%d", i),
			Address:            "127.0.0.1:9000",
			HeartbeatInterval:  50 * time.Millisecond,
			LockTTL:            500 * time.Millisecond,
			HealthCheckTimeout: 100 * time.Millisecond,
		}
		e := NewS3Elector(store, cfg)

		ctx := context.Background()
		e.Start(ctx)
		time.Sleep(20 * time.Millisecond) // let goroutine start and do one cycle
		e.Stop()
	}

	time.Sleep(200 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after 20 S3Elector Start/Stop cycles: before=%d after=%d", before, after)
	}
}

func TestS3Elector_NoGoroutineLeak_MultipleHeartbeats(t *testing.T) {
	store := newMockLeakStore()

	before := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		cfg := S3ElectorConfig{
			LockKey:            fmt.Sprintf("election/leader-%d.json", i),
			Identity:           fmt.Sprintf("node-%d", i),
			Address:            fmt.Sprintf("127.0.0.1:%d", 9000+i),
			HeartbeatInterval:  20 * time.Millisecond,
			LockTTL:            1 * time.Second,
			HealthCheckTimeout: 100 * time.Millisecond,
		}
		e := NewS3Elector(store, cfg)

		ctx := context.Background()
		e.Start(ctx)
		// Let it run several heartbeat cycles.
		time.Sleep(100 * time.Millisecond)
		e.Stop()
	}

	time.Sleep(200 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after multi-heartbeat cycles: before=%d after=%d", before, after)
	}
}

func TestS3Elector_NoGoroutineLeak_ContextCancel(t *testing.T) {
	store := newMockLeakStore()

	before := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		cfg := S3ElectorConfig{
			LockKey:            "election/leader.json",
			Identity:           fmt.Sprintf("node-%d", i),
			HeartbeatInterval:  20 * time.Millisecond,
			LockTTL:            500 * time.Millisecond,
			HealthCheckTimeout: 100 * time.Millisecond,
		}
		e := NewS3Elector(store, cfg)

		ctx, cancel := context.WithCancel(context.Background())
		e.Start(ctx)
		time.Sleep(30 * time.Millisecond)
		// Cancel context instead of calling Stop.
		cancel()
		// Still call Stop to wait for goroutine cleanup.
		e.Stop()
	}

	time.Sleep(200 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after context-cancel cycles: before=%d after=%d", before, after)
	}
}

// --- NoopElector goroutine leak test ---

func TestNoopElector_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 100; i++ {
		e := NewNoopElector()
		e.Start(context.Background())
		_ = e.IsLeader()
		e.Stop()
	}

	runtime.GC()
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

// --- AutoElector goroutine leak test ---

func TestAutoElector_NoGoroutineLeak_NoopMode(t *testing.T) {
	before := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		a := NewAutoElector(AutoElectorConfig{Mode: "none"})
		a.Start(context.Background())
		_ = a.IsLeader()
		a.Stop()
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestAutoElector_NoGoroutineLeak_S3Mode(t *testing.T) {
	store := newMockLeakStore()

	before := runtime.NumGoroutine()

	for i := 0; i < 10; i++ {
		a := NewAutoElector(AutoElectorConfig{
			Mode:    "s3",
			S3Store: store,
			S3Config: S3ElectorConfig{
				LockKey:            "election/leader.json",
				Identity:           fmt.Sprintf("auto-node-%d", i),
				HeartbeatInterval:  30 * time.Millisecond,
				LockTTL:            500 * time.Millisecond,
				HealthCheckTimeout: 100 * time.Millisecond,
			},
		})

		a.Start(context.Background())
		time.Sleep(50 * time.Millisecond)
		a.Stop()
	}

	time.Sleep(200 * time.Millisecond)
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak after S3 AutoElector cycles: before=%d after=%d", before, after)
	}
}

// --- S3Elector memory leak test ---

func TestS3Elector_NoMemoryLeak_HeartbeatCycles(t *testing.T) {
	store := newMockLeakStore()
	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-mem",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  5 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 100 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx := context.Background()
	e.Start(ctx)
	// Let many heartbeat cycles run.
	time.Sleep(500 * time.Millisecond)
	e.Stop()
	forceGC()

	before := heapInUse()

	// Do it again with more heartbeats.
	e2 := NewS3Elector(store, cfg)
	e2.Start(ctx)
	time.Sleep(1 * time.Second)
	e2.Stop()
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes after heartbeat cycles (max %d)", growth, maxGrowth)
	}
}
