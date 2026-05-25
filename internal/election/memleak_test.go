package election

// forceGC, heapInUse, and mockLeakStore are defined in leak_test.go.

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// TestMemLeak_S3Elector_HeartbeatCycles verifies that a running S3Elector
// does not accumulate heap allocations across many heartbeat cycles.
func TestMemLeak_S3Elector_HeartbeatCycles(t *testing.T) {
	store := newMockLeakStore()
	cfg := S3ElectorConfig{
		LockKey:            "election/leader.json",
		Identity:           "node-memleak",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  5 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 100 * time.Millisecond,
	}

	// Warm up — let heartbeat run for a while
	e := NewS3Elector(store, cfg)
	e.Start(context.Background())
	time.Sleep(200 * time.Millisecond)
	e.Stop()
	forceGC()

	before := heapInUse()

	// Second run with more heartbeat cycles
	e2 := NewS3Elector(store, cfg)
	e2.Start(context.Background())
	time.Sleep(500 * time.Millisecond)
	e2.Stop()
	forceGC()

	after := heapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("S3Elector heartbeat memory grew by %d bytes (max allowed %d)", growth, maxAllowed)
	}
}

// TestMemLeak_S3Elector_StateTracking verifies that S3Elector IsLeader
// and lock-file operations do not retain memory across Start/Stop cycles.
func TestMemLeak_S3Elector_StateTracking(t *testing.T) {
	store := newMockLeakStore()

	// Warm up cycles
	for i := 0; i < 5; i++ {
		cfg := S3ElectorConfig{
			LockKey:            "election/leader.json",
			Identity:           fmt.Sprintf("node-%d", i),
			HeartbeatInterval:  20 * time.Millisecond,
			LockTTL:            500 * time.Millisecond,
			HealthCheckTimeout: 100 * time.Millisecond,
		}
		e := NewS3Elector(store, cfg)
		e.Start(context.Background())
		_ = e.IsLeader()
		time.Sleep(40 * time.Millisecond)
		e.Stop()
	}
	forceGC()

	before := heapInUse()

	const iterations = 20
	for i := 0; i < iterations; i++ {
		cfg := S3ElectorConfig{
			LockKey:            "election/leader.json",
			Identity:           fmt.Sprintf("node-%d", i),
			HeartbeatInterval:  20 * time.Millisecond,
			LockTTL:            500 * time.Millisecond,
			HealthCheckTimeout: 100 * time.Millisecond,
		}
		e := NewS3Elector(store, cfg)
		e.Start(context.Background())
		// Check IsLeader several times per cycle
		for j := 0; j < 5; j++ {
			_ = e.IsLeader()
		}
		time.Sleep(40 * time.Millisecond)
		e.Stop()
	}
	forceGC()

	after := heapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("S3Elector state tracking memory grew by %d bytes over %d Start/Stop cycles (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_AutoElector_NoopCycles verifies that AutoElector in noop mode
// does not retain allocations across many Start/IsLeader/Stop cycles.
func TestMemLeak_AutoElector_NoopCycles(t *testing.T) {
	// Warm up
	for i := 0; i < 20; i++ {
		a := NewAutoElector(AutoElectorConfig{Mode: "none"})
		a.Start(context.Background())
		_ = a.IsLeader()
		a.Stop()
	}
	forceGC()

	before := heapInUse()

	const iterations = 1000
	for i := 0; i < iterations; i++ {
		a := NewAutoElector(AutoElectorConfig{Mode: "none"})
		a.Start(context.Background())
		_ = a.IsLeader()
		a.Stop()
	}
	forceGC()

	after := heapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("AutoElector noop memory grew by %d bytes over %d cycles (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_AutoElector_S3Cycles verifies that AutoElector in S3 mode
// releases all heap objects after each Start/Stop cycle.
func TestMemLeak_AutoElector_S3Cycles(t *testing.T) {
	store := newMockLeakStore()

	// Warm up
	for i := 0; i < 5; i++ {
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
		time.Sleep(60 * time.Millisecond)
		a.Stop()
	}
	time.Sleep(100 * time.Millisecond)
	forceGC()

	before := heapInUse()

	const iterations = 15
	for i := 0; i < iterations; i++ {
		a := NewAutoElector(AutoElectorConfig{
			Mode:    "s3",
			S3Store: store,
			S3Config: S3ElectorConfig{
				LockKey:            "election/leader.json",
				Identity:           fmt.Sprintf("mem-node-%d", i),
				HeartbeatInterval:  25 * time.Millisecond,
				LockTTL:            500 * time.Millisecond,
				HealthCheckTimeout: 100 * time.Millisecond,
			},
		})
		a.Start(context.Background())
		time.Sleep(60 * time.Millisecond)
		a.Stop()
	}
	time.Sleep(200 * time.Millisecond)
	forceGC()

	after := heapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("AutoElector S3 mode memory grew by %d bytes over %d cycles (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_NoopElector_Cycles verifies that NoopElector creation and
// lifecycle operations do not leak.
func TestMemLeak_NoopElector_Cycles(t *testing.T) {
	// Warm up
	for i := 0; i < 100; i++ {
		e := NewNoopElector()
		e.Start(context.Background())
		_ = e.IsLeader()
		e.Stop()
	}
	runtime.GC()

	before := heapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		e := NewNoopElector()
		e.Start(context.Background())
		_ = e.IsLeader()
		e.Stop()
	}
	forceGC()

	after := heapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("NoopElector memory grew by %d bytes over %d cycles (max allowed %d)", growth, iterations, maxAllowed)
	}
}
