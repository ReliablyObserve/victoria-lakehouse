// internal/election/k8s_leak_test.go
//
// Goroutine-leak tests for K8sElector. We avoid pulling in go.uber.org/goleak
// (adds dependency weight) and instead use runtime.NumGoroutine snapshots
// with a small tolerance for transient stdlib goroutines.
package election

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestK8sElector_NoGoroutineLeak runs Start/Stop in a tight loop and asserts
// that goroutine count does not grow unbounded. The Stop path must:
//  1. cancel the elector context (so run() returns)
//  2. close e.doneCh after run() exits
//  3. release any held lease via best-effort PUT (timeout-bounded)
//
// Fails-without: the goroutine cleanup in run() / Stop(). If the run loop
// leaks a goroutine on context cancel, this test will surface it as drift.
func TestK8sElector_NoGoroutineLeak(t *testing.T) {
	// Warm up: do one iteration outside the measurement window so any
	// runtime-internal goroutines (e.g. http transport idle-conn keeper)
	// are already started before we snapshot.
	srv := newFakeAPIServer()
	defer srv.Close()
	for i := 0; i < 2; i++ {
		e := newK8sElectorForTest(t, srv, "warmup-pod", nil)
		ctx, cancel := context.WithCancel(context.Background())
		e.Start(ctx)
		waitFor(t, 2*time.Second, e.IsLeader)
		e.Stop()
		cancel()
	}
	// Allow stdlib goroutines to settle.
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()

	const iterations = 30
	for i := 0; i < iterations; i++ {
		e := newK8sElectorForTest(t, srv, "leak-pod", nil)
		ctx, cancel := context.WithCancel(context.Background())
		e.Start(ctx)
		waitFor(t, 2*time.Second, e.IsLeader)
		e.Stop()
		cancel()
	}
	runtime.GC()
	time.Sleep(150 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Allow up to 5 goroutines of slack to absorb stdlib transient state.
	// Anything beyond is real elector leakage.
	const slack = 5
	if growth := after - before; growth > slack {
		t.Errorf("goroutine count grew by %d after %d Start/Stop cycles "+
			"(before=%d after=%d slack=%d); likely leak in K8sElector lifecycle",
			growth, iterations, before, after, slack)
	}
}
