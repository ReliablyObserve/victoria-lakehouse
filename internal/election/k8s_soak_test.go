//go:build soak
// +build soak

// internal/election/k8s_soak_test.go
//
// Long-running soak test for the K8sElector. Skipped by default; opt-in via
// the `soak` build tag:
//
//	go test -count=1 -tags=soak -run TestK8sElector_Soak_1Hour -timeout=70m ./internal/election/
//
// Invariants asserted:
//   - heap growth across the soak window stays under 10 percent
//   - the elector successfully renews at the expected RetryPeriod cadence
//   - no goroutine drift
package election

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestK8sElector_Soak_1Hour holds leadership for 1 hour against a fake API
// server, then validates memory and goroutine stability.
//
// Fails-without: the renewLoop's bounded resource footprint (no allocations
// in the per-tick hot path beyond the http request roundtrip and lease JSON
// marshalling).
func TestK8sElector_Soak_1Hour(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()

	e := newK8sElectorForTest(t, srv, "pod-soak", nil)
	// Production-shape timing for the soak.
	e.cfg.LeaseDuration = 15 * time.Second
	e.cfg.RenewDeadline = 10 * time.Second
	e.cfg.RetryPeriod = 2 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 5*time.Second, e.IsLeader)

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	goroutinesBefore := runtime.NumGoroutine()

	// Hold leadership for an hour. Sample every minute to make hangs obvious
	// in the test output.
	deadline := time.Now().Add(1 * time.Hour)
	for time.Now().Before(deadline) {
		time.Sleep(60 * time.Second)
		if !e.IsLeader() {
			t.Fatal("lost leadership during soak window")
		}
		t.Logf("soak progress: t-remaining=%v, IsLeader=%v",
			time.Until(deadline).Truncate(time.Second), e.IsLeader())
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	goroutinesAfter := runtime.NumGoroutine()

	if memAfter.HeapInuse > memBefore.HeapInuse*11/10 {
		t.Errorf("heap grew by more than 10%%: before=%d after=%d",
			memBefore.HeapInuse, memAfter.HeapInuse)
	}
	if goroutinesAfter > goroutinesBefore+3 {
		t.Errorf("goroutine drift: before=%d after=%d", goroutinesBefore, goroutinesAfter)
	}
	// We expect ~1800 renew PUTs across 1h at RetryPeriod=2s.
	srv.mu.Lock()
	puts := srv.putCount
	srv.mu.Unlock()
	if puts < 1500 || puts > 2100 {
		t.Errorf("expected ~1800 PUT renewals over 1h, got %d", puts)
	}

	e.Stop()
}
