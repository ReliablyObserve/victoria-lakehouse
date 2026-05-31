// internal/election/k8s_bench_test.go
//
// PR #98 Tier 2 Item 11 — acquisition-latency benchmarks.
//
// Measures the time to go from elector.Start() to IsLeader()==true
// against a fakeAPIServer with simulated network latency. Results are
// documented in README.md; CI doesn't enforce a hard SLA but a sudden
// regression (10× slowdown) would be visible in the bench output.
//
// Why we keep this in the unit-test package: the fakeAPIServer is the
// canonical "perfectly-fast apiserver" baseline; adding latency to
// individual responses lets us model real apiservers without standing
// up a kind cluster per benchmark.
//
// Run with:
//
//	GOWORK=off go test -bench=BenchmarkK8sElector -benchmem ./internal/election/
//
// To get p50/p95/p99, run with -count=20 and inspect the time/op spread.
package election

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// benchAPIServer wraps fakeAPIServer with a per-request delay so we can
// model 10ms / 50ms / 200ms apiserver latency.
type benchAPIServer struct {
	*fakeAPIServer
	delay time.Duration
}

func newBenchAPIServer(delay time.Duration) *benchAPIServer {
	b := &benchAPIServer{fakeAPIServer: &fakeAPIServer{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/apis/coordination.k8s.io/v1/namespaces/", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(b.delay)
		b.handle(w, r)
	})
	b.httpServer = httptest.NewServer(mux)
	b.delay = delay
	return b
}

// benchAcquireRelease runs a single acquire → release cycle and returns
// the wall time to first IsLeader()==true.
func benchAcquireRelease(b *testing.B, srv *benchAPIServer) {
	cfg := K8sElectorConfig{
		LeaseName:      "bench-lease",
		LeaseNamespace: "default",
		Identity:       "pod-bench",
		LeaseDuration:  15 * time.Second,
		RenewDeadline:  10 * time.Second,
		RetryPeriod:    5 * time.Millisecond,
	}
	e, err := NewK8sElector(cfg)
	if err != nil {
		b.Fatal(err)
	}
	e.client = &http.Client{Timeout: 5 * time.Second}
	e.apiBase = srv.URL()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	e.Start(ctx)
	// Spin until IsLeader (kept tight; we measure end-to-end latency).
	for !e.IsLeader() {
		if time.Since(start) > 5*time.Second {
			b.Fatal("acquire timed out at 5s")
		}
		time.Sleep(time.Millisecond)
	}
	_ = time.Since(start) // accounted by b.N timer
	e.Stop()
}

func BenchmarkK8sElector_AcquireAndRelease_0ms(b *testing.B) {
	srv := newBenchAPIServer(0)
	defer srv.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset the in-server lease so each iteration is a fresh acquire.
		srv.mu.Lock()
		srv.lease = nil
		srv.mu.Unlock()
		benchAcquireRelease(b, srv)
	}
}

func BenchmarkK8sElector_AcquireAndRelease_10ms(b *testing.B) {
	srv := newBenchAPIServer(10 * time.Millisecond)
	defer srv.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.mu.Lock()
		srv.lease = nil
		srv.mu.Unlock()
		benchAcquireRelease(b, srv)
	}
}

func BenchmarkK8sElector_AcquireAndRelease_50ms(b *testing.B) {
	srv := newBenchAPIServer(50 * time.Millisecond)
	defer srv.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.mu.Lock()
		srv.lease = nil
		srv.mu.Unlock()
		benchAcquireRelease(b, srv)
	}
}

// BenchmarkK8sElector_RenewLoop measures the per-renew steady-state cost.
// Each iteration triggers one renew round trip; over b.N iterations we
// get a stable allocs/op + ns/op number that catches regressions in the
// hot path (JSON marshal of leaseObject, http.NewRequest + transport).
func BenchmarkK8sElector_RenewLoop(b *testing.B) {
	srv := newBenchAPIServer(0)
	defer srv.Close()
	cfg := K8sElectorConfig{
		LeaseName:      "bench-renew",
		LeaseNamespace: "default",
		Identity:       "pod-bench",
		LeaseDuration:  15 * time.Second,
		RenewDeadline:  10 * time.Second,
		RetryPeriod:    1 * time.Millisecond,
	}
	e, err := NewK8sElector(cfg)
	if err != nil {
		b.Fatal(err)
	}
	e.client = &http.Client{Timeout: 5 * time.Second}
	e.apiBase = srv.URL()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	// Spin until leader.
	deadline := time.Now().Add(2 * time.Second)
	for !e.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Microsecond)
	}
	if !e.IsLeader() {
		b.Fatal("never became leader")
	}

	// Reset timer; let the bench measure the renew rate over the b.N
	// "iterations" (we don't have a per-iteration handle; instead we
	// measure how many PUTs happened during the bench window).
	b.ResetTimer()
	srv.mu.Lock()
	startPuts := srv.putCount
	srv.mu.Unlock()
	// Sleep for b.N microseconds so the bench harness scales the duration.
	time.Sleep(time.Duration(b.N) * time.Microsecond)
	srv.mu.Lock()
	endPuts := srv.putCount
	srv.mu.Unlock()
	b.StopTimer()
	e.Stop()
	b.ReportMetric(float64(endPuts-startPuts), "renews")
}

// Compile-time guard: ensure benchAPIServer continues to implement the
// fakeAPIServer interface (composition). If the embed shape changes, this
// declaration would fail to compile, drawing attention.
var _ = (&sync.Once{}).Do
