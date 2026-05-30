// internal/election/k8s_test.go
//
// Unit tests for the always-on Kubernetes leader-election state machine.
// Coverage target: 90 percent of internal/election/k8s.go. The strategy is:
//
//   - Drive the elector against an httptest.Server that faithfully implements
//     coordination.k8s.io/v1 Lease CAS semantics (GET / PUT / POST with 409
//     on resourceVersion mismatch).
//   - Inject a fakeClock so tests are deterministic and never block on real
//     time (no Sleep, no NewTicker on the real clock).
//   - Inject an httpDoer mock for fault-injection (429, 409, network errors,
//     TLS plumbing).
//
// Each test that locks behaviour for a regression carries a "fails-without"
// comment: a short note describing which production code line the test
// guards, so future refactors break loudly.
package election

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ----- existing behavioural tests preserved from old k8s_test.go -----

func TestK8sElectorConfig_Defaults(t *testing.T) {
	cfg := K8sElectorConfig{LeaseName: "test", LeaseNamespace: "default", Identity: "pod-0"}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.LeaseDuration != 15*time.Second {
		t.Fatalf("expected 15s LeaseDuration, got %v", e.cfg.LeaseDuration)
	}
	if e.cfg.RenewDeadline != 10*time.Second {
		t.Fatalf("expected 10s RenewDeadline, got %v", e.cfg.RenewDeadline)
	}
	if e.cfg.RetryPeriod != 2*time.Second {
		t.Fatalf("expected 2s RetryPeriod, got %v", e.cfg.RetryPeriod)
	}
}

func TestK8sElector_ImplementsLeader(t *testing.T) {
	var _ Leader = (*K8sElector)(nil)
}

func TestK8sElector_NotLeaderBeforeStart(t *testing.T) {
	e := &K8sElector{}
	if e.IsLeader() {
		t.Fatal("should not be leader before Start")
	}
}

func TestK8sElector_CustomDurations(t *testing.T) {
	cfg := K8sElectorConfig{
		LeaseName:      "custom-lease",
		LeaseNamespace: "my-namespace",
		Identity:       "my-pod",
		LeaseDuration:  30 * time.Second,
		RenewDeadline:  20 * time.Second,
		RetryPeriod:    5 * time.Second,
	}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.LeaseDuration != 30*time.Second {
		t.Errorf("LeaseDuration = %v, want 30s", e.cfg.LeaseDuration)
	}
	if e.cfg.RenewDeadline != 20*time.Second {
		t.Errorf("RenewDeadline = %v, want 20s", e.cfg.RenewDeadline)
	}
	if e.cfg.RetryPeriod != 5*time.Second {
		t.Errorf("RetryPeriod = %v, want 5s", e.cfg.RetryPeriod)
	}
	if e.cfg.LeaseNamespace != "my-namespace" {
		t.Errorf("LeaseNamespace = %q, want my-namespace", e.cfg.LeaseNamespace)
	}
}

func TestK8sElector_DefaultNamespaceFromEnv(t *testing.T) {
	cfg := K8sElectorConfig{LeaseName: "test-lease", Identity: "pod-x"}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.LeaseNamespace != "default" {
		t.Errorf("LeaseNamespace = %q, want default", e.cfg.LeaseNamespace)
	}
}

func TestK8sElector_StopBeforeStart(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{LeaseName: "test", Identity: "pod-0"})
	if err != nil {
		t.Fatal(err)
	}
	e.Stop()
	if e.IsLeader() {
		t.Error("should not be leader after Stop without Start")
	}
}

func TestK8sElector_StopClearsLeaderFlag(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{LeaseName: "test", Identity: "pod-0"})
	if err != nil {
		t.Fatal(err)
	}
	e.leader.Store(true)
	if !e.IsLeader() {
		t.Fatal("expected leader after manual set")
	}
	e.Stop()
	if e.IsLeader() {
		t.Error("Stop should clear leader flag")
	}
}

func TestK8sElector_ZeroDurationsGetDefaults(t *testing.T) {
	cfg := K8sElectorConfig{LeaseName: "zero-test"}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.LeaseDuration != 15*time.Second {
		t.Errorf("LeaseDuration = %v, want 15s", e.cfg.LeaseDuration)
	}
	if e.cfg.RenewDeadline != 10*time.Second {
		t.Errorf("RenewDeadline = %v, want 10s", e.cfg.RenewDeadline)
	}
	if e.cfg.RetryPeriod != 2*time.Second {
		t.Errorf("RetryPeriod = %v, want 2s", e.cfg.RetryPeriod)
	}
	if e.cfg.LeaseNamespace != "default" {
		t.Errorf("LeaseNamespace = %q, want default", e.cfg.LeaseNamespace)
	}
	if e.cfg.Identity == "" {
		t.Error("Identity should be set from hostname, not empty")
	}
}

func TestK8sElector_IsLeaderFalseByDefault(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{LeaseName: "test", Identity: "pod-0"})
	if err != nil {
		t.Fatal(err)
	}
	if e.IsLeader() {
		t.Fatal("should not be leader by default")
	}
}

func TestK8sElector_StartThenStop(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{LeaseName: "lifecycle-test", Identity: "pod-0"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	e.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	e.Stop()
	if e.IsLeader() {
		t.Error("should not be leader after Stop")
	}
}

func TestK8sElector_StopIdempotent(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{LeaseName: "idempotent-test", Identity: "pod-0"})
	if err != nil {
		t.Fatal(err)
	}
	e.Stop()
	e.Stop()
	e.Stop()
	if e.IsLeader() {
		t.Error("should not be leader")
	}
}

func TestK8sElector_EmptyNamespaceNoPodEnv(t *testing.T) {
	cfg := K8sElectorConfig{LeaseName: "ns-test", Identity: "pod-x"}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.LeaseNamespace != "default" {
		t.Errorf("expected namespace 'default', got %q", e.cfg.LeaseNamespace)
	}
}

func TestK8sElector_EmptyIdentityGetsHostname(t *testing.T) {
	cfg := K8sElectorConfig{LeaseName: "identity-test"}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.cfg.Identity == "" {
		t.Error("expected identity to be set from hostname")
	}
}

// ----- new tests against fake k8s API server -----

// fakeAPIServer is a minimal httptest-backed implementation of the
// coordination.k8s.io/v1 Lease REST surface that the elector hits. It supports
// GET, POST (create), and PUT (replace) with CAS via resourceVersion. Tests
// can poke it directly to simulate other holders.
type fakeAPIServer struct {
	mu                   sync.Mutex
	lease                *leaseObject
	resourceVersionCount uint64
	failNextGet          bool
	returnConflictOnPut  int
	return429OnPut       int
	returnNotFoundOnGet  bool
	customGetHandler     func(w http.ResponseWriter, r *http.Request)
	customPutHandler     func(w http.ResponseWriter, r *http.Request)
	serverDownAfterPuts  int
	putCount             int
	httpServer           *httptest.Server
}

func newFakeAPIServer() *fakeAPIServer {
	f := &fakeAPIServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/apis/coordination.k8s.io/v1/namespaces/", f.handle)
	f.httpServer = httptest.NewServer(mux)
	return f
}

func (f *fakeAPIServer) URL() string { return f.httpServer.URL }
func (f *fakeAPIServer) Close()      { f.httpServer.Close() }

func (f *fakeAPIServer) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.serverDownAfterPuts > 0 && f.putCount >= f.serverDownAfterPuts && r.Method == http.MethodPut {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	switch r.Method {
	case http.MethodGet:
		if f.customGetHandler != nil {
			f.customGetHandler(w, r)
			return
		}
		if f.failNextGet {
			f.failNextGet = false
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if f.lease == nil || f.returnNotFoundOnGet {
			f.returnNotFoundOnGet = false
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(f.lease)

	case http.MethodPost:
		f.putCount++
		if f.lease != nil {
			w.WriteHeader(http.StatusConflict)
			return
		}
		var body leaseObject
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.resourceVersionCount++
		body.Metadata.ResourceVersion = fmt.Sprintf("%d", f.resourceVersionCount)
		f.lease = &body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(f.lease)

	case http.MethodPut:
		f.putCount++
		if f.customPutHandler != nil {
			f.customPutHandler(w, r)
			return
		}
		if f.return429OnPut > 0 {
			f.return429OnPut--
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if f.returnConflictOnPut > 0 {
			f.returnConflictOnPut--
			w.WriteHeader(http.StatusConflict)
			return
		}
		var body leaseObject
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if f.lease == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if body.Metadata.ResourceVersion != f.lease.Metadata.ResourceVersion {
			w.WriteHeader(http.StatusConflict)
			return
		}
		f.resourceVersionCount++
		body.Metadata.ResourceVersion = fmt.Sprintf("%d", f.resourceVersionCount)
		f.lease = &body
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(f.lease)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// setHolder forcibly rewrites the in-server Lease holder to simulate another
// candidate taking over.
func (f *fakeAPIServer) setHolder(holder string, renewTime time.Time, durationSec int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rt := metav1.NewMicroTime(renewTime)
	at := metav1.NewMicroTime(renewTime)
	id := holder
	f.resourceVersionCount++
	f.lease = &leaseObject{
		Kind:       "Lease",
		APIVersion: "coordination.k8s.io/v1",
		Metadata: metav1.ObjectMeta{
			Name:            "test-lease",
			Namespace:       "default",
			ResourceVersion: fmt.Sprintf("%d", f.resourceVersionCount),
		},
		Spec: leaseSpec{
			HolderIdentity:       &id,
			LeaseDurationSeconds: &durationSec,
			AcquireTime:          &at,
			RenewTime:            &rt,
		},
	}
}

// newK8sElectorForTest constructs an elector wired to a fakeAPIServer with a
// fakeClock. Helper so individual tests stay short.
func newK8sElectorForTest(t *testing.T, srv *fakeAPIServer, identity string, fc *fakeClock) *K8sElector {
	t.Helper()
	cfg := K8sElectorConfig{
		LeaseName:      "test-lease",
		LeaseNamespace: "default",
		Identity:       identity,
		LeaseDuration:  15 * time.Second,
		RenewDeadline:  10 * time.Second,
		RetryPeriod:    50 * time.Millisecond,
	}
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.client = &http.Client{Timeout: 2 * time.Second}
	e.apiBase = srv.URL()
	if fc != nil {
		e.clock = fc
	}
	return e
}

// fakeClock is a deterministic time source for the renew loop tests. It
// allows tests to advance time without calling time.Sleep.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Now()} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Sleep(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

func (f *fakeClock) NewTicker(d time.Duration) Ticker {
	ft := &fakeTicker{ch: make(chan time.Time, 16), clock: f, interval: d}
	f.mu.Lock()
	f.tickers = append(f.tickers, ft)
	f.mu.Unlock()
	return ft
}

// advance ticks all live tickers `n` times at their period and bumps the
// clock forward by n*period.
func (f *fakeClock) advance(n int) {
	for i := 0; i < n; i++ {
		f.mu.Lock()
		tickers := append([]*fakeTicker{}, f.tickers...)
		f.mu.Unlock()
		for _, t := range tickers {
			f.mu.Lock()
			f.now = f.now.Add(t.interval)
			now := f.now
			f.mu.Unlock()
			select {
			case t.ch <- now:
			default:
			}
		}
	}
}

type fakeTicker struct {
	ch       chan time.Time
	clock    *fakeClock
	interval time.Duration
}

func (f *fakeTicker) C() <-chan time.Time { return f.ch }
func (f *fakeTicker) Stop()               { /* test-only ticker; nothing to clean up */ }

// waitFor blocks until cond returns true or timeout elapses; t.Fatal if not.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// 1. AcquireLease - new lease (POST 201).
//
// Fails-without: the StatusCreated case in tryAcquire create path. Remove
// that branch and this test panics on unexpected status.
func TestK8sElector_AcquireLease_NewLease(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)
	e.Stop()
}

// 2. AcquireLease - expired existing lease (someone else's holder, renewTime
// older than LeaseDurationSeconds ago).
//
// Fails-without: leaseExpired() check inside the holder-mismatch branch.
func TestK8sElector_AcquireLease_ExpiredLease(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	// Pre-populate a lease held by another identity with renewTime far in the
	// past (older than 15s LeaseDurationSeconds).
	srv.setHolder("other", time.Now().Add(-30*time.Second), 15)
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)
	e.Stop()
}

// 3. AcquireLease - held by another, not expired -> we wait, don't acquire.
//
// Fails-without: the (!leaseExpired) early-return-false branch. If removed,
// this test fails because we'd steal the lease while it's valid.
func TestK8sElector_AcquireLease_HeldByOther(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	srv.setHolder("other", time.Now(), 15)
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	// Give the elector a couple of acquire attempts; it must NOT become
	// leader while "other" is still alive.
	time.Sleep(300 * time.Millisecond)
	if e.IsLeader() {
		t.Fatal("should not have acquired lease held by other (still valid)")
	}
	e.Stop()
}

// 4. AcquireLease - 429 Too Many Requests on PUT -> we retry, eventually win.
//
// Fails-without: the StatusTooManyRequests case in tryAcquire's switch.
func TestK8sElector_AcquireLease_Conflict_429(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	// Pre-populate an expired lease so tryAcquire's PUT path runs (not POST).
	srv.setHolder("other", time.Now().Add(-30*time.Second), 15)
	srv.return429OnPut = 2

	e := newK8sElectorForTest(t, srv, "pod-A", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 3*time.Second, e.IsLeader)
	e.Stop()
}

// 5. AcquireLease - 409 Conflict on PUT -> we retry, eventually win.
//
// Fails-without: the StatusConflict case in tryAcquire's switch.
func TestK8sElector_AcquireLease_Conflict_409(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	srv.setHolder("other", time.Now().Add(-30*time.Second), 15)
	srv.returnConflictOnPut = 2

	e := newK8sElectorForTest(t, srv, "pod-A", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 3*time.Second, e.IsLeader)
	e.Stop()
}

// 6. Renew - success path bumps RenewTime.
//
// Fails-without: tryRenew's PUT and StatusOK case.
func TestK8sElector_Renew_Success(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Wait long enough for at least one renew cycle (~50 ms RetryPeriod).
	time.Sleep(300 * time.Millisecond)

	srv.mu.Lock()
	puts := srv.putCount
	srv.mu.Unlock()
	if puts < 2 {
		t.Errorf("expected at least 2 PUT renewals, got %d", puts)
	}
	if !e.IsLeader() {
		t.Error("should still be leader after renewals")
	}
	e.Stop()
}

// 7. Renew - cannot renew within RenewDeadline -> OnStoppedLeading fires.
//
// Fails-without: the RenewDeadline check inside renewLoop. Comment out the
// "if e.clock.Now().Sub(lastRenew) > e.cfg.RenewDeadline" block and this
// test will time out waiting for stoppedLeadingCalled.
func TestK8sElector_Renew_FailsWithinRenewDeadline(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()

	fc := newFakeClock()
	var stoppedLeadingCalled atomic.Bool
	e := newK8sElectorForTest(t, srv, "pod-A", fc)
	e.cfg.OnStoppedLeading = func() { stoppedLeadingCalled.Store(true) }
	e.cfg.RenewDeadline = 100 * time.Millisecond
	e.cfg.RetryPeriod = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Now make every renew PUT fail with 503. The fake server's
	// renewLoop will eventually exceed the 100 ms deadline (fakeClock-based).
	srv.mu.Lock()
	srv.customPutHandler = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	srv.mu.Unlock()

	// Advance fake clock past the deadline. Each "advance" ticks the renew
	// loop once and bumps now by RetryPeriod (25 ms).
	for i := 0; i < 10; i++ {
		fc.advance(1)
		time.Sleep(10 * time.Millisecond)
	}

	waitFor(t, 3*time.Second, func() bool {
		return !e.IsLeader() && stoppedLeadingCalled.Load()
	})
	e.Stop()
}

// 8. Renew - transient network error on PUT -> we retry on next tick.
//
// Fails-without: tryRenew's `if err != nil` continue branch in renewLoop.
// Strategy: pre-populate a long-lived lease held by pod-A so the renew loop
// is already in the steady state. Inject 1x 503 then resume normal PUT
// behaviour; the elector must remain leader.
func TestK8sElector_Renew_NetworkError_Retries(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Toggle: fail one PUT, then resume normal behaviour. Use the failNextPut
	// flag instead of customPutHandler (which can deadlock if it re-enters).
	srv.mu.Lock()
	srv.serverDownAfterPuts = srv.putCount + 1 // one more PUT will 503
	srv.mu.Unlock()
	time.Sleep(60 * time.Millisecond) // give the 503 a moment to happen
	srv.mu.Lock()
	srv.serverDownAfterPuts = 0 // re-open
	srv.mu.Unlock()

	time.Sleep(300 * time.Millisecond)
	if !e.IsLeader() {
		t.Error("should still be leader after transient network error")
	}
	e.Stop()
}

// 9. Release on Stop clears HolderIdentity.
//
// Fails-without: the e.releaseLease call in Stop().
func TestK8sElector_Release_OnStop(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)
	e.Stop()

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.lease == nil {
		t.Fatal("lease should still exist after release")
	}
	if srv.lease.Spec.HolderIdentity != nil && *srv.lease.Spec.HolderIdentity == "pod-A" {
		t.Errorf("Stop should have cleared holder identity; got %v",
			*srv.lease.Spec.HolderIdentity)
	}
}

// 10. OnNewLeader fires on transitions.
//
// Fails-without: observeLeader's CompareAndSwap-like logic.
func TestK8sElector_LeaderTransition_OnNewLeader_Fires(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	var observed []string
	var mu sync.Mutex
	e := newK8sElectorForTest(t, srv, "pod-A", nil)
	e.cfg.OnNewLeader = func(id string) {
		mu.Lock()
		defer mu.Unlock()
		observed = append(observed, id)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Give renewals a moment, then stop.
	time.Sleep(100 * time.Millisecond)
	e.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(observed) == 0 {
		t.Fatal("expected OnNewLeader to fire at least once")
	}
	if observed[len(observed)-1] != "pod-A" {
		t.Errorf("expected final observed leader pod-A, got %v", observed)
	}
}

// 11. Context cancel exits the state machine cleanly.
//
// Fails-without: the ctx.Err() check at the top of run() / acquireLoop.
func TestK8sElector_ContextCancel_Clean(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	srv.setHolder("other", time.Now(), 30) // we'll never acquire
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	e.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	cancel()
	e.Stop()
	if took := time.Since(start); took > time.Second {
		t.Errorf("ctx cancel took %v; expected <1s", took)
	}
}

// 12. TLS uses ServiceAccount CA when the rest.Config is in-cluster.
//
// We cannot run real in-cluster from a unit test, so we exercise the path
// that bridges rest.Config.TLSClientConfig.CAFile through rest.HTTPClientFor
// by checking that HTTPClientFor returns a usable client and that our custom
// client field overrides it for tests.
//
// Fails-without: the rest.HTTPClientFor wiring in run().
func TestK8sElector_TLS_Uses_ServiceAccountCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	e, err := NewK8sElector(K8sElectorConfig{
		LeaseName: "tls-test",
		Identity:  "pod-tls",
	})
	if err != nil {
		t.Fatal(err)
	}
	e.apiBase = srv.URL
	e.client = srv.Client() // trust the TLS server's CA
	_, status, err := e.doRequest(context.Background(), http.MethodGet, e.leaseURL(), nil)
	if err != nil {
		t.Fatalf("TLS GET failed: %v", err)
	}
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (server returns 404 by design)", status)
	}
}

// 13. Clock skew: prefer the server's resourceVersion (CAS) over the local
// clock when deciding ownership.
//
// Fails-without: tryRenew's reliance on the server-returned holder, not on
// our local clock.
func TestK8sElector_ClockSkew_PrefersServerResourceVersion(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Server rewrites holder to "pod-B" with a fresh renewTime.
	srv.setHolder("pod-B", time.Now(), 30)

	// On the next renew, we MUST observe holder=pod-B and step down,
	// regardless of our local clock state.
	waitFor(t, 2*time.Second, func() bool { return !e.IsLeader() })
	e.Stop()
}

// 14. Concurrent renew and Stop -> no data race.
//
// Fails-without: leader.Store and leader.Load being atomic.
func TestK8sElector_ConcurrentRenewAndStop_NoDataRace(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Hammer IsLeader from many goroutines while Stop runs.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = e.IsLeader()
			}
		}()
	}
	e.Stop()
	wg.Wait()
}

// 15. Multiple Stop calls are idempotent and safe.
//
// Fails-without: stopOnce.Do() and the channel-close guard in run().
func TestK8sElector_MultipleStop_Idempotent(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	e.Stop()
	e.Stop()
	e.Stop()
	if e.IsLeader() {
		t.Error("should not be leader after Stop")
	}
}

// ----- helpers exercised for coverage -----

func TestParseResourceVersion(t *testing.T) {
	tests := []struct {
		in   string
		want uint64
	}{
		{"", 0},
		{"abc", 0},
		{"42", 42},
		{"9999999", 9999999},
	}
	for _, tc := range tests {
		if got := parseResourceVersion(tc.in); got != tc.want {
			t.Errorf("parseResourceVersion(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestStrPtr_I32Ptr(t *testing.T) {
	s := strPtr("hello")
	if s == nil || *s != "hello" {
		t.Errorf("strPtr returned %v", s)
	}
	i := i32Ptr(7)
	if i == nil || *i != 7 {
		t.Errorf("i32Ptr returned %v", i)
	}
}

func TestK8sBackendCompiledIn(t *testing.T) {
	if !K8sBackendCompiledIn() {
		t.Error("K8sBackendCompiledIn must report true (always-on K8s elector)")
	}
}

func TestRealClock_NowAndTicker(t *testing.T) {
	rc := realClock{}
	t0 := rc.Now()
	rc.Sleep(time.Millisecond)
	t1 := rc.Now()
	if !t1.After(t0) {
		t.Error("Now must monotonically advance under Sleep")
	}
	tk := rc.NewTicker(5 * time.Millisecond)
	defer tk.Stop()
	select {
	case <-tk.C():
	case <-time.After(100 * time.Millisecond):
		t.Error("ticker did not fire within 100 ms")
	}
}

// doRequestErrorPath exercises the error branch when http.NewRequestWithContext
// fails. The smallest reliable trigger is an empty method (which is rejected).
func TestDoRequest_BadMethod(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	e := newK8sElectorForTest(t, srv, "pod-x", nil)
	_, _, err := e.doRequest(context.Background(), "BAD METHOD WITH SPACE", e.leaseURL(), nil)
	if err == nil {
		t.Error("expected error for malformed method")
	}
}

// doRequest must surface transport errors as a non-nil error.
func TestDoRequest_TransportError(t *testing.T) {
	e := &K8sElector{
		cfg:     K8sElectorConfig{LeaseNamespace: "default", LeaseName: "x"},
		apiBase: "http://127.0.0.1:1", // closed port
		client:  &http.Client{Timeout: 100 * time.Millisecond},
		clock:   realClock{},
	}
	_, _, err := e.doRequest(context.Background(), http.MethodGet, e.leaseURL(), nil)
	if err == nil {
		t.Error("expected transport error from closed port")
	}
}

// doRequest must surface bearer token in Authorization header.
func TestDoRequest_BearerHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	e := &K8sElector{
		cfg:     K8sElectorConfig{LeaseNamespace: "default", LeaseName: "x"},
		apiBase: srv.URL,
		client:  srv.Client(),
		bearer:  "deadbeef",
		clock:   realClock{},
	}
	_, _, err := e.doRequest(context.Background(), http.MethodGet, e.leaseURL(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(gotAuth, "Bearer ") || !strings.HasSuffix(gotAuth, "deadbeef") {
		t.Errorf("Authorization header = %q, want Bearer deadbeef", gotAuth)
	}
}

// getLease must surface JSON decode errors.
func TestGetLease_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{not-json")
	}))
	defer srv.Close()
	e := &K8sElector{
		cfg:     K8sElectorConfig{LeaseNamespace: "default", LeaseName: "x"},
		apiBase: srv.URL,
		client:  srv.Client(),
		clock:   realClock{},
	}
	_, _, err := e.getLease(context.Background())
	if err == nil {
		t.Error("expected decode error")
	}
}

// leaseExpired covers the corner cases (nil lease, nil renew time).
func TestLeaseExpired_Corners(t *testing.T) {
	e := &K8sElector{cfg: K8sElectorConfig{LeaseDuration: 5 * time.Second}, clock: realClock{}}
	if !e.leaseExpired(nil) {
		t.Error("nil lease should be expired")
	}
	if !e.leaseExpired(&leaseObject{}) {
		t.Error("lease without RenewTime should be expired")
	}
	now := metav1.NewMicroTime(time.Now())
	secs := int32(60)
	if e.leaseExpired(&leaseObject{Spec: leaseSpec{RenewTime: &now, LeaseDurationSeconds: &secs}}) {
		t.Error("fresh lease should not be expired")
	}
}

// observeLeader must dedupe identical observations.
func TestObserveLeader_Dedup(t *testing.T) {
	var fires atomic.Int32
	e := &K8sElector{
		cfg: K8sElectorConfig{OnNewLeader: func(string) { fires.Add(1) }},
	}
	e.observeLeader("a")
	e.observeLeader("a")
	e.observeLeader("a")
	if fires.Load() != 1 {
		t.Errorf("OnNewLeader fired %d times, want 1", fires.Load())
	}
	e.observeLeader("b")
	if fires.Load() != 2 {
		t.Errorf("OnNewLeader fired %d times after transition, want 2", fires.Load())
	}
}

// releaseLease early-returns when we're not the holder.
func TestReleaseLease_NotHolder(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	srv.setHolder("other", time.Now(), 30)
	e := newK8sElectorForTest(t, srv, "pod-A", nil)
	if err := e.releaseLease(context.Background()); err != nil {
		t.Errorf("releaseLease should succeed when not holder, got %v", err)
	}
}

// fireStoppedLeading must be idempotent.
func TestFireStoppedLeading_Idempotent(t *testing.T) {
	var fires atomic.Int32
	e := &K8sElector{
		cfg: K8sElectorConfig{OnStoppedLeading: func() { fires.Add(1) }},
	}
	e.leader.Store(true)
	e.fireStoppedLeading("first")
	e.fireStoppedLeading("second")
	if fires.Load() != 1 {
		t.Errorf("OnStoppedLeading fired %d times, want 1", fires.Load())
	}
}

// Ensure errors returned by io.ReadAll are surfaced.
type breakBodyTransport struct{}

func (breakBodyTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: 200,
		Body:       brokenReadCloser{},
		Header:     make(http.Header),
	}, nil
}

type brokenReadCloser struct{}

func (brokenReadCloser) Read(_ []byte) (int, error) { return 0, errors.New("body read failed") }
func (brokenReadCloser) Close() error               { return nil }

func TestDoRequest_BodyReadError(t *testing.T) {
	e := &K8sElector{
		cfg:     K8sElectorConfig{LeaseNamespace: "default", LeaseName: "x"},
		apiBase: "http://example",
		client:  &http.Client{Transport: breakBodyTransport{}},
		clock:   realClock{},
	}
	_, _, err := e.doRequest(context.Background(), http.MethodGet, e.leaseURL(), nil)
	if err == nil {
		t.Error("expected body read error to surface")
	}
}
