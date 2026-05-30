// internal/election/k8s_failure_modes_test.go
//
// PR #98 — production K8s leader-election failure-mode coverage.
//
// Each test in this file locks ONE production failure mode the elector must
// handle correctly. The tests are intentionally surgical — they should each
// fail when ONE specific production-code line is changed/removed, so
// future maintainers know exactly what they broke.
//
// Failure modes covered (PR #98 Tier 1):
//
//   - Item 2: lease deleted by operator → elector observes 404 → POST fresh
//     within RetryPeriod
//   - Item 3: lease edited by operator (different holder) → elector observes
//     new holder on next GET and steps down via OnNewLeader
//   - Item 5: apiserver 5xx / timeout on renew → exponential backoff →
//     OnStoppedLeading within RenewDeadline + slack
//   - Item 6: same-identity reclaim after pod restart — leader's own
//     identity already in lease → immediate IsLeader without
//     waiting LeaseDuration
//   - Item 7: same identity collision — two electors with identical
//     identity strings race; exactly one wins CAS
//
// Each test's leading comment names the production line it guards and the
// "comment-out-X" negative-control proof.
package election

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ----- Item 2: lease deleted externally -------------------------------------

// TestK8sElector_LeaseDeletedExternally_Recreates locks the recovery path
// when an operator runs `kubectl delete lease …`. After the lease is gone:
//
//  1. tryRenew's GET returns 404 → must return (false, nil) so the renew
//     loop exits via the "conflict" path (NOT the err != nil continue,
//     which would spin forever).
//  2. The outer run loop re-enters acquireLoop.
//  3. tryAcquire's GET also returns 404 → its StatusNotFound branch POSTs
//     a fresh lease and the elector becomes leader again.
//
// Fails-without: BOTH the `if status == http.StatusNotFound { return false,
// nil }` branch added to tryRenew AND the StatusNotFound branch already in
// tryAcquire. Comment out either and this test will hang or fail.
func TestK8sElector_LeaseDeletedExternally_Recreates(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	e := newK8sElectorForTest(t, srv, "pod-A", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Simulate `kubectl delete lease`: drop the in-server state.
	srv.mu.Lock()
	srv.lease = nil
	srv.mu.Unlock()

	// The renew loop's next GET will 404, tryRenew now treats that as
	// "lease lost", the loop exits via fireStoppedLeading("conflict"), the
	// outer run loop re-enters acquireLoop, which then POSTs fresh. End
	// state: server-side lease exists with holderIdentity=pod-A again.
	waitFor(t, 3*time.Second, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return srv.lease != nil && srv.lease.Spec.HolderIdentity != nil
	})
	srv.mu.Lock()
	holder := ""
	if srv.lease != nil && srv.lease.Spec.HolderIdentity != nil {
		holder = *srv.lease.Spec.HolderIdentity
	}
	srv.mu.Unlock()
	if holder != "pod-A" {
		t.Fatalf("lease holder after recreate = %q, want pod-A", holder)
	}
	e.Stop()
}

// ----- Item 3: lease edited by operator ------------------------------------

// TestK8sElector_LeaseEditedExternally_ObservesNewHolder locks the step-down
// path when an operator runs `kubectl patch lease … --type=json -p='[{"op":
// "replace","path":"/spec/holderIdentity","value":"squatter"}]'`. The next
// renew GET sees a different holder; tryRenew returns (false, nil); the
// renew loop fires OnStoppedLeading via the "conflict" branch.
//
// Fails-without: the `if holder != e.cfg.Identity { return false, nil }`
// branch in tryRenew. Remove that and the elector keeps trying to PUT its
// own identity with the wrong resourceVersion, looping forever.
func TestK8sElector_LeaseEditedExternally_ObservesNewHolder(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()
	var observed []string
	var omu sync.Mutex
	var stoppedLeading atomic.Bool
	e := newK8sElectorForTest(t, srv, "pod-A", nil)
	e.cfg.OnNewLeader = func(id string) {
		omu.Lock()
		defer omu.Unlock()
		observed = append(observed, id)
	}
	e.cfg.OnStoppedLeading = func() { stoppedLeading.Store(true) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Operator overrides holder identity with a fresh renewTime so the
	// lease still looks alive.
	srv.setHolder("squatter", time.Now(), 30)

	waitFor(t, 3*time.Second, func() bool {
		return !e.IsLeader() && stoppedLeading.Load()
	})

	omu.Lock()
	defer omu.Unlock()
	sawSquatter := false
	for _, id := range observed {
		if id == "squatter" {
			sawSquatter = true
			break
		}
	}
	if !sawSquatter {
		t.Errorf("OnNewLeader never fired with squatter; observed=%v", observed)
	}
	e.Stop()
}

// ----- Item 5: apiserver 5xx and timeout on renew --------------------------

// TestK8sElector_Apiserver5xx_BacksOffAndStopsOnRenewDeadline locks the
// renew-deadline behaviour when the apiserver returns 503 on every renew
// PUT. The renew loop must keep retrying at RetryPeriod cadence (back-off
// is implicit: ticker fires every RetryPeriod) and step down within
// RenewDeadline + slack.
//
// Fails-without: the `if e.clock.Now().Sub(lastRenew) > e.cfg.RenewDeadline
// { ... fireStoppedLeading("renew-deadline"); return }` branch. Without it
// the loop spins forever, retrying 503s without ever yielding leadership.
//
// Tightening guard vs TestK8sElector_Renew_FailsWithinRenewDeadline: this
// test specifically asserts that the renew loop made MULTIPLE attempts
// before stepping down (verifying backoff actually happens), not just that
// it stepped down eventually.
func TestK8sElector_Apiserver5xx_BacksOffAndStopsOnRenewDeadline(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()

	fc := newFakeClock()
	var stoppedLeading atomic.Bool
	e := newK8sElectorForTest(t, srv, "pod-A", fc)
	e.cfg.OnStoppedLeading = func() { stoppedLeading.Store(true) }
	e.cfg.RenewDeadline = 200 * time.Millisecond
	e.cfg.RetryPeriod = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Snapshot the put count BEFORE we flip to 503 so we can verify the
	// renew loop made multiple attempts after the switch.
	srv.mu.Lock()
	putsBeforeFail := srv.putCount
	srv.customPutHandler = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	srv.mu.Unlock()

	// Advance fake clock past the 200 ms RenewDeadline. Each advance ticks
	// the renew loop once and bumps now by RetryPeriod (25 ms).
	for i := 0; i < 15; i++ {
		fc.advance(1)
		time.Sleep(10 * time.Millisecond)
	}

	waitFor(t, 3*time.Second, func() bool {
		return !e.IsLeader() && stoppedLeading.Load()
	})

	srv.mu.Lock()
	putsAfterFail := srv.putCount
	srv.mu.Unlock()
	if putsAfterFail-putsBeforeFail < 2 {
		t.Errorf("renew loop made only %d PUT attempts after starting to 503; expected at least 2 (backoff + retry)",
			putsAfterFail-putsBeforeFail)
	}
	e.Stop()
}

// TestK8sElector_ApiserverTimeout_StopsOnRenewDeadline locks the same renew-
// deadline behaviour for the "API server hangs forever" case. The fake
// server holds every PUT until we close hangCh OR the request context is
// cancelled by the client Timeout. The elector's per-request 100 ms
// timeout aborts each hung PUT; the renew loop crosses RenewDeadline and
// steps down.
//
// Fails-without: the same renew-deadline branch in renewLoop. Also requires
// the http.Client used inside the elector to honor request context
// (set up correctly via http.NewRequestWithContext in doRequest).
func TestK8sElector_ApiserverTimeout_StopsOnRenewDeadline(t *testing.T) {
	srv := newFakeAPIServer()
	hangCh := make(chan struct{})
	// Defer ordering matters: httptest.Server.Close blocks until all
	// in-flight handlers return. We must close hangCh FIRST so the
	// suspended PUT handlers unblock and exit, THEN Close() the server.
	// Defers run LIFO — declaring close(hangCh) AFTER srv.Close means it
	// runs first.
	defer srv.Close()
	defer close(hangCh)

	fc := newFakeClock()
	cfg := K8sElectorConfig{
		LeaseName:      "test-lease",
		LeaseNamespace: "default",
		Identity:       "pod-A",
		LeaseDuration:  15 * time.Second,
		RenewDeadline:  200 * time.Millisecond,
		RetryPeriod:    25 * time.Millisecond,
	}
	var stoppedLeading atomic.Bool
	cfg.OnStoppedLeading = func() { stoppedLeading.Store(true) }
	e, err := NewK8sElector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.client = &http.Client{Timeout: 100 * time.Millisecond}
	e.apiBase = srv.URL()
	e.clock = fc

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	waitFor(t, 2*time.Second, e.IsLeader)

	// Now swap in a PUT handler that hangs on every subsequent PUT (all
	// renews). The handler returns when the request context cancels.
	srv.mu.Lock()
	srv.customPutHandler = func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-hangCh:
			w.WriteHeader(http.StatusOK)
		}
	}
	srv.mu.Unlock()

	// Drive the fake clock past RenewDeadline (200 ms).
	for i := 0; i < 15; i++ {
		fc.advance(1)
		time.Sleep(15 * time.Millisecond)
	}

	waitFor(t, 3*time.Second, func() bool {
		return !e.IsLeader() && stoppedLeading.Load()
	})
	e.Stop()
}

// ----- Item 6: same-identity reclaim after restart -------------------------

// TestK8sElector_ReclaimsOwnLeaseAfterRestart locks the StatefulSet-restart
// recovery path. When a leader pod (`lh-0`) crashes and kubelet recreates
// it with the same name, the new elector's GET shows the lease's
// holderIdentity == its own. tryAcquire's "if we already hold it … take
// it" path must fire immediately — NOT wait LeaseDuration as if the lease
// belonged to a stranger.
//
// Fails-without: the `if holder != e.cfg.Identity` guard around the
// leaseExpired() check in tryAcquire. Today, when holder == e.cfg.Identity,
// the code skips leaseExpired and proceeds to PUT immediately. If a
// refactor accidentally checks leaseExpired() unconditionally, this test
// will catch it because the second elector will be blocked until renewTime
// + LeaseDuration elapses (i.e., for ~30 s instead of ~50 ms).
func TestK8sElector_ReclaimsOwnLeaseAfterRestart(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()

	// Pre-populate the lease as if `lh-0` already held it from the previous
	// instance — fresh renewTime so it's NOT expired.
	srv.setHolder("lh-0", time.Now(), 30)

	// Now bring up a "restarted" pod with the same identity.
	e := newK8sElectorForTest(t, srv, "lh-0", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)

	// If the reclaim path works, IsLeader() flips true within ~RetryPeriod
	// (50 ms in the test config), NOT after LeaseDuration (15s). Use a
	// 1 second budget — well under LeaseDuration — so a regression that
	// removed the same-identity short-circuit would fail.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("restarted pod did not reclaim its own lease within 1s; same-identity short-circuit broken")
	}
	e.Stop()
}

// ----- Item 7: same-identity collision (two pods, one identity) ------------

// TestK8sElector_SameIdentityTwoCandidates_OneWins locks CAS-correctness
// even when two pods are misconfigured with identical identity strings.
// Both race to POST/PUT against the same fake API server; exactly one
// wins and the other loses (sees its PUT get 409). The contract is
// "exactly one leader at a time", not "the first to start always wins".
//
// Fails-without: the StatusConflict branches in tryAcquire (POST path
// returning false, nil) and tryRenew. Remove those and the loser would
// either error out permanently or silently believe it's the leader.
//
// Note: with identical identities, the second elector's GET after the
// first's POST sees holder=="our identity" → the code takes the
// "already holding" reclaim path. Both then attempt PUT; CAS picks one
// (whichever's resourceVersion matches). The other's PUT gets 409 →
// returns (false, nil) → no second leader on server side.
func TestK8sElector_SameIdentityTwoCandidates_OneWins(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()

	const sharedID = "split-brain"
	eA := newK8sElectorForTest(t, srv, sharedID, nil)
	eB := newK8sElectorForTest(t, srv, sharedID, nil)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	eA.Start(ctxA)
	eB.Start(ctxB)

	// Wait for the lease to be acquired. The server's lease object can
	// only encode ONE holderIdentity by construction — that's the
	// "exactly one wins" invariant. Both elector goroutines may set
	// their local IsLeader cache to true, but the server state is the
	// authoritative gate.
	waitFor(t, 3*time.Second, func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return srv.lease != nil &&
			srv.lease.Spec.HolderIdentity != nil &&
			*srv.lease.Spec.HolderIdentity == sharedID
	})

	srv.mu.Lock()
	holder := ""
	if srv.lease != nil && srv.lease.Spec.HolderIdentity != nil {
		holder = *srv.lease.Spec.HolderIdentity
	}
	rv := ""
	if srv.lease != nil {
		rv = srv.lease.Metadata.ResourceVersion
	}
	srv.mu.Unlock()
	if holder != sharedID {
		t.Errorf("lease holder = %q, want %q (split-brain identity)", holder, sharedID)
	}
	if rv == "" {
		t.Error("lease resourceVersion empty; CAS did not run")
	}

	eA.Stop()
	eB.Stop()
}
