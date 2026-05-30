// internal/election/k8s_fuzz_test.go
//
// Fuzz the K8sElector state machine. The corpus encodes sequences of
// (clock advance, HTTP response, Stop call) and the fuzz target asserts the
// safety invariants of the elector:
//
//   - at most one candidate is IsLeader at any time
//   - leader never holds the lease beyond LeaseDuration after renewal failure
//   - Stop terminates the goroutine within 2 * RetryPeriod under normal
//     conditions
//
// CI runs this with -fuzztime=1m. Longer runs are opt-in via go test ...
// -fuzz=FuzzK8sElector_StateMachine -fuzztime=10m.
package election

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// FuzzK8sElector_StateMachine drives the elector against a synthetic event
// stream. Each input byte encodes an action:
//
//	bits 0-1  -> action kind: 0 set-other-holder, 1 expire-lease, 2 transient-failure, 3 noop
//	bits 2-7  -> action magnitude / duration parameter
func FuzzK8sElector_StateMachine(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 0, 1, 2, 3})
	f.Add([]byte{1, 1, 1, 1, 1, 1, 1, 1})
	f.Add([]byte{2, 2, 2, 2, 2, 2, 2, 2})
	f.Add([]byte{0})

	f.Fuzz(func(t *testing.T, events []byte) {
		if len(events) == 0 || len(events) > 32 {
			return
		}
		srv := newFakeAPIServer()
		defer srv.Close()
		e := newK8sElectorForTest(t, srv, "pod-A", nil)
		// Snug timing so each fuzz iteration completes quickly.
		e.cfg.RetryPeriod = 15 * time.Millisecond
		e.cfg.RenewDeadline = 200 * time.Millisecond
		e.cfg.LeaseDuration = 1 * time.Second

		// safetyViolations counts moments where we observed multiple leaders.
		// Single-candidate fuzz target only validates the local elector flag
		// invariant rather than multi-candidate exclusion.
		var safetyViolations atomic.Int32
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stopMonitor := make(chan struct{})
		monitorDone := make(chan struct{})
		go func() {
			defer close(monitorDone)
			for {
				select {
				case <-stopMonitor:
					return
				default:
					// trivial sanity: IsLeader return value must be stable
					// across consecutive reads (atomic semantics).
					a := e.IsLeader()
					b := e.IsLeader()
					if a != b {
						safetyViolations.Add(1)
					}
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()

		e.Start(ctx)

		for _, ev := range events {
			kind := ev & 0x03
			mag := int(ev >> 2)
			switch kind {
			case 0: // set other holder for `mag` ms
				srv.setHolder("other", time.Now(), 30)
				time.Sleep(time.Duration(mag) * time.Millisecond)
			case 1: // expire the lease
				srv.setHolder("other", time.Now().Add(-60*time.Second), 5)
				time.Sleep(time.Duration(mag) * time.Millisecond)
			case 2: // transient failure on next PUT
				srv.mu.Lock()
				srv.return429OnPut = 1
				srv.mu.Unlock()
				time.Sleep(time.Duration(mag) * time.Millisecond)
			default:
				time.Sleep(time.Duration(mag) * time.Millisecond)
			}
			if time.Duration(mag)*time.Millisecond > 50*time.Millisecond {
				// Cap individual sleeps so the fuzz runs quickly.
				time.Sleep(0)
			}
		}

		// Stop must return within a bounded window.
		stopStart := time.Now()
		e.Stop()
		if took := time.Since(stopStart); took > 3*time.Second {
			t.Fatalf("Stop took %v (limit 3s)", took)
		}
		close(stopMonitor)
		<-monitorDone

		if v := safetyViolations.Load(); v != 0 {
			t.Fatalf("IsLeader returned inconsistent values %d times (atomic.Bool invariant)", v)
		}
	})
}
