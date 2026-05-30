// internal/election/k8s_integration_test.go
//
// Integration test: multiple K8sElector candidates compete against the same
// fake API server and we assert the safety property that at most one is
// leader at any moment, and that leadership migrates when the current leader
// stops cleanly.
package election

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestK8sElector_Integration_MultiCandidate runs 3 candidates against the
// same fakeAPIServer (which implements coordination/v1 CAS faithfully) and
// asserts:
//
//  1. At any sampled instant, the number of IsLeader=true candidates is
//     always at most 1.
//  2. After the current leader Stops, exactly one of the survivors becomes
//     leader within a bounded window.
//
// Together these properties exercise the full state machine: acquire under
// contention, renew, release-on-stop, re-acquire after release.
func TestK8sElector_Integration_MultiCandidate(t *testing.T) {
	srv := newFakeAPIServer()
	defer srv.Close()

	candidates := make([]*K8sElector, 3)
	for i := range candidates {
		candidates[i] = newK8sElectorForTest(t, srv, identityFor(i), nil)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, c := range candidates {
		c.Start(ctx)
	}
	defer func() {
		for _, c := range candidates {
			c.Stop()
		}
	}()

	// Safety monitor: sample state every 5 ms for ~1s and ensure no
	// duplicate leadership ever happens.
	var safety sync.WaitGroup
	safety.Add(1)
	stopMonitor := make(chan struct{})
	violations := 0
	go func() {
		defer safety.Done()
		for {
			select {
			case <-stopMonitor:
				return
			default:
				count := 0
				for _, c := range candidates {
					if c.IsLeader() {
						count++
					}
				}
				if count > 1 {
					violations++
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// Wait for someone to acquire leadership.
	leader := waitForAnyLeader(t, candidates, 3*time.Second)
	if leader == nil {
		t.Fatal("no candidate became leader")
	}

	// Step down the current leader; another should take over.
	leader.Stop()

	// Find the new leader.
	deadline := time.Now().Add(3 * time.Second)
	var newLeader *K8sElector
	for time.Now().Before(deadline) {
		for _, c := range candidates {
			if c != leader && c.IsLeader() {
				newLeader = c
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no successor took leadership after the leader stopped")
	}
	if newLeader == leader {
		t.Fatal("stopped leader regained leadership unexpectedly")
	}

	close(stopMonitor)
	safety.Wait()
	if violations > 0 {
		t.Errorf("safety violation: observed %d instants with >1 leader simultaneously", violations)
	}
}

// waitForAnyLeader returns the first candidate that becomes leader, or nil.
func waitForAnyLeader(t *testing.T, candidates []*K8sElector, timeout time.Duration) *K8sElector {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, c := range candidates {
			if c.IsLeader() {
				return c
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// identityFor returns a stable identity string for a candidate index.
func identityFor(i int) string {
	return "pod-" + string(rune('A'+i))
}
