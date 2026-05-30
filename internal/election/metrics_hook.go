// internal/election/metrics_hook.go
//
// Indirection layer so the election package can emit operator-observable
// metrics without importing internal/metrics (and its 190-package transitive
// closure of VictoriaMetrics/metrics + dependencies). Keeping this hook
// interface-shaped means:
//
//   - The election dep count stays under 340 (TestElectionDepCount).
//   - Tests can inject a recording hook to assert the elector emits all 6
//     expected metric families (see TestK8sElector_EmitsExpectedMetrics).
//   - Main wires the real metrics at startup via SetMetricsHook.
//
// The contract is the 6 metric families locked by Goal 8 of PR #98:
//
//	lakehouse_leader_election_state{role,lease,module}             gauge
//	lakehouse_leader_election_acquire_total{lease,module}          counter
//	lakehouse_leader_election_renew_total{lease,module,result}     counter
//	lakehouse_leader_election_release_total{lease,module}          counter
//	lakehouse_leader_election_acquire_duration_seconds{lease,module} histogram
//	lakehouse_leader_election_lease_holder{lease,module,identity}  gauge
//
// Plus one auxiliary counter for startup-error visibility:
//
//	lakehouse_leader_election_startup_errors_total{lease,module}   counter
package election

import "sync"

// MetricsHook is the abstract sink the elector pushes events to. All methods
// must be safe to call from the elector goroutine and from concurrent
// observer goroutines.
type MetricsHook interface {
	// SetLeaderState records whether this instance currently holds the lease.
	// role is "leader" when leader == 1, "follower" otherwise; lease is the
	// Lease.metadata.name; module is "logs"/"traces"/"unknown".
	SetLeaderState(role, lease, module string, leader bool)

	// IncAcquire increments the counter every time a successful acquire
	// transition (took the lease, previously not held by us) occurs.
	IncAcquire(lease, module string)

	// IncRenew increments the renew counter with the result label
	// ("success" | "conflict" | "failure").
	IncRenew(lease, module, result string)

	// IncRelease increments the counter when we successfully release the
	// lease (Stop → releaseLease success).
	IncRelease(lease, module string)

	// ObserveAcquireDuration records how long the acquire path took, in
	// seconds.
	ObserveAcquireDuration(lease, module string, seconds float64)

	// SetLeaseHolder records the currently observed holder identity as a
	// gauge with value 1. Identity may differ from our own when we're a
	// follower. Implementations should treat this as set-current + reset-old
	// (i.e., a transient "who holds the lease right now" indicator).
	SetLeaseHolder(lease, module, identity string)

	// IncStartupError increments the startup-error counter (InClusterConfig
	// failure, missing SA token, HTTPClient setup failure).
	IncStartupError(lease, module string)
}

// noopMetricsHook is the default when no hook is wired. It also serves as
// the documentation for the contract: every method is a no-op.
type noopMetricsHook struct{}

func (noopMetricsHook) SetLeaderState(string, string, string, bool)    {}
func (noopMetricsHook) IncAcquire(string, string)                      {}
func (noopMetricsHook) IncRenew(string, string, string)                {}
func (noopMetricsHook) IncRelease(string, string)                      {}
func (noopMetricsHook) ObserveAcquireDuration(string, string, float64) {}
func (noopMetricsHook) SetLeaseHolder(string, string, string)          {}
func (noopMetricsHook) IncStartupError(string, string)                 {}

var (
	metricsHookMu sync.RWMutex
	metricsHook   MetricsHook = noopMetricsHook{}
)

// SetMetricsHook installs a hook for the elector to emit events to. Pass nil
// to revert to the noop hook. The hook is process-global (the election
// package is module-scoped), so callers should wire it exactly once during
// startup before any elector goroutine runs.
func SetMetricsHook(h MetricsHook) {
	metricsHookMu.Lock()
	defer metricsHookMu.Unlock()
	if h == nil {
		metricsHook = noopMetricsHook{}
		return
	}
	metricsHook = h
}

// getMetricsHook is the internal accessor that goroutines use. It takes the
// read lock so Set vs callers is data-race-clean.
//
// In hot paths we shadow this once at the top of a call site to avoid lock
// acquisition per metric increment.
func getMetricsHook() MetricsHook {
	metricsHookMu.RLock()
	defer metricsHookMu.RUnlock()
	return metricsHook
}
