// internal/metrics/election_hook.go
//
// Implementation of election.MetricsHook backed by the VictoriaMetrics
// exporter (vmmetrics). Lives in internal/metrics so the heavy vmmetrics
// closure is NOT pulled into internal/election (which would blow the
// TestElectionDepCount limit at 340 packages).
//
// Wiring contract: main.go (lakehouse-logs and lakehouse-traces) calls
//
//	election.SetMetricsHook(metrics.NewElectionHook())
//
// exactly once during startup, before any K8sElector is constructed.
package metrics

import (
	"fmt"
	"sync"

	vmmetrics "github.com/VictoriaMetrics/metrics"
)

// ElectionHook implements election.MetricsHook by writing through to the
// lakehouse_leader_election_* metric families declared in lakehouse.go.
//
// The hook keeps a small in-memory map of "currently-active" lease-holder
// gauges so the SetLeaseHolder pattern (set new identity to 1, reset old
// identity to 0) works without pulling a fully-labeled CounterVec into the
// metrics layer.
type ElectionHook struct {
	mu             sync.Mutex
	currentHolders map[string]string // key: lease|module → identity currently set to 1
}

// NewElectionHook constructs the hook. Pass the result to
// election.SetMetricsHook from main.go's startup sequence.
func NewElectionHook() *ElectionHook {
	return &ElectionHook{currentHolders: make(map[string]string)}
}

// SetLeaderState toggles the {role,lease,module} state gauge between 0 and 1.
func (h *ElectionHook) SetLeaderState(role, lease, module string, leader bool) {
	v := int64(0)
	if leader {
		v = 1
	}
	key := fmt.Sprintf(`role=%q,lease=%q,module=%q`, role, lease, module)
	LeaderElectionState.Set(key, v)
}

func (h *ElectionHook) IncAcquire(lease, module string) {
	LeaderElectionAcquireTotal.Inc(fmt.Sprintf(`lease=%q,module=%q`, lease, module))
}

func (h *ElectionHook) IncRenew(lease, module, result string) {
	LeaderElectionRenewTotal.Inc(fmt.Sprintf(`lease=%q,module=%q,result=%q`, lease, module, result))
}

func (h *ElectionHook) IncRelease(lease, module string) {
	LeaderElectionReleaseTotal.Inc(fmt.Sprintf(`lease=%q,module=%q`, lease, module))
}

func (h *ElectionHook) ObserveAcquireDuration(_, _ string, seconds float64) {
	LeaderElectionAcquireDuration.Observe(seconds)
}

// SetLeaseHolder records the current holder. To make the gauge useful for
// "who holds the lease right now" alerts/dashboards, we reset the previous
// holder's gauge to 0 before setting the new one to 1.
func (h *ElectionHook) SetLeaseHolder(lease, module, identity string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := lease + "|" + module
	if prev, ok := h.currentHolders[key]; ok && prev != "" && prev != identity {
		// Set the previous holder's gauge to 0 so it doesn't appear "still
		// holding" on dashboards. We do this by writing through the same
		// labelled gauge.
		prevLabel := fmt.Sprintf(`lease=%q,module=%q,identity=%q`, lease, module, prev)
		vmmetrics.GetOrCreateGauge(fmt.Sprintf(`lakehouse_leader_election_lease_holder{%s}`, prevLabel), nil).Set(0)
	}
	h.currentHolders[key] = identity
	newLabel := fmt.Sprintf(`lease=%q,module=%q,identity=%q`, lease, module, identity)
	vmmetrics.GetOrCreateGauge(fmt.Sprintf(`lakehouse_leader_election_lease_holder{%s}`, newLabel), nil).Set(1)
}

func (h *ElectionHook) IncStartupError(lease, module string) {
	LeaderElectionStartupErrors.Inc(fmt.Sprintf(`lease=%q,module=%q`, lease, module))
}
