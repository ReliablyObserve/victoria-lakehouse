package lifecycle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// LifecycleInfo provides accessor functions for lifecycle endpoint handlers.
// Using function fields allows decoupling from concrete startup/peercache types.
type LifecycleInfo struct {
	GetPhase     func() string
	IsReady      func() bool
	IsDraining   func() bool
	GetRingState func() *RingState
	GetStaleness func() *StalenessInfo
}

// RingState describes the current peer ring membership.
type RingState struct {
	Members     []string `json:"members"`
	MemberCount int      `json:"member_count"`
	SelfAddr    string   `json:"self_addr"`
}

// StalenessInfo describes cache/WAL staleness for observability.
type StalenessInfo struct {
	StaleDetected     bool          `json:"stale_detected"`
	StalenessAge      time.Duration `json:"staleness_age"`
	WALReconciled     bool          `json:"wal_reconciled"`
	CacheRevalidated  bool          `json:"cache_revalidated"`
	ManifestTimestamp time.Time     `json:"manifest_timestamp"`
}

// HandleDrain returns a handler for POST /internal/lifecycle/drain.
// The preStop hook calls this to set the draining header for load balancers.
func HandleDrain(orch *ShutdownOrchestrator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		if orch == nil {
			http.Error(w, "no orchestrator", http.StatusInternalServerError)
			return
		}
		// Mark as draining — the actual shutdown is triggered by SIGTERM,
		// but preStop hook calls this to notify peers early.
		w.Header().Set("X-Lakehouse-Draining", "true")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "DRAINING")
	}
}

// HandleLifecycleReady returns a handler for GET /internal/lifecycle/ready.
// Returns 200 when ready and not draining, 503 otherwise.
func HandleLifecycleReady(info LifecycleInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			Ready    bool   `json:"ready"`
			Phase    string `json:"phase"`
			Draining bool   `json:"draining"`
		}{
			Ready:    info.IsReady(),
			Phase:    info.GetPhase(),
			Draining: info.IsDraining(),
		}

		w.Header().Set("Content-Type", "application/json")

		if resp.Draining {
			w.Header().Set("X-Lakehouse-Draining", "true")
			w.WriteHeader(http.StatusServiceUnavailable)
		} else if !resp.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		_ = json.NewEncoder(w).Encode(resp)
	}
}

// HandleLifecycleRing returns a handler for GET /internal/lifecycle/ring.
// Returns the current ring membership state as JSON.
func HandleLifecycleRing(info LifecycleInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var state *RingState
		if info.GetRingState != nil {
			state = info.GetRingState()
		}
		if state == nil {
			state = &RingState{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state)
	}
}

// HandleLifecycleStale returns a handler for GET /internal/lifecycle/stale.
// Returns staleness information for cache/WAL observability.
func HandleLifecycleStale(info LifecycleInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var stale *StalenessInfo
		if info.GetStaleness != nil {
			stale = info.GetStaleness()
		}
		if stale == nil {
			stale = &StalenessInfo{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stale)
	}
}
