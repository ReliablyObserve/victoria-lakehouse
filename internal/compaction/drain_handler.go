package compaction

import (
	"fmt"
	"net/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// DrainHandler returns an HTTP handler for POST /lakehouse/drain that
// invokes Scheduler.Drain() and responds when the in-flight set has
// settled (or DrainTimeout elapsed).
//
// Idempotent: subsequent POSTs after a successful drain return 200
// immediately because Scheduler.Drain itself is idempotent.
//
// The handler sets the X-Lakehouse-Draining response header so any
// load balancer or other pod fetching this endpoint immediately
// learns about the drain. Spec §11.1 + §11.2.
//
// If sched is nil (compaction disabled), the handler returns 503 so
// operators / preStop hooks can distinguish "no compactor" from
// "compactor refused to drain".
func DrainHandler(sched *Scheduler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		if sched == nil {
			w.Header().Set("X-Lakehouse-Draining", "false")
			http.Error(w, "compaction not enabled", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("X-Lakehouse-Draining", "true")
		logger.Infof("compaction: /lakehouse/drain invoked; initiating Drain()")
		sched.Drain()
		metrics.CompactionDraining.Set(1)

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "DRAINED")
	}
}
