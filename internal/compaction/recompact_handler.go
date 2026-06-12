package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// RecompactRequest is the body of POST /lakehouse/compaction/recompact.
type RecompactRequest struct {
	Partition string `json:"partition"`
	Level     int    `json:"level,omitempty"` // 0 → derive from the hints / partition max level
}

// RecompactHandler forces (re)compaction of a single partition — the manual trigger
// for the compaction hints (GET /lakehouse/api/v1/stats/compaction surfaces the
// candidates). POST only, JSON body {partition, level?}. Respects HRW ownership: a
// pod that does not own the partition returns 403 (so two pods never both rewrite the
// same partition). Runs synchronously and returns the compaction result.
func RecompactHandler(sched *Scheduler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		if sched == nil {
			http.Error(w, "compaction not enabled", http.StatusServiceUnavailable)
			return
		}
		var req RecompactRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON body: %v", err), http.StatusBadRequest)
			return
		}
		if req.Partition == "" {
			http.Error(w, "partition is required", http.StatusBadRequest)
			return
		}
		// Ownership gate — only the HRW owner may rewrite a partition.
		if !sched.OwnsPartition(req.Partition) {
			http.Error(w,
				fmt.Sprintf("not owner of partition %s (owner=%s)", req.Partition, sched.OwnerOf(req.Partition)),
				http.StatusForbidden)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
		defer cancel()
		result, err := sched.ForceCompactPartition(ctx, req.Partition, req.Level)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"partition":     req.Partition,
			"output_level":  result.OutputLevel,
			"input_files":   len(result.InputFiles),
			"output_files":  len(result.OutputFiles),
			"rows_merged":   result.RowsMerged,
			"bytes_written": result.BytesWritten,
		})
		logger.Infof("recompact API: partition=%s level=%d → output_level=%d rows=%d",
			req.Partition, req.Level, result.OutputLevel, result.RowsMerged)
	}
}
