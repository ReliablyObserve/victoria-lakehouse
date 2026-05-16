package buffer

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Querier provides read access to unflushed in-memory rows.
// BatchWriter satisfies this interface.
type Querier interface {
	BufferedLogRows(startNs, endNs int64) []schema.LogRow
	BufferedTraceRows(startNs, endNs int64) []schema.TraceRow
}

// Handler serves the internal buffer query endpoint, allowing select
// pods to read unflushed data from insert pods over HTTP.
type Handler struct {
	store Querier
}

// NewHandler creates a handler backed by the given Querier.
func NewHandler(store Querier) *Handler {
	return &Handler{store: store}
}

// ServeHTTP streams matching rows as newline-delimited JSON.
// Query parameters: start (ns), end (ns), mode (logs|traces).
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	mode := r.URL.Query().Get("mode")

	if startStr == "" || endStr == "" || mode == "" {
		http.Error(w, "start, end, and mode parameters required", http.StatusBadRequest)
		return
	}

	startNs, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid start parameter", http.StatusBadRequest)
		return
	}
	endNs, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid end parameter", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")

	enc := json.NewEncoder(w)
	switch mode {
	case "logs":
		for _, row := range h.store.BufferedLogRows(startNs, endNs) {
			if err := enc.Encode(row); err != nil {
				return
			}
		}
	case "traces":
		for _, row := range h.store.BufferedTraceRows(startNs, endNs) {
			if err := enc.Encode(row); err != nil {
				return
			}
		}
	default:
		http.Error(w, "mode must be logs or traces", http.StatusBadRequest)
	}
}
