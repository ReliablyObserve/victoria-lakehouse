package crosssignal

import (
	"encoding/json"
	"net/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// PrefetchRouter enqueues cross-signal prefetch work.
type PrefetchRouter interface {
	EnqueueCrossSignal(keys []string) int
}

// EvictionRouter deprioritizes cache entries by trace ID.
type EvictionRouter interface {
	DeprioritizeByTraceIDs(traceIDs []string) int
}

// HandlerConfig holds configuration for the cross-signal HTTP handler.
type HandlerConfig struct {
	AuthKey         string
	PrefetchRouter  PrefetchRouter
	EvictionHandler EvictionRouter
}

// Handler serves the cross-signal HTTP endpoints for prefetch and eviction hints.
type Handler struct {
	authKey         string
	prefetchRouter  PrefetchRouter
	evictionHandler EvictionRouter
}

// NewHandler creates a new Handler from the given config.
func NewHandler(cfg HandlerConfig) *Handler {
	return &Handler{
		authKey:         cfg.AuthKey,
		prefetchRouter:  cfg.PrefetchRouter,
		evictionHandler: cfg.EvictionHandler,
	}
}

// Register adds the cross-signal routes to the provided mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/internal/prefetch/hint", h.HandlePrefetchHint)
	mux.HandleFunc("/internal/cache/evict-hint", h.HandleEvictHint)
}

// HandlePrefetchHint handles POST /internal/prefetch/hint.
func (h *Handler) HandlePrefetchHint(w http.ResponseWriter, r *http.Request) {
	if !h.checkAuth(w, r) {
		return
	}

	var hint PrefetchHint
	if err := json.NewDecoder(r.Body).Decode(&hint); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	metrics.CrossPrefetchReceived.Inc()

	if len(hint.TraceIDs) > 0 && h.prefetchRouter != nil {
		h.prefetchRouter.EnqueueCrossSignal(hint.TraceIDs)
	}

	w.WriteHeader(http.StatusOK)
}

// HandleEvictHint handles POST /internal/cache/evict-hint.
func (h *Handler) HandleEvictHint(w http.ResponseWriter, r *http.Request) {
	if !h.checkAuth(w, r) {
		return
	}

	var hint EvictionHint
	if err := json.NewDecoder(r.Body).Decode(&hint); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	metrics.CrossEvictionReceived.Inc()

	if len(hint.TraceIDs) > 0 && h.evictionHandler != nil {
		n := h.evictionHandler.DeprioritizeByTraceIDs(hint.TraceIDs)
		metrics.CrossEvictionApplied.Add(n)
	}

	w.WriteHeader(http.StatusOK)
}

// checkAuth validates the X-Cross-Signal-Key header against the configured auth key.
// If no auth key is configured (empty string), all requests are allowed.
// Returns true if the request is authorized, false if it was rejected (and a 401 was written).
func (h *Handler) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	if h.authKey == "" {
		return true
	}
	if r.Header.Get("X-Cross-Signal-Key") != h.authKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}
