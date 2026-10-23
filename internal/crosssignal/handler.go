package crosssignal

import (
	"encoding/json"
	"net/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
)

// MaxHintBodyBytes caps the JSON body of a hint. A hint carries trace IDs (32
// hex characters each) and a few numbers, and the sender batches at most
// ClientConfig.MaxBatch of them (default 100, about 4 KB); 4 MiB leaves room
// for about 100 000 IDs per request and keeps an unauthenticated peer port
// from buffering more.
const MaxHintBodyBytes = 4 << 20

// KeyHeader is the header a hint carries the cross_signal.auth_key in.
const KeyHeader = "X-Cross-Signal-Key"

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
	if r.Method != http.MethodPost {
		// Mutating route: POST only, so a forged GET cannot trigger it.
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var hint PrefetchHint
	r.Body = http.MaxBytesReader(w, r.Body, MaxHintBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&hint); err != nil {
		if peerauth.TooLarge(err) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
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
	if r.Method != http.MethodPost {
		// Mutating route: POST only, so a forged GET cannot trigger it.
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var hint EvictionHint
	r.Body = http.MaxBytesReader(w, r.Body, MaxHintBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&hint); err != nil {
		if peerauth.TooLarge(err) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
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

// checkAuth validates the X-Cross-Signal-Key header against the configured auth key
// (in constant time, through internal/peerauth).
// If no auth key is configured (empty string), all requests are allowed.
// Returns true if the request is authorized, false if it was rejected (and a 401 was written).
func (h *Handler) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	if !peerauth.Header(r, KeyHeader, h.authKey) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}
