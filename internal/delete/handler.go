package delete

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/google/uuid"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ManifestQuerier abstracts manifest lookups to avoid direct manifest imports.
// A querier that also has TenantKeyParser (as the manifest does) lends the
// handler its key → tenant attribution; otherwise the default
// {AccountID}/{ProjectID} layout is assumed.
type ManifestQuerier interface {
	GetFilesForRange(startNs, endNs int64) []FileInfo
}

// FileInfo describes a single Parquet file in the manifest.
type FileInfo struct {
	Key       string
	Size      int64
	MinTimeNs int64
	MaxTimeNs int64
}

// Handler serves HTTP endpoints for delete operations.
type Handler struct {
	store    *TombstoneStore
	manifest ManifestQuerier
	detector *StorageClassDetector
	cfg      *config.DeleteConfig
	mode     string
	// globalRead validates the operator credential (see handler_scope.go).
	globalRead func(*http.Request) bool
}

// routePrefix is the URL prefix every delete route of this binary lives under.
func (h *Handler) routePrefix() string {
	if h.mode == "traces" {
		return "/delete/tracessql"
	}
	return "/delete/logsql"
}

// NewHandler creates a Handler with the given dependencies.
// Mode should be "logs" or "traces" and determines the URL prefix.
func NewHandler(store *TombstoneStore, manifest ManifestQuerier, detector *StorageClassDetector, cfg *config.DeleteConfig, mode string, opts ...HandlerOption) *Handler {
	if mode == "" {
		mode = "logs"
	}
	h := &Handler{
		store:    store,
		manifest: manifest,
		detector: detector,
		cfg:      cfg,
		mode:     mode,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// Register mounts all delete endpoints on the given ServeMux.
func (h *Handler) Register(mux *http.ServeMux) {
	prefix := h.routePrefix()
	mux.HandleFunc(prefix+"/delete", h.handleDelete)
	mux.HandleFunc(prefix+"/estimate", h.handleEstimate)
	mux.HandleFunc(prefix+"/tombstones", h.handleListTombstones)
	mux.HandleFunc(prefix+"/leftovers", h.handleLeftovers)
	mux.HandleFunc(prefix+"/tombstone/", h.handleTombstoneByID)
	mux.HandleFunc(prefix+"/verify", h.handleVerify)
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.cfg.Enabled {
		http.Error(w, "delete feature is disabled", http.StatusForbidden)
		return
	}
	caller, ok := h.callerOrError(w, r)
	if !ok {
		return
	}

	query := r.FormValue("query")
	if query == "" {
		http.Error(w, "missing required parameter: query", http.StatusBadRequest)
		return
	}

	startNs, err := strconv.ParseInt(r.FormValue("start"), 10, 64)
	if err != nil {
		http.Error(w, "invalid start parameter", http.StatusBadRequest)
		return
	}
	endNs, err := strconv.ParseInt(r.FormValue("end"), 10, 64)
	if err != nil {
		http.Error(w, "invalid end parameter", http.StatusBadRequest)
		return
	}

	mode := r.FormValue("mode")
	if mode == "" {
		mode = h.cfg.DefaultMode
	}
	switch mode {
	case "hide", "permanent", "auto":
	default:
		http.Error(w, "invalid mode: must be hide, permanent, or auto", http.StatusBadRequest)
		return
	}

	// The delete is the caller's: scoped to its tenant, over its objects.
	files := h.tenantFiles(caller.tenant, startNs, endNs)
	affectedKeys := make([]string, 0, len(files))
	for _, f := range files {
		affectedKeys = append(affectedKeys, f.Key)
	}

	ts := Tombstone{
		ID:           uuid.New().String(),
		Query:        query,
		StartNs:      startNs,
		EndNs:        endNs,
		AffectedKeys: affectedKeys,
		CreatedAt:    time.Now(),
		Mode:         mode,
		Tenants:      []TenantRef{caller.tenant},
	}

	// Reject before storing. An unenforceable tombstone that is accepted looks
	// to the user exactly like a successful delete that removed nothing.
	if err := ts.Validate(); err != nil {
		http.Error(w, "invalid delete request: "+err.Error(), http.StatusBadRequest)
		return
	}

	h.store.Add(ts)
	metrics.DeleteTombstonesTotal.Inc()
	h.store.updateActiveGauges()

	logger.Infof("tombstone created; id=%s, tenant=%s, query=%s, mode=%s, affected_files=%d", ts.ID, caller.tenant, query, mode, len(affectedKeys))

	writeJSON(w, http.StatusOK, map[string]any{
		"tombstone_id":   ts.ID,
		"tenant":         caller.tenant.String(),
		"affected_files": len(affectedKeys),
		"mode":           mode,
		"message":        "tombstone created successfully",
	})
}

func (h *Handler) handleEstimate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	caller, ok := h.callerOrError(w, r)
	if !ok {
		return
	}

	query := r.FormValue("query")
	if query == "" {
		http.Error(w, "missing required parameter: query", http.StatusBadRequest)
		return
	}

	startNs, err := strconv.ParseInt(r.FormValue("start"), 10, 64)
	if err != nil {
		http.Error(w, "invalid start parameter", http.StatusBadRequest)
		return
	}
	endNs, err := strconv.ParseInt(r.FormValue("end"), 10, 64)
	if err != nil {
		http.Error(w, "invalid end parameter", http.StatusBadRequest)
		return
	}

	// What a delete by this caller would act on: its tenant's objects.
	files := h.tenantFiles(caller.tenant, startNs, endNs)

	classMap := make(map[string]int)
	for _, f := range files {
		var class StorageClass
		if cached, ok := h.detector.GetCached(f.Key); ok {
			class = cached
		} else {
			ageHours := time.Since(time.Unix(0, f.MinTimeNs)).Hours()
			class = h.detector.DetectForKey(ageHours, f.Key)
		}
		classMap[string(class)]++
	}

	// Determine recommended mode based on storage classes.
	recommended := "hide"
	allRewritable := true
	for className := range classMap {
		sc := StorageClass(className)
		if !sc.CanRewrite() {
			allRewritable = false
			break
		}
	}
	if allRewritable && len(files) > 0 {
		recommended = "permanent"
	}

	autoBehavior := "hide data at query time"
	if allRewritable {
		autoBehavior = "rewrite files to permanently remove data"
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"tenant":           caller.tenant.String(),
		"affected_files":   len(files),
		"storage_classes":  classMap,
		"recommended_mode": recommended,
		"auto_behavior":    autoBehavior,
	})
}

func (h *Handler) handleListTombstones(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	caller, ok := h.callerOrError(w, r)
	if !ok {
		return
	}

	tombstones := caller.visibleTombstones(h.store.Active())
	writeJSON(w, http.StatusOK, map[string]any{
		// "tenant": the caller's own tombstones; "instance": every tombstone
		// (the validated global-read credential).
		"scope":      caller.scopeName(),
		"tombstones": tombstones,
		"count":      len(tombstones),
		// Whether what is listed would survive a pod loss: write-through
		// persistence armed, and how many records are still owed to S3
		// (only the local disk copy holds those).
		"persistence": map[string]any{
			"enabled":           h.store.PersistenceEnabled(),
			"pending_s3_writes": h.store.PendingS3Writes(),
		},
	})
}

func (h *Handler) handleTombstoneByID(w http.ResponseWriter, r *http.Request) {
	// Extract ID from path: {routePrefix}/tombstone/{id}. The prefix is the
	// mode's own — slicing the traces route with the logs prefix produced
	// "ne/<id>", so by-id lookups and un-deletes 404'd on the traces binary.
	id := strings.TrimPrefix(r.URL.Path, h.routePrefix()+"/tombstone/")
	if id == "" || id == r.URL.Path {
		http.Error(w, "missing tombstone id", http.StatusBadRequest)
		return
	}
	caller, ok := h.callerOrError(w, r)
	if !ok {
		return
	}
	// Another tenant's tombstone is "not found": its existence is not the
	// caller's to learn, and removing it would un-delete another tenant's rows.
	if ts, found := h.store.Get(id); found && !caller.sees(&ts) {
		http.Error(w, "tombstone not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		ts, ok := h.store.Get(id)
		if !ok {
			http.Error(w, "tombstone not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, ts)

	case http.MethodDelete:
		switch err := h.store.TryRemove(id); {
		case errors.Is(err, ErrTombstoneNotFound):
			http.Error(w, "tombstone not found", http.StatusNotFound)
			return
		case errors.Is(err, ErrRewriteInProgress):
			// The rewrite's record lives on this tombstone until its objects
			// are settled; the un-delete is refused rather than lose it.
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		h.store.updateActiveGauges()

		logger.Infof("tombstone removed; id=%s", id)
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "removed",
			"id":     id,
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	caller, ok := h.callerOrError(w, r)
	if !ok {
		return
	}

	query := r.FormValue("query")
	if query == "" {
		http.Error(w, "missing required parameter: query", http.StatusBadRequest)
		return
	}

	startNs, err := strconv.ParseInt(r.FormValue("start"), 10, 64)
	if err != nil {
		http.Error(w, "invalid start parameter", http.StatusBadRequest)
		return
	}
	endNs, err := strconv.ParseInt(r.FormValue("end"), 10, 64)
	if err != nil {
		http.Error(w, "invalid end parameter", http.StatusBadRequest)
		return
	}

	// Find the caller's tombstones that overlap the requested range and match
	// the query.
	candidates := caller.visibleTombstones(h.store.ForRange(startNs, endNs))
	var matchingIDs []string
	for _, ts := range candidates {
		if ts.Query == query || ts.Query == "*" {
			matchingIDs = append(matchingIDs, ts.ID)
		}
	}

	// Compute coverage: what fraction of the requested range is covered by matching tombstones.
	coverage := 0.0
	if len(matchingIDs) > 0 {
		// Simple coverage: if any tombstone fully covers the range, coverage is 1.0.
		// Otherwise, compute fraction covered.
		totalRange := endNs - startNs
		if totalRange <= 0 {
			coverage = 1.0
		} else {
			var covered int64
			for _, ts := range candidates {
				if ts.Query != query && ts.Query != "*" {
					continue
				}
				overlapStart := ts.StartNs
				if overlapStart < startNs {
					overlapStart = startNs
				}
				overlapEnd := ts.EndNs
				if overlapEnd > endNs {
					overlapEnd = endNs
				}
				if overlapEnd > overlapStart {
					covered += overlapEnd - overlapStart
				}
			}
			coverage = float64(covered) / float64(totalRange)
			if coverage > 1.0 {
				coverage = 1.0
			}
		}
	}

	metrics.DeleteVerifyTotal.Inc()

	writeJSON(w, http.StatusOK, map[string]any{
		"verified":      len(matchingIDs) > 0,
		"tombstone_ids": matchingIDs,
		"coverage":      coverage,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
