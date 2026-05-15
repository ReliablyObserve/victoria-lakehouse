package delete

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/google/uuid"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ManifestQuerier abstracts manifest lookups to avoid direct manifest imports.
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
}

// NewHandler creates a Handler with the given dependencies.
// Mode should be "logs" or "traces" and determines the URL prefix.
func NewHandler(store *TombstoneStore, manifest ManifestQuerier, detector *StorageClassDetector, cfg *config.DeleteConfig, mode string) *Handler {
	if mode == "" {
		mode = "logs"
	}
	return &Handler{
		store:    store,
		manifest: manifest,
		detector: detector,
		cfg:      cfg,
		mode:     mode,
	}
}

// Register mounts all delete endpoints on the given ServeMux.
func (h *Handler) Register(mux *http.ServeMux) {
	prefix := "/delete/logsql"
	if h.mode == "traces" {
		prefix = "/delete/tracessql"
	}
	mux.HandleFunc(prefix+"/delete", h.handleDelete)
	mux.HandleFunc(prefix+"/estimate", h.handleEstimate)
	mux.HandleFunc(prefix+"/tombstones", h.handleListTombstones)
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

	files := h.manifest.GetFilesForRange(startNs, endNs)
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
	}

	h.store.Add(ts)
	metrics.DeleteTombstonesTotal.Inc()
	metrics.DeleteTombstonesActive.Set(int64(h.store.Count()))

	logger.Infof("tombstone created; id=%s, query=%s, mode=%s, affected_files=%d", ts.ID, query, mode, len(affectedKeys))

	writeJSON(w, http.StatusOK, map[string]any{
		"tombstone_id":   ts.ID,
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

	files := h.manifest.GetFilesForRange(startNs, endNs)

	classMap := make(map[string]int)
	for _, f := range files {
		var class StorageClass
		if cached, ok := h.detector.GetCached(f.Key); ok {
			class = cached
		} else {
			ageHours := time.Since(time.Unix(0, f.MinTimeNs)).Hours()
			class = h.detector.Detect(ageHours)
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

	tombstones := h.store.Active()
	writeJSON(w, http.StatusOK, map[string]any{
		"tombstones": tombstones,
		"count":      len(tombstones),
	})
}

func (h *Handler) handleTombstoneByID(w http.ResponseWriter, r *http.Request) {
	// Extract ID from path: /delete/logsql/tombstone/{id}
	const prefix = "/delete/logsql/tombstone/"
	id := r.URL.Path[len(prefix):]
	if id == "" {
		http.Error(w, "missing tombstone id", http.StatusBadRequest)
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
		_, ok := h.store.Get(id)
		if !ok {
			http.Error(w, "tombstone not found", http.StatusNotFound)
			return
		}
		h.store.Remove(id)
		metrics.DeleteTombstonesActive.Set(int64(h.store.Count()))

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

	// Find tombstones that overlap with the requested range and match the query.
	candidates := h.store.ForRange(startNs, endNs)
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
