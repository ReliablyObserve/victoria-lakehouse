package internalselect

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/klauspost/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/protocol"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type Handler struct {
	store   storage.Storage
	timeout time.Duration
}

func NewHandler(store storage.Storage, timeout time.Duration) *Handler {
	return &Handler{
		store:   store,
		timeout: timeout,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/internal/select/query", h.handleQuery)
	mux.HandleFunc("/internal/select/field_names", h.handleFieldNames)
	mux.HandleFunc("/internal/select/field_values", h.handleFieldValues)
	mux.HandleFunc("/internal/select/stream_field_names", h.handleStreamFieldNames)
	mux.HandleFunc("/internal/select/stream_field_values", h.handleStreamFieldValues)
	mux.HandleFunc("/internal/select/streams", h.handleStreams)
	mux.HandleFunc("/internal/select/stream_ids", h.handleStreamIDs)
	mux.HandleFunc("/internal/select/tenant_ids", h.handleTenantIDs)
	mux.HandleFunc("/internal/select/delete_run", h.handleDeleteNoop)
	mux.HandleFunc("/internal/select/delete_stop", h.handleDeleteNoop)
	mux.HandleFunc("/internal/select/delete_active_tasks", h.handleDeleteNoop)
}

func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tenantIDs, q, err := parseInternalQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	w.Header().Set("Content-Encoding", "zstd")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)

	enc, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		logger.Errorf("zstd encoder init: %s", err)
		return
	}
	defer func() { _ = enc.Close() }()

	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		if err := protocol.WriteDataBlockStream(enc, db); err != nil {
			logger.Warnf("write data block; error=%s", err)
		}
	}

	if err := h.store.RunQuery(ctx, tenantIDs, q, writeBlock); err != nil {
		logger.Warnf("query execution; error=%s", err)
	}
}

func (h *Handler) handleFieldNames(w http.ResponseWriter, r *http.Request) {
	tenantIDs, q, err := parseInternalQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetFieldNames(ctx, tenantIDs, q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleFieldValues(w http.ResponseWriter, r *http.Request) {
	tenantIDs, q, err := parseInternalQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fieldName := r.URL.Query().Get("field")
	var limit uint64
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, _ = strconv.ParseUint(limitStr, 10, 64)
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetFieldValues(ctx, tenantIDs, q, fieldName, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleStreamFieldNames(w http.ResponseWriter, r *http.Request) {
	tenantIDs, q, err := parseInternalQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetStreamFieldNames(ctx, tenantIDs, q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleStreamFieldValues(w http.ResponseWriter, r *http.Request) {
	tenantIDs, q, err := parseInternalQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fieldName := r.URL.Query().Get("field")
	var limit uint64
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, _ = strconv.ParseUint(limitStr, 10, 64)
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetStreamFieldValues(ctx, tenantIDs, q, fieldName, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleStreams(w http.ResponseWriter, r *http.Request) {
	tenantIDs, q, err := parseInternalQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var limit uint64
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, _ = strconv.ParseUint(limitStr, 10, 64)
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetStreams(ctx, tenantIDs, q, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleStreamIDs(w http.ResponseWriter, r *http.Request) {
	tenantIDs, q, err := parseInternalQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var limit uint64
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		limit, _ = strconv.ParseUint(limitStr, 10, 64)
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetStreamIDs(ctx, tenantIDs, q, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleTenantIDs(w http.ResponseWriter, _ *http.Request) {
	// GetTenantIDs is not part of the storage.Storage interface;
	// return an empty list for now.
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(protocol.MarshalTenantIDs(nil))
}

func (h *Handler) handleDeleteNoop(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// parseInternalQuery extracts tenant IDs and a logstorage.Query from the request.
// It reads time range and query string from URL params or binary body.
func parseInternalQuery(r *http.Request) ([]logstorage.TenantID, *logstorage.Query, error) {
	params := r.URL.Query()

	startNs, _ := strconv.ParseInt(params.Get("start"), 10, 64)
	endNs, _ := strconv.ParseInt(params.Get("end"), 10, 64)

	queryStr := params.Get("query")

	if startNs == 0 && endNs == 0 {
		if body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)); err == nil && len(body) > 0 {
			s, e, qs := parseQueryFromBody(body)
			startNs = s
			endNs = e
			if qs != "" {
				queryStr = qs
			}
		}
	}

	var tenantIDs []logstorage.TenantID
	if accountID := params.Get("AccountID"); accountID != "" {
		aid, _ := strconv.ParseUint(accountID, 10, 32)
		pid, _ := strconv.ParseUint(params.Get("ProjectID"), 10, 32)
		tenantIDs = append(tenantIDs, logstorage.TenantID{
			AccountID: uint32(aid),
			ProjectID: uint32(pid),
		})
	}

	if queryStr == "" {
		queryStr = "*"
	}
	q, err := logstorage.ParseQuery(queryStr)
	if err != nil {
		return nil, nil, fmt.Errorf("parse query: %w", err)
	}
	if startNs != 0 || endNs != 0 {
		q.AddTimeFilter(startNs, endNs)
	}

	return tenantIDs, q, nil
}

// parseQueryFromBody extracts startNs, endNs, and query string from binary body.
func parseQueryFromBody(body []byte) (startNs, endNs int64, query string) {
	if len(body) < 16 {
		return 0, 0, ""
	}

	pos := 0
	startNs = int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	pos += 8
	endNs = int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	pos += 8

	if pos+4 <= len(body) {
		queryLen := int(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if pos+queryLen <= len(body) {
			query = string(body[pos : pos+queryLen])
		}
	}

	return startNs, endNs, query
}

func writeValueWithHitsResponse(w http.ResponseWriter, vals []logstorage.ValueWithHits) {
	w.Header().Set("Content-Type", "application/octet-stream")
	if vals == nil {
		vals = []logstorage.ValueWithHits{}
	}
	_, _ = w.Write(protocol.MarshalValueWithHits(vals))
}

func getFirst(q map[string][]string, key string) string {
	if vals, ok := q[key]; ok && len(vals) > 0 {
		return vals[0]
	}
	return ""
}
