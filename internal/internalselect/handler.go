package internalselect

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/protocol"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type Handler struct {
	store   storage.Storage
	logger  *slog.Logger
	timeout time.Duration
}

func NewHandler(store storage.Storage, logger *slog.Logger, timeout time.Duration) *Handler {
	return &Handler{
		store:   store,
		logger:  logger.With("component", "internalselect"),
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

	qctx, err := parseQueryContext(r)
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
		h.logger.Error("zstd encoder init", "error", err)
		return
	}
	defer func() { _ = enc.Close() }()

	writeBlock := func(_ uint, db *storage.DataBlock) {
		if err := protocol.WriteDataBlockStream(enc, db); err != nil {
			h.logger.Warn("write data block", "error", err)
		}
	}

	if err := h.store.RunQuery(ctx, qctx, writeBlock); err != nil {
		h.logger.Warn("query execution", "error", err)
	}
}

func (h *Handler) handleFieldNames(w http.ResponseWriter, r *http.Request) {
	qctx, err := parseQueryContext(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetFieldNames(ctx, qctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleFieldValues(w http.ResponseWriter, r *http.Request) {
	qctx, err := parseQueryContext(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fieldName := r.URL.Query().Get("field")
	limitStr := r.URL.Query().Get("limit")
	limit := 0
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetFieldValues(ctx, qctx, fieldName, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleStreamFieldNames(w http.ResponseWriter, r *http.Request) {
	qctx, err := parseQueryContext(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetStreamFieldNames(ctx, qctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleStreamFieldValues(w http.ResponseWriter, r *http.Request) {
	qctx, err := parseQueryContext(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fieldName := r.URL.Query().Get("field")

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetStreamFieldValues(ctx, qctx, fieldName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleStreams(w http.ResponseWriter, r *http.Request) {
	qctx, err := parseQueryContext(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetStreams(ctx, qctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleStreamIDs(w http.ResponseWriter, r *http.Request) {
	qctx, err := parseQueryContext(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	vals, err := h.store.GetStreamIDs(ctx, qctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeValueWithHitsResponse(w, vals)
}

func (h *Handler) handleTenantIDs(w http.ResponseWriter, r *http.Request) {
	qctx, err := parseQueryContext(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	ids, err := h.store.GetTenantIDs(ctx, qctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(protocol.MarshalTenantIDs(ids))
}

func (h *Handler) handleDeleteNoop(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func parseQueryContext(r *http.Request) (*storage.QueryContext, error) {
	q := r.URL.Query()

	startNs, _ := strconv.ParseInt(q.Get("start"), 10, 64)
	endNs, _ := strconv.ParseInt(q.Get("end"), 10, 64)

	if startNs == 0 && endNs == 0 {
		if body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)); err == nil && len(body) > 0 {
			return parseQueryContextFromBody(body, q)
		}
	}

	var tenantIDs []storage.TenantID
	if accountID := q.Get("AccountID"); accountID != "" {
		aid, _ := strconv.ParseUint(accountID, 10, 32)
		pid, _ := strconv.ParseUint(q.Get("ProjectID"), 10, 32)
		tenantIDs = append(tenantIDs, storage.TenantID{
			AccountID: uint32(aid),
			ProjectID: uint32(pid),
		})
	}

	query := q.Get("query")
	var requestedCols []string
	if cols := q.Get("columns"); cols != "" {
		requestedCols = strings.Split(cols, ",")
	}

	return &storage.QueryContext{
		TenantIDs:        tenantIDs,
		StartNs:          startNs,
		EndNs:            endNs,
		Query:            query,
		RequestedColumns: requestedCols,
	}, nil
}

func parseQueryContextFromBody(body []byte, q map[string][]string) (*storage.QueryContext, error) {
	if len(body) < 16 {
		return nil, fmt.Errorf("request body too short: %d bytes", len(body))
	}

	qctx := &storage.QueryContext{}

	pos := 0
	if pos+8 > len(body) {
		return qctx, nil
	}
	qctx.StartNs = int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	pos += 8

	if pos+8 > len(body) {
		return qctx, nil
	}
	qctx.EndNs = int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	pos += 8

	if pos+4 <= len(body) {
		queryLen := int(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if pos+queryLen <= len(body) {
			qctx.Query = string(body[pos : pos+queryLen])
		}
	}

	if cols := getFirst(q, "columns"); cols != "" {
		qctx.RequestedColumns = strings.Split(cols, ",")
	}

	return qctx, nil
}

func writeValueWithHitsResponse(w http.ResponseWriter, vals []storage.ValueWithHits) {
	w.Header().Set("Content-Type", "application/octet-stream")
	if vals == nil {
		vals = []storage.ValueWithHits{}
	}
	_, _ = w.Write(protocol.MarshalValueWithHits(vals))
}

func getFirst(q map[string][]string, key string) string {
	if vals, ok := q[key]; ok && len(vals) > 0 {
		return vals[0]
	}
	return ""
}
