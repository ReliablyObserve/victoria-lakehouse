package buffer

import (
	"encoding/json"
	"errors"
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

// TenantScopeVersion is the value the select side sends in the
// `tenant_scope` query parameter of /internal/buffer/query, and
// TenantScopeHeader is the response header this handler echoes the tenant it
// filtered to. Together they make a mixed-version fleet fail CLOSED: a peer
// running an older build ignores the parameter and sends no header, so the
// caller drops its rows instead of merging rows it cannot attribute to a
// tenant. Bump the version if the scoping contract ever changes shape.
const (
	TenantScopeVersion = "v1"
	TenantScopeHeader  = "X-Lakehouse-Tenant-Scope"
)

// Handler serves the internal buffer query endpoint, allowing select
// pods to read unflushed data from insert pods over HTTP.
type Handler struct {
	store   Querier
	authKey string
}

// NewHandler creates a handler backed by the given Querier.
// If authKey is non-empty, requests must include a matching
// Authorization: Bearer <key> header.
func NewHandler(store Querier, authKey string) *Handler {
	return &Handler{store: store, authKey: authKey}
}

// ServeHTTP streams matching rows as newline-delimited JSON.
//
// Query parameters: start (ns), end (ns), mode (logs|traces),
// account_id + project_id (the single tenant to answer for) and tenant_scope
// (the contract version, see TenantScopeVersion). The tenant parameters are
// REQUIRED: the buffer holds every tenant's unflushed rows, so answering
// without them would hand the caller rows it must not see. Requests that omit
// them are rejected rather than served unscoped.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.authKey != "" {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+h.authKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

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

	if r.URL.Query().Get("tenant_scope") != TenantScopeVersion {
		http.Error(w, "tenant_scope parameter required", http.StatusBadRequest)
		return
	}
	accountID, projectID, err := parseTenantParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	// Echo the tenant this answer is filtered to, so the caller can prove the
	// peer honoured the scope before merging the rows.
	w.Header().Set(TenantScopeHeader, strconv.FormatUint(uint64(accountID), 10)+":"+strconv.FormatUint(uint64(projectID), 10))

	enc := json.NewEncoder(w)
	switch mode {
	case "logs":
		for _, row := range h.store.BufferedLogRows(startNs, endNs) {
			if row.AccountID != accountID || row.ProjectID != projectID {
				continue
			}
			if err := enc.Encode(row); err != nil {
				return
			}
		}
	case "traces":
		for _, row := range h.store.BufferedTraceRows(startNs, endNs) {
			if row.AccountID != accountID || row.ProjectID != projectID {
				continue
			}
			if err := enc.Encode(row); err != nil {
				return
			}
		}
	default:
		http.Error(w, "mode must be logs or traces", http.StatusBadRequest)
	}
}

// parseTenantParams reads the single tenant this request may be answered for.
// Both parameters must be present and must parse as uint32, matching the
// AccountID/ProjectID pair VL derives from the request headers.
func parseTenantParams(r *http.Request) (accountID, projectID uint32, err error) {
	q := r.URL.Query()
	accountStr, projectStr := q.Get("account_id"), q.Get("project_id")
	if accountStr == "" || projectStr == "" {
		return 0, 0, errors.New("account_id and project_id parameters required")
	}
	a, err := strconv.ParseUint(accountStr, 10, 32)
	if err != nil {
		return 0, 0, errors.New("invalid account_id parameter")
	}
	p, err := strconv.ParseUint(projectStr, 10, 32)
	if err != nil {
		return 0, 0, errors.New("invalid project_id parameter")
	}
	return uint32(a), uint32(p), nil
}
