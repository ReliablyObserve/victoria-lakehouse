package buffer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Selection is the tenant selection of one buffer query: one tenant, or every
// tenant for a cross-tenant read the select pod has already authorised.
type Selection struct {
	All                  bool
	AccountID, ProjectID uint32
}

// Answer is what an insert pod's buffer returns for one query: the rows of its
// live segments and those segments' nonces.
type Answer struct {
	Nonces []string
	Logs   []schema.LogRow
	Traces []schema.TraceRow
}

// Source is the insert pod's buffer as the bridge serves it. Each binary
// provides it over its own segments (parquets3.BridgeSource), because the
// rows are converted with that binary's VictoriaLogs pin.
type Source interface {
	ReadBuffer(ctx context.Context, sel Selection, startNs, endNs int64, mode string) (Answer, error)
}

// SegmentsHeader is the response header listing the nonces of the segments an
// answer was read from, comma-separated. The select pod drops the objects
// flushed from those segments from its scan, so no row is answered twice.
const SegmentsHeader = "X-Lakehouse-Buffer-Segments"

// ParseSegments reads SegmentsHeader.
func ParseSegments(h string) []string {
	if h == "" {
		return nil
	}
	return strings.Split(h, ",")
}

// TenantScopeVersion is the value the select side sends in the
// `tenant_scope` query parameter of /internal/buffer/query, and
// TenantScopeHeader is the response header this handler echoes the tenant it
// filtered to ("<account>:<project>", or AllTenantsScope for a cross-tenant
// answer). Together they make a mixed-version fleet fail CLOSED: a peer
// running an older build ignores the parameter and sends no header, so the
// caller drops its rows instead of merging rows it cannot attribute to a
// tenant. Bump the version if the scoping contract ever changes shape.
const (
	TenantScopeVersion = "v1"
	TenantScopeHeader  = "X-Lakehouse-Tenant-Scope"
	// AllTenantsScope is echoed for an all_tenants=true request — sent only by
	// a select pod answering a query that presented a valid global-read
	// credential.
	AllTenantsScope = "*"
)

// Handler serves the internal buffer query endpoint, allowing select
// pods to read unflushed data from insert pods over HTTP.
type Handler struct {
	store   Source
	authKey string
}

// NewHandler creates a handler backed by the given Querier.
// If authKey is non-empty, requests must include a matching
// Authorization: Bearer <key> header.
func NewHandler(store Source, authKey string) *Handler {
	return &Handler{store: store, authKey: authKey}
}

// ServeHTTP streams matching rows as newline-delimited JSON.
//
// Query parameters: start (ns), end (ns), mode (logs|traces), tenant_scope
// (the contract version, see TenantScopeVersion) and EITHER account_id +
// project_id (the single tenant to answer for) OR all_tenants=true (a
// cross-tenant read the select pod has already authorised with the global-read
// credential). The tenant selection is REQUIRED: the buffer holds every
// tenant's unflushed rows, so a request that names no tenant is rejected
// rather than served unscoped.
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
	sel, err := parseTenantSelection(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if mode != "logs" && mode != "traces" {
		http.Error(w, "mode must be logs or traces", http.StatusBadRequest)
		return
	}

	ans, err := h.store.ReadBuffer(r.Context(), Selection{All: sel.all, AccountID: sel.accountID, ProjectID: sel.projectID}, startNs, endNs, mode)
	if err != nil {
		http.Error(w, "read the insert buffer: "+err.Error(), http.StatusInternalServerError)
		return
	}
	sort.Strings(ans.Nonces)
	w.Header().Set("Content-Type", "application/x-ndjson")
	// Echo the tenant this answer is filtered to, so the caller can prove the
	// peer honoured the scope before merging the rows.
	w.Header().Set(TenantScopeHeader, sel.String())
	w.Header().Set(SegmentsHeader, strings.Join(ans.Nonces, ","))

	enc := json.NewEncoder(w)
	if mode == "logs" {
		for _, row := range ans.Logs {
			if !sel.owns(row.AccountID, row.ProjectID) {
				continue
			}
			if err := enc.Encode(row); err != nil {
				return
			}
		}
		return
	}
	for _, row := range ans.Traces {
		if !sel.owns(row.AccountID, row.ProjectID) {
			continue
		}
		if err := enc.Encode(row); err != nil {
			return
		}
	}
}

// tenantSelection is the set of tenants one buffer query may be answered for:
// exactly one tenant, or every tenant for an authorised cross-tenant read.
type tenantSelection struct {
	all                  bool
	accountID, projectID uint32
}

func (s tenantSelection) owns(accountID, projectID uint32) bool {
	return s.all || (accountID == s.accountID && projectID == s.projectID)
}

func (s tenantSelection) String() string {
	if s.all {
		return AllTenantsScope
	}
	return strconv.FormatUint(uint64(s.accountID), 10) + ":" + strconv.FormatUint(uint64(s.projectID), 10)
}

// parseTenantSelection reads the tenant selection of a buffer query. Exactly
// one form must be present: all_tenants=true, or account_id + project_id that
// parse as uint32 (the AccountID/ProjectID pair VL derives from the request
// headers). Mixing the forms is rejected so a caller cannot be ambiguous about
// what it asked for.
func parseTenantSelection(r *http.Request) (tenantSelection, error) {
	q := r.URL.Query()
	accountStr, projectStr, allStr := q.Get("account_id"), q.Get("project_id"), q.Get("all_tenants")
	if allStr != "" {
		if allStr != "true" {
			return tenantSelection{}, errors.New("all_tenants must be true when present")
		}
		if accountStr != "" || projectStr != "" {
			return tenantSelection{}, errors.New("all_tenants cannot be combined with account_id/project_id")
		}
		return tenantSelection{all: true}, nil
	}
	if accountStr == "" || projectStr == "" {
		return tenantSelection{}, errors.New("account_id and project_id parameters required")
	}
	a, err := strconv.ParseUint(accountStr, 10, 32)
	if err != nil {
		return tenantSelection{}, errors.New("invalid account_id parameter")
	}
	p, err := strconv.ParseUint(projectStr, 10, 32)
	if err != nil {
		return tenantSelection{}, errors.New("invalid project_id parameter")
	}
	return tenantSelection{accountID: uint32(a), projectID: uint32(p)}, nil
}
