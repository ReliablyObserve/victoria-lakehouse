package buffer

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

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
	// credential, and served only to a caller that presented the peer key.
	AllTenantsScope = "*"
)

// Path is the route of the endpoint.
const Path = "/internal/buffer/query"

// InternalSelectDisableFlag is upstream's -internalselect.disable. VictoriaLogs
// and VictoriaTraces use it to turn off /internal/select/*, the protocol other
// nodes read a node's data with; this endpoint is the Lakehouse's other such
// protocol (select pods read an insert pod's unflushed rows with it), so the
// same flag turns it off. The flag is registered by VictoriaLogs' vlselect
// package in the logs binary and by lakehouse-traces in the traces binary
// (VictoriaTraces registers it in vtselect, which that binary cannot link).
const InternalSelectDisableFlag = "internalselect.disable"

// InternalSelectDisabled reports -internalselect.disable as registered in this
// binary. A binary that has not registered the flag reports false.
func InternalSelectDisabled() bool {
	f := flag.Lookup(InternalSelectDisableFlag)
	return f != nil && f.Value.String() == "true"
}

// DisabledMessage is the answer of a route turned off by
// -internalselect.disable, in upstream's words: VictoriaLogs and
// VictoriaTraces answer "requests to /internal/select/* are disabled with
// -internalselect.disable command-line flag" through httpserver.Errorf (400).
func DisabledMessage(route string) string {
	return "requests to " + route + " are disabled with -" + InternalSelectDisableFlag + " command-line flag"
}

// Gate answers every request to route with upstream's "disabled" error while
// disabled reports true, and hands it to next otherwise. It runs before
// anything else, as upstream's flag check does, so a disabled route reveals
// nothing about its method, credentials or parameters.
func Gate(route string, disabled func() bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if disabled() {
			httpserver.Errorf(w, r, "%s", DisabledMessage(route))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// The answers to a request the peer key does not admit. The status is
// upstream's for a missing or wrong key (VictoriaMetrics'
// httpserver.CheckAuthFlag answers 401), and the wording follows its
// "Expected to receive non-empty authKey when -X is set" / "The provided
// authKey doesn't match -X". The key travels in the Authorization header, not
// in an authKey query argument: the bridge asks on every query, and a query
// string ends up in request logs and in Go's url.Error text.
const (
	MissingKeyMessage = "Expected to receive non-empty Authorization: Bearer <key> when peer.auth_key is set"
	WrongKeyMessage   = "The provided Bearer key doesn't match peer.auth_key"
	// AllTenantsRefusedMessage answers all_tenants=true on a pod without a
	// peer key: the cross-tenant read is served only to a caller that proved
	// it is a peer, and without a key no caller can.
	AllTenantsRefusedMessage = "all_tenants=true is served only to an authenticated peer; set the same peer.auth_key on every pod"
)

// Handler serves the internal buffer query endpoint, allowing select
// pods to read unflushed data from insert pods over HTTP.
type Handler struct {
	store   Source
	authKey string
}

// NewHandler creates a handler backed by the given Querier.
//
// If authKey (peer.auth_key) is non-empty, every request must carry
// Authorization: Bearer <authKey>; a missing or different key is refused with
// 401. If authKey is empty the endpoint answers single-tenant requests without
// a credential, as upstream's /internal/select/* does, and refuses
// all_tenants=true with 403: every tenant's rows go only to a proven peer.
func NewHandler(store Source, authKey string) *Handler {
	return &Handler{store: store, authKey: authKey}
}

// authorized checks the peer key. It writes the refusal and returns false when
// the request may not be served.
func (h *Handler) authorized(w http.ResponseWriter, r *http.Request) bool {
	if h.authKey == "" {
		return true
	}
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || got == "" {
		http.Error(w, MissingKeyMessage, http.StatusUnauthorized)
		return false
	}
	if subtle.ConstantTimeCompare([]byte(got), []byte(h.authKey)) != 1 {
		http.Error(w, WrongKeyMessage, http.StatusUnauthorized)
		return false
	}
	return true
}

// ServeHTTP streams matching rows as newline-delimited JSON.
//
// Query parameters: start (ns), end (ns), mode (logs|traces), tenant_scope
// (the contract version, see TenantScopeVersion) and EITHER account_id +
// project_id (the single tenant to answer for) OR all_tenants=true (a
// cross-tenant read the select pod has already authorised with the global-read
// credential; served only with a valid peer key). The tenant selection is
// REQUIRED: the buffer holds every tenant's unflushed rows, so a request that
// names no tenant is rejected rather than served unscoped.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !h.authorized(w, r) {
		return
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
	// Every tenant's rows go only to a caller that proved it is a peer. With
	// a key configured, authorized has checked it above; without one, nobody
	// can prove it.
	if sel.all && h.authKey == "" {
		http.Error(w, AllTenantsRefusedMessage, http.StatusForbidden)
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
