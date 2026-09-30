package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type AliasListResponse struct {
	Aliases []AliasEntry `json:"aliases"`
}

type Persister interface {
	SaveAliases(entries []AliasEntry) error
}

// RegistryWriter persists alias changes with conditional writes to the shared
// registry, so concurrent changes from other pods are merged, not overwritten.
type RegistryWriter interface {
	Register(ctx context.Context, e AliasEntry) error
	Unregister(ctx context.Context, orgID string) error
}

type Handler struct {
	resolver  *TenantResolver
	persister Persister
	registry  RegistryWriter
	authKey   string
}

// WithRegistry makes the handler persist through the shared registry instead
// of writing the whole alias set blind.
func (h *Handler) WithRegistry(rw RegistryWriter) *Handler {
	h.registry = rw
	return h
}

func NewHandler(r *TenantResolver, p Persister, authKey string) *Handler {
	return &Handler{resolver: r, persister: p, authKey: authKey}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/lakehouse/api/v1/tenants/aliases", h.handleAliases)
	mux.HandleFunc("/lakehouse/api/v1/tenants/aliases/", h.handleAliasDelete)
}

func (h *Handler) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	if h.authKey == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	if auth != "Bearer "+h.authKey {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return false
	}
	return true
}

func (h *Handler) handleAliases(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.listAliases(w, r)
	case http.MethodPost:
		if !h.checkAuth(w, r) {
			return
		}
		h.createAlias(w, r)
	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) listAliases(w http.ResponseWriter, _ *http.Request) {
	all := h.resolver.AllAliases()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(AliasListResponse{Aliases: all})
}

func (h *Handler) createAlias(w http.ResponseWriter, r *http.Request) {
	var entry AliasEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	tid := TenantID{AccountID: entry.AccountID, ProjectID: entry.ProjectID}
	if err := h.resolver.CheckAlias(entry.OrgID, tid); err != nil {
		h.aliasError(w, err)
		return
	}
	if h.registry != nil {
		entry.Source = SourceAPI
		if err := h.registry.Register(r.Context(), entry); err != nil {
			h.aliasError(w, err)
			return
		}
	}
	if err := h.resolver.AddAliasFrom(entry.OrgID, tid, SourceAPI); err != nil {
		h.aliasError(w, err)
		return
	}

	if h.registry == nil && h.persister != nil {
		if err := h.persister.SaveAliases(h.resolver.AllAliases()); err != nil {
			logger.Errorf("failed to persist tenant aliases: %s", err)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(entry)
}

// aliasError maps an alias failure to its status: 409 for a conflict (the
// mapping is refused, nothing changed), 503 when the shared registry cannot be
// reached, 400 otherwise.
func (h *Handler) aliasError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, ErrAliasConflict):
		status = http.StatusConflict
		metrics.TenantAliasRejectedTotal.Inc("admin")
		logger.Warnf("tenant alias API: %s", err)
	case errors.Is(err, ErrRegistryUnavailable), errors.Is(err, ErrAllocationConflict):
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func (h *Handler) handleAliasDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	if !h.checkAuth(w, r) {
		return
	}

	orgID := strings.TrimPrefix(r.URL.Path, "/lakehouse/api/v1/tenants/aliases/")
	if orgID == "" {
		http.Error(w, `{"error":"missing org_id"}`, http.StatusBadRequest)
		return
	}

	if h.registry != nil {
		if err := h.registry.Unregister(r.Context(), orgID); err != nil {
			h.aliasError(w, err)
			return
		}
	}
	h.resolver.RemoveAlias(orgID)

	if h.registry == nil && h.persister != nil {
		if err := h.persister.SaveAliases(h.resolver.AllAliases()); err != nil {
			logger.Errorf("failed to persist tenant aliases: %s", err)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
