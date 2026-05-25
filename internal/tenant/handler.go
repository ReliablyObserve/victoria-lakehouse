package tenant

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type AliasListResponse struct {
	Aliases []AliasEntry `json:"aliases"`
}

type Persister interface {
	SaveAliases(entries []AliasEntry) error
}

type Handler struct {
	resolver  *TenantResolver
	persister Persister
	authKey   string
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

	if err := h.resolver.AddAlias(entry.OrgID, TenantID{AccountID: entry.AccountID, ProjectID: entry.ProjectID}); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	if h.persister != nil {
		if err := h.persister.SaveAliases(h.resolver.AllAliases()); err != nil {
			logger.Errorf("failed to persist tenant aliases: %s", err)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(entry)
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

	h.resolver.RemoveAlias(orgID)

	if h.persister != nil {
		if err := h.persister.SaveAliases(h.resolver.AllAliases()); err != nil {
			logger.Errorf("failed to persist tenant aliases: %s", err)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
