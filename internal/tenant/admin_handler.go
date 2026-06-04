package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// AdminAuthConfig gates the admin endpoint. Reuses the existing
// global-read token surface so operators don't have to mint a new
// credential — admin = privileged = same key family.
type AdminAuthConfig struct {
	HeaderName  string // e.g. "X-Lakehouse-Global-Read"
	HeaderValue string
	BearerToken string
}

// AdminHandler exposes operator-only tenant management endpoints
// — for now just the bucket migrator. Mount it under
// /lakehouse/api/v1/admin/ and gate it with AdminAuthConfig.
type AdminHandler struct {
	migrator *Migrator
	auth     AdminAuthConfig
}

func NewAdminHandler(mig *Migrator, auth AdminAuthConfig) *AdminHandler {
	return &AdminHandler{migrator: mig, auth: auth}
}

// Register wires the admin routes. POST /tenant/migrate runs a
// bucket migration synchronously and returns the per-call summary.
func (h *AdminHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/lakehouse/api/v1/admin/tenant/migrate", h.handleMigrate)
}

func (h *AdminHandler) handleMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	if !h.authorize(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "admin auth required"})
		return
	}

	var req struct {
		// Accepted forms (one of): tenant_key="<acc>:<proj>" OR
		// explicit account_id+project_id. The string form mirrors
		// the rest of the per-tenant API surface.
		TenantKey    string `json:"tenant_key,omitempty"`
		AccountID    uint32 `json:"account_id,omitempty"`
		ProjectID    uint32 `json:"project_id,omitempty"`
		TargetBucket string `json:"target_bucket"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("decode body: %v", err),
		})
		return
	}

	acc, proj := req.AccountID, req.ProjectID
	if req.TenantKey != "" {
		a, p, err := ParseTenantKeyFromString(req.TenantKey)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		acc, proj = a, p
	}

	if req.TargetBucket == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "target_bucket required"})
		return
	}

	// Long-running operations would belong on a background queue;
	// for the MVP this runs synchronously — admins can poll the
	// stats endpoints to watch progress and the request will block
	// for the duration. Acceptable for the expected use case
	// (per-tenant migrations are infrequent admin events).
	ctx, cancel := context.WithTimeout(r.Context(), 30*60*1000*1000*1000) // 30m
	defer cancel()
	result := h.migrator.MigrateTenant(ctx, acc, proj, req.TargetBucket)

	w.Header().Set("Content-Type", "application/json")
	if result.FilesErrored > 0 && result.FilesMoved == 0 {
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(result)
}

// authorize returns true when the request satisfies the configured
// admin auth. An unconfigured auth (empty struct) means the admin
// endpoint is effectively disabled — every request returns 403.
// Operators MUST set at least one of HeaderValue / BearerToken to
// open it; this is conservative-by-default.
func (h *AdminHandler) authorize(r *http.Request) bool {
	if h.auth.HeaderName != "" && h.auth.HeaderValue != "" {
		if r.Header.Get(h.auth.HeaderName) == h.auth.HeaderValue {
			return true
		}
	}
	if h.auth.BearerToken != "" {
		const prefix = "Bearer "
		got := r.Header.Get("Authorization")
		if len(got) > len(prefix) && got[:len(prefix)] == prefix && got[len(prefix):] == h.auth.BearerToken {
			return true
		}
	}
	return false
}
