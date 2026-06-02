package tenant

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
)

var autoIDCounter uint32 = 1000

func (r *TenantResolver) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Accept the public "X-Scope-AccountID"/"X-Scope-ProjectID" header
		// pair as a synonym for VL/VT's native "AccountID"/"ProjectID".
		// The spec exposes the X-Scope-* form externally; upstream code
		// reads the unprefixed names. Translate here so both work.
		if a := req.Header.Get("X-Scope-AccountID"); a != "" && req.Header.Get("AccountID") == "" {
			req.Header.Set("AccountID", a)
		}
		if p := req.Header.Get("X-Scope-ProjectID"); p != "" && req.Header.Get("ProjectID") == "" {
			req.Header.Set("ProjectID", p)
		}

		orgID := req.Header.Get(r.config.OrgIDHeader)
		if orgID == "" {
			next.ServeHTTP(w, req)
			return
		}

		tid, ok := r.Resolve(orgID)
		if !ok {
			if r.config.AutoRegister {
				if err := ValidateOrgID(orgID); err != nil {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"error":  "invalid org ID: " + err.Error(),
						"org_id": orgID,
					})
					return
				}
				newID := atomic.AddUint32(&autoIDCounter, 1)
				tid = TenantID{AccountID: newID, ProjectID: 0}
				if err := r.AddAlias(orgID, tid); err != nil {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"error": "auto-register failed: " + err.Error(),
					})
					return
				}
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":  "unknown tenant",
					"org_id": orgID,
				})
				return
			}
		}

		req.Header.Set("AccountID", fmt.Sprintf("%d", tid.AccountID))
		req.Header.Set("ProjectID", fmt.Sprintf("%d", tid.ProjectID))
		req.Header.Del(r.config.OrgIDHeader)

		next.ServeHTTP(w, req)
	})
}
