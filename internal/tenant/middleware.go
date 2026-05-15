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
		orgID := req.Header.Get(r.config.OrgIDHeader)
		if orgID == "" {
			next.ServeHTTP(w, req)
			return
		}

		tid, ok := r.Resolve(orgID)
		if !ok {
			if r.config.AutoRegister {
				newID := atomic.AddUint32(&autoIDCounter, 1)
				tid = TenantID{AccountID: newID, ProjectID: 0}
				_ = r.AddAlias(orgID, tid)
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

		req.Header.Set("X-Scope-AccountID", fmt.Sprintf("%d", tid.AccountID))
		req.Header.Set("X-Scope-ProjectID", fmt.Sprintf("%d", tid.ProjectID))
		req.Header.Del(r.config.OrgIDHeader)

		next.ServeHTTP(w, req)
	})
}
