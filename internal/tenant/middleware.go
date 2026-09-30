package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

const (
	// registerTimeout bounds one auto-registration, including its retries.
	registerTimeout = 10 * time.Second
	// readRefreshAge is how stale the registry view may be before a read of an
	// unknown OrgID re-reads it (another pod may have registered the name).
	readRefreshAge = time.Second
)

// writePathPrefixes are the routes that ingest data: the native VL/VT insert
// API (which carries the Loki, OTLP, Elasticsearch, syslog and Jaeger-style
// ingest endpoints), the internal insert between peers, and the Splunk HEC /
// Datadog-compatible logs endpoints the logs binary mounts at the root.
var writePathPrefixes = []string{
	"/insert/",
	"/internal/insert",
	"/api/v2/logs",
	"/services/collector/",
}

// IsWritePath reports whether a request ingests data. Only such a request may
// auto-register a tenant; every other route, including every select, Jaeger,
// Tempo and field API, only reads.
func IsWritePath(method, path string) bool {
	if method != http.MethodPost && method != http.MethodPut {
		return false
	}
	for _, p := range writePathPrefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

func writeJSONError(w http.ResponseWriter, status int, body map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

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

		write := IsWritePath(req.Method, req.URL.Path)

		// The null tenant is where unknown OrgIDs read from; nothing may be
		// written to it or a read of a made-up name would return that data.
		if write {
			if acc, err := strconv.ParseUint(req.Header.Get("AccountID"), 10, 32); err == nil && uint32(acc) == NullAccountID {
				writeJSONError(w, http.StatusBadRequest, map[string]string{
					"error": fmt.Sprintf("AccountID %d is reserved and cannot be written to", NullAccountID),
				})
				return
			}
		}

		orgID := req.Header.Get(r.config.OrgIDHeader)
		if orgID == "" {
			next.ServeHTTP(w, req)
			return
		}

		tid, ok := r.Resolve(orgID)
		if !ok {
			if !r.config.AutoRegister {
				writeJSONError(w, http.StatusBadRequest, map[string]string{
					"error":  "unknown tenant",
					"org_id": orgID,
				})
				return
			}
			if err := ValidateOrgID(orgID); err != nil {
				writeJSONError(w, http.StatusBadRequest, map[string]string{
					"error":  "invalid org ID: " + err.Error(),
					"org_id": orgID,
				})
				return
			}
			alloc := r.getAllocator()
			if write {
				if alloc == nil {
					writeJSONError(w, http.StatusServiceUnavailable, map[string]string{
						"error": "auto-register failed: no tenant registry",
					})
					return
				}
				ctx, cancel := context.WithTimeout(req.Context(), registerTimeout)
				newTID, err := alloc.Allocate(ctx, orgID)
				cancel()
				if err != nil {
					logger.Warnf("tenant auto-register of %q failed: %s", orgID, err)
					w.Header().Set("Retry-After", "1")
					writeJSONError(w, http.StatusServiceUnavailable, map[string]string{
						"error": "auto-register failed: tenant registry unavailable, retry",
					})
					return
				}
				tid = newTID
			} else {
				// A read never registers. Another pod may have registered the
				// name since this one last looked, so look again (rate
				// limited); if it is still unknown the request reads as a
				// tenant that holds no data, which answers exactly like an
				// int tenant with no data.
				if alloc != nil {
					alloc.RefreshIfStale(req.Context(), readRefreshAge)
				}
				if known, found := r.Resolve(orgID); found {
					tid = known
				} else {
					metrics.TenantUnknownOrgIDReadsTotal.Inc()
					tid = NullTenant
				}
			}
		}

		req.Header.Set("AccountID", fmt.Sprintf("%d", tid.AccountID))
		req.Header.Set("ProjectID", fmt.Sprintf("%d", tid.ProjectID))
		req.Header.Del(r.config.OrgIDHeader)

		next.ServeHTTP(w, req)
	})
}
