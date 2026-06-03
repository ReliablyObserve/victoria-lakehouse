package tenant

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// RateLimitMiddleware enforces per-tenant ingest rate caps as a
// pre-flight Content-Length check. Mounted AFTER the tenant.Middleware
// so AccountID / ProjectID headers are already populated.
//
// Limits enforced:
//   - bytes/sec: pre-flight Content-Length (rejects with 429 before
//     reading the body)
//   - rows/sec: not pre-flight (rows aren't known until VL parses the
//     request body); rejected post-parse via the cardinality gate at
//     the inserter, so a tenant over its row cap loses individual
//     rows rather than whole requests
//
// Surfaces standard X-RateLimit-* headers on rejection so clients can
// back off cleanly.
func RateLimitMiddleware(limiter *IngestRateLimiter) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if limiter == nil || limiter.policy == nil {
				next.ServeHTTP(w, req)
				return
			}

			// Read account/project from the headers tenant.Middleware
			// already populated. Missing headers = anonymous tenant 0:0
			// (treated by the limiter as unlimited unless explicitly
			// configured).
			accountID, _ := strconv.ParseUint(req.Header.Get("AccountID"), 10, 32)
			projectID, _ := strconv.ParseUint(req.Header.Get("ProjectID"), 10, 32)

			// Pre-flight bytes check via Content-Length. Requests
			// without a length (chunked, websocket, no body) skip the
			// pre-check and pay only at the inserter via cardinality
			// gate — acceptable for the MVP scope.
			bytes := req.ContentLength
			if bytes < 0 {
				bytes = 0
			}

			if !limiter.Allow(uint32(accountID), uint32(projectID), bytes, 0) {
				w.Header().Set("Content-Type", "application/json")
				eff := limiter.policy.For(uint32(accountID), uint32(projectID))
				if eff != nil && eff.MaxBytesPerSec > 0 {
					w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(eff.MaxBytesPerSec, 10))
					w.Header().Set("X-RateLimit-Remaining", "0")
					w.Header().Set("X-RateLimit-Reset", "1")
				}
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error":      "rate limit exceeded",
					"account_id": accountID,
					"project_id": projectID,
				})
				return
			}

			next.ServeHTTP(w, req)
		})
	}
}
