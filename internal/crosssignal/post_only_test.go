package crosssignal

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The hint routes change state (prefetch queue, eviction priority) and their
// only client (Client) sends POST, so any other method is refused with a bare
// 405, after the auth check. The request carries a valid body: a refusal must
// come from the method, not from a parse error.
func TestHintRoutes_POSTOnly(t *testing.T) {
	for path, body := range map[string]string{
		"/internal/prefetch/hint":    `{"trace_ids":["a"],"source_signal":"logs"}`,
		"/internal/cache/evict-hint": `{"trace_ids":["a"],"source_signal":"logs"}`,
	} {
		prefetch, eviction := &mockPrefetchRouter{}, &mockEvictionHandler{}
		mux := http.NewServeMux()
		NewHandler(HandlerConfig{AuthKey: "secret", PrefetchRouter: prefetch, EvictionHandler: eviction}).Register(mux)

		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete} {
			req := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
			req.Header.Set("X-Cross-Signal-Key", "secret")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusMethodNotAllowed || rec.Body.Len() != 0 {
				t.Errorf("%s %s: got %d %q, want a bare 405", method, path, rec.Code, rec.Body.String())
			}
			// Unauthorized still answers first.
			req = httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
			rec = httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s %s without the key: got %d, want 401", method, path, rec.Code)
			}
		}
		if prefetch.enqueued.Load() != 0 || eviction.deprioritized.Load() != 0 {
			t.Errorf("%s: a refused request changed state (prefetch=%d evict=%d)", path, prefetch.enqueued.Load(), eviction.deprioritized.Load())
		}

		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(body)))
		req.Header.Set("X-Cross-Signal-Key", "secret")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("POST %s: got %d, want 200", path, rec.Code)
		}
	}
}
