package crosssignal

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Both hint endpoints refuse a missing or wrong key (same length, shorter,
// longer) with 401 before they read the body, and serve the right key.
func TestHandler_KeyTable(t *testing.T) {
	const key = "cross-signal-secret"
	for _, route := range []struct {
		path string
		call func(*Handler, http.ResponseWriter, *http.Request)
	}{
		{"/internal/prefetch/hint", (*Handler).HandlePrefetchHint},
		{"/internal/cache/evict-hint", (*Handler).HandleEvictHint},
	} {
		for _, tc := range []struct {
			name, header string
			want         int
		}{
			{"right key", key, http.StatusOK},
			{"no key", "", http.StatusUnauthorized},
			{"wrong key, same length", "x" + key[1:], http.StatusUnauthorized},
			{"shorter key", key[:5], http.StatusUnauthorized},
			{"longer key", key + "x", http.StatusUnauthorized},
		} {
			prefetch, evict := &mockPrefetchRouter{}, &mockEvictionHandler{}
			h := NewHandler(HandlerConfig{AuthKey: key, PrefetchRouter: prefetch, EvictionHandler: evict})
			req := httptest.NewRequest(http.MethodPost, route.path, strings.NewReader(`{"trace_ids":["t1"]}`))
			if tc.header != "" {
				req.Header.Set(KeyHeader, tc.header)
			}
			w := httptest.NewRecorder()
			route.call(h, w, req)
			if w.Code != tc.want {
				t.Errorf("%s, %s: %d, want %d", route.path, tc.name, w.Code, tc.want)
			}
			if tc.want == http.StatusUnauthorized && (prefetch.enqueued.Load() != 0 || evict.deprioritized.Load() != 0) {
				t.Errorf("%s, %s: a refused hint was applied", route.path, tc.name)
			}
		}
		// no key configured: everything is allowed, as before
		h := NewHandler(HandlerConfig{PrefetchRouter: &mockPrefetchRouter{}, EvictionHandler: &mockEvictionHandler{}})
		w := httptest.NewRecorder()
		route.call(h, w, httptest.NewRequest(http.MethodPost, route.path, strings.NewReader(`{}`)))
		if w.Code != http.StatusOK {
			t.Errorf("%s without a configured key: %d, want 200", route.path, w.Code)
		}
	}
}

// hintBody is a valid hint of exactly n bytes: the JSON decoder ignores the
// padding field.
func hintBody(n int) string {
	const head, tail = `{"trace_ids":["t1"],"source_signal":"`, `"}`
	return head + strings.Repeat("a", n-len(head)-len(tail)) + tail
}

// The body is capped: a body at the cap is served, one byte over is 413 and
// applies nothing, on both endpoints. A malformed body is still 400.
func TestHandler_BodyCap(t *testing.T) {
	for _, route := range []struct {
		path string
		call func(*Handler, http.ResponseWriter, *http.Request)
	}{
		{"/internal/prefetch/hint", (*Handler).HandlePrefetchHint},
		{"/internal/cache/evict-hint", (*Handler).HandleEvictHint},
	} {
		for _, tc := range []struct {
			name string
			body string
			want int
		}{
			{"at the cap", hintBody(MaxHintBodyBytes), http.StatusOK},
			{"one byte over", hintBody(MaxHintBodyBytes + 1), http.StatusRequestEntityTooLarge},
			{"far over", hintBody(MaxHintBodyBytes * 3), http.StatusRequestEntityTooLarge},
			{"not JSON", "not json", http.StatusBadRequest},
		} {
			prefetch, evict := &mockPrefetchRouter{}, &mockEvictionHandler{}
			h := NewHandler(HandlerConfig{AuthKey: "k", PrefetchRouter: prefetch, EvictionHandler: evict})
			req := httptest.NewRequest(http.MethodPost, route.path, strings.NewReader(tc.body))
			req.Header.Set(KeyHeader, "k")
			w := httptest.NewRecorder()
			route.call(h, w, req)
			if w.Code != tc.want {
				t.Errorf("%s, %s: %d %q, want %d", route.path, tc.name, w.Code, w.Body.String(), tc.want)
			}
			applied := prefetch.enqueued.Load() + evict.deprioritized.Load()
			if (tc.want == http.StatusOK) != (applied == 1) {
				t.Errorf("%s, %s: %d hints applied", route.path, tc.name, applied)
			}
		}
	}
}

// A peer that answers with a redirect does not receive the key: the client
// does not follow it, and no request reaches the redirect target.
func TestClient_RefusesRedirects(t *testing.T) {
	var reached atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		if r.Header.Get(KeyHeader) != "" {
			t.Errorf("the key was forwarded to the redirect target")
		}
	}))
	defer target.Close()
	var answered atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answered.Add(1)
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusFound)
	}))
	defer peer.Close()

	c := NewClient(ClientConfig{Endpoint: peer.URL, AuthKey: "secret", Timeout: 2 * time.Second, MaxBatch: 1, BatchInterval: 20 * time.Millisecond})
	c.EnqueueHint([]string{"t1"}, 1, 2, "logs") // MaxBatch 1: sent at once
	c.SendEvictionHint([]string{"t1"}, "logs")
	c.Close()
	if answered.Load() != 2 {
		t.Fatalf("the peer answered %d requests, want 2 (one prefetch hint, one eviction hint)", answered.Load())
	}
	if reached.Load() != 0 {
		t.Fatalf("%d request(s) reached the redirect target, want none", reached.Load())
	}
}
