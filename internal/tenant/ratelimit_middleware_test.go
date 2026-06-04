package tenant

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestRateLimitMiddleware_NilLimiter_NoOp(t *testing.T) {
	called := false
	h := RateLimitMiddleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest("POST", "/insert", strings.NewReader("body"))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if !called {
		t.Fatal("downstream handler not called with nil limiter")
	}
}

func TestRateLimitMiddleware_OverBytes_Returns429(t *testing.T) {
	pr, _ := NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Ingest: config.TenantIngestOverride{MaxBytesPerSec: 100}},
	}, nil)
	limiter := NewIngestRateLimiter(pr)

	called := false
	h := RateLimitMiddleware(limiter)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	// First request: 80 bytes — fits.
	req := httptest.NewRequest("POST", "/insert", strings.NewReader(strings.Repeat("x", 80)))
	req.Header.Set("AccountID", "1")
	req.Header.Set("ProjectID", "1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if !called {
		t.Fatal("first request should pass through")
	}
	if rr.Code != http.StatusOK {
		t.Errorf("first req status=%d, want 200", rr.Code)
	}

	called = false
	// Second request: 50 bytes — only 20 left, should reject.
	req = httptest.NewRequest("POST", "/insert", strings.NewReader(strings.Repeat("x", 50)))
	req.Header.Set("AccountID", "1")
	req.Header.Set("ProjectID", "1")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if called {
		t.Fatal("second request should be rejected before reaching handler")
	}
	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("rejected status=%d, want 429", rr.Code)
	}
	if got := rr.Header().Get("X-RateLimit-Limit"); got != "100" {
		t.Errorf("X-RateLimit-Limit=%q, want 100", got)
	}
}

func TestRateLimitMiddleware_TenantWithoutOverride_NoCheck(t *testing.T) {
	pr, _ := NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Ingest: config.TenantIngestOverride{MaxBytesPerSec: 100}},
	}, nil)
	limiter := NewIngestRateLimiter(pr)

	called := false
	h := RateLimitMiddleware(limiter)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	}))

	// Tenant 0:0 has no override → large body must pass.
	req := httptest.NewRequest("POST", "/insert", strings.NewReader(strings.Repeat("x", 1<<20)))
	req.Header.Set("AccountID", "0")
	req.Header.Set("ProjectID", "0")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if !called {
		t.Fatal("non-configured tenant must not be rate-limited")
	}
}
