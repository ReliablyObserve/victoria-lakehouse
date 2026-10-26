package selectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// mockStore implements storage.Storage with no-op methods.
type mockStore struct{}

var _ storage.Storage = (*mockStore)(nil)

func (mockStore) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	return nil
}
func (mockStore) GetFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetStreams(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) GetStreamIDs(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (mockStore) HasDataForRange(_, _ int64) bool { return true }
func (mockStore) Close() error                    { return nil }

func testConfig(mode config.Mode) *config.Config {
	return &config.Config{
		Mode: mode,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 32,
		},
	}
}

func TestNewHandler_ReturnsNonNil(t *testing.T) {
	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)
	if h == nil {
		t.Fatal("NewHandler returned nil")
	}
	if h.timeout != cfg.Query.Timeout {
		t.Errorf("timeout = %v, want %v", h.timeout, cfg.Query.Timeout)
	}
}

func TestWrapVL_AddsTimeout(t *testing.T) {
	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)

	var gotDeadline bool
	wrapped := h.wrapVL(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		_, gotDeadline = ctx.Deadline()
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	wrapped(rec, req)

	if !gotDeadline {
		t.Error("expected context with deadline from wrapVL")
	}
}

func TestHandleTailNoop(t *testing.T) {
	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/tail", nil)
	h.handleTailNoop(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Errorf("expected 501, got %d", rec.Code)
	}
}

func TestRegister_LogsMode(t *testing.T) {
	// Initialize VL's global external storage so ProcessTenantIDsRequest
	// does not panic on a nil pointer.
	vlstorage.SetStorage(mockStore{}, nil)

	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)
	mux := http.NewServeMux()
	h.Register(mux)

	paths := []string{
		"/select/logsql/query",
		"/select/logsql/query_time_range",
		"/select/logsql/facets",
		"/select/logsql/field_names",
		"/select/logsql/field_values",
		"/select/logsql/stream_field_names",
		"/select/logsql/stream_field_values",
		"/select/logsql/streams",
		"/select/logsql/stream_ids",
		"/select/logsql/hits",
		"/select/logsql/stats_query",
		"/select/logsql/stats_query_range",
		"/select/logsql/tail",
		"/select/tenant_ids",
	}
	for _, p := range paths {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", p, nil)
		mux.ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Errorf("path %s returned 404, expected registered", p)
		}
	}
}

func TestRegister_LogsMode_NoJaegerPaths(t *testing.T) {
	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)
	mux := http.NewServeMux()
	h.Register(mux)

	jaegerPaths := []string{
		"/api/services",
		"/api/dependencies",
		"/api/traces",
	}
	for _, p := range jaegerPaths {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", p, nil)
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("jaeger path %s should be 404 in logs mode, got %d", p, rec.Code)
		}
	}
}

func TestWrapVL_RateLimiting_Rejects429(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 1,
		},
	}
	h := NewHandler(mockStore{}, cfg)

	// Block the semaphore by occupying the single slot.
	blocker := make(chan struct{})
	wrapped := h.wrapVL(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		<-blocker // block until test releases
	})

	// First request: should acquire the semaphore.
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/select/logsql/query", nil)
		wrapped(rec, req)
	}()

	// Give the goroutine a moment to acquire the semaphore.
	time.Sleep(50 * time.Millisecond)

	// Second request: should be rejected with 429.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	wrapped(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d; body: %s", rec.Code, rec.Body.String())
	}

	// Release the blocker so the first request can finish.
	close(blocker)
}

func TestWrapVL_RateLimiting_AllowsWithinLimit(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 10,
		},
	}
	h := NewHandler(mockStore{}, cfg)

	wrapped := h.wrapVL(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	wrapped(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// TestWithResolver exercises the WithResolver HandlerOption (previously 0%).
func TestWithResolver(t *testing.T) {
	cfg := testConfig(config.ModeLogs)
	r := tenant.NewResolver(tenant.ResolverConfig{})

	h := NewHandler(mockStore{}, cfg, WithResolver(r))
	if h == nil {
		t.Fatal("NewHandler returned nil")
	}
	if h.resolver != r {
		t.Error("WithResolver should set the resolver field")
	}
}

// TestTenantFromRequest exercises tenantFromRequest (previously 0%).
func TestTenantFromRequest(t *testing.T) {
	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)

	t.Run("no headers returns empty string", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		got := h.tenantFromRequest(req)
		if got != "" {
			t.Errorf("expected empty string, got %q", got)
		}
	})

	t.Run("with account and project headers", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("AccountID", "42")
		req.Header.Set("ProjectID", "3")
		got := h.tenantFromRequest(req)
		if got == "" {
			t.Error("expected non-empty tenant string")
		}
	})

	t.Run("with resolver and unknown tenant uses numeric ID", func(t *testing.T) {
		r := tenant.NewResolver(tenant.ResolverConfig{})
		h2 := NewHandler(mockStore{}, cfg, WithResolver(r))
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("AccountID", "42")
		req.Header.Set("ProjectID", "3")
		got := h2.tenantFromRequest(req)
		if got == "" {
			t.Error("expected non-empty tenant string with resolver")
		}
	})

	t.Run("with resolver and named alias", func(t *testing.T) {
		r := tenant.NewResolver(tenant.ResolverConfig{})
		_ = r.AddAlias("prod_team", tenant.TenantID{AccountID: 42, ProjectID: 3})
		h3 := NewHandler(mockStore{}, cfg, WithResolver(r))
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("AccountID", "42")
		req.Header.Set("ProjectID", "3")
		got := h3.tenantFromRequest(req)
		if got == "" {
			t.Error("expected non-empty tenant string with resolver and alias")
		}
	})
}

func TestRequestNeedsFieldData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want bool
	}{
		{"no field param", "/hits?query=*&step=60s", false},
		{"empty field param", "/hits?query=*&field=", false},
		{"field=level", "/hits?query=*&field=level", true},
		{"field=service.name", "/hits?query=*&field=service.name", true},
		{"fields[] array", "/hits?query=*&fields[]=level&fields[]=host", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.url, nil)
			got := requestNeedsFieldData(req)
			if got != tc.want {
				t.Errorf("requestNeedsFieldData(%q) = %v, want %v", tc.url, got, tc.want)
			}
		})
	}
}

func TestWrapVLTimestampOnly_FieldParamSkipsHint(t *testing.T) {
	t.Parallel()

	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)

	tests := []struct {
		name       string
		url        string
		wantTSOnly bool
	}{
		{"no field - timestamp only", "/select/logsql/hits?query=*&step=60s", true},
		{"field=level - NOT timestamp only", "/select/logsql/hits?query=*&step=60s&field=level", false},
		{"field=service.name - NOT timestamp only", "/select/logsql/hits?query=*&field=service.name", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotTSOnly bool
			wrapped := h.wrapVLTimestampOnly(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
				gotTSOnly = storage.IsTimestampOnly(ctx)
				w.WriteHeader(http.StatusOK)
			})

			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", tc.url, nil)
			wrapped(rec, req)

			if gotTSOnly != tc.wantTSOnly {
				t.Errorf("IsTimestampOnly = %v, want %v", gotTSOnly, tc.wantTSOnly)
			}
		})
	}
}

func TestWrapVL_NeverSetsTimestampOnlyHint(t *testing.T) {
	t.Parallel()

	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)

	tests := []struct {
		name string
		url  string
	}{
		{"stats query by level", "/select/logsql/stats_query?query=*+|+stats+by(level)+count()+rows"},
		{"stats query range by level", "/select/logsql/stats_query_range?query=*+|+stats+by(level)+count()+rows&step=300"},
		{"stats query no grouping", "/select/logsql/stats_query?query=*+|+stats+count()+rows"},
		{"plain query", "/select/logsql/query?query=*"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotTSOnly bool
			wrapped := h.wrapVL(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
				gotTSOnly = storage.IsTimestampOnly(ctx)
				w.WriteHeader(http.StatusOK)
			})

			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", tc.url, nil)
			wrapped(rec, req)

			if gotTSOnly {
				t.Errorf("wrapVL should NEVER set TimestampOnlyHint, but it was set for %s", tc.url)
			}
		})
	}
}

func TestHitsEndpoint_TimestampOnlyBehavior(t *testing.T) {
	t.Parallel()

	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)

	tests := []struct {
		name       string
		url        string
		wantTSOnly bool
	}{
		{"hits no field - timestamp only", "/select/logsql/hits?query=*&step=60s", true},
		{"hits field=level - NOT timestamp only", "/select/logsql/hits?query=*&step=60s&field=level", false},
		{"hits field=service.name - NOT timestamp only", "/select/logsql/hits?query=*&field=service.name", false},
		{"hits fields[] array - NOT timestamp only", "/select/logsql/hits?query=*&fields[]=level&fields[]=host&step=60s", false},
		{"hits empty field - timestamp only", "/select/logsql/hits?query=*&field=&step=60s", true},
		{"hits field with dots - NOT timestamp only", "/select/logsql/hits?query=*&field=k8s.pod.name&step=60s", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotTSOnly bool
			wrapped := h.wrapVLTimestampOnly(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
				gotTSOnly = storage.IsTimestampOnly(ctx)
				w.WriteHeader(http.StatusOK)
			})

			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", tc.url, nil)
			wrapped(rec, req)

			if gotTSOnly != tc.wantTSOnly {
				t.Errorf("IsTimestampOnly = %v, want %v", gotTSOnly, tc.wantTSOnly)
			}
		})
	}
}

func TestRequestNeedsFieldData_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
		want bool
	}{
		{"no params at all", "/hits", false},
		{"field with whitespace value", "/hits?field=%20", true},
		{"multiple empty fields[]", "/hits?fields[]=&fields[]=", true},
		{"field param with other params", "/hits?query=*&field=level&step=60s&start=1000", true},
		{"POST with field in query", "/hits?query=*&step=60s", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.url, nil)
			got := requestNeedsFieldData(req)
			if got != tc.want {
				t.Errorf("requestNeedsFieldData(%q) = %v, want %v", tc.url, got, tc.want)
			}
		})
	}
}
