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

// dataStore implements storage.Storage and returns data for testing.
type dataStore struct {
	mockStore
	fieldValues   map[string][]logstorage.ValueWithHits
	runQuerySpans []map[string]string
}

var _ storage.Storage = (*dataStore)(nil)

func (d *dataStore) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, fieldName string, _ uint64) ([]logstorage.ValueWithHits, error) {
	if d.fieldValues != nil {
		if vals, ok := d.fieldValues[fieldName]; ok {
			return vals, nil
		}
	}
	return nil, nil
}

func (d *dataStore) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	if len(d.runQuerySpans) == 0 {
		return nil
	}

	colSet := make(map[string]bool)
	for _, span := range d.runQuerySpans {
		for k := range span {
			colSet[k] = true
		}
	}

	cols := make([]logstorage.BlockColumn, 0, len(colSet))
	for colName := range colSet {
		vals := make([]string, len(d.runQuerySpans))
		for i, span := range d.runQuerySpans {
			vals[i] = span[colName]
		}
		cols = append(cols, logstorage.BlockColumn{Name: colName, Values: vals})
	}

	var db logstorage.DataBlock
	db.SetColumns(cols)
	writeBlock(0, &db)
	return nil
}

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

func TestNewHandler_DefaultMaxConcurrent(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 0,
		},
	}
	h := NewHandler(mockStore{}, cfg)
	if cap(h.sem) != 32 {
		t.Errorf("expected default max concurrent 32, got %d", cap(h.sem))
	}
}

func TestNewHandler_NegativeMaxConcurrent(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: -5,
		},
	}
	h := NewHandler(mockStore{}, cfg)
	if cap(h.sem) != 32 {
		t.Errorf("expected default max concurrent 32 for negative value, got %d", cap(h.sem))
	}
}

func TestNewHandler_CustomMaxConcurrent(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 64,
		},
	}
	h := NewHandler(mockStore{}, cfg)
	if cap(h.sem) != 64 {
		t.Errorf("expected max concurrent 64, got %d", cap(h.sem))
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

func TestWrapVL_RateLimiting_Rejects429(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 1,
		},
	}
	h := NewHandler(mockStore{}, cfg)

	blocker := make(chan struct{})
	wrapped := h.wrapVL(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		<-blocker
	})

	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/select/logsql/query", nil)
		wrapped(rec, req)
	}()

	time.Sleep(50 * time.Millisecond)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	wrapped(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d; body: %s", rec.Code, rec.Body.String())
	}

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

func TestWrapVL_SlowQueryLogging(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 32,
			SlowThreshold: 1 * time.Millisecond,
		},
	}
	h := NewHandler(mockStore{}, cfg)

	wrapped := h.wrapVL(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test?query=test", nil)
	wrapped(rec, req)

	// Should not panic; the slow query path should execute without error.
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
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

func assertRouteRegistered(t *testing.T, mux *http.ServeMux, path string) {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	_, pattern := mux.Handler(req)
	if pattern == "" {
		t.Errorf("path %s not registered (no matching pattern)", path)
	}
}

func assertRouteNotRegistered(t *testing.T, mux *http.ServeMux, path string) {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	_, pattern := mux.Handler(req)
	if pattern != "" {
		t.Errorf("path %s should not be registered, matched pattern %q", path, pattern)
	}
}

func TestRegister_LogsMode(t *testing.T) {
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
		assertRouteRegistered(t, mux, p)
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
		assertRouteNotRegistered(t, mux, p)
	}
}

func TestRegister_TracesMode(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)
	mux := http.NewServeMux()
	h.Register(mux)

	jaegerPaths := []string{
		"/api/services",
		"/api/dependencies",
		"/select/jaeger/api/services",
		"/select/jaeger/api/dependencies",
	}
	for _, p := range jaegerPaths {
		assertRouteRegistered(t, mux, p)
	}
}

func TestRegister_TracesMode_TraceAndSearchPaths(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)
	mux := http.NewServeMux()
	h.Register(mux)

	paths := []string{
		"/select/jaeger/api/traces/",
		"/api/traces/",
	}
	for _, p := range paths {
		assertRouteRegistered(t, mux, p)
	}
}

// TestNormalizeTimeParams exercises all branches of normalizeTimeParams.
func TestNormalizeTimeParams(t *testing.T) {
	t.Run("no params — no-op", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/query", nil)
		normalizeTimeParams(req)
		// should not panic
	})

	t.Run("params already in seconds (small value)", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/query?start=1700000000&end=1700003600", nil)
		normalizeTimeParams(req)
		// values are below 1e12 — no conversion
		if req.FormValue("start") != "1700000000" {
			t.Errorf("start should remain seconds, got %q", req.FormValue("start"))
		}
	})

	t.Run("params in milliseconds (> 1e12) get divided by 1000", func(t *testing.T) {
		// Use millisecond timestamps > 1e12
		req := httptest.NewRequest("GET", "/query?start=1700000000000&end=1700003600000&time=1700001800000", nil)
		normalizeTimeParams(req)
		if req.FormValue("start") != "1700000000" {
			t.Errorf("start ms→s: got %q, want 1700000000", req.FormValue("start"))
		}
		if req.FormValue("end") != "1700003600" {
			t.Errorf("end ms→s: got %q, want 1700003600", req.FormValue("end"))
		}
		if req.FormValue("time") != "1700001800" {
			t.Errorf("time ms→s: got %q, want 1700001800", req.FormValue("time"))
		}
	})

	t.Run("non-numeric param — skipped", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/query?start=notanumber", nil)
		normalizeTimeParams(req)
		// should not panic, non-numeric is skipped
	})

	t.Run("empty param — skipped", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/query?start=", nil)
		normalizeTimeParams(req)
	})
}

// TestNormalizeTempoSearchParams locks the workaround for the upstream VT
// Tempo /api/search quirk where an empty `q` URL parameter clobbers the
// default `{}` filter — see normalizeTempoSearchParams' doc comment.
//
// This test FAILS if normalizeTempoSearchParams stops defaulting `q` or
// stops converting `tags` to TraceQL — both regressions the user has been
// burned by before. The probe at tests/verification/probe_tempo_search_24h.sh
// is the end-to-end half of the same lock.
func TestNormalizeTempoSearchParams(t *testing.T) {
	t.Run("non-search path: untouched", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/select/tempo/api/v2/search/tags?tags=service.name=foo", nil)
		normalizeTempoSearchParams(req)
		if got := req.FormValue("q"); got != "" {
			t.Errorf("non-search path should not have q set, got %q", got)
		}
	})

	t.Run("q already set: untouched", func(t *testing.T) {
		req := httptest.NewRequest("GET", `/select/tempo/api/search?q={resource.service.name="foo"}`, nil)
		normalizeTempoSearchParams(req)
		want := `{resource.service.name="foo"}`
		if got := req.FormValue("q"); got != want {
			t.Errorf("existing q should be preserved: got %q, want %q", got, want)
		}
	})

	t.Run("q missing and no tags: defaults to {}", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/select/tempo/api/search?start=1&end=2", nil)
		normalizeTempoSearchParams(req)
		if got := req.FormValue("q"); got != "{}" {
			t.Errorf("empty q should default to {}, got %q", got)
		}
	})

	t.Run("q empty string and no tags: defaults to {}", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/select/tempo/api/search?q=", nil)
		normalizeTempoSearchParams(req)
		if got := req.FormValue("q"); got != "{}" {
			t.Errorf("empty q should default to {}, got %q", got)
		}
	})

	t.Run("q missing, tags with service.name: TraceQL with resource prefix", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/select/tempo/api/search?tags=service.name=api-gateway", nil)
		normalizeTempoSearchParams(req)
		want := `{resource.service.name="api-gateway"}`
		if got := req.FormValue("q"); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("q missing, tags with two pairs: TraceQL with && and sorted keys", func(t *testing.T) {
		// Use space-encoded as %20 to mimic URL-encoding (Grafana usually does this).
		req := httptest.NewRequest("GET",
			"/select/tempo/api/search?tags=service.name=api-gateway%20db.system=postgres",
			nil)
		normalizeTempoSearchParams(req)
		// Sorted: db.system < resource.service.name lexically.
		want := `{db.system="postgres" && resource.service.name="api-gateway"}`
		if got := req.FormValue("q"); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("q missing, tags with span.* prefix: passthrough", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/select/tempo/api/search?tags=span.http.status_code=500", nil)
		normalizeTempoSearchParams(req)
		want := `{span.http.status_code="500"}`
		if got := req.FormValue("q"); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("q missing, tags with quoted value containing spaces", func(t *testing.T) {
		// "key=\"hello world\"" — outer pair, embedded space inside quotes.
		req := httptest.NewRequest("GET", `/select/tempo/api/search?tags=service.name=%22api+gateway%22`, nil)
		normalizeTempoSearchParams(req)
		// + decodes to space in form values. The value should keep its spaces.
		want := `{resource.service.name="api gateway"}`
		if got := req.FormValue("q"); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("q missing, tags with garbage: falls back to {}", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/select/tempo/api/search?tags=garbage_no_equals", nil)
		normalizeTempoSearchParams(req)
		if got := req.FormValue("q"); got != "{}" {
			t.Errorf("garbage tags should fall back to {}, got %q", got)
		}
	})

	t.Run("tags with empty value: skipped pair, still falls back if all empty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/select/tempo/api/search?tags=service.name=", nil)
		normalizeTempoSearchParams(req)
		if got := req.FormValue("q"); got != "{}" {
			t.Errorf("got %q, want {}", got)
		}
	})
}

// TestTempoTagsToTraceQL exercises the conversion helper directly so we
// catch mapping-rule regressions even if the HTTP wrapper changes.
func TestTempoTagsToTraceQL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"whitespace only", "   ", ""},
		{"no equals", "garbage", ""},
		{"service.name", "service.name=foo", `{resource.service.name="foo"}`},
		{".service.name", ".service.name=foo", `{resource.service.name="foo"}`},
		{"name", "name=GET", `{name="GET"}`},
		{"status", "status=error", `{status="error"}`},
		{"resource.* passthrough", "resource.host.name=ip-10-0-0-1", `{resource.host.name="ip-10-0-0-1"}`},
		{"span.* passthrough", "span.http.method=GET", `{span.http.method="GET"}`},
		{"event.* passthrough", "event.exception.type=OOM", `{event.exception.type="OOM"}`},
		{"bare key", "custom.tag=val", `{custom.tag="val"}`},
		{"multi sorted", "z.last=1 a.first=2", `{a.first="2" && z.last="1"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := tempoTagsToTraceQL(c.in)
			if got != c.want {
				t.Errorf("tempoTagsToTraceQL(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
