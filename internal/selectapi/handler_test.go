package selectapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
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
func (mockStore) Close() error { return nil }

func testConfig(mode config.Mode) *config.Config {
	return &config.Config{
		Mode: mode,
		Query: config.QueryConfig{
			Timeout: 5 * time.Second,
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
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", p, nil)
		mux.ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Errorf("jaeger path %s returned 404 in traces mode", p)
		}
	}
}

func TestHandleJaegerDependencies_EmptyResponse(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/dependencies", nil)
	h.handleJaegerDependencies(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var resp jaegerListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Errorf("expected empty data, got %v", resp.Data)
	}
}

func TestHandleJaegerServices_EmptyStore(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services", nil)
	h.handleJaegerServices(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Errorf("expected empty services, got %v", resp.Data)
	}
}

func TestHandleJaegerSearch_MissingService(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerTrace_MissingTraceID(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerTrace_NotFound(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/abc123", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}

func TestHandleJaegerOperations_MissingService(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services/", nil)
	h.handleJaegerOperations(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestSpanKindName(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{"1", "internal"},
		{"2", "server"},
		{"3", "client"},
		{"4", "producer"},
		{"5", "consumer"},
		{"99", "99"},
		{"", ""},
	}
	for _, tc := range tests {
		got := spanKindName(tc.code)
		if got != tc.want {
			t.Errorf("spanKindName(%q) = %q, want %q", tc.code, got, tc.want)
		}
	}
}

func TestGetVal(t *testing.T) {
	colMap := map[string][]string{
		"trace_id": {"abc", "def"},
		"span_id":  {"s1"},
	}

	if v := getVal(colMap, "trace_id", 0); v != "abc" {
		t.Errorf("getVal(trace_id, 0) = %q, want %q", v, "abc")
	}
	if v := getVal(colMap, "trace_id", 1); v != "def" {
		t.Errorf("getVal(trace_id, 1) = %q, want %q", v, "def")
	}
	// Out of bounds
	if v := getVal(colMap, "span_id", 5); v != "" {
		t.Errorf("getVal(span_id, 5) = %q, want empty", v)
	}
	// Missing column
	if v := getVal(colMap, "missing", 0); v != "" {
		t.Errorf("getVal(missing, 0) = %q, want empty", v)
	}
}

func TestGetValAny(t *testing.T) {
	colMap := map[string][]string{
		"name":      {""},
		"span.name": {"op-1"},
	}

	// Should skip empty "name" and return "span.name" value.
	v := getValAny(colMap, 0, "name", "span.name")
	if v != "op-1" {
		t.Errorf("getValAny = %q, want %q", v, "op-1")
	}

	// No matching columns
	v = getValAny(colMap, 0, "missing1", "missing2")
	if v != "" {
		t.Errorf("getValAny(missing) = %q, want empty", v)
	}
}
