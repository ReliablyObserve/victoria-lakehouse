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

// dataStore implements storage.Storage and returns realistic data for
// Jaeger handler tests. It records which fields were queried so tests
// can verify handler behavior.
type dataStore struct {
	mockStore // embed for default no-op methods

	// fieldValues maps fieldName -> list of values to return from GetFieldValues.
	fieldValues map[string][]logstorage.ValueWithHits

	// runQuerySpans holds synthetic span rows to deliver via RunQuery callbacks.
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

	// Collect all column names across all spans.
	colSet := make(map[string]bool)
	for _, span := range d.runQuerySpans {
		for k := range span {
			colSet[k] = true
		}
	}

	// Build columns with values aligned to span rows.
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

// --- Jaeger handler tests with data ---

func TestHandleJaegerTrace_WithData(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "abc123",
				"span_id":              "span-1",
				"parent_span_id":       "",
				"name":                 "HTTP GET /api",
				"service.name":         "frontend",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "5000000",
				"kind":                 "2",
			},
			{
				"trace_id":             "abc123",
				"span_id":              "span-2",
				"parent_span_id":       "span-1",
				"name":                 "DB query",
				"service.name":         "backend",
				"start_time_unix_nano": "1700000001000000000",
				"duration":             "2000000",
				"kind":                 "3",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/abc123", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Data should contain one trace with spans.
	dataSlice, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("expected data to be []any, got %T", resp.Data)
	}
	if len(dataSlice) != 1 {
		t.Fatalf("expected 1 trace, got %d", len(dataSlice))
	}

	traceData, ok := dataSlice[0].(map[string]any)
	if !ok {
		t.Fatalf("expected trace data to be map, got %T", dataSlice[0])
	}
	if traceData["traceID"] != "abc123" {
		t.Errorf("traceID = %v, want abc123", traceData["traceID"])
	}

	spans, ok := traceData["spans"].([]any)
	if !ok {
		t.Fatalf("expected spans to be []any, got %T", traceData["spans"])
	}
	if len(spans) != 2 {
		t.Errorf("expected 2 spans, got %d", len(spans))
	}

	// Verify processes map is present.
	processes, ok := traceData["processes"].(map[string]any)
	if !ok {
		t.Fatalf("expected processes to be map, got %T", traceData["processes"])
	}
	if len(processes) == 0 {
		t.Error("expected non-empty processes map")
	}
}

// TestHandleJaegerTrace_WithStatusAndResourceAttrs exercises the status_code,
// resource_attr, scope_attr, and status_message column branches.
func TestHandleJaegerTrace_WithStatusAndResourceAttrs(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":                   "xyz789",
				"span_id":                    "span-a",
				"parent_span_id":             "span-parent",
				"name":                       "DB insert",
				"service.name":               "db-service",
				"start_time_unix_nano":       "1700000000000000000",
				"duration_ns":                "3000000",
				"status_code":                "2", // STATUS_CODE_ERROR
				"status_message":             "connection reset",
				"resource_attr:service.name": "db-service",
				"resource_attr:k8s.pod.name": "db-pod-1",
				"scope_attr:scope.version":   "v1.0",
				"custom_tag":                 "custom_value",
			},
			{
				"trace_id":             "xyz789",
				"span_id":              "span-b",
				"name":                 "cache hit",
				"service.name":         "cache-service",
				"start_time_unix_nano": "1700000001000000000",
				"duration_ns":          "100000",
				"status_code":          "1", // STATUS_CODE_OK
				"kind":                 "2",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/xyz789", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice, ok := resp.Data.([]any)
	if !ok || len(dataSlice) == 0 {
		t.Fatal("expected trace data, got empty")
	}
}

func TestHandleJaegerTrace_EmptyTraceID_TracesSuffix(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	// When the last path segment is "traces", the handler treats it as
	// an empty trace ID and returns 400.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/traces", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for trace_id='traces', got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerSearch_ValidService(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-001",
				"span_id":              "span-a",
				"service.name":         "my-service",
				"name":                 "GET /health",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=my-service", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("expected []any data, got %T", resp.Data)
	}
	if len(dataSlice) != 1 {
		t.Errorf("expected 1 trace in search results, got %d", len(dataSlice))
	}
}

func TestHandleJaegerSearch_WithLookbackAndLimit(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-002",
				"span_id":              "span-b",
				"service.name":         "svc-a",
				"name":                 "POST /data",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "3000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc-a&lookback=48h&limit=5", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerServices_WithData(t *testing.T) {
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"service.name": {
				{Value: "frontend", Hits: 10},
				{Value: "backend", Hits: 5},
				{Value: "", Hits: 1}, // empty values should be filtered out
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services", nil)
	h.handleJaegerServices(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 services, got %d: %v", len(resp.Data), resp.Data)
	}
	// Services should be sorted alphabetically.
	if resp.Data[0] != "backend" || resp.Data[1] != "frontend" {
		t.Errorf("expected [backend, frontend], got %v", resp.Data)
	}
	if resp.Total != 2 {
		t.Errorf("total = %d, want 2", resp.Total)
	}
}

func TestHandleJaegerOperations_WithValidService(t *testing.T) {
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"span.name": {
				{Value: "GET /api", Hits: 5},
				{Value: "POST /data", Hits: 3},
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services/frontend/operations", nil)
	h.handleJaegerOperations(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 operations, got %d: %v", len(resp.Data), resp.Data)
	}
	// Should be sorted.
	if resp.Data[0] != "GET /api" || resp.Data[1] != "POST /data" {
		t.Errorf("expected [GET /api, POST /data], got %v", resp.Data)
	}
}

func TestHandleJaegerOperations_NotFoundWithoutOperationsPath(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	// Path without /operations suffix should return 404.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services/frontend/something-else", nil)
	h.handleJaegerOperations(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerDependencies_ViaSelectPrefix(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/jaeger/api/dependencies", nil)
	h.handleJaegerDependencies(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	data, ok := resp["data"].([]any)
	if !ok {
		t.Fatalf("expected data to be []any, got %T", resp["data"])
	}
	if len(data) != 0 {
		t.Errorf("expected empty data array, got %v", data)
	}
}

func TestHandleJaegerTrace_SpanWithParentReference(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "ref-trace",
				"span_id":              "child-span",
				"parent_span_id":       "parent-span",
				"name":                 "child-op",
				"service.name":         "svc",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/ref-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice, ok := resp.Data.([]any)
	if !ok || len(dataSlice) == 0 {
		t.Fatal("expected non-empty data")
	}
	traceData := dataSlice[0].(map[string]any)
	spans := traceData["spans"].([]any)
	span := spans[0].(map[string]any)

	refs, ok := span["references"].([]any)
	if !ok || len(refs) == 0 {
		t.Fatal("expected references for child span")
	}
	ref := refs[0].(map[string]any)
	if ref["refType"] != "CHILD_OF" {
		t.Errorf("refType = %v, want CHILD_OF", ref["refType"])
	}
	if ref["spanID"] != "parent-span" {
		t.Errorf("reference spanID = %v, want parent-span", ref["spanID"])
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

func TestHandleJaegerServices_FallbackToResourceAttr(t *testing.T) {
	// When "service.name" returns empty, the handler tries "resource_attr:service.name".
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"resource_attr:service.name": {
				{Value: "otel-svc", Hits: 3},
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services", nil)
	h.handleJaegerServices(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(resp.Data) != 1 || resp.Data[0] != "otel-svc" {
		t.Errorf("expected [otel-svc], got %v", resp.Data)
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
		req.Header.Set("X-Scope-AccountID", "42")
		req.Header.Set("X-Scope-ProjectID", "3")
		got := h.tenantFromRequest(req)
		if got == "" {
			t.Error("expected non-empty tenant string")
		}
	})

	t.Run("with resolver and unknown tenant uses numeric ID", func(t *testing.T) {
		r := tenant.NewResolver(tenant.ResolverConfig{})
		h2 := NewHandler(mockStore{}, cfg, WithResolver(r))
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-Scope-AccountID", "42")
		req.Header.Set("X-Scope-ProjectID", "3")
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
		req.Header.Set("X-Scope-AccountID", "42")
		req.Header.Set("X-Scope-ProjectID", "3")
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
		name            string
		url             string
		wantTSOnly      bool
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

func TestNormalizeTimeParams(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantStart string
		wantEnd   string
	}{
		{
			name:      "millisecond epochs converted to seconds",
			query:     "start=1779224481316&end=1779231681365&query=*",
			wantStart: "1779224481",
			wantEnd:   "1779231681",
		},
		{
			name:      "second epochs unchanged",
			query:     "start=1779224481&end=1779231681&query=*",
			wantStart: "1779224481",
			wantEnd:   "1779231681",
		},
		{
			name:      "ISO timestamps unchanged",
			query:     "start=2026-05-19T00:00:00Z&end=2026-05-20T00:00:00Z&query=*",
			wantStart: "2026-05-19T00:00:00Z",
			wantEnd:   "2026-05-20T00:00:00Z",
		},
		{
			name:      "relative durations unchanged",
			query:     "start=1h&query=*",
			wantStart: "1h",
			wantEnd:   "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/select/logsql/hits?"+tc.query, nil)
			normalizeTimeParams(req)
			if got := req.FormValue("start"); got != tc.wantStart {
				t.Errorf("start = %q, want %q", got, tc.wantStart)
			}
			if tc.wantEnd != "" {
				if got := req.FormValue("end"); got != tc.wantEnd {
					t.Errorf("end = %q, want %q", got, tc.wantEnd)
				}
			}
		})
	}
}
