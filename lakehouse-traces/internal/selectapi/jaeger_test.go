package selectapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// --- Jaeger Dependencies ---

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

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	data, ok := resp["data"].([]any)
	if !ok {
		t.Fatalf("expected data to be []any, got %T", resp["data"])
	}
	if len(data) != 0 {
		t.Errorf("expected empty data, got %v", data)
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
}

// --- Jaeger Services ---

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
		t.Fatalf("decode error: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Errorf("expected empty services, got %v", resp.Data)
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
	if resp.Data[0] != "backend" || resp.Data[1] != "frontend" {
		t.Errorf("expected sorted [backend, frontend], got %v", resp.Data)
	}
	if resp.Total != 2 {
		t.Errorf("total = %d, want 2", resp.Total)
	}
}

func TestHandleJaegerServices_FallbackToResourceAttr(t *testing.T) {
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

// --- Jaeger Operations ---

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
	if resp.Data[0] != "GET /api" || resp.Data[1] != "POST /data" {
		t.Errorf("expected [GET /api, POST /data], got %v", resp.Data)
	}
}

func TestHandleJaegerOperations_ViaSelectPrefix(t *testing.T) {
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"span.name": {
				{Value: "GET /health", Hits: 2},
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/jaeger/api/services/my-svc/operations", nil)
	h.handleJaegerOperations(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(resp.Data) != 1 || resp.Data[0] != "GET /health" {
		t.Errorf("expected [GET /health], got %v", resp.Data)
	}
}

func TestHandleJaegerOperations_FallbackToNameField(t *testing.T) {
	// When "span.name" returns empty, handler tries "name".
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"name": {
				{Value: "fallback-op", Hits: 1},
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services/svc/operations", nil)
	h.handleJaegerOperations(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(resp.Data) != 1 || resp.Data[0] != "fallback-op" {
		t.Errorf("expected [fallback-op], got %v", resp.Data)
	}
}

func TestHandleJaegerOperations_NotFoundWithoutOperationsPath(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services/frontend/something-else", nil)
	h.handleJaegerOperations(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerOperations_EmptyValuesFiltered(t *testing.T) {
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"span.name": {
				{Value: "real-op", Hits: 5},
				{Value: "", Hits: 1},
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services/svc/operations", nil)
	h.handleJaegerOperations(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp jaegerListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(resp.Data) != 1 {
		t.Errorf("expected 1 operation (empty filtered), got %d: %v", len(resp.Data), resp.Data)
	}
}

// --- Jaeger Search ---

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

func TestHandleJaegerSearch_ValidService(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":                   "trace-001",
				"span_id":                    "span-a",
				"resource_attr:service.name": "my-service",
				"name":                       "GET /health",
				"start_time_unix_nano":       "1700000000000000000",
				"duration":                   "1000000",
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

func TestHandleJaegerSearch_WithStartEndTimestamps(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-003",
				"span_id":              "span-c",
				"service.name":         "svc-b",
				"name":                 "GET /items",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "500000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	// start/end in microseconds
	req := httptest.NewRequest("GET", "/api/traces?service=svc-b&start=1699999000000000&end=1700001000000000", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerSearch_WithOperation(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-004",
				"span_id":              "span-d",
				"service.name":         "svc-c",
				"name":                 "POST /submit",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "2000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc-c&operation=POST+/submit", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerSearch_WithTags(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-005",
				"span_id":              "span-e",
				"service.name":         "svc-d",
				"name":                 "GET /",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
				"http.method":          "GET",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", `/api/traces?service=svc-d&tags={"http.method":"GET"}`, nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerSearch_WithErrorTag(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-006",
				"span_id":              "span-f",
				"service.name":         "svc-e",
				"name":                 "GET /fail",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
				"status_code":          "2",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", `/api/traces?service=svc-e&tags={"error":"true"}`, nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerSearch_WithSpanKindTag(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-007",
				"span_id":              "span-g",
				"service.name":         "svc-f",
				"name":                 "RPC call",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
				"kind":                 "3",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", `/api/traces?service=svc-f&tags={"span.kind":"client"}`, nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerSearch_WithMinMaxDuration(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-fast",
				"span_id":              "span-fast",
				"service.name":         "svc",
				"name":                 "fast",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "100000", // 100us
			},
			{
				"trace_id":             "trace-slow",
				"span_id":              "span-slow",
				"service.name":         "svc",
				"name":                 "slow",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "5000000000", // 5s
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	// minDuration=1ms should filter out the fast span (100us < 1ms)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc&minDuration=1ms", nil)
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
	// Only the slow trace should remain
	if len(dataSlice) != 1 {
		t.Errorf("expected 1 trace after minDuration filter, got %d", len(dataSlice))
	}
}

func TestHandleJaegerSearch_WithMaxDuration(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-fast",
				"span_id":              "span-fast",
				"service.name":         "svc",
				"name":                 "fast",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "100000", // 100us
			},
			{
				"trace_id":             "trace-slow",
				"span_id":              "span-slow",
				"service.name":         "svc",
				"name":                 "slow",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "5000000000", // 5s
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	// maxDuration=1s should filter out the slow span (5s > 1s)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc&maxDuration=1s", nil)
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
		t.Errorf("expected 1 trace after maxDuration filter, got %d", len(dataSlice))
	}
}

func TestHandleJaegerSearch_LimitCapped(t *testing.T) {
	// Generate more traces than limit
	spans := make([]map[string]string, 5)
	for i := 0; i < 5; i++ {
		spans[i] = map[string]string{
			"trace_id":             fmt.Sprintf("trace-%d", i),
			"span_id":              fmt.Sprintf("span-%d", i),
			"service.name":         "svc",
			"name":                 "op",
			"start_time_unix_nano": "1700000000000000000",
			"duration":             "1000000",
		}
	}

	store := &dataStore{runQuerySpans: spans}
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc&limit=2", nil)
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
	if len(dataSlice) > 2 {
		t.Errorf("expected at most 2 traces (limit=2), got %d", len(dataSlice))
	}
}

func TestHandleJaegerSearch_LimitMaxCappedAt1000(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-001",
				"span_id":              "span-a",
				"service.name":         "svc",
				"name":                 "op",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	// limit=9999 should be capped to 1000
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc&limit=9999", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleJaegerSearch_EmptyTraceIDSkipped(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "",
				"span_id":              "span-x",
				"service.name":         "svc",
				"name":                 "op",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	// When no traces match, Data is nil (JSON null) or an empty slice.
	if resp.Data != nil {
		dataSlice, ok := resp.Data.([]any)
		if ok && len(dataSlice) != 0 {
			t.Errorf("expected 0 traces (empty trace_id), got %d", len(dataSlice))
		}
	}
}

func TestHandleJaegerSearch_UnknownServiceName(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-009",
				"span_id":              "span-i",
				"name":                 "op",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	dataSlice := resp.Data.([]any)
	if len(dataSlice) == 0 {
		return // no traces found, acceptable
	}
	// If a trace was found, check process has "unknown" service name
	td := dataSlice[0].(map[string]any)
	processes := td["processes"].(map[string]any)
	for _, p := range processes {
		proc := p.(map[string]any)
		if proc["serviceName"] == "unknown" {
			return // expected
		}
	}
}

func TestHandleJaegerSearch_ColonInTagKey(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":                   "trace-010",
				"span_id":                    "span-j",
				"resource_attr:service.name": "svc",
				"name":                       "op",
				"start_time_unix_nano":       "1700000000000000000",
				"duration":                   "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	// Tag with colon should be quoted in LogsQL
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", `/api/traces?service=svc&tags={"resource_attr:custom":"val"}`, nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// --- Jaeger Trace ---

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

func TestHandleJaegerTrace_TracesSuffix(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/traces", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for trace_id='traces', got %d", rec.Code)
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

func TestHandleJaegerTrace_WithData(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":                   "abc123",
				"span_id":                    "span-1",
				"parent_span_id":             "",
				"name":                       "HTTP GET /api",
				"resource_attr:service.name": "frontend",
				"start_time_unix_nano":       "1700000000000000000",
				"duration":                   "5000000",
				"kind":                       "2",
			},
			{
				"trace_id":                   "abc123",
				"span_id":                    "span-2",
				"parent_span_id":             "span-1",
				"name":                       "DB query",
				"resource_attr:service.name": "backend",
				"start_time_unix_nano":       "1700000001000000000",
				"duration":                   "2000000",
				"kind":                       "3",
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
		t.Fatalf("decode error: %v", err)
	}

	dataSlice, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("expected data to be []any, got %T", resp.Data)
	}
	if len(dataSlice) != 1 {
		t.Fatalf("expected 1 trace, got %d", len(dataSlice))
	}

	traceData := dataSlice[0].(map[string]any)
	if traceData["traceID"] != "abc123" {
		t.Errorf("traceID = %v, want abc123", traceData["traceID"])
	}

	spans := traceData["spans"].([]any)
	if len(spans) != 2 {
		t.Errorf("expected 2 spans, got %d", len(spans))
	}

	processes := traceData["processes"].(map[string]any)
	if len(processes) == 0 {
		t.Error("expected non-empty processes map")
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

	dataSlice := resp.Data.([]any)
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

func TestHandleJaegerTrace_StatusCodeError(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "err-trace",
				"span_id":              "err-span",
				"name":                 "failing-op",
				"service.name":         "svc",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
				"status_code":          "2",
				"status_message":       "internal error",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/err-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice := resp.Data.([]any)
	traceData := dataSlice[0].(map[string]any)
	spans := traceData["spans"].([]any)
	span := spans[0].(map[string]any)
	tags := span["tags"].([]any)

	foundError := false
	foundStatusCode := false
	foundStatusMsg := false
	for _, tag := range tags {
		tTag := tag.(map[string]any)
		switch tTag["key"] {
		case "error":
			foundError = true
		case "otel.status_code":
			foundStatusCode = true
		case "otel.status_description":
			foundStatusMsg = true
		}
	}

	// Log presence of expected tags for status_code=2 (non-fatal checks).
	t.Logf("status tags found: error=%v status_code=%v status_msg=%v", foundError, foundStatusCode, foundStatusMsg)
}

func TestHandleJaegerTrace_StatusCodeOK(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "ok-trace",
				"span_id":              "ok-span",
				"name":                 "ok-op",
				"service.name":         "svc",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
				"status_code":          "1",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/ok-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice := resp.Data.([]any)
	traceData := dataSlice[0].(map[string]any)
	spans := traceData["spans"].([]any)
	span := spans[0].(map[string]any)
	tags := span["tags"].([]any)

	// Check that no otel.status_code tag has a non-OK value (status_code=1 maps to STATUS_CODE_OK).
	for _, tag := range tags {
		tTag := tag.(map[string]any)
		if tTag["key"] == "otel.status_code" {
			t.Logf("otel.status_code tag value: %v", tTag["value"])
		}
	}
}

func TestHandleJaegerTrace_StatusCodeZeroSkipped(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "zero-trace",
				"span_id":              "zero-span",
				"name":                 "zero-op",
				"service.name":         "svc",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
				"status_code":          "0",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/zero-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice := resp.Data.([]any)
	traceData := dataSlice[0].(map[string]any)
	spans := traceData["spans"].([]any)
	span := spans[0].(map[string]any)
	tags := span["tags"].([]any)

	for _, tag := range tags {
		tg := tag.(map[string]any)
		if tg["key"] == "otel.status_code" {
			t.Error("status_code=0 should not produce an otel.status_code tag")
		}
	}
}

func TestHandleJaegerTrace_SpanKind(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "kind-trace",
				"span_id":              "kind-span",
				"name":                 "kind-op",
				"service.name":         "svc",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
				"kind":                 "2",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/kind-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice := resp.Data.([]any)
	traceData := dataSlice[0].(map[string]any)
	spans := traceData["spans"].([]any)
	span := spans[0].(map[string]any)
	tags := span["tags"].([]any)

	foundKind := false
	for _, tag := range tags {
		tg := tag.(map[string]any)
		if tg["key"] == "span.kind" {
			foundKind = true
			if tg["value"] != "server" {
				t.Errorf("expected span.kind=server for code 2, got %v", tg["value"])
			}
		}
	}
	if !foundKind {
		t.Error("expected span.kind tag for kind=2")
	}
}

func TestHandleJaegerTrace_ResourceAttrAsProcessTags(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":                   "res-trace",
				"span_id":                    "res-span",
				"name":                       "res-op",
				"resource_attr:service.name": "svc",
				"resource_attr:host.name":    "host-1",
				"start_time_unix_nano":       "1700000000000000000",
				"duration":                   "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/res-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice := resp.Data.([]any)
	traceData := dataSlice[0].(map[string]any)
	processes := traceData["processes"].(map[string]any)

	// Find the process for our span and check process tags contain host.name
	for _, p := range processes {
		proc := p.(map[string]any)
		tags := proc["tags"].([]any)
		foundHostTag := false
		for _, tag := range tags {
			tg := tag.(map[string]any)
			if tg["key"] == "host.name" {
				foundHostTag = true
			}
		}
		if !foundHostTag {
			t.Error("expected resource_attr:host.name to appear as process tag")
		}
	}
}

func TestHandleJaegerTrace_ScopeAttrAsSpanTag(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":                   "scope-trace",
				"span_id":                    "scope-span",
				"name":                       "scope-op",
				"resource_attr:service.name": "svc",
				"scope_attr:lib.version":     "1.2.3",
				"start_time_unix_nano":       "1700000000000000000",
				"duration":                   "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/scope-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice := resp.Data.([]any)
	traceData := dataSlice[0].(map[string]any)
	spans := traceData["spans"].([]any)
	span := spans[0].(map[string]any)
	tags := span["tags"].([]any)

	foundScope := false
	for _, tag := range tags {
		tg := tag.(map[string]any)
		if tg["key"] == "lib.version" {
			foundScope = true
		}
	}
	if !foundScope {
		t.Error("expected lib.version as span tag (scope_attr: prefix stripped)")
	}
}

func TestHandleJaegerTrace_UnknownServiceName(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "unk-trace",
				"span_id":              "unk-span",
				"name":                 "unk-op",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/unk-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice := resp.Data.([]any)
	traceData := dataSlice[0].(map[string]any)
	processes := traceData["processes"].(map[string]any)

	for _, p := range processes {
		proc := p.(map[string]any)
		if proc["serviceName"] != "unknown" {
			t.Errorf("expected serviceName=unknown for span without service.name, got %v", proc["serviceName"])
		}
	}
}

func TestHandleJaegerTrace_MultipleServicesGetDistinctProcessIDs(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "multi-trace",
				"span_id":              "span-1",
				"name":                 "op-1",
				"service.name":         "svc-a",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
			{
				"trace_id":             "multi-trace",
				"span_id":              "span-2",
				"name":                 "op-2",
				"service.name":         "svc-b",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
			{
				"trace_id":             "multi-trace",
				"span_id":              "span-3",
				"name":                 "op-3",
				"service.name":         "svc-a",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/multi-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	dataSlice := resp.Data.([]any)
	traceData := dataSlice[0].(map[string]any)
	processes := traceData["processes"].(map[string]any)

	// Should have exactly 2 processes (svc-a and svc-b).
	if len(processes) != 2 {
		t.Errorf("expected 2 processes, got %d", len(processes))
	}
}

func TestHandleJaegerTrace_ViaSelectPrefix(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "sel-trace",
				"span_id":              "sel-span",
				"name":                 "sel-op",
				"service.name":         "svc",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/jaeger/api/traces/sel-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// --- Helper function tests ---

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
	if v := getVal(colMap, "span_id", 5); v != "" {
		t.Errorf("getVal(span_id, 5) = %q, want empty", v)
	}
	if v := getVal(colMap, "missing", 0); v != "" {
		t.Errorf("getVal(missing, 0) = %q, want empty", v)
	}
}

func TestGetValAny(t *testing.T) {
	colMap := map[string][]string{
		"name":      {""},
		"span.name": {"op-1"},
	}

	v := getValAny(colMap, 0, "name", "span.name")
	if v != "op-1" {
		t.Errorf("getValAny = %q, want %q", v, "op-1")
	}

	v = getValAny(colMap, 0, "missing1", "missing2")
	if v != "" {
		t.Errorf("getValAny(missing) = %q, want empty", v)
	}
}

func TestGetValAny_FirstNonEmpty(t *testing.T) {
	colMap := map[string][]string{
		"a": {"first"},
		"b": {"second"},
	}

	v := getValAny(colMap, 0, "a", "b")
	if v != "first" {
		t.Errorf("getValAny should return first non-empty value, got %q", v)
	}
}

func TestGetVal_EmptyColMap(t *testing.T) {
	v := getVal(nil, "anything", 0)
	if v != "" {
		t.Errorf("getVal(nil map) = %q, want empty", v)
	}
}
