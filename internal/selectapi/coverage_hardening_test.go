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
)

// TestNewHandler_DefaultMaxConcurrent exercises the MaxConcurrent <= 0 default branch.
func TestNewHandler_DefaultMaxConcurrent(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 0, // should default to 32
		},
	}
	h := NewHandler(mockStore{}, cfg)
	if h == nil {
		t.Fatal("NewHandler returned nil")
	}
	if cap(h.sem) != 32 {
		t.Errorf("sem capacity = %d, want 32 (default)", cap(h.sem))
	}
}

// TestNewHandler_NegativeMaxConcurrent exercises negative MaxConcurrent branch.
func TestNewHandler_NegativeMaxConcurrent(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: -5, // should default to 32
		},
	}
	h := NewHandler(mockStore{}, cfg)
	if cap(h.sem) != 32 {
		t.Errorf("sem capacity = %d, want 32 (default for negative)", cap(h.sem))
	}
}

// TestWrapVL_SlowQueryLogging exercises the slow query logging branch.
func TestWrapVL_SlowQueryLogging(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 10,
			SlowThreshold: 1 * time.Nanosecond, // extremely low threshold to trigger slow query log
		},
	}
	h := NewHandler(mockStore{}, cfg)

	wrapped := h.wrapVL(func(_ context.Context, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/query?query=test", nil)
	wrapped(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// TestWrapVL_SlowQueryWithTenantHeaders exercises slow query path with tenant headers.
func TestWrapVL_SlowQueryWithTenantHeaders(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 10,
			SlowThreshold: 1 * time.Nanosecond,
		},
	}
	h := NewHandler(mockStore{}, cfg)

	wrapped := h.wrapVL(func(_ context.Context, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/query?query=slow_query", nil)
	req.Header.Set("X-Scope-AccountID", "42")
	req.Header.Set("X-Scope-ProjectID", "7")
	wrapped(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// TestNormalizeTimeParams_TimeKey exercises the "time" key normalization branch.
func TestNormalizeTimeParams_TimeKey(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?time=1779224481316&query=*", nil)
	normalizeTimeParams(req)

	if got := req.FormValue("time"); got != "1779224481" {
		t.Errorf("time = %q, want %q", got, "1779224481")
	}
}

// TestNormalizeTimeParams_NoChange exercises the branch where values are under 1e12.
func TestNormalizeTimeParams_NoChange(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?start=1000&end=2000&query=*", nil)
	normalizeTimeParams(req)

	if got := req.FormValue("start"); got != "1000" {
		t.Errorf("start = %q, want %q", got, "1000")
	}
	if got := req.FormValue("end"); got != "2000" {
		t.Errorf("end = %q, want %q", got, "2000")
	}
}

// TestNormalizeTimeParams_EmptyParams exercises the empty key branch.
func TestNormalizeTimeParams_EmptyParams(t *testing.T) {
	req := httptest.NewRequest("GET", "/test?query=*", nil)
	normalizeTimeParams(req)
	// Should not panic or alter anything.
	if got := req.FormValue("start"); got != "" {
		t.Errorf("start = %q, want empty", got)
	}
}

// TestHandleJaegerSearch_WithOperationAndDuration exercises the operation and
// duration filter branches in handleJaegerSearch.
func TestHandleJaegerSearch_WithOperationAndDuration(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-dur-1",
				"span_id":              "span-d1",
				"service.name":         "duration-svc",
				"name":                 "GET /api",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "5000000", // 5ms in ns
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		"/api/traces?service=duration-svc&operation=GET+/api&minDuration=1ms&maxDuration=10ms", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// TestHandleJaegerSearch_WithStartEnd exercises the start/end microsecond params.
func TestHandleJaegerSearch_WithStartEnd(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-se-1",
				"span_id":              "span-se1",
				"service.name":         "time-svc",
				"name":                 "op",
				"start_time_unix_nano": "1700000000000000000",
				"duration_ns":          "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		"/api/traces?service=time-svc&start=1700000000000000&end=1700000001000000", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// TestHandleJaegerSearch_WithTags exercises the tags JSON filter branch.
func TestHandleJaegerSearch_WithTags(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-tags-1",
				"span_id":              "span-t1",
				"service.name":         "tag-svc",
				"name":                 "tagged-op",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	// Test with error=true tag (mapped to status_code=2)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		`/api/traces?service=tag-svc&tags={"error":"true","span.kind":"server","custom:tag":"val"}`, nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// TestHandleJaegerSearch_LimitCapping exercises the limit > 1000 capping branch.
func TestHandleJaegerSearch_LimitCapping(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-lim-1",
				"span_id":              "span-l1",
				"service.name":         "limit-svc",
				"name":                 "op",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		"/api/traces?service=limit-svc&limit=5000", nil) // exceeds 1000 cap
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// TestHandleJaegerSearch_DurationFilterSkipsBelow exercises the minDuration filtering
// where spans below the threshold are excluded.
func TestHandleJaegerSearch_DurationFilterSkipsBelow(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-filter-1",
				"span_id":              "span-f1",
				"service.name":         "filter-svc",
				"name":                 "fast-op",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "100", // 100 ns - very small
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		"/api/traces?service=filter-svc&minDuration=1s", nil) // 1s min, spans are only 100ns
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Spans should be filtered out (too short). Data may be nil or empty slice.
	if resp.Data != nil {
		if dataSlice, ok := resp.Data.([]any); ok && len(dataSlice) != 0 {
			t.Errorf("expected 0 traces (all filtered), got %d", len(dataSlice))
		}
	}
}

// TestHandleJaegerSearch_DurationFilterSkipsAbove exercises the maxDuration filtering.
func TestHandleJaegerSearch_DurationFilterSkipsAbove(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-filter-2",
				"span_id":              "span-f2",
				"service.name":         "filter-svc",
				"name":                 "slow-op",
				"start_time_unix_nano": "1700000000000000000",
				"duration_ns":          "10000000000", // 10s
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET",
		"/api/traces?service=filter-svc&maxDuration=1s", nil) // 1s max, span is 10s
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Data may be nil or empty slice when all spans are filtered out.
	if resp.Data != nil {
		if dataSlice, ok := resp.Data.([]any); ok && len(dataSlice) != 0 {
			t.Errorf("expected 0 traces (all filtered by max), got %d", len(dataSlice))
		}
	}
}

// TestHandleJaegerSearch_EmptyTraceID exercises the empty trace_id filtering.
func TestHandleJaegerSearch_EmptyTraceID(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "", // empty trace ID should be skipped
				"span_id":              "span-x",
				"service.name":         "empty-tid-svc",
				"name":                 "op",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=empty-tid-svc", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Data may be nil or empty slice when all trace_ids are empty.
	if resp.Data != nil {
		if dataSlice, ok := resp.Data.([]any); ok && len(dataSlice) != 0 {
			t.Errorf("expected 0 traces (empty trace_id), got %d", len(dataSlice))
		}
	}
}

// TestHandleJaegerSearch_UnknownServiceProcessDefault exercises the "unknown"
// service name default in search results.
func TestHandleJaegerSearch_UnknownServiceProcessDefault(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-unk-1",
				"span_id":              "span-u1",
				"service.name":         "", // empty service -> "unknown"
				"name":                 "no-svc-op",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	// handleJaegerSearch requires a service param, but the spans have empty service.name
	req := httptest.NewRequest("GET", "/api/traces?service=any-svc", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// TestHandleJaegerOperations_FallbackToNameField exercises the operations handler
// when span.name returns empty and falls back to "name" field.
func TestHandleJaegerOperations_FallbackToNameField(t *testing.T) {
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"name": {
				{Value: "fallback-op", Hits: 2},
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/jaeger/api/services/frontend/operations", nil)
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

// TestHandleJaegerTrace_UnknownServiceDefault exercises the "unknown" service
// name default in the trace handler.
func TestHandleJaegerTrace_UnknownServiceDefault(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "unk-svc-trace",
				"span_id":              "span-unk",
				"name":                 "anon-op",
				"service.name":         "", // empty
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/unk-svc-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}

	var resp jaegerTracesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	dataSlice, ok := resp.Data.([]any)
	if !ok || len(dataSlice) == 0 {
		t.Fatal("expected trace data")
	}
	traceData := dataSlice[0].(map[string]any)
	processes := traceData["processes"].(map[string]any)
	// Should have "unknown" as service name.
	for _, p := range processes {
		proc := p.(map[string]any)
		if proc["serviceName"] == "unknown" {
			return // found expected default
		}
	}
	t.Error("expected 'unknown' service name in processes, not found")
}

// TestHandleJaegerTrace_StatusCodeOK exercises the status_code=1 (OK) branch.
func TestHandleJaegerTrace_StatusCodeOK(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "status-ok-trace",
				"span_id":              "span-sok",
				"name":                 "ok-op",
				"service.name":         "status-svc",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
				"status_code":          "1", // OK - non-zero, non-error
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/status-ok-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
}

// TestHandleJaegerTrace_StatusCodeZero exercises status_code=0 (unset, skipped).
func TestHandleJaegerTrace_StatusCodeZero(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "status-zero-trace",
				"span_id":              "span-s0",
				"name":                 "zero-op",
				"service.name":         "status-svc",
				"start_time_unix_nano": "1700000000000000000",
				"duration":             "1000000",
				"status_code":          "0", // unset, should be skipped
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/status-zero-trace", nil)
	h.handleJaegerTrace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// TestHandleJaegerSearch_InvalidLookback exercises the invalid lookback parsing (skipped).
func TestHandleJaegerSearch_InvalidLookback(t *testing.T) {
	store := &dataStore{
		runQuerySpans: []map[string]string{
			{
				"trace_id":             "trace-bad-lookback",
				"span_id":              "span-bl",
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
	req := httptest.NewRequest("GET", "/api/traces?service=svc&lookback=not-a-duration", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// TestHandleJaegerSearch_InvalidLimit exercises the invalid limit parsing (skipped).
func TestHandleJaegerSearch_InvalidLimit(t *testing.T) {
	store := &dataStore{}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc&limit=abc", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// TestHandleJaegerSearch_ZeroLimit exercises the zero limit (not used).
func TestHandleJaegerSearch_ZeroLimit(t *testing.T) {
	store := &dataStore{}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces?service=svc&limit=0", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
