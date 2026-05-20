package selectapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// TestVerifyJaeger_Services_JSONFormat verifies that /api/services returns HTTP 200
// with a JSON body containing the required Jaeger UI protocol fields: data, total,
// limit, and offset.
func TestVerifyJaeger_Services_JSONFormat(t *testing.T) {
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"service.name": {
				{Value: "svc1", Hits: 5},
				{Value: "svc2", Hits: 3},
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
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}

	// data must be a JSON array
	data, ok := body["data"]
	if !ok {
		t.Fatal("response missing required field: data")
	}
	dataSlice, ok := data.([]any)
	if !ok {
		t.Fatalf("data field must be a JSON array, got %T", data)
	}
	if len(dataSlice) != 2 {
		t.Errorf("expected 2 services in data, got %d: %v", len(dataSlice), dataSlice)
	}

	// total must be present and equal to len(data)
	total, ok := body["total"]
	if !ok {
		t.Fatal("response missing required field: total")
	}
	totalF, ok := total.(float64)
	if !ok {
		t.Fatalf("total field must be a number, got %T", total)
	}
	if int(totalF) != len(dataSlice) {
		t.Errorf("total=%d does not match len(data)=%d", int(totalF), len(dataSlice))
	}

	// limit and offset must be present
	if _, ok := body["limit"]; !ok {
		t.Error("response missing required field: limit")
	}
	if _, ok := body["offset"]; !ok {
		t.Error("response missing required field: offset")
	}
}

// TestVerifyJaeger_Services_AliasPath verifies that both /api/services and
// /select/jaeger/api/services return identical JSON shapes.
func TestVerifyJaeger_Services_AliasPath(t *testing.T) {
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"service.name": {
				{Value: "api-gateway", Hits: 10},
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	paths := []string{
		"/api/services",
		"/select/jaeger/api/services",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", path, nil)
			h.handleJaegerServices(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("path %s: expected 200, got %d; body: %s", path, rec.Code, rec.Body.String())
			}

			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("path %s: invalid JSON: %v", path, err)
			}

			dataSlice, ok := body["data"].([]any)
			if !ok {
				t.Fatalf("path %s: data must be a JSON array, got %T", path, body["data"])
			}
			if len(dataSlice) != 1 {
				t.Errorf("path %s: expected 1 service, got %d", path, len(dataSlice))
			}
			if dataSlice[0] != "api-gateway" {
				t.Errorf("path %s: expected api-gateway, got %v", path, dataSlice[0])
			}
		})
	}
}

// TestVerifyJaeger_Operations_JSONFormat verifies that
// /api/services/{service}/operations returns HTTP 200 with a JSON body
// containing a data array of operation name strings.
func TestVerifyJaeger_Operations_JSONFormat(t *testing.T) {
	store := &dataStore{
		fieldValues: map[string][]logstorage.ValueWithHits{
			"span.name": {
				{Value: "op1", Hits: 7},
				{Value: "op2", Hits: 4},
			},
		},
	}

	cfg := testConfig(config.ModeTraces)
	h := NewHandler(store, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/services/api-gateway/operations", nil)
	h.handleJaegerOperations(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}

	data, ok := body["data"]
	if !ok {
		t.Fatal("response missing required field: data")
	}
	dataSlice, ok := data.([]any)
	if !ok {
		t.Fatalf("data field must be a JSON array, got %T", data)
	}
	if len(dataSlice) != 2 {
		t.Errorf("expected 2 operations, got %d: %v", len(dataSlice), dataSlice)
	}
	// All values must be strings
	for i, op := range dataSlice {
		if _, ok := op.(string); !ok {
			t.Errorf("data[%d] must be a string, got %T", i, op)
		}
	}
}

// TestVerifyJaeger_Dependencies_EmptyArray verifies that /api/dependencies
// returns HTTP 200 with JSON {"data": []} matching the Jaeger UI protocol.
func TestVerifyJaeger_Dependencies_EmptyArray(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/dependencies", nil)
	h.handleJaegerDependencies(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}

	data, ok := body["data"]
	if !ok {
		t.Fatal("response missing required field: data")
	}
	dataSlice, ok := data.([]any)
	if !ok {
		t.Fatalf("data field must be a JSON array, got %T", data)
	}
	if len(dataSlice) != 0 {
		t.Errorf("expected empty data array, got %d elements: %v", len(dataSlice), dataSlice)
	}
}

// TestVerifyJaeger_TraceByID_JSONFormat verifies that /api/traces/{traceID}
// returns valid JSON. When the mock store has no matching data the handler
// returns 404 with a proper JSON error body; that response must still be
// parseable JSON with the expected envelope shape.
func TestVerifyJaeger_TraceByID_JSONFormat(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces/abc123", nil)
	h.handleJaegerTrace(rec, req)

	// With an empty store the handler returns 404 and a JSON body.
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing trace, got %d; body: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	// Response body must be valid JSON even for a 404.
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("404 body is not valid JSON: %v", err)
	}

	// data field must be present
	if _, ok := body["data"]; !ok {
		t.Error("404 response missing required field: data")
	}
}

// TestVerifyJaeger_Search_RequiresService verifies that /api/traces without a
// service query parameter is handled gracefully (400 Bad Request) rather than
// panicking or returning a 500.
func TestVerifyJaeger_Search_RequiresService(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/traces", nil)
	h.handleJaegerSearch(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 when service param is absent, got %d; body: %s", rec.Code, rec.Body.String())
	}
}
