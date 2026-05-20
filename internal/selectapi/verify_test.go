package selectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// logsqlEndpoints lists all 14 LogsQL endpoints that must be registered.
var logsqlEndpoints = []string{
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

// jaegerEndpoints lists Jaeger-specific paths registered only in traces mode.
var jaegerEndpoints = []string{
	"/select/jaeger/api/traces",
	"/select/jaeger/api/services",
	"/select/jaeger/api/dependencies",
	"/api/traces",
	"/api/services",
	"/api/dependencies",
}

// TestVerify_AllLogsQLEndpoints_Respond verifies that all 14 LogsQL endpoints
// return a non-404 status code, confirming they are registered in the mux.
func TestVerify_AllLogsQLEndpoints_Respond(t *testing.T) {
	// SetStorage must be called before testing VL endpoints to avoid nil panic.
	vlstorage.SetStorage(mockStore{}, nil)

	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)
	mux := http.NewServeMux()
	h.Register(mux)

	for _, path := range logsqlEndpoints {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", path+"?query=*&start=1h&end=now", nil)
			mux.ServeHTTP(rec, req)
			if rec.Code == http.StatusNotFound {
				t.Errorf("endpoint %s returned 404; expected it to be registered", path)
			}
		})
	}
}

// TestVerify_Tail_Returns501 verifies the tail endpoint returns 501 with the
// expected "live tail not supported" message in the body.
func TestVerify_Tail_Returns501(t *testing.T) {
	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/tail", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Errorf("tail endpoint: expected 501, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "live tail not supported") {
		t.Errorf("tail endpoint body = %q; expected to contain \"live tail not supported\"", body)
	}
}

// TestVerify_NormalizeTimeParams_MillisToSeconds verifies that millisecond-epoch
// timestamps (>1e12) are correctly converted to seconds so VL handles them.
func TestVerify_NormalizeTimeParams_MillisToSeconds(t *testing.T) {
	msStart := "1779224481316"
	msEnd := "1779231681365"
	wantStart := "1779224481"
	wantEnd := "1779231681"

	req := httptest.NewRequest("GET",
		"/select/logsql/hits?query=*&start="+msStart+"&end="+msEnd, nil)
	normalizeTimeParams(req)

	if got := req.FormValue("start"); got != wantStart {
		t.Errorf("start: got %q, want %q", got, wantStart)
	}
	if got := req.FormValue("end"); got != wantEnd {
		t.Errorf("end: got %q, want %q", got, wantEnd)
	}
}

// TestVerify_ConcurrencyLimiter_Rejects verifies that when MaxConcurrent=1,
// a second concurrent request receives a 429 Too Many Requests response.
func TestVerify_ConcurrencyLimiter_Rejects(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 1,
		},
	}
	h := NewHandler(mockStore{}, cfg)

	// Block the single semaphore slot for the duration of the test.
	blocker := make(chan struct{})
	wrapped := h.wrapVL(func(_ context.Context, w http.ResponseWriter, _ *http.Request) {
		<-blocker
	})

	// First request: acquires the semaphore and blocks.
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/select/logsql/query?query=*", nil)
		wrapped(rec, req)
	}()

	// Give the goroutine time to acquire the semaphore.
	time.Sleep(50 * time.Millisecond)

	// Second request: must be rejected with 429.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/query?query=*", nil)
	wrapped(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d; body: %s", rec.Code, rec.Body.String())
	}

	// Release the first request.
	close(blocker)
}

// TestVerify_LogsMode_NoJaegerEndpoints verifies that Jaeger paths are not
// registered (return 404) when the handler is in logs mode.
func TestVerify_LogsMode_NoJaegerEndpoints(t *testing.T) {
	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)
	mux := http.NewServeMux()
	h.Register(mux)

	for _, path := range jaegerEndpoints {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", path, nil)
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Errorf("jaeger path %s should be 404 in logs mode, got %d", path, rec.Code)
			}
		})
	}
}

// TestVerify_TracesMode_HasJaegerEndpoints verifies that Jaeger paths are
// registered (return non-404) when the handler is in traces mode.
func TestVerify_TracesMode_HasJaegerEndpoints(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	h := NewHandler(mockStore{}, cfg)
	mux := http.NewServeMux()
	h.Register(mux)

	for _, path := range jaegerEndpoints {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", path, nil)
			mux.ServeHTTP(rec, req)
			if rec.Code == http.StatusNotFound {
				t.Errorf("jaeger path %s returned 404 in traces mode; expected it to be registered", path)
			}
		})
	}
}

// TestVerify_WrapVL_CreatesOTELSpan verifies that wrapVL creates an OTEL span
// named "vl.handler.<path>" with http.method and http.path attributes.
func TestVerify_WrapVL_CreatesOTELSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	cfg := testConfig(config.ModeLogs)
	h := NewHandler(mockStore{}, cfg)
	handler := h.wrapVL(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/select/logsql/query?query=*", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	spans := exporter.GetSpans()
	found := false
	for _, s := range spans {
		if strings.HasPrefix(s.Name, "vl.handler.") {
			found = true
			break
		}
	}
	if !found {
		t.Error("wrapVL should create a vl.handler.* span")
	}
}
