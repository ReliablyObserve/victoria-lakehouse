package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selectapi"
)

// The logs binary has no Jaeger endpoints: Jaeger is VictoriaTraces' API,
// served by lakehouse-traces. This pins what lakehouse-logs answers for those
// paths, through the same select registration and request handler the binary
// mounts.
//
// Measured against VictoriaLogs v1.53.0 (the deps/ build, 2026-10-23): hot VL
// answers every one of these paths with 400 and the body
// `unsupported path requested: "<path>"`. lakehouse-logs answers the mux's
// default 404 "404 page not found". That difference is not specific to Jaeger:
// the logs mux has no catch-all, so every unrouted path gets the 404 (tracked
// as a known difference, not changed here).
func TestLogsBinary_DoesNotServeJaeger(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	mux := http.NewServeMux()
	selectapi.NewHandler(nil, cfg).Register(mux)
	handler := newRequestHandler(mux)

	for _, path := range []string{
		"/select/jaeger/api/services",
		"/select/jaeger/api/services/svc/operations",
		"/select/jaeger/api/traces",
		"/select/jaeger/api/traces/abc",
		"/select/jaeger/api/dependencies",
		"/api/services",
		"/api/traces",
		"/api/traces/abc",
		"/api/dependencies",
	} {
		t.Run(path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != http.StatusNotFound || rec.Body.String() != "404 page not found\n" {
				t.Errorf("got %d %q, want the mux's 404 \"404 page not found\" (no Jaeger route)", rec.Code, rec.Body.String())
			}
		})
	}
}
