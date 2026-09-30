package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAnswerHTTP2Probe(t *testing.T) {
	probe := httptest.NewRequest("PRI", "*", nil)
	probe.URL.Path = "*"
	rec := httptest.NewRecorder()
	if !answerHTTP2Probe(rec, probe) {
		t.Fatal("the HTTP/2 prior-knowledge probe was not answered")
	}
	if rec.Code != http.StatusMethodNotAllowed || !strings.Contains(rec.Body.String(), "HTTP/2 is currently not supported on this port") {
		t.Errorf("probe answer = %d %q, want 405 with VictoriaTraces' message", rec.Code, rec.Body.String())
	}

	for _, r := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/health", nil),
		httptest.NewRequest(http.MethodPost, "/insert/opentelemetry/v1/traces", nil),
		httptest.NewRequest("PRI", "/health", nil), // a PRI to a real path is not the probe
	} {
		rec := httptest.NewRecorder()
		if answerHTTP2Probe(rec, r) {
			t.Errorf("%s %s was swallowed as an HTTP/2 probe", r.Method, r.URL.Path)
		}
	}
}
