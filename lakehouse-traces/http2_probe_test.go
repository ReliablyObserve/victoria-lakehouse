package main

import (
	"net/http"
	"net/http/httptest"
	"os"
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

// answerHTTP2Probe mirrors a block of VictoriaTraces' own request dispatcher
// (app/victoria-traces/main.go), which this binary replaces. Hold it to the
// vendored source: the method and path that identify the probe, the answer's
// text and its status. When upstream changes any of them, this fails.
func TestHTTP2ProbeMatchesVendoredVT(t *testing.T) {
	up, err := os.ReadFile("deps/VictoriaTraces/app/victoria-traces/main.go")
	if err != nil {
		t.Fatalf("vendored VictoriaTraces missing (run make deps-vt): %v", err)
	}
	ours, err := os.ReadFile("http2_probe.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`r.Method == "PRI" && r.URL.Path == "*"`,
		`http.Error(w, "HTTP/2 is currently not supported on this port", http.StatusMethodNotAllowed)`,
	} {
		if !strings.Contains(string(up), want) {
			t.Errorf("the vendored VictoriaTraces dispatcher no longer contains %q: re-check http2_probe.go", want)
		}
	}
	// Ours is the negation of the same test, then the same answer.
	for _, want := range []string{
		`r.Method != "PRI" || r.URL.Path != "*"`,
		`http.Error(w, "HTTP/2 is currently not supported on this port", http.StatusMethodNotAllowed)`,
	} {
		if !strings.Contains(string(ours), want) {
			t.Errorf("http2_probe.go no longer contains %q", want)
		}
	}
}
