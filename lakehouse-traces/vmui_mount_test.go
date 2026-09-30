package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The traces binary serves VictoriaTraces' own UI at /select/vmui/, not the
// log-based one VictoriaTraces v0.12.0 replaced, and only when the UI tab is
// enabled.
func TestMountVMUI_ServesVTUI(t *testing.T) {
	mux := http.NewServeMux()
	mountVMUI(mux, true)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/select/vmui/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /select/vmui/ = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "UI for VictoriaTraces") || strings.Contains(body, "UI for VictoriaLogs") {
		t.Errorf("the traces binary must serve VTUI, got: %.200s", body)
	}
	if !strings.Contains(body, "/lakehouse/ui/vmui-tab.js") {
		t.Errorf("the Lakehouse tab is not injected: %.200s", body)
	}

	off := http.NewServeMux()
	mountVMUI(off, false)
	rec = httptest.NewRecorder()
	off.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/select/vmui/", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("with the UI tab disabled /select/vmui/ = %d, want 404", rec.Code)
	}
}
