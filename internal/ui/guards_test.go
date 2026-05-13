package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGuard_VMUIInjectScriptPath(t *testing.T) {
	if !strings.Contains(injectScript, "/lakehouse/ui/vmui-tab.js") {
		t.Errorf("inject script src changed: %q — VMUI tab injection will break", injectScript)
	}
}

func TestGuard_VMUIInjectScriptTag(t *testing.T) {
	if !strings.HasPrefix(injectScript, "<script") {
		t.Error("injectScript must be a <script> tag")
	}
	if !strings.HasSuffix(injectScript, "</script>") {
		t.Error("injectScript must close with </script>")
	}
}

func TestGuard_UIHandlerServesAtLakehouseUI(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/lakehouse/ui/", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		t.Error("/lakehouse/ui/ returned 404 — UI handler path changed")
	}
}

func TestGuard_UIHandlerDisabledReturns404(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: false})
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/lakehouse/ui/", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("disabled UI returned %d, want 404", rec.Code)
	}
}

func TestGuard_VMUIInjectOnlyModifiesHTML(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("var x = 1;"))
	})

	handler := InjectLakehouseTab(upstream)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/vmui/main.js", nil)
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, "vmui-tab.js") {
		t.Error("script injected into non-HTML response")
	}
}

func TestGuard_VMUIInjectPreservesBody(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("<html><body><h1>VMUI</h1></body></html>"))
	})

	handler := InjectLakehouseTab(upstream)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/vmui/", nil)
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "<h1>VMUI</h1>") {
		t.Error("original body content was lost during injection")
	}
	if !strings.Contains(body, "vmui-tab.js") {
		t.Error("script was not injected into HTML response")
	}
}

func TestGuard_StaticFilesEmbedded(t *testing.T) {
	entries, err := staticFiles.ReadDir("static")
	if err != nil {
		t.Fatalf("failed to read embedded static dir: %s", err)
	}

	required := map[string]bool{
		"index.html":  false,
		"vmui-tab.js": false,
	}

	for _, e := range entries {
		if _, ok := required[e.Name()]; ok {
			required[e.Name()] = true
		}
	}

	for name, found := range required {
		if !found {
			t.Errorf("required static file %q not embedded", name)
		}
	}
}
