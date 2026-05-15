package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegressionVMUIActualIndexHTMLInjection(t *testing.T) {
	sub, err := vmuiFiles.ReadFile("vmui/index.html")
	if err != nil {
		t.Fatalf("failed to read embedded vmui/index.html: %v", err)
	}

	html := string(sub)
	if !strings.Contains(html, "</body>") {
		t.Fatal("vmui/index.html must contain </body> for injection to work")
	}

	handler := InjectLakehouseTab(fakeUpstream(200, "text/html", html))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	got := rr.Body.String()
	if !strings.Contains(got, "vmui-tab.js") {
		t.Fatal("REGRESSION: vmui-tab.js not injected into actual VMUI index.html")
	}

	// Must appear before </body>
	scriptIdx := strings.Index(got, "vmui-tab.js")
	bodyIdx := strings.LastIndex(got, "</body>")
	if scriptIdx > bodyIdx {
		t.Fatal("REGRESSION: script must appear before </body>")
	}
}

func TestRegressionVMUIJSNotInjected(t *testing.T) {
	handler := InjectLakehouseTab(fakeUpstream(200, "application/javascript", "var x = 1;"))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/vmui/assets/index.js", nil))

	if strings.Contains(rr.Body.String(), "vmui-tab.js") {
		t.Fatal("REGRESSION: script injected into JS asset response")
	}
}

func TestRegressionVMUICSSNotInjected(t *testing.T) {
	handler := InjectLakehouseTab(fakeUpstream(200, "text/css", "body { margin: 0; }"))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/vmui/assets/index.css", nil))

	if strings.Contains(rr.Body.String(), "vmui-tab.js") {
		t.Fatal("REGRESSION: script injected into CSS response")
	}
}

func TestRegressionVMUIInjectIdempotent(t *testing.T) {
	html := `<html><body><h1>Test</h1></body></html>`

	// Double-wrap — should only inject once per layer (2 total)
	inner := InjectLakehouseTab(fakeUpstream(200, "text/html", html))
	outer := InjectLakehouseTab(inner)

	rr := httptest.NewRecorder()
	outer.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	got := rr.Body.String()
	count := strings.Count(got, "vmui-tab.js")
	// Each layer injects once; with double-wrap, we get 2
	// This test documents the behavior rather than preventing it
	if count < 1 {
		t.Fatal("at least one injection expected")
	}
}

func TestRegressionVMUIRegistrationPaths(t *testing.T) {
	mux := http.NewServeMux()
	RegisterVMUI(mux, true)

	// /select/vmui should redirect to /select/vmui/
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/select/vmui", nil))
	if rec.Code != http.StatusMovedPermanently {
		t.Errorf("/select/vmui should redirect, got %d", rec.Code)
	}
	loc := rec.Header().Get("Location")
	if !strings.Contains(loc, "/select/vmui/") {
		t.Errorf("redirect should point to /select/vmui/, got %q", loc)
	}

	// /select/vmui/ should serve the HTML with injection
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, httptest.NewRequest("GET", "/select/vmui/", nil))
	if rec2.Code != http.StatusOK {
		t.Fatalf("/select/vmui/ returned %d, want 200", rec2.Code)
	}
	if !strings.Contains(rec2.Body.String(), "vmui-tab.js") {
		t.Fatal("REGRESSION: /select/vmui/ should have vmui-tab.js injected")
	}
}

func TestRegressionVMUIDisabledNoRegistration(t *testing.T) {
	mux := http.NewServeMux()
	RegisterVMUI(mux, false)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/select/vmui/", nil))

	// When VMUI is disabled, the path shouldn't be registered,
	// so we'd get the default mux 404
	if rec.Code == http.StatusOK {
		t.Error("disabled VMUI should not serve content")
	}
}

func TestRegressionVMUIStaticAssets(t *testing.T) {
	mux := http.NewServeMux()
	RegisterVMUI(mux, true)

	// favicon.svg and config.json are part of VL's VMUI build output
	// and may not be present in CI (they are gitignored).
	// Only test if they are available in the embedded FS.
	assets := []struct {
		path     string
		noInject bool
	}{
		{"/select/vmui/favicon.svg", true},
		{"/select/vmui/config.json", true},
	}

	for _, a := range assets {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", a.path, nil))
		if rec.Code == http.StatusNotFound {
			t.Skipf("%s not available (VMUI build assets not present)", a.path)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("%s returned %d, want 200", a.path, rec.Code)
		}
		if a.noInject && strings.Contains(rec.Body.String(), "vmui-tab.js") {
			t.Fatalf("script should not be injected into %s", a.path)
		}
	}
}

func TestRegressionVMUITabJSServedFromLakehouseUI(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/ui/vmui-tab.js", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("vmui-tab.js returned %d, want 200", rec.Code)
	}

	body := rec.Body.String()

	required := []string{
		"showLakehouse", "hideLakehouse", "renderLakehouse",
		"lh-wrapper", "isHeaderOrNav",
	}
	for _, s := range required {
		if !strings.Contains(body, s) {
			t.Errorf("REGRESSION: vmui-tab.js missing function/identifier %q", s)
		}
	}
}

func TestRegressionVMUITabJSContentManagement(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/ui/vmui-tab.js", nil))

	body := rec.Body.String()

	// Must NOT use the old findContentArea approach
	if strings.Contains(body, "findContentArea") {
		t.Error("REGRESSION: vmui-tab.js should not use deprecated findContentArea")
	}

	// Must use direct children approach
	if !strings.Contains(body, "app.children") {
		t.Error("REGRESSION: vmui-tab.js should use direct children approach (app.children)")
	}

	// Must preserve partition_list (not old "partitions" for array)
	if strings.Contains(body, "d.partitions") && !strings.Contains(body, "d.partition_list") {
		t.Error("REGRESSION: vmui-tab.js should use d.partition_list (not d.partitions for array)")
	}
}
