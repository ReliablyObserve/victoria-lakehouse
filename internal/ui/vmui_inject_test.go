package ui

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// helper: create an upstream handler that writes the given body with the given content type and status.
func fakeUpstream(status int, contentType string, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		w.Write([]byte(body)) //nolint:errcheck
	})
}

func TestInjectLakehouseTabHTML(t *testing.T) {
	html := `<html><head></head><body><h1>Hello</h1></body></html>`
	handler := InjectLakehouseTab(fakeUpstream(200, "text/html", html))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	got := rr.Body.String()
	if !strings.Contains(got, injectScript) {
		t.Fatalf("expected injected script in response, got:\n%s", got)
	}
	if !strings.Contains(got, "</body>") {
		t.Fatalf("expected </body> in response, got:\n%s", got)
	}
}

func TestInjectLakehouseTabPassthroughNonHTML(t *testing.T) {
	jsBody := `function hello() { return 1; }`
	handler := InjectLakehouseTab(fakeUpstream(200, "application/javascript", jsBody))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/app.js", nil))

	got := rr.Body.String()
	if got != jsBody {
		t.Fatalf("expected passthrough, got:\n%s", got)
	}
	if strings.Contains(got, injectScript) {
		t.Fatalf("script should not be injected into JS")
	}
}

func TestInjectLakehouseTabPreservesStatusCode(t *testing.T) {
	html := `<html><body>Not Found</body></html>`
	handler := InjectLakehouseTab(fakeUpstream(404, "text/html", html))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/missing", nil))

	if rr.Code != 404 {
		t.Fatalf("expected status 404, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), injectScript) {
		t.Fatalf("expected script injection in 404 HTML response")
	}
}

func TestInjectLakehouseTabNoBody(t *testing.T) {
	html := `<html><head><title>No body tag</title></head></html>`
	handler := InjectLakehouseTab(fakeUpstream(200, "text/html", html))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	got := rr.Body.String()
	if got != html {
		t.Fatalf("expected passthrough for HTML without </body>, got:\n%s", got)
	}
	if strings.Contains(got, injectScript) {
		t.Fatalf("script should not be injected when no </body> tag")
	}
}

func TestInjectLakehouseTabContentLengthUpdated(t *testing.T) {
	html := `<html><body>Test</body></html>`
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(html)))
		w.WriteHeader(200)
		w.Write([]byte(html)) //nolint:errcheck
	})
	handler := InjectLakehouseTab(upstream)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	cl := rr.Header().Get("Content-Length")
	if cl == fmt.Sprintf("%d", len(html)) {
		t.Fatalf("Content-Length should have been removed or updated, still %s", cl)
	}
}

func TestInjectLakehouseTabPassthroughImages(t *testing.T) {
	imgData := "\x89PNG\r\n\x1a\nfakeimage"
	handler := InjectLakehouseTab(fakeUpstream(200, "image/png", imgData))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/logo.png", nil))

	got := rr.Body.String()
	if got != imgData {
		t.Fatalf("expected passthrough for image/png")
	}
	if strings.Contains(got, injectScript) {
		t.Fatalf("script should not be injected into images")
	}
}

func TestInjectLakehouseTabPassthroughJSON(t *testing.T) {
	jsonBody := `{"status":"ok"}`
	handler := InjectLakehouseTab(fakeUpstream(200, "application/json", jsonBody))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/api/health", nil))

	got := rr.Body.String()
	if got != jsonBody {
		t.Fatalf("expected passthrough for application/json, got:\n%s", got)
	}
}

func TestInjectLakehouseTabEmptyResponse(t *testing.T) {
	handler := InjectLakehouseTab(fakeUpstream(200, "text/html", ""))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	got := rr.Body.String()
	if got != "" {
		t.Fatalf("expected empty passthrough, got:\n%s", got)
	}
}

func TestInjectLakehouseTabMultipleBodyTags(t *testing.T) {
	// Unusual HTML with multiple </body> tags — inject before the LAST one.
	html := `<html><body>First</body><body>Second</body></html>`
	handler := InjectLakehouseTab(fakeUpstream(200, "text/html", html))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	got := rr.Body.String()
	// The script should appear before the last </body>.
	lastBody := strings.LastIndex(got, "</body>")
	scriptIdx := strings.LastIndex(got, injectScript)
	if scriptIdx < 0 {
		t.Fatalf("expected script injection, got:\n%s", got)
	}
	if scriptIdx > lastBody {
		t.Fatalf("script should appear before the last </body>, script at %d, last </body> at %d", scriptIdx, lastBody)
	}

	// Ensure the first </body> is untouched — script should NOT appear before it.
	firstBody := strings.Index(got, "</body>")
	if scriptIdx < firstBody {
		t.Fatalf("script should not be before the first </body>")
	}
}

func TestInjectLakehouseTabPreservesHeaders(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("X-Custom-Header", "preserved")
		w.WriteHeader(200)
		w.Write([]byte(`<html><body>Hi</body></html>`)) //nolint:errcheck
	})
	handler := InjectLakehouseTab(upstream)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	if rr.Header().Get("Cache-Control") != "max-age=3600" {
		t.Fatalf("Cache-Control header not preserved")
	}
	if rr.Header().Get("X-Custom-Header") != "preserved" {
		t.Fatalf("X-Custom-Header not preserved")
	}
}

func TestInjectLakehouseTabScriptPosition(t *testing.T) {
	html := `<html><body><p>Content</p></body></html>`
	handler := InjectLakehouseTab(fakeUpstream(200, "text/html", html))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	got := rr.Body.String()
	expected := `<p>Content</p>` + injectScript + "\n" + `</body>`
	if !strings.Contains(got, expected) {
		t.Fatalf("script should be directly before </body>, got:\n%s", got)
	}
}

func TestInjectLakehouseTabLargeHTML(t *testing.T) {
	// Build a ~100KB HTML response.
	var sb strings.Builder
	sb.WriteString("<html><body>")
	for sb.Len() < 100*1024 {
		sb.WriteString("<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit.</p>\n")
	}
	sb.WriteString("</body></html>")
	html := sb.String()

	handler := InjectLakehouseTab(fakeUpstream(200, "text/html", html))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))

	got := rr.Body.String()
	if !strings.Contains(got, injectScript) {
		t.Fatalf("expected script injection in large HTML response")
	}
	if len(got) <= len(html) {
		t.Fatalf("modified response should be larger than original: got %d, original %d", len(got), len(html))
	}
}
