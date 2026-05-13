package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestUIHandlerServesIndex(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/lakehouse/ui/", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Lakehouse Explorer") {
		t.Fatalf("expected body to contain 'Lakehouse Explorer', got:\n%s", body[:min(len(body), 500)])
	}
}

func TestUIHandlerServesVMUITabJS(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/lakehouse/ui/vmui-tab.js", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Lakehouse") {
		t.Fatalf("expected body to contain 'Lakehouse', got:\n%s", body[:min(len(body), 500)])
	}
}

func TestUIHandlerDisabled(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: false})
	mux := http.NewServeMux()
	h.Register(mux)

	paths := []string{"/lakehouse/ui/", "/lakehouse/ui/vmui-tab.js", "/lakehouse/ui/index.html"}
	for _, path := range paths {
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest("GET", path, nil))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("path %s: expected 404 when disabled, got %d", path, rr.Code)
		}
	}
}

func TestUIHandlerTrailingSlash(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/lakehouse/ui", nil)
	mux.ServeHTTP(rr, req)

	// A redirect (301, 307, 308) to /lakehouse/ui/ or a direct 200 is acceptable.
	switch rr.Code {
	case http.StatusOK, http.StatusMovedPermanently, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		// acceptable
	default:
		t.Fatalf("expected 200 or redirect for /lakehouse/ui, got %d", rr.Code)
	}
}

func TestUIHandlerNotFoundOther(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/lakehouse/ui/nonexistent", nil))

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonexistent file, got %d", rr.Code)
	}
}

func TestUIHandlerIndexContentType(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/lakehouse/ui/", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	ct := rr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("expected text/html content type, got %q", ct)
	}
}

func TestUIHandlerJSContentType(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/lakehouse/ui/vmui-tab.js", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	ct := rr.Header().Get("Content-Type")
	// Accept any JavaScript-related content type.
	if !strings.Contains(ct, "javascript") && !strings.Contains(ct, "ecmascript") {
		t.Fatalf("expected JavaScript content type, got %q", ct)
	}
}

func TestUIHandlerConcurrent(t *testing.T) {
	h := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	h.Register(mux)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errs := make(chan string, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, httptest.NewRequest("GET", "/lakehouse/ui/", nil))
			if rr.Code != http.StatusOK {
				errs <- "expected 200, got non-200"
			}
			if !strings.Contains(rr.Body.String(), "Lakehouse Explorer") {
				errs <- "missing Lakehouse Explorer in response"
			}
		}()
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		t.Fatal(e)
	}
}
