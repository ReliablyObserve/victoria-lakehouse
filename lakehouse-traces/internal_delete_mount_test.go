package main

import (
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
)

// With the defaults every delete path gets VT's own "disabled" answer, exactly
// like a VictoriaTraces node started without -internaldelete.enable — whatever
// delete.enabled says.
func TestMountInternalProtocol_DeleteIsGatedByDefault(t *testing.T) {
	for _, deleteEnabled := range []bool{true, false} {
		mux := http.NewServeMux()
		mountInternalProtocol(mux, deleteEnabled)

		for _, path := range []string{"/internal/delete/run_task", "/internal/delete/stop_task", "/internal/delete/active_tasks"} {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, boundedRequest(t, path))
			if rec.Code != http.StatusBadRequest || rec.Body.String() != internalDeleteDisabledMessage+"\n" {
				t.Fatalf("delete.enabled=%v %s: got %d %q, want upstream's disabled answer", deleteEnabled, path, rec.Code, rec.Body.String())
			}
		}
	}
}

func TestMountInternalProtocol_DeleteNeedsTheDeleteFeature(t *testing.T) {
	*internalDeleteEnable = true
	t.Cleanup(func() { *internalDeleteEnable = false })
	mux := http.NewServeMux()
	mountInternalProtocol(mux, false)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, boundedRequest(t, "/internal/delete/run_task"))
	if rec.Code != http.StatusBadRequest || rec.Body.String() != internaldelete.DeleteDisabledMessage+"\n" {
		t.Fatalf("got %d %q, want the delete.enabled refusal", rec.Code, rec.Body.String())
	}
	if !internaldelete.FlagEnabled() {
		t.Fatal("internaldelete.FlagEnabled() does not see this binary's -internaldelete.enable")
	}
}

// internal_delete.go is a verbatim copy of VT's gate until this binary can
// mount vtselect.RequestHandler. Fail the moment the vendored VT source stops
// carrying the same flag help, default or answer, so the copy never drifts.
func TestUpstreamInternalDelete_MatchesVendoredVTSelect(t *testing.T) {
	src, err := os.ReadFile("deps/VictoriaTraces/app/vtselect/main.go")
	if err != nil {
		t.Fatalf("vendored VictoriaTraces source missing (run make deps-vt): %v", err)
	}
	// Fold Go string concatenation ("a "+\n "b") so literals compare whole.
	joined := regexp.MustCompile(`"\s*\+\s*"`).ReplaceAllString(string(src), "")

	// Build the expectation from what THIS binary registers, so a change to
	// the copy (not only to upstream) fails the test.
	f := flag.Lookup("internaldelete.enable")
	if f == nil {
		t.Fatal("-internaldelete.enable is not registered in lakehouse-traces")
	}
	wantFlag := `flag.Bool("internaldelete.enable", ` + f.DefValue + `, "` + f.Usage + `")`
	if !strings.Contains(joined, wantFlag) {
		t.Errorf("VT's -internaldelete.enable registration changed; update internal_delete.go\nwant: %s", wantFlag)
	}
	wantAnswer := `httpserver.Errorf(w, r, "` + internalDeleteDisabledMessage + `")`
	if !strings.Contains(joined, wantAnswer) {
		t.Errorf("VT's /internal/delete/* disabled answer changed; update internal_delete.go\nwant: %s", wantAnswer)
	}
	if !strings.Contains(joined, `if strings.HasPrefix(path, "/internal/delete/") {`) ||
		!strings.Contains(joined, `if !*enableInternalDelete {`) || !strings.Contains(joined, `internalselect.RequestHandler(r.Context(), w, r)`) {
		t.Error("VT's /internal/delete/ branch no longer gates on the flag then calls internalselect; re-check internal_delete.go")
	}
}

// boundedRequest carries a deadline: if a regression let the request through to
// internalselect (whose concurrency gate waits on the request context), the test
// fails in seconds with the wrong answer instead of hanging until the go test
// timeout.
func boundedRequest(t *testing.T, path string) *http.Request {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return httptest.NewRequest(http.MethodPost, path, nil).WithContext(ctx)
}
