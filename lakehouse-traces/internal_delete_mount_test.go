package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

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
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
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
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/delete/run_task", nil))
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

	wantFlag := `flag.Bool("internaldelete.enable", false, "Whether to enable /internal/delete/* HTTP endpoints, which are used by vtselect for deleting spans via delete API at vtstorage nodes")`
	if !strings.Contains(joined, wantFlag) {
		t.Errorf("VT's -internaldelete.enable registration changed; update internal_delete.go\nwant: %s", wantFlag)
	}
	wantAnswer := `httpserver.Errorf(w, r, "` + internalDeleteDisabledMessage + `")`
	if !strings.Contains(joined, wantAnswer) {
		t.Errorf("VT's /internal/delete/* disabled answer changed; update internal_delete.go\nwant: %s", wantAnswer)
	}
	if !strings.Contains(joined, `if !*enableInternalDelete {`) || !strings.Contains(joined, `internalselect.RequestHandler(r.Context(), w, r)`) {
		t.Error("VT's /internal/delete/ branch no longer gates on the flag then calls internalselect; re-check internal_delete.go")
	}
}
