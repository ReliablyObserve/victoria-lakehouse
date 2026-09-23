package internaldelete

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandler_DisabledByDefaultAnswersLikeUpstream(t *testing.T) {
	reached := false
	h := Handler(false, true, func(http.ResponseWriter, *http.Request) { reached = true })
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/internal/delete/run_task", nil))

	if reached {
		t.Fatal("the protocol handler ran although -internaldelete.enable is off")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d (upstream httpserver.Errorf default)", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), DisabledMessage+"\n"; got != want {
		t.Fatalf("body = %q, want upstream's %q", got, want)
	}
}

func TestHandler_FlagOnButDeleteFeatureOffIsRefused(t *testing.T) {
	reached := false
	h := Handler(true, false, func(http.ResponseWriter, *http.Request) { reached = true })
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/internal/delete/run_task", nil))

	if reached {
		t.Fatal("the protocol handler ran although delete.enabled is off")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), DeleteDisabledMessage+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestHandler_BothOnReachesTheProtocolHandler(t *testing.T) {
	reached := false
	h := Handler(true, true, func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusNoContent)
	})
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/internal/delete/active_tasks", nil))

	if !reached {
		t.Fatal("the protocol handler did not run with both switches on")
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want the protocol handler's %d", rec.Code, http.StatusNoContent)
	}
}

// The flag must keep upstream's name and default, or a VL/VT command line
// stops meaning the same thing on the lakehouse.
func TestFlag_MatchesUpstream(t *testing.T) {
	f := flag.Lookup("internaldelete.enable")
	if f == nil {
		t.Fatal("-internaldelete.enable is not registered")
	}
	if f.DefValue != "false" {
		t.Fatalf("default = %q, want upstream's false", f.DefValue)
	}
	if Enabled() {
		t.Fatal("Enabled() = true with the flag at its default")
	}
}
