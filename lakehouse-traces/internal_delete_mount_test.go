package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
)

// The binary must mount /internal/delete/* behind the gate, never straight
// onto internalselect: with the defaults every delete path answers upstream's
// "disabled" error, exactly like a VL node started without -internaldelete.enable.
func TestMountInternalProtocol_DeleteIsGatedByDefault(t *testing.T) {
	mux := http.NewServeMux()
	mountInternalProtocol(mux, internaldelete.Enabled(), true)

	for _, path := range []string{"/internal/delete/run_task", "/internal/delete/stop_task", "/internal/delete/active_tasks"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		if rec.Code != http.StatusBadRequest || rec.Body.String() != internaldelete.DisabledMessage+"\n" {
			t.Fatalf("%s: got %d %q, want upstream's disabled answer", path, rec.Code, rec.Body.String())
		}
	}
}

func TestMountInternalProtocol_DeleteNeedsTheDeleteFeature(t *testing.T) {
	mux := http.NewServeMux()
	mountInternalProtocol(mux, true, false)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/delete/run_task", nil))
	if rec.Code != http.StatusBadRequest || rec.Body.String() != internaldelete.DeleteDisabledMessage+"\n" {
		t.Fatalf("got %d %q, want the delete.enabled refusal", rec.Code, rec.Body.String())
	}
}
