package internaldelete

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// /delete/run_task is POST-only once upstream's -delete.enable is on, with
// upstream's exact 405 answer and without reaching the delete handler; the
// other methods and paths are untouched, and while the flag is off the request
// is left to upstream's own "disabled" answer.
func TestRunTaskPOSTOnly(t *testing.T) {
	for _, tc := range []struct {
		name        string
		flagOn      func() bool
		method      string
		path        string
		wantReached bool
		wantBody    string
	}{
		{"GET refused", on, http.MethodGet, "/delete/run_task", false, "Only POST method is allowed; got GET.\n"},
		{"HEAD refused", on, http.MethodHead, "/delete/run_task", false, "Only POST method is allowed; got HEAD.\n"},
		{"PUT refused", on, http.MethodPut, "/delete/run_task", false, "Only POST method is allowed; got PUT.\n"},
		{"DELETE refused", on, http.MethodDelete, "/delete/run_task", false, "Only POST method is allowed; got DELETE.\n"},
		{"doubled slash refused like upstream normalises it", on, http.MethodGet, "/delete//run_task", false, "Only POST method is allowed; got GET.\n"},
		{"POST served", on, http.MethodPost, "/delete/run_task", true, ""},
		{"GET stop_task unchanged", on, http.MethodGet, "/delete/stop_task", true, ""},
		{"GET active_tasks unchanged", on, http.MethodGet, "/delete/active_tasks", true, ""},
		{"GET on another path unchanged", on, http.MethodGet, "/delete/anything", true, ""},
		{"flag off: upstream answers, whatever the method", off, http.MethodGet, "/delete/run_task", true, ""},
	} {
		reached := false
		rec := httptest.NewRecorder()
		RunTaskPOSTOnly(tc.flagOn, upstreamStub(&reached))(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if reached != tc.wantReached {
			t.Errorf("%s: upstream reached=%v, want %v", tc.name, reached, tc.wantReached)
		}
		if !tc.wantReached && (rec.Code != http.StatusMethodNotAllowed || rec.Body.String() != tc.wantBody) {
			t.Errorf("%s: got %d %q, want 405 %q", tc.name, rec.Code, rec.Body.String(), tc.wantBody)
		}
	}
}
