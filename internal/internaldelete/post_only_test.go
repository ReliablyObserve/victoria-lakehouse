package internaldelete

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Any method but POST on a cluster-protocol prefix gets upstream's bare 405
// (no body) without reaching the handler; POST passes; while upstream's gate is
// closed the request is left to upstream's own "disabled" answer.
func TestPOSTOnly(t *testing.T) {
	for _, tc := range []struct {
		name        string
		gate        func() bool
		method      string
		wantReached bool
	}{
		{"GET refused", on, http.MethodGet, false},
		{"HEAD refused", on, http.MethodHead, false},
		{"PUT refused", on, http.MethodPut, false},
		{"DELETE refused", on, http.MethodDelete, false},
		{"PATCH refused", on, http.MethodPatch, false},
		{"POST served", on, http.MethodPost, true},
		{"nil gate is always open: GET refused", nil, http.MethodGet, false},
		{"nil gate: POST served", nil, http.MethodPost, true},
		{"gate closed: GET left to upstream", off, http.MethodGet, true},
		{"gate closed: POST left to upstream", off, http.MethodPost, true},
	} {
		reached := false
		rec := httptest.NewRecorder()
		POSTOnly(tc.gate, upstreamStub(&reached))(rec, httptest.NewRequest(tc.method, "/internal/delete/run_task?filter=*", nil))
		if reached != tc.wantReached {
			t.Errorf("%s: handler reached=%v, want %v", tc.name, reached, tc.wantReached)
		}
		if !tc.wantReached && (rec.Code != http.StatusMethodNotAllowed || rec.Body.Len() != 0) {
			t.Errorf("%s: got %d %q, want a bare 405", tc.name, rec.Code, rec.Body.String())
		}
	}
}
