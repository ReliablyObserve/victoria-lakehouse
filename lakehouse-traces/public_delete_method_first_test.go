package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// The check is in processDeleteRunTaskRequest ahead of tenant and filter
// parsing: a non-POST request with no filter, a filter that does not parse, or a
// tenant header that does not parse still gets 405, never a parsing error.
func TestProcessDeleteRunTaskRequest_MethodCheckPrecedesParsing(t *testing.T) {
	enablePublicDelete(t)
	for name, tc := range map[string]struct {
		form    url.Values
		headers map[string]string
	}{
		"no tenant, no filter":  {nil, nil},
		"bad filter":            {url.Values{"filter": {"level:("}}, nil},
		"bad tenant header":     {url.Values{"filter": {"*"}}, map[string]string{"AccountID": "not-a-number"}},
		"bad tenant and filter": {url.Values{"filter": {"level:("}}, map[string]string{"AccountID": "not-a-number"}},
	} {
		for _, method := range nonPOST {
			// Through the mux, and calling the handler directly.
			store, srv := postOnlyServer(t, true)
			rec := queryRequest(srv, method, "/delete/run_task", tc.form, tc.headers)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s via mux: got %d %q, want 405 before any parsing", name, method, rec.Code, rec.Body.String())
			}
			if store.Count() != 0 {
				t.Errorf("%s %s: a task was created", name, method)
			}

			target := "/delete/run_task"
			if len(tc.form) > 0 {
				target += "?" + tc.form.Encode()
			}
			req := httptest.NewRequest(method, target, nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			direct := httptest.NewRecorder()
			processDeleteRunTaskRequest(req.Context(), direct, req)
			if direct.Code != http.StatusMethodNotAllowed || direct.Body.String() != "Only POST method is allowed; got "+method+".\n" {
				t.Errorf("%s %s direct: got %d %q, want 405", name, method, direct.Code, direct.Body.String())
			}
		}
	}
}
