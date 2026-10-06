package internaldelete

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
)

// A binary that does not register -deleteAuthKey (the traces binary: its
// VictoriaLogs revision and VictoriaTraces v0.12.0 predate the flag) is passed
// through unchanged, whatever the path.
func TestDeleteAuth_WithoutTheFlagPassesEverythingThrough(t *testing.T) {
	for _, p := range []string{"/delete/run_task", "/delete/logsql/delete", "/delete/tracessql/delete", "/select/logsql/query"} {
		reached := false
		rec := httptest.NewRecorder()
		DeleteAuth(upstreamStub(&reached)).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, p, nil))
		if !reached || rec.Code != http.StatusTeapot {
			t.Errorf("%s: reached=%v status=%d, want the handler to answer", p, reached, rec.Code)
		}
	}
}

// With the flag registered (as upstream's vlselect does) and set, only /delete/*
// asks for the key, upstream's check answers (401 with its text), and the
// handler is reached only with the right key. The path is normalised like
// upstream does it ("//" -> "/").
func TestDeleteAuth_FlagSet(t *testing.T) {
	key := flagutil.NewPassword(DeleteAuthKeyFlagName, "test copy of upstream's flag")
	if err := key.Set("s3cret"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = key.Set("") })

	for _, tc := range []struct {
		name, target string
		wantReached  bool
		wantCode     int
		wantBody     string
	}{
		{"no key", "/delete/run_task", false, http.StatusUnauthorized, "Expected to receive non-empty authKey when -deleteAuthKey is set\n"},
		{"wrong key", "/delete/run_task?authKey=nope", false, http.StatusUnauthorized, "The provided authKey doesn't match -deleteAuthKey\n"},
		{"right key", "/delete/run_task?authKey=s3cret", true, http.StatusTeapot, ""},
		{"doubled slash is still /delete/", "/delete//run_task", false, http.StatusUnauthorized, "Expected to receive non-empty authKey when -deleteAuthKey is set\n"},
		{"the lakehouse API too", "/delete/logsql/tombstones", false, http.StatusUnauthorized, "Expected to receive non-empty authKey when -deleteAuthKey is set\n"},
		{"other paths never ask", "/select/logsql/query", true, http.StatusTeapot, ""},
		{"a look-alike prefix never asks", "/deleted/x", true, http.StatusTeapot, ""},
	} {
		reached := false
		rec := httptest.NewRecorder()
		DeleteAuth(upstreamStub(&reached)).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, tc.target, nil))
		if reached != tc.wantReached || rec.Code != tc.wantCode || (tc.wantBody != "" && rec.Body.String() != tc.wantBody) {
			t.Errorf("%s: reached=%v status=%d body=%q; want reached=%v status=%d body=%q", tc.name, reached, rec.Code, rec.Body.String(), tc.wantReached, tc.wantCode, tc.wantBody)
		}
	}
}
