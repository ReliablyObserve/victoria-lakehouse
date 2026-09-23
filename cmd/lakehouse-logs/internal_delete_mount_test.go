package main

import (
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
)

// upstream's answer while -internaldelete.enable is off (vlselect/main.go).
const upstreamInternalDeleteDisabled = "requests to /internal/delete/* are disabled; pass -internaldelete.enable command-line flag for enabling them; " +
	"see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs\n"

// The binary mounts /internal/delete/* on upstream's vlselect.RequestHandler,
// never straight onto internalselect: with the defaults every delete path gets
// upstream's own "disabled" answer, exactly like a VictoriaLogs node started
// without -internaldelete.enable — whatever delete.enabled says.
func TestMountInternalProtocol_DeleteIsGatedByDefault(t *testing.T) {
	for _, deleteEnabled := range []bool{true, false} {
		mux := http.NewServeMux()
		mountInternalProtocol(mux, deleteEnabled)

		for _, path := range []string{"/internal/delete/run_task", "/internal/delete/stop_task", "/internal/delete/active_tasks"} {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, boundedRequest(t, path))
			if rec.Code != http.StatusBadRequest || rec.Body.String() != upstreamInternalDeleteDisabled {
				t.Fatalf("delete.enabled=%v %s: got %d %q, want upstream's disabled answer", deleteEnabled, path, rec.Code, rec.Body.String())
			}
		}
	}
}

// The flag is upstream's own registration, with upstream's default.
func TestInternalDeleteFlag_IsUpstreams(t *testing.T) {
	f := flag.Lookup(internaldelete.FlagName)
	if f == nil || f.DefValue != "false" {
		t.Fatalf("-%s = %+v, want upstream's flag with default false", internaldelete.FlagName, f)
	}
}

func TestMountInternalProtocol_DeleteNeedsTheDeleteFeature(t *testing.T) {
	if err := flag.Set(internaldelete.FlagName, "true"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set(internaldelete.FlagName, "false") })
	mux := http.NewServeMux()
	mountInternalProtocol(mux, false)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, boundedRequest(t, "/internal/delete/run_task"))
	if rec.Code != http.StatusBadRequest || rec.Body.String() != internaldelete.DeleteDisabledMessage+"\n" {
		t.Fatalf("got %d %q, want the delete.enabled refusal", rec.Code, rec.Body.String())
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
