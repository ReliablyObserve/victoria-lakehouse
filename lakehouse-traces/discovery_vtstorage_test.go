package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
)

// Hot-boundary discovery (internal/discovery, issue #250) polls a storage node's
// /internal/partition/list. VictoriaTraces v0.12.0's vtstorage answers 405 to
// every non-POST /internal/* request, so the poll has to be a POST. This test
// runs the REAL vendored vtstorage.RequestHandler over a real local storage (a
// temporary -storageDataPath, patched flags and all) and drives it with the
// production discovery client: GET is refused, POST works, and the client
// derives the hot boundary from the partition it listed.
func TestDiscoveryAgainstRealVTStorage_POSTOnly(t *testing.T) {
	t.Chdir(t.TempDir()) // vtstorage's default -storageDataPath is relative
	vtstorage.Init()
	t.Cleanup(vtstorage.Stop)

	// One row so the storage has a day partition to list.
	ts := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	lr.MustAdd(logstorage.TenantID{}, ts.UnixNano(), []logstorage.Field{{Name: "_msg", Value: "x"}}, 0)
	(&vtstorage.Storage{}).MustAddRows(lr)
	logstorage.PutLogRows(lr)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !vtstorage.RequestHandler(w, r) {
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	do := func(method string) (int, string) {
		req, err := http.NewRequest(method, srv.URL+"/internal/partition/list", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	// What discovery did before the fix: a GET. v0.12.0 refuses it.
	if code, body := do(http.MethodGet); code != http.StatusMethodNotAllowed || !strings.Contains(body, "Only POST method is allowed; got GET.") {
		t.Fatalf("GET /internal/partition/list: got %d %q, want 405 \"Only POST method is allowed; got GET.\"", code, body)
	}
	// What it does now.
	code, body := do(http.MethodPost)
	if code != http.StatusOK {
		t.Fatalf("POST /internal/partition/list: got %d %q, want 200", code, body)
	}
	var dates []string
	if err := json.Unmarshal([]byte(body), &dates); err != nil || len(dates) != 1 || dates[0] != "20260929" {
		t.Fatalf("POST /internal/partition/list body = %q (err %v), want [\"20260929\"]", body, err)
	}

	// The production client, end to end.
	d := discovery.New("", []string{srv.Listener.Addr().String()}, "", "", "9428", 5*time.Second)
	if _, err := d.DiscoverStorageNodes(context.Background()); err != nil {
		t.Fatal(err)
	}
	boundary, err := d.PollPartitionList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if boundary == nil {
		t.Fatal("PollPartitionList found no hot boundary against the real v0.12.0 vtstorage: the client must POST")
	}
	if boundary.MinDate != "20260929" || boundary.MaxDate != "20260929" {
		t.Errorf("boundary = %+v, want 20260929..20260929", boundary)
	}
}

// The other vtstorage admin routes refuse non-POST too (v0.12.0, SSRF guard).
// LH traces does not serve them (the registry rows say "unsupported"), but the
// upstream a Lakehouse-fronted stack is compared against does, so pin it.
func TestRealVTStorage_InternalRoutesRefuseNonPOST(t *testing.T) {
	t.Chdir(t.TempDir())
	vtstorage.Init()
	t.Cleanup(vtstorage.Stop)

	for _, path := range []string{
		"/internal/force_merge", "/internal/force_flush", "/internal/log_new_streams",
		"/internal/partition/attach", "/internal/partition/detach", "/internal/partition/list",
		"/internal/partition/snapshot/create", "/internal/partition/snapshot/list",
		"/internal/partition/snapshot/delete", "/internal/partition/snapshot/delete_stale",
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete} {
			rec := httptest.NewRecorder()
			handled := vtstorage.RequestHandler(rec, httptest.NewRequest(method, path, nil))
			if !handled || rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: handled=%v code=%d, want handled with 405", method, path, handled, rec.Code)
			}
		}
	}
}

// vtstorage's auth-key flags (-forceFlushAuthKey, -partitionManageAuthKey, ...)
// are the SAME flags VictoriaLogs' vlstorage registers in this binary: the
// vtstorage-flag-dedup helpers hand VictoriaTraces VictoriaLogs' value, not a
// detached copy. A detached copy would read empty whatever the operator set,
// and a mounted vtstorage would then answer without the key (fail open).
func TestRealVTStorage_AuthKeyIsTheSharedFlag(t *testing.T) {
	t.Chdir(t.TempDir())
	vtstorage.Init()
	t.Cleanup(vtstorage.Stop)

	if flag.Lookup("partitionManageAuthKey") == nil {
		t.Fatal("-partitionManageAuthKey is not registered")
	}
	if err := flag.Set("partitionManageAuthKey", "s3cret"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set("partitionManageAuthKey", "") })

	call := func(query string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/internal/partition/list"+query, nil)
		if !vtstorage.RequestHandler(rec, req) {
			t.Fatal("not handled")
		}
		return rec.Code
	}
	if code := call(""); code != http.StatusUnauthorized {
		t.Errorf("no key: got %d, want 401: the flag value set on the command line did not reach vtstorage", code)
	}
	if code := call("?authKey=wrong"); code != http.StatusUnauthorized {
		t.Errorf("wrong key: got %d, want 401", code)
	}
	if code := call("?authKey=s3cret"); code != http.StatusOK {
		t.Errorf("right key: got %d, want 200", code)
	}
}
