package main

import (
	"encoding/json"
	"flag"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// -deleteAuthKey (VictoriaLogs v1.53.0, issue #1749) protects every /delete/*
// request with a dedicated authKey, and overrides -httpAuth.* there (issue
// #1764). The flag is registered by upstream's vlselect package; the logs binary
// serves upstream's delete API through vlselect.RequestHandler and its own
// /delete/logsql/* API next to it, and internaldelete.DeleteAuth puts the same
// check first on both.

// setFlag sets a flag whose default is empty (every flag set here) and restores
// it. The value is not read back: a flagutil.Password prints as "secret".
func setFlag(t *testing.T, name, value string) {
	t.Helper()
	if err := flag.Set(name, value); err != nil {
		t.Fatalf("-%s=%s: %v", name, value, err)
	}
	t.Cleanup(func() { _ = flag.Set(name, "") })
}

// deleteAPIs mounts both delete APIs the logs binary serves: upstream's public
// one and the lakehouse's own /delete/logsql/*, behind DeleteAuth.
func deleteAPIs(t *testing.T) (*delete.TombstoneStore, http.Handler) {
	t.Helper()
	store := delete.NewTombstoneStore()
	internalvlstorage.SetStorage(nopStorage{}, store)
	mux := http.NewServeMux()
	mountPublicDelete(mux, true, testGlobalRead)
	delete.NewHandler(store, &manifestQuerierAdapter{m: manifest.New("test-bucket", "")}, delete.NewStorageClassDetector(nil),
		&config.DeleteConfig{Enabled: true, DefaultMode: "hide"}, "logs").Register(mux)
	return store, internaldelete.DeleteAuth(mux)
}

const (
	missingKeyAnswer = "Expected to receive non-empty authKey when -deleteAuthKey is set\n"
	wrongKeyAnswer   = "The provided authKey doesn't match -deleteAuthKey\n"
)

func TestDeleteAuthKey_ProtectsBothDeleteAPIs(t *testing.T) {
	enablePublicDelete(t)
	setFlag(t, internaldelete.DeleteAuthKeyFlagName, "s3cret")

	for _, tc := range []struct {
		name, method, path string
		form               url.Values
	}{
		{"upstream run_task", http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}}},
		{"upstream stop_task", http.MethodPost, "/delete/stop_task", url.Values{"task_id": {"t1"}}},
		{"upstream active_tasks", http.MethodGet, "/delete/active_tasks", nil},
		{"upstream unknown path", http.MethodGet, "/delete/anything", nil},
		{"doubled slash is normalised like upstream", http.MethodPost, "/delete//run_task", url.Values{"filter": {"*"}}},
		{"lakehouse tombstones", http.MethodGet, "/delete/logsql/tombstones", nil},
		{"lakehouse delete", http.MethodPost, "/delete/logsql/delete", url.Values{"query": {"*"}, "start": {"0"}, "end": {"1"}}},
	} {
		store, srv := deleteAPIs(t)

		// Every argument in the URL query, no body: the SSRF shape, and the only
		// one a GET can carry.
		rec := queryRequest(srv, tc.method, tc.path, tc.form, nil)
		if rec.Code != http.StatusUnauthorized || rec.Body.String() != missingKeyAnswer {
			t.Errorf("%s without authKey: got %d %q, want 401 %q", tc.name, rec.Code, rec.Body.String(), missingKeyAnswer)
		}

		withKey := func(key string) url.Values {
			f := url.Values{}
			for k, v := range tc.form {
				f[k] = v
			}
			f.Set("authKey", key)
			return f
		}
		rec = queryRequest(srv, tc.method, tc.path, withKey("wrong"), nil)
		if rec.Code != http.StatusUnauthorized || rec.Body.String() != wrongKeyAnswer {
			t.Errorf("%s with a wrong authKey: got %d %q, want 401 %q", tc.name, rec.Code, rec.Body.String(), wrongKeyAnswer)
		}
		if store.Count() != 0 {
			t.Errorf("%s: a refused request created %d tombstone(s)", tc.name, store.Count())
		}

		rec = queryRequest(srv, tc.method, tc.path, withKey("s3cret"), nil)
		if rec.Code == http.StatusUnauthorized {
			t.Errorf("%s with the right authKey: refused with 401 %q", tc.name, rec.Body.String())
		}
	}
}

// The right authKey runs the task like before; and the key guards nothing else:
// paths outside /delete/* never ask for it.
func TestDeleteAuthKey_RightKeyRunsTheTaskAndOtherPathsAreFree(t *testing.T) {
	enablePublicDelete(t)
	setFlag(t, internaldelete.DeleteAuthKeyFlagName, "s3cret")
	store, srv := deleteAPIs(t)
	rec := publicDeleteRequest(srv, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}, "authKey": {"s3cret"}}, map[string]string{"AccountID": "7"})
	if rec.Code != http.StatusOK || store.Count() != 1 {
		t.Fatalf("run_task with the key: %d %q, %d tombstones; want 200 and one task", rec.Code, rec.Body.String(), store.Count())
	}
	h := internaldelete.DeleteAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }))
	for _, p := range []string{"/select/logsql/query", "/internal/delete/run_task", "/deleted", "/health"} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, p, nil))
		if rr.Code != http.StatusTeapot {
			t.Errorf("%s asked for the delete key: %d", p, rr.Code)
		}
	}
}

// Without the flag nothing changes: no key is asked for.
func TestDeleteAuthKey_UnsetAsksForNothing(t *testing.T) {
	enablePublicDelete(t)
	store, srv := deleteAPIs(t)
	rec := publicDeleteRequest(srv, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}}, nil)
	if rec.Code != http.StatusOK || store.Count() != 1 {
		t.Fatalf("run_task without -deleteAuthKey: %d %q, %d tombstones; want 200 and one task", rec.Code, rec.Body.String(), store.Count())
	}
}

// The authKey is checked before the disabled answers, as upstream does: an
// unauthenticated caller learns nothing about whether deletion is on.
func TestDeleteAuthKey_ComesBeforeTheDisabledAnswers(t *testing.T) {
	setFlag(t, internaldelete.DeleteAuthKeyFlagName, "s3cret")
	_, srv := deleteAPIs(t) // -delete.enable is off
	rec := publicDeleteRequest(srv, http.MethodPost, "/delete/run_task", url.Values{"filter": {"*"}}, nil)
	if rec.Code != http.StatusUnauthorized || rec.Body.String() != missingKeyAnswer {
		t.Errorf("flag off, no key: got %d %q, want 401 %q", rec.Code, rec.Body.String(), missingKeyAnswer)
	}
	rec = publicDeleteRequest(srv, http.MethodPost, "/delete/run_task", url.Values{"filter": {"*"}, "authKey": {"s3cret"}}, nil)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != upstreamPublicDeleteDisabled {
		t.Errorf("flag off, right key: got %d %q, want upstream's disabled answer", rec.Code, rec.Body.String())
	}
}

// Through the real HTTP server: with -httpAuth.* set, the delete key replaces
// the Basic Auth requirement on /delete/* (issue #1764) while every other path
// still needs Basic Auth. Without -deleteAuthKey, /delete/* still needs Basic
// Auth, from DeleteAuth.
func TestDeleteAuthKey_OverridesHTTPAuthThroughTheServer(t *testing.T) {
	enablePublicDelete(t)
	setFlag(t, "httpAuth.username", "ops")
	setFlag(t, "httpAuth.password", "pw")

	_, delAPI := deleteAPIs(t)
	other := http.NewServeMux()
	other.HandleFunc("/other", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("OK")) })
	root := http.NewServeMux()
	root.Handle("/delete/", delAPI)
	root.Handle("/", other)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	registerAuthKeyProtectedPaths()
	httpserver.Serve([]string{addr}, func(w http.ResponseWriter, r *http.Request) bool {
		root.ServeHTTP(w, r)
		return true
	}, httpserver.ServeOptions{})
	t.Cleanup(func() { _ = httpserver.Stop([]string{addr}) })

	do := func(method, path string, basic bool) (int, string) {
		t.Helper()
		var resp *http.Response
		var err error
		for deadline := time.Now().Add(5 * time.Second); ; {
			req, _ := http.NewRequest(method, "http://"+addr+path, strings.NewReader(""))
			if basic {
				req.SetBasicAuth("ops", "pw")
			}
			resp, err = http.DefaultClient.Do(req)
			if err == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		b := new(strings.Builder)
		buf := make([]byte, 512)
		for {
			n, e := resp.Body.Read(buf)
			b.Write(buf[:n])
			if e != nil {
				break
			}
		}
		return resp.StatusCode, b.String()
	}

	// -deleteAuthKey set: the key alone opens /delete/*; Basic Auth alone does not.
	setFlag(t, internaldelete.DeleteAuthKeyFlagName, "s3cret")
	if code, body := do(http.MethodPost, "/delete/run_task?filter=*&authKey=s3cret", false); code != http.StatusOK {
		t.Errorf("authKey without Basic Auth: %d %q, want 200 (the key overrides -httpAuth.*)", code, body)
	}
	if code, _ := do(http.MethodPost, "/delete/run_task?filter=*", true); code != http.StatusUnauthorized {
		t.Errorf("Basic Auth without the authKey: %d, want 401", code)
	}
	if code, _ := do(http.MethodGet, "/delete/logsql/tombstones?authKey=s3cret", false); code == http.StatusUnauthorized {
		t.Errorf("lakehouse /delete/logsql/tombstones refused with the right authKey")
	}
	// Other paths keep -httpAuth.*.
	if code, _ := do(http.MethodGet, "/other", false); code != http.StatusUnauthorized {
		t.Errorf("/other without Basic Auth: %d, want 401", code)
	}
	if code, body := do(http.MethodGet, "/other", true); code != http.StatusOK || body != "OK" {
		t.Errorf("/other with Basic Auth: %d %q, want 200 OK", code, body)
	}

	// -deleteAuthKey unset: /delete/* falls back to Basic Auth, enforced by DeleteAuth
	// (the server exempts the path), for both delete APIs.
	setFlag(t, internaldelete.DeleteAuthKeyFlagName, "")
	for _, p := range []string{"/delete/run_task?filter=*", "/delete/logsql/tombstones"} {
		if code, _ := do(http.MethodPost, p, false); code != http.StatusUnauthorized {
			t.Errorf("%s without credentials and without -deleteAuthKey: %d, want 401", p, code)
		}
	}
	if code, _ := do(http.MethodPost, "/delete/run_task?filter=*", true); code != http.StatusOK {
		t.Errorf("/delete/run_task with Basic Auth and no -deleteAuthKey: %d, want 200", code)
	}
}

// main serves newRequestHandler(handler); the behaviour is checked through the
// real HTTP server with that very function in TestRequestHandler_*, and the
// source only has to keep calling it.
func TestMainWiresDeleteAuth(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if want := "requestHandler := newRequestHandler(handler)"; !strings.Contains(string(src), want) {
		t.Errorf("main.go no longer contains %q: -deleteAuthKey would stop guarding /delete/* or stop overriding -httpAuth.*", want)
	}
}

// The handler main serves, through the real HTTP server: -deleteAuthKey set
// replaces -httpAuth.* on /delete/* only; unset, /delete/* keeps -httpAuth.*;
// every other path always needs it.
func TestRequestHandler_DeleteKeyOverridesHTTPAuthOnDeleteOnly(t *testing.T) {
	enablePublicDelete(t)
	_, delAPI := deleteAPIs(t)
	root := http.NewServeMux()
	root.Handle("/delete/", delAPI)
	root.HandleFunc("/other", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	do := serveBehindHTTPAuthFunc(t, newRequestHandler(root))

	setFlag(t, internaldelete.DeleteAuthKeyFlagName, "s3cret")
	if code := do(http.MethodPost, "/delete/run_task?filter=*&authKey=s3cret", false); code != http.StatusOK {
		t.Errorf("delete with the key, no Basic Auth: %d, want 200", code)
	}
	if code := do(http.MethodPost, "/delete/run_task?filter=*", true); code != http.StatusUnauthorized {
		t.Errorf("delete with Basic Auth only, key set: %d, want 401", code)
	}
	if code := do(http.MethodGet, "/other", false); code != http.StatusUnauthorized {
		t.Errorf("/other without Basic Auth: %d, want 401", code)
	}
	setFlag(t, internaldelete.DeleteAuthKeyFlagName, "")
	if code := do(http.MethodPost, "/delete/run_task?filter=*", false); code != http.StatusUnauthorized {
		t.Errorf("delete without anything, key unset: %d, want 401", code)
	}
	if code := do(http.MethodPost, "/delete/run_task?filter=*", true); code != http.StatusOK {
		t.Errorf("delete with Basic Auth, key unset: %d, want 200", code)
	}
}

// A string OrgID tenant (alias acme -> 2002:0) through the tenant middleware
// the binary puts in front of every route: the delete key is checked first,
// before the tenant is resolved, and the task lands on the aliased tenant.
func TestDeleteAuthKey_StringOrgIDThroughTheResolver(t *testing.T) {
	enablePublicDelete(t)
	setFlag(t, internaldelete.DeleteAuthKeyFlagName, "s3cret")
	store, srv := publicDeleteServer(t, true)
	guarded := internaldelete.DeleteAuth(srv)
	acme := map[string]string{"X-Scope-OrgID": "acme"}

	rec := queryRequest(guarded, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}}, acme)
	if rec.Code != http.StatusUnauthorized || rec.Body.String() != missingKeyAnswer || store.Count() != 0 {
		t.Errorf("alias tenant without the key: %d %q, %d tasks; want 401 %q and none", rec.Code, rec.Body.String(), store.Count(), missingKeyAnswer)
	}
	// An unknown OrgID without the key learns nothing about tenants: 401, not the resolver's 400.
	rec = queryRequest(guarded, http.MethodPost, "/delete/run_task", url.Values{"filter": {"*"}}, map[string]string{"X-Scope-OrgID": "nobody"})
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unknown OrgID without the key: %d, want 401 before the tenant is resolved", rec.Code)
	}
	rec = queryRequest(guarded, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}, "authKey": {"s3cret"}}, acme)
	var resp struct {
		TaskID string `json:"task_id"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil {
		t.Fatalf("alias tenant with the key: %d %q", rec.Code, rec.Body.String())
	}
	if ts, ok := store.Get(resp.TaskID); !ok || !ts.ScopedExactlyTo(2002, 0) {
		t.Errorf("task = %+v, want scoped to the alias's tenant 2002:0", ts)
	}
}
