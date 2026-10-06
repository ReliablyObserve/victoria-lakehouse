package main

import (
	"flag"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
)

// Security regression: on main, VictoriaMetrics' httpserver skipped Basic Auth
// (-httpAuth.*) for ANY path ending in /config, /reload, /snapshot,
// /force_merge, /force_flush, /delete_series and the like, so
// `DELETE /lakehouse/api/v1/tenants/aliases/config` or
// `/delete/logsql/tombstone/reload` answered without credentials. The test
// serves the binary's DeleteAuth over a catch-all handler through the real HTTP
// server with -httpAuth.* set: every such path must be stopped with 401 before
// the handler (for /delete/* by DeleteAuth, the server exempts it), and the
// same paths with credentials must get through.
func TestHTTPAuth_SuffixPathsStillNeedCredentials(t *testing.T) {
	catchAll := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	registerAuthKeyProtectedPaths() // as main does, before httpserver.Serve
	do := serveBehindHTTPAuth(t, internaldelete.DeleteAuth(catchAll))
	for _, p := range suffixPaths {
		for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
			if code := do(m, p, false); code != http.StatusUnauthorized {
				t.Errorf("%s %s without credentials: %d, want 401 (a suffix path must not skip -httpAuth.*)", m, p, code)
			}
		}
		if code := do(http.MethodPost, p, true); code != http.StatusOK {
			t.Errorf("POST %s with credentials: %d, want 200", p, code)
		}
	}
}

// serveBehindHTTPAuth starts VictoriaMetrics' real HTTP server (the one both
// binaries serve through httpserver.Serve) with -httpAuth.username/password set,
// in front of handler, and returns a request function reporting the status.
func serveBehindHTTPAuth(t *testing.T, handler http.Handler) func(method, path string, basic bool) int {
	t.Helper()
	return serveBehindHTTPAuthFunc(t, func(w http.ResponseWriter, r *http.Request) bool {
		handler.ServeHTTP(w, r)
		return true
	})
}

// serveBehindHTTPAuthFunc is serveBehindHTTPAuth for a request handler in the
// httpserver.Serve shape.
func serveBehindHTTPAuthFunc(t *testing.T, requestHandler func(http.ResponseWriter, *http.Request) bool) func(method, path string, basic bool) int {
	t.Helper()
	setAuthFlag(t, "httpAuth.username", "ops")
	setAuthFlag(t, "httpAuth.password", "pw")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	httpserver.Serve([]string{addr}, requestHandler, httpserver.ServeOptions{})
	t.Cleanup(func() { _ = httpserver.Stop([]string{addr}) })
	return func(method, path string, basic bool) int {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); ; {
			req, _ := http.NewRequest(method, "http://"+addr+path, strings.NewReader(""))
			if basic {
				req.SetBasicAuth("ops", "pw")
			}
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
				return resp.StatusCode
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s %s: %v", method, path, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func setAuthFlag(t *testing.T, name, value string) {
	t.Helper()
	if err := flag.Set(name, value); err != nil {
		t.Fatalf("-%s: %v", name, err)
	}
	t.Cleanup(func() { _ = flag.Set(name, "") })
}

// suffixPaths are paths whose last segment VictoriaMetrics' lib/httpserver
// v1.146.1 (what main built against) exempted from -httpAuth.* by suffix
// alone (isProtectedByAuthFlag: /config, /reload, /snapshot, /force_merge,
// /force_flush, /delete_series, ...), whatever came before it. Since v1.152.x
// the exemption is opt-in per binary (RegisterAuthKeyProtectedPathsFunc).
var suffixPaths = []string{
	"/lakehouse/api/v1/tenants/aliases/config",
	"/lakehouse/api/v1/anything/config",
	"/delete/logsql/tombstone/reload",
	"/lakehouse/api/v1/x/reload",
	"/lakehouse/api/v1/x/snapshot",
	"/lakehouse/api/v1/x/force_merge",
	"/lakehouse/api/v1/x/force_flush",
	"/lakehouse/api/v1/x/delete_series",
	"/config",
	"/reload",
	"/snapshot",
	"/force_merge",
	"/force_flush",
	"/delete_series",
}
