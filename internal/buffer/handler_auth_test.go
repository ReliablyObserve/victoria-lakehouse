package buffer

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// #384: /internal/buffer/query is on the insert pods' ingest port and serves
// every tenant's unflushed rows. These tests pin who may read what:
//
//   - with peer.auth_key set, every request needs Authorization: Bearer <key>
//     (401 otherwise, upstream's status for a missing or wrong key);
//   - all_tenants=true is served only to a caller that presented the key; on a
//     pod without a key it is refused with 403, since nobody can prove to be a
//     peer there;
//   - a single-tenant request is answered with that tenant's rows only, with or
//     without a key;
//   - -internalselect.disable turns the route off with upstream's answer.

const authTestKey = "cluster-gossip-secret"

func authWindow(base time.Time) url.Values {
	return url.Values{
		"start":        {fmt.Sprint(base.Add(-time.Minute).UnixNano())},
		"end":          {fmt.Sprint(base.Add(time.Minute).UnixNano())},
		"mode":         {"logs"},
		"tenant_scope": {TenantScopeVersion},
	}
}

func allTenants(base time.Time) url.Values {
	q := authWindow(base)
	q.Set("all_tenants", "true")
	return q
}

func oneTenant(base time.Time, account, project string) url.Values {
	q := authWindow(base)
	q.Set("account_id", account)
	q.Set("project_id", project)
	return q
}

// serve sends one request; authorization is the raw header ("" = none).
func serve(h http.Handler, q url.Values, authorization string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, Path+"?"+q.Encode(), nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func bodies(t *testing.T, rec *httptest.ResponseRecorder) []string {
	t.Helper()
	var out []string
	dec := json.NewDecoder(rec.Body)
	for dec.More() {
		var row schema.LogRow
		if err := dec.Decode(&row); err != nil {
			t.Fatalf("decode: %v", err)
		}
		out = append(out, row.Body)
	}
	return out
}

func TestBufferQueryAuth_AllTenantsWithoutAnyKeyIsRefused(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), "")

	for _, authz := range []string{"", "Bearer anything", "Basic dXNlcjpwYXNz"} {
		rec := serve(h, allTenants(base), authz)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("Authorization %q: status = %d, want 403 (no pod key: no caller is a proven peer)", authz, rec.Code)
		}
		if got := strings.TrimSpace(rec.Body.String()); got != AllTenantsRefusedMessage {
			t.Errorf("body = %q, want %q", got, AllTenantsRefusedMessage)
		}
		if rec.Header().Get(TenantScopeHeader) != "" || rec.Header().Get(SegmentsHeader) != "" {
			t.Errorf("a refused request disclosed scope/segment headers: %v", rec.Header())
		}
	}
}

func TestBufferQueryAuth_MissingOrWrongKeyIsRefusedWith401(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), authTestKey)

	cases := []struct {
		name, authz, want string
	}{
		{"no header", "", MissingKeyMessage},
		{"empty bearer", "Bearer ", MissingKeyMessage},
		{"basic auth instead", "Basic " + authTestKey, MissingKeyMessage},
		{"bearer without space", "Bearer" + authTestKey, MissingKeyMessage},
		{"wrong key", "Bearer wrong", WrongKeyMessage},
		{"key prefix", "Bearer " + authTestKey[:5], WrongKeyMessage},
		{"key with suffix", "Bearer " + authTestKey + "x", WrongKeyMessage},
		{"lowercase scheme", "bearer " + authTestKey, MissingKeyMessage},
	}
	for _, sel := range []struct {
		name string
		q    url.Values
	}{{"all_tenants", allTenants(base)}, {"single tenant", oneTenant(base, "0", "0")}} {
		for _, tc := range cases {
			t.Run(sel.name+"/"+tc.name, func(t *testing.T) {
				rec := serve(h, sel.q, tc.authz)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401", rec.Code)
				}
				if got := strings.TrimSpace(rec.Body.String()); got != tc.want {
					t.Errorf("body = %q, want %q", got, tc.want)
				}
				if strings.Contains(rec.Body.String(), "tenant-") {
					t.Errorf("a refused request disclosed rows: %q", rec.Body.String())
				}
			})
		}
	}
}

// The key is checked before the parameters are looked at, so a caller
// without it learns nothing from the parameter errors either.
func TestBufferQueryAuth_KeyCheckedBeforeParameters(t *testing.T) {
	h := NewHandler(&mockBufferStore{}, authTestKey)
	rec := serve(h, url.Values{"start": {"x"}}, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 before any parameter validation", rec.Code)
	}
}

func TestBufferQueryAuth_RightKeyServesEveryTenantOnAllTenants(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), authTestKey)

	rec := serve(h, allTenants(base), "Bearer "+authTestKey)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(TenantScopeHeader); got != AllTenantsScope {
		t.Errorf("%s = %q, want %q", TenantScopeHeader, got, AllTenantsScope)
	}
	if got := bodies(t, rec); len(got) != 3 {
		t.Errorf("rows = %v, want every tenant's 3", got)
	}
}

// A single-tenant read returns exactly the named tenant, with the right key
// and, on a pod without a key, without one: numeric AccountID/ProjectID, the
// only form the endpoint takes (a select pod resolves string OrgIDs/aliases to
// that pair before it asks; see the parquets3 bridge tests).
func TestBufferQueryAuth_SingleTenantReturnsOnlyThatTenant(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	for _, key := range []string{"", authTestKey} {
		h := NewHandler(tenantBufferStore(base), key)
		authz := ""
		if key != "" {
			authz = "Bearer " + key
		}
		for _, tc := range []struct {
			account, project string
			want             []string
		}{
			{"0", "0", []string{"tenant-0-0"}},
			{"1001", "0", []string{"tenant-1001-0"}},
			{"2002", "7", []string{"tenant-2002-7"}},
			{"2002", "0", nil},
			{"4294967295", "4294967295", nil},
		} {
			rec := serve(h, oneTenant(base, tc.account, tc.project), authz)
			if rec.Code != http.StatusOK {
				t.Fatalf("key %q tenant %s:%s: status = %d, want 200", key, tc.account, tc.project, rec.Code)
			}
			got := bodies(t, rec)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("key %q tenant %s:%s: rows = %v, want %v", key, tc.account, tc.project, got, tc.want)
			}
		}
	}
}

func TestGate_DisabledAnswersUpstreamErrorForEveryMethod(t *testing.T) {
	served := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { served++; w.WriteHeader(http.StatusOK) })
	h := Gate(Path, func() bool { return true }, next)
	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodHead} {
		req := httptest.NewRequest(m, Path+"?all_tenants=true", nil)
		req.Header.Set("Authorization", "Bearer "+authTestKey)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (httpserver.Errorf, as upstream)", m, rec.Code)
		}
		if m != http.MethodHead && !strings.Contains(rec.Body.String(), DisabledMessage(Path)) {
			t.Errorf("%s: body %q does not carry %q", m, rec.Body.String(), DisabledMessage(Path))
		}
	}
	if served != 0 {
		t.Fatalf("a disabled route reached its handler %d time(s)", served)
	}
}

func TestGate_EnabledPassesThrough(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := Gate(Path, func() bool { return false }, NewHandler(tenantBufferStore(base), authTestKey))
	if rec := serve(h, allTenants(base), "Bearer "+authTestKey); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec := serve(h, allTenants(base), ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401: the gate must not bypass the key check", rec.Code)
	}
}

// The gate reads the flag the binary registered, by name, at request time.
func TestInternalSelectDisabled_ReadsTheRegisteredFlag(t *testing.T) {
	if InternalSelectDisabled() {
		t.Fatal("reports disabled while no -internalselect.disable is registered, or while it is at its default")
	}
	// flag.CommandLine cannot register a name twice, and -count=N runs this
	// test N times in one process: register once, reuse afterwards.
	f := flag.Lookup(InternalSelectDisableFlag)
	if f == nil {
		flag.CommandLine.Bool(InternalSelectDisableFlag, false, "test copy")
		f = flag.Lookup(InternalSelectDisableFlag)
	}
	if InternalSelectDisabled() {
		t.Fatal("reports disabled with the flag at its default")
	}
	if err := f.Value.Set("true"); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Value.Set("false") }()
	if !InternalSelectDisabled() {
		t.Fatal("does not report -internalselect.disable=true")
	}
}

// DisabledMessage("/internal/select/*") must be upstream's answer word for
// word, in both upstreams the binaries embed.
func TestDisabledMessage_IsUpstreamsWording(t *testing.T) {
	want := DisabledMessage("/internal/select/*")
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	for _, src := range []string{
		"deps/VictoriaLogs/app/vlselect/main.go",
		"lakehouse-traces/deps/VictoriaTraces/app/vtselect/main.go",
	} {
		f, err := os.Open(filepath.Join(root, src))
		if err != nil {
			t.Fatalf("%s not checked out (make deps-logs deps-vt): %v", src, err)
		}
		found := false
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if strings.Contains(sc.Text(), `"`+want+`"`) {
				found = true
			}
		}
		_ = f.Close()
		if !found {
			t.Errorf("%s no longer answers %q; update DisabledMessage to upstream's wording", src, want)
		}
	}
}

// Whatever the Authorization header holds, a pod with a key answers 200 only
// to exactly "Bearer <key>", and never discloses a row otherwise.
func FuzzBufferQueryAuth(f *testing.F) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), authTestKey)
	for _, s := range []string{"", "Bearer ", "Bearer " + authTestKey, "Bearer " + authTestKey + " ", "bearer " + authTestKey,
		"Bearer  " + authTestKey, "Bearer " + authTestKey + "\x00", "Basic " + authTestKey, authTestKey, "Bearer  " + authTestKey} {
		f.Add(s, true)
		f.Add(s, false)
	}
	f.Fuzz(func(t *testing.T, authz string, all bool) {
		q := oneTenant(base, "1001", "0")
		if all {
			q = allTenants(base)
		}
		req := httptest.NewRequest(http.MethodGet, Path+"?"+q.Encode(), nil)
		req.Header["Authorization"] = []string{authz}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if authz == "Bearer "+authTestKey {
			if rec.Code != http.StatusOK {
				t.Fatalf("the right key got %d", rec.Code)
			}
			return
		}
		if rec.Code == http.StatusOK || strings.Contains(rec.Body.String(), "tenant-") {
			t.Fatalf("Authorization %q was served (%d): %q", authz, rec.Code, rec.Body.String())
		}
	})
}
