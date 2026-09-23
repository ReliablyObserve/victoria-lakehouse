package delete

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// Both tenant forms on every scoped delete-API path, on both object key
// layouts. Integer tenants (AccountID / ProjectID headers) are upstream's form;
// string tenants (X-Scope-OrgID, resolved through the tenant aliases) are the
// lakehouse extension, resolved by the same middleware in front of the select
// path, which turns them into the integer headers before any handler runs.

type keyLayout struct {
	name     string
	template string
	key1001  string
	key2002  string
}

func keyLayouts() []keyLayout {
	return []keyLayout{
		{"account-project", "{AccountID}/{ProjectID}/",
			"1001/0/logs/dt=2026-03-01/hour=07/a.parquet", "2002/0/logs/dt=2026-03-01/hour=07/b.parquet"},
		{"orgid", "{OrgID}/",
			"1001/logs/dt=2026-03-01/hour=07/a.parquet", "2002/logs/dt=2026-03-01/hour=07/b.parquet"},
	}
}

func newLayoutHandler(t *testing.T, l keyLayout, aliases bool) (*TombstoneStore, http.Handler) {
	t.Helper()
	m := lhmanifest.New("test-bucket", "")
	m.SetPrefixTemplate(l.template)
	for _, k := range []string{l.key1001, l.key2002} {
		m.AddFile("dt=2026-03-01/hour=07", lhmanifest.FileInfo{Key: k, Size: 1024, RowCount: 10, MinTimeNs: 1, MaxTimeNs: 1 << 40})
	}
	markListed(t, m, []string{l.key1001, l.key2002})
	store := NewTombstoneStore()
	auth := tenant.NewGlobalReadAuth(globalHeader, "letmein", "")
	h := NewHandler(store, &scopedManifest{leftoverManifest{m: m}}, NewStorageClassDetector(nil), defaultCfg(), "logs",
		WithGlobalReadAuthorizer(func(r *http.Request) bool { return auth.Enabled() && auth.Authorize(r) }))
	mux := http.NewServeMux()
	h.Register(mux)
	// A stand-in for the select path, behind the same middleware.
	mux.HandleFunc("/select/logsql/query", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if !aliases {
		// The binaries install the tenant middleware only when aliases or
		// auto-register are configured.
		return store, mux
	}
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	if err := resolver.AddAlias("acme", tenant.TenantID{AccountID: 2002}); err != nil {
		t.Fatal(err)
	}
	return store, resolver.Middleware(mux)
}

func (l keyLayout) accountOnly() bool { return strings.HasPrefix(l.template, "{OrgID}") }

func TestDeleteAPI_BothTenantFormsOnBothKeyLayouts(t *testing.T) {
	forms := []struct {
		name    string
		headers map[string]string
	}{
		{"int-headers", map[string]string{"AccountID": "2002", "ProjectID": "0"}},
		{"string-orgid-alias", map[string]string{"X-Scope-OrgID": "acme"}},
	}
	for _, l := range keyLayouts() {
		for _, form := range forms {
			t.Run(l.name+"/"+form.name, func(t *testing.T) {
				store, srv := newLayoutHandler(t, l, true)
				store.Add(Tombstone{ID: "other", Query: "*", EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 1001}}})

				// create: scoped to 2002:0, over 2002's object only
				rec := scopeReq{method: http.MethodPost, path: "/delete/logsql/delete", form: deleteForm("level:error"), headers: form.headers}.do(srv)
				body := decodeJSON(t, rec.Body)
				if rec.Code != http.StatusOK || body["tenant"] != "2002:0" || body["affected_files"] != float64(1) {
					t.Fatalf("create = %d %v, want tenant 2002:0 over its 1 object", rec.Code, body)
				}
				id := body["tombstone_id"].(string)
				if ts, _ := store.Get(id); len(ts.AffectedKeys) != 1 || ts.AffectedKeys[0] != l.key2002 {
					t.Fatalf("affected keys = %v, want only %s", ts.AffectedKeys, l.key2002)
				}

				// estimate: 2002's objects only
				rec = scopeReq{method: http.MethodPost, path: "/delete/logsql/estimate", form: deleteForm("*"), headers: form.headers}.do(srv)
				if b := decodeJSON(t, rec.Body); b["affected_files"] != float64(1) {
					t.Errorf("estimate = %v, want 1 object", b)
				}

				// list: only its own
				rec = scopeReq{method: http.MethodGet, path: "/delete/logsql/tombstones", headers: form.headers}.do(srv)
				if b := decodeJSON(t, rec.Body); b["count"] != float64(1) || b["scope"] != "tenant" {
					t.Errorf("list = %v, want only its own tombstone", b)
				}

				// by id: own found, the other tenant's not found (read and un-delete)
				if rec := (scopeReq{method: http.MethodGet, path: "/delete/logsql/tombstone/" + id, headers: form.headers}).do(srv); rec.Code != http.StatusOK {
					t.Errorf("GET own = %d", rec.Code)
				}
				for _, method := range []string{http.MethodGet, http.MethodDelete} {
					if rec := (scopeReq{method: method, path: "/delete/logsql/tombstone/other", headers: form.headers}).do(srv); rec.Code != http.StatusNotFound {
						t.Errorf("%s other tenant's tombstone = %d, want 404", method, rec.Code)
					}
				}

				// verify: its own only
				rec = scopeReq{method: http.MethodPost, path: "/delete/logsql/verify",
					form: url.Values{"query": {"level:error"}, "start": {"0"}, "end": {"4000000000000000000"}}, headers: form.headers}.do(srv)
				if b := decodeJSON(t, rec.Body); len(b["tombstone_ids"].([]any)) != 1 {
					t.Errorf("verify = %v, want only its own tombstone", b)
				}

				// leftovers: the tenant view
				rec = scopeReq{method: http.MethodGet, path: "/delete/logsql/leftovers", headers: form.headers}.do(srv)
				if b := decodeJSON(t, rec.Body); b["scope"] != "tenant" {
					t.Errorf("leftovers scope = %v, want tenant", b["scope"])
				}

				// un-delete its own
				if rec := (scopeReq{method: http.MethodDelete, path: "/delete/logsql/tombstone/" + id, headers: form.headers}).do(srv); rec.Code != http.StatusOK {
					t.Errorf("un-delete own = %d", rec.Code)
				}
				if _, ok := store.Get("other"); !ok || store.Count() != 1 {
					t.Fatalf("store = %d records, want only the other tenant's left", store.Count())
				}
			})
		}
	}
}

// An unknown string OrgID gets exactly the answer the select path gives it (the
// same middleware answers both), and nothing is created.
func TestDeleteAPI_UnknownOrgIDAnswersLikeSelect(t *testing.T) {
	for _, l := range keyLayouts() {
		store, srv := newLayoutHandler(t, l, true)
		unknown := map[string]string{"X-Scope-OrgID": "nobody"}
		sel := scopeReq{method: http.MethodGet, path: "/select/logsql/query", headers: unknown}.do(srv)
		for _, r := range []scopeReq{
			{method: http.MethodPost, path: "/delete/logsql/delete", form: deleteForm("*"), headers: unknown},
			{method: http.MethodPost, path: "/delete/logsql/estimate", form: deleteForm("*"), headers: unknown},
			{method: http.MethodGet, path: "/delete/logsql/tombstones", headers: unknown},
			{method: http.MethodGet, path: "/delete/logsql/leftovers", headers: unknown},
			{method: http.MethodDelete, path: "/delete/logsql/tombstone/x", headers: unknown},
		} {
			rec := r.do(srv)
			if rec.Code != sel.Code || rec.Body.String() != sel.Body.String() || rec.Code != http.StatusBadRequest {
				t.Errorf("%s: %s %s = %d %q, want the select path's %d %q", l.name, r.method, r.path, rec.Code, rec.Body.String(), sel.Code, sel.Body.String())
			}
		}
		if store.Count() != 0 {
			t.Fatalf("%s: an unknown OrgID created a tombstone", l.name)
		}
	}
}

// Integer tenants are upstream's form: their answers do not change whether or
// not string-tenant aliases are configured.
func TestDeleteAPI_IntTenantAnswerIndependentOfAliases(t *testing.T) {
	answer := func(aliases bool) []string {
		store, srv := newLayoutHandler(t, keyLayouts()[0], aliases)
		store.Add(Tombstone{ID: "fixed", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 2002}}})
		store.Add(Tombstone{ID: "foreign", Query: "level:error", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{AccountID: 1001}}})
		h := map[string]string{"AccountID": "2002"}
		var out []string
		for _, r := range []scopeReq{
			{method: http.MethodGet, path: "/delete/logsql/tombstones", headers: h},
			{method: http.MethodGet, path: "/delete/logsql/tombstone/fixed", headers: h},
			{method: http.MethodGet, path: "/delete/logsql/tombstone/foreign", headers: h},
			{method: http.MethodPost, path: "/delete/logsql/estimate", form: deleteForm("*"), headers: h},
			{method: http.MethodPost, path: "/delete/logsql/verify", form: url.Values{"query": {"level:error"}, "start": {"0"}, "end": {"10"}}, headers: h},
		} {
			rec := r.do(srv)
			out = append(out, r.path+" "+http.StatusText(rec.Code)+" "+normalizeJSON(t, rec.Body.String()))
		}
		rec := scopeReq{method: http.MethodPost, path: "/delete/logsql/delete", form: deleteForm("*"), headers: h}.do(srv)
		b := decodeJSON(t, rec.Body)
		delete(b, "tombstone_id")
		keys := make([]string, 0, len(b))
		for k, v := range b {
			keys = append(keys, k+"="+strings.TrimSpace(jsonString(t, v)))
		}
		sort.Strings(keys)
		return append(out, "create "+strings.Join(keys, ","))
	}
	with, without := answer(true), answer(false)
	for i := range with {
		if with[i] != without[i] {
			t.Errorf("int-tenant answer changed with aliases configured:\n with:    %s\n without: %s", with[i], without[i])
		}
	}
}

// On the account-only ({OrgID}) key layout a delete scoped to a ProjectID
// other than 0 is refused; string OrgIDs map to ProjectID 0 and are accepted.
func TestDeleteAPI_OrgIDLayoutRefusesNonZeroProject(t *testing.T) {
	l := keyLayouts()[1]
	store, srv := newLayoutHandler(t, l, true)
	rec := scopeReq{method: http.MethodPost, path: "/delete/logsql/delete", form: deleteForm("*"), headers: map[string]string{"AccountID": "2002", "ProjectID": "3"}}.do(srv)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), ErrProjectNotInKeyLayout.Error()) {
		t.Fatalf("ProjectID 3 on the {OrgID} layout = %d %q, want 400", rec.Code, rec.Body.String())
	}
	if store.Count() != 0 {
		t.Fatal("a refused delete created a tombstone")
	}
	if !l.accountOnly() || !AccountOnlyKeys(&scopedManifest{leftoverManifest{m: func() *lhmanifest.Manifest {
		m := lhmanifest.New("b", "")
		m.SetPrefixTemplate(l.template)
		return m
	}()}}) {
		t.Fatal("the {OrgID} manifest must report account-only keys")
	}
}

func normalizeJSON(t *testing.T, s string) string {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}
	return jsonString(t, v)
}

func jsonString(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
