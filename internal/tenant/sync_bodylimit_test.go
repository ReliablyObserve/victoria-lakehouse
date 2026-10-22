package tenant

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
)

func aliasSyncBody(t *testing.T, n int) []byte {
	t.Helper()
	d := AliasDelta{NodeID: "peer-1"}
	for i := 0; i < n; i++ {
		d.Aliases = append(d.Aliases, AliasEntry{OrgID: fmt.Sprintf("org-%06d", i), AccountID: uint32(i), ProjectID: 1, Source: SourceConfig})
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func postAliasSync(h http.Handler, body []byte, authz string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/internal/tenant/sync", bytes.NewReader(body))
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// The largest legitimate alias push (every alias of a very large fleet) passes
// the default cap; one byte over a lowered cap is refused with 413.
func TestSyncHandler_BodyCap(t *testing.T) {
	t.Cleanup(func() { peerauth.SetMaxBodyBytes(0) })
	peerauth.SetMaxBodyBytes(0)

	body := aliasSyncBody(t, 50000)
	t.Logf("tenant sync body for 50000 aliases: %d bytes (default cap %d)", len(body), peerauth.DefaultMaxBodyBytes)
	if int64(len(body))*4 > peerauth.DefaultMaxBodyBytes {
		t.Fatalf("the largest legitimate body (%d bytes) leaves no headroom under the default cap %d", len(body), peerauth.DefaultMaxBodyBytes)
	}
	r := NewResolver(ResolverConfig{})
	if w := postAliasSync(NewSyncHandler(r, ""), body, ""); w.Code != http.StatusOK {
		t.Fatalf("largest legitimate body: %d %q, want 200", w.Code, w.Body.String())
	}
	if _, ok := r.Resolve("org-049999"); !ok {
		t.Error("the last alias of the largest body was not merged")
	}

	small := aliasSyncBody(t, 20)
	peerauth.SetMaxBodyBytes(int64(len(small)))
	if w := postAliasSync(NewSyncHandler(NewResolver(ResolverConfig{}), ""), small, ""); w.Code != http.StatusOK {
		t.Fatalf("body exactly at the cap: %d, want 200", w.Code)
	}
	peerauth.SetMaxBodyBytes(int64(len(small)) - 1)
	r = NewResolver(ResolverConfig{})
	if w := postAliasSync(NewSyncHandler(r, "k"), small, "Bearer k"); w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("body one byte over the cap: %d, want 413", w.Code)
	}
	if _, ok := r.Resolve("org-000001"); ok {
		t.Error("an oversized body was merged")
	}
	if w := postAliasSync(NewSyncHandler(r, "k"), small, ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated oversized body: %d, want 401", w.Code)
	}
}

// Enforcing: no credential, a wrong key (longer, shorter, same length) and a
// different scheme are all refused; only Bearer <key> is served.
func TestSyncHandler_KeyTable(t *testing.T) {
	body := aliasSyncBody(t, 1)
	for _, tc := range []struct {
		name, authz string
		want        int
	}{
		{"none", "", 401}, {"wrong same length", "Bearer x-key", 401}, {"wrong longer", "Bearer s3cret-and-more", 401},
		{"wrong shorter", "Bearer s3", 401}, {"basic", "Basic s3cret", 401}, {"right", "Bearer s3cret", 200},
	} {
		if w := postAliasSync(NewSyncHandler(NewResolver(ResolverConfig{}), "s3cret"), body, tc.authz); w.Code != tc.want {
			t.Errorf("%s: %d, want %d", tc.name, w.Code, tc.want)
		}
	}
}
