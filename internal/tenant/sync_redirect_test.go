package tenant

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// A peer that answers an alias push with a redirect is not followed: the
// bearer key is not repeated at the redirect target.
func TestSyncPusher_RefusesRedirects(t *testing.T) {
	var reached, answered atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer target.Close()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answered.Add(1)
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer peer.Close()

	res := NewResolver(ResolverConfig{})
	if err := res.AddAlias("org-x", TenantID{AccountID: 5, ProjectID: 1}); err != nil {
		t.Fatal(err)
	}
	sp := NewSyncPusher(SyncPusherConfig{Resolver: res, GetPeers: func() []string { return []string{peer.Listener.Addr().String()} }, AuthKey: "secret", SelfAddr: "self:1"})
	sp.Push(context.Background())
	if answered.Load() != 1 || reached.Load() != 0 {
		t.Fatalf("peer answered %d, redirect target reached %d; want 1 and 0", answered.Load(), reached.Load())
	}
}
