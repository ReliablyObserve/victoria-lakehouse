package stats

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// A peer that answers a stats push with a redirect is not followed: the
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

	reg := NewTenantRegistry("src")
	reg.RecordWrite("1:1", 10, 20, 1, "STANDARD")
	sp := NewSyncPusher(SyncPusherConfig{Registry: reg, GetPeers: func() []string { return []string{peer.URL} }, SelfAddr: "self", AuthKey: "secret"})
	_ = sp.PushDelta(context.Background()) // a redirect is a refused push, not a followed one
	if answered.Load() != 1 || reached.Load() != 0 {
		t.Fatalf("peer answered %d, redirect target reached %d; want 1 and 0", answered.Load(), reached.Load())
	}
}
