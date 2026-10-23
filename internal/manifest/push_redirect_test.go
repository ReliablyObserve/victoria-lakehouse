package manifest

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// A peer that answers a manifest push with a redirect is not followed: the
// bearer key is not repeated at the redirect target.
func TestPusher_RefusesRedirects(t *testing.T) {
	var reached, answered atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer target.Close()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answered.Add(1)
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer peer.Close()

	p := NewPusher(PusherConfig{GetPeers: func() []string { return []string{peer.Listener.Addr().String()} }, SelfAddr: "self:1", AuthSecret: "secret"})
	p.Notify([]FileInfo{{Key: "0/0/logs/dt=2026-10-22/hour=10/a.parquet", Size: 1}}, nil)
	if answered.Load() != 1 || reached.Load() != 0 {
		t.Fatalf("peer answered %d, redirect target reached %d; want 1 and 0", answered.Load(), reached.Load())
	}
}
