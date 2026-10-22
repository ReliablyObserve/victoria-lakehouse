package peercache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// A peer that answers with a redirect does not receive the key a second time:
// the client returns the redirect as a failed call and no request reaches the
// redirect target (a custom header such as X-Peer-Auth-Key is otherwise
// forwarded to any host).
func TestPeerCache_RefusesRedirects(t *testing.T) {
	var reached, answered atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached.Add(1) }))
	defer target.Close()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answered.Add(1)
		http.Redirect(w, r, target.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}))
	defer peer.Close()

	pc := New("self:1", "secret", 2*time.Second, 2)
	addr := peer.Listener.Addr().String()
	if data, ok, err := pc.Fetch(context.Background(), addr, "blk"); ok || err == nil {
		t.Errorf("Fetch through a redirect: %q %v %v, want a failed call", data, ok, err)
	}
	if ok, _ := pc.Has(context.Background(), addr, "blk"); ok {
		t.Error("Has through a redirect reported the block as present")
	}
	if answered.Load() != 2 || reached.Load() != 0 {
		t.Fatalf("peer answered %d, redirect target reached %d; want 2 and 0", answered.Load(), reached.Load())
	}
}
