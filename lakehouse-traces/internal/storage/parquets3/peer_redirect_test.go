package parquets3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
)

// redirectingPeer answers every request with a 307 to a second server and
// counts both: the second one must never be reached, with or without the key.
func redirectingPeer(t *testing.T) (peer *httptest.Server, answered, reached *atomic.Int64) {
	t.Helper()
	answered, reached = new(atomic.Int64), new(atomic.Int64)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		if r.Header.Get("Authorization") != "" || r.Header.Get(peerauth.PeerHeader) != "" {
			t.Errorf("the peer key was forwarded to the redirect target")
		}
	}))
	t.Cleanup(target.Close)
	peer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answered.Add(1)
		http.Redirect(w, r, target.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}))
	t.Cleanup(peer.Close)
	return peer, answered, reached
}

// The select pods' buffer bridge sends peer.auth_key to the insert pods; it
// does not follow a redirect an insert pod answers with.
func TestBufferBridge_RefusesRedirects(t *testing.T) {
	peer, answered, reached := redirectingPeer(t)
	b := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeTraces)
	b.SetAuthKey("secret")
	b.SetEndpoints([]string{peer.Listener.Addr().String()})
	rows, _ := b.QueryTraces(context.Background(), 0, time.Now().UnixNano(), tenantScope{all: true})
	if len(rows) != 0 {
		t.Errorf("got %d rows from a redirecting peer", len(rows))
	}
	if answered.Load() == 0 {
		t.Fatal("the bridge never asked the peer")
	}
	if reached.Load() != 0 {
		t.Fatalf("%d request(s) reached the redirect target, want none", reached.Load())
	}
}

// The zone probe sends the key in both header forms; it does not follow a
// redirect either, and learns no zone from it.
func TestStorage_FetchPeerAZ_RefusesRedirects(t *testing.T) {
	peer, answered, reached := redirectingPeer(t)
	cfg := testConfig()
	cfg.Peer.AuthKey = "secret"
	s := &Storage{cfg: cfg}
	if az := s.fetchPeerAZ(context.Background(), peer.Listener.Addr().String()); az != "" {
		t.Errorf("zone from a redirecting peer = %q, want none", az)
	}
	if answered.Load() != 1 || reached.Load() != 0 {
		t.Fatalf("peer answered %d, redirect target reached %d; want 1 and 0", answered.Load(), reached.Load())
	}
}
