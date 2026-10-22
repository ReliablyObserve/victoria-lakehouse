package parquets3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
)

// The peer's zone is read from /internal/cache/stats, which the binary serves
// with HandleCacheStats (key in Authorization: Bearer) behind peer.auth_enabled,
// and the peer cache handler serves with the key in X-Peer-Auth-Key. The probe
// presents the key in both forms, so zone-aware routing keeps working on a
// cluster that enforces the key.
func TestStorage_FetchPeerAZ_KeyInBothForms(t *testing.T) {
	const key = "test-secret"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !peerauth.Bearer(r, key) || !peerauth.Header(r, peerauth.PeerHeader, key) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"az":"az-a"}`))
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.Peer.AuthKey = key
	s := &Storage{cfg: cfg}
	if az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String()); az != "az-a" {
		t.Errorf("zone of a peer enforcing the key = %q, want az-a", az)
	}

	// no key configured: nothing is sent (a peer that does not enforce is unaffected)
	var sent bool
	open := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent = r.Header.Get("Authorization") != "" || r.Header.Get(peerauth.PeerHeader) != ""
		_, _ = w.Write([]byte(`{"az":"az-b"}`))
	}))
	defer open.Close()
	s = &Storage{cfg: testConfig()}
	if az := s.fetchPeerAZ(context.Background(), open.Listener.Addr().String()); az != "az-b" || sent {
		t.Errorf("without a key: zone %q, credential sent %v", az, sent)
	}
}
