package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// These tests drive the functions main builds the peer-key paths from, over a
// real Storage (parquets3.New over a fake bucket), not handlers rebuilt in the
// test: a binary that stopped passing the key to a pusher, or built its peer
// cache handler without it, fails here.

// keyRecorder is a peer that records the credential of each request by path.
type keyRecorder struct {
	srv *httptest.Server
	mu  sync.Mutex
	got map[string]string // path -> Authorization
}

func newKeyRecorder(t *testing.T) *keyRecorder {
	t.Helper()
	r := &keyRecorder{got: map[string]string{}}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.got[req.URL.Path] = req.Header.Get("Authorization")
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *keyRecorder) addr() string { return r.srv.Listener.Addr().String() }

func (r *keyRecorder) credential(path string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.got[path]
	return v, ok
}

// Each pusher the binary builds presents peer.auth_key on its own path when
// one is set, and no credential when there is none.
func TestPeerWiring_PushersPresentTheKey(t *testing.T) {
	for _, key := range []string{"", peerKey} {
		want := ""
		if key != "" {
			want = "Bearer " + key
		}
		peer := config.PeerConfig{AuthKey: key}
		rec := newKeyRecorder(t)
		peers := func() []string { return []string{rec.addr()} }

		newManifestPusher(peer, peers, "self:1").Notify([]manifest.FileInfo{{Key: "0/0/logs/dt=2026-10-22/hour=10/a.parquet", Size: 1}}, nil)

		res := tenant.NewResolver(tenant.ResolverConfig{})
		if err := res.AddAlias("org-x", tenant.TenantID{AccountID: 5, ProjectID: 1}); err != nil {
			t.Fatal(err)
		}
		newTenantAliasPusher(peer, res, peers, "self:1", 0).Push(context.Background())

		reg := stats.NewTenantRegistry("src")
		reg.RecordWrite("1:1", 10, 20, 1, "STANDARD")
		if err := newStatsPusher(peer, reg, peers, "self:1", false).PushDelta(context.Background()); err != nil {
			t.Fatalf("stats push: %v", err)
		}

		for _, path := range []string{"/internal/manifest/update", "/internal/tenant/sync", "/internal/stats/sync"} {
			got, ok := rec.credential(path)
			if !ok {
				t.Errorf("key %q: nothing was pushed to %s", key, path)
			} else if got != want {
				t.Errorf("key %q: %s carried Authorization %q, want %q", key, path, got, want)
			}
		}
	}
}

// The endpoints mounted over a real Storage, including the peer cache handler
// the Storage built from the configuration, refuse a request without the key
// when the key is enforced (peer.auth_enabled unset or true with a key), and
// serve it with the key; with the key only sent (peer.auth_enabled false) or no
// key at all, they serve without one.
func TestPeerWiring_StoreEndpointsFollowThePeerConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		peer    config.PeerConfig
		enforce bool
	}{
		{"enforced (auth_enabled true)", config.PeerConfig{AuthKey: peerKey, AuthEnabled: boolPtr(true)}, true},
		{"enforced (auth_enabled unset, a key alone enforces)", config.PeerConfig{AuthKey: peerKey}, true},
		{"key sent, not enforced (auth_enabled false)", config.PeerConfig{AuthKey: peerKey, AuthEnabled: boolPtr(false)}, false},
		{"no key", config.PeerConfig{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s3 := newFakeS3()
			defer s3.Close()
			cfg := podConfig(t, config.RoleAll, s3.URL())
			cfg.Peer = tc.peer
			store := newPod(t, cfg)
			mux := http.NewServeMux()
			mountPeerEndpointsFor(mux, cfg, store, stats.NewTenantRegistry("recv"), tenant.NewResolver(tenant.ResolverConfig{}))

			for _, e := range peerEndpointList() {
				for _, bad := range []string{"", "wrong-" + peerKey} {
					w := httptest.NewRecorder()
					mux.ServeHTTP(w, e.request(t, bad))
					if refused := w.Code == http.StatusUnauthorized; refused != tc.enforce {
						t.Errorf("%s with key %q: %d, refused=%v want %v", e.name, bad, w.Code, refused, tc.enforce)
					}
				}
				if tc.enforce {
					w := httptest.NewRecorder()
					mux.ServeHTTP(w, e.request(t, peerKey))
					if w.Code == http.StatusUnauthorized {
						t.Errorf("%s refused the right key", e.name)
					}
				}
			}
		})
	}
}

// initPeerAuth sets the body cap and the startup report; peerAuthInfoFor is
// what /lakehouse/info shows.
func TestPeerWiring_InitAndInfo(t *testing.T) {
	t.Cleanup(func() { peerauth.SetMaxBodyBytes(0) })
	cfg := config.Default()
	cfg.Peer = config.PeerConfig{AuthKey: peerKey, AuthEnabled: boolPtr(true), MaxRequestBodyBytes: 12345}
	initPeerAuth(cfg)
	if got := peerauth.MaxBodyBytes(); got != 12345 {
		t.Errorf("body cap = %d, want 12345", got)
	}
	if metrics.PeerAuthEnabled.Get() != 1 {
		t.Error("the gauge does not show an enforcing pod")
	}
	if info := peerAuthInfoFor(cfg.Peer); !info.Enabled || !info.KeyConfigured {
		t.Errorf("info = %+v", info)
	}

	// A key alone enforces (auth_enabled unset).
	cfg.Peer = config.PeerConfig{AuthKey: peerKey}
	initPeerAuth(cfg)
	if metrics.PeerAuthEnabled.Get() != 1 {
		t.Error("the gauge does not show a pod that enforces a configured key")
	}
	if info := peerAuthInfoFor(cfg.Peer); !info.Enabled || !info.KeyConfigured {
		t.Errorf("info = %+v", info)
	}

	// An explicit false only sends the key.
	cfg.Peer = config.PeerConfig{AuthKey: peerKey, AuthEnabled: boolPtr(false)}
	initPeerAuth(cfg)
	if metrics.PeerAuthEnabled.Get() != 0 {
		t.Error("the gauge shows an enforcing pod for auth_enabled false")
	}
	if info := peerAuthInfoFor(cfg.Peer); info.Enabled || !info.KeyConfigured {
		t.Errorf("info = %+v", info)
	}

	cfg.Peer = config.PeerConfig{}
	initPeerAuth(cfg)
	if metrics.PeerAuthEnabled.Get() != 0 {
		t.Error("the gauge does not show a pod without peer authentication")
	}
	if info := peerAuthInfoFor(cfg.Peer); info.Enabled || info.KeyConfigured {
		t.Errorf("info = %+v", info)
	}
}
