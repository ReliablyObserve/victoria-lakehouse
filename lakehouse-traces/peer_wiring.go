package main

import (
	"net/http"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage/parquets3"
)

// The pieces of main that carry the peer key, as functions: the binary builds
// its pushers, mounts its internal endpoints and reports the peer
// authentication state through these, and the tests call the same functions.

// initPeerAuth applies the peer settings that are process-wide: the request
// body cap of the internal endpoints, and the startup report (gauge and
// warning) of the peer authentication state.
func initPeerAuth(cfg *config.Config) {
	peerauth.SetMaxBodyBytes(cfg.Peer.MaxRequestBodyBytes)
	peerauth.Report(cfg.Peer.AuthEnabled, cfg.Peer.AuthKey != "", cfg.Discovery.PeerHeadlessService != "" || cfg.Role == config.RoleInsert || cfg.Role == config.RoleSelect)
}

// peerAuthInfoFor is the peer_auth field of /lakehouse/info.
func peerAuthInfoFor(peer config.PeerConfig) PeerAuthInfo {
	return PeerAuthInfo{Enabled: peer.AuthEnabled, KeyConfigured: peer.AuthKey != ""}
}

// newManifestPusher is the pusher that tells the peers about new and removed
// files; it presents peer.auth_key whenever one is set.
func newManifestPusher(peer config.PeerConfig, getPeers func() []string, self string) *manifest.Pusher {
	return manifest.NewPusher(manifest.PusherConfig{GetPeers: getPeers, AuthSecret: peer.AuthKey, SelfAddr: self})
}

// newTenantAliasPusher is the pusher that gossips tenant aliases.
func newTenantAliasPusher(peer config.PeerConfig, resolver *tenant.TenantResolver, getPeers func() []string, self string, interval time.Duration) *tenant.SyncPusher {
	return tenant.NewSyncPusher(tenant.SyncPusherConfig{
		Resolver: resolver,
		GetPeers: getPeers,
		AuthKey:  peer.AuthKey,
		SelfAddr: self,
		Interval: interval,
	})
}

// newStatsPusher is the pusher that gossips tenant statistics; getPeers
// returns host:port and the pusher is given each peer's stats sync URL.
func newStatsPusher(peer config.PeerConfig, registry *stats.TenantRegistry, getPeers func() []string, self string, compress bool) *stats.SyncPusher {
	return stats.NewSyncPusher(stats.SyncPusherConfig{
		Registry: registry,
		GetPeers: func() []string {
			peers := getPeers()
			urls := make([]string, len(peers))
			for i, p := range peers {
				urls[i] = "http://" + p + "/internal/stats/sync"
			}
			return urls
		},
		AuthKey:  peer.AuthKey,
		SelfAddr: self,
		Compress: compress,
	})
}

// mountPeerEndpointsFor mounts the pods' internal endpoints over the pod's own
// Storage (its cache, its manifest and the peer cache handler it built from
// the same configuration); the traces binary mounts no tenant sync.
func mountPeerEndpointsFor(mux *http.ServeMux, cfg *config.Config, store *parquets3.Storage, registry *stats.TenantRegistry) {
	peerDeps := peerEndpoints{Cache: store, Manifest: store, PeerCache: store.PeerHandler()}
	if cfg.Stats.Enabled {
		peerDeps.Stats = registry
	}
	mountPeerEndpoints(mux, cfg.Peer, peerDeps)
}
