package main

import (
	"net/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// peerEndpoints is what the pods' internal endpoints serve from. A nil Stats
// or PeerCache leaves that endpoint unmounted.
type peerEndpoints struct {
	Cache     CacheStore
	Manifest  ManifestStore
	PeerCache *peercache.Handler
	Stats     *stats.TenantRegistry
}

// mountPeerEndpoints mounts the endpoints the pods call on each other: cache
// clear, stats and fetch/has, the manifest push and stats sync.
// Each requires peer.EnforcedKey() (the key, unless peer.auth_enabled is
// false or there is none); the pod's own calls to its peers send
// peer.AuthKey whenever it is set.
func mountPeerEndpoints(mux *http.ServeMux, peer config.PeerConfig, d peerEndpoints) {
	key := peer.EnforcedKey()
	mux.HandleFunc("/internal/cache/clear", HandleCacheClear(d.Cache, key))
	mux.HandleFunc("/internal/cache/stats", HandleCacheStats(d.Cache, key))
	if d.PeerCache != nil {
		mux.Handle("/internal/cache/", d.PeerCache)
	}
	mux.HandleFunc("/internal/manifest/update", HandleManifestUpdate(d.Manifest, key))
	if d.Stats != nil {
		mux.Handle("/internal/stats/sync", stats.NewSyncHandler(d.Stats, key))
	}
}

// mountTenantAliasAPI mounts the tenant alias admin API under the peer key
// (peer.EnforcedKey()): the alias writes are guarded by the same key as the
// pods' internal endpoints.
func mountTenantAliasAPI(mux *http.ServeMux, peer config.PeerConfig, resolver *tenant.TenantResolver, reg *tenant.Registry) {
	if resolver == nil {
		return
	}
	h := tenant.NewHandler(resolver, nil, peer.EnforcedKey())
	if reg != nil {
		h = h.WithRegistry(reg)
	}
	h.Register(mux)
}
