package main

import (
	"encoding/json"
	"net/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
)

// CacheStore defines the interface for cache operations used by HTTP handlers.
type CacheStore interface {
	ClearCaches()
	MemCacheStats() cache.Stats
	SelfAZ() string
}

// ManifestStore defines the interface for manifest operations used by HTTP handlers.
type ManifestStore interface {
	Manifest() *manifest.Manifest
}

// HandleCacheClear returns an HTTP handler that clears all caches.
// It requires POST method and optional bearer token auth.
func HandleCacheClear(store CacheStore, authKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !peerauth.Bearer(r, authKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		store.ClearCaches()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"cleared": true})
	}
}

// HandleCacheStats returns an HTTP handler that reports memory cache statistics.
// It requires GET method and optional bearer token auth.
func HandleCacheStats(store CacheStore, authKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !peerauth.Bearer(r, authKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		stats := store.MemCacheStats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"l1_entries":   stats.Entries,
			"l1_size":      stats.Size,
			"l1_max_size":  stats.MaxSize,
			"l1_hits":      stats.Hits,
			"l1_misses":    stats.Misses,
			"l1_evictions": stats.Evictions,
			"az":           store.SelfAZ(),
		})
	}
}

// LakehouseInfoConfig holds the configuration for the /lakehouse/info endpoint.
type LakehouseInfoConfig struct {
	Version  string
	Mode     string
	Topology string
	Compat   string
	IsReady  func() bool
	Phase    func() string
	PeerAuth PeerAuthInfo
}

// PeerAuthInfo is the peer authentication state /lakehouse/info reports under
// "peer_auth". It never carries the key.
type PeerAuthInfo struct {
	// Enabled is the effective state: this pod refuses requests without the
	// key on its internal endpoints (peer.auth_key is set and
	// peer.auth_enabled is not false).
	Enabled bool `json:"enabled"`
	// KeyConfigured is whether peer.auth_key is set: this pod sends it on its
	// own peer calls.
	KeyConfigured bool `json:"key_configured"`
}

// HandleLakehouseInfo returns an HTTP handler that reports build and config info.
func HandleLakehouseInfo(cfg LakehouseInfoConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		compatKey := "vt_compat"
		if cfg.Mode == "logs" {
			compatKey = "vl_compat"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"version":   cfg.Version,
			"mode":      cfg.Mode,
			"topology":  cfg.Topology,
			"ready":     cfg.IsReady(),
			"phase":     cfg.Phase(),
			compatKey:   cfg.Compat,
			"peer_auth": cfg.PeerAuth,
		})
	}
}

// HandleManifestUpdate returns an HTTP handler that processes manifest update notifications.
// It requires POST method, bearer token auth when authKey is set, and a JSON body (capped at
// peer.max_request_body_bytes) with added/removed files.
func HandleManifestUpdate(store ManifestStore, authKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !peerauth.Bearer(r, authKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		peerauth.LimitBody(w, r)
		var update manifest.ManifestUpdate
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			if peerauth.TooLarge(err) {
				peerauth.ReadError(w, err)
				return
			}
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		m := store.Manifest()
		for _, fi := range update.Added {
			partition := manifest.ExtractPartition(fi.Key)
			if partition != "" {
				m.AddFile(partition, fi)
			}
		}
		for _, key := range update.Removed {
			partition := manifest.ExtractPartition(key)
			if partition != "" {
				m.RemoveFile(partition, key)
			}
		}

		metrics.ManifestUpdateReceivedTotal.Inc()
		w.WriteHeader(http.StatusOK)
	}
}
