package main

import (
	"encoding/json"
	"net/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
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
		if authKey != "" {
			auth := r.Header.Get("Authorization")
			if auth != "Bearer "+authKey {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
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
		if authKey != "" {
			auth := r.Header.Get("Authorization")
			if auth != "Bearer "+authKey {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
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
		})
	}
}

// HandleManifestUpdate returns an HTTP handler that processes manifest update notifications.
// It requires POST method, optional bearer token auth, and a JSON body with added/removed files.
func HandleManifestUpdate(store ManifestStore, authKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if authKey != "" {
			auth := r.Header.Get("Authorization")
			if auth != "Bearer "+authKey {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}

		var update manifest.ManifestUpdate
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
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
