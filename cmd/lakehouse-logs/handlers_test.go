package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// mockCacheStore implements CacheStore for testing.
type mockCacheStore struct {
	cleared bool
	stats   cache.Stats
	az      string
}

func (m *mockCacheStore) ClearCaches()          { m.cleared = true }
func (m *mockCacheStore) MemCacheStats() cache.Stats { return m.stats }
func (m *mockCacheStore) SelfAZ() string         { return m.az }

// mockManifestStore implements ManifestStore for testing.
type mockManifestStore struct {
	manifest *manifest.Manifest
}

func (m *mockManifestStore) Manifest() *manifest.Manifest { return m.manifest }

func TestHandleCacheClear(t *testing.T) {
	t.Run("POST succeeds without auth", func(t *testing.T) {
		store := &mockCacheStore{}
		handler := HandleCacheClear(store, "")

		req := httptest.NewRequest(http.MethodPost, "/internal/cache/clear", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		if !store.cleared {
			t.Fatal("expected ClearCaches to be called")
		}
		ct := w.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Fatalf("expected Content-Type application/json, got %q", ct)
		}
		var resp map[string]bool
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if !resp["cleared"] {
			t.Fatal("expected cleared=true in response")
		}
	})

	t.Run("GET returns method not allowed", func(t *testing.T) {
		store := &mockCacheStore{}
		handler := HandleCacheClear(store, "")

		req := httptest.NewRequest(http.MethodGet, "/internal/cache/clear", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected status 405, got %d", w.Code)
		}
		if store.cleared {
			t.Fatal("ClearCaches should not be called on wrong method")
		}
	})

	t.Run("POST with valid auth succeeds", func(t *testing.T) {
		store := &mockCacheStore{}
		handler := HandleCacheClear(store, "secret-key")

		req := httptest.NewRequest(http.MethodPost, "/internal/cache/clear", nil)
		req.Header.Set("Authorization", "Bearer secret-key")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		if !store.cleared {
			t.Fatal("expected ClearCaches to be called")
		}
	})

	t.Run("POST with invalid auth returns unauthorized", func(t *testing.T) {
		store := &mockCacheStore{}
		handler := HandleCacheClear(store, "secret-key")

		req := httptest.NewRequest(http.MethodPost, "/internal/cache/clear", nil)
		req.Header.Set("Authorization", "Bearer wrong-key")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected status 401, got %d", w.Code)
		}
		if store.cleared {
			t.Fatal("ClearCaches should not be called with invalid auth")
		}
	})

	t.Run("POST without auth header when auth required returns unauthorized", func(t *testing.T) {
		store := &mockCacheStore{}
		handler := HandleCacheClear(store, "secret-key")

		req := httptest.NewRequest(http.MethodPost, "/internal/cache/clear", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected status 401, got %d", w.Code)
		}
	})
}

func TestHandleCacheStats(t *testing.T) {
	t.Run("GET returns stats JSON", func(t *testing.T) {
		store := &mockCacheStore{
			stats: cache.Stats{
				Entries:   100,
				Size:      4096,
				MaxSize:   8192,
				Hits:      500,
				Misses:    50,
				Evictions: 10,
			},
			az: "us-east-1a",
		}
		handler := HandleCacheStats(store, "")

		req := httptest.NewRequest(http.MethodGet, "/internal/cache/stats", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		ct := w.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Fatalf("expected Content-Type application/json, got %q", ct)
		}

		var resp map[string]any
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp["l1_entries"] != float64(100) {
			t.Fatalf("expected l1_entries=100, got %v", resp["l1_entries"])
		}
		if resp["l1_size"] != float64(4096) {
			t.Fatalf("expected l1_size=4096, got %v", resp["l1_size"])
		}
		if resp["l1_max_size"] != float64(8192) {
			t.Fatalf("expected l1_max_size=8192, got %v", resp["l1_max_size"])
		}
		if resp["l1_hits"] != float64(500) {
			t.Fatalf("expected l1_hits=500, got %v", resp["l1_hits"])
		}
		if resp["l1_misses"] != float64(50) {
			t.Fatalf("expected l1_misses=50, got %v", resp["l1_misses"])
		}
		if resp["l1_evictions"] != float64(10) {
			t.Fatalf("expected l1_evictions=10, got %v", resp["l1_evictions"])
		}
		if resp["az"] != "us-east-1a" {
			t.Fatalf("expected az=us-east-1a, got %v", resp["az"])
		}
	})

	t.Run("POST returns method not allowed", func(t *testing.T) {
		store := &mockCacheStore{}
		handler := HandleCacheStats(store, "")

		req := httptest.NewRequest(http.MethodPost, "/internal/cache/stats", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected status 405, got %d", w.Code)
		}
	})

	t.Run("GET with valid auth succeeds", func(t *testing.T) {
		store := &mockCacheStore{az: "eu-west-1b"}
		handler := HandleCacheStats(store, "my-token")

		req := httptest.NewRequest(http.MethodGet, "/internal/cache/stats", nil)
		req.Header.Set("Authorization", "Bearer my-token")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("GET with invalid auth returns unauthorized", func(t *testing.T) {
		store := &mockCacheStore{}
		handler := HandleCacheStats(store, "my-token")

		req := httptest.NewRequest(http.MethodGet, "/internal/cache/stats", nil)
		req.Header.Set("Authorization", "Bearer wrong")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected status 401, got %d", w.Code)
		}
	})
}

func TestHandleLakehouseInfo(t *testing.T) {
	t.Run("returns correct JSON for logs mode", func(t *testing.T) {
		handler := HandleLakehouseInfo(LakehouseInfoConfig{
			Version:  "1.2.3",
			Mode:     "logs",
			Topology: "direct",
			Compat:   "0.40.0",
			IsReady:  func() bool { return true },
			Phase:    func() string { return "ready" },
		})

		req := httptest.NewRequest(http.MethodGet, "/lakehouse/info", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
		ct := w.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Fatalf("expected Content-Type application/json, got %q", ct)
		}

		var resp map[string]any
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp["version"] != "1.2.3" {
			t.Fatalf("expected version=1.2.3, got %v", resp["version"])
		}
		if resp["mode"] != "logs" {
			t.Fatalf("expected mode=logs, got %v", resp["mode"])
		}
		if resp["topology"] != "direct" {
			t.Fatalf("expected topology=direct, got %v", resp["topology"])
		}
		if resp["ready"] != true {
			t.Fatalf("expected ready=true, got %v", resp["ready"])
		}
		if resp["phase"] != "ready" {
			t.Fatalf("expected phase=ready, got %v", resp["phase"])
		}
		if resp["vl_compat"] != "0.40.0" {
			t.Fatalf("expected vl_compat=0.40.0, got %v", resp["vl_compat"])
		}
		if _, ok := resp["vt_compat"]; ok {
			t.Fatal("should not have vt_compat key in logs mode")
		}
	})

	t.Run("returns correct JSON for traces mode", func(t *testing.T) {
		handler := HandleLakehouseInfo(LakehouseInfoConfig{
			Version:  "2.0.0",
			Mode:     "traces",
			Topology: "auto",
			Compat:   "0.8.2",
			IsReady:  func() bool { return false },
			Phase:    func() string { return "init" },
		})

		req := httptest.NewRequest(http.MethodGet, "/lakehouse/info", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}

		var resp map[string]any
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp["ready"] != false {
			t.Fatalf("expected ready=false, got %v", resp["ready"])
		}
		if resp["phase"] != "init" {
			t.Fatalf("expected phase=init, got %v", resp["phase"])
		}
		if resp["vt_compat"] != "0.8.2" {
			t.Fatalf("expected vt_compat=0.8.2, got %v", resp["vt_compat"])
		}
		if _, ok := resp["vl_compat"]; ok {
			t.Fatal("should not have vl_compat key in traces mode")
		}
	})
}

func TestHandleManifestUpdate(t *testing.T) {
	t.Run("POST with valid payload succeeds", func(t *testing.T) {
		m := manifest.New("test-bucket", "logs/")
		store := &mockManifestStore{manifest: m}
		handler := HandleManifestUpdate(store, "")

		body := `{
			"added": [{"key": "logs/2024-01-15/data.parquet", "size": 1024}],
			"removed": ["logs/2024-01-14/old.parquet"],
			"source": "compactor"
		}`
		req := httptest.NewRequest(http.MethodPost, "/internal/manifest/update", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d; body: %s", w.Code, w.Body.String())
		}
	})

	t.Run("GET returns method not allowed", func(t *testing.T) {
		m := manifest.New("test-bucket", "logs/")
		store := &mockManifestStore{manifest: m}
		handler := HandleManifestUpdate(store, "")

		req := httptest.NewRequest(http.MethodGet, "/internal/manifest/update", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected status 405, got %d", w.Code)
		}
	})

	t.Run("POST with invalid JSON returns bad request", func(t *testing.T) {
		m := manifest.New("test-bucket", "logs/")
		store := &mockManifestStore{manifest: m}
		handler := HandleManifestUpdate(store, "")

		req := httptest.NewRequest(http.MethodPost, "/internal/manifest/update", strings.NewReader("{invalid"))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("POST with valid auth succeeds", func(t *testing.T) {
		m := manifest.New("test-bucket", "logs/")
		store := &mockManifestStore{manifest: m}
		handler := HandleManifestUpdate(store, "update-secret")

		body := `{"added": [], "removed": [], "source": "peer"}`
		req := httptest.NewRequest(http.MethodPost, "/internal/manifest/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer update-secret")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("POST with invalid auth returns unauthorized", func(t *testing.T) {
		m := manifest.New("test-bucket", "logs/")
		store := &mockManifestStore{manifest: m}
		handler := HandleManifestUpdate(store, "update-secret")

		body := `{"added": [], "removed": [], "source": "peer"}`
		req := httptest.NewRequest(http.MethodPost, "/internal/manifest/update", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer bad-secret")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Fatalf("expected status 401, got %d", w.Code)
		}
	})

	t.Run("POST with empty body returns bad request", func(t *testing.T) {
		m := manifest.New("test-bucket", "logs/")
		store := &mockManifestStore{manifest: m}
		handler := HandleManifestUpdate(store, "")

		req := httptest.NewRequest(http.MethodPost, "/internal/manifest/update", strings.NewReader(""))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", w.Code)
		}
	})
}
