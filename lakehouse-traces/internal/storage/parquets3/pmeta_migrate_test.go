package parquets3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestCleanupLegacyGlobalBundles_KeySafety pins the exact key strings the
// cleanup computes and proves the OLD (pure dt=/hour=) bundle key can NEVER
// equal the NEW tenant-scoped bundle key — so the cleanup cannot touch a live
// tenant bundle. This needs no S3.
func TestCleanupLegacyGlobalBundles_KeySafety(t *testing.T) {
	const prefix = "logs/"
	const fileKey = "1/1/logs/dt=2026-06-09/hour=10/x.parquet"

	oldBundleKey := prefix + manifest.ExtractPartition(fileKey) + "/_pmeta.bundle"
	oldBloomKey := prefix + manifest.ExtractPartition(fileKey) + "/_bloom.bin"
	tenantBundleKey := prefix + manifest.ExtractTenantPartition(fileKey) + "/_pmeta.bundle"

	const wantOldBundle = "logs/dt=2026-06-09/hour=10/_pmeta.bundle"
	const wantOldBloom = "logs/dt=2026-06-09/hour=10/_bloom.bin"
	const wantTenantBundle = "logs/1/1/logs/dt=2026-06-09/hour=10/_pmeta.bundle"

	if oldBundleKey != wantOldBundle {
		t.Fatalf("old bundle key = %q, want %q", oldBundleKey, wantOldBundle)
	}
	if oldBloomKey != wantOldBloom {
		t.Fatalf("old bloom key = %q, want %q", oldBloomKey, wantOldBloom)
	}
	if tenantBundleKey != wantTenantBundle {
		t.Fatalf("tenant bundle key = %q, want %q", tenantBundleKey, wantTenantBundle)
	}
	if oldBundleKey == tenantBundleKey {
		t.Fatalf("SAFETY VIOLATION: old bundle key equals tenant bundle key (%q)", oldBundleKey)
	}
	// The old key must NOT carry the tenant prefix segment ("1/1/logs/" after
	// the AutoPrefix); the tenant key must.
	if strings.HasPrefix(strings.TrimPrefix(oldBundleKey, prefix), "1/1/logs/") {
		t.Fatalf("old bundle key unexpectedly carries tenant prefix: %q", oldBundleKey)
	}
	if !strings.HasPrefix(strings.TrimPrefix(tenantBundleKey, prefix), "1/1/logs/") {
		t.Fatalf("tenant bundle key missing tenant prefix: %q", tenantBundleKey)
	}
}

// TestCleanupLegacyGlobalBundles_DeletesOnlyOldKeys_AndMarker exercises the
// full cleanup against an in-memory S3 backend: it deletes exactly the old
// global bundle objects, leaves the tenant-scoped bundle untouched, writes the
// marker, and is a no-op on a second run (marker present).
func TestCleanupLegacyGlobalBundles_DeletesOnlyOldKeys_AndMarker(t *testing.T) {
	mock := newMigMockS3()
	defer mock.close()

	s := newMigTestStorage(t, mock.url())
	prefix := s.cfg.AutoPrefix() // "logs/"

	// Two files across two partitions, manifest keyed by pure dt=/hour=.
	fileA := "1/1/logs/dt=2026-06-09/hour=10/a.parquet"
	fileB := "1/1/logs/dt=2026-06-09/hour=11/b.parquet"
	s.manifest.AddFile(manifest.ExtractPartition(fileA), manifest.FileInfo{Key: fileA, Size: 1})
	s.manifest.AddFile(manifest.ExtractPartition(fileB), manifest.FileInfo{Key: fileB, Size: 1})

	// Seed S3: old global bundles (must be deleted), tenant bundles (must survive).
	oldKeys := []string{
		prefix + "dt=2026-06-09/hour=10/_pmeta.bundle",
		prefix + "dt=2026-06-09/hour=10/_bloom.bin",
		prefix + "dt=2026-06-09/hour=11/_pmeta.bundle",
		prefix + "dt=2026-06-09/hour=11/_bloom.bin",
	}
	tenantKeys := []string{
		prefix + "1/1/logs/dt=2026-06-09/hour=10/_pmeta.bundle",
		prefix + "1/1/logs/dt=2026-06-09/hour=11/_pmeta.bundle",
	}
	for _, k := range oldKeys {
		mock.put(k, []byte("old"))
	}
	for _, k := range tenantKeys {
		mock.put(k, []byte("keep"))
	}

	ctx := context.Background()
	deleted := s.CleanupLegacyGlobalBundles(ctx, prefix)
	if deleted != len(oldKeys) {
		t.Fatalf("deleted = %d, want %d", deleted, len(oldKeys))
	}

	for _, k := range oldKeys {
		if mock.has(k) {
			t.Errorf("old key not deleted: %q", k)
		}
	}
	for _, k := range tenantKeys {
		if !mock.has(k) {
			t.Errorf("tenant key was wrongly deleted: %q", k)
		}
	}
	markerKey := prefix + pmetaTenantIsolatedMarkerSuffix
	if !mock.has(markerKey) {
		t.Fatalf("marker not written: %q", markerKey)
	}

	// Second run: marker present → no-op (re-seed an old key to prove it is left alone).
	mock.put(oldKeys[0], []byte("old-again"))
	deleted2 := s.CleanupLegacyGlobalBundles(ctx, prefix)
	if deleted2 != 0 {
		t.Fatalf("second run deleted = %d, want 0 (marker gates it)", deleted2)
	}
	if !mock.has(oldKeys[0]) {
		t.Errorf("no-op run wrongly deleted re-seeded old key %q", oldKeys[0])
	}
}

// --- minimal in-memory S3 backend with PUT/GET/DELETE -----------------------

type migMockS3 struct {
	mu    sync.RWMutex
	files map[string][]byte
	srv   *httptest.Server
}

func newMigMockS3() *migMockS3 {
	m := &migMockS3{files: make(map[string][]byte)}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *migMockS3) put(key string, data []byte) {
	m.mu.Lock()
	m.files[key] = data
	m.mu.Unlock()
}

func (m *migMockS3) has(key string) bool {
	m.mu.RLock()
	_, ok := m.files[key]
	m.mu.RUnlock()
	return ok
}

func (m *migMockS3) handler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	key := parts[1]

	switch r.Method {
	case http.MethodPut:
		data, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		m.put(key, data)
		w.WriteHeader(http.StatusOK)
		return
	case http.MethodDelete:
		m.mu.Lock()
		delete(m.files, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	m.mu.RLock()
	data, ok := m.files[key]
	m.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>`)
		return
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (m *migMockS3) close()      { m.srv.Close() }
func (m *migMockS3) url() string { return m.srv.URL }

func newMigTestStorage(t *testing.T, s3url string) *Storage {
	t.Helper()
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3 = config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       s3url,
		ForcePathStyle: true,
		AccessKey:      "test",
		SecretKey:      "test",
	}
	pool, err := s3reader.NewClientPool(context.Background(), &cfg.S3)
	if err != nil {
		t.Fatalf("NewClientPool: %v", err)
	}
	return &Storage{
		cfg:        cfg,
		pool:       pool,
		manifest:   manifest.New("test-bucket", "logs/"),
		registry:   schema.NewRegistry(schema.LogsProfile),
		memCache:   cache.NewLRU(64 * 1024 * 1024),
		sfGroup:    cache.NewGroup(),
		labelIndex: cache.NewLabelIndex(),
		discovery:  discovery.New("", nil, "", "", "9428", 5*time.Second),
		dlSem:      make(chan struct{}, 4),
	}
}
