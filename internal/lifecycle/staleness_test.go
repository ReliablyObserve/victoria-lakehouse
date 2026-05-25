package lifecycle

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func testStartupConfig() config.StartupConfig {
	return config.StartupConfig{
		StaleThreshold:    1 * time.Hour,
		WALReconciliation: true,
		CacheRevalidation: true,
		MaxResyncTime:     10 * time.Minute,
	}
}

func TestStalenessDetector_FreshManifest(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	d := NewStalenessDetector(testStartupConfig(), manifestPath)
	d.Check()

	if d.IsStale() {
		t.Error("fresh manifest should not be stale")
	}
}

func TestStalenessDetector_StaleManifest(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Set modification time to 2 hours ago
	staleTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(manifestPath, staleTime, staleTime); err != nil {
		t.Fatal(err)
	}

	d := NewStalenessDetector(testStartupConfig(), manifestPath)
	d.Check()

	if !d.IsStale() {
		t.Error("2-hour-old manifest should be stale (threshold 1h)")
	}
	if d.StalenessAge() < 1*time.Hour {
		t.Errorf("staleness age %v should be >= 1h", d.StalenessAge())
	}
}

func TestStalenessDetector_MissingManifest(t *testing.T) {
	d := NewStalenessDetector(testStartupConfig(), "/nonexistent/manifest.json")
	d.Check()

	if d.IsStale() {
		t.Error("missing manifest should be treated as fresh start, not stale")
	}
}

type mockManifestChecker struct {
	covered map[int64]bool
}

func (m *mockManifestChecker) HasFileForTimestamp(ts int64) bool {
	return m.covered[ts]
}

func TestReconcileWAL_SomeNeedReflush(t *testing.T) {
	d := NewStalenessDetector(testStartupConfig(), "")
	d.staleDetected = true // force stale

	entries := []WALEntry{
		{TimestampNs: 1000, IsLog: true},
		{TimestampNs: 2000, IsLog: true},
		{TimestampNs: 3000, IsLog: true},
	}
	checker := &mockManifestChecker{covered: map[int64]bool{
		1000: true,  // already in S3
		2000: false, // not in S3
		3000: true,  // already in S3
	}}

	needsReflush := d.ReconcileWAL(entries, checker)
	if needsReflush != 1 {
		t.Errorf("needs reflush = %d, want 1", needsReflush)
	}
	if !d.WALReconciled() {
		t.Error("should be marked as reconciled")
	}
}

func TestReconcileWAL_SkipsWhenNotStale(t *testing.T) {
	d := NewStalenessDetector(testStartupConfig(), "")
	// staleDetected is false

	entries := []WALEntry{{TimestampNs: 1000}}
	checker := &mockManifestChecker{covered: map[int64]bool{}}

	needsReflush := d.ReconcileWAL(entries, checker)
	if needsReflush != 0 {
		t.Errorf("should skip reconciliation when not stale, got %d", needsReflush)
	}
}

func TestReconcileWAL_SkipsWhenDisabled(t *testing.T) {
	cfg := testStartupConfig()
	cfg.WALReconciliation = false
	d := NewStalenessDetector(cfg, "")
	d.staleDetected = true

	entries := []WALEntry{{TimestampNs: 1000}}
	checker := &mockManifestChecker{covered: map[int64]bool{}}

	needsReflush := d.ReconcileWAL(entries, checker)
	if needsReflush != 0 {
		t.Errorf("should skip reconciliation when disabled, got %d", needsReflush)
	}
}

func TestInvalidateCache_Stale(t *testing.T) {
	d := NewStalenessDetector(testStartupConfig(), "")
	d.staleDetected = true

	d.InvalidateCache(500)
	if !d.CacheRevalidated() {
		t.Error("should be marked as revalidated")
	}
}

func TestInvalidateCache_NotStale(t *testing.T) {
	d := NewStalenessDetector(testStartupConfig(), "")

	d.InvalidateCache(500)
	if d.CacheRevalidated() {
		t.Error("should not revalidate when not stale")
	}
}

func TestStalenessInfo(t *testing.T) {
	d := NewStalenessDetector(testStartupConfig(), "")
	d.staleDetected = true
	d.stalenessAge = 2 * time.Hour
	d.walReconciled = true
	d.cacheRevalidated = true

	info := d.Info()
	if !info.StaleDetected {
		t.Error("info should show stale")
	}
	if info.StalenessAge != 2*time.Hour {
		t.Errorf("staleness age = %v, want 2h", info.StalenessAge)
	}
}
