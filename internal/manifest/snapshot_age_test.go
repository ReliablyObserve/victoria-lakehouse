package manifest

import (
	"path/filepath"
	"testing"
	"time"
)

// TestSavedAt_RecordedOnSaveTo pins the contract: SaveTo updates
// the in-memory SavedAt timestamp atomically with the on-disk
// snapshot rename. Without this the `lakehouse_manifest_snapshot_age_seconds`
// metric would report a stale or unset age even though the
// snapshot was just persisted.
func TestSavedAt_RecordedOnSaveTo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.bin")

	m := New("bucket", "prefix/")
	m.AddFile("dt=2026-06-05/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	if !m.SavedAt().IsZero() {
		t.Errorf("fresh manifest reports SavedAt=%v, want zero", m.SavedAt())
	}

	before := time.Now()
	if err := m.SaveTo(path); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}
	after := time.Now()

	got := m.SavedAt()
	if got.Before(before) || got.After(after) {
		t.Errorf("SavedAt %v not in [%v, %v] — timestamp didn't track the actual save", got, before, after)
	}
}

// TestSavedAt_RestoredOnLoadFrom pins that LoadFrom rehydrates
// SavedAt from the persisted snapshot. A pod that just restarted
// with a 1-hour-old snapshot must report a 1-hour age — operators
// need that signal to know they're running on stale data.
func TestSavedAt_RestoredOnLoadFrom(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.bin")

	// Write a snapshot from one manifest.
	src := New("bucket", "prefix/")
	src.AddFile("dt=2026-06-05/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	if err := src.SaveTo(path); err != nil {
		t.Fatalf("seed SaveTo: %v", err)
	}
	srcSavedAt := src.SavedAt()
	if srcSavedAt.IsZero() {
		t.Fatalf("seed SavedAt is zero — SaveTo bug")
	}

	// Load it from a fresh manifest; SavedAt should match the source's.
	dst := New("bucket", "prefix/")
	if err := dst.LoadFrom(path); err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	got := dst.SavedAt()
	if !got.Equal(srcSavedAt) {
		t.Errorf("LoadFrom didn't restore SavedAt; got %v want %v", got, srcSavedAt)
	}
}

// TestSavedAt_NoOverwriteWhenSaveFails ensures a failed SaveTo
// (e.g. disk full) doesn't reset the SavedAt to "now" — the
// metric would otherwise report a fresh snapshot even though
// disk has none. We simulate the failure by saving to an
// unwritable path.
func TestSavedAt_NoOverwriteWhenSaveFails(t *testing.T) {
	m := New("bucket", "prefix/")
	m.AddFile("dt=2026-06-05/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	// First successful save sets the baseline.
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.bin")
	if err := m.SaveTo(path); err != nil {
		t.Fatalf("baseline SaveTo: %v", err)
	}
	baseline := m.SavedAt()

	// Sleep so a successful re-save would have a different timestamp.
	time.Sleep(10 * time.Millisecond)

	// Attempt to save to an unwritable path (directory that doesn't exist
	// AND can't be created — root-only path under /proc).
	badPath := "/proc/self/cannot-create-manifest-here/manifest.bin"
	err := m.SaveTo(badPath)
	if err == nil {
		t.Skip("expected SaveTo to fail on /proc path; environment allows it")
	}

	// SavedAt should still be the baseline, not the failed-write time.
	got := m.SavedAt()
	if !got.Equal(baseline) {
		t.Errorf("SavedAt advanced after failed SaveTo; got %v want %v (baseline)", got, baseline)
	}
}
