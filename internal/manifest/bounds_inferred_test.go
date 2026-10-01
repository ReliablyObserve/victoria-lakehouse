package manifest

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Time bounds the manifest infers from a listing (the partition hour) must be
// told apart from exact ones: the buffer watermark is the newest MaxTimeNs among
// the selected objects, and an inferred hour end hides the rest of the hour.

const (
	biPart = "dt=2026-10-01/hour=07"
	biKey  = "0/0/logs/" + biPart + "/a.parquet"
)

func biHourStart() int64 { return time.Date(2026, 10, 1, 7, 0, 0, 0, time.UTC).UnixNano() }
func biHourEnd() int64   { return biHourStart() + int64(time.Hour) - 1 }

func biFile(t *testing.T, m *Manifest, key string) FileInfo {
	t.Helper()
	fi, ok := m.GetFileByKey(key)
	if !ok {
		t.Fatalf("%s is not in the manifest", key)
	}
	return fi
}

func TestBoundsInferred_ListingOnlyObjectIsMarked(t *testing.T) {
	m := New("b", "")
	if !m.ApplyListing([]ListedObject{{Key: biKey, Size: 10}}, time.Now()) {
		t.Fatal("listing rejected")
	}
	fi := biFile(t, m, biKey)
	if !fi.BoundsInferred {
		t.Fatal("a listing-only object must be marked BoundsInferred")
	}
	if fi.MinTimeNs != biHourStart() || fi.MaxTimeNs != biHourEnd() {
		t.Fatalf("inferred bounds = [%d, %d], want the partition hour [%d, %d]", fi.MinTimeNs, fi.MaxTimeNs, biHourStart(), biHourEnd())
	}
	if mn, mx := fi.ExactBounds(); mn != 0 || mx != 0 {
		t.Fatalf("ExactBounds of an inferred entry = (%d, %d), want (0, 0)", mn, mx)
	}
}

func TestBoundsInferred_ExactEntryStaysExactAcrossListing(t *testing.T) {
	m := New("b", "")
	exMin, exMax := biHourStart()+int64(10*time.Minute), biHourStart()+int64(12*time.Minute)
	m.AddFile(biPart, FileInfo{Key: biKey, Size: 10, RowCount: 3, MinTimeNs: exMin, MaxTimeNs: exMax})
	// A refresh lists the same object again: the bounds the flush recorded win.
	if !m.ApplyListing([]ListedObject{{Key: biKey, Size: 10}}, time.Now().Add(-time.Minute)) {
		t.Fatal("listing rejected")
	}
	fi := biFile(t, m, biKey)
	if fi.BoundsInferred || fi.MinTimeNs != exMin || fi.MaxTimeNs != exMax {
		t.Fatalf("exact entry after listing = %+v, want exact bounds [%d, %d] and no inferred mark", fi, exMin, exMax)
	}
}

func TestBoundsInferred_EnrichOverwritesInferredBounds(t *testing.T) {
	m := New("b", "")
	m.ApplyListing([]ListedObject{{Key: biKey, Size: 10}}, time.Now())
	exMin, exMax := biHourStart()+int64(10*time.Minute), biHourStart()+int64(12*time.Minute)

	// Without usable exact bounds (a footer-only read has a row count only) the
	// inferred ones stay, still marked.
	m.EnrichFileMetadata(biKey, 3, 0, 0)
	if fi := biFile(t, m, biKey); !fi.BoundsInferred || fi.RowCount != 3 || fi.MaxTimeNs != biHourEnd() {
		t.Fatalf("row count only: %+v, want bounds still inferred", fi)
	}
	// A lone or inverted bound is not a range.
	m.EnrichFileMetadata(biKey, 3, exMin, 0)
	m.EnrichFileMetadata(biKey, 3, exMax, exMin)
	if fi := biFile(t, m, biKey); !fi.BoundsInferred || fi.MinTimeNs != biHourStart() || fi.MaxTimeNs != biHourEnd() {
		t.Fatalf("partial bounds changed the entry: %+v", fi)
	}
	// Exact bounds REPLACE the inferred ones (not fill-zero) and clear the mark.
	m.EnrichFileMetadata(biKey, 3, exMin, exMax)
	fi := biFile(t, m, biKey)
	if fi.BoundsInferred || fi.MinTimeNs != exMin || fi.MaxTimeNs != exMax {
		t.Fatalf("after exact enrich: %+v, want [%d, %d] exact", fi, exMin, exMax)
	}
	// Exact bounds are never overwritten again.
	m.EnrichFileMetadata(biKey, 3, exMin-1, exMax+1)
	if fi := biFile(t, m, biKey); fi.MinTimeNs != exMin || fi.MaxTimeNs != exMax {
		t.Fatalf("exact bounds were overwritten: %+v", fi)
	}
}

func TestBoundsInferred_FileMetaApplyToOverwritesInferred(t *testing.T) {
	exMin, exMax := biHourStart()+5, biHourStart()+9
	fi := FileInfo{Key: biKey, MinTimeNs: biHourStart(), MaxTimeNs: biHourEnd(), BoundsInferred: true}
	FileMeta{RowCount: 4, MinTimeNs: exMin, MaxTimeNs: exMax}.ApplyTo(&fi)
	if fi.BoundsInferred || fi.MinTimeNs != exMin || fi.MaxTimeNs != exMax || fi.RowCount != 4 {
		t.Fatalf("ApplyTo over inferred bounds: %+v", fi)
	}
	// A meta with no bounds leaves the mark in place.
	fi = FileInfo{Key: biKey, MinTimeNs: biHourStart(), MaxTimeNs: biHourEnd(), BoundsInferred: true}
	FileMeta{RowCount: 4}.ApplyTo(&fi)
	if !fi.BoundsInferred || fi.MaxTimeNs != biHourEnd() || fi.RowCount != 4 {
		t.Fatalf("ApplyTo without bounds: %+v", fi)
	}
	// Exact bounds already known win over the meta (fill-zero, as before).
	fi = FileInfo{Key: biKey, MinTimeNs: 1, MaxTimeNs: 2}
	FileMeta{MinTimeNs: exMin, MaxTimeNs: exMax}.ApplyTo(&fi)
	if fi.MinTimeNs != 1 || fi.MaxTimeNs != 2 {
		t.Fatalf("ApplyTo overwrote exact bounds: %+v", fi)
	}
}

func TestBoundsInferred_NeverExportedAsFact(t *testing.T) {
	inferred := FileInfo{Key: biKey, RowCount: 3, MinTimeNs: biHourStart(), MaxTimeNs: biHourEnd(), BoundsInferred: true}
	if fm := FileInfoToMeta(inferred); fm.MinTimeNs != 0 || fm.MaxTimeNs != 0 || fm.RowCount != 3 {
		t.Fatalf("FileInfoToMeta exported inferred bounds: %+v", fm)
	}
	exact := FileInfo{Key: biKey, RowCount: 3, MinTimeNs: 5, MaxTimeNs: 9}
	if fm := FileInfoToMeta(exact); fm.MinTimeNs != 5 || fm.MaxTimeNs != 9 {
		t.Fatalf("FileInfoToMeta dropped exact bounds: %+v", fm)
	}
	if mn, mx := exact.ExactBounds(); mn != 5 || mx != 9 {
		t.Fatalf("ExactBounds of an exact entry = (%d, %d)", mn, mx)
	}
}

// The restart story at the manifest level: the snapshot taken before the final
// flush lacks the flushed object, the listing finds it, and exact bounds then
// arrive from the footer or the facet. A snapshot taken AFTER the final flush
// carries them from the start.
func TestBoundsInferred_SnapshotAfterFlushLoadsExactBounds(t *testing.T) {
	exMin, exMax := biHourStart()+int64(10*time.Minute), biHourStart()+int64(12*time.Minute)
	dir := t.TempDir()

	m1 := New("b", "")
	before := filepath.Join(dir, "before.snapshot")
	if err := m1.SaveTo(before); err != nil { // snapshot BEFORE the final flush
		t.Fatal(err)
	}
	m1.AddFile(biPart, FileInfo{Key: biKey, Size: 10, RowCount: 3, MinTimeNs: exMin, MaxTimeNs: exMax}) // the final flush
	after := filepath.Join(dir, "after.snapshot")
	if err := m1.SaveTo(after); err != nil { // snapshot AFTER it
		t.Fatal(err)
	}

	restart := func(snapshot string) *Manifest {
		m := New("b", "")
		if err := m.LoadFrom(snapshot); err != nil {
			t.Fatal(err)
		}
		if !m.ApplyListing([]ListedObject{{Key: biKey, Size: 10}}, time.Now().Add(-time.Minute)) {
			t.Fatal("listing rejected")
		}
		return m
	}

	if fi := biFile(t, restart(before), biKey); !fi.BoundsInferred || fi.MaxTimeNs != biHourEnd() {
		t.Fatalf("restart from the old snapshot: %+v, want the object learned by listing with inferred bounds", fi)
	}
	fi := biFile(t, restart(after), biKey)
	if fi.BoundsInferred || fi.MinTimeNs != exMin || fi.MaxTimeNs != exMax || fi.RowCount != 3 {
		t.Fatalf("restart from the post-flush snapshot: %+v, want exact bounds [%d, %d]", fi, exMin, exMax)
	}
}

// An inferred mark survives a snapshot round trip, so a restart does not mistake
// the hour for the object's range.
func TestBoundsInferred_FlagSurvivesSnapshot(t *testing.T) {
	m := New("b", "")
	m.ApplyListing([]ListedObject{{Key: biKey, Size: 10}}, time.Now())
	path := filepath.Join(t.TempDir(), "s.snapshot")
	if err := m.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	m2 := New("b", "")
	if err := m2.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	if fi := biFile(t, m2, biKey); !fi.BoundsInferred {
		t.Fatalf("snapshot round trip lost the inferred mark: %+v", fi)
	}
}

// Older versions stored the inferred partition hour unmarked, in snapshots, the
// file-metadata cache and the facets: handing it back is not evidence of the
// object's real range.
func TestBoundsInferred_HourShapedBoundsAreNotExact(t *testing.T) {
	t.Run("enrich", func(t *testing.T) {
		m := New("b", "")
		m.ApplyListing([]ListedObject{{Key: biKey, Size: 10}}, time.Now())
		m.EnrichFileMetadata(biKey, 3, biHourStart(), biHourEnd()) // a v0.143.7 cache entry
		if fi := biFile(t, m, biKey); !fi.BoundsInferred || fi.RowCount != 3 {
			t.Errorf("hour-shaped bounds cleared the flag: %+v", fi)
		}
	})
	t.Run("ApplyTo", func(t *testing.T) {
		fi := FileInfo{Key: biKey, MinTimeNs: biHourStart(), MaxTimeNs: biHourEnd(), BoundsInferred: true}
		FileMeta{RowCount: 3, MinTimeNs: biHourStart(), MaxTimeNs: biHourEnd()}.ApplyTo(&fi)
		if !fi.BoundsInferred {
			t.Errorf("hour-shaped meta cleared the flag: %+v", fi)
		}
	})
	t.Run("legacy snapshot", func(t *testing.T) {
		old := New("b", "")
		old.AddFile(biPart, FileInfo{Key: biKey, Size: 10, RowCount: 3, MinTimeNs: biHourStart(), MaxTimeNs: biHourEnd()}) // unflagged, as v0.143.7 wrote it
		exactKey := "0/0/logs/" + biPart + "/b.parquet"
		old.AddFile(biPart, FileInfo{Key: exactKey, Size: 10, RowCount: 3, MinTimeNs: biHourStart() + 5, MaxTimeNs: biHourStart() + 9})
		path := filepath.Join(t.TempDir(), "s.snapshot")
		if err := old.SaveTo(path); err != nil {
			t.Fatal(err)
		}
		m := New("b", "")
		if err := m.LoadFrom(path); err != nil {
			t.Fatal(err)
		}
		if fi := biFile(t, m, biKey); !fi.BoundsInferred {
			t.Errorf("an hour-wide entry from an older snapshot was accepted as exact: %+v", fi)
		}
		if fi := biFile(t, m, exactKey); fi.BoundsInferred {
			t.Errorf("an exact entry was marked inferred on load: %+v", fi)
		}
	})
}

// A persist that outlived its shutdown timeout must not race the next one.
func TestSaveTo_ConcurrentSavesDoNotCollide(t *testing.T) {
	m := New("b", "")
	for i := 0; i < 50; i++ {
		m.AddFile(biPart, FileInfo{Key: "0/0/logs/" + biPart + "/f" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".parquet", Size: 1, RowCount: 1, MinTimeNs: biHourStart() + 1, MaxTimeNs: biHourStart() + 2})
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest-snapshot.json")
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := m.SaveTo(path); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("SaveTo: %v", err)
	}
	loaded := New("b", "")
	if err := loaded.LoadFrom(path); err != nil {
		t.Fatalf("snapshot unreadable after concurrent saves: %v", err)
	}
	if loaded.TotalFiles() != 50 {
		t.Errorf("snapshot holds %d files, want 50", loaded.TotalFiles())
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("directory holds %d entries after the saves, want only the snapshot", len(entries))
	}
}

func TestHourShaped_EdgeCases(t *testing.T) {
	if hourShaped("no-partition/key.parquet", 1, 2) {
		t.Error("a key without a partition cannot be hour-shaped")
	}
	if hourShaped("0/0/logs/dt=2026-13-45/hour=99/a.parquet", 1, 2) {
		t.Error("an unparsable partition cannot be hour-shaped")
	}
	if hourShaped(biKey, biHourStart(), biHourEnd()-1) || hourShaped(biKey, biHourStart()+1, biHourEnd()) {
		t.Error("only exactly [hour, hour+1h-1ns] is hour-shaped")
	}
	if !hourShaped(biKey, biHourStart(), biHourEnd()) {
		t.Error("the exact partition hour must be hour-shaped")
	}
}

func TestSaveTo_UnwritableDirectoryFails(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := New("b", "").SaveTo(filepath.Join(blocker, "sub", "snap.json")); err == nil {
		t.Error("SaveTo under a regular file must fail")
	}
	ro := filepath.Join(dir, "ro")
	if err := os.Mkdir(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(ro, 0o700) }()
	if os.Geteuid() != 0 {
		if err := New("b", "").SaveTo(filepath.Join(ro, "snap.json")); err == nil {
			t.Error("SaveTo into a read-only directory must fail (temp file creation)")
		}
	}
}
