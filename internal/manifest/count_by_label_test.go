package manifest

import (
	"path/filepath"
	"testing"
	"time"
)

// TestCountByLabel covers the PERF-2 fast-path contract: interior files are
// summed from LabelAggregates; a file straddling the range boundary is NOT
// summed (its whole-file aggregate would over-count rows outside the window) and
// flips complete=false so the caller knows to scan it.
func TestCountByLabel(t *testing.T) {
	m := New("bucket", "")
	base := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	const part = "dt=2026-05-10/hour=12"
	add := func(key string, minOff, maxOff time.Duration, sg map[string]int64) {
		m.AddFile(part, FileInfo{
			Key:             "1/1/" + part + "/" + key,
			RowCount:        1,
			MinTimeNs:       base.Add(minOff).UnixNano(),
			MaxTimeNs:       base.Add(maxOff).UnixNano(),
			LabelAggregates: map[string]map[string]int64{"service.name": sg},
		})
	}
	start := base.Add(5 * time.Minute).UnixNano()
	end := base.Add(50 * time.Minute).UnixNano()

	// Two interior files (fully within [+5m,+50m]).
	add("a.parquet", 10*time.Minute, 20*time.Minute, map[string]int64{"api-gateway": 100, "user-service": 50})
	add("b.parquet", 25*time.Minute, 40*time.Minute, map[string]int64{"api-gateway": 30})

	counts, uncovered, complete := m.CountByLabel(start, end, "1", "1", "service.name")
	if !complete || len(uncovered) != 0 {
		t.Fatalf("all-interior: want complete & 0 uncovered, got complete=%v uncovered=%d", complete, len(uncovered))
	}
	if counts["api-gateway"] != 130 || counts["user-service"] != 50 {
		t.Fatalf("interior sum wrong: %v", counts)
	}

	// Boundary file straddling `end` (+45m..+70m) — must NOT be summed.
	add("c.parquet", 45*time.Minute, 70*time.Minute, map[string]int64{"order-service": 999})
	counts2, uncovered2, complete2 := m.CountByLabel(start, end, "1", "1", "service.name")
	if complete2 {
		t.Fatal("boundary file must flip complete=false")
	}
	if len(uncovered2) != 1 {
		t.Fatalf("want 1 uncovered file, got %d", len(uncovered2))
	}
	if _, leaked := counts2["order-service"]; leaked {
		t.Fatalf("boundary aggregate must NOT be summed (over-count risk): %v", counts2)
	}
	if counts2["api-gateway"] != 130 {
		t.Fatalf("interior must still be summed: %v", counts2)
	}

	// Snapshot round-trip: LabelAggregates must survive Save/Load.
	path := filepath.Join(t.TempDir(), "manifest.bin")
	if err := m.SaveTo(path); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}
	m2 := New("bucket", "")
	if err := m2.LoadFrom(path); err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	c3, _, _ := m2.CountByLabel(start, end, "1", "1", "service.name")
	if c3["api-gateway"] != 130 || c3["user-service"] != 50 {
		t.Fatalf("LabelAggregates not persisted across snapshot: %v", c3)
	}
}

// TestCountByLabel_MissingAggregate: a fully-contained file WITHOUT an aggregate
// for the field (old file / capped field) is uncovered, not silently zero.
func TestCountByLabel_MissingAggregate(t *testing.T) {
	m := New("bucket", "")
	base := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	const part = "dt=2026-05-10/hour=12"
	m.AddFile(part, FileInfo{
		Key:       "1/1/" + part + "/old.parquet",
		RowCount:  1,
		MinTimeNs: base.Add(10 * time.Minute).UnixNano(),
		MaxTimeNs: base.Add(20 * time.Minute).UnixNano(),
		// no LabelAggregates (predates the feature)
	})
	_, uncovered, complete := m.CountByLabel(
		base.UnixNano(), base.Add(time.Hour).UnixNano(), "1", "1", "service.name")
	if complete || len(uncovered) != 1 {
		t.Fatalf("file without aggregate must be uncovered: complete=%v uncovered=%d", complete, len(uncovered))
	}
}
