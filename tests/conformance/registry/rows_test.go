package registry

import "testing"

// TestRows_LoadAndCounts is the controller ruling: coverage is measured by
// DISTINCT upstream keys per kind (one real inventory item can be exercised
// by more than one row — e.g. an edge case or a differ variant — without
// inflating the count), not by raw row counts. Thresholds match the real
// upstream inventory (pipes 48, filters 33, stats 24, traceql 9), and no row
// may cite one of the 7 phantom names that were removed from the brief's
// original lists because they are not real upstream inventory items.
func TestRows_LoadAndCounts(t *testing.T) {
	reg, err := LoadDir("rows")
	if err != nil {
		t.Fatal(err)
	}

	distinctUpstream := func(kind Kind, surface Surface) int {
		seen := map[string]bool{}
		for _, r := range reg.Rows {
			if r.Kind == kind && r.Surface == surface && r.Upstream != nil {
				seen[r.Upstream.Key()] = true
			}
		}
		return len(seen)
	}
	if n := distinctUpstream(KindPipe, SurfaceVL); n < 48 {
		t.Fatalf("want >= 48 distinct pipe upstream keys, got %d", n)
	}
	if n := distinctUpstream(KindFilter, SurfaceVL); n < 33 {
		t.Fatalf("want >= 33 distinct filter upstream keys, got %d", n)
	}
	if n := distinctUpstream(KindStats, SurfaceVL); n < 24 {
		t.Fatalf("want >= 24 distinct stats upstream keys, got %d", n)
	}
	if n := distinctUpstream(KindTraceQL, SurfaceVT); n < 9 {
		t.Fatalf("want >= 9 distinct traceql upstream keys, got %d", n)
	}

	phantom := map[string]bool{
		"pipe:pack": true, "pipe:unpack": true, "pipe:update": true, "pipe:sort_topk": true,
		"stats:json_values_sorted": true, "stats:json_values_topk": true,
		"filter:generic": true,
	}
	for _, r := range reg.Rows {
		if r.Upstream != nil && phantom[r.Upstream.Key()] {
			t.Fatalf("row %s cites phantom upstream item %s (not in the real inventory)", r.ID, r.Upstream.Key())
		}
	}

	if reg.ByID["vl.select.tail.unsupported"] == nil || reg.ByID["vl.select.tail.unsupported"].Expect != ExpectUnsupported {
		t.Fatal("tail row must be declared unsupported")
	}
	if reg.ByID["vt.tempo.metrics_instant.absent"] == nil {
		t.Fatal("metrics/instant must be declared absent (upstream lacks it too)")
	}
	if reg.ByID["lh.stats.overview.schema"] == nil {
		t.Fatal("LH stats overview row missing")
	}
}
