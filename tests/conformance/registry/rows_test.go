package registry

import "testing"

func TestRows_LoadAndCounts(t *testing.T) {
	reg, err := LoadDir("rows")
	if err != nil {
		t.Fatal(err)
	}
	count := func(kind Kind, surface Surface) int {
		n := 0
		for _, r := range reg.Rows {
			if r.Kind == kind && r.Surface == surface {
				n++
			}
		}
		return n
	}
	if n := count(KindPipe, SurfaceVL); n < 52 {
		t.Fatalf("want >= 52 pipe rows, got %d", n)
	}
	if n := count(KindFilter, SurfaceVL); n < 32 {
		t.Fatalf("want >= 32 filter rows, got %d", n)
	}
	if n := count(KindStats, SurfaceVL); n < 26 {
		t.Fatalf("want >= 26 stats rows, got %d", n)
	}
	if n := count(KindTraceQL, SurfaceVT); n < 9 {
		t.Fatalf("want >= 9 traceql rows, got %d", n)
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
