package pmeta

import (
	"bytes"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
)

func hasKey(keys []string, want string) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}

var bloomCols = map[string][]string{
	"service.name": {"api-gateway", "order-service", "user-service"},
	"trace_id":     {"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11", "t12"},
}

// TestBloomFacet_ParityWithLegacyIndex: the facet's blooms must behave identically
// to the legacy PartitionedIndex (same BuildFileColumns) — the dual-write gate.
func TestBloomFacet_ParityWithLegacyIndex(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	pi.AddFile("p", "fileA", bloomCols)
	legacy := pi.GetPartition("p")

	f := NewBloomFactory(0.01)("p").(*bloomFacet)
	f.Merge(FileContribution{Partition: "p", FileKey: "fileA", BloomValues: bloomCols})

	for col, vs := range bloomCols {
		for _, v := range vs {
			gotF := hasKey(f.mayContain([]string{"fileA"}, col, v), "fileA")
			gotL := hasKey(legacy.MayContain([]string{"fileA"}, col, v), "fileA")
			if !gotF || !gotL {
				t.Errorf("present %s=%s: facet=%v legacy=%v (both want true)", col, v, gotF, gotL)
			}
		}
	}
	// Absent values: facet and legacy must agree (same FP behaviour).
	for _, v := range []string{"nope-1", "absent-zzz", "missing"} {
		fa := hasKey(f.mayContain([]string{"fileA"}, "service.name", v), "fileA")
		la := hasKey(legacy.MayContain([]string{"fileA"}, "service.name", v), "fileA")
		if fa != la {
			t.Errorf("absent %q: facet=%v legacy=%v disagree", v, fa, la)
		}
	}
}

func TestBloomFacet_EncodeDecodeRoundTrip(t *testing.T) {
	f := NewBloomFactory(0.01)("p").(*bloomFacet)
	if f.Kind() != FacetBloom {
		t.Fatalf("Kind = %d, want %d", f.Kind(), FacetBloom)
	}
	f.Merge(FileContribution{FileKey: "k1", BloomValues: bloomCols})
	// no-op merges (must not panic or add)
	f.Merge(FileContribution{FileKey: "", BloomValues: bloomCols})
	f.Merge(FileContribution{FileKey: "k2"})
	if f.EstimateBytes() <= 0 {
		t.Fatal("EstimateBytes should be > 0 after a merge")
	}

	var buf bytes.Buffer
	if err := f.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	g := NewBloomFactory(0.01)("p").(*bloomFacet)
	if err := g.Decode(&buf); err != nil {
		t.Fatal(err)
	}
	for _, v := range bloomCols["trace_id"] {
		if !hasKey(g.mayContain([]string{"k1"}, "trace_id", v), "k1") {
			t.Errorf("decoded facet missing trace_id=%s", v)
		}
	}
}

func TestStore_BloomMayContain(t *testing.T) {
	s := NewStore()
	s.Register(FacetBloom, NewBloomFactory(0.01))

	// Unknown partition → ok=false (caller falls back to legacy bloom).
	if _, ok := s.BloomMayContain("missing", []string{"k"}, "c", "v"); ok {
		t.Fatal("unknown partition must report ok=false")
	}

	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "f1", BloomValues: bloomCols})
	got, ok := s.BloomMayContain("p", []string{"f1"}, "service.name", "api-gateway")
	if !ok {
		t.Fatal("bloom facet should exist after flush")
	}
	if !hasKey(got, "f1") {
		t.Fatalf("present value not found via Store.BloomMayContain: %v", got)
	}
}
