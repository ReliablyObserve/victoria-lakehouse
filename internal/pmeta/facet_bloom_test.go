package pmeta

import (
	"bytes"
	"runtime"
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

// FuzzBloomFacetDecode: arbitrary bytes fed straight into the bloom facet's
// Decode (bloomindex.Unmarshal underneath) must never panic, hang, or
// over-allocate (the index's entry-count cap + length validation bound
// allocation). The bundle codec's CRC normally shields this path, but Decode
// must hold on its own. Seeded with a valid Encode output.
func FuzzBloomFacetDecode(f *testing.F) {
	seedFacet := NewBloomFactory(0.01)("p").(*bloomFacet)
	seedFacet.Merge(FileContribution{FileKey: "k1", BloomValues: bloomCols})
	var seed bytes.Buffer
	if err := seedFacet.Encode(&seed); err != nil {
		f.Fatal(err)
	}
	f.Add(seed.Bytes())
	f.Add([]byte(nil))
	f.Add([]byte{1, 0, 0, 0, 0})             // v1, 0 entries
	f.Add([]byte{2, 0xff, 0xff, 0xff, 0xff}) // v2, huge entry count, no payload
	f.Fuzz(func(t *testing.T, data []byte) {
		bf := NewBloomFactory(0.01)("p").(*bloomFacet)
		_ = bf.Decode(bytes.NewReader(data)) // must simply not panic
	})
}

// TestBloomFacet_DecodeHostileCountBounded pins issue #763 on the pmeta path:
// the bloom facet payload is decoded by bloomindex.Unmarshal, and a 5-byte value
// declaring 1,000,000 entries used to preallocate ~56 MB (the bundle CRC catches
// corruption, not a hostile writer). Decode must fail without allocating in
// proportion to the declared count.
func TestBloomFacet_DecodeHostileCountBounded(t *testing.T) {
	const ceiling = 4 << 20 // assumed generous: TotalAlloc is process-wide; the crashers allocated ~56 MB
	for name, data := range map[string][]byte{
		"v1": {1, 0x40, 0x42, 0x0F, 0x00},
		"v2": {2, 0x40, 0x42, 0x0F, 0x00},
	} {
		t.Run(name, func(t *testing.T) {
			f := NewBloomFactory(0.01)("p").(*bloomFacet)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			err := f.Decode(bytes.NewReader(data))
			runtime.ReadMemStats(&after)
			if err == nil {
				t.Fatal("expected an error")
			}
			if a := after.TotalAlloc - before.TotalAlloc; a > ceiling {
				t.Fatalf("Decode allocated %d bytes for a %d-byte payload (ceiling %d)", a, len(data), ceiling)
			}
			if f.idx.Len() != 0 {
				t.Fatalf("failed Decode replaced the live index (%d entries)", f.idx.Len())
			}
		})
	}
}
