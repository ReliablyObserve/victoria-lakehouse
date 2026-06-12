package pmeta

import (
	"bytes"
	"fmt"
	"math"
	"testing"
)

// TestAddHighCardValues_SurvivesEncodeDecode locks the tap→facet durability fix:
// values fed via addHighCardValues (the always-sketch id path — e.g. span_id, which
// is deliberately NOT bloomed) must round-trip through the facet codec so the
// distinct count survives restart instead of resetting to 0 (the reported bug).
func TestAddHighCardValues_SurvivesEncodeDecode(t *testing.T) {
	dict := NewDict()
	f := NewFieldCatalogFactoryCapped(dict, 50000, []string{"span_id"})("p").(*fieldCatalogFacet)

	const n = 4000
	f.addHighCardValues("span_id", seq(n, "span-"))
	if !f.IsHighCard("span_id") {
		t.Fatal("span_id not marked high-card after addHighCardValues")
	}
	before := f.fieldHLL("span_id").estimate()
	if e := math.Abs(float64(before)-n) / n; e > 0.05 {
		t.Fatalf("estimate %d, want ~%d (err %.3f)", before, n, e)
	}

	var buf bytes.Buffer
	if err := f.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	g := NewFieldCatalogFactoryCapped(dict, 50000, []string{"span_id"})("p").(*fieldCatalogFacet)
	if err := g.Decode(&buf); err != nil {
		t.Fatal(err)
	}
	if after := g.fieldHLL("span_id").estimate(); after != before {
		t.Errorf("estimate changed across restart: before=%d after=%d", before, after)
	}
}

// TestAddPartitionCardinality_FieldCardinalityReadsFacet locks the wiring: a value
// stream fed to a partition via the Store surfaces through FieldCardinality (the
// Cardinality Explorer source), proving the tap→facet path is connected end to end.
func TestAddPartitionCardinality_FieldCardinalityReadsFacet(t *testing.T) {
	dict := NewDict()
	st := NewStore()
	st.SetDict(dict)
	st.Register(FacetFieldCatalog, NewFieldCatalogFactoryCapped(dict, 50000, []string{"span_id"}))
	// A flush creates the partition's catalog facet (the real flush→tap order).
	st.OnFileFlush(FileContribution{Partition: "p1", Labels: map[string][]string{"service.name": {"api"}}})

	const n = 3000
	st.AddPartitionCardinality("p1", "span_id", seq(n, "s-"))
	got := st.FieldCardinality("span_id")
	if e := math.Abs(float64(got)-n) / n; e > 0.05 {
		t.Fatalf("FieldCardinality(span_id)=%d, want ~%d", got, n)
	}
	// No-op safety: a partition with no catalog facet must not panic.
	st.AddPartitionCardinality("missing", "span_id", func(yield func(string) bool) { yield("x") })
}

// TestAddHighCardValues_SkipsEmpty: empty strings must not inflate the sketch.
func TestAddHighCardValues_SkipsEmpty(t *testing.T) {
	dict := NewDict()
	f := NewFieldCatalogFactoryCapped(dict, 0, []string{"span_id"})("p").(*fieldCatalogFacet)
	f.addHighCardValues("span_id", func(yield func(string) bool) {
		for i := 0; i < 100; i++ {
			if !yield("") {
				return
			}
		}
	})
	if h := f.fieldHLL("span_id"); h != nil && h.estimate() != 0 {
		t.Errorf("empty values inflated sketch to %d, want 0", h.estimate())
	}
}

// FuzzAddHighCardValues_RoundTrip: arbitrary NUL-delimited id streams must feed the
// sketch without panicking and survive encode/decode with an identical estimate
// (registers are deterministic) — guards the codec against adversarial value sets.
func FuzzAddHighCardValues_RoundTrip(f *testing.F) {
	f.Add("a\x00b\x00c")
	f.Add("")
	f.Add("dup\x00dup\x00dup")
	f.Fuzz(func(t *testing.T, blob string) {
		dict := NewDict()
		fac := NewFieldCatalogFactoryCapped(dict, 50000, []string{"id"})("p").(*fieldCatalogFacet)
		fac.addHighCardValues("id", splitNUL(blob))

		var buf bytes.Buffer
		if err := fac.Encode(&buf); err != nil {
			t.Fatalf("encode: %v", err)
		}
		g := NewFieldCatalogFactoryCapped(dict, 50000, []string{"id"})("p").(*fieldCatalogFacet)
		if err := g.Decode(&buf); err != nil {
			t.Fatalf("decode: %v", err)
		}
		fh, gh := fac.fieldHLL("id"), g.fieldHLL("id")
		if (fh == nil) != (gh == nil) {
			t.Fatalf("sketch presence mismatch after round-trip: %v vs %v", fh != nil, gh != nil)
		}
		if fh != nil && fh.estimate() != gh.estimate() {
			t.Errorf("estimate drift: before=%d after=%d", fh.estimate(), gh.estimate())
		}
	})
}

// seq yields n distinct values prefixed by p (no slice materialized).
func seq(n int, p string) func(func(string) bool) {
	return func(yield func(string) bool) {
		for i := 0; i < n; i++ {
			if !yield(fmt.Sprintf("%s%08x", p, i)) {
				return
			}
		}
	}
}

// splitNUL yields the NUL-delimited fields of s.
func splitNUL(s string) func(func(string) bool) {
	return func(yield func(string) bool) {
		start := 0
		for i := 0; i <= len(s); i++ {
			if i == len(s) || s[i] == 0 {
				if !yield(s[start:i]) {
					return
				}
				start = i + 1
			}
		}
	}
}
