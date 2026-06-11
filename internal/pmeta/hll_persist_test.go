package pmeta

import (
	"bytes"
	"fmt"
	"math"
	"testing"
)

// TestFieldCatalog_HLLSurvivesEncodeDecode is the durability guard: a high-card
// field's distinct-count sketch must round-trip through the facet codec so the
// Cardinality Explorer's high-card counts survive a restart (loaded from the
// bundle), not reset to 0. Pre-HLL bundles (no section) decode fine (EOF-tolerant).
func TestFieldCatalog_HLLSurvivesEncodeDecode(t *testing.T) {
	dict := NewDict()
	f := NewFieldCatalogFactory(dict)("p").(*fieldCatalogFacet)

	const n = 3000
	vals := make([]string, n)
	for i := range vals {
		vals[i] = fmt.Sprintf("id-%06x", i)
	}
	f.Merge(FileContribution{HighCardValues: map[string][]string{"trace_id": vals}})

	if h := f.fieldHLL("trace_id"); h == nil {
		t.Fatal("high-card field has no sketch after Merge")
	}
	before := f.fieldHLL("trace_id").estimate()
	if e := math.Abs(float64(before)-n) / n; e > 0.05 {
		t.Fatalf("pre-encode estimate %d, want ~%d", before, n)
	}

	var buf bytes.Buffer
	if err := f.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	g := NewFieldCatalogFactory(dict)("p").(*fieldCatalogFacet)
	if err := g.Decode(&buf); err != nil {
		t.Fatal(err)
	}
	if h := g.fieldHLL("trace_id"); h == nil {
		t.Fatal("sketch did not survive Decode")
	}
	if after := g.fieldHLL("trace_id").estimate(); after != before {
		t.Errorf("estimate changed across encode/decode: before=%d after=%d", before, after)
	}
	if !g.IsHighCard("trace_id") {
		t.Error("high-card flag lost across encode/decode")
	}
}
