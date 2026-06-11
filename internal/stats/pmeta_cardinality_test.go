package stats

import "testing"

// TestPmetaCardinalityOf_PrefixFallback locks the lookup the Cardinality Explorer
// uses to read accurate pmeta cardinality: exact field name first, then the
// suffix after ":" so a traces "resource_attr:k8s.cluster.name" entry matches the
// bare "k8s.cluster.name" the pmeta catalog is keyed by.
func TestPmetaCardinalityOf_PrefixFallback(t *testing.T) {
	card := map[string]uint64{"k8s.cluster.name": 3, "service.name": 5}
	fn := func(f string) uint64 { return card[f] }

	if got := pmetaCardinalityOf(fn, "k8s.cluster.name"); got != 3 {
		t.Errorf("bare lookup = %d, want 3", got)
	}
	if got := pmetaCardinalityOf(fn, "resource_attr:k8s.cluster.name"); got != 3 {
		t.Errorf("prefixed lookup = %d, want 3 (suffix fallback)", got)
	}
	if got := pmetaCardinalityOf(fn, "span_attr:unknown.field"); got != 0 {
		t.Errorf("unknown = %d, want 0", got)
	}
}
