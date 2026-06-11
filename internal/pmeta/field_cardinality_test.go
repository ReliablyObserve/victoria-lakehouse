package pmeta

import (
	"fmt"
	"math"
	"testing"
)

// TestFieldCardinality_UnionsLowCardAcrossPartitions locks the Cardinality
// Explorer's count source: FieldCardinality reads the PERSISTED catalog facets
// (no side map) and unions a low-card field's enumerated values across
// partitions — so a value present in two partitions is counted ONCE. This is
// what makes k8s.cluster.name report its true distinct count instead of 0 (the
// in-memory hllByField only ever held high-card fields).
func TestFieldCardinality_UnionsLowCardAcrossPartitions(t *testing.T) {
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))

	s.OnFileFlush(FileContribution{Partition: "p1", FileKey: "f1",
		Labels: map[string][]string{"k8s.cluster.name": {"prod-us-east-1", "prod-eu-west-1"}}})
	s.OnFileFlush(FileContribution{Partition: "p2", FileKey: "f2",
		Labels: map[string][]string{"k8s.cluster.name": {"prod-eu-west-1", "prod-ap-southeast-1"}}})

	if got := s.FieldCardinality("k8s.cluster.name"); got != 3 {
		t.Errorf("FieldCardinality(k8s.cluster.name) = %d, want 3 (union of {us,eu} and {eu,ap} — eu counted once)", got)
	}
	if got := s.FieldCardinality("absent.field"); got != 0 {
		t.Errorf("FieldCardinality(absent) = %d, want 0", got)
	}
}

// TestFieldCardinality_HighCardUsesHLL verifies a high-card field (sketched into
// the merged HLL, not enumerable from the catalog) returns its HLL estimate.
func TestFieldCardinality_HighCardUsesHLL(t *testing.T) {
	s := NewStore()
	const n = 5000
	s.AddCardinality("trace_id", func(yield func(string) bool) {
		for i := 0; i < n; i++ {
			if !yield(fmt.Sprintf("%032x", i)) {
				return
			}
		}
	})
	got := s.FieldCardinality("trace_id")
	if e := math.Abs(float64(got)-float64(n)) / float64(n); e > 0.05 {
		t.Errorf("FieldCardinality(trace_id) = %d, want ≈%d (HLL, ≤5%% err)", got, n)
	}
}
