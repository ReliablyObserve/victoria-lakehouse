package pmeta

import (
	"fmt"
	"reflect"
	"testing"
)

// TestRebuildFieldCatalogValues_SubtractsWhatOnlyRemovedRowsCarried is the
// reason the rebuild exists. Merge only adds, so after a delete removes the
// only rows carrying a value, the union still lists it. Replaying the surviving
// files' labels is the subtraction.
func TestRebuildFieldCatalogValues_SubtractsWhatOnlyRemovedRowsCarried(t *testing.T) {
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))

	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "a",
		Labels: map[string][]string{"service.name": {"api-gateway", "order-service"}}})
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "b",
		Labels: map[string][]string{"service.name": {"api-gateway"}}})

	if got := s.FieldValues("p", "service.name", "", 0); !reflect.DeepEqual(got, []string{"api-gateway", "order-service"}) {
		t.Fatalf("fixture: catalog before = %v", got)
	}

	// File "a" was rewritten to drop its order-service rows; the partition now
	// holds the replacement "a2" and the untouched "b".
	if !s.RebuildFieldCatalogValues("p", []FileContribution{
		{FileKey: "a2", Labels: map[string][]string{"service.name": {"api-gateway"}}},
		{FileKey: "b", Labels: map[string][]string{"service.name": {"api-gateway"}}},
	}) {
		t.Fatal("RebuildFieldCatalogValues must report that it rebuilt an existing catalog")
	}
	if got := s.FieldValues("p", "service.name", "", 0); !reflect.DeepEqual(got, []string{"api-gateway"}) {
		t.Fatalf("catalog after the rebuild = %v, want only the surviving value", got)
	}
	if !s.Bundle("p").Dirty() {
		t.Error("a rebuilt catalog must be marked dirty so the corrected bundle persists")
	}
}

// TestRebuildFieldCatalogValues_KeepsHighCardStateAndSketches pins what the
// rebuild must NOT touch. HLL sketches are fed by the row tap, not by labels, so
// replaying labels cannot recreate them; a rebuild that reset them would zero
// the cardinality readout for trace_id/span_id. High-card is sticky: a field
// that crossed the cap must not become enumerable again (a truncated list would
// be served as authoritative).
func TestRebuildFieldCatalogValues_KeepsHighCardStateAndSketches(t *testing.T) {
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactoryCapped(NewDict(), 3, nil))

	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "a",
		Labels: map[string][]string{"env": {"prod"}, "pod": {"p1", "p2", "p3", "p4"}}})
	const n = 2000
	s.AddPartitionCardinality("p", "trace_id", func(yield func(string) bool) {
		for i := 0; i < n; i++ {
			if !yield(fmt.Sprintf("%032x", i)) {
				return
			}
		}
	})
	before := s.FieldCardinality("trace_id")
	if before == 0 {
		t.Fatal("fixture: the trace_id sketch should be populated")
	}

	s.RebuildFieldCatalogValues("p", []FileContribution{
		{FileKey: "a2", Labels: map[string][]string{"env": {"prod"}, "pod": {"p1"}}},
	})

	if got := s.FieldCardinality("trace_id"); got != before {
		t.Errorf("the row-tap sketch changed across the rebuild: %d -> %d", before, got)
	}
	c, _ := s.catalog("p")
	if !c.IsHighCard("pod") {
		t.Error("a field that crossed the cap must stay high-card after a rebuild")
	}
	if got := s.FieldValues("p", "pod", "", 0); got != nil {
		t.Errorf("a high-card field must not be enumerable, got %v", got)
	}
	if got := s.FieldValues("p", "env", "", 0); !reflect.DeepEqual(got, []string{"prod"}) {
		t.Errorf("low-card values after the rebuild = %v", got)
	}
}

func TestRebuildFieldCatalogValues_AppliesTheSameCapsAsMerge(t *testing.T) {
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactoryCapped(NewDict(), 2, []string{"span_id"}))
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "a",
		Labels: map[string][]string{"env": {"prod"}}})

	s.RebuildFieldCatalogValues("p", []FileContribution{{
		FileKey:         "a2",
		Labels:          map[string][]string{"env": {"prod", "dev", "qa"}, "span_id": {"s1"}, "region": {"eu"}},
		TruncatedFields: []string{"region"},
	}})

	c, _ := s.catalog("p")
	for _, field := range []string{"env", "span_id", "region"} {
		if !c.IsHighCard(field) {
			t.Errorf("%s: the rebuild must apply the cap / always-sketch / truncation rules Merge applies", field)
		}
	}
}

func TestRebuildFieldCatalogValues_NoCatalogToRebuild(t *testing.T) {
	s := NewStore()
	if s.RebuildFieldCatalogValues("absent", nil) {
		t.Error("a partition with no bundle has nothing to rebuild")
	}
	s.Register(FacetFileMeta, NewFileMetaFactory())
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "a"})
	if s.RebuildFieldCatalogValues("p", nil) {
		t.Error("a bundle with no catalog facet has nothing to rebuild")
	}
}
