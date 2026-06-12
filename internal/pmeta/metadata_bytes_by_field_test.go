package pmeta

import "testing"

// TestMetadataBytesByField_BloomPlusCatalog locks the per-field metadata
// decomposition the Storage Details table reads: for every resident bundle,
// MetadataBytesByField sums each field's bloom bitset bytes (FacetBloom) plus its
// catalog-entry / HLL bytes (FacetFieldCatalog), keyed by field name and
// accumulated across partitions. Asserts per-field totals are > 0 and that a
// field's reported total equals bloom + catalog summed independently.
func TestMetadataBytesByField_BloomPlusCatalog(t *testing.T) {
	s := NewStore()
	dict := NewDict()
	s.Register(FacetBloom, NewBloomFactory(0.01))
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(dict))

	// Two partitions, each with a bloomed + cataloged low-card field (service.name)
	// and a high-card field (trace_id) fed via HighCardValues so the catalog keeps a
	// persisted per-field HLL — exercising both BytesByField branches.
	s.OnFileFlush(FileContribution{
		Partition:      "100/1/dt=2026-06-10",
		FileKey:        "100/1/dt=2026-06-10/f1.parquet",
		Labels:         map[string][]string{"service.name": {"api-gateway", "order-service"}},
		BloomValues:    map[string][]string{"service.name": {"api-gateway", "order-service"}, "trace_id": {"t1", "t2", "t3"}},
		HighCardValues: map[string][]string{"trace_id": {"t1", "t2", "t3"}},
	})
	s.OnFileFlush(FileContribution{
		Partition:      "100/1/dt=2026-06-11",
		FileKey:        "100/1/dt=2026-06-11/f2.parquet",
		Labels:         map[string][]string{"service.name": {"order-service", "user-service"}},
		BloomValues:    map[string][]string{"service.name": {"order-service", "user-service"}, "trace_id": {"t4", "t5"}},
		HighCardValues: map[string][]string{"trace_id": {"t4", "t5"}},
	})

	got := s.MetadataBytesByField()

	// service.name: cataloged (low-card value set) + bloomed → must be > 0.
	if got["service.name"] <= 0 {
		t.Fatalf("service.name metadata = %d, want > 0", got["service.name"])
	}
	// trace_id: bloomed + a persisted catalog HLL → must be > 0.
	if got["trace_id"] <= 0 {
		t.Fatalf("trace_id metadata = %d, want > 0", got["trace_id"])
	}

	// Independently sum each facet's per-field contribution across both bundles and
	// assert the store's total equals bloom + catalog for each field.
	wantBloom := map[string]int64{}
	wantCatalog := map[string]int64{}
	for _, p := range s.Partitions() {
		if fc, ok := s.Get(p, FacetBloom); ok {
			for f, n := range fc.(*bloomFacet).BytesByField() {
				wantBloom[f] += n
			}
		}
		if fc, ok := s.Get(p, FacetFieldCatalog); ok {
			for f, n := range fc.(*fieldCatalogFacet).BytesByField() {
				wantCatalog[f] += n
			}
		}
	}
	for _, field := range []string{"service.name", "trace_id"} {
		want := wantBloom[field] + wantCatalog[field]
		if got[field] != want {
			t.Errorf("%s total = %d, want bloom(%d)+catalog(%d) = %d",
				field, got[field], wantBloom[field], wantCatalog[field], want)
		}
		if wantBloom[field] == 0 {
			t.Errorf("%s bloom contribution = 0, want > 0 (field was bloomed)", field)
		}
		if wantCatalog[field] == 0 {
			t.Errorf("%s catalog contribution = 0, want > 0 (field was cataloged/sketched)", field)
		}
	}
}

// TestMetadataBytesByField_EmptyStore: no facets resident → empty (non-nil) map.
func TestMetadataBytesByField_EmptyStore(t *testing.T) {
	if got := NewStore().MetadataBytesByField(); got == nil || len(got) != 0 {
		t.Errorf("empty store MetadataBytesByField = %v, want empty non-nil map", got)
	}
}
