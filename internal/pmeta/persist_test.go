package pmeta

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// memOS is an in-memory ObjectStore for tests (returns copies, ErrNotFound on miss).
type memOS struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemOS() *memOS { return &memOS{m: map[string][]byte{}} }

func (o *memOS) GetObject(_ context.Context, k string) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.m[k]
	if !ok {
		return nil, ErrNotFound
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

func (o *memOS) PutObject(_ context.Context, k string, d []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	cp := make([]byte, len(d))
	copy(cp, d)
	o.m[k] = cp
	return nil
}

func catalogStore() *Store {
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))
	return s
}

func samplePartitions() map[string][]FileContribution {
	return map[string][]FileContribution{
		"logs/dt=2026-06-09/hour=10": {
			{Labels: map[string][]string{"service.name": {"api-gateway", "order-service"}, "level": {"ERROR", "INFO"}}},
		},
		"logs/dt=2026-06-09/hour=11": {
			{Labels: map[string][]string{"service.name": {"user-service"}, "level": {"WARN"}}},
		},
		"logs/dt=2026-06-09/hour=12": {
			{Labels: map[string][]string{"service.name": {"payment-service", "api-gateway"}}},
		},
	}
}

func loadPartitions(s *Store, parts map[string][]FileContribution) {
	for p, cs := range parts {
		for _, c := range cs {
			c.Partition = p
			s.OnFileFlush(c)
		}
	}
}

func partitionParity(t *testing.T, label string, src, dst *Store, parts map[string][]FileContribution) {
	t.Helper()
	for p := range parts {
		sa, ok := src.Get(p, FacetFieldCatalog)
		if !ok {
			t.Fatalf("%s: src missing catalog for %s", label, p)
		}
		da, ok := dst.Get(p, FacetFieldCatalog)
		if !ok {
			t.Fatalf("%s: dst missing catalog for %s", label, p)
		}
		sf, df := sa.(*fieldCatalogFacet), da.(*fieldCatalogFacet)
		if !equal(sf.Fields(), df.Fields()) {
			t.Fatalf("%s: %s fields differ: %v vs %v", label, p, sf.Fields(), df.Fields())
		}
		for _, fld := range sf.Fields() {
			if a, b := sf.Values(fld, "", 0), df.Values(fld, "", 0); !equal(a, b) {
				t.Fatalf("%s: %s/%s values differ: %v vs %v", label, p, fld, a, b)
			}
		}
	}
}

// TestStore_PersistedBytes is the regression guard for the incremental on-S3
// metadata footprint (Storage Overview "Metadata on S3" tile). It must equal the
// sum of the bundle objects' sizes WITHOUT ever listing S3: bundles record their
// encoded size on persist + on warm-load, and a re-persist (the compaction path)
// updates it.
func TestStore_PersistedBytes(t *testing.T) {
	ctx := context.Background()
	parts := samplePartitions()

	src := catalogStore()
	loadPartitions(src, parts)
	if got := src.PersistedBytes(); got != 0 {
		t.Errorf("PersistedBytes before any persist = %d, want 0", got)
	}

	os := newMemOS()
	if _, err := src.PersistDirty(ctx, os); err != nil {
		t.Fatal(err)
	}

	// Sum the bytes actually written to the object store — PersistedBytes must
	// match it exactly, derived incrementally rather than by a LIST.
	os.mu.Lock()
	var want int64
	for _, v := range os.m {
		want += int64(len(v))
	}
	os.mu.Unlock()
	if want == 0 {
		t.Fatal("no bundle objects written")
	}
	if got := src.PersistedBytes(); got != want {
		t.Errorf("PersistedBytes after persist = %d, want %d (sum of on-S3 bundle bytes)", got, want)
	}

	// Cold-load into a FRESH store: the warm path records the loaded sizes, so a
	// restarted pod reports the footprint without re-persisting.
	dst := catalogStore()
	keys := make([]string, 0, len(parts))
	for p := range parts {
		keys = append(keys, p)
	}
	dst.WarmPartitions(ctx, os, keys, 4)
	if got := dst.PersistedBytes(); got != want {
		t.Errorf("PersistedBytes after warm-load = %d, want %d", got, want)
	}

	// Re-persist after a mutation (compaction path) updates the recorded size.
	before := src.PersistedBytes()
	loadPartitions(src, map[string][]FileContribution{
		"logs/dt=2026-06-09/hour=10": {{Labels: map[string][]string{"service.name": {"svc-x", "svc-y", "svc-z", "svc-w"}}}},
	})
	if _, err := src.PersistDirty(ctx, os); err != nil {
		t.Fatal(err)
	}
	if after := src.PersistedBytes(); after <= before {
		t.Errorf("PersistedBytes after adding values = %d, want > %d (re-persist must update the size)", after, before)
	}
}

func TestPersistWarm_RoundTripParity(t *testing.T) {
	ctx := context.Background()
	parts := samplePartitions()

	src := catalogStore()
	loadPartitions(src, parts)

	os := newMemOS()
	n, err := src.PersistDirty(ctx, os)
	if err != nil || n != len(parts) {
		t.Fatalf("PersistDirty: n=%d err=%v (want %d)", n, err, len(parts))
	}
	if dp := src.DirtyPartitions(); len(dp) != 0 {
		t.Fatalf("dirty not cleared after persist: %v", dp)
	}

	// Cold-load into a FRESH store (fresh dict) — simulates a restarted pod.
	dst := catalogStore()
	keys := make([]string, 0, len(parts))
	for p := range parts {
		keys = append(keys, p)
	}
	wr := dst.WarmPartitions(ctx, os, keys, 4)
	if wr.Loaded != len(parts) || len(wr.NeedsRebuild) != 0 || len(wr.SkippedFacets) != 0 {
		t.Fatalf("warm: loaded=%d rebuild=%v skipped=%v", wr.Loaded, wr.NeedsRebuild, wr.SkippedFacets)
	}
	partitionParity(t, "persist→warm", src, dst, parts)
}

func TestWarm_MissingBundle_NeedsRebuild(t *testing.T) {
	ctx := context.Background()
	dst := catalogStore()
	wr := dst.WarmPartitions(ctx, newMemOS(), []string{"logs/dt=2026-06-09/hour=10"}, 2)
	if wr.Loaded != 0 || len(wr.NeedsRebuild) != 1 {
		t.Fatalf("missing bundle: loaded=%d rebuild=%v", wr.Loaded, wr.NeedsRebuild)
	}
}

func TestWarm_CorruptBundle_NeedsRebuild(t *testing.T) {
	ctx := context.Background()
	os := newMemOS()
	dst := catalogStore()
	_ = os.PutObject(ctx, dst.bundleKey("p"), []byte("this is not a pmeta bundle"))
	wr := dst.WarmPartitions(ctx, os, []string{"p"}, 2)
	if len(wr.NeedsRebuild) != 1 || wr.NeedsRebuild[0] != "p" {
		t.Fatalf("corrupt bundle must need rebuild, got %v", wr.NeedsRebuild)
	}
}

// TestWarm_SkippedFacet_ThenRebuildParity: a bundle whose facet payload is
// corrupt loads (TOC ok) but reports the facet skipped; rebuilding from files
// restores parity.
func TestWarm_SkippedFacet_ThenRebuildParity(t *testing.T) {
	ctx := context.Background()
	parts := map[string][]FileContribution{
		"p": {{Labels: map[string][]string{"service.name": {"api-gateway", "order-service"}}}},
	}
	src := catalogStore()
	loadPartitions(src, parts)
	os := newMemOS()
	if _, err := src.PersistDirty(ctx, os); err != nil {
		t.Fatal(err)
	}
	// Corrupt the stored bundle's last byte (the catalog payload).
	raw := os.m[src.bundleKey("p")]
	raw[len(raw)-1] ^= 0xFF

	dst := catalogStore()
	wr := dst.WarmPartitions(ctx, os, []string{"p"}, 1)
	if len(wr.SkippedFacets["p"]) != 1 || wr.SkippedFacets["p"][0] != FacetFieldCatalog {
		t.Fatalf("expected catalog facet skipped, got %v", wr.SkippedFacets)
	}

	// Self-heal: replay the partition's files → parity restored.
	dst.Rebuild("p", parts["p"])
	partitionParity(t, "skip→rebuild", src, dst, parts)
}

func TestPersistDirty_Idempotent(t *testing.T) {
	ctx := context.Background()
	src := catalogStore()
	loadPartitions(src, samplePartitions())
	os := newMemOS()
	if _, err := src.PersistDirty(ctx, os); err != nil {
		t.Fatal(err)
	}
	// Nothing dirty now → a second persist writes 0.
	n, err := src.PersistDirty(ctx, os)
	if err != nil || n != 0 {
		t.Fatalf("second PersistDirty: n=%d err=%v (want 0)", n, err)
	}
}

// TestHLL_NotPersistedDocumented pins the CURRENT behavior as the documented
// limitation: the per-field HLL sketches live on the Store (one merged sketch
// per field, globally), NOT in any facet payload, so they do not survive
// PersistDirty → restart → WarmPartitions. A fresh store reports Cardinality 0
// until live flushes re-feed HighCardValues. If this test starts failing because
// cardinality round-trips, the sketches became persisted — update this test and
// docs/architecture/field-value-catalog.md together.
func TestHLL_NotPersistedDocumented(t *testing.T) {
	ctx := context.Background()
	src := catalogStore()
	traces := make([]string, 5000)
	for i := range traces {
		traces[i] = fmt.Sprintf("trace-%d", i)
	}
	src.OnFileFlush(FileContribution{
		Partition:      "p",
		FileKey:        "f1",
		Labels:         map[string][]string{"service.name": {"api-gateway"}},
		HighCardValues: map[string][]string{"trace_id": traces},
	})
	if src.Cardinality("trace_id") == 0 {
		t.Fatal("precondition: live store must have a trace_id sketch")
	}

	os := newMemOS()
	if _, err := src.PersistDirty(ctx, os); err != nil {
		t.Fatal(err)
	}

	// Fresh store + warm = restarted pod. The bundle (catalog facet) loads…
	dst := catalogStore()
	wr := dst.WarmPartitions(ctx, os, []string{"p"}, 1)
	if wr.Loaded != 1 || len(wr.NeedsRebuild) != 0 || len(wr.SkippedFacets) != 0 {
		t.Fatalf("warm: loaded=%d rebuild=%v skipped=%v", wr.Loaded, wr.NeedsRebuild, wr.SkippedFacets)
	}
	if vals := dst.FieldValues("p", "service.name", "", 0); !equal(vals, []string{"api-gateway"}) {
		t.Fatalf("catalog must round-trip, got %v", vals)
	}
	// …but the sketch does not: it is in-RAM only (the documented limitation).
	if c := dst.Cardinality("trace_id"); c != 0 {
		t.Fatalf("Cardinality after warm = %d — the HLL sketch is documented as NOT persisted; if it now round-trips, update this test + the catalog docs", c)
	}
}
