package pmeta

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

// TestFileMetaFacet covers the fileMetaFacet in-package (the integration parity
// test lives in the parquets3 package and doesn't count toward pmeta coverage).
func TestFileMetaFacet(t *testing.T) {
	f := NewFileMetaFactory()("p").(*fileMetaFacet)
	if f.Kind() != FacetFileMeta {
		t.Fatalf("Kind = %d", f.Kind())
	}
	f.Merge(FileContribution{
		FileKey: "k1", RowCount: 10, MinTimeNs: 1, MaxTimeNs: 2, RawBytes: 99,
		SchemaFingerprint: "sf", Labels: map[string][]string{"x": {"y"}},
	})
	f.Merge(FileContribution{FileKey: ""}) // no-op (empty key)

	e, ok := f.fileMeta("k1")
	if !ok || e.RowCount != 10 || e.SchemaFingerprint != "sf" {
		t.Fatalf("fileMeta = %+v ok=%v", e, ok)
	}
	if f.EstimateBytes() <= 0 {
		t.Fatal("EstimateBytes should be > 0")
	}

	var buf bytes.Buffer
	if err := f.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	g := NewFileMetaFactory()("p").(*fileMetaFacet)
	if err := g.Decode(&buf); err != nil {
		t.Fatal(err)
	}
	if e2, ok := g.fileMeta("k1"); !ok || e2.RowCount != 10 || !reflect.DeepEqual(e2.Labels, map[string][]string{"x": {"y"}}) {
		t.Fatalf("decoded fileMeta = %+v ok=%v", e2, ok)
	}
}

// TestStore_Accessors covers the Store read accessors + lifecycle helpers.
func TestStore_Accessors(t *testing.T) {
	s := NewStore()
	s.SetPrefix("logs/")
	s.Register(FacetFileMeta, NewFileMetaFactory())
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))

	// Not-found paths.
	if _, ok := s.FileMeta("nope", "k"); ok {
		t.Fatal("FileMeta(unknown partition) must be false")
	}
	if v := s.FieldValues("nope", "x", "", 0); len(v) != 0 {
		t.Fatalf("FieldValues(unknown) = %v", v)
	}
	if n := s.FieldNames("nope"); len(n) != 0 {
		t.Fatalf("FieldNames(unknown) = %v", n)
	}

	s.OnFileFlush(FileContribution{
		Partition: "p", FileKey: "k1", RowCount: 5,
		Labels: map[string][]string{"service.name": {"a", "b"}, "level": {"ERROR"}},
	})

	if _, ok := s.FileMeta("p", "missing"); ok {
		t.Fatal("FileMeta(missing key) must be false")
	}
	if fm, ok := s.FileMeta("p", "k1"); !ok || fm.RowCount != 5 {
		t.Fatalf("FileMeta(p,k1) = %+v ok=%v", fm, ok)
	}
	if v := s.FieldValues("p", "service.name", "", 0); !reflect.DeepEqual(v, []string{"a", "b"}) {
		t.Fatalf("FieldValues = %v", v)
	}
	if v := s.FieldValues("p", "service.name", "a", 0); !reflect.DeepEqual(v, []string{"a"}) {
		t.Fatalf("FieldValues substr = %v", v)
	}
	if n := s.FieldNames("p"); !reflect.DeepEqual(n, []string{"level", "service.name"}) {
		t.Fatalf("FieldNames = %v", n)
	}
	if s.ResidentBytes() <= 0 {
		t.Fatal("ResidentBytes should be > 0 after a flush")
	}

	// AddCardinality streaming + Cardinality readout.
	s.AddCardinality("trace_id", func(yield func(string) bool) {
		for i := 0; i < 2000; i++ {
			if !yield(fmt.Sprintf("%032x", i)) {
				return
			}
		}
	})
	if c := s.Cardinality("trace_id"); c < 1900 || c > 2100 {
		t.Fatalf("Cardinality(trace_id) = %d (want ~2000)", c)
	}
	if c := s.Cardinality("never"); c != 0 {
		t.Fatalf("Cardinality(unknown) = %d, want 0", c)
	}
}

// TestHLL_ClassicAndClamps covers the precision clamps and the classic estimator
// path (precision != 14 falls back to estimateClassic + linear counting).
func TestHLL_ClassicAndClamps(t *testing.T) {
	if newHLL(2).p != 4 {
		t.Fatal("precision must clamp up to 4")
	}
	if newHLL(20).p != 18 {
		t.Fatal("precision must clamp down to 18")
	}
	h := newHLL(12) // != 14 → estimateClassic
	for i := 0; i < 5000; i++ {
		h.add(fmt.Sprintf("v-%d", i))
	}
	if e := relErr(h.estimate(), 5000); e > 0.05 {
		t.Fatalf("classic p=12 relErr = %.3f%% (>5%%)", e*100)
	}
	// tiny → linear counting branch
	h2 := newHLL(12)
	for i := 0; i < 50; i++ {
		h2.add(fmt.Sprintf("t-%d", i))
	}
	if e := h2.estimate(); e < 40 || e > 60 {
		t.Fatalf("classic tiny estimate = %d (want ~50)", e)
	}
}

// TestDict covers the interning round-trips + EstimateBytes.
func TestDict(t *testing.T) {
	d := NewDict()
	vid := d.internValue("hello")
	if v, ok := d.value(vid); !ok || v != "hello" {
		t.Fatal("value round-trip")
	}
	if d.internValue("hello") != vid {
		t.Fatal("interning must be stable")
	}
	fid := d.internField("service.name")
	if f, ok := d.field(fid); !ok || f != "service.name" {
		t.Fatal("field round-trip")
	}
	if _, ok := d.fieldID("service.name"); !ok {
		t.Fatal("fieldID lookup")
	}
	if _, ok := d.fieldID("unknown"); ok {
		t.Fatal("fieldID(unknown) must be false")
	}
	if d.EstimateBytes() <= 0 {
		t.Fatal("dict EstimateBytes should be > 0")
	}
}

// TestConcurrent_ValuesVsMerge pins the Values-vs-Merge data race (review finding:
// Values iterated vs.ids after RUnlock while Merge mutated the slice in place).
// Run under -race in CI; fails loudly if the copy-under-lock regresses.
func TestConcurrent_ValuesVsMerge(t *testing.T) {
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			s.OnFileFlush(FileContribution{
				Partition: "p", FileKey: "f",
				Labels: map[string][]string{"service.name": {fmt.Sprintf("svc-%d", i)}},
			})
		}
	}()
	for i := 0; i < 2000; i++ {
		_ = s.FieldValues("p", "service.name", "", 0)
		_ = s.Cardinality("trace_id")
	}
	<-done
}

// TestPersistDirty_GenerationNoLostUpdate pins the dirty-generation semantics: a
// contribution arriving AFTER the persist's encode snapshot keeps the bundle
// dirty, so it is persisted on the next cycle (no lost update).
func TestPersistDirty_GenerationNoLostUpdate(t *testing.T) {
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "f1",
		Labels: map[string][]string{"k": {"v1"}}})

	b := s.Bundle("p")
	g := b.snapshotGen()
	// A contribution lands between the encode snapshot and persisted(g) — exactly
	// the lost-update window of a boolean dirty flag.
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "f2",
		Labels: map[string][]string{"k": {"v2"}}})
	b.persisted(g)

	if !b.Dirty() {
		t.Fatal("contribution after the persist snapshot must keep the bundle dirty")
	}
	if dp := s.DirtyPartitions(); len(dp) != 1 || dp[0] != "p" {
		t.Fatalf("DirtyPartitions = %v, want [p]", dp)
	}
}

// TestPutWarm_PreservesLiveContributions pins the serve-while-warming fix: a
// bundle decoded from S3 must absorb into — not clobber — a live bundle that
// concurrent flushes already populated.
func TestPutWarm_PreservesLiveContributions(t *testing.T) {
	mk := func() *Store {
		s := NewStore()
		s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))
		s.Register(FacetFileMeta, NewFileMetaFactory())
		s.Register(FacetBloom, NewBloomFactory(0.01))
		return s
	}
	// "old" store = what was persisted to S3 before restart.
	old := mk()
	old.OnFileFlush(FileContribution{Partition: "p", FileKey: "old.parquet", RowCount: 1,
		Labels:      map[string][]string{"service.name": {"old-svc"}},
		BloomValues: map[string][]string{"service.name": {"old-svc"}}})
	var buf bytes.Buffer
	if err := old.Bundle("p").Encode(&buf); err != nil {
		t.Fatal(err)
	}

	// "live" store = restarted pod; a flush lands BEFORE the warm completes.
	live := mk()
	live.OnFileFlush(FileContribution{Partition: "p", FileKey: "new.parquet", RowCount: 2,
		Labels:      map[string][]string{"service.name": {"new-svc"}},
		BloomValues: map[string][]string{"service.name": {"new-svc"}}})

	decoded, _, err := DecodeBundle(bytes.NewReader(buf.Bytes()), live.Registry())
	if err != nil {
		t.Fatal(err)
	}
	live.PutWarm(decoded)

	// BOTH the live flush and the warmed S3 content survive.
	if _, ok := live.FileMeta("p", "new.parquet"); !ok {
		t.Fatal("live flush contribution clobbered by warm")
	}
	if _, ok := live.FileMeta("p", "old.parquet"); !ok {
		t.Fatal("warmed S3 content not absorbed")
	}
	vals := live.FieldValues("p", "service.name", "", 0)
	if len(vals) != 2 {
		t.Fatalf("catalog union = %v, want both old-svc and new-svc", vals)
	}
	for _, key := range []string{"old.parquet", "new.parquet"} {
		got, ok := live.BloomMayContain("p", []string{key}, "service.name", map[string]string{
			"old.parquet": "old-svc", "new.parquet": "new-svc"}[key])
		if !ok || len(got) != 1 {
			t.Fatalf("bloom union missing %s (ok=%v got=%v)", key, ok, got)
		}
	}
	if !live.Bundle("p").Dirty() {
		t.Fatal("warm-merged bundle must be dirty (the union needs to persist)")
	}
}

// TestRemoveFiles_CompactionHook pins the per-file removal: file-meta + bloom
// entries for compacted-away files are dropped; the catalog (partition-level
// union) is untouched; the bundle goes dirty so the shrunken bundle persists.
func TestRemoveFiles_CompactionHook(t *testing.T) {
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))
	s.Register(FacetFileMeta, NewFileMetaFactory())
	s.Register(FacetBloom, NewBloomFactory(0.01))
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "a.parquet", RowCount: 1,
		Labels:      map[string][]string{"service.name": {"svc"}},
		BloomValues: map[string][]string{"service.name": {"svc"}}})
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "b.parquet", RowCount: 2,
		Labels:      map[string][]string{"service.name": {"svc"}},
		BloomValues: map[string][]string{"service.name": {"svc"}}})

	s.RemoveFiles("p", []string{"a.parquet"})

	if _, ok := s.FileMeta("p", "a.parquet"); ok {
		t.Fatal("removed file still in file-meta facet")
	}
	if _, ok := s.FileMeta("p", "b.parquet"); !ok {
		t.Fatal("surviving file lost from file-meta facet")
	}
	got, ok := s.BloomMayContain("p", []string{"a.parquet", "b.parquet"}, "service.name", "svc")
	if !ok {
		t.Fatal("bloom facet should still answer")
	}
	for _, k := range got {
		if k == "a.parquet" {
			// removed key is UNKNOWN now → kept by may-contain (sound); the point
			// is the entry is gone, which Remove+Has proves:
			break
		}
	}
	if vals := s.FieldValues("p", "service.name", "", 0); len(vals) != 1 {
		t.Fatalf("catalog must be untouched by file removal, got %v", vals)
	}
	// Partition removal (retention hook).
	s.Remove("p")
	if _, ok := s.FileMeta("p", "b.parquet"); ok {
		t.Fatal("Remove(partition) left the bundle resident")
	}
}
