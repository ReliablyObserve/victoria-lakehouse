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
