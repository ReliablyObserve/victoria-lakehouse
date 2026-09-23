package pmeta

import (
	"reflect"
	"testing"
)

// TestFieldValuesExact_DistinguishesAbsentFromHighCard: a caller unioning
// partitions must be able to tell "this partition has no value for the field"
// (safe to union) from "this partition cannot enumerate the field" (the union
// would be partial).
func TestFieldValuesExact_DistinguishesAbsentFromHighCard(t *testing.T) {
	d := NewDict()
	s := NewStore()
	s.SetDict(d)
	s.Register(FacetFieldCatalog, NewFieldCatalogFactoryCapped(d, 2, []string{"trace_id"}))
	s.Register(FacetFileMeta, NewFileMetaFactory())

	s.OnFileFlush(FileContribution{Partition: "low", FileKey: "low/a", RowCount: 2,
		Labels: map[string][]string{"level": {"INFO", "ERROR"}}})
	s.OnFileFlush(FileContribution{Partition: "high", FileKey: "high/a", RowCount: 3,
		Labels: map[string][]string{"level": {"DEBUG", "WARN", "INFO"}}})
	s.OnFileFlush(FileContribution{Partition: "trunc", FileKey: "trunc/a", RowCount: 1,
		Labels: map[string][]string{"level": {"INFO"}}, TruncatedFields: []string{"level"}})
	s.OnFileFlush(FileContribution{Partition: "sketch", FileKey: "sketch/a", RowCount: 1,
		Labels: map[string][]string{"trace_id": {"t1"}}})

	cases := []struct {
		name, partition, field string
		want                   []string
		wantOK                 bool
	}{
		{"low-card", "low", "level", []string{"ERROR", "INFO"}, true},
		{"field absent in a catalogued partition", "low", "service.name", nil, true},
		{"field never interned anywhere", "low", "no.such.field", nil, true},
		{"crossed the threshold", "high", "level", nil, false},
		{"truncated extractor list", "trunc", "level", nil, false},
		{"always-sketch field", "sketch", "trace_id", nil, false},
		{"partition without a bundle", "missing", "level", nil, false},
	}
	for _, tc := range cases {
		got, ok := s.FieldValuesExact(tc.partition, tc.field, "", 0)
		if ok != tc.wantOK || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: FieldValuesExact(%q, %q) = %v, %v; want %v, %v", tc.name, tc.partition, tc.field, got, ok, tc.want, tc.wantOK)
		}
	}
	if got, ok := s.FieldValuesExact("low", "level", "", 1); !ok || !reflect.DeepEqual(got, []string{"ERROR"}) {
		t.Errorf("limit 1 = %v, %v; want the sorted prefix [ERROR], true", got, ok)
	}
}

// TestCatalogCoversFile: only a file whose labels were folded into the catalog
// counts as covered.
func TestCatalogCoversFile(t *testing.T) {
	s := newTestStore()
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "p/labelled", RowCount: 1,
		Labels: map[string][]string{"level": {"INFO"}}})
	s.OnFileReplay(FileContribution{Partition: "p", FileKey: "p/unlabelled", RowCount: 1})

	for key, want := range map[string]bool{"p/labelled": true, "p/unlabelled": false, "p/unknown": false} {
		if got := s.CatalogCoversFile("p", key); got != want {
			t.Errorf("CatalogCoversFile(%q) = %v, want %v", key, got, want)
		}
	}
	if s.CatalogCoversFile("other", "p/labelled") {
		t.Error("a file is covered only in its own partition")
	}
	s.RemoveFiles("p", []string{"p/labelled"})
	if s.CatalogCoversFile("p", "p/labelled") {
		t.Error("a removed file must no longer count as covered")
	}
}
