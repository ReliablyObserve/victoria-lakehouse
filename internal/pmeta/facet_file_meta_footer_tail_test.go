package pmeta

import (
	"bytes"
	"testing"
)

// TestFileMetaFacet_FooterTail: the file-meta facet carries FileInfo.FooterTail
// through Merge, Encode/Decode (the persisted bundle) and the exported view; a
// file without it, and a bundle written before the field existed, read as 0
// (unknown, so the read uses the prefetch window).
func TestFileMetaFacet_FooterTail(t *testing.T) {
	s := NewStore()
	s.Register(FacetFileMeta, NewFileMetaFactory())
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "a", RowCount: 1, FooterTail: 786_432})
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "b", RowCount: 1})
	for key, want := range map[string]int32{"a": 786_432, "b": 0} {
		v, ok := s.FileMeta("p", key)
		if !ok || v.FooterTail != want {
			t.Fatalf("%s: FooterTail=%d ok=%v, want %d", key, v.FooterTail, ok, want)
		}
	}
	fc, _ := s.Get("p", FacetFileMeta)
	var buf bytes.Buffer
	if err := fc.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	g := NewFileMetaFactory()("p").(*fileMetaFacet)
	if err := g.Decode(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	if e, _ := g.fileMeta("a"); e.FooterTail != 786_432 {
		t.Fatalf("tail length lost across Encode/Decode: %+v", e)
	}
	if e, _ := g.fileMeta("b"); e.FooterTail != 0 {
		t.Fatalf("unknown tail length decoded as %d", e.FooterTail)
	}
	if bytes.Contains(buf.Bytes(), []byte(`"ft":0`)) {
		t.Fatalf("an unknown tail length must not be written: %s", buf.Bytes())
	}
	old := NewFileMetaFactory()("p").(*fileMetaFacet)
	if err := old.Decode(bytes.NewReader([]byte(`{"k":{"rc":1,"mn":1,"mx":2}}`))); err != nil {
		t.Fatal(err)
	}
	if e, ok := old.fileMeta("k"); !ok || e.FooterTail != 0 {
		t.Fatalf("old bundle entry = %+v ok=%v", e, ok)
	}
}
