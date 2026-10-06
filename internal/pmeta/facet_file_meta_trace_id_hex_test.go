package pmeta

import (
	"bytes"
	"testing"
)

// TestFileMetaFacet_TraceIDHex: the file-meta facet carries the file's
// lh.trace_id_hex attestation through Merge, Encode/Decode (the persisted
// bundle) and the exported view, and a file without it reads as unattested.
func TestFileMetaFacet_TraceIDHex(t *testing.T) {
	s := NewStore()
	s.Register(FacetFileMeta, NewFileMetaFactory())
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "hex", RowCount: 1, TraceIDHex: true})
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "other", RowCount: 1})
	for key, want := range map[string]bool{"hex": true, "other": false} {
		v, ok := s.FileMeta("p", key)
		if !ok || v.TraceIDHex != want {
			t.Fatalf("%s: TraceIDHex=%v ok=%v, want %v", key, v.TraceIDHex, ok, want)
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
	if e, _ := g.fileMeta("hex"); !e.TraceIDHex {
		t.Fatal("attestation lost across Encode/Decode")
	}
	if e, _ := g.fileMeta("other"); e.TraceIDHex {
		t.Fatal("unattested file decoded as attested")
	}
	// A bundle written before the field existed decodes as unattested.
	old := NewFileMetaFactory()("p").(*fileMetaFacet)
	if err := old.Decode(bytes.NewReader([]byte(`{"k":{"rc":1,"mn":1,"mx":2}}`))); err != nil {
		t.Fatal(err)
	}
	if e, ok := old.fileMeta("k"); !ok || e.TraceIDHex {
		t.Fatalf("old bundle entry = %+v ok=%v", e, ok)
	}
}
