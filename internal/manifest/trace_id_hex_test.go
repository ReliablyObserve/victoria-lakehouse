package manifest

import (
	"path/filepath"
	"testing"
)

// TestTraceIDHex_SnapshotAndMark: the lh.trace_id_hex attestation survives the
// manifest snapshot, MarkTraceIDHex sets it for a known key only, and the
// file-metadata sidecar round trip carries it.
func TestTraceIDHex_SnapshotAndMark(t *testing.T) {
	src := New("bucket", "prefix/")
	src.AddFile("dt=2026-10-05/hour=10", FileInfo{Key: "prefix/dt=2026-10-05/hour=10/a.parquet", Size: 1, RowCount: 1, MinTimeNs: 1, MaxTimeNs: 2, TraceIDHex: true})
	src.AddFile("dt=2026-10-05/hour=10", FileInfo{Key: "prefix/dt=2026-10-05/hour=10/b.parquet", Size: 1, RowCount: 1, MinTimeNs: 1, MaxTimeNs: 2})
	path := filepath.Join(t.TempDir(), "manifest.snapshot")
	if err := src.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	dst := New("bucket", "prefix/")
	if err := dst.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	a, _ := dst.GetFileByKey("prefix/dt=2026-10-05/hour=10/a.parquet")
	b, _ := dst.GetFileByKey("prefix/dt=2026-10-05/hour=10/b.parquet")
	if !a.TraceIDHex || b.TraceIDHex {
		t.Fatalf("after the snapshot: a=%v b=%v, want true false", a.TraceIDHex, b.TraceIDHex)
	}
	dst.MarkTraceIDHex("prefix/dt=2026-10-05/hour=10/b.parquet")
	dst.MarkTraceIDHex("prefix/unknown.parquet") // no entry: no-op
	if b, _ := dst.GetFileByKey("prefix/dt=2026-10-05/hour=10/b.parquet"); !b.TraceIDHex {
		t.Fatal("MarkTraceIDHex did not set the bit")
	}
	sc := &FileMetaSidecar{Files: map[string]FileMeta{"k": FileInfoToMeta(FileInfo{RowCount: 1, TraceIDHex: true})}}
	data, err := MarshalFileMetaSidecar(sc)
	if err != nil {
		t.Fatal(err)
	}
	back, err := UnmarshalFileMetaSidecar(data)
	if err != nil {
		t.Fatal(err)
	}
	if !back.Files["k"].TraceIDHex {
		t.Fatalf("sidecar lost the bit: %s", data)
	}
}
