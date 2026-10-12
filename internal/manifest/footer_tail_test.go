package manifest

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"path/filepath"
	"testing"
)

const ftPart = "dt=2026-10-05/hour=10"

func ftKey(n string) string { return "prefix/" + ftPart + "/" + n + ".parquet" }

// TestFooterTail_SnapshotRoundTripAndMark: FileInfo.FooterTail survives the
// binary snapshot, SetFooterTail records or corrects it for a known key only, and
// ignores a non-positive value.
func TestFooterTail_SnapshotRoundTripAndMark(t *testing.T) {
	src := New("bucket", "prefix/")
	src.AddFile(ftPart, FileInfo{Key: ftKey("a"), Size: 9, RowCount: 1, MinTimeNs: 1, MaxTimeNs: 2, FooterTail: 786_432})
	src.AddFile(ftPart, FileInfo{Key: ftKey("b"), Size: 9, RowCount: 1, MinTimeNs: 1, MaxTimeNs: 2})
	path := filepath.Join(t.TempDir(), "manifest.snapshot")
	if err := src.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	dst := New("bucket", "prefix/")
	if err := dst.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	get := func(n string) int32 {
		fi, ok := dst.GetFileByKey(ftKey(n))
		if !ok {
			t.Fatalf("%s missing after load", n)
		}
		return fi.FooterTail
	}
	if get("a") != 786_432 || get("b") != 0 {
		t.Fatalf("after the snapshot: a=%d b=%d, want 786432 0", get("a"), get("b"))
	}

	dst.SetFooterTail(ftKey("b"), 4096)    // learned for a file that lacked it
	dst.SetFooterTail(ftKey("a"), 900_000) // a recorded value that was wrong is corrected
	dst.SetFooterTail(ftKey("nope"), 7)    // unknown key: no-op
	if get("b") != 4096 || get("a") != 900_000 {
		t.Fatalf("after SetFooterTail: a=%d b=%d", get("a"), get("b"))
	}
	for _, bad := range []int32{0, -1, -4096} {
		dst.SetFooterTail(ftKey("a"), bad)
		if get("a") != 900_000 {
			t.Fatalf("SetFooterTail(%d) changed the value to %d", bad, get("a"))
		}
	}
	dst.SetFooterTail(ftKey("a"), 900_000) // unchanged: the read-lock fast path
	if get("a") != 900_000 {
		t.Fatal("an unchanged SetFooterTail altered the value")
	}
}

// TestFooterTail_SnapshotIsAdditive: the field is additive in both directions. A
// snapshot written before it existed loads with FooterTail 0 and every other field
// intact; a snapshot written with it loads in a build that has no such field.
func TestFooterTail_SnapshotIsAdditive(t *testing.T) {
	type oldFileInfo struct { // the shape before FooterTail
		Key        string
		Size       int64
		RowCount   int64
		TraceIDHex bool
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(oldFileInfo{Key: ftKey("old"), Size: 5, RowCount: 3, TraceIDHex: true}); err != nil {
		t.Fatal(err)
	}
	var now FileInfo
	if err := gob.NewDecoder(&buf).Decode(&now); err != nil {
		t.Fatalf("a snapshot from before FooterTail does not decode: %v", err)
	}
	if now.Key != ftKey("old") || now.Size != 5 || now.RowCount != 3 || !now.TraceIDHex || now.FooterTail != 0 {
		t.Fatalf("decoded %+v", now)
	}

	buf.Reset()
	if err := gob.NewEncoder(&buf).Encode(FileInfo{Key: ftKey("new"), Size: 5, RowCount: 3, FooterTail: 1234}); err != nil {
		t.Fatal(err)
	}
	var old oldFileInfo
	if err := gob.NewDecoder(&buf).Decode(&old); err != nil {
		t.Fatalf("a snapshot with FooterTail does not decode in a build without it: %v", err)
	}
	if old.Key != ftKey("new") || old.RowCount != 3 {
		t.Fatalf("decoded %+v", old)
	}

	// The JSON form (legacy snapshots, the API) is omitempty: nothing is written
	// for an unknown tail, and an old document reads as unknown.
	b, err := json.Marshal(FileInfo{Key: "k"})
	if err != nil || bytes.Contains(b, []byte("footer_tail")) {
		t.Fatalf("an unknown FooterTail must not be written: %s %v", b, err)
	}
	var fi FileInfo
	if err := json.Unmarshal([]byte(`{"key":"k","size":1}`), &fi); err != nil || fi.FooterTail != 0 {
		t.Fatalf("old JSON: %+v %v", fi, err)
	}
	if b, _ = json.Marshal(FileInfo{Key: "k", FooterTail: 77}); !bytes.Contains(b, []byte(`"footer_tail":77`)) {
		t.Fatalf("FooterTail not written: %s", b)
	}
}

// TestFooterTail_SidecarCarriesIt: the file-metadata sidecar (and the pmeta facet
// that mirrors it) round-trips the field, and ApplyTo fills it only when the
// manifest has none.
func TestFooterTail_SidecarCarriesIt(t *testing.T) {
	sc := &FileMetaSidecar{Files: map[string]FileMeta{"k": FileInfoToMeta(FileInfo{RowCount: 1, FooterTail: 555})}}
	data, err := MarshalFileMetaSidecar(sc)
	if err != nil {
		t.Fatal(err)
	}
	back, err := UnmarshalFileMetaSidecar(data)
	if err != nil {
		t.Fatal(err)
	}
	if back.Files["k"].FooterTail != 555 {
		t.Fatalf("sidecar lost the tail length: %s", data)
	}
	var fi FileInfo
	back.Files["k"].ApplyTo(&fi)
	if fi.FooterTail != 555 {
		t.Fatalf("ApplyTo: FooterTail = %d, want 555", fi.FooterTail)
	}
	fi = FileInfo{FooterTail: 111}
	back.Files["k"].ApplyTo(&fi)
	if fi.FooterTail != 111 {
		t.Fatalf("ApplyTo overwrote a value the manifest already had: %d", fi.FooterTail)
	}
}
