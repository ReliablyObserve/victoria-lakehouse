package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestReadExisting_Missing proves a missing file is reported as "no bytes,
// no error" — the existing behavior a caller relies on to treat it as
// stale (mismatching any non-empty want) rather than a hard failure.
func TestReadExisting_Missing(t *testing.T) {
	data, err := readExisting(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("missing file must not be an error, got: %v", err)
	}
	if data != nil {
		t.Fatalf("missing file must read as nil, got: %v", data)
	}
}

// TestReadExisting_Present proves a real file's bytes are returned as-is.
func TestReadExisting_Present(t *testing.T) {
	p := filepath.Join(t.TempDir(), "present.yaml")
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := readExisting(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("got %q, want %q", data, "hello")
	}
}

// TestReadExisting_OtherErrorSurfaces proves that an error other than
// "file does not exist" (here: EISDIR, from pointing readExisting at a
// directory) is returned to the caller rather than silently discarded —
// the bug this test guards against would have misreported a permission
// error or EISDIR the same way as a simple "not written yet" file.
func TestReadExisting_OtherErrorSurfaces(t *testing.T) {
	dir := t.TempDir()
	_, err := readExisting(dir)
	if err == nil {
		t.Fatal("reading a directory must return an error, got nil")
	}
	if os.IsNotExist(err) {
		t.Fatalf("a directory's read error must not look like os.IsNotExist: %v", err)
	}
}
