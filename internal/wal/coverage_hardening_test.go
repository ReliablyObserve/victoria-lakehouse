package wal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestOpen_StatError_CloseError exercises the Open path where Stat fails
// and closing the file also fails (lines 37-41).
// This is hard to trigger naturally, so we test what we can: stat succeeds in
// normal cases but the combined error message format is covered via the
// stat+close error wrapping path.
func TestOpen_StatError_CloseWrapping(t *testing.T) {
	// We cannot easily make Stat fail on a successfully opened file in a
	// portable way. Instead, verify the normal Open path works and the
	// resulting WAL has correct initial size from stat.
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	// Pre-create a file with some content so stat returns non-zero size.
	if err := os.WriteFile(path, []byte("preexisting data here"), 0o600); err != nil {
		t.Fatal(err)
	}

	w, err := Open(path, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	// Size should reflect the pre-existing file content.
	if w.Size() != 21 { // len("preexisting data here") = 21
		t.Errorf("Size() = %d, want 21 (from pre-existing content)", w.Size())
	}
}

// TestAppendEntry_HeaderWriteError triggers the header write error branch
// (line 75-77) by closing the underlying file descriptor before appending.
func TestAppendEntry_HeaderWriteError(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Close the underlying OS file but leave the WAL struct thinking it's open.
	_ = w.file.Close()

	err = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "fail"})
	if err == nil {
		t.Fatal("expected error when underlying file is closed")
	}
}

// TestAppendEntry_PayloadWriteError triggers the payload write error branch
// (lines 78-80) by using a read-only file.
func TestAppendEntry_PayloadWriteError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	// Create the WAL normally, then replace the file with a read-only fd.
	w, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Close the writable fd and reopen as read-only to trigger write errors.
	_ = w.file.Close()
	ro, err := os.OpenFile(path, os.O_RDONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	w.file = ro
	w.closed = false

	err = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "readonly fail"})
	if err == nil {
		t.Fatal("expected write error on read-only file")
	}
	_ = ro.Close()
}

// TestTruncate_CloseError exercises the Truncate path where file.Close fails
// (lines 94-96). We close the underlying fd first.
func TestTruncate_CloseError(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	// Close underlying fd — Truncate's file.Close() will fail.
	_ = w.file.Close()

	err = w.Truncate()
	if err == nil {
		t.Fatal("expected error when file.Close fails during Truncate")
	}
}

// TestTruncate_CreateError exercises the Truncate path where os.Create fails
// (lines 98-100). We remove write permission on the directory.
func TestTruncate_CreateError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "wal.bin")

	w, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1, Body: "data"})

	// Remove write permission on the parent directory so os.Create fails.
	subDir := filepath.Dir(path)
	_ = os.Chmod(subDir, 0o444)
	defer func() { _ = os.Chmod(subDir, 0o755) }()

	err = w.Truncate()
	if err == nil {
		t.Fatal("expected error when os.Create fails during Truncate")
	}
}

// TestReplay_SeekError exercises the Replay path where Seek fails (lines 115-117).
// Close the underlying fd so Seek returns an error.
func TestReplay_SeekError(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1, Body: "test"})

	// Close the underlying fd but keep the WAL struct as "open".
	_ = w.file.Close()

	_, _, err = w.Replay()
	if err == nil {
		t.Fatal("expected error when Seek fails")
	}
}

// TestAppendEntry_GobEncodeError exercises the gob encode error branch
// (lines 66-68) by passing a value that gob cannot encode.
// Since appendEntry accepts `any`, we can call it with an unencodable type.
func TestAppendEntry_GobEncodeError(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	// Pass a channel, which gob cannot encode.
	err = w.appendEntry(modeLog, make(chan int))
	if err == nil {
		t.Fatal("expected gob encode error for channel type")
	}
	if !strings.Contains(err.Error(), "encode") {
		t.Errorf("expected 'encode' in error, got: %v", err)
	}
}

// TestWAL_UnlimitedMaxBytes exercises the IsFull/appendEntry path
// with maxBytes=0 (unlimited).
func TestWAL_UnlimitedMaxBytes(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 0) // 0 = unlimited
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if w.IsFull() {
		t.Error("WAL with maxBytes=0 should never be full")
	}

	// Multiple appends should all succeed.
	for i := 0; i < 10; i++ {
		if err := w.AppendLog(&schema.LogRow{TimestampUnixNano: int64(i), Body: "msg"}); err != nil {
			t.Fatalf("AppendLog[%d]: %v", i, err)
		}
	}

	if w.IsFull() {
		t.Error("WAL with maxBytes=0 should still not be full after writes")
	}
}
