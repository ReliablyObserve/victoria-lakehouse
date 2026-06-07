package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestLoadFrom_StreamingPath verifies the binary-format gob decode
// reads incrementally from the file instead of slurping the whole
// payload into memory. We can't easily measure peak RSS in a unit
// test, but we CAN verify the contract: an oversized file is
// rejected at Stat before any decode work happens — proving the
// early-stat path is in place.
func TestLoadFrom_StreamingPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.bin")

	// Round-trip a populated manifest through SaveTo/LoadFrom to
	// confirm streaming decode produces identical state.
	src := New("bucket", "prefix/")
	for i := 0; i < 100; i++ {
		src.AddFile("dt=2026-06-05/hour=10", FileInfo{
			Key:  fileKey(i),
			Size: int64(1000 + i),
		})
	}
	if err := src.SaveTo(path); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}

	dst := New("bucket", "prefix/")
	if err := dst.LoadFrom(path); err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if dst.TotalFiles() != src.TotalFiles() {
		t.Errorf("TotalFiles after streaming decode = %d, want %d",
			dst.TotalFiles(), src.TotalFiles())
	}
	if dst.TotalBytes() != src.TotalBytes() {
		t.Errorf("TotalBytes after streaming decode = %d, want %d",
			dst.TotalBytes(), src.TotalBytes())
	}
}

// TestLoadFrom_RefusesOversizedFile pins the early-stat rejection.
// A 51 GiB snapshot (above the 50 GiB cap) should be refused
// without the decoder ever running — otherwise the gob decoder
// would allocate buffers proportional to the snapshot before
// hitting the LimitReader.
func TestLoadFrom_RefusesOversizedFile(t *testing.T) {
	// We can't actually create a 51 GiB file in CI. Substitute by
	// truncating to a size larger than the cap. The file content
	// doesn't need to be valid gob — LoadFrom rejects on size
	// before reading any bytes.
	dir := t.TempDir()
	path := filepath.Join(dir, "huge.bin")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Truncate to (cap + 1) bytes. Sparse file; on most filesystems
	// this consumes 0 disk blocks.
	if err := f.Truncate(maxManifestSnapshotBytes + 1); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m := New("bucket", "prefix/")
	err = m.LoadFrom(path)
	if err == nil {
		t.Fatal("expected error for oversized snapshot, got nil — early-stat rejection broken")
	}
	if !contains(err.Error(), "exceeds limit") {
		t.Errorf("error %q doesn't reference the size cap", err)
	}
}

// TestLoadFrom_LegacyJSONStillWorks pins backwards compatibility
// for pre-binary-format snapshots. The streaming refactor must not
// regress the JSON path — operators upgrading from older builds
// still load their existing snapshots on first start.
func TestLoadFrom_LegacyJSONStillWorks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")

	snap := persistedManifest{
		Files: map[string][]FileInfo{
			"dt=2026-06-05/hour=10": {{Key: "legacy.parquet", Size: 12345}},
		},
		TotalFiles_: 1,
		TotalBytes_: 12345,
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	m := New("bucket", "prefix/")
	if err := m.LoadFrom(path); err != nil {
		t.Fatalf("LoadFrom legacy JSON: %v", err)
	}
	if m.TotalFiles() != 1 {
		t.Errorf("legacy JSON load: TotalFiles=%d, want 1", m.TotalFiles())
	}
	if m.TotalBytes() != 12345 {
		t.Errorf("legacy JSON load: TotalBytes=%d, want 12345", m.TotalBytes())
	}
}

// TestLoadFrom_MissingFileNoError pins the cold-start contract:
// a non-existent snapshot is a clean no-op, not an error. The
// lifecycle manager treats LoadFrom's nil-no-error path as
// "no snapshot to recover from", and falls through to S3 LIST.
func TestLoadFrom_MissingFileNoError(t *testing.T) {
	m := New("bucket", "prefix/")
	if err := m.LoadFrom("/nonexistent/path/manifest.bin"); err != nil {
		t.Errorf("LoadFrom missing file = %v, want nil — cold-start regression", err)
	}
}

// TestLoadFrom_TruncatedShortFile guards against a file that's
// too short to contain even the magic prefix. Treat as missing.
func TestLoadFrom_TruncatedShortFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tiny.bin")
	if err := os.WriteFile(path, []byte("xx"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	m := New("bucket", "prefix/")
	if err := m.LoadFrom(path); err != nil {
		t.Errorf("LoadFrom truncated file should not error, got %v", err)
	}
}

func fileKey(i int) string {
	return "f" + intToString(i) + ".parquet"
}

func intToString(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
