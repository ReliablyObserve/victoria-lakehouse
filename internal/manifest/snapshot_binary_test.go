package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSnapshot_BinaryRoundtrip pins the basic save+load cycle. A
// manifest with non-trivial state must come back identical.
func TestSnapshot_BinaryRoundtrip(t *testing.T) {
	src := New("bucket", "prefix/")
	src.prefixTemplate = "{AccountID}/{ProjectID}/"
	for i := 0; i < 100; i++ {
		src.AddFile(
			fmt.Sprintf("dt=2026-06-04/hour=%02d", i%24),
			FileInfo{
				Key:               fmt.Sprintf("0/0/traces/dt=2026-06-04/hour=%02d/f%d.parquet", i%24, i),
				Size:              int64(1000 + i),
				RowCount:          int64(100 + i),
				MinTimeNs: int64(i) * 1e9,
				MaxTimeNs: int64(i+1) * 1e9,
			},
		)
	}

	path := filepath.Join(t.TempDir(), "manifest.snap")
	if err := src.SaveTo(path); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Verify magic prefix is present — distinguishes from JSON.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if !bytes.HasPrefix(data, manifestBinaryMagic) {
		t.Fatalf("snapshot missing binary magic; first bytes=%q", data[:min(20, len(data))])
	}

	dst := New("bucket", "prefix/")
	dst.prefixTemplate = "{AccountID}/{ProjectID}/"
	if err := dst.LoadFrom(path); err != nil {
		t.Fatalf("load: %v", err)
	}
	if dst.totalFiles != src.totalFiles {
		t.Errorf("totalFiles drift: src=%d dst=%d", src.totalFiles, dst.totalFiles)
	}
	if dst.totalBytes != src.totalBytes {
		t.Errorf("totalBytes drift: src=%d dst=%d", src.totalBytes, dst.totalBytes)
	}
	if len(dst.files) != len(src.files) {
		t.Errorf("partition count drift: src=%d dst=%d", len(src.files), len(dst.files))
	}
	if len(dst.byKey) != len(src.byKey) {
		t.Errorf("byKey not rebuilt: src=%d dst=%d", len(src.byKey), len(dst.byKey))
	}
}

// TestSnapshot_JSONBackwardCompatibility pins that a snapshot saved
// by an older build (JSON only, no magic prefix) still loads cleanly.
// Without this guarantee, an upgrade would lose the manifest on
// first pod restart.
func TestSnapshot_JSONBackwardCompatibility(t *testing.T) {
	// Manually write a JSON snapshot (no binary magic).
	src := persistedManifest{
		Files: map[string][]FileInfo{
			"dt=2026-06-04/hour=00": {
				{Key: "0/0/traces/dt=2026-06-04/hour=00/a.parquet", Size: 100, RowCount: 10},
			},
		},
		MinTimeNs:   int64(1e18),
		MaxTimeNs:   int64(2e18),
		TotalFiles_: 1,
		TotalBytes_: 100,
		SavedAt:     time.Now(),
	}
	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	path := filepath.Join(t.TempDir(), "manifest.json.snap")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	// LoadFrom must detect "not binary" and fall through to JSON.
	dst := New("b", "")
	dst.prefixTemplate = "{AccountID}/{ProjectID}/"
	if err := dst.LoadFrom(path); err != nil {
		t.Fatalf("load JSON-format snapshot: %v", err)
	}
	if dst.totalFiles != 1 {
		t.Errorf("totalFiles after JSON load = %d, want 1", dst.totalFiles)
	}
	if len(dst.byKey) != 1 {
		t.Errorf("byKey not rebuilt from JSON snapshot: %d entries", len(dst.byKey))
	}
}

// TestSnapshot_BinarySizeReduction pins the actual size win. The
// audit's claim was "~3-5× smaller binary"; we don't need to enforce
// a specific ratio, but the binary must be strictly smaller than
// the equivalent JSON for a non-trivial corpus.
func TestSnapshot_BinarySizeReduction(t *testing.T) {
	src := New("b", "")
	src.prefixTemplate = "{AccountID}/{ProjectID}/"
	for i := 0; i < 1000; i++ {
		src.AddFile(
			fmt.Sprintf("dt=2026-06-04/hour=%02d", i%24),
			FileInfo{
				Key:               fmt.Sprintf("0/0/traces/dt=2026-06-04/hour=%02d/file%07d.parquet", i%24, i),
				Size:              int64(1000 + i),
				RowCount:          int64(500 + i),
				RawBytes:          int64(5000 + i*5),
				MinTimeNs: int64(i) * 1e9,
				MaxTimeNs: int64(i+1) * 1e9,
			},
		)
	}

	// Binary write.
	binPath := filepath.Join(t.TempDir(), "binary.snap")
	if err := src.SaveTo(binPath); err != nil {
		t.Fatalf("save binary: %v", err)
	}
	binData, _ := os.ReadFile(binPath)

	// JSON write of the same snapshot for comparison.
	src.mu.RLock()
	snap := persistedManifest{
		Files:       src.files,
		MinTimeNs:   src.minTime.UnixNano(),
		MaxTimeNs:   src.maxTime.UnixNano(),
		TotalFiles_: src.totalFiles,
		TotalBytes_: src.totalBytes,
		SavedAt:     time.Now(),
	}
	src.mu.RUnlock()
	jsonData, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}

	ratio := float64(len(jsonData)) / float64(len(binData))
	t.Logf("manifest snapshot sizes: binary=%d, json=%d, ratio=%.2fx",
		len(binData), len(jsonData), ratio)
	if len(binData) >= len(jsonData) {
		t.Errorf("binary format isn't smaller than JSON: binary=%d, json=%d", len(binData), len(jsonData))
	}
}

// TestSnapshot_LoadMissingFile is the cold-start case: no snapshot
// on disk. LoadFrom must return nil (the manifest stays empty), not
// an error — first-run pods don't have a snapshot yet.
func TestSnapshot_LoadMissingFile(t *testing.T) {
	dst := New("b", "")
	path := filepath.Join(t.TempDir(), "does-not-exist.snap")
	if err := dst.LoadFrom(path); err != nil {
		t.Errorf("loading missing snapshot should be nil, got: %v", err)
	}
	if dst.totalFiles != 0 {
		t.Errorf("manifest dirty after no-op load: totalFiles=%d", dst.totalFiles)
	}
}

// TestSnapshot_CorruptionDetection guards against silent data loss
// when a snapshot's bytes are truncated or scrambled. Either a JSON
// or a binary parse error must surface as a load error, not as an
// empty manifest.
func TestSnapshot_CorruptionDetection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.snap")
	// Write the binary magic but follow with garbage.
	garbage := append([]byte(nil), manifestBinaryMagic...)
	garbage = append(garbage, []byte("\xff\xff\xff\xff this is not gob")...)
	if err := os.WriteFile(path, garbage, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	dst := New("b", "")
	if err := dst.LoadFrom(path); err == nil {
		t.Error("corrupt binary snapshot should fail load, got nil")
	}
}

// TestSnapshot_Atomicity ensures the write is atomic — a crash during
// SaveTo (between the tmp write and the rename) leaves either the
// new snapshot fully there or the old one unchanged. Verified by
// asserting that .tmp doesn't linger on success and that SaveTo to
// an existing file replaces it without intermediate state.
func TestSnapshot_Atomicity(t *testing.T) {
	src := New("b", "")
	src.AddFile("dt=2026-06-04/hour=00", FileInfo{Key: "0/0/x", Size: 1})

	path := filepath.Join(t.TempDir(), "atomic.snap")

	// First save creates the file.
	if err := src.SaveTo(path); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot not at expected path: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf(".tmp file lingered after successful save: %v", err)
	}

	// Update + second save.
	src.AddFile("dt=2026-06-04/hour=00", FileInfo{Key: "0/0/y", Size: 2})
	if err := src.SaveTo(path); err != nil {
		t.Fatalf("second save: %v", err)
	}

	// Reload to verify the latest state landed.
	dst := New("b", "")
	if err := dst.LoadFrom(path); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if dst.totalFiles != 2 {
		t.Errorf("after second save, totalFiles=%d, want 2", dst.totalFiles)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
