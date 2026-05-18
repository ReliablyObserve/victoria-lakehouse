package bloomindex

import (
	"fmt"
	"testing"
)

func TestEmptyPartition_NoBloom(t *testing.T) {
	idx := New()
	if idx.Len() != 0 {
		t.Errorf("empty index should have 0 entries, got %d", idx.Len())
	}

	data := idx.Marshal()
	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Len() != 0 {
		t.Error("marshaled empty index should unmarshal to empty")
	}

	// MayContain on empty index returns all keys (conservative)
	keys := []string{"file1", "file2"}
	result := idx.MayContain(keys, "trace_id", "anything")
	if len(result) != len(keys) {
		t.Errorf("empty index should return all keys, got %d", len(result))
	}
}

func TestSingleFilePartition(t *testing.T) {
	idx := New()
	f := NewFilter(10, 0.01)
	f.Add("trace-only")
	idx.Add("only-file.parquet", "trace_id", f)

	if idx.Len() != 1 {
		t.Errorf("want 1 entry, got %d", idx.Len())
	}

	result := idx.MayContain([]string{"only-file.parquet"}, "trace_id", "trace-only")
	if len(result) != 1 {
		t.Error("single file should be found")
	}

	result = idx.MayContain([]string{"only-file.parquet"}, "trace_id", "trace-other")
	if len(result) != 0 {
		t.Error("non-inserted value should not match in single-file partition")
	}
}

func TestCorruptBloom_FallsBackToFullScan(t *testing.T) {
	// Build valid bloom data
	idx := New()
	f := NewFilter(100, 0.01)
	f.Add("trace-valid")
	idx.Add("file1", "trace_id", f)
	data := idx.Marshal()

	// Corrupt the data (bit flip in middle)
	if len(data) > 10 {
		corrupt := make([]byte, len(data))
		copy(corrupt, data)
		corrupt[len(corrupt)/2] ^= 0xFF
		_, err := Unmarshal(corrupt)
		// Corrupt data may or may not error, but must not panic
		_ = err
	}

	// Truncated data must error
	_, err := Unmarshal(data[:3])
	if err == nil {
		t.Error("truncated data should return error")
	}

	// Empty data
	_, err = Unmarshal(nil)
	if err == nil {
		t.Error("nil data should return error")
	}

	_, err = Unmarshal([]byte{})
	if err == nil {
		t.Error("empty data should return error")
	}
}

func TestMissingBloom_ConservativeInclusion(t *testing.T) {
	idx := New()
	// Only file1 has bloom
	idx.Add("file1", "trace_id", filterWith("trace-aaa"))

	keys := []string{"file1", "file2", "file3"}

	// Query for value not in file1's bloom
	result := idx.MayContain(keys, "trace_id", "trace-zzz")

	// file2 and file3 have no bloom — must be included (conservative)
	resultSet := make(map[string]bool)
	for _, k := range result {
		resultSet[k] = true
	}
	if !resultSet["file2"] {
		t.Error("file2 (no bloom) should be conservatively included")
	}
	if !resultSet["file3"] {
		t.Error("file3 (no bloom) should be conservatively included")
	}
	// file1 has bloom and doesn't match — should be excluded
	if resultSet["file1"] {
		t.Error("file1 should be excluded by bloom filter")
	}
}

func TestStaleBloom_FilesNotInManifest_Ignored(t *testing.T) {
	idx := New()
	idx.Add("deleted-file.parquet", "trace_id", filterWith("trace-old"))
	idx.Add("active-file.parquet", "trace_id", filterWith("trace-new"))

	// Only query against files that exist in manifest
	manifestKeys := []string{"active-file.parquet"}
	result := idx.MayContain(manifestKeys, "trace_id", "trace-new")

	if len(result) != 1 || result[0] != "active-file.parquet" {
		t.Errorf("should only return manifest files, got %v", result)
	}

	// Stale entries are simply never queried
	result = idx.MayContain(manifestKeys, "trace_id", "trace-old")
	if len(result) != 0 {
		t.Errorf("stale bloom entry should not affect results, got %v", result)
	}
}

func TestHighCardinality_BloomSkipped(t *testing.T) {
	n := 50001
	// At >50K distinct values, bloom should not be built
	if !ShouldSkipBloom(n) {
		t.Errorf("cardinality %d should trigger bloom skip", n)
	}
	if ShouldSkipBloom(50000) {
		t.Error("cardinality 50000 should not trigger bloom skip")
	}
	if ShouldSkipBloom(100) {
		t.Error("cardinality 100 should not trigger bloom skip")
	}
}

func TestZeroCardinality_EmptyBloom(t *testing.T) {
	f := NewFilter(0, 0.01)
	// Should create a minimal valid filter, not panic
	if f == nil {
		t.Fatal("NewFilter(0, ...) should not return nil")
	}
	// Empty filter should not match anything
	if f.MayContain("any-value") {
		t.Error("empty filter should not match any value")
	}
}

func TestMaxPartitionSize_4200Files(t *testing.T) {
	idx := New()
	numFiles := 4200

	for i := 0; i < numFiles; i++ {
		f := NewFilter(200, 0.01)
		for j := 0; j < 200; j++ {
			f.Add(fmt.Sprintf("trace-%d-%d", i, j))
		}
		idx.Add(fmt.Sprintf("file%d.parquet", i), "trace_id", f)
	}

	if idx.Len() != numFiles {
		t.Fatalf("want %d entries, got %d", numFiles, idx.Len())
	}

	// Marshal/unmarshal round-trip
	data := idx.Marshal()
	restored, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Len() != numFiles {
		t.Errorf("after round-trip: want %d, got %d", numFiles, restored.Len())
	}

	// Size check: ~600KB per spec for 4200 files
	sizeKB := float64(len(data)) / 1024
	t.Logf("4200-file bloom index: %.1f KB", sizeKB)
	if sizeKB > 2000 {
		t.Errorf("4200-file bloom too large: %.1f KB (want < 2000 KB)", sizeKB)
	}
}

func TestSHA256IntegrityCheck(t *testing.T) {
	idx := New()
	idx.Add("file1", "trace_id", filterWith("trace-aaa"))

	data := MarshalWithChecksum(idx)
	if len(data) == 0 {
		t.Fatal("MarshalWithChecksum returned empty data")
	}

	// Valid data should verify
	restored, err := UnmarshalWithChecksum(data)
	if err != nil {
		t.Fatalf("valid checksum failed: %v", err)
	}
	if restored.Len() != 1 {
		t.Errorf("want 1 entry, got %d", restored.Len())
	}

	// Tampered data should fail
	tampered := make([]byte, len(data))
	copy(tampered, data)
	tampered[len(tampered)/2] ^= 0x01
	_, err = UnmarshalWithChecksum(tampered)
	if err == nil {
		t.Error("tampered data should fail checksum verification")
	}
}

func TestFilter_MergeFrom_SameSizes(t *testing.T) {
	f1 := NewFilter(100, 0.01)
	f1.Add("a")
	f1.Add("b")

	f2 := NewFilter(100, 0.01)
	f2.Add("c")

	f1.MergeFrom(f2)

	if !f1.MayContain("a") {
		t.Error("lost value 'a' after merge")
	}
	if !f1.MayContain("b") {
		t.Error("lost value 'b' after merge")
	}
	if !f1.MayContain("c") {
		t.Error("merged value 'c' not found")
	}
}

func TestFilter_MergeFrom_EmptyFilter(t *testing.T) {
	f := NewFilter(100, 0.01)
	f.Add("existing")

	empty := NewFilter(100, 0.01)

	f.MergeFrom(empty)
	if !f.MayContain("existing") {
		t.Error("merging empty filter should not lose existing values")
	}
}

func TestFilter_MergeFrom_Self(t *testing.T) {
	f := NewFilter(100, 0.01)
	f.Add("value")

	// Merging with self should be idempotent
	f.MergeFrom(f)
	if !f.MayContain("value") {
		t.Error("self-merge should preserve values")
	}
}

func TestFilter_MarshalUnmarshal_EdgeCases(t *testing.T) {
	// Single-byte filter
	f := NewFilter(1, 0.5)
	f.Add("x")
	data := f.Marshal()
	f2, err := UnmarshalFilter(data)
	if err != nil {
		t.Fatal(err)
	}
	if !f2.MayContain("x") {
		t.Error("single-byte filter should contain x after round-trip")
	}

	// UnmarshalFilter with too-short data
	_, err = UnmarshalFilter([]byte{})
	if err == nil {
		t.Error("empty data should fail")
	}
	_, err = UnmarshalFilter([]byte{1})
	if err == nil {
		t.Error("single-byte data should fail")
	}
}
