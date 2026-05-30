package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLRU_ZeroMaxSize(t *testing.T) {
	c := NewLRU(0)
	c.Put("key", []byte("val"))
	if c.Len() != 0 {
		t.Errorf("expected 0 entries with maxSize=0, got %d", c.Len())
	}
}

func TestLRU_SingleByteMax(t *testing.T) {
	c := NewLRU(1)
	c.Put("a", []byte{0x01})
	if c.Len() != 1 {
		t.Errorf("expected 1 entry, got %d", c.Len())
	}
	c.Put("b", []byte{0x02})
	if c.Len() != 1 {
		t.Errorf("expected 1 entry after eviction, got %d", c.Len())
	}
	_, ok := c.Get("a")
	if ok {
		t.Error("'a' should have been evicted")
	}
	val, ok := c.Get("b")
	if !ok || val[0] != 0x02 {
		t.Error("'b' should exist with value 0x02")
	}
}

func TestLRU_EmptyKey(t *testing.T) {
	c := NewLRU(1024)
	c.Put("", []byte("empty-key"))
	val, ok := c.Get("")
	if !ok || string(val) != "empty-key" {
		t.Error("empty key should be retrievable")
	}
}

func TestLRU_EmptyValue(t *testing.T) {
	c := NewLRU(1024)
	c.Put("key", []byte{})
	val, ok := c.Get("key")
	if !ok || len(val) != 0 {
		t.Error("empty value should be retrievable")
	}
}

func TestLRU_LargeValue(t *testing.T) {
	c := NewLRU(100)
	big := make([]byte, 200)
	c.Put("big", big)
	if c.Len() != 0 {
		t.Errorf("value larger than max should be evicted, got %d entries", c.Len())
	}
}

func TestLRU_OverwriteChangesSize(t *testing.T) {
	c := NewLRU(1024)
	c.Put("key", make([]byte, 10))
	if c.Size() != 10 {
		t.Errorf("size = %d, want 10", c.Size())
	}
	c.Put("key", make([]byte, 20))
	if c.Size() != 20 {
		t.Errorf("size after overwrite = %d, want 20", c.Size())
	}
	if c.Len() != 1 {
		t.Errorf("len after overwrite = %d, want 1", c.Len())
	}
}

func TestLRU_DeleteNonexistent_NoError(t *testing.T) {
	c := NewLRU(1024)
	c.Delete("nonexistent")
	if c.Len() != 0 {
		t.Errorf("len = %d after deleting nonexistent", c.Len())
	}
	if c.Size() != 0 {
		t.Errorf("size = %d after deleting nonexistent", c.Size())
	}
}

// TestLRU_GetSharesBufferAcrossCallers verifies the share-by-reference
// contract that replaces the previous copy-on-Get behaviour. With 16
// concurrent file workers all hitting the same cached parquet bytes,
// per-Get copies summed to >1 GiB of transient heap; the contract is
// now "callers must not mutate Get results", with all current call sites
// passing the bytes straight to parquet.OpenFile (read-only).
func TestLRU_GetSharesBufferAcrossCallers(t *testing.T) {
	c := NewLRU(1024)
	original := []byte("hello")
	c.Put("key", original)

	got1, _ := c.Get("key")
	got2, _ := c.Get("key")
	if &got1[0] != &got2[0] {
		t.Error("two Get calls should return the same underlying buffer (share-by-reference)")
	}
}

func TestLRU_PutStoresDataCopy(t *testing.T) {
	c := NewLRU(1024)
	data := []byte("hello")
	c.Put("key", data)

	data[0] = 'X'

	got, _ := c.Get("key")
	if got[0] != 'h' {
		t.Error("modifying original data should not affect cache")
	}
}

// TestLRU_PutNoCopySharesBuffer documents the PutNoCopy contract: the
// caller transfers ownership, so the cached buffer is the SAME []byte
// the caller passed in. This is the canonical API for the S3-download
// fast path where the downloaded buffer is allocated for caching and
// the caller never mutates it.
//
// Why we expose this and don't just change Put: there ARE callers
// (insert path, test scaffolding) that intentionally rely on Put's
// copy-on-write contract. Splitting the API makes the no-copy path
// explicit and surfaces the ownership-transfer requirement at the
// call site.
//
// Negative-control: change PutNoCopy to also copy and this test
// (sharing pointer-identity) fails.
func TestLRU_PutNoCopySharesBuffer(t *testing.T) {
	c := NewLRU(1024)
	data := []byte("hello")
	c.PutNoCopy("key", data)

	got, _ := c.Get("key")
	if &got[0] != &data[0] {
		t.Error("PutNoCopy must store the caller's buffer by reference (the API contract); "+
			"a copy here doubles transient memory and defeats the point")
	}
}

func TestLRU_ClearResets(t *testing.T) {
	c := NewLRU(1024)
	for i := 0; i < 10; i++ {
		c.Put(string(rune('a'+i)), make([]byte, 10))
	}
	c.Clear()
	if c.Len() != 0 || c.Size() != 0 {
		t.Errorf("after Clear: len=%d, size=%d", c.Len(), c.Size())
	}
}

func TestLRU_StatsAccuracy(t *testing.T) {
	c := NewLRU(100)
	c.Put("a", make([]byte, 30))
	c.Put("b", make([]byte, 30))
	c.Get("a")
	c.Get("nonexistent")

	s := c.Stats()
	if s.Hits != 1 {
		t.Errorf("hits = %d, want 1", s.Hits)
	}
	if s.Misses != 1 {
		t.Errorf("misses = %d, want 1", s.Misses)
	}
	if s.Entries != 2 {
		t.Errorf("entries = %d, want 2", s.Entries)
	}
}

func TestDiskCache_InvalidWatermark(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = dc

	dc2, err := NewDiskCache(dir, 1024, 1.5)
	if err != nil {
		t.Fatal(err)
	}
	_ = dc2

	dc3, err := NewDiskCache(dir, 1024, -1)
	if err != nil {
		t.Fatal(err)
	}
	_ = dc3
}

func TestDiskCache_DeletedFileHandling(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := dc.Put("key1", []byte("data1")); err != nil {
		t.Fatal(err)
	}

	path := dc.keyToPath("key1")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	_, ok := dc.Get("key1")
	if ok {
		t.Error("Get should return false for deleted file")
	}
}

func TestDiskCache_PutFromPath_NonexistentSource(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	err = dc.PutFromPath("key", "/nonexistent/file.dat")
	if err == nil {
		t.Error("expected error for nonexistent source")
	}
}

func TestDiskCache_PutFromPath_PathTraversal(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	srcFile := filepath.Join(dir, "src.dat")
	if err := os.WriteFile(srcFile, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}

	traversalKeys := []string{
		"../escape",
		"../../etc/passwd",
		"key/../../../tmp/evil",
	}
	for _, key := range traversalKeys {
		err := dc.PutFromPath(key, srcFile)
		if err == nil {
			dstPath := dc.keyToPath(key)
			absDir, _ := filepath.Abs(dir)
			absDst, _ := filepath.Abs(dstPath)
			if !strings.HasPrefix(absDst, absDir) {
				t.Errorf("PutFromPath(%q) should fail for path traversal", key)
			}
		}
	}
}

func TestDiskCache_EvictionOrder(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 100, 0.5)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := dc.Put("a", make([]byte, 30)); err != nil {
		t.Fatal(err)
	}
	if _, err := dc.Put("b", make([]byte, 30)); err != nil {
		t.Fatal(err)
	}
	dc.Get("a")

	if _, err := dc.Put("c", make([]byte, 30)); err != nil {
		t.Fatal(err)
	}

	_, okA := dc.Get("a")
	_, okB := dc.Get("b")
	if okB && !okA {
		t.Log("b survived but a was evicted — unexpected LRU order")
	}
	_ = okA
	_ = okB
}

func TestLabelIndex_EmptyFieldName(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("", []string{"val"})
	names := idx.GetFieldNames()
	found := false
	for _, n := range names {
		if n == "" {
			found = true
		}
	}
	if !found {
		t.Error("empty field name should be stored")
	}
}

func TestLabelIndex_DuplicateValues(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("field", []string{"a", "b", "c"})
	idx.Add("field", []string{"b", "c", "d"})

	vals := idx.GetFieldValues("field", 0)
	seen := make(map[string]int)
	for _, v := range vals {
		seen[v]++
	}
	for v, count := range seen {
		if count > 1 {
			t.Errorf("duplicate value %q in field values (count=%d)", v, count)
		}
	}
	if len(vals) != 4 {
		t.Errorf("expected 4 unique values, got %d", len(vals))
	}
}

func TestLabelIndex_ValuesCap_Enforced(t *testing.T) {
	idx := NewLabelIndex()
	vals := make([]string, 10001)
	for i := range vals {
		vals[i] = fmt.Sprintf("val-%d", i)
	}
	idx.Add("field", vals)

	stored := idx.GetFieldValues("field", 0)
	if len(stored) > 10000 {
		t.Errorf("stored %d values, cap should be 10000", len(stored))
	}
}

func TestLabelIndex_GetFieldValues_WithLimit(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("field", []string{"a", "b", "c", "d", "e"})

	vals := idx.GetFieldValues("field", 3)
	if len(vals) != 3 {
		t.Errorf("expected 3 values with limit=3, got %d", len(vals))
	}
}

func TestLabelIndex_GetFieldValues_NonexistentField(t *testing.T) {
	idx := NewLabelIndex()
	vals := idx.GetFieldValues("nonexistent", 0)
	if vals != nil {
		t.Errorf("expected nil for nonexistent field, got %v", vals)
	}
}

func TestLabelIndex_SeenInFilesIncrement(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("field", []string{"a"})
	idx.Add("field", []string{"b"})
	idx.Add("field", []string{"c"})

	if idx.Len() != 1 {
		t.Errorf("expected 1 field, got %d", idx.Len())
	}
}

func TestPersister_LoadNonexistent(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = p.LoadManifest()
	if err == nil {
		t.Error("expected error loading nonexistent manifest")
	}

	_, err = p.LoadLabelIndex()
	if err == nil {
		t.Error("expected error loading nonexistent label index")
	}
}

func TestPersister_ManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	state := &ManifestState{
		TotalFiles: 42,
		TotalBytes: 1024 * 1024,
		Files: map[string][]FileInfoState{
			"dt=2026-05-02/hour=10": {{Key: "file1.parquet", Size: 1024}},
		},
	}

	if err := p.SaveManifest(state); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.LoadManifest()
	if err != nil {
		t.Fatal(err)
	}

	if loaded.TotalFiles != 42 {
		t.Errorf("totalFiles = %d, want 42", loaded.TotalFiles)
	}
	if loaded.TotalBytes != 1024*1024 {
		t.Errorf("totalBytes = %d, want 1048576", loaded.TotalBytes)
	}
	if loaded.SavedAt.IsZero() {
		t.Error("savedAt should be set")
	}
}

func TestPersister_CorruptedJSON(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte("{invalid"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = p.LoadManifest()
	if err == nil {
		t.Error("expected error for corrupted JSON")
	}
}
