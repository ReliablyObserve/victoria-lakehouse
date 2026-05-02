package cache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiskCache_PutGet(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	path, err := dc.Put("file1", []byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		t.Fatal("expected non-empty path")
	}

	gotPath, ok := dc.Get("file1")
	if !ok {
		t.Fatal("expected hit")
	}
	if gotPath != path {
		t.Errorf("path = %q, want %q", gotPath, path)
	}

	data, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("file content = %q, want %q", data, "hello world")
	}
}

func TestDiskCache_GetMiss(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	_, ok := dc.Get("missing")
	if ok {
		t.Error("expected miss")
	}
}

func TestDiskCache_PutOverwrite(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := dc.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := dc.Put("k", []byte("v2-updated")); err != nil {
		t.Fatal(err)
	}

	path, ok := dc.Get("k")
	if !ok {
		t.Fatal("expected hit")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "v2-updated" {
		t.Errorf("content = %q, want %q", data, "v2-updated")
	}
	if dc.Len() != 1 {
		t.Errorf("len = %d, want 1", dc.Len())
	}
}

func TestDiskCache_Eviction(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 100, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := dc.Put("a", make([]byte, 50)); err != nil {
		t.Fatal(err)
	}
	if _, err := dc.Put("b", make([]byte, 50)); err != nil {
		t.Fatal(err)
	}

	stats := dc.Stats()
	if stats.Evictions == 0 {
		t.Error("expected evictions when exceeding watermark")
	}
}

func TestDiskCache_Delete(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	path, _ := dc.Put("k", []byte("data"))
	dc.Delete("k")

	if _, ok := dc.Get("k"); ok {
		t.Error("deleted key should not exist")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should be deleted from disk")
	}
}

func TestDiskCache_DeleteNonexistent(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}
	dc.Delete("missing")
}

func TestDiskCache_Clear(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := dc.Put("k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := dc.Put("k2", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := dc.Clear(); err != nil {
		t.Fatal(err)
	}

	if dc.Len() != 0 {
		t.Errorf("len after clear = %d, want 0", dc.Len())
	}
	if dc.Size() != 0 {
		t.Errorf("size after clear = %d, want 0", dc.Size())
	}
}

func TestDiskCache_PutFromPath(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	srcPath := filepath.Join(t.TempDir(), "source.dat")
	if err := os.WriteFile(srcPath, []byte("from-file"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := dc.PutFromPath("imported", srcPath); err != nil {
		t.Fatal(err)
	}

	cachedPath, ok := dc.Get("imported")
	if !ok {
		t.Fatal("expected hit")
	}
	data, _ := os.ReadFile(cachedPath)
	if string(data) != "from-file" {
		t.Errorf("content = %q, want %q", data, "from-file")
	}
}

func TestDiskCache_StaleFileRemoved(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	path, err := dc.Put("k", []byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	_, ok := dc.Get("k")
	if ok {
		t.Error("should miss when file is deleted externally")
	}
}

func TestDiskCache_KeySanitization(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	key := "logs/dt=2026-05-01/hour=10/file.parquet"
	_, err = dc.Put(key, []byte("data"))
	if err != nil {
		t.Fatal(err)
	}

	_, ok := dc.Get(key)
	if !ok {
		t.Error("expected hit for sanitized key")
	}
}

func TestDiskCache_Stats(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := dc.Put("k", []byte("12345")); err != nil {
		t.Fatal(err)
	}
	dc.Get("k")
	dc.Get("missing")

	stats := dc.Stats()
	if stats.Entries != 1 {
		t.Errorf("entries = %d, want 1", stats.Entries)
	}
	if stats.Size != 5 {
		t.Errorf("size = %d, want 5", stats.Size)
	}
	if stats.Hits != 1 {
		t.Errorf("hits = %d, want 1", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("misses = %d, want 1", stats.Misses)
	}
}

func TestDiskCache_Dir(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}
	if dc.Dir() != dir {
		t.Errorf("dir = %q, want %q", dc.Dir(), dir)
	}
}

func TestDiskCache_DefaultWatermark(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	if dc == nil {
		t.Fatal("expected non-nil cache with invalid watermark")
	}
}
