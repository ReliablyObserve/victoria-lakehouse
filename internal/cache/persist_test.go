package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLabelIndex_AddAndGet(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("service.name", []string{"api", "web", "worker"})

	names := idx.GetFieldNames()
	if len(names) != 1 {
		t.Fatalf("field names len = %d, want 1", len(names))
	}
	if names[0] != "service.name" {
		t.Errorf("name = %q, want %q", names[0], "service.name")
	}

	vals := idx.GetFieldValues("service.name", 0)
	if len(vals) != 3 {
		t.Fatalf("values len = %d, want 3", len(vals))
	}
}

func TestLabelIndex_AddMergesValues(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("svc", []string{"a", "b"})
	idx.Add("svc", []string{"b", "c"})

	vals := idx.GetFieldValues("svc", 0)
	if len(vals) != 3 {
		t.Errorf("values len = %d, want 3 (a,b,c deduplicated)", len(vals))
	}
}

func TestLabelIndex_AddIncrementsSeenInFiles(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("svc", nil)
	idx.Add("svc", nil)
	idx.Add("svc", nil)

	vals := idx.GetFieldValues("svc", 0)
	_ = vals
	if idx.Len() != 1 {
		t.Errorf("len = %d, want 1", idx.Len())
	}
}

func TestLabelIndex_AddNilValues(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("field", nil)

	names := idx.GetFieldNames()
	if len(names) != 1 {
		t.Fatalf("len = %d, want 1", len(names))
	}
}

func TestLabelIndex_GetFieldValuesLimit(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("svc", []string{"a", "b", "c", "d", "e"})

	vals := idx.GetFieldValues("svc", 3)
	if len(vals) != 3 {
		t.Errorf("values len = %d, want 3 (limited)", len(vals))
	}
}

func TestLabelIndex_GetFieldValuesMissing(t *testing.T) {
	idx := NewLabelIndex()
	vals := idx.GetFieldValues("missing", 0)
	if vals != nil {
		t.Errorf("expected nil for missing field, got %v", vals)
	}
}

func TestLabelIndex_ValuesCap(t *testing.T) {
	idx := NewLabelIndex()
	vals := make([]string, 10001)
	for i := range vals {
		vals[i] = string(rune('a' + (i % 26)))
	}
	idx.Add("big", vals)

	result := idx.GetFieldValues("big", 0)
	if len(result) > 10000 {
		t.Errorf("values should be capped at 10000, got %d", len(result))
	}
}

func TestLabelIndex_Len(t *testing.T) {
	idx := NewLabelIndex()
	if idx.Len() != 0 {
		t.Errorf("len = %d, want 0", idx.Len())
	}
	idx.Add("a", nil)
	idx.Add("b", nil)
	if idx.Len() != 2 {
		t.Errorf("len = %d, want 2", idx.Len())
	}
}

func TestPersister_SaveLoadManifest(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	state := &ManifestState{
		Files: map[string][]FileInfoState{
			"2026-05-01": {{Key: "file1.parquet", Size: 1000}},
		},
		MinTime:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		MaxTime:    time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		TotalFiles: 1,
		TotalBytes: 1000,
	}

	if err := p.SaveManifest(state); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.LoadManifest()
	if err != nil {
		t.Fatal(err)
	}

	if loaded.TotalFiles != 1 {
		t.Errorf("total files = %d, want 1", loaded.TotalFiles)
	}
	if loaded.TotalBytes != 1000 {
		t.Errorf("total bytes = %d, want 1000", loaded.TotalBytes)
	}
	if loaded.SavedAt.IsZero() {
		t.Error("saved_at should be set")
	}
	if len(loaded.Files["2026-05-01"]) != 1 {
		t.Error("expected 1 file info")
	}
}

func TestPersister_SaveLoadLabelIndex(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	idx := NewLabelIndex()
	idx.Add("service.name", []string{"api", "web"})
	idx.Add("level", []string{"info", "error", "warn"})

	if err := p.SaveLabelIndex(idx); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.LoadLabelIndex()
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Len() != 2 {
		t.Errorf("loaded len = %d, want 2", loaded.Len())
	}

	vals := loaded.GetFieldValues("service.name", 0)
	if len(vals) != 2 {
		t.Errorf("service.name values = %d, want 2", len(vals))
	}

	vals = loaded.GetFieldValues("level", 0)
	if len(vals) != 3 {
		t.Errorf("level values = %d, want 3", len(vals))
	}
}

func TestPersister_LoadMissing(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = p.LoadManifest()
	if err == nil {
		t.Error("expected error for missing manifest")
	}

	_, err = p.LoadLabelIndex()
	if err == nil {
		t.Error("expected error for missing label index")
	}
}

func TestPersister_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	state := &ManifestState{TotalFiles: 42}
	if err := p.SaveManifest(state); err != nil {
		t.Fatal(err)
	}

	tmpPath := filepath.Join(dir, "manifest.json.tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("temp file should be cleaned up after atomic rename")
	}

	finalPath := filepath.Join(dir, "manifest.json")
	if _, err := os.Stat(finalPath); os.IsNotExist(err) {
		t.Error("final file should exist")
	}
}

func TestPersister_Dir(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if p.Dir() != dir {
		t.Errorf("dir = %q, want %q", p.Dir(), dir)
	}
}

func TestPersister_CreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "path")
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p.Dir()); os.IsNotExist(err) {
		t.Error("persister should create directory")
	}
}
