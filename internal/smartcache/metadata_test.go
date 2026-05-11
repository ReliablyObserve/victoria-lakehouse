package smartcache

import (
	"path/filepath"
	"testing"
	"time"
)

func TestMetadataMap_SetGet(t *testing.T) {
	m := NewMetadataMap()

	now := time.Now()
	meta := EntryMeta{
		CreatedAt:         now,
		LastAccess:        now,
		AccessCount:       0,
		AccessWindowStart: now,
		Signal:            "logs",
		Size:              1024,
	}

	m.Set("file1.parquet", meta)

	got, ok := m.Get("file1.parquet")
	if !ok {
		t.Fatal("expected to find entry")
	}
	if got.Signal != "logs" {
		t.Errorf("signal = %q, want %q", got.Signal, "logs")
	}
	if got.Size != 1024 {
		t.Errorf("size = %d, want 1024", got.Size)
	}
}

func TestMetadataMap_Delete(t *testing.T) {
	m := NewMetadataMap()
	m.Set("key1", EntryMeta{Signal: "logs", Size: 100})
	m.Delete("key1")

	_, ok := m.Get("key1")
	if ok {
		t.Fatal("expected entry to be deleted")
	}
}

func TestMetadataMap_RecordAccess(t *testing.T) {
	m := NewMetadataMap()
	now := time.Now()
	m.Set("key1", EntryMeta{
		CreatedAt:         now.Add(-time.Hour),
		LastAccess:        now.Add(-time.Hour),
		AccessCount:       0,
		AccessWindowStart: now.Add(-time.Hour),
		Signal:            "logs",
		Size:              100,
	})

	m.RecordAccess("key1")

	got, ok := m.Get("key1")
	if !ok {
		t.Fatal("expected to find entry")
	}
	if got.AccessCount != 1 {
		t.Errorf("access count = %d, want 1", got.AccessCount)
	}
	if got.LastAccess.Before(now) {
		t.Error("expected LastAccess to be updated to now or later")
	}
}

func TestMetadataMap_Len(t *testing.T) {
	m := NewMetadataMap()
	m.Set("a", EntryMeta{Size: 10})
	m.Set("b", EntryMeta{Size: 20})
	m.Set("c", EntryMeta{Size: 30})

	if m.Len() != 3 {
		t.Errorf("len = %d, want 3", m.Len())
	}
}

func TestMetadataMap_TotalSize(t *testing.T) {
	m := NewMetadataMap()
	m.Set("a", EntryMeta{Size: 100})
	m.Set("b", EntryMeta{Size: 200})

	if m.TotalSize() != 300 {
		t.Errorf("total size = %d, want 300", m.TotalSize())
	}
}

func TestMetadataMap_PinUnpin(t *testing.T) {
	m := NewMetadataMap()
	m.Set("key1", EntryMeta{Signal: "logs", Size: 100})

	m.Pin("key1", "query-1", 5*time.Minute)

	got, _ := m.Get("key1")
	if len(got.PinnedBy) != 1 {
		t.Fatalf("expected 1 pin, got %d", len(got.PinnedBy))
	}
	if _, ok := got.PinnedBy["query-1"]; !ok {
		t.Error("expected pin by query-1")
	}

	m.Unpin("key1", "query-1")

	got, _ = m.Get("key1")
	if len(got.PinnedBy) != 0 {
		t.Errorf("expected 0 pins after unpin, got %d", len(got.PinnedBy))
	}
}

func TestSnapshot_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smartcache.meta.json")

	m := NewMetadataMap()
	now := time.Now().Truncate(time.Millisecond)
	m.Set("file1", EntryMeta{
		CreatedAt:   now,
		LastAccess:  now,
		AccessCount: 5,
		Signal:      "logs",
		Size:        1024,
		TraceIDs:    []string{"abc", "def"},
	})
	m.Set("file2", EntryMeta{
		CreatedAt:  now.Add(-time.Hour),
		LastAccess: now.Add(-30 * time.Minute),
		Signal:     "traces",
		Size:       2048,
	})

	if err := m.SaveSnapshot(path); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	loaded := NewMetadataMap()
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	if loaded.Len() != 2 {
		t.Fatalf("loaded len = %d, want 2", loaded.Len())
	}

	got, ok := loaded.Get("file1")
	if !ok {
		t.Fatal("expected file1 in loaded snapshot")
	}
	if got.AccessCount != 5 {
		t.Errorf("access count = %d, want 5", got.AccessCount)
	}
	if got.Signal != "logs" {
		t.Errorf("signal = %q, want %q", got.Signal, "logs")
	}
	if len(got.TraceIDs) != 2 {
		t.Errorf("trace_ids len = %d, want 2", len(got.TraceIDs))
	}
}

func TestSnapshot_LoadMissing(t *testing.T) {
	m := NewMetadataMap()
	err := m.LoadSnapshot("/nonexistent/path/smartcache.meta.json")
	if err != nil {
		t.Fatalf("load missing snapshot should return nil, got: %v", err)
	}
	if m.Len() != 0 {
		t.Errorf("expected empty map after loading missing file")
	}
}

func TestMetadataMap_Reconcile(t *testing.T) {
	m := NewMetadataMap()
	m.Set("exists", EntryMeta{Signal: "logs", Size: 100})
	m.Set("orphan", EntryMeta{Signal: "logs", Size: 200})

	diskFiles := map[string]int64{
		"exists":    100,
		"untracked": 300,
	}

	m.Reconcile(diskFiles)

	if _, ok := m.Get("orphan"); ok {
		t.Error("expected orphan to be removed during reconciliation")
	}

	got, ok := m.Get("exists")
	if !ok {
		t.Fatal("expected exists to survive reconciliation")
	}
	if got.Size != 100 {
		t.Errorf("existing entry size = %d, want 100", got.Size)
	}

	got, ok = m.Get("untracked")
	if !ok {
		t.Fatal("expected untracked to be added during reconciliation")
	}
	if got.Size != 300 {
		t.Errorf("untracked size = %d, want 300", got.Size)
	}
}
