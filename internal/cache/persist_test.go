package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
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

// --- New per-tenant cardinality tests ---

func TestLabelIndexExistingBehavior(t *testing.T) {
	// Pin existing Add/GetFieldNames/GetFieldValues behavior
	idx := NewLabelIndex()
	idx.Add("service.name", []string{"api", "web", "worker"})
	idx.Add("level", []string{"info", "error"})

	names := idx.GetFieldNames()
	sort.Strings(names)
	if len(names) != 2 {
		t.Fatalf("field names len = %d, want 2", len(names))
	}
	if names[0] != "level" || names[1] != "service.name" {
		t.Errorf("names = %v, want [level service.name]", names)
	}

	vals := idx.GetFieldValues("service.name", 0)
	if len(vals) != 3 {
		t.Errorf("service.name values = %d, want 3", len(vals))
	}

	// Add merges and deduplicates
	idx.Add("service.name", []string{"web", "db"})
	vals = idx.GetFieldValues("service.name", 0)
	if len(vals) != 4 {
		t.Errorf("after merge, values = %d, want 4 (api,web,worker,db)", len(vals))
	}

	// Limit works
	vals = idx.GetFieldValues("service.name", 2)
	if len(vals) != 2 {
		t.Errorf("limited values = %d, want 2", len(vals))
	}

	// Missing field returns nil
	vals = idx.GetFieldValues("nonexistent", 0)
	if vals != nil {
		t.Errorf("expected nil for missing field, got %v", vals)
	}

	// Len
	if idx.Len() != 2 {
		t.Errorf("len = %d, want 2", idx.Len())
	}
}

func TestLabelInfoPerTenantCardinality(t *testing.T) {
	idx := NewLabelIndex()

	// Tenant A adds 3 unique values
	idx.AddWithTenant("service.name", []string{"api", "web", "worker"}, "tenant-a")
	// Tenant B adds 2 unique values (one overlaps globally)
	idx.AddWithTenant("service.name", []string{"web", "db"}, "tenant-b")

	li := idx.GetLabelInfo("service.name")
	if li == nil {
		t.Fatal("expected label info for service.name")
	}

	// Global cardinality = 4 (api, web, worker, db)
	if li.Cardinality != 4 {
		t.Errorf("cardinality = %d, want 4", li.Cardinality)
	}

	// Per-tenant cardinality
	if li.PerTenant == nil {
		t.Fatal("PerTenant should not be nil")
	}
	if li.PerTenant["tenant-a"] != 3 {
		t.Errorf("tenant-a cardinality = %d, want 3", li.PerTenant["tenant-a"])
	}
	if li.PerTenant["tenant-b"] != 2 {
		t.Errorf("tenant-b cardinality = %d, want 2", li.PerTenant["tenant-b"])
	}
}

func TestLabelIndexAddWithTenantBackwardCompat(t *testing.T) {
	idx := NewLabelIndex()

	// Plain Add (no tenant) should still work
	idx.Add("svc", []string{"a", "b", "c"})

	li := idx.GetLabelInfo("svc")
	if li == nil {
		t.Fatal("expected label info for svc")
	}
	if li.Cardinality != 3 {
		t.Errorf("cardinality = %d, want 3", li.Cardinality)
	}

	// PerTenant should be nil or empty when using plain Add
	if len(li.PerTenant) != 0 {
		t.Errorf("PerTenant should be nil/empty after plain Add, got %v", li.PerTenant)
	}

	// Mix: plain Add then AddWithTenant
	idx.AddWithTenant("svc", []string{"c", "d"}, "t1")
	li = idx.GetLabelInfo("svc")
	if li.Cardinality != 4 {
		t.Errorf("cardinality = %d, want 4 (a,b,c,d)", li.Cardinality)
	}
	if li.PerTenant["t1"] != 2 {
		t.Errorf("t1 cardinality = %d, want 2", li.PerTenant["t1"])
	}
}

func TestLabelIndexPersistRoundTripWithPerTenant(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	idx := NewLabelIndex()
	idx.AddWithTenant("host", []string{"h1", "h2", "h3"}, "tenant-a")
	idx.AddWithTenant("host", []string{"h2", "h4"}, "tenant-b")
	idx.Add("level", []string{"info", "warn"})

	if err := p.SaveLabelIndex(idx); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.LoadLabelIndex()
	if err != nil {
		t.Fatal(err)
	}

	if loaded.Len() != 2 {
		t.Fatalf("loaded len = %d, want 2", loaded.Len())
	}

	// Check host field per-tenant preserved
	li := loaded.GetLabelInfo("host")
	if li == nil {
		t.Fatal("expected host label info")
	}
	if li.Cardinality != 4 {
		t.Errorf("host cardinality = %d, want 4", li.Cardinality)
	}
	if li.PerTenant["tenant-a"] != 3 {
		t.Errorf("loaded tenant-a = %d, want 3", li.PerTenant["tenant-a"])
	}
	if li.PerTenant["tenant-b"] != 2 {
		t.Errorf("loaded tenant-b = %d, want 2", li.PerTenant["tenant-b"])
	}

	// Check level has no per-tenant
	li = loaded.GetLabelInfo("level")
	if li == nil {
		t.Fatal("expected level label info")
	}
	if len(li.PerTenant) != 0 {
		t.Errorf("level PerTenant should be empty, got %v", li.PerTenant)
	}

	// Verify values survived
	vals := loaded.GetFieldValues("host", 0)
	if len(vals) != 4 {
		t.Errorf("host values = %d, want 4", len(vals))
	}
}

func TestLabelIndexAddWithTenantConcurrent(t *testing.T) {
	idx := NewLabelIndex()

	var wg sync.WaitGroup
	tenants := []string{"t1", "t2", "t3", "t4"}
	for _, tenant := range tenants {
		wg.Add(1)
		go func(tn string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				idx.AddWithTenant("field", []string{tn + "-val"}, tn)
			}
		}(tenant)
	}
	wg.Wait()

	li := idx.GetLabelInfo("field")
	if li == nil {
		t.Fatal("expected label info")
	}

	// 4 unique values: t1-val, t2-val, t3-val, t4-val
	if li.Cardinality != 4 {
		t.Errorf("cardinality = %d, want 4", li.Cardinality)
	}

	// Each tenant should have cardinality 1
	for _, tn := range tenants {
		if li.PerTenant[tn] != 1 {
			t.Errorf("tenant %s cardinality = %d, want 1", tn, li.PerTenant[tn])
		}
	}
}

func TestLabelIndexAddWithTenantDedup(t *testing.T) {
	idx := NewLabelIndex()

	// Same tenant, same values multiple times
	idx.AddWithTenant("svc", []string{"a", "b"}, "t1")
	idx.AddWithTenant("svc", []string{"a", "b"}, "t1")
	idx.AddWithTenant("svc", []string{"a", "b", "c"}, "t1")

	li := idx.GetLabelInfo("svc")
	if li == nil {
		t.Fatal("expected label info")
	}

	// Global cardinality: a, b, c = 3
	if li.Cardinality != 3 {
		t.Errorf("cardinality = %d, want 3", li.Cardinality)
	}

	// Per-tenant cardinality should be max seen = 3 (from the last call with a,b,c)
	if li.PerTenant["t1"] != 3 {
		t.Errorf("t1 cardinality = %d, want 3", li.PerTenant["t1"])
	}
}

func TestLabelIndexAddWithTenantMultipleFields(t *testing.T) {
	idx := NewLabelIndex()

	idx.AddWithTenant("host", []string{"h1", "h2"}, "t1")
	idx.AddWithTenant("host", []string{"h3"}, "t2")
	idx.AddWithTenant("level", []string{"info", "warn", "error"}, "t1")
	idx.AddWithTenant("level", []string{"info", "debug"}, "t2")

	// Check host
	li := idx.GetLabelInfo("host")
	if li == nil {
		t.Fatal("expected host label info")
	}
	if li.Cardinality != 3 {
		t.Errorf("host cardinality = %d, want 3", li.Cardinality)
	}
	if li.PerTenant["t1"] != 2 {
		t.Errorf("host t1 = %d, want 2", li.PerTenant["t1"])
	}
	if li.PerTenant["t2"] != 1 {
		t.Errorf("host t2 = %d, want 1", li.PerTenant["t2"])
	}

	// Check level
	li = idx.GetLabelInfo("level")
	if li == nil {
		t.Fatal("expected level label info")
	}
	if li.Cardinality != 4 {
		t.Errorf("level cardinality = %d, want 4", li.Cardinality)
	}
	if li.PerTenant["t1"] != 3 {
		t.Errorf("level t1 = %d, want 3", li.PerTenant["t1"])
	}
	if li.PerTenant["t2"] != 2 {
		t.Errorf("level t2 = %d, want 2", li.PerTenant["t2"])
	}
}

func TestLabelIndexGetLabelInfoNonExistent(t *testing.T) {
	idx := NewLabelIndex()
	li := idx.GetLabelInfo("nonexistent")
	if li != nil {
		t.Errorf("expected nil for nonexistent field, got %+v", li)
	}
}

func TestLabelIndexGetAllLabelInfo(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("a", []string{"v1"})
	idx.Add("b", []string{"v2", "v3"})
	idx.AddWithTenant("c", []string{"v4"}, "t1")

	all := idx.GetAllLabelInfo()
	if len(all) != 3 {
		t.Fatalf("GetAllLabelInfo len = %d, want 3", len(all))
	}

	// Collect names
	names := make([]string, len(all))
	for i, li := range all {
		names[i] = li.Name
	}
	sort.Strings(names)
	if names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Errorf("names = %v, want [a b c]", names)
	}
}

func TestLabelIndexValuesCapped(t *testing.T) {
	idx := NewLabelIndex()

	// Create 10001 unique values
	vals := make([]string, 10001)
	for i := range vals {
		vals[i] = fmt.Sprintf("val-%d", i)
	}
	idx.AddWithTenant("big", vals, "t1")

	li := idx.GetLabelInfo("big")
	if li == nil {
		t.Fatal("expected label info")
	}
	if li.Cardinality > 10000 {
		t.Errorf("cardinality = %d, should be capped at 10000", li.Cardinality)
	}
	if len(li.Values) > 10000 {
		t.Errorf("values len = %d, should be capped at 10000", len(li.Values))
	}

	// Per-tenant should reflect the number of unique values in the input
	// (up to the cap since we can only track what's stored)
	if li.PerTenant["t1"] < 10000 {
		t.Errorf("t1 cardinality = %d, want at least 10000", li.PerTenant["t1"])
	}
}
