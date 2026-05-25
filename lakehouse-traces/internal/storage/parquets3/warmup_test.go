package parquets3

import (
	"runtime"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// mockOwnershipChecker implements OwnershipChecker for tests.
type mockOwnershipChecker struct {
	owned map[string]bool
}

func (m *mockOwnershipChecker) IsLocal(key string) bool {
	return m.owned[key]
}

func TestFilterOwnedFiles_NilChecker_ReturnsAll(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
		{Key: "c.parquet"},
	}
	got := filterOwnedFiles(files, nil)
	if len(got) != len(files) {
		t.Fatalf("nil checker: got %d files, want %d", len(got), len(files))
	}
	for i := range files {
		if got[i].Key != files[i].Key {
			t.Errorf("index %d: got %q, want %q", i, got[i].Key, files[i].Key)
		}
	}
}

func TestFilterOwnedFiles_AllOwned(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
		{Key: "c.parquet"},
	}
	checker := &mockOwnershipChecker{
		owned: map[string]bool{
			"a.parquet": true,
			"b.parquet": true,
			"c.parquet": true,
		},
	}
	got := filterOwnedFiles(files, checker)
	if len(got) != 3 {
		t.Fatalf("all owned: got %d files, want 3", len(got))
	}
}

func TestFilterOwnedFiles_NoneOwned(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
		{Key: "c.parquet"},
	}
	checker := &mockOwnershipChecker{
		owned: map[string]bool{},
	}
	got := filterOwnedFiles(files, checker)
	if len(got) != 0 {
		t.Fatalf("none owned: got %d files, want 0", len(got))
	}
}

func TestFilterOwnedFiles_MixedOwnership(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "hour/01/file1.parquet"},
		{Key: "hour/01/file2.parquet"},
		{Key: "hour/02/file3.parquet"},
		{Key: "hour/02/file4.parquet"},
	}
	checker := &mockOwnershipChecker{
		owned: map[string]bool{
			"hour/01/file1.parquet": true,
			"hour/02/file3.parquet": true,
		},
	}
	got := filterOwnedFiles(files, checker)
	if len(got) != 2 {
		t.Fatalf("mixed: got %d files, want 2", len(got))
	}
	if got[0].Key != "hour/01/file1.parquet" {
		t.Errorf("got[0].Key = %q, want hour/01/file1.parquet", got[0].Key)
	}
	if got[1].Key != "hour/02/file3.parquet" {
		t.Errorf("got[1].Key = %q, want hour/02/file3.parquet", got[1].Key)
	}
}

func TestFilterOwnedFiles_EmptyFiles(t *testing.T) {
	got := filterOwnedFiles(nil, &mockOwnershipChecker{owned: map[string]bool{}})
	if len(got) != 0 {
		t.Fatalf("empty input: got %d files, want 0", len(got))
	}
	got = filterOwnedFiles([]manifest.FileInfo{}, &mockOwnershipChecker{owned: map[string]bool{}})
	if len(got) != 0 {
		t.Fatalf("empty slice: got %d files, want 0", len(got))
	}
}

func TestFilterOwnedFiles_PreservesOrder(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "z.parquet"},
		{Key: "a.parquet"},
		{Key: "m.parquet"},
		{Key: "b.parquet"},
		{Key: "y.parquet"},
	}
	checker := &mockOwnershipChecker{
		owned: map[string]bool{
			"z.parquet": true,
			"m.parquet": true,
			"y.parquet": true,
		},
	}
	got := filterOwnedFiles(files, checker)
	if len(got) != 3 {
		t.Fatalf("order: got %d files, want 3", len(got))
	}
	expected := []string{"z.parquet", "m.parquet", "y.parquet"}
	for i, want := range expected {
		if got[i].Key != want {
			t.Errorf("order[%d]: got %q, want %q", i, got[i].Key, want)
		}
	}
}

func TestFilterOwnedFiles_NoGoroutineLeak(t *testing.T) {
	// Record baseline goroutine count
	runtime.GC()
	baseline := runtime.NumGoroutine()

	files := make([]manifest.FileInfo, 10_000)
	owned := make(map[string]bool, 5_000)
	for i := range files {
		key := "file" + string(rune('A'+i%26)) + "_" + string(rune('0'+i/26%10)) + ".parquet"
		files[i] = manifest.FileInfo{Key: key}
		if i%2 == 0 {
			owned[key] = true
		}
	}
	checker := &mockOwnershipChecker{owned: owned}

	for i := 0; i < 100; i++ {
		_ = filterOwnedFiles(files, checker)
	}

	runtime.GC()
	after := runtime.NumGoroutine()
	// Allow small variance (test framework, GC, etc.)
	if after > baseline+5 {
		t.Fatalf("goroutine leak: baseline=%d, after=%d (delta=%d)", baseline, after, after-baseline)
	}
}

func TestFilterOwnedFiles_NoMemoryLeak(t *testing.T) {
	files := make([]manifest.FileInfo, 100_000)
	owned := make(map[string]bool, 50_000)
	for i := range files {
		key := "partition/hour/" + string(rune('a'+i%26)) + "/" + string(rune('0'+i/1000%10)) + "_" + string(rune('0'+i%10)) + ".parquet"
		files[i] = manifest.FileInfo{Key: key}
		if i%2 == 0 {
			owned[key] = true
		}
	}
	checker := &mockOwnershipChecker{owned: owned}

	// Warm up and stabilize allocations
	for i := 0; i < 10; i++ {
		_ = filterOwnedFiles(files, checker)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := 0; i < 50; i++ {
		result := filterOwnedFiles(files, checker)
		_ = result
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// HeapInuse is the bytes in in-use spans. Growth should be minimal
	// since filterOwnedFiles only allocates the result slice (which gets GC'd).
	growth := int64(after.HeapInuse) - int64(before.HeapInuse)
	const maxGrowthBytes = 10 * 1024 * 1024 // 10 MB
	if growth > maxGrowthBytes {
		t.Fatalf("memory leak: heap grew %d bytes (%.1f MB), limit %d bytes (%.1f MB)",
			growth, float64(growth)/(1024*1024), maxGrowthBytes, float64(maxGrowthBytes)/(1024*1024))
	}
}
