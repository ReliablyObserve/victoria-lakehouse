package parquets3

import (
	"runtime"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func makeFiles(keys ...string) []manifest.FileInfo {
	files := make([]manifest.FileInfo, len(keys))
	for i, k := range keys {
		files[i] = manifest.FileInfo{Key: k}
	}
	return files
}

func fileKeys(files []manifest.FileInfo) []string {
	keys := make([]string, len(files))
	for i, f := range files {
		keys[i] = f.Key
	}
	return keys
}

func TestSortFilesByCacheAffinity_CachedFirst(t *testing.T) {
	files := makeFiles("a", "b", "c")
	cached := map[string]bool{"c": true}

	sortFilesByCacheAffinity(files, cached)

	keys := fileKeys(files)
	if keys[0] != "c" {
		t.Fatalf("expected cached file 'c' first, got order: %v", keys)
	}
	// Non-cached files preserve relative order.
	if keys[1] != "a" || keys[2] != "b" {
		t.Fatalf("expected non-cached order [a, b], got: %v", keys[1:])
	}
}

func TestSortFilesByCacheAffinity_AllCached(t *testing.T) {
	files := makeFiles("x", "y", "z")
	cached := map[string]bool{"x": true, "y": true, "z": true}

	sortFilesByCacheAffinity(files, cached)

	keys := fileKeys(files)
	// Stable sort: order must be preserved when all are cached.
	expected := []string{"x", "y", "z"}
	for i, k := range keys {
		if k != expected[i] {
			t.Fatalf("expected order %v, got %v", expected, keys)
		}
	}
}

func TestSortFilesByCacheAffinity_NoneCached(t *testing.T) {
	files := makeFiles("a", "b", "c")
	cached := map[string]bool{}

	sortFilesByCacheAffinity(files, cached)

	keys := fileKeys(files)
	expected := []string{"a", "b", "c"}
	for i, k := range keys {
		if k != expected[i] {
			t.Fatalf("expected order %v, got %v", expected, keys)
		}
	}
}

func TestSortFilesByCacheAffinity_MixedOrder(t *testing.T) {
	files := makeFiles("a", "b", "c", "d", "e")
	cached := map[string]bool{"b": true, "d": true}

	sortFilesByCacheAffinity(files, cached)

	keys := fileKeys(files)

	// Cached files must come first, preserving their relative order.
	if keys[0] != "b" || keys[1] != "d" {
		t.Fatalf("expected cached files [b, d] at front, got: %v", keys)
	}
	// Non-cached files follow, preserving their relative order.
	if keys[2] != "a" || keys[3] != "c" || keys[4] != "e" {
		t.Fatalf("expected non-cached files [a, c, e] at end, got: %v", keys[2:])
	}
}

func TestSortFilesByCacheAffinity_Empty(t *testing.T) {
	var files []manifest.FileInfo
	cached := map[string]bool{"x": true}

	// Must not panic on empty slice.
	sortFilesByCacheAffinity(files, cached)

	if len(files) != 0 {
		t.Fatalf("expected empty slice, got %d items", len(files))
	}
}

func TestSortFilesByCacheAffinity_SingleFile(t *testing.T) {
	files := makeFiles("only")
	cached := map[string]bool{"only": true}

	sortFilesByCacheAffinity(files, cached)

	if files[0].Key != "only" {
		t.Fatalf("expected 'only', got %q", files[0].Key)
	}

	// Single non-cached file.
	files2 := makeFiles("solo")
	sortFilesByCacheAffinity(files2, map[string]bool{})
	if files2[0].Key != "solo" {
		t.Fatalf("expected 'solo', got %q", files2[0].Key)
	}
}

func TestSortFilesByCacheAffinity_NoGoroutineLeak(t *testing.T) {
	// sort.SliceStable is synchronous, so goroutine count should stay stable.
	before := runtime.NumGoroutine()

	n := 10_000
	files := make([]manifest.FileInfo, n)
	cached := make(map[string]bool, n/2)
	for i := 0; i < n; i++ {
		key := "file-" + string(rune('A'+i%26)) + "-" + string(rune('0'+i%10))
		files[i] = manifest.FileInfo{Key: key}
		if i%2 == 0 {
			cached[key] = true
		}
	}

	sortFilesByCacheAffinity(files, cached)

	after := runtime.NumGoroutine()
	// Allow a small margin for GC / runtime goroutines.
	if after > before+5 {
		t.Fatalf("goroutine leak: before=%d, after=%d", before, after)
	}

	// Verify invariant: all cached files before all non-cached.
	seenNonCached := false
	for _, f := range files {
		if cached[f.Key] {
			if seenNonCached {
				t.Fatal("cached file found after non-cached file")
			}
		} else {
			seenNonCached = true
		}
	}
}

func TestSortFilesByCacheAffinity_NoMemoryLeak(t *testing.T) {
	n := 100_000
	files := make([]manifest.FileInfo, n)
	cached := make(map[string]bool, n/3)
	for i := 0; i < n; i++ {
		key := "key-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		files[i] = manifest.FileInfo{Key: key}
		if i%3 == 0 {
			cached[key] = true
		}
	}

	// Force GC and measure baseline.
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Repeat sort 50 times to amplify any leak.
	for iter := 0; iter < 50; iter++ {
		sortFilesByCacheAffinity(files, cached)
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// The sort is in-place with no allocations, so heap growth should be minimal.
	growth := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)
	const maxGrowth = 10 * 1024 * 1024 // 10 MB
	if growth > maxGrowth {
		t.Fatalf("memory growth %d bytes exceeds %d byte limit", growth, maxGrowth)
	}
}

func TestFooterCache_Has(t *testing.T) {
	fc := NewFooterCache(10)

	if fc.Has("missing") {
		t.Fatal("expected Has to return false for missing key")
	}

	fc.Put("present", &CachedFooter{FileSize: 100})
	if !fc.Has("present") {
		t.Fatal("expected Has to return true for present key")
	}

	fc.Remove("present")
	if fc.Has("present") {
		t.Fatal("expected Has to return false after removal")
	}
}
