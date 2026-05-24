package smartcache

import (
	"runtime"
	"strings"
	"testing"
)

func TestChunkCacheKey_Format(t *testing.T) {
	key := ChunkCacheKey{
		FileKey:  "tenant/2024/01/data.parquet",
		Column:   "_msg",
		RowGroup: 3,
	}

	got := key.String()
	want := "tenant/2024/01/data.parquet:_msg:3"

	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestChunkCacheKey_DifferentColumnsAreDifferentKeys(t *testing.T) {
	base := ChunkCacheKey{
		FileKey:  "file.parquet",
		Column:   "_msg",
		RowGroup: 0,
	}
	other := ChunkCacheKey{
		FileKey:  "file.parquet",
		Column:   "_time",
		RowGroup: 0,
	}

	if base.String() == other.String() {
		t.Errorf("different columns produced same key: %q", base.String())
	}
}

func TestChunkCacheKey_DifferentRowGroupsAreDifferentKeys(t *testing.T) {
	base := ChunkCacheKey{
		FileKey:  "file.parquet",
		Column:   "_msg",
		RowGroup: 0,
	}
	other := ChunkCacheKey{
		FileKey:  "file.parquet",
		Column:   "_msg",
		RowGroup: 1,
	}

	if base.String() == other.String() {
		t.Errorf("different row groups produced same key: %q", base.String())
	}
}

func TestChunkCacheKey_EmptyColumn(t *testing.T) {
	key := ChunkCacheKey{
		FileKey:  "file.parquet",
		Column:   "",
		RowGroup: 0,
	}

	got := key.String()
	want := "file.parquet::0"

	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestChunkCacheKey_LongFileKey(t *testing.T) {
	longKey := strings.Repeat("a", 1000)
	key := ChunkCacheKey{
		FileKey:  longKey,
		Column:   "col",
		RowGroup: 42,
	}

	got := key.String()

	if !strings.HasPrefix(got, longKey+":") {
		t.Errorf("long file key not preserved in String() output")
	}
	if !strings.HasSuffix(got, ":col:42") {
		t.Errorf("suffix mismatch: got %q", got[len(got)-10:])
	}
}

func TestChunkCacheKey_NoGoroutineLeak(t *testing.T) {
	// Warm up the runtime to stabilize goroutine count.
	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < 10000; i++ {
		key := ChunkCacheKey{
			FileKey:  "file.parquet",
			Column:   "_msg",
			RowGroup: i,
		}
		_ = key.String()
	}

	after := runtime.NumGoroutine()

	// Allow a small delta for runtime variance; creating keys must not spawn goroutines.
	if diff := after - before; diff > 5 {
		t.Errorf("goroutine leak: before=%d after=%d delta=%d", before, after, diff)
	}
}

func TestChunkCacheKey_NoMemoryLeak(t *testing.T) {
	// Force GC and measure baseline.
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := 0; i < 100_000; i++ {
		key := ChunkCacheKey{
			FileKey:  "file.parquet",
			Column:   "_msg",
			RowGroup: i,
		}
		_ = key.String()
	}

	// Force GC so discarded keys are collected.
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// HeapInuse should not grow by more than 10 MB.
	const maxGrowth = 10 * 1024 * 1024
	growth := int64(after.HeapInuse) - int64(before.HeapInuse)

	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes (limit %d)", growth, maxGrowth)
	}
}
