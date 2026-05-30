package parquets3

import (
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/smartcache"
)

// testL1 is a minimal L1 cache for testing.
type testL1 struct {
	data map[string][]byte
}

func newTestL1() *testL1                              { return &testL1{data: make(map[string][]byte)} }
func (t *testL1) Get(key string) ([]byte, bool)       { v, ok := t.data[key]; return v, ok }
func (t *testL1) Put(key string, val []byte)          { t.data[key] = val }
func (t *testL1) PutNoCopy(key string, val []byte)    { t.data[key] = val }

// testL2 is a minimal L2 cache for testing.
type testL2 struct {
	data map[string][]byte
}

func newTestL2() *testL2                            { return &testL2{data: make(map[string][]byte)} }
func (t *testL2) Get(key string) ([]byte, bool)     { v, ok := t.data[key]; return v, ok }
func (t *testL2) Put(key string, data []byte) error { t.data[key] = data; return nil }
func (t *testL2) Delete(key string)                 { delete(t.data, key) }
func (t *testL2) Size() int64                       { return int64(len(t.data)) }

// testPeerLookup always returns local ownership.
type testPeerLookup struct{}

func (t *testPeerLookup) Lookup(key string) (string, bool) { return "self", true }
func (t *testPeerLookup) Members() []string                { return []string{"self"} }
func (t *testPeerLookup) MemberCount() int                 { return 1 }

func newTestController(l2 *testL2) *smartcache.Controller {
	return smartcache.NewController(smartcache.ControllerConfig{
		L1:         newTestL1(),
		L2:         l2,
		PeerLookup: &testPeerLookup{},
		Metadata:   smartcache.NewMetadataMap(),
		Signal:     "traces",
	})
}

// makeTestTraceParquetData creates a small Parquet file from trace rows for testing.
func makeTestTraceParquetData(t *testing.T) ([]byte, string) {
	t.Helper()
	rows := []schema.TraceRow{
		{
			TimestampUnixNano: time.Now().UnixNano(),
			StartTimeUnixNano: time.Now().UnixNano(),
			TraceID:           "trace-aaa",
			SpanID:            "span-001",
			SpanName:          "GET /api/users",
			ServiceName:       "svc-a",
			DurationNs:        42000,
		},
		{
			TimestampUnixNano: time.Now().UnixNano(),
			StartTimeUnixNano: time.Now().UnixNano(),
			TraceID:           "trace-bbb",
			SpanID:            "span-002",
			SpanName:          "POST /api/orders",
			ServiceName:       "svc-b",
			DurationNs:        99000,
		},
	}
	result, err := writeTracesParquet(rows, 1000, 1)
	if err != nil {
		t.Fatalf("writeTracesParquet: %v", err)
	}
	return result.Data, "traces/dt=2026-05-24/hour=10/test.parquet"
}

func TestCacheOnFlush_NilCache_Noop(t *testing.T) {
	// Calling cacheOnFlush with nil controller must not panic.
	data, fileKey := makeTestTraceParquetData(t)
	cacheOnFlush(nil, fileKey, data)
	// Also test with nil data + nil controller.
	cacheOnFlush(nil, "any-key", nil)
	// Also test with nil data + real controller.
	l2 := newTestL2()
	sc := newTestController(l2)
	cacheOnFlush(sc, "any-key", nil)
	if len(l2.data) != 0 {
		t.Errorf("expected no cache entries for nil data, got %d", len(l2.data))
	}
}

func TestCacheOnFlush_StoresColumnChunks(t *testing.T) {
	l2 := newTestL2()
	sc := newTestController(l2)
	data, fileKey := makeTestTraceParquetData(t)

	cacheOnFlush(sc, fileKey, data)

	if len(l2.data) == 0 {
		t.Fatal("expected L2 cache entries after cacheOnFlush, got 0")
	}

	// Verify that cache keys use ChunkCacheKey format.
	for key := range l2.data {
		// Keys should contain the file key, a column name, and row group index.
		if len(key) == 0 {
			t.Errorf("empty cache key found")
		}
		t.Logf("cached key: %s (size=%d)", key, len(l2.data[key]))
	}

	// Check that known trace columns are cached.
	expectedCols := []string{"span.name", "service.name", "timestamp_unix_nano", "trace_id", "span_id"}
	for _, col := range expectedCols {
		cacheKey := smartcache.ChunkCacheKey{
			FileKey:  fileKey,
			Column:   col,
			RowGroup: 0,
		}.String()
		if _, ok := l2.data[cacheKey]; !ok {
			t.Errorf("expected column %q to be cached with key %q", col, cacheKey)
		}
	}
}

func TestCacheOnFlush_InvalidParquetData(t *testing.T) {
	l2 := newTestL2()
	sc := newTestController(l2)

	// Should not panic on invalid parquet data.
	cacheOnFlush(sc, "bad-key", []byte("not a parquet file"))

	if len(l2.data) != 0 {
		t.Errorf("expected no cache entries for invalid data, got %d", len(l2.data))
	}
}

func TestCacheOnFlush_NoGoroutineLeak(t *testing.T) {
	l2 := newTestL2()
	sc := newTestController(l2)
	data, fileKey := makeTestTraceParquetData(t)

	// Warm up runtime to stabilize goroutine count.
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	for i := 0; i < 1000; i++ {
		cacheOnFlush(sc, fileKey, data)
	}

	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Allow some slack for runtime goroutines.
	if after > baseline+5 {
		t.Errorf("goroutine leak: baseline=%d after=%d (delta=%d, max allowed=5)",
			baseline, after, after-baseline)
	}
}

func TestCacheOnFlush_NoMemoryLeak(t *testing.T) {
	l2 := newTestL2()
	sc := newTestController(l2)
	data, fileKey := makeTestTraceParquetData(t)

	// Force GC and measure baseline.
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := 0; i < 1000; i++ {
		cacheOnFlush(sc, fileKey, data)
	}

	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// The L2 map holds the same keys repeatedly (overwritten), so growth
	// should be bounded. Allow up to 10MB.
	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	const maxGrowthBytes = 10 * 1024 * 1024 // 10MB
	if growth > maxGrowthBytes {
		t.Errorf("memory leak: growth=%d bytes (limit=%d)", growth, maxGrowthBytes)
	}
	t.Logf("memory growth after 1000 calls: %d bytes (%.2f MB)", growth, float64(growth)/1024/1024)
}

func TestCacheOnFlush_EmptyFileKey(t *testing.T) {
	l2 := newTestL2()
	sc := newTestController(l2)
	data, _ := makeTestTraceParquetData(t)

	// Empty file key should still work (keys will have empty prefix).
	cacheOnFlush(sc, "", data)

	if len(l2.data) == 0 {
		t.Fatal("expected cache entries even with empty file key")
	}
}
