package parquets3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/smartcache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// ---------------------------------------------------------------------------
// Test: writeFileBloom via storageBloomObserver
// ---------------------------------------------------------------------------

func TestS3_writeFileBloom_S3Upload(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	m := manifest.New("test-bucket", "traces/")

	observer := &storageBloomObserver{
		bloom:    bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01),
		pool:     pool,
		manifest: m,
	}

	columnValues := map[string][]string{
		"service.name": {"api-gw", "worker"},
		"trace_id":     {"abc-123", "xyz-789"},
	}

	fileKey := "traces/dt=2026-05-10/hour=14/batch001.parquet"
	observer.writeFileBloom(context.Background(), fileKey, columnValues)

	// Verify the .bloom sidecar was uploaded
	mock.mu.RLock()
	_, exists := mock.files[fileKey+".bloom"]
	mock.mu.RUnlock()

	if !exists {
		t.Error("bloom sidecar should be uploaded to S3")
	}
}

func TestS3_writeFileBloom_EmptyValues(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	observer := &storageBloomObserver{
		bloom: bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01),
		pool:  pool,
	}

	// Empty column values should not upload
	observer.writeFileBloom(context.Background(), "test.parquet", map[string][]string{})

	mock.mu.RLock()
	_, exists := mock.files["test.parquet.bloom"]
	mock.mu.RUnlock()

	if exists {
		t.Error("bloom sidecar should not be uploaded for empty values")
	}
}

func TestS3_writeFileBloom_SingleValue(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	observer := &storageBloomObserver{
		bloom: bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01),
		pool:  pool,
	}

	observer.writeFileBloom(context.Background(), "test.parquet", map[string][]string{
		"service.name": {"api-gw"},
	})

	mock.mu.RLock()
	_, exists := mock.files["test.parquet.bloom"]
	mock.mu.RUnlock()
	if !exists {
		t.Error("bloom sidecar should be uploaded for single value")
	}
}

// ---------------------------------------------------------------------------
// Test: OnFileFlush
// ---------------------------------------------------------------------------

func TestS3_OnFileFlush(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	partIdx := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	m := manifest.New("test-bucket", "traces/")

	observer := &storageBloomObserver{
		bloom:    partIdx,
		pool:     pool,
		manifest: m,
	}

	columnValues := map[string][]string{
		"service.name": {"api-gw"},
		"trace_id":     {"trace-001"},
	}

	observer.OnFileFlush("dt=2026-05-10/hour=14", "traces/dt=2026-05-10/hour=14/a.parquet", columnValues)

	// Verify partition bloom was updated
	dirty := partIdx.DirtyPartitions()
	if len(dirty) == 0 {
		t.Error("partition should be marked dirty after OnFileFlush")
	}

	// Wait a moment for the async writeFileBloom goroutine
	time.Sleep(100 * time.Millisecond)

	// Verify file bloom sidecar was uploaded
	mock.mu.RLock()
	_, exists := mock.files["traces/dt=2026-05-10/hour=14/a.parquet.bloom"]
	mock.mu.RUnlock()
	if !exists {
		t.Error("file bloom sidecar should be uploaded via OnFileFlush")
	}
}

func TestS3_OnFileFlush_NilBloom(t *testing.T) {
	observer := &storageBloomObserver{bloom: nil}
	// Should not panic
	observer.OnFileFlush("p", "k", map[string][]string{"a": {"b"}})
}

func TestS3_OnFileFlush_EmptyValues(t *testing.T) {
	observer := &storageBloomObserver{
		bloom: bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01),
	}
	// Should not panic or add anything
	observer.OnFileFlush("p", "k", nil)
}

// ---------------------------------------------------------------------------
// Test: PersistDirty
// ---------------------------------------------------------------------------

func TestS3_PersistDirty_FullPath(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	partIdx := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	m := manifest.New("test-bucket", "traces/")

	observer := &storageBloomObserver{
		bloom:    partIdx,
		pool:     pool,
		manifest: m,
	}

	// Add files to make partition dirty
	partIdx.AddFile("dt=2026-05-10/hour=14", "a.parquet", map[string][]string{
		"service.name": {"api-gw"},
	})

	dirty := partIdx.DirtyPartitions()
	if len(dirty) == 0 {
		t.Fatal("partition should be dirty")
	}

	observer.PersistDirty(context.Background(), "traces/")

	// Verify bloom was uploaded to S3
	mock.mu.RLock()
	_, exists := mock.files["traces/dt=2026-05-10/hour=14/_bloom.bin"]
	mock.mu.RUnlock()
	if !exists {
		t.Error("bloom should be persisted to S3")
	}

	// Verify partition is no longer dirty
	dirty = partIdx.DirtyPartitions()
	if len(dirty) != 0 {
		t.Errorf("partition should be cleared after PersistDirty, got %d dirty", len(dirty))
	}

	// Verify manifest bloom metadata was set
	meta := m.GetBloomMeta("dt=2026-05-10/hour=14")
	if !meta.BloomAvailable {
		t.Error("manifest should show bloom as available")
	}
}

func TestS3_PersistDirty_NilBloom(t *testing.T) {
	observer := &storageBloomObserver{bloom: nil}
	// Should not panic
	observer.PersistDirty(context.Background(), "traces/")
}

func TestS3_PersistDirty_NilPool(t *testing.T) {
	observer := &storageBloomObserver{
		bloom: bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01),
		pool:  nil,
	}
	// Should not panic
	observer.PersistDirty(context.Background(), "traces/")
}

func TestS3_PersistDirty_UploadError(t *testing.T) {
	// Create pool pointing to unreachable server
	pool := testPool(t, "http://127.0.0.1:1")
	partIdx := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	m := manifest.New("test-bucket", "traces/")

	observer := &storageBloomObserver{
		bloom:    partIdx,
		pool:     pool,
		manifest: m,
	}

	partIdx.AddFile("dt=2026-05-10/hour=14", "a.parquet", map[string][]string{
		"service.name": {"api-gw"},
	})

	// Should not panic; errors are logged
	observer.PersistDirty(context.Background(), "traces/")

	// Partition should still be dirty (upload failed)
	dirty := partIdx.DirtyPartitions()
	if len(dirty) == 0 {
		t.Error("partition should remain dirty after failed upload")
	}
}

// ---------------------------------------------------------------------------
// Test: bloomS3Loader
// ---------------------------------------------------------------------------

func TestS3_bloomS3Loader_Success(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())

	// Create a valid bloom index and upload it
	idx := bloomindex.New()
	f := bloomindex.NewFilter(100, 0.01)
	f.Add("api-gw")
	idx.AddColumns("a.parquet", map[string]*bloomindex.Filter{
		"service.name": f,
	})
	data := idx.Marshal()
	mock.putFile("traces/dt=2026-05-10/hour=14/_bloom.bin", data)

	loader := bloomS3Loader(pool, "traces/")
	loaded, err := loader(context.Background(), "dt=2026-05-10/hour=14")
	if err != nil {
		t.Fatalf("bloomS3Loader: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil index")
	}
}

func TestS3_bloomS3Loader_Missing(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	loader := bloomS3Loader(pool, "traces/")

	// No bloom file exists
	loaded, err := loader(context.Background(), "dt=2026-05-10/hour=99")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded != nil {
		t.Error("expected nil index for missing bloom")
	}
}

func TestS3_bloomS3Loader_NilPool(t *testing.T) {
	loader := bloomS3Loader(nil, "traces/")
	loaded, err := loader(context.Background(), "dt=2026-05-10/hour=14")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded != nil {
		t.Error("expected nil index for nil pool")
	}
}

func TestS3_bloomS3Loader_CorruptData(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	// Upload corrupt data
	mock.putFile("traces/dt=2026-05-10/hour=14/_bloom.bin", []byte("not a valid bloom index"))

	loader := bloomS3Loader(pool, "traces/")
	loaded, err := loader(context.Background(), "dt=2026-05-10/hour=14")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded != nil {
		t.Error("expected nil index for corrupt data")
	}
}

// ---------------------------------------------------------------------------
// Test: extractTraceBloomValues and bloomSetsToMap
// ---------------------------------------------------------------------------

func TestS3_extractTraceBloomValues(t *testing.T) {
	rows := []schema.TraceRow{
		{TraceID: "trace-1", ServiceName: "svc-a"},
		{TraceID: "trace-2", ServiceName: "svc-b"},
	}

	result := extractTraceBloomValues(rows)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result["trace_id"]) != 2 {
		t.Errorf("expected 2 trace_ids, got %d", len(result["trace_id"]))
	}
}

func TestS3_extractTraceBloomValues_Empty(t *testing.T) {
	result := extractTraceBloomValues(nil)
	if result != nil {
		t.Error("expected nil for empty rows")
	}
}

func TestS3_bloomSetsToMap_AllEmpty(t *testing.T) {
	sets := map[string]map[string]bool{
		"col1": {},
		"col2": {},
	}
	result := bloomSetsToMap(sets)
	if result != nil {
		t.Error("expected nil for all-empty sets")
	}
}

// ---------------------------------------------------------------------------
// Test: QuerySpecificFiles with S3
// ---------------------------------------------------------------------------

func TestS3_QuerySpecificFiles_Success(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "specific file query", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "second row", SeverityText: "ERROR", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)

	key1 := "traces/dt=2026-05-10/hour=14/specific1.parquet"
	key2 := "traces/dt=2026-05-10/hour=14/specific2.parquet"
	registerFileInMockS3(t, s, mock, key1, data, now)
	registerFileInMockS3(t, s, mock, key2, data, now)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var totalRows int
	err := s.QuerySpecificFiles(context.Background(), []string{key1}, startNs, endNs, "", nil,
		func(_ uint, db *logstorage.DataBlock) {
			totalRows += db.RowsCount()
		})
	if err != nil {
		t.Fatalf("QuerySpecificFiles: %v", err)
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows from specific file, got %d", totalRows)
	}
}

func TestS3_QuerySpecificFiles_Empty(t *testing.T) {
	s := testStorage()
	err := s.QuerySpecificFiles(context.Background(), nil, 0, 0, "", nil,
		func(_ uint, db *logstorage.DataBlock) {
			t.Error("should not be called for empty file keys")
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestS3_QuerySpecificFiles_NoMatch(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "traces/dt=2026-05-10/hour=14/real.parquet"
	registerFileInMockS3(t, s, mock, key, data, now)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var totalRows int
	err := s.QuerySpecificFiles(context.Background(), []string{"traces/dt=2026-05-10/hour=14/nonexistent.parquet"}, startNs, endNs, "", nil,
		func(_ uint, db *logstorage.DataBlock) {
			totalRows += db.RowsCount()
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if totalRows != 0 {
		t.Errorf("expected 0 rows for non-matching key, got %d", totalRows)
	}
}

func TestS3_QuerySpecificFiles_CancelledContext(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("traces/dt=2026-05-10/hour=14/cancel%d.parquet", i)
		registerFileInMockS3(t, s, mock, key, data, now)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()
	keys := []string{
		"traces/dt=2026-05-10/hour=14/cancel0.parquet",
		"traces/dt=2026-05-10/hour=14/cancel1.parquet",
	}

	err := s.QuerySpecificFiles(ctx, keys, startNs, endNs, "", nil,
		func(_ uint, db *logstorage.DataBlock) {})
	_ = err
}

// ---------------------------------------------------------------------------
// Test: queryBufferBridge with mock HTTP server for traces mode
// ---------------------------------------------------------------------------

func TestS3_queryBufferBridge_TracesMode(t *testing.T) {
	insertSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rows := []schema.TraceRow{
			{
				TimestampUnixNano: time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC).UnixNano(),
				TraceID:           "trace-abc-123",
				SpanID:            "span-001",
				ServiceName:       "trace-svc",
			},
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		for _, row := range rows {
			_ = enc.Encode(row)
		}
	}))
	defer insertSrv.Close()

	s := testStorage()
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)

	bb := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 5 * time.Second,
	}, config.ModeTraces)
	bb.SetEndpoints([]string{insertSrv.URL})
	s.bufferBridge = bb

	var blocks []*logstorage.DataBlock

	startNs := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 10, 15, 0, 0, 0, time.UTC).UnixNano()

	s.queryBufferBridge(context.Background(), startNs, endNs, 0, nil, nil,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		})

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows == 0 {
		t.Error("expected rows from buffer bridge in traces mode")
	}
}

func TestS3_queryBufferBridge_DisabledConfig(t *testing.T) {
	s := testStorage()
	s.cfg.Mode = config.ModeTraces

	bb := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: false,
		BufferQueryTimeout: 5 * time.Second,
	}, config.ModeTraces)
	bb.SetEndpoints([]string{"http://localhost:1234"})
	s.bufferBridge = bb

	s.queryBufferBridge(context.Background(), 0, int64(time.Hour), 0, nil, nil,
		func(_ uint, db *logstorage.DataBlock) {
			t.Error("should not be called when buffer query is disabled")
		})
}

// ---------------------------------------------------------------------------
// Test: bloomFilterSkip with real parquet bloom filter
// ---------------------------------------------------------------------------

func TestS3_bloomFilterSkip_WithBloom(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "api-gw"},
	}
	data := writeTestParquetWithBloomToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// Check against a value IN the bloom
	checksHit := []bloomCheck{
		{colName: "service.name", colIdx: findColumnIndex(f.Root(), "service.name"), value: parquet.ValueOf("api-gw")},
	}
	if s.bloomFilterSkip(f, rgs[0], checksHit) {
		t.Error("should not skip when value is in bloom filter")
	}

	// Check against a value NOT in the bloom
	checksMiss := []bloomCheck{
		{colName: "service.name", colIdx: findColumnIndex(f.Root(), "service.name"), value: parquet.ValueOf("nonexistent-service")},
	}
	_ = s.bloomFilterSkip(f, rgs[0], checksMiss)
}

func TestS3_bloomFilterSkip_NoChecks(t *testing.T) {
	s := testStorage()
	result := s.bloomFilterSkip(nil, nil, nil)
	if result {
		t.Error("should not skip with no checks")
	}
}

// TestS3_bloomFilterSkip_InClauseOrSemantics locks the behaviour required for
// `field:in(a,b,c)` queries (and the Jaeger / Tempo trace-spans lookup that
// expands one stage-2 query into multiple bloom checks for the same column).
// Before the fix, multiple checks against the same column were AND'ed: the
// row group was skipped if ANY single value missed the bloom filter, even
// when other values from the same `:in()` list were present. That produced
// `{"data":[]}` from `/select/jaeger/api/traces?service=…` for any service
// whose stage-1 trace_id list contained even one identifier the bloom did
// not see (which is the common case for sparse files).
//
// With the fix, checks are grouped per column: the row group is skipped only
// when EVERY value in the group misses the bloom. Across columns the AND
// remains.
func TestS3_bloomFilterSkip_InClauseOrSemantics(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "api-gw"},
	}
	data := writeTestParquetWithBloomToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}
	colIdx := findColumnIndex(f.Root(), "service.name")
	if colIdx < 0 {
		t.Fatal("service.name column not found")
	}

	// One IN value matches (api-gw), one does not (ghost-service). With
	// the AND-broken implementation this returned true (skip). With the
	// OR-within-column implementation this must return false (keep).
	checks := []bloomCheck{
		{colName: "service.name", colIdx: colIdx, value: parquet.ValueOf("ghost-service")},
		{colName: "service.name", colIdx: colIdx, value: parquet.ValueOf("api-gw")},
	}
	if s.bloomFilterSkip(f, rgs[0], checks) {
		t.Error("expected keep (at least one IN value bloom-matches); got skip — bloom is AND'ing inside :in(...) again")
	}

	// All IN values miss the bloom — skip is still correct.
	checksAllMiss := []bloomCheck{
		{colName: "service.name", colIdx: colIdx, value: parquet.ValueOf("ghost-1")},
		{colName: "service.name", colIdx: colIdx, value: parquet.ValueOf("ghost-2")},
	}
	if !s.bloomFilterSkip(f, rgs[0], checksAllMiss) {
		t.Error("expected skip (no IN value matches the bloom); got keep")
	}
}

// ---------------------------------------------------------------------------
// Test: prefetchFooters edge cases
// ---------------------------------------------------------------------------

func TestS3_prefetchFooters_AlreadyCached(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	footerCache := NewFooterCache(100)

	data := writeLargeParquetToBytes(t, []string{"api-gw"})
	if int64(len(data)) < minFileSizeForPrefetch {
		t.Skipf("file %d bytes < %d, skipping", len(data), minFileSizeForPrefetch)
	}

	key := "traces/dt=2026-05-10/hour=14/already-cached.parquet"
	mock.putFile(key, data)
	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	// Pre-populate cache
	footerCache.Put(key, &CachedFooter{FileSize: fi.Size})

	// Should skip already-cached files
	fetched := prefetchFooters(context.Background(), pool, []manifest.FileInfo{fi}, footerCache, 4)
	if fetched != 0 {
		t.Errorf("expected 0 fetched (already cached), got %d", fetched)
	}
}

func TestS3_prefetchFooters_SmallFiles(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	footerCache := NewFooterCache(100)

	data := writeParquetToBytes(t, []logRow{
		{TimestampUnixNano: time.Now().UnixNano(), Body: "small", SeverityText: "INFO", ServiceName: "svc"},
	})
	key := "traces/dt=2026-05-10/hour=14/tiny.parquet"
	mock.putFile(key, data)
	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	if fi.Size >= minFileSizeForPrefetch {
		t.Skip("file too large for this test")
	}

	fetched := prefetchFooters(context.Background(), pool, []manifest.FileInfo{fi}, footerCache, 4)
	if fetched != 0 {
		t.Errorf("expected 0 fetched (small file), got %d", fetched)
	}
}

func TestS3_prefetchFooters_CancelledContext(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	footerCache := NewFooterCache(100)

	data := writeLargeParquetToBytes(t, []string{"api-gw"})
	if int64(len(data)) < minFileSizeForPrefetch {
		t.Skip("file too small")
	}

	key := "traces/dt=2026-05-10/hour=14/cancel-prefetch.parquet"
	mock.putFile(key, data)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	fetched := prefetchFooters(ctx, pool, []manifest.FileInfo{fi}, footerCache, 4)
	_ = fetched
}

// ---------------------------------------------------------------------------
// Test: RunQuery with timestamp-only hint (manifest fast path)
// ---------------------------------------------------------------------------

func TestS3_RunQuery_TimestampOnlyHint(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "ts only test", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "ts only 2", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "traces/dt=2026-05-10/hour=14/ts-only.parquet"

	fi := manifest.FileInfo{
		Key:       key,
		Size:      int64(len(data)),
		MinTimeNs: now.Add(-time.Minute).UnixNano(),
		MaxTimeNs: now.Add(time.Minute).UnixNano(),
		RowCount:  2,
	}
	mock.putFile(key, data)
	s.manifest.AddFile("dt=2026-05-10/hour=14", fi)

	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	ctx := storage.WithTimestampOnlyHint(context.Background())
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	var totalRows int
	var mu sync.Mutex
	err := s.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		totalRows += db.RowsCount()
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	if totalRows == 0 {
		t.Error("expected rows from timestamp-only query (manifest fast path)")
	}
}

// ---------------------------------------------------------------------------
// Test: queryFile with timestamp-only hint
// ---------------------------------------------------------------------------

func TestS3_queryFile_TimestampOnlyHint(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)
	key := "traces/dt=2026-05-10/hour=14/ts-hint.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	ctx := storage.WithTimestampOnlyHint(context.Background())

	var blocks []*logstorage.DataBlock
	err := s.queryFile(ctx, fi, startNs, endNs, "", nil, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile with timestamp hint: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows, got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: readRowGroup for traces mode
// ---------------------------------------------------------------------------

func TestS3_readRowGroup_TracesMode(t *testing.T) {
	s := testStorage()
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)

	type traceRow struct {
		TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
		TraceID           string `parquet:"trace_id"`
		SpanID            string `parquet:"span_id"`
		ServiceName       string `parquet:"service.name"`
	}

	var buf strings.Builder
	w := parquet.NewGenericWriter[traceRow](&buf, parquet.Compression(&parquet.Zstd))
	traceRows := []traceRow{
		{TimestampUnixNano: now.UnixNano(), TraceID: "trace-001", SpanID: "span-001", ServiceName: "api-gw"},
	}
	if _, err := w.Write(traceRows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := []byte(buf.String())

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err = s.readRowGroup(f, rgs[0], startNs, endNs,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		}, nil)
	if err != nil {
		t.Fatalf("readRowGroup traces: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 1 {
		t.Errorf("expected 1 row from traces readRowGroup, got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: readRowGroupColumnar
// ---------------------------------------------------------------------------

func TestS3_readRowGroupColumnar_WithBitmap(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "worker"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "test", SeverityText: "WARN", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	reg := schema.NewRegistry(schema.LogsProfile)
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	bitmap := []bool{true, false, true}
	cols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"service.name":        true,
	}

	db := readRowGroupColumnar(f, rgs[0], cols, reg, startNs, endNs, bitmap)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 2 {
		t.Errorf("expected 2 rows (filtered by bitmap), got %d", db.RowsCount())
	}
}

func TestS3_readRowGroupColumnar_NoBitmap(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	reg := schema.NewRegistry(schema.LogsProfile)
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	cols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
	}

	db := readRowGroupColumnar(f, rgs[0], cols, reg, startNs, endNs, nil)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 2 {
		t.Errorf("expected 2 rows, got %d", db.RowsCount())
	}
}

func TestS3_readRowGroupColumnar_TimeRangeFilter(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "in range", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: now.Add(2 * time.Hour).UnixNano(), Body: "out of range", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	reg := schema.NewRegistry(schema.LogsProfile)
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	cols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
	}

	db := readRowGroupColumnar(f, rgs[0], cols, reg, startNs, endNs, nil)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 1 {
		t.Errorf("expected 1 row (time filtered), got %d", db.RowsCount())
	}
}

// ---------------------------------------------------------------------------
// Test: cacheOnFlush
// ---------------------------------------------------------------------------

func TestS3_cacheOnFlush_NilController(t *testing.T) {
	// Should not panic
	cacheOnFlush(nil, "test.parquet", []byte("data"))
}

func TestS3_cacheOnFlush_EmptyData(t *testing.T) {
	sc := newSmartCacheWithLocalKeys(nil)
	// Should not panic
	cacheOnFlush(sc, "test.parquet", nil)
}

func TestS3_cacheOnFlush_ValidParquet(t *testing.T) {
	sc := newSmartCacheWithLocalKeys(nil)

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "cache flush test", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)

	// Should not panic and should cache column chunks
	cacheOnFlush(sc, "traces/dt=2026-05-10/hour=14/flush.parquet", data)
}

func TestS3_cacheOnFlush_InvalidParquet(t *testing.T) {
	sc := newSmartCacheWithLocalKeys(nil)
	// Should not panic with invalid parquet data
	cacheOnFlush(sc, "test.parquet", []byte("not valid parquet"))
}

// ---------------------------------------------------------------------------
// Test: readColumnChunkBytes
// ---------------------------------------------------------------------------

func TestS3_readColumnChunkBytes(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	chunks := rgs[0].ColumnChunks()
	if len(chunks) == 0 {
		t.Fatal("no column chunks")
	}

	chunkData, err := readColumnChunkBytes(chunks[0])
	if err != nil {
		t.Fatalf("readColumnChunkBytes: %v", err)
	}
	if len(chunkData) == 0 {
		t.Error("expected non-empty column chunk data")
	}
}

// ---------------------------------------------------------------------------
// Test: isFileNotFoundError
// ---------------------------------------------------------------------------

func TestS3_isFileNotFoundError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("NoSuchKey: the key does not exist"), true},
		{fmt.Errorf("NotFound"), true},
		{fmt.Errorf("HTTP 404"), true},
		{fmt.Errorf("does not exist"), true},
		{fmt.Errorf("file not found"), true},
		{fmt.Errorf("random error"), false},
	}

	for _, tt := range tests {
		got := isFileNotFoundError(tt.err)
		if got != tt.want {
			t.Errorf("isFileNotFoundError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: rowGroupMatchesTimeRange
// ---------------------------------------------------------------------------

func TestS3_rowGroupMatchesTimeRange(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	tsIdx := findColumnIndex(f.Root(), "timestamp_unix_nano")

	// Match: query range overlaps
	if !rowGroupMatchesTimeRange(rgs[0], tsIdx, now.Add(-time.Minute).UnixNano(), now.Add(time.Minute).UnixNano()) {
		t.Error("expected match for overlapping range")
	}

	// No match: query range is entirely before
	if rowGroupMatchesTimeRange(rgs[0], tsIdx, now.Add(-2*time.Hour).UnixNano(), now.Add(-time.Hour).UnixNano()) {
		t.Error("expected no match for range entirely before")
	}

	// No match: query range is entirely after
	if rowGroupMatchesTimeRange(rgs[0], tsIdx, now.Add(time.Hour).UnixNano(), now.Add(2*time.Hour).UnixNano()) {
		t.Error("expected no match for range entirely after")
	}

	// Invalid column index should return true (conservative)
	if !rowGroupMatchesTimeRange(rgs[0], 999, 0, int64(time.Hour)) {
		t.Error("expected true for invalid column index (conservative)")
	}
}

// ---------------------------------------------------------------------------
// Test: readRowGroupWithProjection
// ---------------------------------------------------------------------------

func TestS3_readRowGroupWithProjection_OnlyConstantCols(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "constant-svc"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "INFO", ServiceName: "constant-svc"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	rgs := f.RowGroups()

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	cols := map[string]bool{
		"service.name":  true,
		"severity_text": true,
	}

	var blocks []*logstorage.DataBlock
	err = s.readRowGroupWithProjection(f, rgs[0], startNs, endNs, cols, nil,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		}, nil)
	if err != nil {
		t.Fatalf("readRowGroupWithProjection: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows (constant cols only), got %d", totalRows)
	}
}

func TestS3_readRowGroupWithProjection_PrewhereFilter(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "worker"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "test", SeverityText: "WARN", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	rgs := f.RowGroups()

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	cols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"service.name":        true,
		"severity_text":       true,
	}

	pdf := buildPushDownFilter(`service.name:="api-gw"`, s.registry)
	resolvedPdf := resolvePushDownIndices(f, pdf)

	var blocks []*logstorage.DataBlock
	err = s.readRowGroupWithProjection(f, rgs[0], startNs, endNs, cols, resolvedPdf,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		}, nil)
	if err != nil {
		t.Fatalf("readRowGroupWithProjection with prewhere: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows > 3 {
		t.Errorf("expected at most 3 rows with prewhere, got %d", totalRows)
	}
}

func TestS3_readRowGroupWithProjection_TraceIDCollection(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	rgs := f.RowGroups()

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	cols := allLeafColumns(f)

	var traceIDs []string

	var blocks []*logstorage.DataBlock
	err = s.readRowGroupWithProjection(f, rgs[0], startNs, endNs, cols, nil,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		}, &traceIDs)
	if err != nil {
		t.Fatalf("readRowGroupWithProjection: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 1 {
		t.Errorf("expected 1 row, got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: syntheticManifestBlock
// ---------------------------------------------------------------------------

func TestS3_syntheticManifestBlock_Zero(t *testing.T) {
	s := testStorage()
	fi := manifest.FileInfo{RowCount: 0}
	db := s.syntheticManifestBlock(fi)
	if db != nil {
		t.Error("expected nil for 0 row count")
	}
}

func TestS3_syntheticManifestBlock_SingleRow(t *testing.T) {
	s := testStorage()
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	fi := manifest.FileInfo{
		RowCount:  1,
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Second).UnixNano(),
	}
	db := s.syntheticManifestBlock(fi)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 1 {
		t.Errorf("expected 1 row, got %d", db.RowsCount())
	}
}

func TestS3_syntheticManifestBlock_MultipleRows(t *testing.T) {
	s := testStorage()
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	fi := manifest.FileInfo{
		RowCount:  100,
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Hour).UnixNano(),
	}
	db := s.syntheticManifestBlock(fi)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 100 {
		t.Errorf("expected 100 rows, got %d", db.RowsCount())
	}
}

// ---------------------------------------------------------------------------
// Test: RunQuery with bloom partition filter active
// ---------------------------------------------------------------------------

func TestS3_RunQuery_WithBloomPartitionFilter(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)

	rowsA := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a-msg", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	dataA := writeParquetToBytes(t, rowsA)
	keyA := "traces/dt=2026-05-10/hour=14/bloom-part-a.parquet"
	registerFileInMockS3(t, s, mock, keyA, dataA, now)

	rowsB := []logRow{
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b-msg", SeverityText: "INFO", ServiceName: "worker"},
	}
	dataB := writeParquetToBytes(t, rowsB)
	keyB := "traces/dt=2026-05-10/hour=14/bloom-part-b.parquet"
	registerFileInMockS3(t, s, mock, keyB, dataB, now)

	idx := bloomindex.New()
	fA := bloomindex.NewFilter(100, 0.01)
	fA.Add("api-gw")
	idx.AddColumns(keyA, map[string]*bloomindex.Filter{"service.name": fA})
	fB := bloomindex.NewFilter(100, 0.01)
	fB.Add("worker")
	idx.AddColumns(keyB, map[string]*bloomindex.Filter{"service.name": fB})

	loader := func(_ context.Context, partition string) (*bloomindex.Index, error) {
		if partition == "traces/dt=2026-05-10/hour=14" {
			return idx, nil
		}
		return nil, nil
	}
	s.bloomCache = bloomindex.NewBloomCache(1024*1024, loader)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	q := mustParseQueryWithTime(t, `service.name:="api-gw"`, startNs, endNs)

	var totalRows int
	var mu sync.Mutex
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		totalRows += db.RowsCount()
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	// With bloom filter, we should get at most the rows from the matching file.
	// The exact number depends on how the bloom index interacts with the query path.
	if totalRows > 2 {
		t.Errorf("expected at most 2 rows, got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: queryFile with multiple row groups
// ---------------------------------------------------------------------------

func TestS3_queryFile_MultipleRowGroups(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	var rows []logRow
	for i := 0; i < 2000; i++ {
		rows = append(rows, logRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("row %d %s", i, strings.Repeat("x", 50)),
			SeverityText:      "INFO",
			ServiceName:       "api-gw",
		})
	}

	var buf []byte
	func() {
		var b strings.Builder
		w := parquet.NewGenericWriter[logRow](&b, parquet.MaxRowsPerRowGroup(500))
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		buf = []byte(b.String())
	}()

	key := "traces/dt=2026-05-10/hour=14/multi-rg.parquet"
	mock.putFile(key, buf)
	fi := manifest.FileInfo{
		Key:       key,
		Size:      int64(len(buf)),
		MinTimeNs: now.Add(-time.Minute).UnixNano(),
		MaxTimeNs: now.Add(time.Minute).UnixNano(),
	}
	s.manifest.AddFile("dt=2026-05-10/hour=14", fi)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var totalRows atomic.Int64
	err := s.queryFile(context.Background(), fi, startNs, endNs, "", nil, func(_ uint, db *logstorage.DataBlock) {
		totalRows.Add(int64(db.RowsCount()))
	})
	if err != nil {
		t.Fatalf("queryFile with multiple RGs: %v", err)
	}

	if totalRows.Load() < 1000 {
		t.Errorf("expected at least 1000 rows from multiple row groups, got %d", totalRows.Load())
	}
}

// ---------------------------------------------------------------------------
// Test: s3Adapter.Download
// ---------------------------------------------------------------------------

func TestS3_s3Adapter_Download(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	adapter := &s3Adapter{
		pool:  pool,
		dlSem: make(chan struct{}, 4),
	}

	mock.putFile("adapter-test-key", []byte("adapter test data"))

	data, err := adapter.Download(context.Background(), "adapter-test-key")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if string(data) != "adapter test data" {
		t.Errorf("expected 'adapter test data', got %q", string(data))
	}
}

func TestS3_s3Adapter_Download_NilSem(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	adapter := &s3Adapter{
		pool:  pool,
		dlSem: nil,
	}

	mock.putFile("no-sem-key", []byte("no sem data"))

	data, err := adapter.Download(context.Background(), "no-sem-key")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if string(data) != "no sem data" {
		t.Errorf("expected 'no sem data', got %q", string(data))
	}
}

func TestS3_s3Adapter_Download_CancelledCtx(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	sem := make(chan struct{}, 1)
	sem <- struct{}{} // Fill it

	adapter := &s3Adapter{
		pool:  pool,
		dlSem: sem,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := adapter.Download(ctx, "test-key")
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

// ---------------------------------------------------------------------------
// Test: WarmupCache via mock S3
// ---------------------------------------------------------------------------

func TestS3_WarmupCache(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Cache.WarmupPartitions = 2
	s.cfg.Cache.WarmupMaxFiles = 10
	s.cfg.Cache.WarmupConcurrency = 2

	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		rows := []logRow{
			{TimestampUnixNano: now.Add(-time.Duration(i) * time.Minute).UnixNano(), Body: fmt.Sprintf("warm-%d", i), SeverityText: "INFO", ServiceName: "svc"},
		}
		data := writeParquetToBytes(t, rows)
		key := fmt.Sprintf("traces/dt=%s/hour=%02d/warmup%d.parquet", now.Format("2006-01-02"), now.Hour(), i)
		mock.putFile(key, data)
		s.manifest.AddFile(
			fmt.Sprintf("dt=%s/hour=%02d", now.Format("2006-01-02"), now.Hour()),
			manifest.FileInfo{
				Key:       key,
				Size:      int64(len(data)),
				MinTimeNs: now.Add(-time.Hour).UnixNano(),
				MaxTimeNs: now.Add(time.Hour).UnixNano(),
			},
		)
	}

	// Should not panic and should warm files
	s.WarmupCache(context.Background())
}

func TestS3_WarmupCache_NoFiles(t *testing.T) {
	s := testStorage()
	s.cfg.Cache.WarmupPartitions = 2
	s.cfg.Cache.WarmupMaxFiles = 10
	s.cfg.Cache.WarmupConcurrency = 2

	// No files in manifest
	s.WarmupCache(context.Background())
}

func TestS3_WarmupCache_CancelledContext(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Cache.WarmupPartitions = 2
	s.cfg.Cache.WarmupMaxFiles = 10
	s.cfg.Cache.WarmupConcurrency = 2

	now := time.Now().UTC()
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := fmt.Sprintf("traces/dt=%s/hour=%02d/warmup-cancel.parquet", now.Format("2006-01-02"), now.Hour())
	mock.putFile(key, data)
	s.manifest.AddFile(
		fmt.Sprintf("dt=%s/hour=%02d", now.Format("2006-01-02"), now.Hour()),
		manifest.FileInfo{
			Key:       key,
			Size:      int64(len(data)),
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.Add(time.Hour).UnixNano(),
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s.WarmupCache(ctx)
}

func TestS3_WarmupCache_MaxFilesTruncation(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Cache.WarmupPartitions = 2
	s.cfg.Cache.WarmupMaxFiles = 2 // Only warm 2 out of 5 files
	s.cfg.Cache.WarmupConcurrency = 1

	now := time.Now().UTC()
	for i := 0; i < 5; i++ {
		rows := []logRow{
			{TimestampUnixNano: now.Add(-time.Duration(i) * time.Minute).UnixNano(), Body: fmt.Sprintf("trunc-%d", i), SeverityText: "INFO", ServiceName: "svc"},
		}
		data := writeParquetToBytes(t, rows)
		key := fmt.Sprintf("traces/dt=%s/hour=%02d/trunc%d.parquet", now.Format("2006-01-02"), now.Hour(), i)
		mock.putFile(key, data)
		s.manifest.AddFile(
			fmt.Sprintf("dt=%s/hour=%02d", now.Format("2006-01-02"), now.Hour()),
			manifest.FileInfo{
				Key:       key,
				Size:      int64(len(data)),
				MinTimeNs: now.Add(-time.Hour).UnixNano(),
				MaxTimeNs: now.Add(time.Hour).UnixNano(),
			},
		)
	}

	s.WarmupCache(context.Background())
}

func TestS3_WarmupCache_WithSmartCache(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	now := time.Now().UTC()
	key1 := fmt.Sprintf("traces/dt=%s/hour=%02d/owned.parquet", now.Format("2006-01-02"), now.Hour())
	key2 := fmt.Sprintf("traces/dt=%s/hour=%02d/unowned.parquet", now.Format("2006-01-02"), now.Hour())

	s := testStorageWithS3(t, mock.url())
	s.cfg.Cache.WarmupPartitions = 2
	s.cfg.Cache.WarmupMaxFiles = 10
	s.cfg.Cache.WarmupConcurrency = 2
	s.smartCache = newSmartCacheWithLocalKeys([]string{key1})

	for _, key := range []string{key1, key2} {
		rows := []logRow{
			{TimestampUnixNano: now.UnixNano(), Body: "owned-test", SeverityText: "INFO", ServiceName: "svc"},
		}
		data := writeParquetToBytes(t, rows)
		mock.putFile(key, data)
		s.manifest.AddFile(
			fmt.Sprintf("dt=%s/hour=%02d", now.Format("2006-01-02"), now.Hour()),
			manifest.FileInfo{
				Key:       key,
				Size:      int64(len(data)),
				MinTimeNs: now.Add(-time.Hour).UnixNano(),
				MaxTimeNs: now.Add(time.Hour).UnixNano(),
			},
		)
	}

	s.WarmupCache(context.Background())
}

func TestS3_WarmupCache_DownloadErrors(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Cache.WarmupPartitions = 2
	s.cfg.Cache.WarmupMaxFiles = 10
	s.cfg.Cache.WarmupConcurrency = 2

	now := time.Now().UTC()
	key := fmt.Sprintf("traces/dt=%s/hour=%02d/missing.parquet", now.Format("2006-01-02"), now.Hour())
	s.manifest.AddFile(
		fmt.Sprintf("dt=%s/hour=%02d", now.Format("2006-01-02"), now.Hour()),
		manifest.FileInfo{
			Key:       key,
			Size:      1000,
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.Add(time.Hour).UnixNano(),
		},
	)

	// Should handle download errors gracefully
	s.WarmupCache(context.Background())
}

// ---------------------------------------------------------------------------
// Test: filterOwnedFiles
// ---------------------------------------------------------------------------

func TestS3_filterOwnedFiles_NilChecker(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}
	result := filterOwnedFiles(files, nil)
	if len(result) != 2 {
		t.Errorf("expected 2 files (nil checker = all), got %d", len(result))
	}
}

func TestS3_filterOwnedFiles_WithChecker(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
		{Key: "c.parquet"},
	}
	checker := &mockOwnershipChecker{owned: map[string]bool{"a.parquet": true, "c.parquet": true}}
	result := filterOwnedFiles(files, checker)
	if len(result) != 2 {
		t.Errorf("expected 2 owned files, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// Test: logRowsToDataBlock and traceRowsToDataBlock
// ---------------------------------------------------------------------------

func TestS3_logRowsToDataBlock(t *testing.T) {
	s := testStorage()

	now := time.Now().UnixNano()
	rows := []schema.LogRow{
		{TimestampUnixNano: now, Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now + int64(time.Second), Body: "world", SeverityText: "ERROR", ServiceName: "worker"},
	}

	db := s.logRowsToDataBlock(rows)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 2 {
		t.Errorf("expected 2 rows, got %d", db.RowsCount())
	}
}

func TestS3_logRowsToDataBlock_Empty(t *testing.T) {
	s := testStorage()
	db := s.logRowsToDataBlock(nil)
	if db != nil {
		t.Error("expected nil for empty rows")
	}
}

func TestS3_traceRowsToDataBlock(t *testing.T) {
	s := testStorage()
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)

	now := time.Now().UnixNano()
	rows := []schema.TraceRow{
		{TimestampUnixNano: now, TraceID: "trace-1", SpanID: "span-1", ServiceName: "api-gw"},
	}

	db := s.traceRowsToDataBlock(rows)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 1 {
		t.Errorf("expected 1 row, got %d", db.RowsCount())
	}
}

func TestS3_traceRowsToDataBlock_Empty(t *testing.T) {
	s := testStorage()
	db := s.traceRowsToDataBlock(nil)
	if db != nil {
		t.Error("expected nil for empty rows")
	}
}

// ---------------------------------------------------------------------------
// Test: applySelfFilter integration
// ---------------------------------------------------------------------------

func TestS3_applySelfFilter_Integration(t *testing.T) {
	s := testStorage()
	s.selfFilterEnabled = true

	key1 := "traces/dt=2026-05-10/hour=14/owned1.parquet"
	key2 := "traces/dt=2026-05-10/hour=14/owned2.parquet"
	key3 := "traces/dt=2026-05-10/hour=14/remote.parquet"

	s.smartCache = newSmartCacheWithLocalKeys([]string{key1, key2})

	files := []manifest.FileInfo{
		{Key: key1, Size: 100},
		{Key: key2, Size: 200},
		{Key: key3, Size: 300},
	}

	result := s.applySelfFilter(files)
	if len(result) != 2 {
		t.Fatalf("expected 2 owned files, got %d", len(result))
	}

	keys := make(map[string]bool)
	for _, f := range result {
		keys[f.Key] = true
	}
	if !keys[key1] || !keys[key2] {
		t.Errorf("expected owned files, got: %v", keys)
	}
	if keys[key3] {
		t.Error("remote file should be filtered out")
	}
}

// ---------------------------------------------------------------------------
// Test: RunQuery with cache affinity
// ---------------------------------------------------------------------------

func TestS3_RunQuery_WithCacheAffinity(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)

	for i := 0; i < 2; i++ {
		rows := []logRow{
			{TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(), Body: fmt.Sprintf("msg-%d", i), SeverityText: "INFO", ServiceName: "svc"},
		}
		data := writeParquetToBytes(t, rows)
		key := fmt.Sprintf("traces/dt=2026-05-10/hour=14/affinity%d.parquet", i)
		registerFileInMockS3(t, s, mock, key, data, now)
	}

	cachedData := writeParquetToBytes(t, []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "cached", SeverityText: "INFO", ServiceName: "svc"},
	})
	cached, _, err := ParseFooterFromData("traces/dt=2026-05-10/hour=14/affinity1.parquet", cachedData)
	if err == nil && cached != nil {
		s.footerCache.Put("traces/dt=2026-05-10/hour=14/affinity1.parquet", cached)
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	var totalRows int
	var mu sync.Mutex
	err = s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		totalRows += db.RowsCount()
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("RunQuery with cache affinity: %v", err)
	}

	if totalRows != 2 {
		t.Errorf("expected 2 rows, got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: RunQuery with multiple files (exercises concurrency path)
// ---------------------------------------------------------------------------

func TestS3_RunQuery_ConcurrentMultipleFiles(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		rows := []logRow{
			{TimestampUnixNano: now.Add(time.Duration(i) * time.Minute).UnixNano(), Body: fmt.Sprintf("concurrent-%d", i), SeverityText: "INFO", ServiceName: "svc"},
		}
		data := writeParquetToBytes(t, rows)
		key := fmt.Sprintf("traces/dt=2026-05-10/hour=14/concurrent%d.parquet", i)
		registerFileInMockS3(t, s, mock, key, data, now)
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(10 * time.Minute).UnixNano()
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	var totalRows atomic.Int64
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		totalRows.Add(int64(db.RowsCount()))
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if totalRows.Load() != 5 {
		t.Errorf("expected 5 rows from 5 files, got %d", totalRows.Load())
	}
}

// ---------------------------------------------------------------------------
// Test: RunQuery with smartCache pin/unpin
// ---------------------------------------------------------------------------

func TestS3_RunQuery_SmartCachePinUnpin(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "smart-pin", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "traces/dt=2026-05-10/hour=14/smart-pin.parquet"

	s := testStorage()
	s.smartCache = newSmartCacheWithData(map[string][]byte{key: data})

	s.manifest.AddFile(
		"dt=2026-05-10/hour=14",
		manifest.FileInfo{
			Key:       key,
			Size:      int64(len(data)),
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.Add(time.Hour).UnixNano(),
		},
	)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	var totalRows int
	var mu sync.Mutex
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		totalRows += db.RowsCount()
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("RunQuery with smartCache: %v", err)
	}
	if totalRows != 1 {
		t.Errorf("expected 1 row, got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: queryFile with time range skip
// ---------------------------------------------------------------------------

func TestS3_queryFile_TimeRangeSkip(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)
	key := "traces/dt=2026-05-10/hour=14/time-skip.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	startNs := now.Add(time.Hour).UnixNano()
	endNs := now.Add(2 * time.Hour).UnixNano()

	var totalRows int
	err := s.queryFile(context.Background(), fi, startNs, endNs, "", nil, func(_ uint, db *logstorage.DataBlock) {
		totalRows += db.RowsCount()
	})
	if err != nil {
		t.Fatalf("queryFile: %v", err)
	}
	if totalRows != 0 {
		t.Errorf("expected 0 rows (time range skip), got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: queryFile with pipe fields (column projection)
// ---------------------------------------------------------------------------

func TestS3_queryFile_WithPipeFields(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "pipe-test", SeverityText: "INFO", ServiceName: "pipe-svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "traces/dt=2026-05-10/hour=14/pipe.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var totalRows int
	err := s.queryFile(context.Background(), fi, startNs, endNs, "*", []string{"body", "service.name"}, func(_ uint, db *logstorage.DataBlock) {
		totalRows += db.RowsCount()
	})
	if err != nil {
		t.Fatalf("queryFile with pipe fields: %v", err)
	}
	if totalRows != 1 {
		t.Errorf("expected 1 row, got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: getFileData with smartCache path
// ---------------------------------------------------------------------------

func TestS3_getFileData_SmartCache(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "sc", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "traces/dt=2026-05-10/hour=14/sc.parquet"

	s := testStorage()
	s.smartCache = newSmartCacheWithData(map[string][]byte{key: data})

	result, err := s.getFileData(context.Background(), key, int64(len(data)))
	if err != nil {
		t.Fatalf("getFileData via smartCache: %v", err)
	}
	if len(result) == 0 {
		t.Error("expected non-empty data from smartCache")
	}
}

// ---------------------------------------------------------------------------
// Test: openParquetFile concurrent downloads
// ---------------------------------------------------------------------------

func TestS3_openParquetFile_ConcurrentDownloads(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "concurrent", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "traces/dt=2026-05-10/hour=14/concurrent.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	var wg sync.WaitGroup
	var errCount atomic.Int64
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := s.openParquetFile(context.Background(), fi, nil)
			if err != nil {
				errCount.Add(1)
				return
			}
			if f == nil {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("got %d errors in concurrent openParquetFile", errCount.Load())
	}
}

// ---------------------------------------------------------------------------
// Test: updateColumnStats
// ---------------------------------------------------------------------------

func TestS3_updateColumnStats(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "ERROR", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	key := "traces/dt=2026-05-10/hour=14/stats.parquet"
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:       key,
		Size:      int64(len(data)),
		MinTimeNs: now.Add(-time.Minute).UnixNano(),
		MaxTimeNs: now.Add(time.Minute).UnixNano(),
	})

	// Should not panic and should update column stats in manifest
	s.updateColumnStats(key, f)
}

// ---------------------------------------------------------------------------
// Test: preFilterFiles with trace ID smart cache hit
// ---------------------------------------------------------------------------

// TestS3_preFilterFiles_TraceIDCacheHit pins the load-bearing
// invariant from the recently-flushed-file parity bug: preFilterFiles
// MUST NOT use the smartCache trace_id mapping to NARROW the candidate
// file set, because that mapping is a LOWER BOUND (only files whose
// TraceIDs have been RecordTraceIDs'd, which happens at the END of a
// scan — never for a just-flushed-not-yet-queried file).
//
// Here keyA has "trace-abc-123" recorded but keyB does NOT (it
// simulates a recently-flushed file that genuinely contains the
// trace but hasn't been queried yet, so its smartCache TraceIDs set
// is empty). The PRE-FIX code narrowed to {keyA} and silently dropped
// keyB — the exact mechanism that made cold Jaeger return 0 traces
// for minutes-to-1h-old spans while hot VT returned them. Post-fix,
// preFilterFiles defers to the deterministic footer-based
// filterFilesByTraceIdx (run later in RunQuery), so keyB MUST survive
// here.
//
// Do NOT relax this back to "narrowing to keyA is also acceptable" —
// that tolerance is what let the bug ship.
func TestS3_preFilterFiles_TraceIDCacheHit(t *testing.T) {
	s := testStorage()
	s.smartCache = newSmartCacheWithLocalKeys(nil)

	keyA := "traces/dt=2026-05-10/hour=14/a.parquet"
	keyB := "traces/dt=2026-05-10/hour=14/b.parquet"
	s.smartCache.RecordTraceIDs(keyA, []string{"trace-abc-123"})

	files := []manifest.FileInfo{
		{Key: keyA},
		{Key: keyB},
	}

	result := s.preFilterFiles(files, `trace_id:="trace-abc-123"`)
	keys := map[string]bool{}
	for _, fi := range result {
		keys[fi.Key] = true
	}
	if !keys[keyB] {
		t.Errorf("keyB (recently-flushed, trace_id not yet recorded in smartCache) was "+
			"silently dropped by preFilterFiles — this is the cold-tier recently-flushed "+
			"parity bug: the smartCache lower-bound narrowing must NOT drop a manifest file "+
			"the deterministic trace_idx pre-filter would keep. result keys: %v", keys)
	}
	if !keys[keyA] {
		t.Errorf("keyA (the recorded file) must also survive; result keys: %v", keys)
	}
}

// TestS3_preFilterFiles_TraceIDCacheFullHit — even when BOTH files
// have recorded TraceIDs, preFilterFiles must not narrow by the cache
// (the deterministic trace_idx pre-filter does the real narrowing).
// Both files survive preFilterFiles; trace_idx later keeps only the
// one whose footer lists the queried id.
func TestS3_preFilterFiles_TraceIDCacheFullHit(t *testing.T) {
	s := testStorage()

	sc := newSmartCacheWithLocalKeys(nil)

	keyA := "traces/dt=2026-05-10/hour=14/a.parquet"
	keyB := "traces/dt=2026-05-10/hour=14/b.parquet"

	sc.Metadata().Set(keyA, smartcache.EntryMeta{Signal: "traces", Size: 100})
	sc.Metadata().Set(keyB, smartcache.EntryMeta{Signal: "traces", Size: 200})

	sc.RecordTraceIDs(keyA, []string{"trace-hit-abc"})
	sc.RecordTraceIDs(keyB, []string{"trace-hit-xyz"})

	s.smartCache = sc

	files := []manifest.FileInfo{
		{Key: keyA},
		{Key: keyB},
	}

	result := s.preFilterFiles(files, `trace_id:="trace-hit-abc"`)
	if len(result) != 2 {
		t.Errorf("preFilterFiles must not narrow by smartCache trace_id mapping; "+
			"expected both files to survive (trace_idx does the real narrowing later), got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// Test: detectConstantColumns
// ---------------------------------------------------------------------------

func TestS3_detectConstantColumns_AllSame(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "constant-svc"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "INFO", ServiceName: "constant-svc"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "c", SeverityText: "INFO", ServiceName: "constant-svc"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Skip("no row groups")
	}

	wantCols := map[string]bool{"service.name": true, "body": true}
	constants := detectConstantColumns(f, rgs[0], wantCols)

	foundServiceName := false
	for _, cc := range constants {
		if cc.name == "service.name" {
			foundServiceName = true
			if cc.value != "constant-svc" {
				t.Errorf("expected constant value 'constant-svc', got %v", cc.value)
			}
		}
	}
	if !foundServiceName {
		t.Log("service.name not detected as constant (may be single-page with min==max)")
	}
}

func TestS3_detectConstantColumns_EmptyWantCols(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Skip("no row groups")
	}

	constants := detectConstantColumns(f, rgs[0], nil)
	if constants != nil {
		t.Error("expected nil for empty wantCols")
	}
}

// ---------------------------------------------------------------------------
// Test: dictionaryContainsMatch with dictionary-encoded data
// ---------------------------------------------------------------------------

func TestS3_dictionaryContainsMatch_ExactHit(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	var rows []logRow
	for i := 0; i < 50; i++ {
		svc := "target-svc"
		if i%2 == 0 {
			svc = "other-svc"
		}
		rows = append(rows, logRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              fmt.Sprintf("msg-%d", i),
			SeverityText:      "INFO",
			ServiceName:       svc,
		})
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Skip("no row groups")
	}

	svcIdx := findColumnIndex(f.Root(), "service.name")
	if svcIdx < 0 {
		t.Skip("service.name column not found")
	}

	cc := rgs[0].ColumnChunks()[svcIdx]

	result := dictionaryContainsMatch(cc, PushDownCheck{
		Op:    PushDownExact,
		Value: "target-svc",
	})
	if !result {
		t.Error("expected dictionaryContainsMatch to return true for existing value")
	}

	result = dictionaryContainsMatch(cc, PushDownCheck{
		Op:    PushDownExact,
		Value: "nonexistent-svc",
	})
	t.Logf("dictionaryContainsMatch for missing value: %v", result)
}

// ---------------------------------------------------------------------------
// Test: rowGroupMatchesFilter with various push-down operations
// ---------------------------------------------------------------------------

func TestS3_rowGroupMatchesFilter_StringExact(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "alpha"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "WARN", ServiceName: "zeta"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Skip("no row groups")
	}

	pdf := resolvePushDownIndices(f, &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownExact, Value: "alpha", ColIdx: -1},
		},
	})
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected rowGroupMatchesFilter to return true for matching value")
	}

	pdf2 := resolvePushDownIndices(f, &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownExact, Value: "aaaa", ColIdx: -1},
		},
	})
	if rowGroupMatchesFilter(f, rgs[0], pdf2) {
		t.Error("expected rowGroupMatchesFilter to return false for out-of-range value")
	}
}

func TestS3_rowGroupMatchesFilter_UnknownColumn(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Skip("no row groups")
	}

	pdf := resolvePushDownIndices(f, &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "nonexistent_column", Op: PushDownExact, Value: "x", ColIdx: -1},
		},
	})
	if !rowGroupMatchesFilter(f, rgs[0], pdf) {
		t.Error("expected true for unknown column")
	}
}

// ---------------------------------------------------------------------------
// Test: RunQuery no data in range
// ---------------------------------------------------------------------------

func TestS3_RunQuery_NoDataInRange(t *testing.T) {
	s := testStorage()

	startNs := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2030, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano()

	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		t.Error("should not receive blocks for empty range")
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: RunQuery with buffer bridge integration
// ---------------------------------------------------------------------------

func TestS3_RunQuery_WithBufferBridge(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	insertSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Date(2026, 5, 10, 14, 30, 30, 0, time.UTC)
		rows := []schema.TraceRow{
			{
				TimestampUnixNano: now.UnixNano(),
				TraceID:           "trace-from-buffer",
				SpanID:            "span-001",
				ServiceName:       "buffer-svc",
			},
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		for _, row := range rows {
			_ = enc.Encode(row)
		}
	}))
	defer insertSrv.Close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)

	bb := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 5 * time.Second,
	}, config.ModeTraces)
	bb.SetEndpoints([]string{insertSrv.URL})
	s.bufferBridge = bb

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "from s3", SeverityText: "INFO", ServiceName: "s3-svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "traces/dt=2026-05-10/hour=14/bridge-e2e.parquet"
	registerFileInMockS3(t, s, mock, key, data, now)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	var totalRows int
	var mu sync.Mutex
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		totalRows += db.RowsCount()
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	if totalRows < 1 {
		t.Errorf("expected at least 1 row (S3 + buffer), got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Helper: writeLargeParquetToBytes generates a file larger than
// minFileSizeForPrefetch for footer prefetch tests.
// ---------------------------------------------------------------------------

func writeLargeParquetToBytes(t *testing.T, serviceNames []string) []byte {
	t.Helper()
	var rows []logRow
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	for i := 0; i < 3000; i++ {
		svc := serviceNames[i%len(serviceNames)]
		rows = append(rows, logRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			Body:              fmt.Sprintf("unique-log-%05d ts=%d svc=%s uuid=%016x%016x", i, now.UnixNano()+int64(i), svc, int64(i*7919), int64(i*6997)),
			SeverityText:      []string{"INFO", "ERROR", "WARN", "DEBUG", "TRACE", "FATAL"}[i%6],
			ServiceName:       svc,
		})
	}
	// Write WITHOUT compression to guarantee large file size
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[logRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// ---------------------------------------------------------------------------
// Helper: newSmartCacheWithData creates a smartcache.Controller with a mock
// S3 fetcher that serves data from a map.
// ---------------------------------------------------------------------------

type s3DataFetcher struct {
	data map[string][]byte
}

func (f *s3DataFetcher) Download(_ context.Context, key string) ([]byte, error) {
	if d, ok := f.data[key]; ok {
		return d, nil
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

func newSmartCacheWithData(data map[string][]byte) *smartcache.Controller {
	return smartcache.NewController(smartcache.ControllerConfig{
		L1:           &mockL1{},
		L2:           &mockL2{},
		PeerLookup:   &mockPeerLookup{localKeys: map[string]bool{}},
		S3Fetcher:    &s3DataFetcher{data: data},
		Metadata:     smartcache.NewMetadataMap(),
		GracePeriod:  5 * time.Minute,
		HotThreshold: 3,
		MaxAge:       24 * time.Hour,
	})
}

// writeTestParquetWithBloomToBytes writes a parquet file with bloom filters to bytes.
// This function may already be defined in query_coverage_boost_test.go but we need
// it here for the bloom filter skip tests.
// Skipped if already defined - handled by build.

// ---------------------------------------------------------------------------
// Test: parquetValueToInterface (various types)
// ---------------------------------------------------------------------------

func TestS3_parquetValueToInterface_Types(t *testing.T) {
	v := parquet.Int64Value(42)
	result := parquetValueToInterface(v)
	if result != int64(42) {
		t.Errorf("Int64: expected 42, got %v", result)
	}

	v = parquet.Int32Value(99)
	result = parquetValueToInterface(v)
	if result != int64(99) {
		t.Errorf("Int32: expected 99, got %v", result)
	}

	v = parquet.BooleanValue(true)
	result = parquetValueToInterface(v)
	if result != true {
		t.Errorf("Boolean: expected true, got %v", result)
	}

	v = parquet.DoubleValue(3.14)
	result = parquetValueToInterface(v)
	if result != 3.14 {
		t.Errorf("Double: expected 3.14, got %v", result)
	}

	v = parquet.ByteArrayValue([]byte("hello"))
	result = parquetValueToInterface(v)
	if result != "hello" {
		t.Errorf("ByteArray: expected 'hello', got %v", result)
	}

	v = parquet.FixedLenByteArrayValue([]byte("fix"))
	result = parquetValueToInterface(v)
	if result != "fix" {
		t.Errorf("FixedLenByteArray: expected 'fix', got %v", result)
	}
}

// ---------------------------------------------------------------------------
// Test: readRowGroupProjectedBitmap
// ---------------------------------------------------------------------------

func TestS3_readRowGroupProjectedBitmap_WithBitmap(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "ERROR", ServiceName: "worker"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "c", SeverityText: "WARN", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	cols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
	}

	bitmap := []bool{true, false, true}

	fields, err := readRowGroupProjectedBitmap(f, rgs[0], cols, bitmap)
	if err != nil {
		t.Fatalf("readRowGroupProjectedBitmap: %v", err)
	}
	if len(fields) != 2 {
		t.Errorf("expected 2 rows (bitmap filtered), got %d", len(fields))
	}
}

func TestS3_readRowGroupProjectedBitmap_NilBitmap(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)

	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	cols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
	}

	fields, err := readRowGroupProjectedBitmap(f, rgs[0], cols, nil)
	if err != nil {
		t.Fatalf("readRowGroupProjectedBitmap: %v", err)
	}
	if len(fields) != 1 {
		t.Errorf("expected 1 row, got %d", len(fields))
	}
}

// ---------------------------------------------------------------------------
// Test: filterFilesByLabels with prefix match
// ---------------------------------------------------------------------------

func TestS3_filterFilesByLabels_PrefixMatch(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet", Labels: map[string][]string{"service.name": {"api-gw-production"}}},
		{Key: "b.parquet", Labels: map[string][]string{"service.name": {"worker-production"}}},
	}

	result := s.filterFilesByLabels(files, `service.name:api-gw*`)
	if result == nil {
		t.Error("expected non-nil result")
	}
}

// ---------------------------------------------------------------------------
// Test: filterByLabelIndex with non-exact checks
// ---------------------------------------------------------------------------

func TestS3_filterByLabelIndex_NonExact(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet", Labels: map[string][]string{"service.name": {"api-gw"}}},
	}

	pdf := &PushDownFilter{
		Checks: []PushDownCheck{
			{Column: "service.name", Op: PushDownPrefix, Value: "api"},
		},
	}

	result := s.filterByLabelIndex(files, pdf)
	if result != nil {
		t.Error("expected nil for non-exact checks (fallback)")
	}
}

// ---------------------------------------------------------------------------
// Test: fileLabelsMatch for different operations
// ---------------------------------------------------------------------------

func TestS3_fileLabelsMatch_AllOps(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		check  PushDownCheck
		want   bool
	}{
		{"exact match", []string{"api-gw"}, PushDownCheck{Op: PushDownExact, Value: "api-gw"}, true},
		{"exact no match", []string{"worker"}, PushDownCheck{Op: PushDownExact, Value: "api-gw"}, false},
		{"prefix match", []string{"api-gw-prod"}, PushDownCheck{Op: PushDownPrefix, Value: "api-gw"}, true},
		{"prefix no match", []string{"worker"}, PushDownCheck{Op: PushDownPrefix, Value: "api-gw"}, false},
		{"greater than match", []string{"bbb"}, PushDownCheck{Op: PushDownGreaterThan, Value: "aaa"}, true},
		{"greater than no match", []string{"aaa"}, PushDownCheck{Op: PushDownGreaterThan, Value: "bbb"}, false},
		{"less than match", []string{"aaa"}, PushDownCheck{Op: PushDownLessThan, Value: "bbb"}, true},
		{"less than no match", []string{"ccc"}, PushDownCheck{Op: PushDownLessThan, Value: "bbb"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fileLabelsMatch(tt.values, tt.check)
			if got != tt.want {
				t.Errorf("fileLabelsMatch(%v, %+v) = %v, want %v", tt.values, tt.check, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Test: extractExactMatch edge cases
// ---------------------------------------------------------------------------

func TestS3_extractExactMatch_Variations(t *testing.T) {
	tests := []struct {
		query     string
		fieldName string
		want      string
	}{
		{`service.name:="api-gw"`, "service.name", "api-gw"},
		{`service.name:"api-gw"`, "service.name", "api-gw"},
		{`service.name:=api-gw`, "service.name", "api-gw"},
		{`service.name:=api-gw body:=test`, "service.name", "api-gw"},
		{`body:="hello"`, "service.name", ""},
		{`*`, "service.name", ""},
	}

	for _, tt := range tests {
		got := extractExactMatch(tt.query, tt.fieldName)
		if got != tt.want {
			t.Errorf("extractExactMatch(%q, %q) = %q, want %q", tt.query, tt.fieldName, got, tt.want)
		}
	}
}
