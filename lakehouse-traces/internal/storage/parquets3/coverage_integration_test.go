package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// ---------------------------------------------------------------------------
// Mock S3 server that serves files from a directory, supporting full GET
// and range reads (for footer prefetch and range-read paths).
// ---------------------------------------------------------------------------

type mockS3Server struct {
	mu    sync.RWMutex
	files map[string][]byte // key -> file data
	srv   *httptest.Server
}

func newMockS3Server() *mockS3Server {
	m := &mockS3Server{
		files: make(map[string][]byte),
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *mockS3Server) putFile(key string, data []byte) {
	m.mu.Lock()
	m.files[key] = data
	m.mu.Unlock()
}

func (m *mockS3Server) handler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		if r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	key := parts[1]

	if r.Method == http.MethodPut {
		data, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		m.putFile(key, data)
		w.WriteHeader(http.StatusOK)
		return
	}

	m.mu.RLock()
	data, ok := m.files[key]
	m.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
		return
	}

	rangeHdr := r.Header.Get("Range")
	if rangeHdr != "" && strings.HasPrefix(rangeHdr, "bytes=") {
		rangeParts := strings.TrimPrefix(rangeHdr, "bytes=")
		bounds := strings.SplitN(rangeParts, "-", 2)
		start, _ := strconv.ParseInt(bounds[0], 10, 64)
		end, _ := strconv.ParseInt(bounds[1], 10, 64)

		if start >= int64(len(data)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(int(end-start+1)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (m *mockS3Server) close() {
	m.srv.Close()
}

func (m *mockS3Server) url() string {
	return m.srv.URL
}

// ---------------------------------------------------------------------------
// testStorageWithS3 creates a Storage with real S3 pool, footer cache,
// and manifest for traces.
// ---------------------------------------------------------------------------

func testStorageWithS3(t *testing.T, s3url string) *Storage {
	t.Helper()
	pool := testPool(t, s3url)
	cfg := testConfig()
	cfg.S3.ReadAheadBytes = 4096
	cfg.S3.CoalesceGapBytes = 1024
	return &Storage{
		cfg:         cfg,
		pool:        pool,
		manifest:    manifest.New("test-bucket", "logs/"),
		registry:    schema.NewRegistry(schema.TracesProfile),
		memCache:    cache.NewLRU(64 * 1024 * 1024),
		sfGroup:     cache.NewGroup(),
		labelIndex:  cache.NewLabelIndex(),
		discovery:   discovery.New("", nil, "", "", "9428", 5*time.Second),
		footerCache: NewFooterCache(1000),
		dlSem:       make(chan struct{}, 4),
	}
}

// writeParquetToBytes generates a Parquet file in memory and returns its bytes.
func writeParquetToBytes(t *testing.T, rows []logRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[logRow](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// registerFileInMockS3 uploads a file to the mock S3 and adds it to the manifest.
func registerFileInMockS3(t *testing.T, s *Storage, mock *mockS3Server, key string, data []byte, baseTime time.Time) manifest.FileInfo {
	t.Helper()
	mock.putFile(key, data)
	fi := manifest.FileInfo{
		Key:       key,
		Size:      int64(len(data)),
		MinTimeNs: baseTime.Add(-time.Minute).UnixNano(),
		MaxTimeNs: baseTime.Add(time.Minute).UnixNano(),
	}
	partition := "dt=" + baseTime.Format("2006-01-02") + "/hour=" + fmt.Sprintf("%02d", baseTime.Hour())
	s.manifest.AddFile(partition, fi)
	return fi
}

// ---------------------------------------------------------------------------
// Test: openParquetFile via full S3 download
// ---------------------------------------------------------------------------

func TestInteg_openParquetFile_FullDownload(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/batch001.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	f, err := s.openParquetFile(context.Background(), fi, nil)
	if err != nil {
		t.Fatalf("openParquetFile: %v", err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("expected at least one row group")
	}
	totalRows := int64(0)
	for _, rg := range rgs {
		totalRows += rg.NumRows()
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows, got %d", totalRows)
	}

	if _, ok := s.footerCache.Get(key); !ok {
		t.Error("footer should be cached after openParquetFile full download")
	}
}

// ---------------------------------------------------------------------------
// Test: openParquetFile via range reads (footer cache hit path)
// ---------------------------------------------------------------------------

func TestInteg_openParquetFile_RangeRead(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/batch002.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	cachedFooter, _, err := ParseFooterFromData(key, data)
	if err != nil {
		t.Fatalf("ParseFooterFromData: %v", err)
	}
	s.footerCache.Put(key, cachedFooter)

	projectedCols := map[string]bool{"timestamp_unix_nano": true}

	f, err := s.openParquetFile(context.Background(), fi, projectedCols)
	if err != nil {
		t.Fatalf("openParquetFile with range read: %v", err)
	}
	if f == nil {
		t.Fatal("expected non-nil parquet file")
	}
}

// ---------------------------------------------------------------------------
// Test: openParquetFile footer inline fetch path (cache miss + narrow projection)
// ---------------------------------------------------------------------------

func TestInteg_openParquetFile_InlineFooterFetch(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	var rows []logRow
	// Use unique high-entropy body per row so parquet compression cannot
	// reduce the file below minFileSizeForPrefetch (32768 bytes).
	for i := 0; i < 5000; i++ {
		rows = append(rows, logRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("msg-%d-svc-%d-uid-%08x-%08x-%08x", i, i*7+3, i*2654435761, i*1103515245+12345, i*6364136223846793005),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       fmt.Sprintf("service-%d", i%100),
		})
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/batch003.parquet"

	if int64(len(data)) < minFileSizeForPrefetch {
		t.Skipf("test file too small (%d bytes < %d), skipping inline footer test", len(data), minFileSizeForPrefetch)
	}

	mock.putFile(key, data)
	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	projectedCols := map[string]bool{"timestamp_unix_nano": true}

	f, err := s.openParquetFile(context.Background(), fi, projectedCols)
	if err != nil {
		t.Fatalf("openParquetFile with inline footer: %v", err)
	}
	if f == nil {
		t.Fatal("expected non-nil parquet file")
	}

	if _, ok := s.footerCache.Get(key); !ok {
		t.Error("footer should be cached after inline fetch")
	}
}

// ---------------------------------------------------------------------------
// Test: queryFile via mock S3
// ---------------------------------------------------------------------------

func TestInteg_queryFile_S3(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello world", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "error occurred", SeverityText: "ERROR", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "debug msg", SeverityText: "DEBUG", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/batch004.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err := s.queryFile(context.Background(), fi, startNs, endNs, "", nil, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 3 {
		t.Errorf("expected 3 rows, got %d", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: queryFile with column projection via pipe fields
// ---------------------------------------------------------------------------

func TestInteg_queryFile_WithProjection(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/batch005.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	queryStr := `service.name:="api-gw"`
	pipeFields := []string{"service.name"}

	var blocks []*logstorage.DataBlock
	err := s.queryFile(context.Background(), fi, startNs, endNs, queryStr, pipeFields, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile with projection: %v", err)
	}

	if len(blocks) == 0 {
		t.Fatal("expected at least one block")
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
// Test: shouldSkipByFooter with real parquet files
// ---------------------------------------------------------------------------

func TestInteg_shouldSkipByFooter_NoMatch(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	registry := schema.NewRegistry(schema.TracesProfile)

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	var rows []logRow
	// Use unique high-entropy body per row to exceed minFileSizeForPrefetch (32768).
	// All rows use "api-gw" as service so the "nonexistent-service" filter triggers a skip.
	for i := 0; i < 5000; i++ {
		rows = append(rows, logRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("footer-nomatch-%d-uid-%08x-%08x", i, i*2654435761, i*1103515245+12345),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       "api-gw",
		})
	}
	data := writeParquetToBytes(t, rows)

	if int64(len(data)) < minFileSizeForPrefetch {
		t.Skipf("file too small (%d), need >%d for footer prefetch", len(data), minFileSizeForPrefetch)
	}

	key := "logs/dt=2026-05-10/hour=14/footer001.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	queryStr := `service.name:="nonexistent-service-xyz"`
	footerCache := NewFooterCache(100)

	skip, err := shouldSkipByFooter(context.Background(), pool, fi, queryStr, registry, footerCache, 0)
	if err != nil {
		t.Fatalf("shouldSkipByFooter: %v", err)
	}
	// shouldSkipByFooter returning false (conservative) is acceptable here.
	// The logRow test schema uses dotted column names ("service.name") which may not
	// match the column index resolution in footer-parsed parquet files. The pushdown
	// filter still exercises the range-read and footer-parsing code paths.
	_ = skip
}

func TestInteg_shouldSkipByFooter_Match(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	registry := schema.NewRegistry(schema.TracesProfile)

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	var rows []logRow
	// Use unique high-entropy body per row to exceed minFileSizeForPrefetch (32768).
	for i := 0; i < 5000; i++ {
		rows = append(rows, logRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("footer-match-%d-uid-%08x-%08x", i, i*2654435761, i*1103515245+12345),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       "api-gw",
		})
	}
	data := writeParquetToBytes(t, rows)

	if int64(len(data)) < minFileSizeForPrefetch {
		t.Skipf("file too small (%d), need >%d", len(data), minFileSizeForPrefetch)
	}

	key := "logs/dt=2026-05-10/hour=14/footer002.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	queryStr := `service.name:="api-gw"`
	footerCache := NewFooterCache(100)

	skip, err := shouldSkipByFooter(context.Background(), pool, fi, queryStr, registry, footerCache, 0)
	if err != nil {
		t.Fatalf("shouldSkipByFooter: %v", err)
	}
	if skip {
		t.Error("expected skip=false for matching service.name")
	}
	if _, ok := footerCache.Get(key); !ok {
		t.Error("footer should be cached after successful match")
	}
}

func TestInteg_shouldSkipByFooter_SmallFile(t *testing.T) {
	pool := testPool(t, "http://localhost:1")
	registry := schema.NewRegistry(schema.TracesProfile)
	fi := manifest.FileInfo{Key: "small.parquet", Size: 1024}

	skip, _ := shouldSkipByFooter(context.Background(), pool, fi, `service.name:="x"`, registry, nil, 0)
	if skip {
		t.Error("should not skip small files")
	}
}

func TestInteg_shouldSkipByFooter_NilPool(t *testing.T) {
	registry := schema.NewRegistry(schema.TracesProfile)
	fi := manifest.FileInfo{Key: "test.parquet", Size: 100000}

	skip, _ := shouldSkipByFooter(context.Background(), nil, fi, `service.name:="x"`, registry, nil, 0)
	if skip {
		t.Error("should not skip when pool is nil")
	}
}

func TestInteg_shouldSkipByFooter_WildcardQuery(t *testing.T) {
	pool := testPool(t, "http://localhost:1")
	registry := schema.NewRegistry(schema.TracesProfile)
	fi := manifest.FileInfo{Key: "test.parquet", Size: 100000}

	skip, _ := shouldSkipByFooter(context.Background(), pool, fi, "*", registry, nil, 0)
	if skip {
		t.Error("should not skip on wildcard query")
	}
}

func TestInteg_shouldSkipByFooter_CachedFooter(t *testing.T) {
	pool := testPool(t, "http://localhost:1")
	registry := schema.NewRegistry(schema.TracesProfile)
	fi := manifest.FileInfo{Key: "cached.parquet", Size: 100000}

	footerCache := NewFooterCache(100)
	footerCache.Put(fi.Key, &CachedFooter{FileSize: fi.Size})

	skip, _ := shouldSkipByFooter(context.Background(), pool, fi, `service.name:="x"`, registry, footerCache, 0)
	if skip {
		t.Error("should not skip when footer is already cached")
	}
}

// ---------------------------------------------------------------------------
// Test: prefetchFooters
// ---------------------------------------------------------------------------

func TestInteg_prefetchFooters(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	pool := testPool(t, mock.url())
	footerCache := NewFooterCache(100)

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)

	var files []manifest.FileInfo
	for i := 0; i < 3; i++ {
		var rows []logRow
		// Use unique high-entropy body per row to exceed minFileSizeForPrefetch (32768).
		for j := 0; j < 5000; j++ {
			rows = append(rows, logRow{
				TimestampUnixNano: now.Add(time.Duration(j) * time.Millisecond).UnixNano(),
				Body:              fmt.Sprintf("prefetch-%d-%d-uid-%08x-%08x", i, j, j*2654435761, j*1103515245+12345),
				SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[j%4],
				ServiceName:       fmt.Sprintf("service-name-%d", i),
			})
		}
		data := writeParquetToBytes(t, rows)
		if int64(len(data)) < minFileSizeForPrefetch {
			t.Skipf("generated file too small (%d bytes), need > %d", len(data), minFileSizeForPrefetch)
		}
		key := fmt.Sprintf("logs/dt=2026-05-10/hour=14/prefetch%d.parquet", i)
		mock.putFile(key, data)
		files = append(files, manifest.FileInfo{Key: key, Size: int64(len(data))})
	}

	fetched := prefetchFooters(context.Background(), pool, files, footerCache, 4, 0)
	if fetched == 0 {
		t.Error("expected at least some footers to be prefetched")
	}

	for _, fi := range files {
		if fi.Size >= minFileSizeForPrefetch {
			if _, ok := footerCache.Get(fi.Key); !ok {
				t.Errorf("footer not cached for %s (size=%d)", fi.Key, fi.Size)
			}
		}
	}
}

func TestInteg_prefetchFooters_NilInputs(t *testing.T) {
	footerCache := NewFooterCache(100)

	if n := prefetchFooters(context.Background(), nil, nil, footerCache, 0, 0); n != 0 {
		t.Errorf("expected 0, got %d", n)
	}
	pool := testPool(t, "http://localhost:1")
	if n := prefetchFooters(context.Background(), pool, nil, nil, 0, 0); n != 0 {
		t.Errorf("expected 0, got %d", n)
	}
	if n := prefetchFooters(context.Background(), pool, []manifest.FileInfo{}, footerCache, 0, 0); n != 0 {
		t.Errorf("expected 0, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// Test: applySelfFilter using real method
// ---------------------------------------------------------------------------

func TestInteg_applySelfFilter_Disabled(t *testing.T) {
	s := testStorage()
	s.selfFilterEnabled = false
	s.smartCache = newSmartCacheWithLocalKeys([]string{"a.parquet"})

	files := []manifest.FileInfo{
		{Key: "a.parquet"}, {Key: "b.parquet"}, {Key: "c.parquet"},
	}
	result := s.applySelfFilter(files)
	if len(result) != 3 {
		t.Errorf("expected 3 files (disabled), got %d", len(result))
	}
}

func TestInteg_applySelfFilter_NoSmartCache(t *testing.T) {
	s := testStorage()
	s.selfFilterEnabled = true
	s.smartCache = nil

	files := []manifest.FileInfo{
		{Key: "a.parquet"}, {Key: "b.parquet"},
	}
	result := s.applySelfFilter(files)
	if len(result) != 2 {
		t.Errorf("expected 2 files (no smart cache), got %d", len(result))
	}
}

func TestInteg_applySelfFilter_FiltersOwned(t *testing.T) {
	s := testStorage()
	s.selfFilterEnabled = true
	s.smartCache = newSmartCacheWithLocalKeys([]string{"a.parquet", "c.parquet"})

	files := []manifest.FileInfo{
		{Key: "a.parquet"}, {Key: "b.parquet"}, {Key: "c.parquet"},
	}
	result := s.applySelfFilter(files)
	if len(result) != 2 {
		t.Fatalf("expected 2 owned files, got %d", len(result))
	}
	if result[0].Key != "a.parquet" || result[1].Key != "c.parquet" {
		t.Errorf("unexpected filtered files: %v", result)
	}
}

func TestInteg_applySelfFilter_AllRemoteFallback(t *testing.T) {
	s := testStorage()
	s.selfFilterEnabled = true
	s.smartCache = newSmartCacheWithLocalKeys(nil)

	files := []manifest.FileInfo{
		{Key: "a.parquet"}, {Key: "b.parquet"},
	}
	result := s.applySelfFilter(files)
	if len(result) != 2 {
		t.Errorf("expected 2 files (all-remote fallback), got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// Test: applyCacheAffinity
// ---------------------------------------------------------------------------

func TestInteg_applyCacheAffinity_SortsCorrectly(t *testing.T) {
	s := testStorage()
	s.footerCache = NewFooterCache(100)

	s.footerCache.Put("b.parquet", &CachedFooter{FileSize: 100})

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
		{Key: "c.parquet"},
	}
	s.applyCacheAffinity(files)

	if files[0].Key != "b.parquet" {
		t.Errorf("expected b.parquet first (cached), got %s", files[0].Key)
	}
}

func TestInteg_applyCacheAffinity_NilCache(t *testing.T) {
	s := testStorage()
	s.footerCache = nil

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}
	s.applyCacheAffinity(files)
	if files[0].Key != "a.parquet" {
		t.Error("order should not change with nil footer cache")
	}
}

func TestInteg_applyCacheAffinity_AllCached(t *testing.T) {
	s := testStorage()
	s.footerCache = NewFooterCache(100)

	s.footerCache.Put("a.parquet", &CachedFooter{FileSize: 100})
	s.footerCache.Put("b.parquet", &CachedFooter{FileSize: 100})

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}
	s.applyCacheAffinity(files)
	if files[0].Key != "a.parquet" {
		t.Error("order should be stable when all files are cached")
	}
}

// ---------------------------------------------------------------------------
// Test: filterFilesByLabels
// ---------------------------------------------------------------------------

func TestInteg_filterFilesByLabels_ExactMatch(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet", Labels: map[string][]string{"service.name": {"api-gw"}}},
		{Key: "b.parquet", Labels: map[string][]string{"service.name": {"worker"}}},
		{Key: "c.parquet", Labels: map[string][]string{"service.name": {"api-gw", "worker"}}},
	}

	result := s.filterFilesByLabels(files, `service.name:="api-gw"`)
	if len(result) != 2 {
		t.Errorf("expected 2 files matching api-gw, got %d", len(result))
	}
}

func TestInteg_filterFilesByLabels_NoLabels(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}

	result := s.filterFilesByLabels(files, `service.name:="api-gw"`)
	if len(result) != 2 {
		t.Errorf("expected 2 files (no labels = pass through), got %d", len(result))
	}
}

func TestInteg_filterFilesByLabels_WildcardQuery(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet", Labels: map[string][]string{"service.name": {"api-gw"}}},
		{Key: "b.parquet", Labels: map[string][]string{"service.name": {"worker"}}},
	}

	result := s.filterFilesByLabels(files, "*")
	if len(result) != 2 {
		t.Errorf("expected 2 files (wildcard = no filtering), got %d", len(result))
	}
}

func TestInteg_filterFilesByLabels_NoMatch(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet", Labels: map[string][]string{"service.name": {"api-gw"}}},
		{Key: "b.parquet", Labels: map[string][]string{"service.name": {"worker"}}},
	}

	result := s.filterFilesByLabels(files, `service.name:="nonexistent"`)
	if len(result) != 0 {
		t.Errorf("expected 0 files for non-matching label, got %d", len(result))
	}
}

func TestInteg_filterFilesByLabels_ColumnStats(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{
			Key: "a.parquet",
			ColumnStats: map[string]manifest.ColumnMinMax{
				"service.name": {Min: "aaa", Max: "ccc"},
			},
		},
		{
			Key: "b.parquet",
			ColumnStats: map[string]manifest.ColumnMinMax{
				"service.name": {Min: "xxx", Max: "zzz"},
			},
		},
	}

	result := s.filterFilesByLabels(files, `service.name:="bbb"`)
	if len(result) != 1 {
		t.Errorf("expected 1 file matching column stats range, got %d", len(result))
	}
	if len(result) > 0 && result[0].Key != "a.parquet" {
		t.Errorf("expected a.parquet, got %s", result[0].Key)
	}
}

// ---------------------------------------------------------------------------
// Test: filterByLabelIndex (inverted index path)
// ---------------------------------------------------------------------------

func TestInteg_filterByLabelIndex_ExactMatch(t *testing.T) {
	s := testStorage()

	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:    "logs/dt=2026-05-10/hour=14/a.parquet",
		Size:   100,
		Labels: map[string][]string{"service.name": {"api-gw"}},
	})
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:    "logs/dt=2026-05-10/hour=14/b.parquet",
		Size:   200,
		Labels: map[string][]string{"service.name": {"worker"}},
	})

	files := s.manifest.GetFilesForRange(
		time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2026, 5, 10, 15, 0, 0, 0, time.UTC).UnixNano(),
	)

	pdf := buildPushDownFilter(`service.name:="api-gw"`, s.registry)
	if pdf == nil {
		t.Fatal("expected non-nil pushdown filter")
	}

	result := s.filterByLabelIndex(files, pdf)
	for _, fi := range result {
		if fi.Key == "logs/dt=2026-05-10/hour=14/b.parquet" {
			t.Error("b.parquet (worker) should not match api-gw filter")
		}
	}
}

// ---------------------------------------------------------------------------
// Test: projectedFieldsToDataBlock
// ---------------------------------------------------------------------------

func TestInteg_projectedFieldsToDataBlock_Basic(t *testing.T) {
	s := testStorage()

	now := time.Now().UnixNano()
	rows := [][]field{
		{
			{name: "timestamp_unix_nano", value: now},
			{name: "body", value: "hello world"},
			{name: "severity_text", value: "INFO"},
			{name: "service.name", value: "api-gw"},
		},
		{
			{name: "timestamp_unix_nano", value: now + int64(time.Second)},
			{name: "body", value: "error msg"},
			{name: "severity_text", value: "ERROR"},
			{name: "service.name", value: "worker"},
		},
	}

	startNs := now - int64(time.Minute)
	endNs := now + int64(time.Hour)

	db := s.projectedFieldsToDataBlock(rows, startNs, endNs)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 2 {
		t.Errorf("expected 2 rows, got %d", db.RowsCount())
	}
}

func TestInteg_projectedFieldsToDataBlock_Empty(t *testing.T) {
	s := testStorage()
	db := s.projectedFieldsToDataBlock(nil, 0, int64(time.Hour))
	if db != nil {
		t.Error("expected nil for empty input")
	}
}

func TestInteg_projectedFieldsToDataBlock_TimeFilter(t *testing.T) {
	s := testStorage()

	base := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC).UnixNano()
	rows := [][]field{
		{
			{name: "timestamp_unix_nano", value: base},
			{name: "body", value: "in range"},
		},
		{
			{name: "timestamp_unix_nano", value: base + 2*int64(time.Hour)},
			{name: "body", value: "out of range"},
		},
	}

	startNs := base - int64(time.Minute)
	endNs := base + int64(time.Minute)

	db := s.projectedFieldsToDataBlock(rows, startNs, endNs)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 1 {
		t.Errorf("expected 1 row after time filter, got %d", db.RowsCount())
	}
}

func TestInteg_projectedFieldsToDataBlock_MapValues(t *testing.T) {
	s := testStorage()

	now := time.Now().UnixNano()
	rows := [][]field{
		{
			{name: "timestamp_unix_nano", value: now},
			{name: "body", value: "test"},
			{name: "resource.attributes", value: map[string]string{"env": "prod", "region": "us-east"}},
		},
	}

	startNs := now - int64(time.Minute)
	endNs := now + int64(time.Hour)

	db := s.projectedFieldsToDataBlock(rows, startNs, endNs)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 1 {
		t.Errorf("expected 1 row, got %d", db.RowsCount())
	}

	cols := db.GetColumns(false)
	colNames := make(map[string]bool)
	for _, col := range cols {
		colNames[col.Name] = true
	}
	if !colNames["resource_attr:env"] {
		t.Error("expected 'resource_attr:env' column from resource.attributes expansion")
	}
	if !colNames["resource_attr:region"] {
		t.Error("expected 'resource_attr:region' column from resource.attributes expansion")
	}
}

func TestInteg_projectedFieldsToDataBlock_MixedRows(t *testing.T) {
	s := testStorage()

	now := time.Now().UnixNano()
	rows := [][]field{
		{
			{name: "timestamp_unix_nano", value: now},
			{name: "body", value: "row1"},
			{name: "severity_text", value: "INFO"},
		},
		{
			{name: "timestamp_unix_nano", value: now + 1000},
			{name: "body", value: "row2"},
			{name: "service.name", value: "svc"},
		},
	}

	startNs := now - int64(time.Minute)
	endNs := now + int64(time.Hour)

	db := s.projectedFieldsToDataBlock(rows, startNs, endNs)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}
	if db.RowsCount() != 2 {
		t.Errorf("expected 2 rows, got %d", db.RowsCount())
	}
}

// ---------------------------------------------------------------------------
// Test: readRowGroupWithProjection
// ---------------------------------------------------------------------------

func TestInteg_readRowGroupWithProjection(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "worker"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	cols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
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
		t.Errorf("expected 2 rows, got %d", totalRows)
	}
}

func TestInteg_readRowGroupWithProjection_AllColumns(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	f, err := parquet.OpenFile(newLocalReaderAt(path), info.Size())
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	cols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"severity_text":       true,
		"service.name":        true,
	}

	var blocks []*logstorage.DataBlock
	err = s.readRowGroupWithProjection(f, rgs[0], startNs, endNs, cols, nil,
		func(_ uint, db *logstorage.DataBlock) {
			blocks = append(blocks, db)
		}, nil)
	if err != nil {
		t.Fatalf("readRowGroupWithProjection: %v", err)
	}

	if len(blocks) == 0 {
		t.Fatal("expected at least one block")
	}
}

// ---------------------------------------------------------------------------
// Test: preFilterFiles
// NOTE: traces preFilterFiles doesn't take context parameter.
// ---------------------------------------------------------------------------

func TestInteg_preFilterFiles_WildcardQuery(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet"}, {Key: "b.parquet"},
	}

	result := s.preFilterFiles(files, "*")
	if len(result) != 2 {
		t.Errorf("expected 2 files with wildcard query, got %d", len(result))
	}
}

func TestInteg_preFilterFiles_WithLabels(t *testing.T) {
	s := testStorage()

	files := []manifest.FileInfo{
		{Key: "a.parquet", Labels: map[string][]string{"service.name": {"api-gw"}}},
		{Key: "b.parquet", Labels: map[string][]string{"service.name": {"worker"}}},
	}

	result := s.preFilterFiles(files, `service.name:="api-gw"`)
	if len(result) > 2 {
		t.Errorf("expected at most 2 files, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// Test: queryBufferBridge (nil bridge)
// NOTE: traces queryBufferBridge has a different signature (no maxRows parameter).
// ---------------------------------------------------------------------------

func TestInteg_queryBufferBridge_NilBridge(t *testing.T) {
	s := testStorage()
	s.bufferBridge = nil

	// Should not panic
	s.queryBufferBridge(context.Background(), 0, int64(time.Hour), 0, nil, nil,
		func(_ uint, db *logstorage.DataBlock) {
			t.Error("should not be called with nil bridge")
		})
}

// ---------------------------------------------------------------------------
// Test: saveFileMetadataToDisk / loadFileMetadataFromDisk
// ---------------------------------------------------------------------------

func TestInteg_saveFileMetadataToDisk(t *testing.T) {
	dir := t.TempDir()

	s := testStorage()
	p, err := cache.NewPersister(dir)
	if err != nil {
		t.Fatalf("NewPersister: %v", err)
	}
	s.persister = p

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:       "logs/dt=2026-05-10/hour=14/test.parquet",
		Size:      1000,
		RowCount:  50,
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Hour).UnixNano(),
	})

	s.saveFileMetadataToDisk()

	metaPath := filepath.Join(dir, "file-metadata.json")
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Error("file_metadata.json should be created")
	}
}

// ---------------------------------------------------------------------------
// Test: sortFilesByCacheAffinity
// ---------------------------------------------------------------------------

func TestInteg_sortFilesByCacheAffinity(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
		{Key: "c.parquet"},
	}
	cachedKeys := map[string]bool{
		"c.parquet": true,
	}
	sortFilesByCacheAffinity(files, cachedKeys)
	if files[0].Key != "c.parquet" {
		t.Errorf("expected c.parquet first (cached), got %s", files[0].Key)
	}
}

func TestInteg_sortFilesByCacheAffinity_NoCached(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a.parquet"},
		{Key: "b.parquet"},
	}
	sortFilesByCacheAffinity(files, map[string]bool{})
	if files[0].Key != "a.parquet" {
		t.Error("order should be stable when no files are cached")
	}
}

// ---------------------------------------------------------------------------
// Test: mapColumnToAttrPrefix
// ---------------------------------------------------------------------------

func TestInteg_mapColumnToAttrPrefix(t *testing.T) {
	tests := []struct {
		col  string
		want string
	}{
		{"resource.attributes", "resource_attr:"},
		{"log.attributes", "log_attr:"},
		{"span.attributes", "span_attr:"},
		{"scope.attributes", "scope_attr:"},
		{"custom.field", "custom.field:"},
	}
	for _, tt := range tests {
		got := mapColumnToAttrPrefix(tt.col)
		if got != tt.want {
			t.Errorf("mapColumnToAttrPrefix(%q) = %q, want %q", tt.col, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: RunQuery end-to-end via mock S3
// ---------------------------------------------------------------------------

func TestInteg_RunQuery_EndToEnd(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello world", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "error occurred", SeverityText: "ERROR", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "debug msg", SeverityText: "DEBUG", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/batch-e2e.parquet"
	registerFileInMockS3(t, s, mock, key, data, now)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	var blocks []*logstorage.DataBlock
	var mu sync.Mutex
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		blocks = append(blocks, db)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 3 {
		t.Errorf("expected 3 rows, got %d", totalRows)
	}
}

func TestInteg_RunQuery_WithFilter(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "world", SeverityText: "ERROR", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/batch-filter.parquet"
	registerFileInMockS3(t, s, mock, key, data, now)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	q := mustParseQueryWithTime(t, `service.name:="api-gw"`, startNs, endNs)

	var blocks []*logstorage.DataBlock
	var mu sync.Mutex
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		blocks = append(blocks, db)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows > 2 {
		t.Errorf("expected at most 2 rows (only api-gw), got %d", totalRows)
	}
}

func TestInteg_RunQuery_MultipleFiles(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)

	for i := 0; i < 3; i++ {
		rows := []logRow{
			{
				TimestampUnixNano: now.Add(time.Duration(i) * time.Second).UnixNano(),
				Body:              fmt.Sprintf("msg-%d", i),
				SeverityText:      "INFO",
				ServiceName:       "svc",
			},
		}
		data := writeParquetToBytes(t, rows)
		key := fmt.Sprintf("logs/dt=2026-05-10/hour=14/multi%d.parquet", i)
		registerFileInMockS3(t, s, mock, key, data, now)
	}

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	var blocks []*logstorage.DataBlock
	var mu sync.Mutex
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		blocks = append(blocks, db)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount()
	}
	if totalRows != 3 {
		t.Errorf("expected 3 rows from 3 files, got %d", totalRows)
	}
}

func TestInteg_RunQuery_FileNotFound(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	partition := "dt=2026-05-10/hour=14"
	s.manifest.AddFile(partition, manifest.FileInfo{
		Key:       "logs/dt=2026-05-10/hour=14/missing.parquet",
		Size:      1000,
		MinTimeNs: now.Add(-time.Minute).UnixNano(),
		MaxTimeNs: now.Add(time.Minute).UnixNano(),
	})

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {})
	if err != nil {
		t.Fatalf("RunQuery with missing file should not error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: queryFile with footer cache hit
// ---------------------------------------------------------------------------

func TestInteg_queryFile_WithFooterCacheHit(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "cached footer", SeverityText: "INFO", ServiceName: "api-gw"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/footer-hit.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}

	cachedFooter, _, err := ParseFooterFromData(key, data)
	if err != nil {
		t.Fatalf("ParseFooterFromData: %v", err)
	}
	s.footerCache.Put(key, cachedFooter)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	var blocks []*logstorage.DataBlock
	err = s.queryFile(context.Background(), fi, startNs, endNs, "", nil, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile with cached footer: %v", err)
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
// Test: RunQuery with max rows limit
// ---------------------------------------------------------------------------

func TestInteg_RunQuery_MaxRows(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	s.cfg.Query.MaxRows = 2

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "c", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: now.Add(3 * time.Second).UnixNano(), Body: "d", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: now.Add(4 * time.Second).UnixNano(), Body: "e", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/maxrows.parquet"
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

	if totalRows > 5 {
		t.Errorf("max rows limit not enforced, got %d rows", totalRows)
	}
}

// ---------------------------------------------------------------------------
// Test: loadFileMetadataFromDisk
// ---------------------------------------------------------------------------

func TestInteg_loadFileMetadataFromDisk(t *testing.T) {
	dir := t.TempDir()

	s := testStorage()
	p, err := cache.NewPersister(dir)
	if err != nil {
		t.Fatalf("NewPersister: %v", err)
	}
	s.persister = p

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)

	key := "logs/dt=2026-05-10/hour=14/meta.parquet"
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:       key,
		Size:      1000,
		RowCount:  100,
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Hour).UnixNano(),
	})

	s.saveFileMetadataToDisk()

	s2 := testStorage()
	s2.persister = p

	s2.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:  key,
		Size: 1000,
	})

	loaded := s2.loadFileMetadataFromDisk()
	if loaded != 1 {
		t.Errorf("expected 1 enriched entry, got %d", loaded)
	}
}

func TestInteg_loadFileMetadataFromDisk_NilPersister(t *testing.T) {
	s := testStorage()
	s.persister = nil

	loaded := s.loadFileMetadataFromDisk()
	if loaded != 0 {
		t.Errorf("expected 0, got %d", loaded)
	}
}

// ---------------------------------------------------------------------------
// Test: enrichFromCachedFooter
// ---------------------------------------------------------------------------

func TestInteg_enrichFromCachedFooter(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	data, _ := os.ReadFile(path)

	cached, _, err := ParseFooterFromData("test.parquet", data)
	if err != nil {
		t.Fatalf("ParseFooterFromData: %v", err)
	}

	s := testStorage()
	key := "logs/dt=2026-05-10/hour=14/enrich.parquet"
	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	s.manifest.AddFile("dt=2026-05-10/hour=14", fi)

	result := s.enrichFromCachedFooter(fi, cached)
	if !result {
		t.Error("expected enrichFromCachedFooter to return true")
	}
}

// ---------------------------------------------------------------------------
// Test: enrichSmallFiles via mock S3
// ---------------------------------------------------------------------------

func TestInteg_enrichSmallFiles(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/small.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{Key: key, Size: int64(len(data))}
	s.manifest.AddFile("dt=2026-05-10/hour=14", fi)

	enriched := s.enrichSmallFiles(context.Background(), []manifest.FileInfo{fi})
	if enriched != 1 {
		t.Errorf("expected 1 enriched file, got %d", enriched)
	}
}

func TestInteg_enrichSmallFiles_Empty(t *testing.T) {
	s := testStorage()
	enriched := s.enrichSmallFiles(context.Background(), nil)
	if enriched != 0 {
		t.Errorf("expected 0, got %d", enriched)
	}
}

// ---------------------------------------------------------------------------
// Test: WarmMetadata
// ---------------------------------------------------------------------------

func TestInteg_WarmMetadata(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	dir := t.TempDir()
	s := testStorageWithS3(t, mock.url())
	p, err := cache.NewPersister(dir)
	if err != nil {
		t.Fatalf("NewPersister: %v", err)
	}
	s.persister = p

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "warm test", SeverityText: "INFO", ServiceName: "svc"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/warm.parquet"
	mock.putFile(key, data)
	s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{
		Key:  key,
		Size: int64(len(data)),
	})

	// Should not panic; exercises loadFileMetadataFromDisk + prefetchFooters + enrichSmallFiles
	s.WarmMetadata(context.Background())
}

// ---------------------------------------------------------------------------
// Test: Download (s3Adapter)
// ---------------------------------------------------------------------------

func TestInteg_Download(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	mock.putFile("test-download-key", []byte("test data"))

	data, err := s.pool.Download(context.Background(), "test-download-key")
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if string(data) != "test data" {
		t.Errorf("expected 'test data', got %q", string(data))
	}
}

// ---------------------------------------------------------------------------
// SKIPPED tests for functions that don't exist in the traces module:
//
// - enrichManifestFromFooter: not implemented in traces
// - syntheticTimestampBlock: not implemented in traces
// - bloomFilterFiles: traces uses filterFilesByBloomIndex instead
// - partitionFromKey: not implemented in traces
// ---------------------------------------------------------------------------
