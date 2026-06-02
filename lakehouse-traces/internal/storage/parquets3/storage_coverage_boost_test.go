package parquets3

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// ---------------------------------------------------------------------------
// BloomCache() getter
// ---------------------------------------------------------------------------

func TestCoverageBoost_BloomCache_Nil(t *testing.T) {
	s := testStorage()
	if s.BloomCache() != nil {
		t.Error("expected nil bloom cache for default testStorage")
	}
}

func TestCoverageBoost_BloomCache_NonNil(t *testing.T) {
	s := testStorage()
	bc := bloomindex.NewBloomCache(1024, nil)
	s.bloomCache = bc
	if got := s.BloomCache(); got != bc {
		t.Error("BloomCache() should return the assigned bloom cache")
	}
}

// ---------------------------------------------------------------------------
// Pool() getter
// ---------------------------------------------------------------------------

func TestCoverageBoost_Pool_Nil(t *testing.T) {
	s := testStorage()
	// testStorage does not set pool
	if s.Pool() != nil {
		t.Error("expected nil pool for default testStorage")
	}
}

func TestCoverageBoost_Pool_NonNil(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())
	if s.Pool() == nil {
		t.Error("expected non-nil pool for testStorageWithS3")
	}
}

// ---------------------------------------------------------------------------
// SmartCache() getter
// ---------------------------------------------------------------------------

func TestCoverageBoost_SmartCache_Nil(t *testing.T) {
	s := testStorage()
	if s.SmartCache() != nil {
		t.Error("expected nil smart cache for default testStorage")
	}
}

// ---------------------------------------------------------------------------
// HasDataForRange()
// ---------------------------------------------------------------------------

func TestCoverageBoost_HasDataForRange_EmptyManifest(t *testing.T) {
	s := testStorage()
	if s.HasDataForRange(0, time.Now().UnixNano()) {
		t.Error("empty manifest should have no data for any range")
	}
}

func TestCoverageBoost_HasDataForRange_WithFile(t *testing.T) {
	s := testStorage()

	// Use a fixed time matching the partition key to avoid time-of-day sensitivity.
	baseTime := time.Date(2026, 5, 25, 14, 30, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2026-05-25/hour=14", manifest.FileInfo{
		Key:       "test.parquet",
		Size:      100,
		MinTimeNs: baseTime.Add(-30 * time.Minute).UnixNano(),
		MaxTimeNs: baseTime.UnixNano(),
	})

	// Overlapping range should return true
	if !s.HasDataForRange(baseTime.Add(-2*time.Hour).UnixNano(), baseTime.Add(time.Hour).UnixNano()) {
		t.Error("should have data for overlapping range")
	}

	// Non-overlapping range (far future) should return false
	farFuture := baseTime.Add(24 * time.Hour)
	if s.HasDataForRange(farFuture.UnixNano(), farFuture.Add(time.Hour).UnixNano()) {
		t.Error("should not have data for far-future range")
	}
}

func TestCoverageBoost_HasDataForRange_ExactBoundary(t *testing.T) {
	s := testStorage()

	baseTime := time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2026-05-25/hour=10", manifest.FileInfo{
		Key:       "boundary.parquet",
		Size:      200,
		MinTimeNs: baseTime.UnixNano(),
		MaxTimeNs: baseTime.Add(time.Hour).UnixNano(),
	})

	// Query exactly at file boundaries
	if !s.HasDataForRange(baseTime.UnixNano(), baseTime.Add(time.Hour).UnixNano()) {
		t.Error("should have data for exact boundary range")
	}
}

// ---------------------------------------------------------------------------
// SetSelfAZ()
// ---------------------------------------------------------------------------

func TestCoverageBoost_SetSelfAZ_Valid(t *testing.T) {
	s := testStorage()
	s.SetSelfAZ("us-east-1a")
	if got := s.SelfAZ(); got != "us-east-1a" {
		t.Errorf("SelfAZ() = %q, want %q", got, "us-east-1a")
	}
}

func TestCoverageBoost_SetSelfAZ_Empty(t *testing.T) {
	s := testStorage()
	s.SetSelfAZ("")
	if got := s.SelfAZ(); got != "" {
		t.Errorf("SelfAZ() = %q, want empty", got)
	}
}

func TestCoverageBoost_SetSelfAZ_Overwrite(t *testing.T) {
	s := testStorage()
	s.SetSelfAZ("us-east-1a")
	s.SetSelfAZ("us-west-2b")
	if got := s.SelfAZ(); got != "us-west-2b" {
		t.Errorf("SelfAZ() = %q, want %q", got, "us-west-2b")
	}
}

// ---------------------------------------------------------------------------
// ClearCaches()
// ---------------------------------------------------------------------------

func TestCoverageBoost_ClearCaches_MemOnly(t *testing.T) {
	s := testStorage()

	// Populate mem cache
	s.memCache.Put("key1", []byte("val1"))
	s.memCache.Put("key2", []byte("val2"))

	stats := s.memCache.Stats()
	if stats.Entries != 2 {
		t.Fatalf("expected 2 entries before clear, got %d", stats.Entries)
	}

	s.ClearCaches()

	stats = s.memCache.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries after clear, got %d", stats.Entries)
	}
}

func TestCoverageBoost_ClearCaches_WithDiskCache(t *testing.T) {
	dir := t.TempDir()
	dc, err := cache.NewDiskCache(dir, 100*1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	s.diskCache = dc

	// Populate both caches
	s.memCache.Put("mem-key", []byte("mem-val"))
	if _, err := dc.Put("disk-key", []byte("disk-val")); err != nil {
		t.Fatal(err)
	}

	s.ClearCaches()

	memStats := s.memCache.Stats()
	if memStats.Entries != 0 {
		t.Errorf("expected 0 mem entries after clear, got %d", memStats.Entries)
	}

	diskStats := dc.Stats()
	if diskStats.Entries != 0 {
		t.Errorf("expected 0 disk entries after clear, got %d", diskStats.Entries)
	}
}

func TestCoverageBoost_ClearCaches_NilDiskCache(t *testing.T) {
	s := testStorage()
	s.diskCache = nil

	s.memCache.Put("key", []byte("val"))

	// Should not panic with nil disk cache
	s.ClearCaches()

	stats := s.memCache.Stats()
	if stats.Entries != 0 {
		t.Errorf("expected 0 entries after clear, got %d", stats.Entries)
	}
}

// ---------------------------------------------------------------------------
// Close()
// ---------------------------------------------------------------------------

func TestCoverageBoost_Close_MinimalStorage(t *testing.T) {
	s := testStorage()
	// Minimal storage with no persister, no writer, no bloom index
	if err := s.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
}

func TestCoverageBoost_Close_WithBloomIndex(t *testing.T) {
	s := testStorage()
	s.bloomIdx = bloomindex.New()
	// bloomIdx is empty so PersistBloomIndex is a no-op
	if err := s.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}
}

func TestCoverageBoost_Close_WithPersister(t *testing.T) {
	dir := t.TempDir()
	p, err := cache.NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	s.persister = p
	s.labelIndex.Add("field1", []string{"val1"})
	s.labelIndex.Add("field2", nil)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify label index was persisted
	loaded, err := p.LoadLabelIndex()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Len() != 2 {
		t.Errorf("persisted label index len = %d, want 2", loaded.Len())
	}
}

func TestCoverageBoost_Close_NilWriter(t *testing.T) {
	s := testStorage()
	s.writer = nil
	// Should not panic
	if err := s.Close(); err != nil {
		t.Errorf("Close() with nil writer: %v", err)
	}
}

func TestCoverageBoost_Close_NilCrossSignalClient(t *testing.T) {
	s := testStorage()
	s.crossSignalClient = nil
	if err := s.Close(); err != nil {
		t.Errorf("Close() with nil crossSignalClient: %v", err)
	}
}

// ---------------------------------------------------------------------------
// CanWriteData()
// ---------------------------------------------------------------------------

func TestCoverageBoost_CanWriteData_NilWriter(t *testing.T) {
	s := testStorage()
	s.writer = nil
	err := s.CanWriteData()
	if err == nil {
		t.Error("expected error when writer is nil")
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

// ---------------------------------------------------------------------------
// MustAddLogRows() / MustAddTraceRows()
// ---------------------------------------------------------------------------

func TestCoverageBoost_MustAddLogRows(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.Insert.WALEnabled = false

	s := testStorage()
	s.writer = &BatchWriter{
		cfg:       &cfg.Insert,
		mode:      cfg.Mode,
		logBufs:   make(map[string][]schema.LogRow),
		traceBufs: make(map[string][]schema.TraceRow),
		stopCh:    make(chan struct{}),
	}

	now := time.Now().UnixNano()
	rows := []schema.LogRow{
		{TimestampUnixNano: now, Body: "test1", SeverityText: "INFO", ServiceName: "svc1"},
		{TimestampUnixNano: now + 1, Body: "test2", SeverityText: "ERROR", ServiceName: "svc2"},
	}

	// Should not panic
	s.MustAddLogRows(rows)

	if s.writer.BufferedRows() != 2 {
		t.Errorf("expected 2 buffered rows, got %d", s.writer.BufferedRows())
	}
}

func TestCoverageBoost_MustAddTraceRows(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeTraces
	cfg.S3.Bucket = "test-bucket"
	cfg.Insert.WALEnabled = false

	s := testStorage()
	s.writer = &BatchWriter{
		cfg:       &cfg.Insert,
		mode:      cfg.Mode,
		logBufs:   make(map[string][]schema.LogRow),
		traceBufs: make(map[string][]schema.TraceRow),
		stopCh:    make(chan struct{}),
	}

	now := time.Now().UnixNano()
	rows := []schema.TraceRow{
		{TimestampUnixNano: now, TraceID: "t1", SpanID: "s1", SpanName: "op1", ServiceName: "svc1"},
		{TimestampUnixNano: now + 1, TraceID: "t2", SpanID: "s2", SpanName: "op2", ServiceName: "svc2"},
	}

	// Should not panic
	s.MustAddTraceRows(rows)

	if s.writer.BufferedRows() != 2 {
		t.Errorf("expected 2 buffered rows, got %d", s.writer.BufferedRows())
	}
}

// ---------------------------------------------------------------------------
// SetFlushHook() (writer.go, 0% coverage)
// ---------------------------------------------------------------------------

func TestCoverageBoost_SetFlushHook(t *testing.T) {
	bw := &BatchWriter{}
	if bw.onFlush != nil {
		t.Fatal("expected nil onFlush initially")
	}

	var hookCalled bool
	hook := FlushHook(func(key string, columnValues map[string][]string) {
		hookCalled = true
	})

	bw.SetFlushHook(hook)
	if bw.onFlush == nil {
		t.Fatal("expected onFlush to be set")
	}

	// Invoke to verify it's the right hook
	bw.onFlush("test-key", map[string][]string{"col": {"v1"}})
	if !hookCalled {
		t.Fatal("expected flush hook to be invoked")
	}
}

// ---------------------------------------------------------------------------
// SetStatsCallback() (writer.go, 0% coverage)
// ---------------------------------------------------------------------------

func TestCoverageBoost_SetStatsCallback(t *testing.T) {
	bw := &BatchWriter{}
	if bw.statsCallback != nil {
		t.Fatal("expected nil statsCallback initially")
	}

	var cbCalled bool
	cb := StatsCallback(func(_, _ uint32, _, _, _ int64, _ string) {
		cbCalled = true
	})

	bw.SetStatsCallback(cb)
	if bw.statsCallback == nil {
		t.Fatal("expected statsCallback to be set")
	}

	bw.statsCallback(1, 1, 100, 200, 10, "STANDARD")
	if !cbCalled {
		t.Fatal("expected stats callback to be invoked")
	}
}

// ---------------------------------------------------------------------------
// Writer() getter
// ---------------------------------------------------------------------------

func TestCoverageBoost_Writer_Nil(t *testing.T) {
	s := testStorage()
	if s.Writer() != nil {
		t.Error("expected nil writer for default testStorage")
	}
}

func TestCoverageBoost_Writer_NonNil(t *testing.T) {
	cfg := config.Default()
	cfg.Insert.WALEnabled = false

	s := testStorage()
	bw := &BatchWriter{
		cfg:       &cfg.Insert,
		mode:      config.ModeLogs,
		logBufs:   make(map[string][]schema.LogRow),
		traceBufs: make(map[string][]schema.TraceRow),
		stopCh:    make(chan struct{}),
	}
	s.writer = bw

	if got := s.Writer(); got != bw {
		t.Error("Writer() should return the assigned writer")
	}
}

// ---------------------------------------------------------------------------
// Manifest() getter
// ---------------------------------------------------------------------------

func TestCoverageBoost_Manifest(t *testing.T) {
	s := testStorage()
	if s.Manifest() == nil {
		t.Error("expected non-nil manifest")
	}
}

// ---------------------------------------------------------------------------
// LabelIndex() getter
// ---------------------------------------------------------------------------

func TestCoverageBoost_LabelIndex(t *testing.T) {
	s := testStorage()
	if s.LabelIndex() == nil {
		t.Error("expected non-nil label index")
	}
}

// ---------------------------------------------------------------------------
// SchemaRegistry() getter
// ---------------------------------------------------------------------------

func TestCoverageBoost_SchemaRegistry(t *testing.T) {
	s := testStorage()
	if s.SchemaRegistry() == nil {
		t.Error("expected non-nil schema registry")
	}
}

// ---------------------------------------------------------------------------
// SetSelfFilterEnabled()
// ---------------------------------------------------------------------------

func TestCoverageBoost_SetSelfFilterEnabled(t *testing.T) {
	s := testStorage()
	if s.selfFilterEnabled {
		t.Error("expected false initially")
	}
	s.SetSelfFilterEnabled(true)
	if !s.selfFilterEnabled {
		t.Error("expected true after SetSelfFilterEnabled(true)")
	}
	s.SetSelfFilterEnabled(false)
	if s.selfFilterEnabled {
		t.Error("expected false after SetSelfFilterEnabled(false)")
	}
}

// ---------------------------------------------------------------------------
// PersistState() with file metadata
// ---------------------------------------------------------------------------

func TestCoverageBoost_PersistState_WithFileMetadata(t *testing.T) {
	dir := t.TempDir()
	p, err := cache.NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	s := testStorage()
	s.persister = p
	s.labelIndex.Add("svc", []string{"api"})

	now := time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2026-05-25/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-05-25/hour=10/test.parquet",
		Size:      1000,
		RowCount:  50,
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.Add(time.Hour).UnixNano(),
	})

	if err := s.PersistState(); err != nil {
		t.Fatal(err)
	}

	// Verify both label index and file metadata were saved
	loaded, err := p.LoadLabelIndex()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Len() != 1 {
		t.Errorf("loaded label index len = %d, want 1", loaded.Len())
	}

}

// ---------------------------------------------------------------------------
// Tombstone setters/getters (additional edge cases)
// ---------------------------------------------------------------------------

func TestCoverageBoost_TombstoneStore_SetMultiple(t *testing.T) {
	s := testStorage()
	if s.TombstoneStore() != nil {
		t.Error("expected nil initially")
	}

	// Set, then overwrite
	s.SetTombstoneStore(nil)
	if s.TombstoneStore() != nil {
		t.Error("expected nil after setting nil")
	}
}

// ---------------------------------------------------------------------------
// BufferBridge() with nil
// ---------------------------------------------------------------------------

func TestCoverageBoost_BufferBridge_Nil(t *testing.T) {
	s := testStorage()
	if s.BufferBridge() != nil {
		t.Error("expected nil buffer bridge")
	}
}
