package parquets3

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type logRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	SeverityText      string `parquet:"severity_text"`
	ServiceName       string `parquet:"service.name"`
}

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	return cfg
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func writeTestParquet(t *testing.T, dir string, rows []logRow) string {
	t.Helper()
	path := filepath.Join(dir, "test.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[logRow](f, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestStorage_ImplementsInterface(t *testing.T) {
	var _ storage.Storage = (*Storage)(nil)
}

func TestRunQuery_EmptyManifest(t *testing.T) {
	s := &Storage{
		cfg:      testConfig(),
		logger:   testLogger(),
		manifest: manifest.New("test", "logs/", testLogger()),
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	called := false
	err := s.RunQuery(context.Background(), &storage.QueryContext{
		StartNs: time.Now().Add(-time.Hour).UnixNano(),
		EndNs:   time.Now().UnixNano(),
	}, func(workerID uint, db *storage.DataBlock) {
		called = true
	})
	if err != nil {
		t.Errorf("RunQuery: %v", err)
	}
	if called {
		t.Error("empty manifest should not call writeBlock")
	}
}

func TestQueryFile_ReadsParquet(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 5, 2, 10, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "hello world", SeverityText: "INFO", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "error occurred", SeverityText: "ERROR", ServiceName: "api-gw"},
		{TimestampUnixNano: now.Add(2 * time.Second).UnixNano(), Body: "debug msg", SeverityText: "DEBUG", ServiceName: "worker"},
	}
	path := writeTestParquet(t, dir, rows)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	s := &Storage{
		cfg:      testConfig(),
		logger:   testLogger(),
		manifest: manifest.New("test", "logs/", testLogger()),
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	fi := manifest.FileInfo{Key: path, Size: info.Size()}

	var blocks []*storage.DataBlock
	err = s.queryLocalFile(path, fi.Size, &storage.QueryContext{
		StartNs: now.Add(-time.Minute).UnixNano(),
		EndNs:   now.Add(time.Minute).UnixNano(),
	}, func(workerID uint, db *storage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("queryFile: %v", err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount
	}
	if totalRows != 3 {
		t.Errorf("expected 3 rows, got %d", totalRows)
	}

	// Verify column name mapping
	if len(blocks) > 0 {
		colNames := make(map[string]bool)
		for _, col := range blocks[0].Columns {
			colNames[col.Name] = true
		}
		if !colNames["_time"] {
			t.Error("expected _time column (mapped from timestamp_unix_nano)")
		}
		if !colNames["_msg"] {
			t.Error("expected _msg column (mapped from body)")
		}
		if !colNames["level"] {
			t.Error("expected level column (mapped from severity_text)")
		}
		if !colNames["service.name"] {
			t.Error("expected service.name column")
		}
	}
}

func TestQueryFile_TimeRangeFilter(t *testing.T) {
	dir := t.TempDir()

	base := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: base.UnixNano(), Body: "hour 10", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: base.Add(time.Hour).UnixNano(), Body: "hour 11", SeverityText: "INFO", ServiceName: "svc"},
		{TimestampUnixNano: base.Add(2 * time.Hour).UnixNano(), Body: "hour 12", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	s := &Storage{
		cfg:      testConfig(),
		logger:   testLogger(),
		manifest: manifest.New("test", "logs/", testLogger()),
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	var blocks []*storage.DataBlock
	err := s.queryLocalFile(path, info.Size(), &storage.QueryContext{
		StartNs: base.Add(30 * time.Minute).UnixNano(),
		EndNs:   base.Add(90 * time.Minute).UnixNano(),
	}, func(workerID uint, db *storage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatal(err)
	}

	totalRows := 0
	for _, b := range blocks {
		totalRows += b.RowsCount
	}
	if totalRows != 1 {
		t.Errorf("expected 1 row in time range, got %d", totalRows)
	}
}

func TestGetFieldNames_FromParquet(t *testing.T) {
	dir := t.TempDir()

	rows := []logRow{
		{TimestampUnixNano: time.Now().UnixNano(), Body: "test", SeverityText: "INFO", ServiceName: "svc"},
	}
	path := writeTestParquet(t, dir, rows)
	info, _ := os.Stat(path)

	m := manifest.New("test", "logs/", testLogger())
	m.AddFile("dt=2026-05-02/hour=10", manifest.FileInfo{Key: path, Size: info.Size()})

	s := &Storage{
		cfg:      testConfig(),
		logger:   testLogger(),
		manifest: m,
		registry: schema.NewRegistry(schema.LogsProfile),
		pool:     nil, // not used for local files
	}

	fields, err := s.getFieldNamesLocal(context.Background(), &storage.QueryContext{
		StartNs: time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC).UnixNano(),
		EndNs:   time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC).UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(fields) == 0 {
		t.Fatal("expected field names")
	}

	names := make(map[string]bool)
	for _, f := range fields {
		names[f.Value] = true
	}

	for _, expected := range []string{"_time", "_msg", "level", "service.name"} {
		if !names[expected] {
			t.Errorf("missing expected field %q", expected)
		}
	}
}

func TestClose(t *testing.T) {
	s := &Storage{
		cfg:      testConfig(),
		logger:   testLogger(),
		manifest: manifest.New("test", "logs/", testLogger()),
		registry: schema.NewRegistry(schema.LogsProfile),
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// queryLocalFile opens a local Parquet file instead of S3
func (s *Storage) queryLocalFile(path string, size int64, qctx *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
	f, err := parquet.OpenFile(newLocalReaderAt(path), size)
	if err != nil {
		return err
	}

	tsIdx := findColumnIndex(f.Root(), s.registry.TimestampColumn())

	for _, rg := range f.RowGroups() {
		if tsIdx >= 0 && !rowGroupMatchesTimeRange(rg, tsIdx, qctx.StartNs, qctx.EndNs) {
			continue
		}
		if err := s.readRowGroup(f, rg, qctx, writeBlock); err != nil {
			return err
		}
	}
	return nil
}

// getFieldNamesLocal reads field names from local files referenced by the manifest
func (s *Storage) getFieldNamesLocal(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	files := s.manifest.GetFilesForRange(qctx.StartNs, qctx.EndNs)
	if len(files) == 0 {
		return nil, nil
	}

	fi := files[0]
	f, err := parquet.OpenFile(newLocalReaderAt(fi.Key), fi.Size)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var result []storage.ValueWithHits
	for _, name := range columnNames(f.Root()) {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}
		if !seen[internalName] {
			seen[internalName] = true
			result = append(result, storage.ValueWithHits{Value: internalName, Hits: 1})
		}
	}
	return result, nil
}

type localReaderAt struct {
	f *os.File
}

func newLocalReaderAt(path string) *localReaderAt {
	f, _ := os.Open(path)
	return &localReaderAt{f: f}
}

func (r *localReaderAt) ReadAt(p []byte, off int64) (int, error) {
	return r.f.ReadAt(p, off)
}
