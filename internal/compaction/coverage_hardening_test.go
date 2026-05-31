package compaction

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// (PartitionSharding negative/zero shardCount coverage tests removed in PR A —
// sharding.go was deleted; HRW ownership replaces the modulo-shard scheme.
// Single-pod and degraded-discovery behavior is covered in ownership_test.go.)

// TestWriteCompactedLogs_VariousCompressionLevels exercises different
// compression level paths through zstdLevel.
func TestWriteCompactedLogs_VariousCompressionLevels(t *testing.T) {
	input := []schema.LogRow{
		{TimestampUnixNano: 100, Body: "hello", ServiceName: "svc"},
		{TimestampUnixNano: 200, Body: "world", ServiceName: "svc"},
	}

	tests := []struct {
		name  string
		level int
	}{
		{"fastest", 1},
		{"default", 3},
		{"better", 7},
		{"best", 15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := writeCompactedLogs(input, 100, tt.level)
			if err != nil {
				t.Fatalf("writeCompactedLogs level %d: %v", tt.level, err)
			}
			if len(data) == 0 {
				t.Fatal("expected non-empty output")
			}

			// Verify roundtrip.
			rows, err := readLogRows(data)
			if err != nil {
				t.Fatalf("readLogRows: %v", err)
			}
			if len(rows) != 2 {
				t.Fatalf("expected 2 rows, got %d", len(rows))
			}
		})
	}
}

// TestWriteCompactedTraces_VariousCompressionLevels exercises different
// compression level paths through zstdLevel.
func TestWriteCompactedTraces_VariousCompressionLevels(t *testing.T) {
	input := []schema.TraceRow{
		{TimestampUnixNano: 100, TraceID: "t1", SpanID: "s1", ServiceName: "svc"},
		{TimestampUnixNano: 200, TraceID: "t2", SpanID: "s2", ServiceName: "svc"},
	}

	tests := []struct {
		name  string
		level int
	}{
		{"fastest", 0},
		{"default", 5},
		{"better", 10},
		{"best", 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := writeCompactedTraces(input, 100, tt.level)
			if err != nil {
				t.Fatalf("writeCompactedTraces level %d: %v", tt.level, err)
			}
			if len(data) == 0 {
				t.Fatal("expected non-empty output")
			}

			rows, err := readTraceRows(data)
			if err != nil {
				t.Fatalf("readTraceRows: %v", err)
			}
			if len(rows) != 2 {
				t.Fatalf("expected 2 rows, got %d", len(rows))
			}
		})
	}
}

// TestWriteCompactedLogs_SmallRowGroupSize exercises the row group splitting.
func TestWriteCompactedLogs_SmallRowGroupSize(t *testing.T) {
	input := make([]schema.LogRow, 10)
	for i := range input {
		input[i] = schema.LogRow{
			TimestampUnixNano: int64(i * 100),
			Body:              "msg",
			ServiceName:       "svc",
		}
	}

	data, err := writeCompactedLogs(input, 3, 1)
	if err != nil {
		t.Fatalf("writeCompactedLogs: %v", err)
	}

	// Verify all rows are present.
	reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()
	if reader.NumRows() != 10 {
		t.Errorf("NumRows = %d, want 10", reader.NumRows())
	}
}

// TestWriteCompactedTraces_SmallRowGroupSize exercises the row group splitting.
func TestWriteCompactedTraces_SmallRowGroupSize(t *testing.T) {
	input := make([]schema.TraceRow, 10)
	for i := range input {
		input[i] = schema.TraceRow{
			TimestampUnixNano: int64(i * 100),
			TraceID:           "t1",
			SpanID:            "s1",
			ServiceName:       "svc",
		}
	}

	data, err := writeCompactedTraces(input, 3, 1)
	if err != nil {
		t.Fatalf("writeCompactedTraces: %v", err)
	}

	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()
	if reader.NumRows() != 10 {
		t.Errorf("NumRows = %d, want 10", reader.NumRows())
	}
}

// TestWriteCompactedLogs_SingleRow exercises the single-row edge case.
func TestWriteCompactedLogs_SingleRow(t *testing.T) {
	input := []schema.LogRow{
		{TimestampUnixNano: 100, Body: "single", ServiceName: "svc"},
	}

	data, err := writeCompactedLogs(input, 100, 1)
	if err != nil {
		t.Fatalf("writeCompactedLogs: %v", err)
	}

	rows, err := readLogRows(data)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Body != "single" {
		t.Errorf("Body = %q, want single", rows[0].Body)
	}
}

// TestCompactor_SchemaFingerprintMismatch_FromCompact exercises the
// schema fingerprint mismatch error path directly in Compact.
func TestCompactor_SchemaFingerprintMismatch_FromCompact(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")

	compactor := NewCompactor(CompactorConfig{
		Pool:     pool,
		Manifest: m,
		Prefix:   "logs/",
		Mode:     config.ModeLogs,
	})

	_, err := compactor.Compact(context.Background(), "dt=2026-05-01/hour=10", []manifest.FileInfo{
		{Key: "a.parquet", SchemaFingerprint: "fp-a"},
		{Key: "b.parquet", SchemaFingerprint: "fp-b"},
	}, 0)
	if err == nil {
		t.Fatal("expected error for fingerprint mismatch")
	}
	if !strings.Contains(err.Error(), "schema fingerprint mismatch") {
		t.Errorf("unexpected error: %v", err)
	}
}

// mockBloomRebuilder implements BloomRebuilder for testing.
type mockBloomRebuilder struct {
	called    bool
	partition string
	err       error
}

func (m *mockBloomRebuilder) RebuildPartition(_ context.Context, partition string, _ []manifest.FileInfo) error {
	m.called = true
	m.partition = partition
	return m.err
}

// TestCompactor_WithBloomRebuilder exercises the BloomRebuilder path.
func TestCompactor_WithBloomRebuilder(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "abc123"
	ctx := context.Background()

	// Create two parquet files.
	for i := 0; i < 2; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: "msg", ServiceName: "svc"},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := pool.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

	rebuilder := &mockBloomRebuilder{}
	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     1000,
		CompressionLevel: 1,
		BloomRebuilder:   rebuilder,
	})

	files := m.FilesForPartition(partition)
	_, err := compactor.Compact(ctx, partition, files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if !rebuilder.called {
		t.Error("expected BloomRebuilder to be called")
	}
	if rebuilder.partition != partition {
		t.Errorf("BloomRebuilder partition = %q, want %q", rebuilder.partition, partition)
	}
}

// TestCompactor_WithBloomRebuilder_Error exercises the error path in
// BloomRebuilder (error is logged but not returned).
func TestCompactor_WithBloomRebuilder_Error(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "abc123"
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: "msg", ServiceName: "svc"},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := pool.Upload(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		m.AddFile(partition, manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

	rebuilder := &mockBloomRebuilder{err: fmt.Errorf("bloom rebuild failed")}
	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     1000,
		CompressionLevel: 1,
		BloomRebuilder:   rebuilder,
	})

	files := m.FilesForPartition(partition)
	result, err := compactor.Compact(ctx, partition, files, 0)
	// Compact should still succeed; bloom error is only logged.
	if err != nil {
		t.Fatalf("Compact should succeed despite bloom error: %v", err)
	}
	if result.RowsMerged != 2 {
		t.Errorf("RowsMerged = %d, want 2", result.RowsMerged)
	}
}

// TestCompactor_DeleteError exercises the path where deleting source files
// fails (error is logged but compaction still succeeds).
func TestCompactor_DeleteError(t *testing.T) {
	pool := newErrorPool()
	m := manifest.New("test-bucket", "logs/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "abc123"
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		rows := []schema.LogRow{
			{TimestampUnixNano: int64(i*1000 + 1), Body: "msg", ServiceName: "svc"},
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		pool.uploaded[key] = data
		m.AddFile(partition, manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          1,
			SchemaFingerprint: fp,
			CompactionLevel:   0,
		})
	}

	// Set delete error after files are uploaded.
	pool.deleteErr = fmt.Errorf("simulated delete failure")

	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     1000,
		CompressionLevel: 1,
	})

	files := m.FilesForPartition(partition)
	result, err := compactor.Compact(ctx, partition, files, 0)
	// Compact should succeed; delete error is only logged.
	if err != nil {
		t.Fatalf("Compact should succeed despite delete error: %v", err)
	}
	if result.RowsMerged != 2 {
		t.Errorf("RowsMerged = %d, want 2", result.RowsMerged)
	}
}
