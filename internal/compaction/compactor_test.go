package compaction

import (
	"bytes"
	"context"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func makeTestParquet(t *testing.T, rows []schema.LogRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestCompactor_MergeLogRows(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "abc123"

	// File 1: 2 rows
	file1Rows := []schema.LogRow{
		{TimestampUnixNano: 3000, Body: "third", ServiceName: "svc-a"},
		{TimestampUnixNano: 1000, Body: "first", ServiceName: "svc-a"},
	}
	file1Data := makeTestParquet(t, file1Rows)
	file1Key := "logs/dt=2026-05-04/hour=10/batch-001.parquet"
	if err := pool.Upload(context.Background(), file1Key, file1Data); err != nil {
		t.Fatal(err)
	}

	// File 2: 1 row
	file2Rows := []schema.LogRow{
		{TimestampUnixNano: 2000, Body: "second", ServiceName: "svc-a"},
	}
	file2Data := makeTestParquet(t, file2Rows)
	file2Key := "logs/dt=2026-05-04/hour=10/batch-002.parquet"
	if err := pool.Upload(context.Background(), file2Key, file2Data); err != nil {
		t.Fatal(err)
	}

	// Register files in manifest.
	fi1 := manifest.FileInfo{
		Key:               file1Key,
		Size:              int64(len(file1Data)),
		RowCount:          2,
		MinTimeNs:         1000,
		MaxTimeNs:         3000,
		SchemaFingerprint: fp,
		CompactionLevel:   0,
	}
	fi2 := manifest.FileInfo{
		Key:               file2Key,
		Size:              int64(len(file2Data)),
		RowCount:          1,
		MinTimeNs:         2000,
		MaxTimeNs:         2000,
		SchemaFingerprint: fp,
		CompactionLevel:   0,
	}
	m.AddFile(partition, fi1)
	m.AddFile(partition, fi2)

	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     1000,
		CompressionLevel: 3,
	})

	result, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact error: %v", err)
	}

	// Verify result.
	if result.RowsMerged != 3 {
		t.Errorf("expected 3 rows merged, got %d", result.RowsMerged)
	}
	if result.OutputLevel != 1 {
		t.Errorf("expected output level 1, got %d", result.OutputLevel)
	}
	if result.Partition != partition {
		t.Errorf("expected partition %q, got %q", partition, result.Partition)
	}
	if len(result.InputFiles) != 2 {
		t.Errorf("expected 2 input files, got %d", len(result.InputFiles))
	}

	// Verify source files deleted from pool.
	pool.mu.Lock()
	if _, exists := pool.uploaded[file1Key]; exists {
		t.Error("source file 1 should be deleted from pool")
	}
	if _, exists := pool.uploaded[file2Key]; exists {
		t.Error("source file 2 should be deleted from pool")
	}
	pool.mu.Unlock()

	// Verify output file exists in pool.
	pool.mu.Lock()
	outputData, exists := pool.uploaded[result.OutputFile]
	pool.mu.Unlock()
	if !exists {
		t.Fatal("output file not found in pool")
	}

	// Read back the compacted file and verify rows are sorted.
	rows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows in output, got %d", len(rows))
	}
	if rows[0].TimestampUnixNano != 1000 {
		t.Errorf("expected first row ts=1000, got %d", rows[0].TimestampUnixNano)
	}
	if rows[1].TimestampUnixNano != 2000 {
		t.Errorf("expected second row ts=2000, got %d", rows[1].TimestampUnixNano)
	}
	if rows[2].TimestampUnixNano != 3000 {
		t.Errorf("expected third row ts=3000, got %d", rows[2].TimestampUnixNano)
	}

	// Verify manifest: source files removed, compacted file added.
	partFiles := m.FilesForPartition(partition)
	if len(partFiles) != 1 {
		t.Fatalf("expected 1 file in manifest partition, got %d", len(partFiles))
	}
	if partFiles[0].Key != result.OutputFile {
		t.Errorf("expected manifest file key %q, got %q", result.OutputFile, partFiles[0].Key)
	}
	if partFiles[0].CompactionLevel != 1 {
		t.Errorf("expected compaction level 1, got %d", partFiles[0].CompactionLevel)
	}
}

func TestCompactor_SchemaFingerprintMismatchSkipped(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")

	const partition = "dt=2026-05-04/hour=10"

	fi1 := manifest.FileInfo{
		Key:               "logs/dt=2026-05-04/hour=10/batch-001.parquet",
		SchemaFingerprint: "fp-aaa",
		CompactionLevel:   0,
	}
	fi2 := manifest.FileInfo{
		Key:               "logs/dt=2026-05-04/hour=10/batch-002.parquet",
		SchemaFingerprint: "fp-bbb",
		CompactionLevel:   0,
	}

	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     1000,
		CompressionLevel: 3,
	})

	_, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err == nil {
		t.Fatal("expected error for schema fingerprint mismatch, got nil")
	}
	// Verify the error message mentions mismatch.
	if got := err.Error(); got != "schema fingerprint mismatch: fp-aaa vs fp-bbb" {
		t.Errorf("unexpected error: %s", got)
	}
}
