package compaction

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
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
		CompressionLevel: 7,
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

// TestCompactor_PreservesRawBytes is a regression guard for the bug
// where the compactor's output FileInfo did not carry RawBytes
// forward from the input files. The omitempty default of 0 then
// silently zeroed every compacted file's raw-byte accounting while
// Size kept tracking the compressed bytes correctly — over time
// /api/v1/tenants reported total_bytes > raw_bytes (impossible
// compression ratio < 1.0) for any tenant whose files had been
// compacted.
func TestCompactor_PreservesRawBytes(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "abc123"

	file1Rows := []schema.LogRow{
		{TimestampUnixNano: 3000, Body: "third", ServiceName: "svc-a"},
		{TimestampUnixNano: 1000, Body: "first", ServiceName: "svc-a"},
	}
	file1Data := makeTestParquet(t, file1Rows)
	file1Key := "logs/dt=2026-05-04/hour=10/batch-001.parquet"
	if err := pool.Upload(context.Background(), file1Key, file1Data); err != nil {
		t.Fatal(err)
	}

	file2Rows := []schema.LogRow{
		{TimestampUnixNano: 2000, Body: "second", ServiceName: "svc-a"},
	}
	file2Data := makeTestParquet(t, file2Rows)
	file2Key := "logs/dt=2026-05-04/hour=10/batch-002.parquet"
	if err := pool.Upload(context.Background(), file2Key, file2Data); err != nil {
		t.Fatal(err)
	}

	const (
		raw1 = int64(420)
		raw2 = int64(180)
	)

	fi1 := manifest.FileInfo{
		Key:               file1Key,
		Size:              int64(len(file1Data)),
		RowCount:          2,
		MinTimeNs:         1000,
		MaxTimeNs:         3000,
		RawBytes:          raw1,
		SchemaFingerprint: fp,
		CompactionLevel:   0,
	}
	fi2 := manifest.FileInfo{
		Key:               file2Key,
		Size:              int64(len(file2Data)),
		RowCount:          1,
		MinTimeNs:         2000,
		MaxTimeNs:         2000,
		RawBytes:          raw2,
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
		CompressionLevel: 7,
	})

	result, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact error: %v", err)
	}

	partFiles := m.FilesForPartition(partition)
	if len(partFiles) != 1 {
		t.Fatalf("expected 1 file in manifest partition, got %d", len(partFiles))
	}
	merged := partFiles[0]
	if merged.Key != result.OutputFile {
		t.Errorf("manifest entry key mismatch: %q vs %q", merged.Key, result.OutputFile)
	}

	wantRaw := raw1 + raw2
	if merged.RawBytes != wantRaw {
		t.Errorf("compacted RawBytes = %d, want %d (sum of inputs %d + %d) — regression of compactor RawBytes drop",
			merged.RawBytes, wantRaw, raw1, raw2)
	}
	if merged.Size <= 0 {
		t.Errorf("compacted Size = %d, want > 0", merged.Size)
	}
}

// --- helpers for trace parquet ---

func makeTestTraceParquet(t *testing.T, rows []schema.TraceRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// --- error-returning mock pool ---

type errorPool struct {
	mu          sync.Mutex
	uploaded    map[string][]byte
	downloadErr error
	uploadErr   error
	deleteErr   error
}

func newErrorPool() *errorPool {
	return &errorPool{uploaded: make(map[string][]byte)}
}

func (e *errorPool) Upload(_ context.Context, key string, data []byte) error {
	if e.uploadErr != nil {
		return e.uploadErr
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.uploaded[key] = append([]byte(nil), data...)
	return nil
}

func (e *errorPool) Download(_ context.Context, key string) ([]byte, error) {
	if e.downloadErr != nil {
		return nil, e.downloadErr
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	d, ok := e.uploaded[key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), d...), nil
}

func (e *errorPool) Delete(_ context.Context, key string) error {
	if e.deleteErr != nil {
		return e.deleteErr
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.uploaded, key)
	return nil
}

// --- trace merge & compaction tests ---

func TestCompactor_MergeTraceRows(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "traces/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "trace-fp"

	file1Rows := []schema.TraceRow{
		{TimestampUnixNano: 3000, TraceID: "t1", SpanID: "s1", SpanName: "third", ServiceName: "svc-a"},
		{TimestampUnixNano: 1000, TraceID: "t1", SpanID: "s2", SpanName: "first", ServiceName: "svc-a"},
	}
	file1Data := makeTestTraceParquet(t, file1Rows)
	file1Key := "traces/dt=2026-05-04/hour=10/batch-001.parquet"
	if err := pool.Upload(context.Background(), file1Key, file1Data); err != nil {
		t.Fatal(err)
	}

	file2Rows := []schema.TraceRow{
		{TimestampUnixNano: 2000, TraceID: "t2", SpanID: "s3", SpanName: "second", ServiceName: "svc-b"},
	}
	file2Data := makeTestTraceParquet(t, file2Rows)
	file2Key := "traces/dt=2026-05-04/hour=10/batch-002.parquet"
	if err := pool.Upload(context.Background(), file2Key, file2Data); err != nil {
		t.Fatal(err)
	}

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
		Prefix:           "traces/",
		Mode:             config.ModeTraces,
		RowGroupSize:     1000,
		CompressionLevel: 7,
	})

	result, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact error: %v", err)
	}

	if result.RowsMerged != 3 {
		t.Errorf("expected 3 rows merged, got %d", result.RowsMerged)
	}
	if result.OutputLevel != 1 {
		t.Errorf("expected output level 1, got %d", result.OutputLevel)
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

	// Read back the compacted trace file and verify sorted order.
	pool.mu.Lock()
	outputData, exists := pool.uploaded[result.OutputFile]
	pool.mu.Unlock()
	if !exists {
		t.Fatal("output file not found in pool")
	}

	rows, err := readTraceRows(outputData)
	if err != nil {
		t.Fatalf("readTraceRows: %v", err)
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

	// Verify manifest.
	partFiles := m.FilesForPartition(partition)
	if len(partFiles) != 1 {
		t.Fatalf("expected 1 file in manifest, got %d", len(partFiles))
	}
	if partFiles[0].CompactionLevel != 1 {
		t.Errorf("expected compaction level 1, got %d", partFiles[0].CompactionLevel)
	}
}

// TestCompactor_PreservesTraceIndexFooter is the regression that closes
// the cold-tier trace-by-ID fast path across compaction. Before this
// fix, writeCompactedTraces dropped the `_trace_idx` Parquet KV
// metadata, so Storage.LookupTraceIndex returned a miss for every file
// the compactor produced — every cold trace-by-ID lookup degraded to a
// full span scan once the original files merged. We assert the index
// survives the round-trip with the same per-trace (start, end) bounds.
func TestCompactor_PreservesTraceIndexFooter(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "traces/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "trace-fp"

	// Two source files. Each carries a different trace; the compactor
	// must emit a merged file whose footer index covers both.
	src1 := []schema.TraceRow{
		{TimestampUnixNano: 1000, TraceID: "trace-alpha", SpanID: "a1", SpanName: "op1", ServiceName: "svc", StartTimeUnixNano: 1000, DurationNs: 500},
	}
	src2 := []schema.TraceRow{
		{TimestampUnixNano: 2000, TraceID: "trace-beta", SpanID: "b1", SpanName: "op2", ServiceName: "svc", StartTimeUnixNano: 2000, DurationNs: 750},
	}
	data1 := makeTestTraceParquet(t, src1)
	data2 := makeTestTraceParquet(t, src2)
	const key1 = "traces/dt=2026-05-04/hour=10/src-001.parquet"
	const key2 = "traces/dt=2026-05-04/hour=10/src-002.parquet"
	if err := pool.Upload(context.Background(), key1, data1); err != nil {
		t.Fatal(err)
	}
	if err := pool.Upload(context.Background(), key2, data2); err != nil {
		t.Fatal(err)
	}

	fi1 := manifest.FileInfo{Key: key1, Size: int64(len(data1)), RowCount: 1, MinTimeNs: 1000, MaxTimeNs: 1000, SchemaFingerprint: fp}
	fi2 := manifest.FileInfo{Key: key2, Size: int64(len(data2)), RowCount: 1, MinTimeNs: 2000, MaxTimeNs: 2000, SchemaFingerprint: fp}
	m.AddFile(partition, fi1)
	m.AddFile(partition, fi2)

	compactor := NewCompactor(CompactorConfig{
		Pool: pool, Manifest: m, Prefix: "traces/", Mode: config.ModeTraces,
		RowGroupSize: 1000, CompressionLevel: 7,
	})

	result, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	pool.mu.Lock()
	out, ok := pool.uploaded[result.OutputFile]
	pool.mu.Unlock()
	if !ok {
		t.Fatalf("output file %q not in pool", result.OutputFile)
	}

	// Open the compacted file via standard parquet-go and pull the KV
	// metadata. This is exactly what an external tool (duckdb, pyarrow,
	// parquet-tools) would do — proves the file is still pure Parquet.
	pf, err := parquet.OpenFile(bytes.NewReader(out), int64(len(out)))
	if err != nil {
		t.Fatalf("parquet.OpenFile: %v", err)
	}
	meta := pf.Metadata()
	if meta == nil {
		t.Fatal("compacted file has nil metadata")
	}
	var idxBlob string
	for _, kv := range meta.KeyValueMetadata {
		if kv.Key == traceindex.MetadataKey {
			idxBlob = kv.Value
			break
		}
	}
	if idxBlob == "" {
		t.Fatalf("compacted file is missing %q KV metadata — trace-by-ID fast path will miss on this file", traceindex.MetadataKey)
	}

	entries, err := traceindex.Unmarshal([]byte(idxBlob))
	if err != nil {
		t.Fatalf("traceindex.Unmarshal: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("merged index has %d entries, want 2 (alpha + beta)", len(entries))
	}
	got := map[string]traceindex.Entry{}
	for _, e := range entries {
		got[e.TraceID] = e
	}
	alpha, okA := got["trace-alpha"]
	if !okA || alpha.StartNs != 1000 || alpha.EndNs != 1500 {
		t.Errorf("trace-alpha index = %+v, want {Start:1000 End:1500}", alpha)
	}
	beta, okB := got["trace-beta"]
	if !okB || beta.StartNs != 2000 || beta.EndNs != 2750 {
		t.Errorf("trace-beta index = %+v, want {Start:2000 End:2750}", beta)
	}
	if alpha.Partition != traceindex.Partition("trace-alpha") {
		t.Errorf("alpha partition = %d, want %d (must match VT's xxhash%%1024)", alpha.Partition, traceindex.Partition("trace-alpha"))
	}
}

func TestReadTraceRows_ValidData(t *testing.T) {
	input := []schema.TraceRow{
		{TimestampUnixNano: 100, TraceID: "t1", SpanID: "s1", ServiceName: "svc"},
		{TimestampUnixNano: 200, TraceID: "t2", SpanID: "s2", ServiceName: "svc"},
	}
	data := makeTestTraceParquet(t, input)
	rows, err := readTraceRows(data)
	if err != nil {
		t.Fatalf("readTraceRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].TraceID != "t1" {
		t.Errorf("row[0].TraceID = %q, want t1", rows[0].TraceID)
	}
	if rows[1].TraceID != "t2" {
		t.Errorf("row[1].TraceID = %q, want t2", rows[1].TraceID)
	}
}

func TestReadTraceRows_InvalidData(t *testing.T) {
	// The parquet library panics on invalid data; verify our code propagates the panic.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for invalid parquet data")
		}
	}()
	_, _ = readTraceRows([]byte("not-parquet-data"))
}

func TestReadLogRows_InvalidData(t *testing.T) {
	// The parquet library panics on invalid data; verify our code propagates the panic.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for invalid parquet data")
		}
	}()
	_, _ = readLogRows([]byte("not-parquet-data"))
}

func TestWriteCompactedTraces_RoundTrip(t *testing.T) {
	input := []schema.TraceRow{
		{TimestampUnixNano: 100, TraceID: "t1", SpanID: "s1", SpanName: "op1", ServiceName: "svc-a", DurationNs: 500},
		{TimestampUnixNano: 200, TraceID: "t2", SpanID: "s2", SpanName: "op2", ServiceName: "svc-b", DurationNs: 1000},
	}

	data, err := writeCompactedTraces(input, 100, 3)
	if err != nil {
		t.Fatalf("writeCompactedTraces: %v", err)
	}

	rows, err := readTraceRows(data)
	if err != nil {
		t.Fatalf("readTraceRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].SpanName != "op1" {
		t.Errorf("row[0].SpanName = %q, want op1", rows[0].SpanName)
	}
	if rows[1].DurationNs != 1000 {
		t.Errorf("row[1].DurationNs = %d, want 1000", rows[1].DurationNs)
	}
}

func TestWriteCompactedLogs_RoundTrip(t *testing.T) {
	input := []schema.LogRow{
		{TimestampUnixNano: 100, Body: "hello", ServiceName: "svc"},
		{TimestampUnixNano: 200, Body: "world", ServiceName: "svc"},
	}

	data, err := writeCompactedLogs(input, 100, 3)
	if err != nil {
		t.Fatalf("writeCompactedLogs: %v", err)
	}

	rows, err := readLogRows(data)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Body != "hello" {
		t.Errorf("row[0].Body = %q, want hello", rows[0].Body)
	}
	if rows[1].Body != "world" {
		t.Errorf("row[1].Body = %q, want world", rows[1].Body)
	}
}

func TestWriteCompactedTraces_EmptyRows(t *testing.T) {
	data, err := writeCompactedTraces(nil, 100, 1)
	if err != nil {
		t.Fatalf("writeCompactedTraces with nil rows: %v", err)
	}
	// Empty parquet file should still be a valid file (non-zero bytes with footer).
	if len(data) == 0 {
		t.Fatal("expected non-empty output even for zero rows")
	}
}

func TestWriteCompactedLogs_EmptyRows(t *testing.T) {
	data, err := writeCompactedLogs(nil, 100, 1)
	if err != nil {
		t.Fatalf("writeCompactedLogs with nil rows: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty output even for zero rows")
	}
}

// --- zstdLevel tests ---

func TestZstdLevel_AllLevels(t *testing.T) {
	tests := []struct {
		input int
		want  zstd.Level
	}{
		{0, zstd.SpeedFastest},            // level <= 1
		{1, zstd.SpeedFastest},            // level <= 1
		{2, zstd.SpeedDefault},            // 2 <= level <= 5
		{5, zstd.SpeedDefault},            // 2 <= level <= 5
		{6, zstd.SpeedBetterCompression},  // 6 <= level <= 10
		{10, zstd.SpeedBetterCompression}, // 6 <= level <= 10
		{11, zstd.SpeedBestCompression},   // level > 10
		{100, zstd.SpeedBestCompression},  // large value
	}
	for _, tc := range tests {
		got := zstdLevel(tc.input)
		if got != tc.want {
			t.Errorf("zstdLevel(%d) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// --- error path tests ---

func TestCompactor_EmptyFileList(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")

	compactor := NewCompactor(CompactorConfig{
		Pool:     pool,
		Manifest: m,
		Prefix:   "logs/",
		Mode:     config.ModeLogs,
	})

	_, err := compactor.Compact(context.Background(), "dt=2026-05-04/hour=10", nil, 0)
	if err == nil {
		t.Fatal("expected error for empty file list")
	}
	if err.Error() != "no files to compact" {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestCompactor_DownloadFailure(t *testing.T) {
	pool := newErrorPool()
	pool.downloadErr = fmt.Errorf("simulated download error")
	m := manifest.New("test-bucket", "logs/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "abc"
	fi := manifest.FileInfo{
		Key:               "logs/batch-001.parquet",
		SchemaFingerprint: fp,
	}

	compactor := NewCompactor(CompactorConfig{
		Pool:     pool,
		Manifest: m,
		Prefix:   "logs/",
		Mode:     config.ModeLogs,
	})

	_, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi}, 0)
	if err == nil {
		t.Fatal("expected error for download failure")
	}
}

func TestCompactor_DownloadReturnsNil(t *testing.T) {
	// mockPool returns nil,nil for missing keys — simulates file not found.
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "abc"
	fi := manifest.FileInfo{
		Key:               "logs/missing-file.parquet",
		SchemaFingerprint: fp,
	}

	compactor := NewCompactor(CompactorConfig{
		Pool:     pool,
		Manifest: m,
		Prefix:   "logs/",
		Mode:     config.ModeLogs,
	})

	_, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi}, 0)
	if err == nil {
		t.Fatal("expected error for nil download data")
	}
}

func TestCompactor_UnsupportedMode(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "other/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "abc"
	rows := []schema.LogRow{{TimestampUnixNano: 1, Body: "x", ServiceName: "s"}}
	data := makeTestParquet(t, rows)
	key := "other/batch-001.parquet"
	if err := pool.Upload(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}

	fi := manifest.FileInfo{
		Key:               key,
		Size:              int64(len(data)),
		RowCount:          1,
		SchemaFingerprint: fp,
	}

	compactor := NewCompactor(CompactorConfig{
		Pool:     pool,
		Manifest: m,
		Prefix:   "other/",
		Mode:     config.Mode("unsupported"),
	})

	_, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi}, 0)
	if err == nil {
		t.Fatal("expected error for unsupported mode")
	}
}

func TestCompactor_UploadFailure(t *testing.T) {
	pool := newErrorPool()
	m := manifest.New("test-bucket", "logs/")

	const partition = "dt=2026-05-04/hour=10"
	const fp = "abc"

	rows := []schema.LogRow{{TimestampUnixNano: 1, Body: "x", ServiceName: "s"}}
	data := makeTestParquet(t, rows)
	key := "logs/batch-001.parquet"
	pool.uploaded[key] = data

	fi := manifest.FileInfo{
		Key:               key,
		Size:              int64(len(data)),
		RowCount:          1,
		SchemaFingerprint: fp,
	}
	m.AddFile(partition, fi)

	// Set upload error after the file is already stored (so download works but upload of compacted file fails).
	pool.uploadErr = fmt.Errorf("simulated upload failure")

	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     1000,
		CompressionLevel: 1,
	})

	_, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi}, 0)
	if err == nil {
		t.Fatal("expected error for upload failure")
	}
}

func TestMergeLogFiles_SortByTimestampThenService(t *testing.T) {
	c := NewCompactor(CompactorConfig{Mode: config.ModeLogs})

	file1 := makeTestParquet(t, []schema.LogRow{
		{TimestampUnixNano: 100, ServiceName: "zeta"},
		{TimestampUnixNano: 200, ServiceName: "alpha"},
	})
	file2 := makeTestParquet(t, []schema.LogRow{
		{TimestampUnixNano: 100, ServiceName: "alpha"},
		{TimestampUnixNano: 200, ServiceName: "zeta"},
	})

	merged, err := c.mergeLogFiles([][]byte{file1, file2})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	if len(merged) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(merged))
	}

	// Same timestamp: sorted by service.name ascending
	if merged[0].TimestampUnixNano != 100 || merged[0].ServiceName != "alpha" {
		t.Errorf("row 0: expected ts=100 svc=alpha, got ts=%d svc=%s",
			merged[0].TimestampUnixNano, merged[0].ServiceName)
	}
	if merged[1].TimestampUnixNano != 100 || merged[1].ServiceName != "zeta" {
		t.Errorf("row 1: expected ts=100 svc=zeta, got ts=%d svc=%s",
			merged[1].TimestampUnixNano, merged[1].ServiceName)
	}
	if merged[2].TimestampUnixNano != 200 || merged[2].ServiceName != "alpha" {
		t.Errorf("row 2: expected ts=200 svc=alpha, got ts=%d svc=%s",
			merged[2].TimestampUnixNano, merged[2].ServiceName)
	}
	if merged[3].TimestampUnixNano != 200 || merged[3].ServiceName != "zeta" {
		t.Errorf("row 3: expected ts=200 svc=zeta, got ts=%d svc=%s",
			merged[3].TimestampUnixNano, merged[3].ServiceName)
	}
}

// TestMergeTraceFiles_EmptyInputFileNotFatal pins the compaction-EOF
// resilience fix. A valid 0-row parquet file (a flush can legitimately
// produce one) reads back as total=0, err=io.EOF from parquet-go. The
// pre-fix readTraceRows treated that as fatal, so a SINGLE empty input
// file aborted the whole partition's compaction. Because the scheduler
// re-picks the oldest partition first, one empty file in an old
// partition starved compaction of every newer partition indefinitely —
// the cold-tier L0 backlog (and the recently-flushed trace_id-query
// reachability lag) grew without bound. mergeTraceFiles MUST treat an
// empty input as zero rows and merge the rest.
func TestMergeTraceFiles_EmptyInputFileNotFatal(t *testing.T) {
	c := NewCompactor(CompactorConfig{Mode: config.ModeTraces})

	empty := makeTestTraceParquet(t, nil) // 0-row parquet
	full := makeTestTraceParquet(t, []schema.TraceRow{
		{TimestampUnixNano: 100, ServiceName: "api", TraceID: "aaa"},
		{TimestampUnixNano: 200, ServiceName: "api", TraceID: "bbb"},
	})

	// Empty file alone: must not error, must yield zero rows.
	merged, err := c.mergeTraceFiles([][]byte{empty})
	if err != nil {
		t.Fatalf("merge of empty file errored (compaction would be starved): %v", err)
	}
	if len(merged) != 0 {
		t.Fatalf("expected 0 rows from empty file, got %d", len(merged))
	}

	// Empty file mixed with a real file: must not error, must keep the
	// real file's rows (empty input contributes nothing, blocks nothing).
	merged, err = c.mergeTraceFiles([][]byte{empty, full, empty})
	if err != nil {
		t.Fatalf("merge with empty inputs errored: %v", err)
	}
	if len(merged) != 2 {
		t.Fatalf("expected 2 rows (empty inputs ignored), got %d", len(merged))
	}
}

// TestMergeLogFiles_EmptyInputFileNotFatal — same resilience for logs.
func TestMergeLogFiles_EmptyInputFileNotFatal(t *testing.T) {
	emptyRows, err := readLogRows(makeTestParquet(t, nil))
	if err != nil {
		t.Fatalf("readLogRows on 0-row file must not error: %v", err)
	}
	if len(emptyRows) != 0 {
		t.Fatalf("expected 0 log rows, got %d", len(emptyRows))
	}
}

func TestMergeTraceFiles_SortByTimestampThenServiceThenTraceID(t *testing.T) {
	c := NewCompactor(CompactorConfig{Mode: config.ModeTraces})

	file1 := makeTestTraceParquet(t, []schema.TraceRow{
		{TimestampUnixNano: 100, ServiceName: "api", TraceID: "bbb"},
		{TimestampUnixNano: 100, ServiceName: "api", TraceID: "aaa"},
	})

	merged, err := c.mergeTraceFiles([][]byte{file1})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	if len(merged) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(merged))
	}

	if merged[0].TraceID != "aaa" {
		t.Errorf("row 0: expected trace_id=aaa, got %s", merged[0].TraceID)
	}
	if merged[1].TraceID != "bbb" {
		t.Errorf("row 1: expected trace_id=bbb, got %s", merged[1].TraceID)
	}
}

func TestMergeTraceFiles_CrossFileSortByService(t *testing.T) {
	c := NewCompactor(CompactorConfig{Mode: config.ModeTraces})

	file1 := makeTestTraceParquet(t, []schema.TraceRow{
		{TimestampUnixNano: 100, ServiceName: "zeta", TraceID: "t1"},
		{TimestampUnixNano: 200, ServiceName: "alpha", TraceID: "t2"},
	})
	file2 := makeTestTraceParquet(t, []schema.TraceRow{
		{TimestampUnixNano: 100, ServiceName: "alpha", TraceID: "t3"},
		{TimestampUnixNano: 200, ServiceName: "zeta", TraceID: "t4"},
	})

	merged, err := c.mergeTraceFiles([][]byte{file1, file2})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}

	if len(merged) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(merged))
	}

	// ts=100: alpha before zeta
	if merged[0].TimestampUnixNano != 100 || merged[0].ServiceName != "alpha" {
		t.Errorf("row 0: expected ts=100 svc=alpha, got ts=%d svc=%s",
			merged[0].TimestampUnixNano, merged[0].ServiceName)
	}
	if merged[1].TimestampUnixNano != 100 || merged[1].ServiceName != "zeta" {
		t.Errorf("row 1: expected ts=100 svc=zeta, got ts=%d svc=%s",
			merged[1].TimestampUnixNano, merged[1].ServiceName)
	}
	// ts=200: alpha before zeta
	if merged[2].TimestampUnixNano != 200 || merged[2].ServiceName != "alpha" {
		t.Errorf("row 2: expected ts=200 svc=alpha, got ts=%d svc=%s",
			merged[2].TimestampUnixNano, merged[2].ServiceName)
	}
	if merged[3].TimestampUnixNano != 200 || merged[3].ServiceName != "zeta" {
		t.Errorf("row 3: expected ts=200 svc=zeta, got ts=%d svc=%s",
			merged[3].TimestampUnixNano, merged[3].ServiceName)
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
		CompressionLevel: 7,
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
