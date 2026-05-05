package delete

import (
	"bytes"
	"context"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// mockRewriterPool is an in-memory mock implementing RewriterPool.
type mockRewriterPool struct {
	objects map[string][]byte
}

func newMockRewriterPool() *mockRewriterPool {
	return &mockRewriterPool{objects: make(map[string][]byte)}
}

func (m *mockRewriterPool) Upload(_ context.Context, key string, data []byte) error {
	m.objects[key] = append([]byte(nil), data...)
	return nil
}

func (m *mockRewriterPool) Download(_ context.Context, key string) ([]byte, error) {
	data, ok := m.objects[key]
	if !ok {
		return nil, context.DeadlineExceeded
	}
	return data, nil
}

func (m *mockRewriterPool) Delete(_ context.Context, key string) error {
	delete(m.objects, key)
	return nil
}

// buildTestParquet creates a Parquet file in memory with the given LogRows.
func buildTestParquet(t *testing.T, rows []schema.LogRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.MaxRowsPerRowGroup(100),
	)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("write test parquet: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close test parquet writer: %v", err)
	}
	return buf.Bytes()
}

func TestRewriteFile_MatchingRowsRemoved(t *testing.T) {
	pool := newMockRewriterPool()
	ctx := context.Background()

	// Create test data with 5 rows: 2 errors and 3 info.
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "error happened", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 1500, Body: "all good", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "another error", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 2500, Body: "running fine", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 3000, Body: "ok", SeverityText: "info", ServiceName: "web"},
	}

	key := "logs/dt=2026-01-15/hour=10/00001.parquet"
	pool.objects[key] = buildTestParquet(t, rows)

	rw := NewRewriter(pool, "logs/", 100)

	tombstones := []Tombstone{
		{
			ID:      "t1",
			Query:   `severity_text:="error"`,
			StartNs: 0,
			EndNs:   5000,
		},
	}

	result, err := rw.RewriteFile(ctx, key, tombstones)
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}

	if result.RowsRemoved != 2 {
		t.Fatalf("expected 2 rows removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 3 {
		t.Fatalf("expected 3 rows kept, got %d", result.RowsKept)
	}

	// Old file should be deleted.
	if _, ok := pool.objects[key]; ok {
		t.Fatal("expected old file to be deleted")
	}

	// New file should exist.
	if result.NewKey == "" {
		t.Fatal("expected NewKey to be set")
	}
	if _, ok := pool.objects[result.NewKey]; !ok {
		t.Fatalf("expected new file at %s", result.NewKey)
	}

	// Verify new file has correct rows.
	newData := pool.objects[result.NewKey]
	reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(newData))
	defer reader.Close()

	readRows := make([]schema.LogRow, 10)
	n, _ := reader.Read(readRows)
	if n != 3 {
		t.Fatalf("expected 3 rows in new file, got %d", n)
	}

	// Verify none are errors.
	for i := 0; i < n; i++ {
		if readRows[i].SeverityText == "error" {
			t.Fatalf("row %d should not be error: %+v", i, readRows[i])
		}
	}

	if result.BytesBefore == 0 {
		t.Fatal("expected BytesBefore > 0")
	}
	if result.BytesAfter == 0 {
		t.Fatal("expected BytesAfter > 0")
	}
	if result.Duration == 0 {
		t.Fatal("expected Duration > 0")
	}
}

func TestRewriteFile_NoMatchingRows(t *testing.T) {
	pool := newMockRewriterPool()
	ctx := context.Background()

	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "all good", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "fine", SeverityText: "info", ServiceName: "web"},
	}

	key := "logs/dt=2026-01-15/hour=10/00002.parquet"
	originalData := buildTestParquet(t, rows)
	pool.objects[key] = originalData

	rw := NewRewriter(pool, "logs/", 100)

	tombstones := []Tombstone{
		{
			ID:      "t1",
			Query:   `severity_text:="error"`,
			StartNs: 0,
			EndNs:   5000,
		},
	}

	result, err := rw.RewriteFile(ctx, key, tombstones)
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}

	if result.RowsRemoved != 0 {
		t.Fatalf("expected 0 rows removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 2 {
		t.Fatalf("expected 2 rows kept, got %d", result.RowsKept)
	}

	// Old file should NOT be deleted.
	if _, ok := pool.objects[key]; !ok {
		t.Fatal("expected old file to remain untouched")
	}

	// NewKey should be empty (no new file created).
	if result.NewKey != "" {
		t.Fatalf("expected empty NewKey, got %s", result.NewKey)
	}

	// BytesAfter should be 0 (no new file written).
	if result.BytesAfter != 0 {
		t.Fatalf("expected BytesAfter == 0, got %d", result.BytesAfter)
	}
}

func TestRewriteFile_AllRowsMatching(t *testing.T) {
	pool := newMockRewriterPool()
	ctx := context.Background()

	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "error one", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "error two", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 3000, Body: "error three", SeverityText: "error", ServiceName: "web"},
	}

	key := "logs/dt=2026-02-01/hour=05/00003.parquet"
	pool.objects[key] = buildTestParquet(t, rows)

	rw := NewRewriter(pool, "logs/", 100)

	tombstones := []Tombstone{
		{
			ID:      "t1",
			Query:   `severity_text:="error"`,
			StartNs: 0,
			EndNs:   5000,
		},
	}

	result, err := rw.RewriteFile(ctx, key, tombstones)
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}

	if result.RowsRemoved != 3 {
		t.Fatalf("expected 3 rows removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 0 {
		t.Fatalf("expected 0 rows kept, got %d", result.RowsKept)
	}

	// Old file should be deleted.
	if _, ok := pool.objects[key]; ok {
		t.Fatal("expected old file to be deleted")
	}

	// No new file should be created (all rows removed).
	if result.NewKey != "" {
		t.Fatalf("expected empty NewKey when all rows removed, got %s", result.NewKey)
	}

	// No new files in pool (only the old key was there).
	if len(pool.objects) != 0 {
		t.Fatalf("expected empty pool, got %d objects", len(pool.objects))
	}

	if result.BytesAfter != 0 {
		t.Fatalf("expected BytesAfter == 0, got %d", result.BytesAfter)
	}
}

func TestRewriteFile_MultipleTombstones(t *testing.T) {
	pool := newMockRewriterPool()
	ctx := context.Background()

	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "error in web", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "info in db", SeverityText: "info", ServiceName: "db"},
		{TimestampUnixNano: 3000, Body: "warn in web", SeverityText: "warn", ServiceName: "web"},
		{TimestampUnixNano: 4000, Body: "info in web", SeverityText: "info", ServiceName: "web"},
	}

	key := "logs/dt=2026-03-10/hour=12/00004.parquet"
	pool.objects[key] = buildTestParquet(t, rows)

	rw := NewRewriter(pool, "logs/", 100)

	// Two tombstones: one removes errors, one removes service=db rows.
	tombstones := []Tombstone{
		{
			ID:      "t1",
			Query:   `severity_text:="error"`,
			StartNs: 0,
			EndNs:   5000,
		},
		{
			ID:      "t2",
			Query:   `service.name:="db"`,
			StartNs: 0,
			EndNs:   5000,
		},
	}

	result, err := rw.RewriteFile(ctx, key, tombstones)
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}

	// Row 1 (error, web) matches t1; Row 2 (info, db) matches t2.
	if result.RowsRemoved != 2 {
		t.Fatalf("expected 2 rows removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 2 {
		t.Fatalf("expected 2 rows kept, got %d", result.RowsKept)
	}
}

func TestRewriteFile_TimeRangeFiltering(t *testing.T) {
	pool := newMockRewriterPool()
	ctx := context.Background()

	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "early error", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 5000, Body: "late error", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 3000, Body: "mid info", SeverityText: "info", ServiceName: "web"},
	}

	key := "logs/dt=2026-04-01/hour=08/00005.parquet"
	pool.objects[key] = buildTestParquet(t, rows)

	rw := NewRewriter(pool, "logs/", 100)

	// Tombstone only covers [0, 2000] — should only match first row.
	tombstones := []Tombstone{
		{
			ID:      "t1",
			Query:   `severity_text:="error"`,
			StartNs: 0,
			EndNs:   2000,
		},
	}

	result, err := rw.RewriteFile(ctx, key, tombstones)
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}

	if result.RowsRemoved != 1 {
		t.Fatalf("expected 1 row removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 2 {
		t.Fatalf("expected 2 rows kept, got %d", result.RowsKept)
	}
}

func TestExtractPartition_Standard(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{
			key:  "logs/dt=2026-01-01/hour=10/00001.parquet",
			want: "dt=2026-01-01/hour=10",
		},
		{
			key:  "tenant-a/logs/dt=2026-05-15/hour=23/compacted-L2-abc12345.parquet",
			want: "dt=2026-05-15/hour=23",
		},
		{
			key:  "dt=2026-12-31/hour=00/file.parquet",
			want: "dt=2026-12-31/hour=00",
		},
		{
			key:  "some/path/without/partitions/file.parquet",
			want: "unknown",
		},
		{
			key:  "logs/dt=2026-06-01/hour=15/sub/deep/file.parquet",
			want: "dt=2026-06-01/hour=15",
		},
	}

	for _, tc := range tests {
		got := extractPartition(tc.key)
		if got != tc.want {
			t.Errorf("extractPartition(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

func TestExtractPartition_SinglePartition(t *testing.T) {
	got := extractPartition("data/dt=2026-01-01/file.parquet")
	if got != "dt=2026-01-01" {
		t.Errorf("expected dt=2026-01-01, got %s", got)
	}
}

func TestNewRewriter_DefaultRowGroupSize(t *testing.T) {
	pool := newMockRewriterPool()
	rw := NewRewriter(pool, "prefix/", 0)
	if rw.rowGroupSize != 10000 {
		t.Fatalf("expected default rowGroupSize 10000, got %d", rw.rowGroupSize)
	}
}

func TestNewRewriter_NegativeRowGroupSize(t *testing.T) {
	pool := newMockRewriterPool()
	rw := NewRewriter(pool, "prefix/", -5)
	if rw.rowGroupSize != 10000 {
		t.Fatalf("expected default rowGroupSize 10000, got %d", rw.rowGroupSize)
	}
}

func TestNewRewriter_CustomRowGroupSize(t *testing.T) {
	pool := newMockRewriterPool()
	rw := NewRewriter(pool, "prefix/", 500)
	if rw.rowGroupSize != 500 {
		t.Fatalf("expected rowGroupSize 500, got %d", rw.rowGroupSize)
	}
}

func TestLogRowToMap(t *testing.T) {
	row := &schema.LogRow{
		TimestampUnixNano: 12345,
		Body:              "test body",
		SeverityText:      "warn",
		ServiceName:       "api",
		K8sNamespaceName:  "default",
		TraceID:           "abc123",
		ResourceAttributes: map[string]string{
			"custom.attr": "val1",
		},
		LogAttributes: map[string]string{
			"log.custom": "val2",
		},
	}

	m := logRowToMap(row)

	if m["body"] != "test body" {
		t.Fatalf("expected body=test body, got %s", m["body"])
	}
	if m["severity_text"] != "warn" {
		t.Fatalf("expected severity_text=warn, got %s", m["severity_text"])
	}
	if m["service.name"] != "api" {
		t.Fatalf("expected service.name=api, got %s", m["service.name"])
	}
	if m["k8s.namespace.name"] != "default" {
		t.Fatalf("expected k8s.namespace.name=default, got %s", m["k8s.namespace.name"])
	}
	if m["trace_id"] != "abc123" {
		t.Fatalf("expected trace_id=abc123, got %s", m["trace_id"])
	}
	if m["custom.attr"] != "val1" {
		t.Fatalf("expected custom.attr=val1, got %s", m["custom.attr"])
	}
	if m["log.custom"] != "val2" {
		t.Fatalf("expected log.custom=val2, got %s", m["log.custom"])
	}
}

func TestRewriteFile_DownloadError(t *testing.T) {
	pool := newMockRewriterPool()
	ctx := context.Background()
	// Don't add any file — download will fail.

	rw := NewRewriter(pool, "logs/", 100)
	tombstones := []Tombstone{{ID: "t1", Query: "*", StartNs: 0, EndNs: 5000}}

	_, err := rw.RewriteFile(ctx, "nonexistent/file.parquet", tombstones)
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

func TestRewriteFile_WildcardTombstone(t *testing.T) {
	pool := newMockRewriterPool()
	ctx := context.Background()

	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "one", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "two", SeverityText: "info", ServiceName: "web"},
	}

	key := "logs/dt=2026-01-01/hour=00/00006.parquet"
	pool.objects[key] = buildTestParquet(t, rows)

	rw := NewRewriter(pool, "logs/", 100)

	// Wildcard tombstone removes all rows within time range.
	tombstones := []Tombstone{
		{ID: "t1", Query: "*", StartNs: 0, EndNs: 5000},
	}

	result, err := rw.RewriteFile(ctx, key, tombstones)
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}

	if result.RowsRemoved != 2 {
		t.Fatalf("expected 2 rows removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 0 {
		t.Fatalf("expected 0 rows kept, got %d", result.RowsKept)
	}
	if len(pool.objects) != 0 {
		t.Fatalf("expected pool to be empty, got %d objects", len(pool.objects))
	}
}
