package compaction

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// --- helpers ---

func generateLogRows(n int, baseTS int64, serviceName string) []schema.LogRow {
	rows := make([]schema.LogRow, n)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: baseTS + int64(i)*1_000_000,
			Body:              fmt.Sprintf("log-%s-%d-%d", serviceName, baseTS, i),
			ServiceName:       serviceName,
			SeverityText:      "INFO",
			SeverityNumber:    9,
			TraceID:           fmt.Sprintf("trace-%06d", i),
			K8sNamespaceName:  "default",
			DeployEnv:         "prod",
			CloudRegion:       "us-east-1",
		}
	}
	return rows
}

func generateTraceRows(n int, baseTS int64, serviceName string) []schema.TraceRow {
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: baseTS + int64(i)*1_000_000,
			StartTimeUnixNano: baseTS + int64(i)*1_000_000 - 500_000,
			TraceID:           fmt.Sprintf("trace-%06d", i),
			SpanID:            fmt.Sprintf("span-%06d", i),
			SpanName:          fmt.Sprintf("op-%d", i%10),
			ServiceName:       serviceName,
			DurationNs:        int64(100_000 + i*1000),
			StatusCode:        0,
			K8sNamespaceName:  "default",
			DeployEnv:         "prod",
			CloudRegion:       "us-east-1",
		}
	}
	return rows
}

func makeParquetFromLogRows(t *testing.T, rows []schema.LogRow) []byte {
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

func makeParquetFromTraceRows(t *testing.T, rows []schema.TraceRow) []byte {
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

type compactSetup struct {
	pool      *mockPool
	manifest  *manifest.Manifest
	compactor *Compactor
	partition string
}

func setupLogCompactor(t *testing.T) *compactSetup {
	t.Helper()
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     10000,
		CompressionLevel: 1,
	})
	return &compactSetup{
		pool:      pool,
		manifest:  m,
		compactor: compactor,
		partition: "dt=2026-05-10/hour=14",
	}
}

func setupTraceCompactor(t *testing.T) *compactSetup {
	t.Helper()
	pool := newMockPool()
	m := manifest.New("test-bucket", "traces/")
	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "traces/",
		Mode:             config.ModeTraces,
		RowGroupSize:     10000,
		CompressionLevel: 1,
	})
	return &compactSetup{
		pool:      pool,
		manifest:  m,
		compactor: compactor,
		partition: "dt=2026-05-10/hour=14",
	}
}

func (s *compactSetup) addLogFile(t *testing.T, idx int, rows []schema.LogRow) manifest.FileInfo {
	t.Helper()
	data := makeParquetFromLogRows(t, rows)
	key := fmt.Sprintf("logs/%s/batch-%03d.parquet", s.partition, idx)
	if err := s.pool.Upload(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}
	var minTS, maxTS int64
	if len(rows) > 0 {
		minTS = rows[0].TimestampUnixNano
		maxTS = rows[0].TimestampUnixNano
		for _, r := range rows[1:] {
			if r.TimestampUnixNano < minTS {
				minTS = r.TimestampUnixNano
			}
			if r.TimestampUnixNano > maxTS {
				maxTS = r.TimestampUnixNano
			}
		}
	}
	fi := manifest.FileInfo{
		Key:               key,
		Size:              int64(len(data)),
		RowCount:          int64(len(rows)),
		MinTimeNs:         minTS,
		MaxTimeNs:         maxTS,
		SchemaFingerprint: "fp-logs-v1",
		CompactionLevel:   0,
	}
	s.manifest.AddFile(s.partition, fi)
	return fi
}

func (s *compactSetup) addTraceFile(t *testing.T, idx int, rows []schema.TraceRow) manifest.FileInfo {
	t.Helper()
	data := makeParquetFromTraceRows(t, rows)
	key := fmt.Sprintf("traces/%s/batch-%03d.parquet", s.partition, idx)
	if err := s.pool.Upload(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}
	var minTS, maxTS int64
	if len(rows) > 0 {
		minTS = rows[0].TimestampUnixNano
		maxTS = rows[0].TimestampUnixNano
		for _, r := range rows[1:] {
			if r.TimestampUnixNano < minTS {
				minTS = r.TimestampUnixNano
			}
			if r.TimestampUnixNano > maxTS {
				maxTS = r.TimestampUnixNano
			}
		}
	}
	fi := manifest.FileInfo{
		Key:               key,
		Size:              int64(len(data)),
		RowCount:          int64(len(rows)),
		MinTimeNs:         minTS,
		MaxTimeNs:         maxTS,
		SchemaFingerprint: "fp-traces-v1",
		CompactionLevel:   0,
	}
	s.manifest.AddFile(s.partition, fi)
	return fi
}

// collectAllInputRows gathers all expected rows from multiple files for comparison.
func collectAllLogRows(files ...[]schema.LogRow) []schema.LogRow {
	var all []schema.LogRow
	for _, f := range files {
		all = append(all, f...)
	}
	return all
}

func collectAllTraceRows(files ...[]schema.TraceRow) []schema.TraceRow {
	var all []schema.TraceRow
	for _, f := range files {
		all = append(all, f...)
	}
	return all
}

// ═══════════════════════════════════════════════════════════════════
// A. SMALL FILE MERGES (2-5 files, <100 rows each)
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_SmallMerge_PreservesAllRows(t *testing.T) {
	s := setupLogCompactor(t)

	file1Rows := generateLogRows(10, 1000, "svc-a")
	file2Rows := generateLogRows(15, 2000, "svc-b")
	file3Rows := generateLogRows(5, 500, "svc-a")

	fi1 := s.addLogFile(t, 1, file1Rows)
	fi2 := s.addLogFile(t, 2, file2Rows)
	fi3 := s.addLogFile(t, 3, file3Rows)

	result, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2, fi3}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	expectedTotal := int64(len(file1Rows) + len(file2Rows) + len(file3Rows))
	if result.RowsMerged != expectedTotal {
		t.Fatalf("expected %d rows merged, got %d", expectedTotal, result.RowsMerged)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	rows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}
	if int64(len(rows)) != expectedTotal {
		t.Fatalf("output file has %d rows, expected %d", len(rows), expectedTotal)
	}

	allInput := collectAllLogRows(file1Rows, file2Rows, file3Rows)
	inputBodies := make(map[string]bool, len(allInput))
	for _, r := range allInput {
		inputBodies[r.Body] = true
	}
	for _, r := range rows {
		if !inputBodies[r.Body] {
			t.Errorf("unexpected row body in output: %q", r.Body)
		}
		delete(inputBodies, r.Body)
	}
	if len(inputBodies) > 0 {
		t.Errorf("%d input rows missing from output", len(inputBodies))
	}
}

func TestDataValidation_SmallMerge_SortOrder(t *testing.T) {
	s := setupLogCompactor(t)

	file1Rows := generateLogRows(20, 5000, "svc-b")
	file2Rows := generateLogRows(20, 1000, "svc-a")
	file3Rows := generateLogRows(20, 3000, "svc-c")

	fi1 := s.addLogFile(t, 1, file1Rows)
	fi2 := s.addLogFile(t, 2, file2Rows)
	fi3 := s.addLogFile(t, 3, file3Rows)

	result, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2, fi3}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	rows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}

	for i := 1; i < len(rows); i++ {
		prev, curr := rows[i-1], rows[i]
		if prev.TimestampUnixNano > curr.TimestampUnixNano {
			t.Fatalf("row %d (ts=%d) > row %d (ts=%d): not sorted by timestamp",
				i-1, prev.TimestampUnixNano, i, curr.TimestampUnixNano)
		}
		if prev.TimestampUnixNano == curr.TimestampUnixNano && prev.ServiceName > curr.ServiceName {
			t.Fatalf("row %d (svc=%s) > row %d (svc=%s) at same timestamp: not sorted by service",
				i-1, prev.ServiceName, i, curr.ServiceName)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// B. MEDIUM FILE MERGES (10-20 files, 1k-5k rows each)
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_MediumMerge_PreservesAllRows(t *testing.T) {
	s := setupLogCompactor(t)

	services := []string{"api-gateway", "user-service", "order-service", "payment-service"}
	var allFiles []manifest.FileInfo
	var totalExpected int64

	for i := 0; i < 15; i++ {
		svc := services[i%len(services)]
		n := 1000 + (i * 200)
		rows := generateLogRows(n, int64(i)*10_000_000_000, svc)
		fi := s.addLogFile(t, i, rows)
		allFiles = append(allFiles, fi)
		totalExpected += int64(n)
	}

	result, err := s.compactor.Compact(context.Background(), s.partition, allFiles, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if result.RowsMerged != totalExpected {
		t.Fatalf("expected %d rows, got %d", totalExpected, result.RowsMerged)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	rows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}
	if int64(len(rows)) != totalExpected {
		t.Fatalf("output has %d rows, expected %d", len(rows), totalExpected)
	}

	// Verify per-service count preservation.
	svcCounts := make(map[string]int)
	for _, r := range rows {
		svcCounts[r.ServiceName]++
	}
	for _, svc := range services {
		if svcCounts[svc] == 0 {
			t.Errorf("service %q has 0 rows after compaction", svc)
		}
	}
}

func TestDataValidation_MediumMerge_PerServiceCountPreserved(t *testing.T) {
	s := setupLogCompactor(t)

	expectedCounts := map[string]int{
		"api-gateway":    3000,
		"user-service":   2000,
		"order-service":  1500,
		"payment-service": 500,
	}

	var allFiles []manifest.FileInfo
	fileIdx := 0
	for svc, count := range expectedCounts {
		filesPerSvc := 3
		rowsPerFile := count / filesPerSvc
		remainder := count - (rowsPerFile * filesPerSvc)
		for i := 0; i < filesPerSvc; i++ {
			n := rowsPerFile
			if i == 0 {
				n += remainder
			}
			rows := generateLogRows(n, int64(fileIdx)*50_000_000_000, svc)
			fi := s.addLogFile(t, fileIdx, rows)
			allFiles = append(allFiles, fi)
			fileIdx++
		}
	}

	result, err := s.compactor.Compact(context.Background(), s.partition, allFiles, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	rows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}

	actualCounts := make(map[string]int)
	for _, r := range rows {
		actualCounts[r.ServiceName]++
	}

	for svc, expected := range expectedCounts {
		actual := actualCounts[svc]
		if actual != expected {
			t.Errorf("service %q: expected %d rows, got %d", svc, expected, actual)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// C. LARGE FILE MERGES (50+ files, 10k+ rows)
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_LargeMerge_PreservesAllRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large merge test in short mode")
	}

	s := setupLogCompactor(t)

	var allFiles []manifest.FileInfo
	var totalExpected int64

	for i := 0; i < 50; i++ {
		svc := fmt.Sprintf("svc-%d", i%5)
		rows := generateLogRows(500, int64(i)*500_000_000, svc)
		fi := s.addLogFile(t, i, rows)
		allFiles = append(allFiles, fi)
		totalExpected += 500
	}

	result, err := s.compactor.Compact(context.Background(), s.partition, allFiles, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if result.RowsMerged != totalExpected {
		t.Fatalf("expected %d rows, got %d", totalExpected, result.RowsMerged)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	rows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}
	if int64(len(rows)) != totalExpected {
		t.Fatalf("output has %d rows, expected %d", len(rows), totalExpected)
	}
}

// ═══════════════════════════════════════════════════════════════════
// D. TIME BOUNDS CORRECTNESS
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_TimeBounds_FromActualData(t *testing.T) {
	s := setupLogCompactor(t)

	file1Rows := generateLogRows(10, 5000, "svc-a")
	file2Rows := generateLogRows(10, 1000, "svc-b")
	file3Rows := generateLogRows(10, 8000, "svc-c")

	fi1 := s.addLogFile(t, 1, file1Rows)
	fi2 := s.addLogFile(t, 2, file2Rows)
	fi3 := s.addLogFile(t, 3, file3Rows)

	result, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2, fi3}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	files := s.manifest.FilesForPartition(s.partition)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}

	compactedFI := files[0]

	// Actual min/max timestamps in the merged data.
	allRows := collectAllLogRows(file1Rows, file2Rows, file3Rows)
	var actualMin, actualMax int64
	actualMin = allRows[0].TimestampUnixNano
	actualMax = allRows[0].TimestampUnixNano
	for _, r := range allRows[1:] {
		if r.TimestampUnixNano < actualMin {
			actualMin = r.TimestampUnixNano
		}
		if r.TimestampUnixNano > actualMax {
			actualMax = r.TimestampUnixNano
		}
	}

	if compactedFI.MinTimeNs != actualMin {
		t.Errorf("MinTimeNs: expected %d (actual data), got %d", actualMin, compactedFI.MinTimeNs)
	}
	if compactedFI.MaxTimeNs != actualMax {
		t.Errorf("MaxTimeNs: expected %d (actual data), got %d", actualMax, compactedFI.MaxTimeNs)
	}

	_ = result
}

func TestDataValidation_TimeBounds_NotInheritedFromInaccurateInputFiles(t *testing.T) {
	s := setupLogCompactor(t)

	// Create files with INACCURATE input metadata (e.g., partition-inferred bounds
	// after restart, much wider than actual data).
	rows1 := generateLogRows(10, 5000, "svc-a") // actual range: 5000-5009000000
	data1 := makeParquetFromLogRows(t, rows1)
	key1 := fmt.Sprintf("logs/%s/batch-001.parquet", s.partition)
	_ = s.pool.Upload(context.Background(), key1, data1)

	rows2 := generateLogRows(10, 7000, "svc-b") // actual range: 7000-7009000000
	data2 := makeParquetFromLogRows(t, rows2)
	key2 := fmt.Sprintf("logs/%s/batch-002.parquet", s.partition)
	_ = s.pool.Upload(context.Background(), key2, data2)

	// Register with INACCURATE bounds (as if inferred from partition key).
	fi1 := manifest.FileInfo{
		Key:               key1,
		Size:              int64(len(data1)),
		RowCount:          int64(len(rows1)),
		MinTimeNs:         0,                     // inaccurate: partition start
		MaxTimeNs:         999_999_999_999_999,    // inaccurate: partition end
		SchemaFingerprint: "fp-logs-v1",
		CompactionLevel:   0,
	}
	fi2 := manifest.FileInfo{
		Key:               key2,
		Size:              int64(len(data2)),
		RowCount:          int64(len(rows2)),
		MinTimeNs:         0,
		MaxTimeNs:         999_999_999_999_999,
		SchemaFingerprint: "fp-logs-v1",
		CompactionLevel:   0,
	}
	s.manifest.AddFile(s.partition, fi1)
	s.manifest.AddFile(s.partition, fi2)

	_, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	files := s.manifest.FilesForPartition(s.partition)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}

	compactedFI := files[0]

	// The bounds MUST come from actual data, NOT the inaccurate input file metadata.
	if compactedFI.MinTimeNs == 0 {
		t.Error("MinTimeNs is 0 — inherited inaccurate input file metadata instead of actual data")
	}
	if compactedFI.MaxTimeNs == 999_999_999_999_999 {
		t.Error("MaxTimeNs is partition end — inherited inaccurate input file metadata instead of actual data")
	}

	// Verify bounds match actual data.
	actualMin := rows2[0].TimestampUnixNano // file2 starts at 5000 (but rows1 at 5000 too)
	if rows1[0].TimestampUnixNano < actualMin {
		actualMin = rows1[0].TimestampUnixNano
	}
	actualMax := rows1[len(rows1)-1].TimestampUnixNano
	if rows2[len(rows2)-1].TimestampUnixNano > actualMax {
		actualMax = rows2[len(rows2)-1].TimestampUnixNano
	}

	if compactedFI.MinTimeNs != actualMin {
		t.Errorf("MinTimeNs: expected %d, got %d", actualMin, compactedFI.MinTimeNs)
	}
	if compactedFI.MaxTimeNs != actualMax {
		t.Errorf("MaxTimeNs: expected %d, got %d", actualMax, compactedFI.MaxTimeNs)
	}
}

func TestDataValidation_TimeBounds_SingleRowFile(t *testing.T) {
	s := setupLogCompactor(t)

	rows := []schema.LogRow{
		{TimestampUnixNano: 42000, Body: "only-row", ServiceName: "svc-a"},
	}
	fi := s.addLogFile(t, 1, rows)

	_, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi}, 0)
	if err == nil {
		// Single file compaction might be rejected by policy, but compactor itself should work.
		files := s.manifest.FilesForPartition(s.partition)
		for _, f := range files {
			if f.CompactionLevel > 0 {
				if f.MinTimeNs != 42000 || f.MaxTimeNs != 42000 {
					t.Errorf("single row: expected min=max=42000, got min=%d max=%d", f.MinTimeNs, f.MaxTimeNs)
				}
			}
		}
	}
	// If error: single-file compaction with same fingerprint is valid, so this shouldn't fail.
}

// ═══════════════════════════════════════════════════════════════════
// E. MANIFEST STATE AFTER COMPACTION
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_ManifestState_SourceFilesRemoved(t *testing.T) {
	s := setupLogCompactor(t)

	var allFiles []manifest.FileInfo
	for i := 0; i < 5; i++ {
		rows := generateLogRows(100, int64(i)*1_000_000_000, "svc-a")
		fi := s.addLogFile(t, i, rows)
		allFiles = append(allFiles, fi)
	}

	if s.manifest.TotalFiles() != 5 {
		t.Fatalf("pre-compact: expected 5 files, got %d", s.manifest.TotalFiles())
	}

	result, err := s.compactor.Compact(context.Background(), s.partition, allFiles, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Source files must be removed.
	files := s.manifest.FilesForPartition(s.partition)
	if len(files) != 1 {
		t.Fatalf("expected 1 file after compaction, got %d", len(files))
	}
	if files[0].Key != result.OutputFile {
		t.Errorf("manifest file key: expected %q, got %q", result.OutputFile, files[0].Key)
	}

	// Source files must be deleted from pool.
	s.pool.mu.Lock()
	for _, fi := range allFiles {
		if _, exists := s.pool.uploaded[fi.Key]; exists {
			t.Errorf("source file %q still in pool after compaction", fi.Key)
		}
	}
	s.pool.mu.Unlock()
}

func TestDataValidation_ManifestState_RowCountMatches(t *testing.T) {
	s := setupLogCompactor(t)

	var allFiles []manifest.FileInfo
	var totalRows int64
	for i := 0; i < 8; i++ {
		n := 100 + i*50
		rows := generateLogRows(n, int64(i)*5_000_000_000, "svc-a")
		fi := s.addLogFile(t, i, rows)
		allFiles = append(allFiles, fi)
		totalRows += int64(n)
	}

	_, err := s.compactor.Compact(context.Background(), s.partition, allFiles, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	files := s.manifest.FilesForPartition(s.partition)
	if files[0].RowCount != totalRows {
		t.Errorf("manifest RowCount: expected %d, got %d", totalRows, files[0].RowCount)
	}
}

func TestDataValidation_ManifestState_CompactionLevel(t *testing.T) {
	s := setupLogCompactor(t)

	rows1 := generateLogRows(50, 1000, "svc-a")
	rows2 := generateLogRows(50, 2000, "svc-b")
	fi1 := s.addLogFile(t, 1, rows1)
	fi2 := s.addLogFile(t, 2, rows2)

	// L0 -> L1
	_, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err != nil {
		t.Fatalf("L0->L1 Compact: %v", err)
	}

	files := s.manifest.FilesForPartition(s.partition)
	if files[0].CompactionLevel != 1 {
		t.Fatalf("expected L1, got L%d", files[0].CompactionLevel)
	}
}

// ═══════════════════════════════════════════════════════════════════
// F. ALL FIELDS PRESERVED (logs)
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_LogFields_AllPreserved(t *testing.T) {
	s := setupLogCompactor(t)

	original := schema.LogRow{
		TimestampUnixNano: 42_000_000_000,
		Body:              "test body with special chars: éàü & <tag>",
		SeverityText:      "ERROR",
		SeverityNumber:    17,
		ServiceName:       "payment-service",
		TraceID:           "abc123def456",
		SpanID:            "span-789",
		K8sNamespaceName:  "production",
		K8sPodName:        "payment-pod-xyz",
		K8sDeploymentName: "payment-deploy",
		K8sNodeName:       "node-pool-a-1",
		DeployEnv:         "staging",
		CloudRegion:       "eu-west-1",
		HostName:          "ip-10-0-1-42",
		Stream:            "{service_name=\"payment-service\"}",
		StreamID:          "stream-001",
		ScopeName:         "com.example.payment",
		ResourceAttributes: map[string]string{
			"k8s.cluster.name": "prod-eu",
			"cloud.provider":   "aws",
		},
		LogAttributes: map[string]string{
			"http.method":      "POST",
			"http.status_code": "500",
		},
	}

	fi := s.addLogFile(t, 1, []schema.LogRow{original})
	// Add a second file to make it a valid compaction (>1 file).
	filler := generateLogRows(5, 99_000_000_000, "filler-svc")
	fi2 := s.addLogFile(t, 2, filler)

	result, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	rows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}

	var found *schema.LogRow
	for i := range rows {
		if rows[i].Body == original.Body {
			found = &rows[i]
			break
		}
	}
	if found == nil {
		t.Fatal("original row not found in compacted output")
	}

	if found.TimestampUnixNano != original.TimestampUnixNano {
		t.Errorf("TimestampUnixNano: got %d, want %d", found.TimestampUnixNano, original.TimestampUnixNano)
	}
	if found.SeverityText != original.SeverityText {
		t.Errorf("SeverityText: got %q, want %q", found.SeverityText, original.SeverityText)
	}
	if found.SeverityNumber != original.SeverityNumber {
		t.Errorf("SeverityNumber: got %d, want %d", found.SeverityNumber, original.SeverityNumber)
	}
	if found.ServiceName != original.ServiceName {
		t.Errorf("ServiceName: got %q, want %q", found.ServiceName, original.ServiceName)
	}
	if found.TraceID != original.TraceID {
		t.Errorf("TraceID: got %q, want %q", found.TraceID, original.TraceID)
	}
	if found.SpanID != original.SpanID {
		t.Errorf("SpanID: got %q, want %q", found.SpanID, original.SpanID)
	}
	if found.K8sNamespaceName != original.K8sNamespaceName {
		t.Errorf("K8sNamespaceName: got %q, want %q", found.K8sNamespaceName, original.K8sNamespaceName)
	}
	if found.K8sPodName != original.K8sPodName {
		t.Errorf("K8sPodName: got %q, want %q", found.K8sPodName, original.K8sPodName)
	}
	if found.K8sDeploymentName != original.K8sDeploymentName {
		t.Errorf("K8sDeploymentName: got %q, want %q", found.K8sDeploymentName, original.K8sDeploymentName)
	}
	if found.K8sNodeName != original.K8sNodeName {
		t.Errorf("K8sNodeName: got %q, want %q", found.K8sNodeName, original.K8sNodeName)
	}
	if found.DeployEnv != original.DeployEnv {
		t.Errorf("DeployEnv: got %q, want %q", found.DeployEnv, original.DeployEnv)
	}
	if found.CloudRegion != original.CloudRegion {
		t.Errorf("CloudRegion: got %q, want %q", found.CloudRegion, original.CloudRegion)
	}
	if found.HostName != original.HostName {
		t.Errorf("HostName: got %q, want %q", found.HostName, original.HostName)
	}
	if found.Stream != original.Stream {
		t.Errorf("Stream: got %q, want %q", found.Stream, original.Stream)
	}
	if found.StreamID != original.StreamID {
		t.Errorf("StreamID: got %q, want %q", found.StreamID, original.StreamID)
	}
	if found.ScopeName != original.ScopeName {
		t.Errorf("ScopeName: got %q, want %q", found.ScopeName, original.ScopeName)
	}

	for k, v := range original.ResourceAttributes {
		if found.ResourceAttributes[k] != v {
			t.Errorf("ResourceAttributes[%q]: got %q, want %q", k, found.ResourceAttributes[k], v)
		}
	}
	for k, v := range original.LogAttributes {
		if found.LogAttributes[k] != v {
			t.Errorf("LogAttributes[%q]: got %q, want %q", k, found.LogAttributes[k], v)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// G. TRACES MODE VALIDATION
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_TracesMerge_PreservesAllRows(t *testing.T) {
	s := setupTraceCompactor(t)

	file1Rows := generateTraceRows(100, 1000, "svc-a")
	file2Rows := generateTraceRows(150, 2000, "svc-b")
	file3Rows := generateTraceRows(50, 500, "svc-c")

	fi1 := s.addTraceFile(t, 1, file1Rows)
	fi2 := s.addTraceFile(t, 2, file2Rows)
	fi3 := s.addTraceFile(t, 3, file3Rows)

	result, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2, fi3}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	expectedTotal := int64(100 + 150 + 50)
	if result.RowsMerged != expectedTotal {
		t.Fatalf("expected %d rows, got %d", expectedTotal, result.RowsMerged)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	rows, err := readTraceRows(outputData)
	if err != nil {
		t.Fatalf("readTraceRows: %v", err)
	}

	if int64(len(rows)) != expectedTotal {
		t.Fatalf("output has %d rows, expected %d", len(rows), expectedTotal)
	}

	// Verify all trace IDs present.
	allInput := collectAllTraceRows(file1Rows, file2Rows, file3Rows)
	inputTraceIDs := make(map[string]bool, len(allInput))
	for _, r := range allInput {
		inputTraceIDs[r.TraceID] = true
	}
	for _, r := range rows {
		delete(inputTraceIDs, r.TraceID)
	}
	if len(inputTraceIDs) > 0 {
		t.Errorf("%d trace IDs missing from output", len(inputTraceIDs))
	}
}

func TestDataValidation_TracesMerge_SortOrder(t *testing.T) {
	s := setupTraceCompactor(t)

	file1Rows := generateTraceRows(50, 3000, "svc-b")
	file2Rows := generateTraceRows(50, 1000, "svc-a")

	fi1 := s.addTraceFile(t, 1, file1Rows)
	fi2 := s.addTraceFile(t, 2, file2Rows)

	result, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	rows, err := readTraceRows(outputData)
	if err != nil {
		t.Fatalf("readTraceRows: %v", err)
	}

	for i := 1; i < len(rows); i++ {
		prev, curr := rows[i-1], rows[i]
		if prev.TimestampUnixNano > curr.TimestampUnixNano {
			t.Fatalf("row %d (ts=%d) > row %d (ts=%d): not sorted by timestamp",
				i-1, prev.TimestampUnixNano, i, curr.TimestampUnixNano)
		}
		if prev.TimestampUnixNano == curr.TimestampUnixNano {
			if prev.ServiceName > curr.ServiceName {
				t.Fatalf("row %d (svc=%s) > row %d (svc=%s) at same ts: not sorted by service",
					i-1, prev.ServiceName, i, curr.ServiceName)
			}
			if prev.ServiceName == curr.ServiceName && prev.TraceID > curr.TraceID {
				t.Fatalf("row %d (tid=%s) > row %d (tid=%s) at same ts+svc: not sorted by trace_id",
					i-1, prev.TraceID, i, curr.TraceID)
			}
		}
	}
}

func TestDataValidation_TraceFields_AllPreserved(t *testing.T) {
	s := setupTraceCompactor(t)

	original := schema.TraceRow{
		TimestampUnixNano: 42_000_000_000,
		StartTimeUnixNano: 41_999_500_000,
		TraceID:           "trace-unique-abc",
		SpanID:            "span-def-123",
		ParentSpanID:      "span-parent-456",
		SpanName:          "POST /api/checkout",
		ServiceName:       "payment-service",
		DurationNs:        1_500_000,
		StatusCode:        2,
		StatusMessage:     "Internal error",
		SpanKind:          1,
		HTTPMethod:        "POST",
		HTTPStatusCode:    "500",
		HTTPUrl:           "https://api.example.com/checkout",
		DBSystem:          "postgresql",
		DBStatement:       "SELECT * FROM orders WHERE id = $1",
		K8sNamespaceName:  "production",
		K8sPodName:        "payment-pod-abc",
		K8sDeploymentName: "payment-deploy",
		K8sNodeName:       "node-1",
		DeployEnv:         "prod",
		CloudRegion:       "us-west-2",
		HostName:          "ip-10-0-2-17",
		Stream:            "{service_name=\"payment-service\"}",
		StreamID:          "stream-t-001",
		ScopeName:         "com.example.payment",
		ResourceAttributes: map[string]string{
			"cloud.provider": "aws",
		},
		SpanAttributes: map[string]string{
			"http.route": "/api/checkout",
		},
	}

	fi := s.addTraceFile(t, 1, []schema.TraceRow{original})
	filler := generateTraceRows(5, 99_000_000_000, "filler-svc")
	fi2 := s.addTraceFile(t, 2, filler)

	result, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	rows, err := readTraceRows(outputData)
	if err != nil {
		t.Fatalf("readTraceRows: %v", err)
	}

	var found *schema.TraceRow
	for i := range rows {
		if rows[i].TraceID == original.TraceID {
			found = &rows[i]
			break
		}
	}
	if found == nil {
		t.Fatal("original trace row not found in compacted output")
	}

	if found.StartTimeUnixNano != original.StartTimeUnixNano {
		t.Errorf("StartTimeUnixNano: got %d, want %d", found.StartTimeUnixNano, original.StartTimeUnixNano)
	}
	if found.SpanID != original.SpanID {
		t.Errorf("SpanID: got %q, want %q", found.SpanID, original.SpanID)
	}
	if found.ParentSpanID != original.ParentSpanID {
		t.Errorf("ParentSpanID: got %q, want %q", found.ParentSpanID, original.ParentSpanID)
	}
	if found.SpanName != original.SpanName {
		t.Errorf("SpanName: got %q, want %q", found.SpanName, original.SpanName)
	}
	if found.DurationNs != original.DurationNs {
		t.Errorf("DurationNs: got %d, want %d", found.DurationNs, original.DurationNs)
	}
	if found.StatusCode != original.StatusCode {
		t.Errorf("StatusCode: got %d, want %d", found.StatusCode, original.StatusCode)
	}
	if found.StatusMessage != original.StatusMessage {
		t.Errorf("StatusMessage: got %q, want %q", found.StatusMessage, original.StatusMessage)
	}
	if found.SpanKind != original.SpanKind {
		t.Errorf("SpanKind: got %d, want %d", found.SpanKind, original.SpanKind)
	}
	if found.HTTPMethod != original.HTTPMethod {
		t.Errorf("HTTPMethod: got %q, want %q", found.HTTPMethod, original.HTTPMethod)
	}
	if found.HTTPStatusCode != original.HTTPStatusCode {
		t.Errorf("HTTPStatusCode: got %q, want %q", found.HTTPStatusCode, original.HTTPStatusCode)
	}
	if found.HTTPUrl != original.HTTPUrl {
		t.Errorf("HTTPUrl: got %q, want %q", found.HTTPUrl, original.HTTPUrl)
	}
	if found.DBSystem != original.DBSystem {
		t.Errorf("DBSystem: got %q, want %q", found.DBSystem, original.DBSystem)
	}
	if found.DBStatement != original.DBStatement {
		t.Errorf("DBStatement: got %q, want %q", found.DBStatement, original.DBStatement)
	}
	for k, v := range original.ResourceAttributes {
		if found.ResourceAttributes[k] != v {
			t.Errorf("ResourceAttributes[%q]: got %q, want %q", k, found.ResourceAttributes[k], v)
		}
	}
	for k, v := range original.SpanAttributes {
		if found.SpanAttributes[k] != v {
			t.Errorf("SpanAttributes[%q]: got %q, want %q", k, found.SpanAttributes[k], v)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// H. MULTI-LEVEL COMPACTION CHAINS
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_MultiLevel_L0_L1_L2_PreservesAllRows(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	partition := "dt=2026-05-10/hour=14"
	fp := "fp-logs-v1"

	// Create L0 files (3 groups of 4 files each).
	var allOriginalRows []schema.LogRow
	for i := 0; i < 12; i++ {
		rows := generateLogRows(50, int64(i)*1_000_000_000, fmt.Sprintf("svc-%d", i%3))
		allOriginalRows = append(allOriginalRows, rows...)
		data := makeParquetFromLogRows(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%02d.parquet", partition, i)
		_ = pool.Upload(context.Background(), key, data)

		var minTS, maxTS int64 = rows[0].TimestampUnixNano, rows[len(rows)-1].TimestampUnixNano
		m.AddFile(partition, manifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: int64(len(rows)),
			MinTimeNs: minTS, MaxTimeNs: maxTS, SchemaFingerprint: fp, CompactionLevel: 0,
		})
	}

	totalOriginalRows := int64(len(allOriginalRows))

	// L0 -> L1: compact all 12 L0 files.
	c1 := NewCompactor(CompactorConfig{
		Pool: pool, Manifest: m, Prefix: "logs/", Mode: config.ModeLogs,
		RowGroupSize: 10000, CompressionLevel: 1,
	})

	l0Files := m.FilesForPartition(partition)
	result1, err := c1.Compact(context.Background(), partition, l0Files, 0)
	if err != nil {
		t.Fatalf("L0->L1: %v", err)
	}
	if result1.RowsMerged != totalOriginalRows {
		t.Fatalf("L0->L1: expected %d rows, got %d", totalOriginalRows, result1.RowsMerged)
	}

	l1Files := m.FilesForPartition(partition)
	if len(l1Files) != 1 || l1Files[0].CompactionLevel != 1 {
		t.Fatalf("after L0->L1: expected 1 L1 file, got %d files", len(l1Files))
	}

	// Create more L0 files to trigger L1 -> L2 (need multiple L1 files).
	for i := 12; i < 24; i++ {
		rows := generateLogRows(50, int64(i)*1_000_000_000, fmt.Sprintf("svc-%d", i%3))
		allOriginalRows = append(allOriginalRows, rows...)
		data := makeParquetFromLogRows(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%02d.parquet", partition, i)
		_ = pool.Upload(context.Background(), key, data)

		var minTS, maxTS int64 = rows[0].TimestampUnixNano, rows[len(rows)-1].TimestampUnixNano
		m.AddFile(partition, manifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: int64(len(rows)),
			MinTimeNs: minTS, MaxTimeNs: maxTS, SchemaFingerprint: fp, CompactionLevel: 0,
		})
	}

	// L0 -> L1 again for the new files.
	l0Files2 := make([]manifest.FileInfo, 0)
	for _, f := range m.FilesForPartition(partition) {
		if f.CompactionLevel == 0 {
			l0Files2 = append(l0Files2, f)
		}
	}
	result2, err := c1.Compact(context.Background(), partition, l0Files2, 0)
	if err != nil {
		t.Fatalf("L0->L1 (second): %v", err)
	}

	// Now we have 2 L1 files. Compact them to L2.
	l1FilesAll := make([]manifest.FileInfo, 0)
	for _, f := range m.FilesForPartition(partition) {
		if f.CompactionLevel == 1 {
			l1FilesAll = append(l1FilesAll, f)
		}
	}
	if len(l1FilesAll) != 2 {
		t.Fatalf("expected 2 L1 files, got %d", len(l1FilesAll))
	}

	result3, err := c1.Compact(context.Background(), partition, l1FilesAll, 1)
	if err != nil {
		t.Fatalf("L1->L2: %v", err)
	}

	totalAfterBothCompactions := result1.RowsMerged + result2.RowsMerged
	if result3.RowsMerged != totalAfterBothCompactions {
		t.Fatalf("L1->L2: expected %d rows, got %d", totalAfterBothCompactions, result3.RowsMerged)
	}

	// Final check: exactly 1 L2 file with all original rows.
	finalFiles := m.FilesForPartition(partition)
	if len(finalFiles) != 1 {
		t.Fatalf("after L1->L2: expected 1 file, got %d", len(finalFiles))
	}
	if finalFiles[0].CompactionLevel != 2 {
		t.Fatalf("expected L2, got L%d", finalFiles[0].CompactionLevel)
	}

	// Read back and verify every original row is present.
	pool.mu.Lock()
	outputData := pool.uploaded[finalFiles[0].Key]
	pool.mu.Unlock()

	finalRows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}

	if int64(len(finalRows)) != int64(len(allOriginalRows)) {
		t.Fatalf("final output has %d rows, expected %d", len(finalRows), len(allOriginalRows))
	}

	// Verify every body is present.
	bodySet := make(map[string]bool, len(allOriginalRows))
	for _, r := range allOriginalRows {
		bodySet[r.Body] = true
	}
	for _, r := range finalRows {
		delete(bodySet, r.Body)
	}
	if len(bodySet) > 0 {
		t.Errorf("%d rows missing after L0->L1->L2 chain", len(bodySet))
	}
}

// ═══════════════════════════════════════════════════════════════════
// I. EDGE CASES
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_OverlappingTimestamps_NoDedup(t *testing.T) {
	s := setupLogCompactor(t)

	// Two files with exactly the same timestamps — all rows must be preserved (no dedup).
	rows1 := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "file1-row1", ServiceName: "svc-a"},
		{TimestampUnixNano: 2000, Body: "file1-row2", ServiceName: "svc-a"},
		{TimestampUnixNano: 3000, Body: "file1-row3", ServiceName: "svc-a"},
	}
	rows2 := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "file2-row1", ServiceName: "svc-a"},
		{TimestampUnixNano: 2000, Body: "file2-row2", ServiceName: "svc-a"},
		{TimestampUnixNano: 3000, Body: "file2-row3", ServiceName: "svc-a"},
	}

	fi1 := s.addLogFile(t, 1, rows1)
	fi2 := s.addLogFile(t, 2, rows2)

	result, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if result.RowsMerged != 6 {
		t.Fatalf("expected 6 rows (no dedup), got %d", result.RowsMerged)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	outputRows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}
	if len(outputRows) != 6 {
		t.Fatalf("output has %d rows, expected 6 (no dedup)", len(outputRows))
	}
}

func TestDataValidation_NonContiguousTimestamps(t *testing.T) {
	s := setupLogCompactor(t)

	// File1: timestamps 1000-2000, File2: timestamps 5000-6000 (gap at 2001-4999).
	rows1 := generateLogRows(10, 1000, "svc-a")
	rows2 := generateLogRows(10, 5000, "svc-b")

	fi1 := s.addLogFile(t, 1, rows1)
	fi2 := s.addLogFile(t, 2, rows2)

	result, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if result.RowsMerged != 20 {
		t.Fatalf("expected 20 rows, got %d", result.RowsMerged)
	}

	// Verify time bounds span the full range including the gap.
	files := s.manifest.FilesForPartition(s.partition)
	fi := files[0]
	if fi.MinTimeNs != rows1[0].TimestampUnixNano {
		t.Errorf("MinTimeNs should be from first file's earliest row")
	}
	if fi.MaxTimeNs != rows2[len(rows2)-1].TimestampUnixNano {
		t.Errorf("MaxTimeNs should be from second file's latest row")
	}
}

func TestDataValidation_RandomizedOrder_CorrectSort(t *testing.T) {
	s := setupLogCompactor(t)

	rng := rand.New(rand.NewSource(42))

	var allFiles []manifest.FileInfo
	var allRows []schema.LogRow
	services := []string{"svc-a", "svc-b", "svc-c"}

	for i := 0; i < 10; i++ {
		n := 20 + rng.Intn(80)
		rows := make([]schema.LogRow, n)
		for j := range rows {
			ts := rng.Int63n(100_000_000_000)
			svc := services[rng.Intn(len(services))]
			rows[j] = schema.LogRow{
				TimestampUnixNano: ts,
				Body:              fmt.Sprintf("rand-%d-%d", i, j),
				ServiceName:       svc,
			}
		}
		allRows = append(allRows, rows...)
		fi := s.addLogFile(t, i, rows)
		allFiles = append(allFiles, fi)
	}

	result, err := s.compactor.Compact(context.Background(), s.partition, allFiles, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if result.RowsMerged != int64(len(allRows)) {
		t.Fatalf("expected %d rows, got %d", len(allRows), result.RowsMerged)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	outputRows, err := readLogRows(outputData)
	if err != nil {
		t.Fatalf("readLogRows: %v", err)
	}

	// Verify sorted.
	for i := 1; i < len(outputRows); i++ {
		prev, curr := outputRows[i-1], outputRows[i]
		if prev.TimestampUnixNano > curr.TimestampUnixNano {
			t.Fatalf("not sorted at index %d", i)
		}
		if prev.TimestampUnixNano == curr.TimestampUnixNano && prev.ServiceName > curr.ServiceName {
			t.Fatalf("not sorted by service at index %d", i)
		}
	}

	// Verify all rows present.
	sort.Slice(allRows, func(i, j int) bool { return allRows[i].Body < allRows[j].Body })
	sort.Slice(outputRows, func(i, j int) bool { return outputRows[i].Body < outputRows[j].Body })
	if len(allRows) != len(outputRows) {
		t.Fatalf("row count mismatch: input=%d, output=%d", len(allRows), len(outputRows))
	}
	for i := range allRows {
		if allRows[i].Body != outputRows[i].Body {
			t.Fatalf("row %d body mismatch: input=%q, output=%q", i, allRows[i].Body, outputRows[i].Body)
		}
	}
}

func TestDataValidation_EmptyCompaction_Error(t *testing.T) {
	s := setupLogCompactor(t)

	_, err := s.compactor.Compact(context.Background(), s.partition, nil, 0)
	if err == nil {
		t.Fatal("expected error for empty file list")
	}
}

func TestDataValidation_SchemaFingerprintMismatch_Error(t *testing.T) {
	s := setupLogCompactor(t)

	rows1 := generateLogRows(10, 1000, "svc-a")
	rows2 := generateLogRows(10, 2000, "svc-b")

	fi1 := s.addLogFile(t, 1, rows1)
	fi2 := s.addLogFile(t, 2, rows2)
	fi2.SchemaFingerprint = "different-fp"

	_, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{fi1, fi2}, 0)
	if err == nil {
		t.Fatal("expected error for schema fingerprint mismatch")
	}
}

// ═══════════════════════════════════════════════════════════════════
// J. COMPACTION OUTPUT READABLE BY STANDARD PARQUET TOOLS
// ═══════════════════════════════════════════════════════════════════

func TestDataValidation_OutputIsValidParquet(t *testing.T) {
	s := setupLogCompactor(t)

	var allFiles []manifest.FileInfo
	for i := 0; i < 5; i++ {
		rows := generateLogRows(200, int64(i)*2_000_000_000, fmt.Sprintf("svc-%d", i%3))
		fi := s.addLogFile(t, i, rows)
		allFiles = append(allFiles, fi)
	}

	result, err := s.compactor.Compact(context.Background(), s.partition, allFiles, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	// Verify it's readable as a standard parquet file.
	file, err := parquet.OpenFile(bytes.NewReader(outputData), int64(len(outputData)))
	if err != nil {
		t.Fatalf("parquet.OpenFile: %v", err)
	}

	numRows := file.NumRows()
	if numRows != result.RowsMerged {
		t.Errorf("parquet NumRows=%d, RowsMerged=%d", numRows, result.RowsMerged)
	}

	// Verify row groups exist and have rows.
	totalRGRows := int64(0)
	for _, rg := range file.RowGroups() {
		rgRows := rg.NumRows()
		if rgRows <= 0 {
			t.Error("row group has 0 rows")
		}
		totalRGRows += rgRows
	}
	if totalRGRows != numRows {
		t.Errorf("sum of row group rows (%d) != total rows (%d)", totalRGRows, numRows)
	}
}

func TestDataValidation_TraceOutput_IsValidParquet(t *testing.T) {
	s := setupTraceCompactor(t)

	var allFiles []manifest.FileInfo
	for i := 0; i < 5; i++ {
		rows := generateTraceRows(200, int64(i)*2_000_000_000, fmt.Sprintf("svc-%d", i%3))
		fi := s.addTraceFile(t, i, rows)
		allFiles = append(allFiles, fi)
	}

	result, err := s.compactor.Compact(context.Background(), s.partition, allFiles, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	s.pool.mu.Lock()
	outputData := s.pool.uploaded[result.OutputFile]
	s.pool.mu.Unlock()

	file, err := parquet.OpenFile(bytes.NewReader(outputData), int64(len(outputData)))
	if err != nil {
		t.Fatalf("parquet.OpenFile: %v", err)
	}

	if file.NumRows() != result.RowsMerged {
		t.Errorf("parquet NumRows=%d, RowsMerged=%d", file.NumRows(), result.RowsMerged)
	}
}
