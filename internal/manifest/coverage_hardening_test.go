package manifest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestUpdateFileColumnStats_Found verifies that UpdateFileColumnStats applies
// column stats to a matching file.
func TestUpdateFileColumnStats_Found(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:  "logs/dt=2026-05-01/hour=10/a.parquet",
		Size: 100,
	})

	stats := map[string]ColumnMinMax{
		"severity": {Min: "INFO", Max: "ERROR"},
		"duration": {Min: "10", Max: "5000"},
	}
	m.UpdateFileColumnStats("logs/dt=2026-05-01/hour=10/a.parquet", stats)

	files := m.FilesForPartition("dt=2026-05-01/hour=10")
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].ColumnStats == nil {
		t.Fatal("ColumnStats should not be nil after update")
	}
	if files[0].ColumnStats["severity"].Min != "INFO" {
		t.Errorf("severity Min = %q, want INFO", files[0].ColumnStats["severity"].Min)
	}
	if files[0].ColumnStats["duration"].Max != "5000" {
		t.Errorf("duration Max = %q, want 5000", files[0].ColumnStats["duration"].Max)
	}
}

// TestUpdateFileColumnStats_NotFound verifies no panic when key doesn't match.
func TestUpdateFileColumnStats_NotFound(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:  "logs/dt=2026-05-01/hour=10/a.parquet",
		Size: 100,
	})

	// Should not panic or modify anything.
	m.UpdateFileColumnStats("nonexistent-key.parquet", map[string]ColumnMinMax{
		"x": {Min: "1", Max: "2"},
	})

	files := m.FilesForPartition("dt=2026-05-01/hour=10")
	if files[0].ColumnStats != nil {
		t.Error("ColumnStats should still be nil for non-matching key")
	}
}

// TestUpdateFileColumnStats_MultiplePartitions verifies correct file is updated
// across multiple partitions.
func TestUpdateFileColumnStats_MultiplePartitions(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{Key: "b.parquet", Size: 200})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{Key: "c.parquet", Size: 300})

	stats := map[string]ColumnMinMax{"col": {Min: "x", Max: "y"}}
	m.UpdateFileColumnStats("c.parquet", stats)

	// Only c.parquet should have stats.
	files10 := m.FilesForPartition("dt=2026-05-01/hour=10")
	if files10[0].ColumnStats != nil {
		t.Error("a.parquet should not have column stats")
	}

	files11 := m.FilesForPartition("dt=2026-05-01/hour=11")
	for _, fi := range files11 {
		if fi.Key == "b.parquet" && fi.ColumnStats != nil {
			t.Error("b.parquet should not have column stats")
		}
		if fi.Key == "c.parquet" {
			if fi.ColumnStats == nil {
				t.Error("c.parquet should have column stats")
			}
		}
	}
}

// TestUpdateFileColumnStats_EmptyStats verifies empty stats map is applied.
func TestUpdateFileColumnStats_EmptyStats(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	m.UpdateFileColumnStats("a.parquet", map[string]ColumnMinMax{})

	files := m.FilesForPartition("dt=2026-05-01/hour=10")
	if files[0].ColumnStats == nil {
		t.Error("ColumnStats should be set (even if empty)")
	}
	if len(files[0].ColumnStats) != 0 {
		t.Errorf("ColumnStats should be empty, got %d entries", len(files[0].ColumnStats))
	}
}

// TestEnrichFileMetadata_Found verifies enrichment of a matching file.
func TestEnrichFileMetadata_Found(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:  "a.parquet",
		Size: 100,
	})

	m.EnrichFileMetadata("a.parquet", 500, 1000000, 2000000)

	files := m.FilesForPartition("dt=2026-05-01/hour=10")
	if files[0].RowCount != 500 {
		t.Errorf("RowCount = %d, want 500", files[0].RowCount)
	}
	if files[0].MinTimeNs != 1000000 {
		t.Errorf("MinTimeNs = %d, want 1000000", files[0].MinTimeNs)
	}
	if files[0].MaxTimeNs != 2000000 {
		t.Errorf("MaxTimeNs = %d, want 2000000", files[0].MaxTimeNs)
	}
}

// TestEnrichFileMetadata_DoesNotOverwrite verifies that existing non-zero values
// are not overwritten.
func TestEnrichFileMetadata_DoesNotOverwrite(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:       "a.parquet",
		Size:      100,
		RowCount:  100,
		MinTimeNs: 500,
		MaxTimeNs: 600,
	})

	m.EnrichFileMetadata("a.parquet", 999, 111, 222)

	files := m.FilesForPartition("dt=2026-05-01/hour=10")
	if files[0].RowCount != 100 {
		t.Errorf("RowCount = %d, want 100 (original)", files[0].RowCount)
	}
	if files[0].MinTimeNs != 500 {
		t.Errorf("MinTimeNs = %d, want 500 (original)", files[0].MinTimeNs)
	}
	if files[0].MaxTimeNs != 600 {
		t.Errorf("MaxTimeNs = %d, want 600 (original)", files[0].MaxTimeNs)
	}
}

// TestEnrichFileMetadata_PartialEnrich verifies partial enrichment when some
// fields are zero and some are not.
func TestEnrichFileMetadata_PartialEnrich(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:       "a.parquet",
		Size:      100,
		RowCount:  50,  // already set
		MinTimeNs: 0,   // not set
		MaxTimeNs: 800, // already set
	})

	m.EnrichFileMetadata("a.parquet", 999, 111, 222)

	files := m.FilesForPartition("dt=2026-05-01/hour=10")
	if files[0].RowCount != 50 {
		t.Errorf("RowCount = %d, want 50 (original)", files[0].RowCount)
	}
	if files[0].MinTimeNs != 111 {
		t.Errorf("MinTimeNs = %d, want 111 (enriched)", files[0].MinTimeNs)
	}
	if files[0].MaxTimeNs != 800 {
		t.Errorf("MaxTimeNs = %d, want 800 (original)", files[0].MaxTimeNs)
	}
}

// TestEnrichFileMetadata_ZeroValues verifies zero enrichment values are not applied.
func TestEnrichFileMetadata_ZeroValues(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:  "a.parquet",
		Size: 100,
	})

	m.EnrichFileMetadata("a.parquet", 0, 0, 0)

	files := m.FilesForPartition("dt=2026-05-01/hour=10")
	if files[0].RowCount != 0 {
		t.Errorf("RowCount = %d, want 0 (zero input shouldn't apply)", files[0].RowCount)
	}
}

// TestEnrichFileMetadata_NotFound verifies no panic when key doesn't match.
func TestEnrichFileMetadata_NotFound(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	// Should not panic.
	m.EnrichFileMetadata("nonexistent.parquet", 500, 1000, 2000)

	files := m.FilesForPartition("dt=2026-05-01/hour=10")
	if files[0].RowCount != 0 {
		t.Error("file should not be enriched when key doesn't match")
	}
}

// TestEnrichFileMetadata_MultiplePartitions verifies enrichment finds the
// correct file across multiple partitions.
func TestEnrichFileMetadata_MultiplePartitions(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{Key: "b.parquet", Size: 200})

	m.EnrichFileMetadata("b.parquet", 300, 1000, 2000)

	files10 := m.FilesForPartition("dt=2026-05-01/hour=10")
	if files10[0].RowCount != 0 {
		t.Error("a.parquet should not be enriched")
	}

	files11 := m.FilesForPartition("dt=2026-05-01/hour=11")
	if files11[0].RowCount != 300 {
		t.Errorf("b.parquet RowCount = %d, want 300", files11[0].RowCount)
	}
}

// TestIndexFileLabels_ExceedsMaxLabels verifies the maxLabelsPerField guard.
func TestIndexFileLabels_ExceedsMaxLabels(t *testing.T) {
	m := New("bucket", "logs/")

	// Create a file with maxLabelsPerField labels in one field.
	labels := make([]string, maxLabelsPerField)
	for i := range labels {
		labels[i] = "val"
	}
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:    "a.parquet",
		Size:   100,
		Labels: map[string][]string{"field": labels},
	})

	// The label index should not include this field because it hit the limit.
	keys := m.GetFileKeysByLabel("field", "val")
	if keys != nil {
		t.Error("expected nil keys for field with >= maxLabelsPerField values")
	}
}

// TestTenantSummaries_OrgIDTemplate verifies the single-segment OrgID template path.
func TestTenantSummaries_OrgIDSingleSegment(t *testing.T) {
	m := New("bucket", "logs/")
	m.SetPrefixTemplate("{OrgID}/logs/")

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:      "myorg/logs/dt=2026-05-01/hour=10/a.parquet",
		Size:     100,
		RowCount: 50,
		RawBytes: 500,
	})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:      "myorg/logs/dt=2026-05-01/hour=10/b.parquet",
		Size:     200,
		RowCount: 100,
		RawBytes: 1000,
	})

	summaries := m.TenantSummaries()
	if len(summaries) != 1 {
		t.Fatalf("expected 1 tenant summary, got %d", len(summaries))
	}
	s := summaries[0]
	if s.AccountID != "myorg" {
		t.Errorf("AccountID = %q, want myorg", s.AccountID)
	}
	if s.ProjectID != "" {
		t.Errorf("ProjectID = %q, want empty for OrgID template", s.ProjectID)
	}
	if s.TotalFiles != 2 {
		t.Errorf("TotalFiles = %d, want 2", s.TotalFiles)
	}
	if s.TotalBytes != 300 {
		t.Errorf("TotalBytes = %d, want 300", s.TotalBytes)
	}
}

// TestTenantSummaries_ShortKey verifies keys with too few segments are skipped.
func TestTenantSummaries_ShortKey(t *testing.T) {
	m := New("bucket", "logs/")

	// Key without enough segments for 2-segment template.
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:  "short.parquet",
		Size: 100,
	})

	summaries := m.TenantSummaries()
	if len(summaries) != 0 {
		t.Errorf("expected 0 summaries for short key, got %d", len(summaries))
	}
}

// TestGetPartitions_FilterByDateRange verifies start/end date filtering.
func TestGetPartitions_FilterByDateRange(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "b.parquet", Size: 200})
	m.AddFile("dt=2026-05-03/hour=10", FileInfo{Key: "c.parquet", Size: 300})

	// Filter to middle date only.
	parts := m.GetPartitions("2026-05-02", "2026-05-02")
	if len(parts) != 1 {
		t.Fatalf("expected 1 partition in date range, got %d", len(parts))
	}
	if parts[0].Date != "2026-05-02" {
		t.Errorf("date = %q, want 2026-05-02", parts[0].Date)
	}

	// Filter with start only.
	parts = m.GetPartitions("2026-05-02", "")
	if len(parts) != 2 {
		t.Fatalf("expected 2 partitions from 2026-05-02 onwards, got %d", len(parts))
	}

	// Filter with end only.
	parts = m.GetPartitions("", "2026-05-01")
	if len(parts) != 1 {
		t.Fatalf("expected 1 partition up to 2026-05-01, got %d", len(parts))
	}
}

// TestGetFilesForRange_FileTimeBoundsFiltering verifies files are filtered by
// their MinTimeNs/MaxTimeNs when set. The filter uses: MaxTimeNs < startNs
// || MinTimeNs > endNs to skip files.
func TestGetFilesForRange_FileTimeBoundsFiltering(t *testing.T) {
	m := New("bucket", "logs/")

	// dt=2026-05-01/hour=10 => partition covers [2026-05-01 10:00, 2026-05-01 11:00)
	// Use actual nanosecond timestamps within this hour.
	partTime, _ := ParsePartitionTime("dt=2026-05-01/hour=10")
	hourStartNs := partTime.UnixNano()
	midNs := hourStartNs + 1800000000000 // 30 minutes in
	hourEndNs := hourStartNs + 3600000000000 - 1

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:       "early.parquet",
		Size:      100,
		MinTimeNs: hourStartNs,
		MaxTimeNs: midNs, // first half hour
	})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:       "late.parquet",
		Size:      200,
		MinTimeNs: midNs + 1,
		MaxTimeNs: hourEndNs,
	})

	// Query the full partition hour to get both.
	files := m.GetFilesForRange(hourStartNs, hourEndNs)
	if len(files) != 2 {
		t.Fatalf("expected 2 files for full range, got %d", len(files))
	}

	// Query first half only: [hourStart, midNs].
	// late.parquet: MinTimeNs=midNs+1 > endNs=midNs => skipped.
	files = m.GetFilesForRange(hourStartNs, midNs)
	if len(files) != 1 {
		t.Fatalf("expected 1 file for first half, got %d", len(files))
	}
	if files[0].Key != "early.parquet" {
		t.Errorf("expected early.parquet, got %s", files[0].Key)
	}

	// Query second half only: [midNs+1, hourEnd].
	// early.parquet: MaxTimeNs=midNs < startNs=midNs+1 => skipped.
	files = m.GetFilesForRange(midNs+1, hourEndNs)
	if len(files) != 1 {
		t.Fatalf("expected 1 file for second half, got %d", len(files))
	}
	if files[0].Key != "late.parquet" {
		t.Errorf("expected late.parquet, got %s", files[0].Key)
	}
}

// coverageS3Client creates an S3 client pointing at the given httptest server URL.
func coverageS3Client(t *testing.T, endpoint string) *s3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})
	return client
}

// TestWritePartitionSidecar_FullPath exercises the full WritePartitionSidecar
// code path including the PutObject call via a mock S3 server.
func TestWritePartitionSidecar_FullPath(t *testing.T) {
	var receivedBody string
	var receivedKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			receivedKey = r.URL.Path
			buf := make([]byte, 4096)
			n, _ := r.Body.Read(buf)
			receivedBody = string(buf[:n])
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("test-bucket", "prefix/")
	m.AddFile("dt=2026-05-20/hour=11", FileInfo{
		Key:      "prefix/dt=2026-05-20/hour=11/abc.parquet",
		Size:     1000,
		RowCount: 500,
	})

	err := m.WritePartitionSidecar(context.Background(), client, "dt=2026-05-20/hour=11")
	if err != nil {
		t.Fatalf("WritePartitionSidecar: %v", err)
	}

	if receivedKey == "" {
		t.Error("expected PutObject to be called")
	}
	if receivedBody == "" {
		t.Error("expected non-empty body in PutObject")
	}
}

// TestWritePartitionSidecar_S3Error exercises the error path when PutObject fails.
func TestWritePartitionSidecar_S3Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("test-bucket", "prefix/")
	m.AddFile("dt=2026-05-20/hour=11", FileInfo{
		Key:      "prefix/dt=2026-05-20/hour=11/abc.parquet",
		Size:     1000,
		RowCount: 500,
	})

	err := m.WritePartitionSidecar(context.Background(), client, "dt=2026-05-20/hour=11")
	if err == nil {
		t.Fatal("expected error from failing S3 PutObject")
	}
}

// TestRefreshFromS3_PreservesEnrichment exercises the enrichment preservation
// loop in RefreshFromS3 (lines 179-201): when a file exists in the manifest
// with enrichment data before refresh, those fields are preserved after refresh.
func TestRefreshFromS3_PreservesEnrichment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>logs/dt=2026-05-01/hour=10/a.parquet</Key>
    <Size>5000</Size>
  </Contents>
</ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("test-bucket", "logs/")

	// Pre-populate with enrichment data that should be preserved across refresh.
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:               "logs/dt=2026-05-01/hour=10/a.parquet",
		Size:              5000,
		RowCount:          1000,
		RawBytes:          50000,
		MinTimeNs:         1000000,
		MaxTimeNs:         2000000,
		SchemaFingerprint: "fp123",
		Labels:            map[string][]string{"service": {"api"}},
		ColumnStats:       map[string]ColumnMinMax{"col1": {Min: "a", Max: "z"}},
		StorageClass:      "GLACIER",
		CompactionLevel:   2,
	})

	err := m.RefreshFromS3(context.Background(), client)
	if err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}

	// Verify enrichment data was preserved.
	files := m.FilesForPartition("dt=2026-05-01/hour=10")
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	fi := files[0]
	if fi.RowCount != 1000 {
		t.Errorf("RowCount = %d, want 1000 (preserved)", fi.RowCount)
	}
	if fi.RawBytes != 50000 {
		t.Errorf("RawBytes = %d, want 50000 (preserved)", fi.RawBytes)
	}
	if fi.SchemaFingerprint != "fp123" {
		t.Errorf("SchemaFingerprint = %q, want fp123 (preserved)", fi.SchemaFingerprint)
	}
	if fi.Labels == nil || fi.Labels["service"][0] != "api" {
		t.Errorf("Labels not preserved: %v", fi.Labels)
	}
	if fi.ColumnStats == nil || fi.ColumnStats["col1"].Min != "a" {
		t.Errorf("ColumnStats not preserved: %v", fi.ColumnStats)
	}
	if fi.StorageClass != "GLACIER" {
		t.Errorf("StorageClass = %q, want GLACIER (preserved)", fi.StorageClass)
	}
	if fi.CompactionLevel != 2 {
		t.Errorf("CompactionLevel = %d, want 2 (preserved)", fi.CompactionLevel)
	}
}

// TestRefreshFromS3_MetricsWithRowData exercises the metrics code paths
// that require totalRows > 0 and totalRawBytes > 0 (compression ratio).
func TestRefreshFromS3_MetricsWithRowData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>0/0/logs/dt=2026-05-01/hour=10/a.parquet</Key>
    <Size>5000</Size>
  </Contents>
</ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("test-bucket", "0/0/logs/")

	// Pre-populate with row/raw data so totalRows > 0 after enrichment preservation.
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:      "0/0/logs/dt=2026-05-01/hour=10/a.parquet",
		Size:     5000,
		RowCount: 1000,
		RawBytes: 50000,
	})

	err := m.RefreshFromS3(context.Background(), client)
	if err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}

	// Verify the file's enrichment was preserved (and therefore metrics paths executed).
	if m.TotalRows() != 1000 {
		t.Errorf("TotalRows = %d, want 1000", m.TotalRows())
	}
	if m.TotalRawBytes() != 50000 {
		t.Errorf("TotalRawBytes = %d, want 50000", m.TotalRawBytes())
	}
}

// TestLoadSidecars_Basic exercises the full LoadSidecars code path using a
// mock S3 server that returns a sidecar JSON file.
func TestLoadSidecars_Basic(t *testing.T) {
	sc := &FileMetaSidecar{
		Files: map[string]FileMeta{
			"prefix/dt=2026-05-20/hour=11/abc.parquet": {
				RowCount:  500,
				MinTimeNs: 1000000,
				MaxTimeNs: 2000000,
				RawBytes:  5000,
			},
		},
	}
	sidecarData, err := MarshalFileMetaSidecar(sc)
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(sidecarData)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("test-bucket", "prefix/")
	m.AddFile("dt=2026-05-20/hour=11", FileInfo{
		Key:  "prefix/dt=2026-05-20/hour=11/abc.parquet",
		Size: 1000,
	})

	enriched := m.LoadSidecars(context.Background(), client, 1)
	if enriched != 1 {
		t.Errorf("enriched = %d, want 1", enriched)
	}

	files := m.FilesForPartition("dt=2026-05-20/hour=11")
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].RowCount != 500 {
		t.Errorf("RowCount = %d, want 500 (from sidecar)", files[0].RowCount)
	}
	if files[0].MinTimeNs != 1000000 {
		t.Errorf("MinTimeNs = %d, want 1000000", files[0].MinTimeNs)
	}
}

// TestLoadSidecars_EmptyManifest exercises the early return path when no
// partitions exist.
func TestLoadSidecars_EmptyManifest(t *testing.T) {
	m := New("test-bucket", "prefix/")

	// Should return 0 immediately since no partitions exist.
	enriched := m.LoadSidecars(context.Background(), nil, 1)
	if enriched != 0 {
		t.Errorf("enriched = %d, want 0 for empty manifest", enriched)
	}
}

// TestLoadSidecars_DefaultConcurrency exercises the concurrency <= 0 path.
func TestLoadSidecars_DefaultConcurrency(t *testing.T) {
	sc := &FileMetaSidecar{
		Files: map[string]FileMeta{
			"prefix/dt=2026-05-20/hour=11/abc.parquet": {
				RowCount: 100,
			},
		},
	}
	sidecarData, _ := MarshalFileMetaSidecar(sc)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(sidecarData)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("test-bucket", "prefix/")
	m.AddFile("dt=2026-05-20/hour=11", FileInfo{
		Key:  "prefix/dt=2026-05-20/hour=11/abc.parquet",
		Size: 100,
	})

	// Pass concurrency=0 to exercise the default path.
	enriched := m.LoadSidecars(context.Background(), client, 0)
	if enriched != 1 {
		t.Errorf("enriched = %d, want 1 with default concurrency", enriched)
	}
}

// TestLoadSidecars_S3Error exercises the error path when GetObject fails.
func TestLoadSidecars_S3Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("test-bucket", "prefix/")
	m.AddFile("dt=2026-05-20/hour=11", FileInfo{
		Key:  "prefix/dt=2026-05-20/hour=11/abc.parquet",
		Size: 100,
	})

	// Should not panic; errors are silently ignored.
	enriched := m.LoadSidecars(context.Background(), client, 1)
	if enriched != 0 {
		t.Errorf("enriched = %d, want 0 (S3 errors)", enriched)
	}
}

// TestLoadSidecars_InvalidJSON exercises the path where the sidecar file
// contains invalid JSON.
func TestLoadSidecars_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("not-valid-json{"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("test-bucket", "prefix/")
	m.AddFile("dt=2026-05-20/hour=11", FileInfo{
		Key:  "prefix/dt=2026-05-20/hour=11/abc.parquet",
		Size: 100,
	})

	enriched := m.LoadSidecars(context.Background(), client, 1)
	if enriched != 0 {
		t.Errorf("enriched = %d, want 0 (invalid JSON)", enriched)
	}
}

// TestLoadSidecars_ContextCancelled exercises the context cancellation path.
func TestLoadSidecars_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("test-bucket", "prefix/")
	m.AddFile("dt=2026-05-20/hour=11", FileInfo{
		Key:  "prefix/dt=2026-05-20/hour=11/abc.parquet",
		Size: 100,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	enriched := m.LoadSidecars(ctx, client, 1)
	if enriched != 0 {
		t.Errorf("enriched = %d, want 0 (cancelled context)", enriched)
	}
}
