package manifest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestExtractPartition_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"standard", "logs/dt=2026-05-02/hour=10/00000-abc.parquet", "dt=2026-05-02/hour=10"},
		{"no hour", "logs/dt=2026-05-02/file.parquet", "dt=2026-05-02"},
		{"nested prefix", "a/b/c/dt=2026-01-01/hour=00/f.parquet", "dt=2026-01-01/hour=00"},
		{"no partition", "nopartition.parquet", ""},
		{"empty", "", ""},
		{"just dt", "dt=2026-01-01/file.parquet", "dt=2026-01-01"},
		{"hour only", "hour=10/file.parquet", ""},
		{"double dt", "dt=2026-01-01/dt=2026-02-01/hour=01/f.parquet", "dt=2026-02-01/hour=01"},
		{"double hour", "dt=2026-01-01/hour=05/hour=10/f.parquet", "dt=2026-01-01/hour=10"},
		{"no parquet ext", "dt=2026-01-01/hour=10/file.csv", "dt=2026-01-01/hour=10"},
		{"slash only", "/", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPartition(tt.key)
			if got != tt.want {
				t.Errorf("extractPartition(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestParsePartitionTime_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		wantHr  int
	}{
		{"with hour", "dt=2026-05-02/hour=10", false, 10},
		{"hour 0", "dt=2026-05-02/hour=00", false, 0},
		{"hour 23", "dt=2026-05-02/hour=23", false, 23},
		{"no hour", "dt=2026-05-02", false, 0},
		{"invalid hour 24", "dt=2026-05-02/hour=24", false, 0},
		{"negative hour", "dt=2026-05-02/hour=-1", false, 0},
		{"non-numeric hour", "dt=2026-05-02/hour=abc", false, 0},
		{"no dt", "hour=10", true, 0},
		{"empty", "", true, 0},
		{"bad date", "dt=not-a-date", true, 0},
		{"bad format", "dt=20260502", true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePartitionTime(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for %q", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error for %q: %v", tt.input, err)
				return
			}
			if got.Hour() != tt.wantHr {
				t.Errorf("hour = %d, want %d for %q", got.Hour(), tt.wantHr, tt.input)
			}
		})
	}
}

func TestHasDataForRange_EdgeCases(t *testing.T) {
	m := New("bucket", "logs/")

	if m.HasDataForRange(0, 1000) {
		t.Error("empty manifest should return false")
	}

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "f.parquet", Size: 100})

	baseNs := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC).UnixNano()

	tests := []struct {
		name  string
		start int64
		end   int64
		want  bool
	}{
		{"exact match", baseNs, endNs, true},
		{"overlaps start", baseNs - int64(time.Hour), baseNs + int64(30*time.Minute), true},
		{"overlaps end", baseNs + int64(30*time.Minute), endNs + int64(time.Hour), true},
		{"contains", baseNs - int64(time.Hour), endNs + int64(time.Hour), true},
		{"before", baseNs - int64(2*time.Hour), baseNs - int64(time.Hour), false},
		{"after", endNs + int64(time.Hour), endNs + int64(2*time.Hour), false},
		{"zero range", 0, 0, false},
		{"reversed range (start > end)", endNs, baseNs, true},
		{"single nanosecond overlap", baseNs, baseNs + 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.HasDataForRange(tt.start, tt.end)
			if got != tt.want {
				t.Errorf("HasDataForRange(%d, %d) = %v, want %v", tt.start, tt.end, got, tt.want)
			}
		})
	}
}

func TestGetFilesForRange_MultiPartition(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{Key: "b.parquet", Size: 200})
	m.AddFile("dt=2026-05-01/hour=12", FileInfo{Key: "c.parquet", Size: 300})

	start := time.Date(2026, 5, 1, 10, 30, 0, 0, time.UTC).UnixNano()
	end := time.Date(2026, 5, 1, 11, 30, 0, 0, time.UTC).UnixNano()

	files := m.GetFilesForRange(start, end)
	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d", len(files))
	}
}

func TestGetFilesForRange_Empty(t *testing.T) {
	m := New("bucket", "logs/")

	files := m.GetFilesForRange(0, 1000)
	if len(files) != 0 {
		t.Errorf("expected 0 files from empty manifest, got %d", len(files))
	}
}

func TestAddFile_UpdatesMinMax(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	if m.TotalFiles() != 1 {
		t.Errorf("totalFiles = %d, want 1", m.TotalFiles())
	}
	if m.TotalBytes() != 100 {
		t.Errorf("totalBytes = %d, want 100", m.TotalBytes())
	}

	m.AddFile("dt=2026-04-01/hour=00", FileInfo{Key: "b.parquet", Size: 200})
	if m.MinTime().Month() != time.April {
		t.Errorf("minTime month = %v, want April", m.MinTime().Month())
	}

	m.AddFile("dt=2026-06-01/hour=23", FileInfo{Key: "c.parquet", Size: 300})
	if m.TotalFiles() != 3 {
		t.Errorf("totalFiles = %d, want 3", m.TotalFiles())
	}
	if m.TotalBytes() != 600 {
		t.Errorf("totalBytes = %d, want 600", m.TotalBytes())
	}
}

func TestAddFile_InvalidPartition(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("invalid-partition", FileInfo{Key: "f.parquet", Size: 100})
	if m.TotalFiles() != 1 {
		t.Errorf("totalFiles = %d, want 1", m.TotalFiles())
	}
	if !m.MinTime().IsZero() {
		t.Error("minTime should be zero for invalid partition")
	}
}

func TestPartitionCount(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "b.parquet", Size: 100})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{Key: "c.parquet", Size: 100})

	if m.PartitionCount() != 2 {
		t.Errorf("partitionCount = %d, want 2", m.PartitionCount())
	}
}

func TestSaveTo_OverwriteExisting(t *testing.T) {
	m := New("bucket", "logs/")

	savePath := filepath.Join(t.TempDir(), "manifest.json")

	// Save with one file
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	if err := m.SaveTo(savePath); err != nil {
		t.Fatalf("first save: %v", err)
	}

	// Modify and save again to same path
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{Key: "b.parquet", Size: 200})
	if err := m.SaveTo(savePath); err != nil {
		t.Fatalf("second save: %v", err)
	}

	// Load and verify the second save took effect
	m2 := New("bucket", "logs/")
	if err := m2.LoadFrom(savePath); err != nil {
		t.Fatalf("load: %v", err)
	}
	if m2.TotalFiles() != 2 {
		t.Errorf("TotalFiles = %d, want 2 after overwrite", m2.TotalFiles())
	}
	if m2.TotalBytes() != 300 {
		t.Errorf("TotalBytes = %d, want 300 after overwrite", m2.TotalBytes())
	}
}

func TestLoadFrom_InvalidJSON(t *testing.T) {
	m := New("bucket", "logs/")

	path := filepath.Join(t.TempDir(), "manifest.json")
	_ = os.WriteFile(path, []byte("this is not json{{{"), 0o600)

	err := m.LoadFrom(path)
	if err == nil {
		t.Fatal("expected error loading invalid JSON")
	}
	// Should be an unmarshal error
	if m.TotalFiles() != 0 {
		t.Error("manifest should remain empty after failed load")
	}
}

func TestSaveTo_ReadOnlyDir(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	// Create a read-only directory
	roDir := filepath.Join(t.TempDir(), "readonly")
	if err := os.MkdirAll(roDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(roDir, 0o444); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(roDir, 0o755) }()

	path := filepath.Join(roDir, "manifest.json")
	err := m.SaveTo(path)
	if err == nil {
		t.Fatal("expected error saving to read-only directory")
	}
}

func TestRemoveFile_NonexistentPartition(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	// Remove from a partition that doesn't exist
	m.RemoveFile("dt=2026-06-01/hour=00", "a.parquet")

	// Should not change anything
	if m.TotalFiles() != 1 {
		t.Errorf("TotalFiles = %d, want 1", m.TotalFiles())
	}
	if m.TotalBytes() != 100 {
		t.Errorf("TotalBytes = %d, want 100", m.TotalBytes())
	}
}

func TestRemoveFile_LastInPartition(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	m.RemoveFile("dt=2026-05-01/hour=10", "a.parquet")

	if m.TotalFiles() != 0 {
		t.Errorf("TotalFiles = %d, want 0", m.TotalFiles())
	}
	if m.PartitionCount() != 0 {
		t.Errorf("PartitionCount = %d, want 0 (empty partition should be deleted)", m.PartitionCount())
	}
}

func TestAllFiles_Empty(t *testing.T) {
	m := New("bucket", "logs/")

	all := m.AllFiles()
	if len(all) != 0 {
		t.Errorf("AllFiles on empty = %d partitions, want 0", len(all))
	}
}

func TestLoadFrom_RestoresTimeBounds(t *testing.T) {
	m := New("bucket", "logs/")

	may1h10 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	may2h15 := time.Date(2026, 5, 2, 15, 0, 0, 0, time.UTC)

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-02/hour=15", FileInfo{Key: "b.parquet", Size: 200})

	// Record the original times (as UnixNano, timezone-independent)
	origMinNs := m.MinTime().UnixNano()
	origMaxNs := m.MaxTime().UnixNano()

	savePath := filepath.Join(t.TempDir(), "manifest.json")
	if err := m.SaveTo(savePath); err != nil {
		t.Fatal(err)
	}

	m2 := New("bucket", "logs/")
	if err := m2.LoadFrom(savePath); err != nil {
		t.Fatal(err)
	}

	// Compare using UnixNano to avoid timezone issues (LoadFrom uses time.Unix(0, ns)
	// which may use local timezone)
	if m2.MinTime().UnixNano() != origMinNs {
		t.Errorf("minTime ns = %d, want %d", m2.MinTime().UnixNano(), origMinNs)
	}
	if m2.MinTime().UnixNano() != may1h10.UnixNano() {
		t.Errorf("minTime ns = %d, want %d (may1h10)", m2.MinTime().UnixNano(), may1h10.UnixNano())
	}

	// maxTime = end of hour=15 partition = hour=16
	expectedMax := may2h15.Add(time.Hour)
	if m2.MaxTime().UnixNano() != origMaxNs {
		t.Errorf("maxTime ns = %d, want %d", m2.MaxTime().UnixNano(), origMaxNs)
	}
	if m2.MaxTime().UnixNano() != expectedMax.UnixNano() {
		t.Errorf("maxTime ns = %d, want %d (expected)", m2.MaxTime().UnixNano(), expectedMax.UnixNano())
	}

	// Verify HasDataForRange works with restored time bounds
	if !m2.HasDataForRange(may1h10.UnixNano(), expectedMax.UnixNano()) {
		t.Error("expected data in restored range")
	}
}

func TestGetFilesForRange_InvalidPartitionSkipped(t *testing.T) {
	m := New("bucket", "logs/")

	// Directly inject an invalid partition key into the files map
	m.mu.Lock()
	m.files["invalid-partition"] = []FileInfo{{Key: "bad.parquet", Size: 100}}
	m.files["dt=2026-05-01/hour=10"] = []FileInfo{{Key: "good.parquet", Size: 200}}
	m.minTime = time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	m.maxTime = time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC)
	m.totalFiles = 2
	m.mu.Unlock()

	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	end := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC).UnixNano()

	files := m.GetFilesForRange(start, end)
	// Should only return the good file (invalid partition is skipped by parsePartitionTime)
	if len(files) != 1 {
		t.Errorf("expected 1 file (invalid partition skipped), got %d", len(files))
	}
	if len(files) > 0 && files[0].Key != "good.parquet" {
		t.Errorf("expected good.parquet, got %s", files[0].Key)
	}
}

func TestSaveTo_MkdirAllError(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	// Use /dev/null as parent — can't create subdirectories under it
	err := m.SaveTo("/dev/null/sub/manifest.json")
	if err == nil {
		t.Fatal("expected error from MkdirAll on /dev/null path")
	}
}

func TestLoadFrom_ReadError(t *testing.T) {
	m := New("bucket", "logs/")

	// Create a directory where the file should be — reading it will error
	dir := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	err := m.LoadFrom(dir)
	if err == nil {
		t.Fatal("expected error reading a directory as file")
	}
}

// testS3Client creates an S3 client pointing at the given httptest server URL.
func testS3Client(t *testing.T, endpoint string) *s3.Client {
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

func TestRefreshFromS3_WithParquetFiles(t *testing.T) {
	// Mock S3 ListObjectsV2 that returns parquet files in partitioned paths
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>logs/dt=2026-05-01/hour=10/00001.parquet</Key>
    <Size>5000</Size>
  </Contents>
  <Contents>
    <Key>logs/dt=2026-05-01/hour=11/00002.parquet</Key>
    <Size>3000</Size>
  </Contents>
  <Contents>
    <Key>logs/dt=2026-05-02/hour=14/00003.parquet</Key>
    <Size>7000</Size>
  </Contents>
  <Contents>
    <Key>logs/dt=2026-05-01/hour=10/readme.txt</Key>
    <Size>100</Size>
  </Contents>
  <Contents>
    <Key>logs/nopartition.parquet</Key>
    <Size>200</Size>
  </Contents>
</ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := testS3Client(t, srv.URL)
	m := New("test-bucket", "logs/")

	err := m.RefreshFromS3(context.Background(), client)
	if err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}

	// 3 valid parquet files (one txt and one with no partition are skipped)
	if m.TotalFiles() != 3 {
		t.Errorf("TotalFiles = %d, want 3", m.TotalFiles())
	}
	if m.TotalBytes() != 15000 {
		t.Errorf("TotalBytes = %d, want 15000", m.TotalBytes())
	}
	if m.PartitionCount() != 3 {
		t.Errorf("PartitionCount = %d, want 3", m.PartitionCount())
	}

	// Check time bounds
	may1h10 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	may2h15 := time.Date(2026, 5, 2, 15, 0, 0, 0, time.UTC)
	if m.MinTime().UnixNano() != may1h10.UnixNano() {
		t.Errorf("MinTime = %v, want %v", m.MinTime(), may1h10)
	}
	if m.MaxTime().UnixNano() != may2h15.UnixNano() {
		t.Errorf("MaxTime = %v, want %v", m.MaxTime(), may2h15)
	}

	// HasDataForRange should work
	if !m.HasDataForRange(may1h10.UnixNano(), may2h15.UnixNano()) {
		t.Error("expected data in range")
	}
}

func TestRefreshFromS3_Empty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := testS3Client(t, srv.URL)
	m := New("test-bucket", "logs/")

	err := m.RefreshFromS3(context.Background(), client)
	if err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}

	if m.TotalFiles() != 0 {
		t.Errorf("TotalFiles = %d, want 0", m.TotalFiles())
	}
	if m.PartitionCount() != 0 {
		t.Errorf("PartitionCount = %d, want 0", m.PartitionCount())
	}
}

func TestRefreshFromS3_Error(t *testing.T) {
	// Server that returns 500 on ListObjectsV2
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := testS3Client(t, srv.URL)
	m := New("test-bucket", "logs/")

	err := m.RefreshFromS3(context.Background(), client)
	if err == nil {
		t.Fatal("expected error from failing S3")
	}
}

func TestRefreshFromS3_ReplacesOldData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>logs/dt=2026-06-01/hour=00/new.parquet</Key>
    <Size>9999</Size>
  </Contents>
</ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := testS3Client(t, srv.URL)
	m := New("test-bucket", "logs/")

	// Pre-populate with old data
	m.AddFile("dt=2026-01-01/hour=00", FileInfo{Key: "old.parquet", Size: 1})

	err := m.RefreshFromS3(context.Background(), client)
	if err != nil {
		t.Fatalf("RefreshFromS3: %v", err)
	}

	// Old data should be replaced
	if m.TotalFiles() != 1 {
		t.Errorf("TotalFiles = %d, want 1", m.TotalFiles())
	}
	if m.TotalBytes() != 9999 {
		t.Errorf("TotalBytes = %d, want 9999", m.TotalBytes())
	}
}
