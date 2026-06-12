package manifest

import (
	"encoding/json"
	"testing"
	"time"
)

func TestExtractPartition(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"logs/dt=2026-05-02/hour=10/00000-abc.parquet", "dt=2026-05-02/hour=10"},
		{"traces/dt=2026-04-01/hour=00/file.parquet", "dt=2026-04-01/hour=00"},
		{"prefix/tenant/logs/dt=2026-01-15/hour=23/data.parquet", "dt=2026-01-15/hour=23"},
		{"dt=2026-05-02/hour=10/file.parquet", "dt=2026-05-02/hour=10"},
		{"no-partition/file.parquet", ""},
		{"dt=2026-05-02/file.parquet", "dt=2026-05-02"},
	}
	for _, tt := range tests {
		got := extractPartition(tt.key)
		if got != tt.want {
			t.Errorf("extractPartition(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

// TestExtractTenantPartition pins the tenant-isolated partition: the FULL key
// directory (incl. the account/project prefix) so each tenant's pmeta bundles
// are physically separate, mirroring the data path. "" when there's no dt=.
func TestExtractTenantPartition(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"0/0/logs/dt=2026-06-09/hour=10/00000-abc.parquet", "0/0/logs/dt=2026-06-09/hour=10"},
		{"1/2/traces/dt=2026-04-01/hour=00/file.parquet", "1/2/traces/dt=2026-04-01/hour=00"},
		{"7/3/logs/dt=2026-01-15/hour=23/data.parquet", "7/3/logs/dt=2026-01-15/hour=23"},
		{"0/0/logs/dt=2026-05-02/file.parquet", "0/0/logs/dt=2026-05-02"},
		{"0/0/logs/no-partition/file.parquet", ""},
		{"no-partition/file.parquet", ""},
	}
	for _, tt := range tests {
		got := ExtractTenantPartition(tt.key)
		if got != tt.want {
			t.Errorf("ExtractTenantPartition(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestParsePartitionTime(t *testing.T) {
	tests := []struct {
		partition string
		wantYear  int
		wantMonth time.Month
		wantDay   int
		wantHour  int
		wantErr   bool
	}{
		{"dt=2026-05-02/hour=10", 2026, time.May, 2, 10, false},
		{"dt=2026-01-15/hour=00", 2026, time.January, 15, 0, false},
		{"dt=2026-12-31/hour=23", 2026, time.December, 31, 23, false},
		{"dt=2026-05-02", 2026, time.May, 2, 0, false},
		{"hour=10", 0, 0, 0, 0, true},
		{"invalid", 0, 0, 0, 0, true},
	}
	for _, tt := range tests {
		got, err := parsePartitionTime(tt.partition)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parsePartitionTime(%q) expected error", tt.partition)
			}
			continue
		}
		if err != nil {
			t.Errorf("parsePartitionTime(%q) error: %v", tt.partition, err)
			continue
		}
		if got.Year() != tt.wantYear || got.Month() != tt.wantMonth || got.Day() != tt.wantDay || got.Hour() != tt.wantHour {
			t.Errorf("parsePartitionTime(%q) = %v, want %d-%02d-%02d %02d:00",
				tt.partition, got, tt.wantYear, tt.wantMonth, tt.wantDay, tt.wantHour)
		}
	}
}

func TestManifest_HasDataForRange(t *testing.T) {
	m := newTestManifest()

	may2 := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	may3 := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)

	m.mu.Lock()
	m.files = map[string][]FileInfo{
		"dt=2026-05-02/hour=10": {{Key: "logs/dt=2026-05-02/hour=10/file.parquet", Size: 1000}},
		"dt=2026-05-02/hour=11": {{Key: "logs/dt=2026-05-02/hour=11/file.parquet", Size: 2000}},
	}
	m.minTime = may2.Add(10 * time.Hour)
	m.maxTime = may2.Add(12 * time.Hour)
	m.totalFiles = 2
	m.mu.Unlock()

	// Query overlapping the data range
	if !m.HasDataForRange(may2.Add(10*time.Hour).UnixNano(), may2.Add(11*time.Hour).UnixNano()) {
		t.Error("expected data for overlapping range")
	}

	// Query entirely before
	if m.HasDataForRange(may2.UnixNano(), may2.Add(9*time.Hour).UnixNano()) {
		t.Error("expected no data for range before min")
	}

	// Query entirely after
	if m.HasDataForRange(may3.UnixNano(), may3.Add(time.Hour).UnixNano()) {
		t.Error("expected no data for range after max")
	}
}

func TestManifest_GetFilesForRange(t *testing.T) {
	m := newTestManifest()

	may2 := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)

	m.mu.Lock()
	m.files = map[string][]FileInfo{
		"dt=2026-05-02/hour=10": {
			{Key: "logs/dt=2026-05-02/hour=10/a.parquet", Size: 1000},
			{Key: "logs/dt=2026-05-02/hour=10/b.parquet", Size: 2000},
		},
		"dt=2026-05-02/hour=11": {
			{Key: "logs/dt=2026-05-02/hour=11/c.parquet", Size: 3000},
		},
		"dt=2026-05-02/hour=14": {
			{Key: "logs/dt=2026-05-02/hour=14/d.parquet", Size: 4000},
		},
	}
	m.minTime = may2.Add(10 * time.Hour)
	m.maxTime = may2.Add(15 * time.Hour)
	m.totalFiles = 4
	m.rebuildIndex()
	m.mu.Unlock()

	// Query for hour 10-12 should get 3 files (hour=10 and hour=11)
	files := m.GetFilesForRange(
		may2.Add(10*time.Hour).UnixNano(),
		may2.Add(12*time.Hour).UnixNano(),
	)
	if len(files) != 3 {
		t.Errorf("expected 3 files for hour 10-12, got %d", len(files))
	}

	// Query for hour 14-15 should get 1 file
	files = m.GetFilesForRange(
		may2.Add(14*time.Hour).UnixNano(),
		may2.Add(15*time.Hour).UnixNano(),
	)
	if len(files) != 1 {
		t.Errorf("expected 1 file for hour 14-15, got %d", len(files))
	}

	// Query for hour 12-13 should get 0 files (gap)
	files = m.GetFilesForRange(
		may2.Add(12*time.Hour).UnixNano(),
		may2.Add(13*time.Hour).UnixNano(),
	)
	if len(files) != 0 {
		t.Errorf("expected 0 files for hour 12-13, got %d", len(files))
	}
}

func TestManifest_Empty(t *testing.T) {
	m := newTestManifest()

	if m.HasDataForRange(0, time.Now().UnixNano()) {
		t.Error("empty manifest should have no data")
	}
	if files := m.GetFilesForRange(0, time.Now().UnixNano()); len(files) != 0 {
		t.Error("empty manifest should return no files")
	}
	if m.TotalFiles() != 0 {
		t.Error("empty manifest should have 0 files")
	}
	if m.TotalBytes() != 0 {
		t.Error("empty manifest should have 0 bytes")
	}
}

func TestFileInfo_CompressionRatio(t *testing.T) {
	fi := FileInfo{RawBytes: 10000, Size: 2500}
	if r := fi.CompressionRatio(); r != 4.0 {
		t.Errorf("CompressionRatio = %f, want 4.0", r)
	}

	fi2 := FileInfo{RawBytes: 0, Size: 100}
	if r := fi2.CompressionRatio(); r != 0 {
		t.Errorf("CompressionRatio with zero raw = %f, want 0", r)
	}

	fi3 := FileInfo{RawBytes: 100, Size: 0}
	if r := fi3.CompressionRatio(); r != 0 {
		t.Errorf("CompressionRatio with zero size = %f, want 0", r)
	}
}

func TestManifest_AddFile_EnhancedFileInfo(t *testing.T) {
	m := newTestManifest()

	may2h10 := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)

	fi := FileInfo{
		Key:               "logs/dt=2026-05-02/hour=10/abc.parquet",
		Size:              5000,
		RowCount:          200,
		MinTimeNs:         may2h10.UnixNano(),
		MaxTimeNs:         may2h10.Add(30 * time.Minute).UnixNano(),
		RawBytes:          25000,
		SchemaFingerprint: "abcd1234",
	}
	m.AddFile("dt=2026-05-02/hour=10", fi)

	files := m.GetFilesForRange(may2h10.UnixNano(), may2h10.Add(time.Hour).UnixNano())
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].RowCount != 200 {
		t.Errorf("RowCount = %d, want 200", files[0].RowCount)
	}
	if files[0].RawBytes != 25000 {
		t.Errorf("RawBytes = %d, want 25000", files[0].RawBytes)
	}
	if files[0].SchemaFingerprint != "abcd1234" {
		t.Errorf("SchemaFingerprint = %q, want abcd1234", files[0].SchemaFingerprint)
	}
	if r := files[0].CompressionRatio(); r != 5.0 {
		t.Errorf("CompressionRatio = %f, want 5.0", r)
	}
}

func TestManifest_SaveLoadRoundTrip(t *testing.T) {
	m := newTestManifest()

	may2h10 := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	may2h11 := time.Date(2026, 5, 2, 11, 0, 0, 0, time.UTC)

	fi1 := FileInfo{
		Key:               "logs/dt=2026-05-02/hour=10/a.parquet",
		Size:              1000,
		RowCount:          50,
		MinTimeNs:         may2h10.UnixNano(),
		MaxTimeNs:         may2h10.Add(59 * time.Minute).UnixNano(),
		RawBytes:          5000,
		SchemaFingerprint: "fp1",
	}
	fi2 := FileInfo{
		Key:               "logs/dt=2026-05-02/hour=11/b.parquet",
		Size:              2000,
		RowCount:          100,
		MinTimeNs:         may2h11.UnixNano(),
		MaxTimeNs:         may2h11.Add(59 * time.Minute).UnixNano(),
		RawBytes:          10000,
		SchemaFingerprint: "fp1",
	}
	m.AddFile("dt=2026-05-02/hour=10", fi1)
	m.AddFile("dt=2026-05-02/hour=11", fi2)

	savePath := t.TempDir() + "/manifest.json"
	if err := m.SaveTo(savePath); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}

	m2 := newTestManifest()
	if err := m2.LoadFrom(savePath); err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}

	if m2.TotalFiles() != 2 {
		t.Errorf("loaded TotalFiles = %d, want 2", m2.TotalFiles())
	}
	if m2.TotalBytes() != 3000 {
		t.Errorf("loaded TotalBytes = %d, want 3000", m2.TotalBytes())
	}

	files := m2.GetFilesForRange(may2h10.UnixNano(), may2h11.Add(time.Hour).UnixNano())
	if len(files) != 2 {
		t.Fatalf("loaded GetFilesForRange = %d, want 2", len(files))
	}

	var found bool
	for _, f := range files {
		if f.Key == fi1.Key && f.RowCount == 50 && f.RawBytes == 5000 {
			found = true
		}
	}
	if !found {
		t.Error("fi1 metadata not preserved after save/load")
	}
}

func TestManifest_LoadFrom_NotExist(t *testing.T) {
	m := newTestManifest()
	if err := m.LoadFrom(t.TempDir() + "/nonexistent.json"); err != nil {
		t.Errorf("LoadFrom nonexistent should return nil, got: %v", err)
	}
	if m.TotalFiles() != 0 {
		t.Error("should be empty after loading nonexistent")
	}
}

func TestManifest_SaveTo_CreatesDir(t *testing.T) {
	m := newTestManifest()
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "test.parquet", Size: 100})

	path := t.TempDir() + "/subdir/deep/manifest.json"
	if err := m.SaveTo(path); err != nil {
		t.Fatalf("SaveTo should create dirs: %v", err)
	}
}

func TestFileInfo_Labels(t *testing.T) {
	fi := FileInfo{
		Key:  "test.parquet",
		Size: 1000,
		Labels: map[string][]string{
			"service.name":  {"api", "worker"},
			"severity_text": {"INFO", "ERROR"},
		},
	}

	if !fi.MatchesLabel("service.name", "api") {
		t.Error("should match service.name=api")
	}
	if !fi.MatchesLabel("service.name", "worker") {
		t.Error("should match service.name=worker")
	}
	if fi.MatchesLabel("service.name", "unknown") {
		t.Error("should not match service.name=unknown")
	}
	if fi.MatchesLabel("missing.field", "any") {
		t.Error("should not match missing field")
	}
}

func TestFileInfo_MatchesLabel_NilLabels(t *testing.T) {
	fi := FileInfo{Key: "test.parquet", Size: 1000}
	if fi.MatchesLabel("service.name", "api") {
		t.Error("nil labels should not match")
	}
}

func TestManifest_AllFiles(t *testing.T) {
	m := newTestManifest()
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-02/hour=11", FileInfo{Key: "b.parquet", Size: 200})
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "c.parquet", Size: 300})

	all := m.AllFiles()
	total := 0
	for _, files := range all {
		total += len(files)
	}
	if total != 3 {
		t.Errorf("AllFiles total = %d, want 3", total)
	}
}

func TestManifest_RemoveFile(t *testing.T) {
	m := newTestManifest()
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "b.parquet", Size: 200})

	m.RemoveFile("dt=2026-05-02/hour=10", "a.parquet")

	if m.TotalFiles() != 1 {
		t.Errorf("TotalFiles = %d, want 1", m.TotalFiles())
	}
	if m.TotalBytes() != 200 {
		t.Errorf("TotalBytes = %d, want 200", m.TotalBytes())
	}
}

func TestManifest_RemoveFile_NotFound(t *testing.T) {
	m := newTestManifest()
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	m.RemoveFile("dt=2026-05-02/hour=10", "nonexistent.parquet")

	if m.TotalFiles() != 1 {
		t.Errorf("TotalFiles = %d, want 1 (no change)", m.TotalFiles())
	}
}

func TestManifest_SaveLoadRoundTrip_WithLabels(t *testing.T) {
	m := newTestManifest()
	may2h10 := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)

	fi := FileInfo{
		Key:       "test.parquet",
		Size:      1000,
		RowCount:  50,
		MinTimeNs: may2h10.UnixNano(),
		MaxTimeNs: may2h10.Add(30 * time.Minute).UnixNano(),
		Labels: map[string][]string{
			"service.name": {"api", "worker"},
		},
	}
	m.AddFile("dt=2026-05-02/hour=10", fi)

	path := t.TempDir() + "/manifest.json"
	if err := m.SaveTo(path); err != nil {
		t.Fatal(err)
	}

	m2 := newTestManifest()
	if err := m2.LoadFrom(path); err != nil {
		t.Fatal(err)
	}

	files := m2.GetFilesForRange(may2h10.UnixNano(), may2h10.Add(time.Hour).UnixNano())
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if !files[0].MatchesLabel("service.name", "api") {
		t.Error("labels not preserved after save/load")
	}
}

func TestManifest_GetPartitions(t *testing.T) {
	m := newTestManifest()

	m.mu.Lock()
	m.files = map[string][]FileInfo{
		"dt=2026-05-01/hour=10": {{Key: "logs/dt=2026-05-01/hour=10/a.parquet", Size: 1000}},
		"dt=2026-05-01/hour=11": {{Key: "logs/dt=2026-05-01/hour=11/b.parquet", Size: 2000}},
		"dt=2026-05-02/hour=00": {
			{Key: "logs/dt=2026-05-02/hour=00/c.parquet", Size: 3000},
			{Key: "logs/dt=2026-05-02/hour=00/d.parquet", Size: 4000},
		},
		"dt=2026-05-03/hour=05": {{Key: "logs/dt=2026-05-03/hour=05/e.parquet", Size: 5000}},
	}
	m.totalFiles = 5
	m.mu.Unlock()

	// All partitions
	all := m.GetPartitions("", "")
	if len(all) != 3 {
		t.Fatalf("expected 3 dates, got %d", len(all))
	}
	if all[0].Date != "2026-05-01" || all[0].Files != 2 || all[0].Bytes != 3000 {
		t.Errorf("date 0: got %+v", all[0])
	}
	if all[1].Date != "2026-05-02" || all[1].Files != 2 || all[1].Bytes != 7000 {
		t.Errorf("date 1: got %+v", all[1])
	}
	if all[2].Date != "2026-05-03" || all[2].Files != 1 || all[2].Bytes != 5000 {
		t.Errorf("date 2: got %+v", all[2])
	}

	// Filtered by date range
	filtered := m.GetPartitions("2026-05-02", "2026-05-02")
	if len(filtered) != 1 {
		t.Fatalf("expected 1 date, got %d", len(filtered))
	}
	if filtered[0].Date != "2026-05-02" {
		t.Errorf("expected 2026-05-02, got %s", filtered[0].Date)
	}
	if len(filtered[0].Hours) != 1 || filtered[0].Hours[0] != 0 {
		t.Errorf("expected hours [0], got %v", filtered[0].Hours)
	}

	// Empty range
	empty := m.GetPartitions("2026-06-01", "2026-06-30")
	if len(empty) != 0 {
		t.Errorf("expected 0 dates, got %d", len(empty))
	}
}

func TestFileInfoJSONBackwardCompat(t *testing.T) {
	// Old JSON without new StorageClass fields must still deserialize correctly.
	oldJSON := `{"key":"logs/dt=2026-05-02/hour=10/a.parquet","size":1000,"row_count":50}`
	var fi FileInfo
	if err := json.Unmarshal([]byte(oldJSON), &fi); err != nil {
		t.Fatalf("unmarshal old JSON: %v", err)
	}
	if fi.Key != "logs/dt=2026-05-02/hour=10/a.parquet" {
		t.Errorf("Key = %q, want logs/dt=2026-05-02/hour=10/a.parquet", fi.Key)
	}
	if fi.Size != 1000 {
		t.Errorf("Size = %d, want 1000", fi.Size)
	}
	if fi.StorageClass != "" {
		t.Errorf("StorageClass = %q, want empty string", fi.StorageClass)
	}
	if !fi.ClassCheckedAt.IsZero() {
		t.Errorf("ClassCheckedAt = %v, want zero", fi.ClassCheckedAt)
	}
	if fi.ClassSource != "" {
		t.Errorf("ClassSource = %q, want empty string", fi.ClassSource)
	}
	if !fi.CreatedAt.IsZero() {
		t.Errorf("CreatedAt = %v, want zero", fi.CreatedAt)
	}
}

func TestFileInfoJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	fi := FileInfo{
		Key:            "logs/dt=2026-05-02/hour=10/a.parquet",
		Size:           2000,
		StorageClass:   "GLACIER",
		ClassCheckedAt: now,
		ClassSource:    "s3_head",
		CreatedAt:      now.Add(-24 * time.Hour),
	}

	data, err := json.Marshal(fi)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var fi2 FileInfo
	if err := json.Unmarshal(data, &fi2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if fi2.StorageClass != "GLACIER" {
		t.Errorf("StorageClass = %q, want GLACIER", fi2.StorageClass)
	}
	if !fi2.ClassCheckedAt.Equal(now) {
		t.Errorf("ClassCheckedAt = %v, want %v", fi2.ClassCheckedAt, now)
	}
	if fi2.ClassSource != "s3_head" {
		t.Errorf("ClassSource = %q, want s3_head", fi2.ClassSource)
	}
	if !fi2.CreatedAt.Equal(now.Add(-24 * time.Hour)) {
		t.Errorf("CreatedAt = %v, want %v", fi2.CreatedAt, now.Add(-24*time.Hour))
	}
}

func TestAddFilePreservesNewFields(t *testing.T) {
	m := newTestManifest()
	may2h10 := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	checked := time.Date(2026, 5, 13, 8, 0, 0, 0, time.UTC)

	fi := FileInfo{
		Key:            "logs/dt=2026-05-02/hour=10/abc.parquet",
		Size:           5000,
		MinTimeNs:      may2h10.UnixNano(),
		MaxTimeNs:      may2h10.Add(30 * time.Minute).UnixNano(),
		StorageClass:   "STANDARD",
		ClassCheckedAt: checked,
		ClassSource:    "list_objects",
		CreatedAt:      may2h10,
	}
	m.AddFile("dt=2026-05-02/hour=10", fi)

	files := m.GetFilesForRange(may2h10.UnixNano(), may2h10.Add(time.Hour).UnixNano())
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	got := files[0]
	if got.StorageClass != "STANDARD" {
		t.Errorf("StorageClass = %q, want STANDARD", got.StorageClass)
	}
	if !got.ClassCheckedAt.Equal(checked) {
		t.Errorf("ClassCheckedAt = %v, want %v", got.ClassCheckedAt, checked)
	}
	if got.ClassSource != "list_objects" {
		t.Errorf("ClassSource = %q, want list_objects", got.ClassSource)
	}
	if !got.CreatedAt.Equal(may2h10) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, may2h10)
	}
}

func newTestManifest() *Manifest {
	return New("test-bucket", "logs/")
}

func TestManifest_PartitionStats(t *testing.T) {
	m := New("test-bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key: "logs/dt=2026-05-01/hour=10/a.parquet", Size: 1000, RowCount: 500,
	})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key: "logs/dt=2026-05-01/hour=10/b.parquet", Size: 2000, RowCount: 300,
	})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{
		Key: "logs/dt=2026-05-01/hour=11/c.parquet", Size: 1500, RowCount: 400,
	})

	stats := m.GetPartitionStats()
	if len(stats) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(stats))
	}

	h10 := stats["dt=2026-05-01/hour=10"]
	if h10.TotalRows != 800 {
		t.Errorf("hour=10 rows = %d, want 800", h10.TotalRows)
	}
	if h10.FileCount != 2 {
		t.Errorf("hour=10 files = %d, want 2", h10.FileCount)
	}

	h11 := stats["dt=2026-05-01/hour=11"]
	if h11.TotalRows != 400 {
		t.Errorf("hour=11 rows = %d, want 400", h11.TotalRows)
	}
}

func TestManifest_GetRowCountForRange(t *testing.T) {
	m := New("test-bucket", "logs/")

	may1h10 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	may1h11 := time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC)
	may1h12 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key: "logs/dt=2026-05-01/hour=10/a.parquet", Size: 1000, RowCount: 500,
		MinTimeNs: may1h10.UnixNano(), MaxTimeNs: may1h10.Add(30 * time.Minute).UnixNano(),
	})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{
		Key: "logs/dt=2026-05-01/hour=11/b.parquet", Size: 2000, RowCount: 300,
		MinTimeNs: may1h11.UnixNano(), MaxTimeNs: may1h11.Add(30 * time.Minute).UnixNano(),
	})
	m.AddFile("dt=2026-05-01/hour=12", FileInfo{
		Key: "logs/dt=2026-05-01/hour=12/c.parquet", Size: 1500, RowCount: 400,
		MinTimeNs: may1h12.UnixNano(), MaxTimeNs: may1h12.Add(30 * time.Minute).UnixNano(),
	})

	// Range covering hours 10-11 should return 800 rows
	total := m.GetRowCountForRange(may1h10.UnixNano(), may1h12.UnixNano())
	if total != 800 {
		t.Errorf("row count for 10-12 range = %d, want 800", total)
	}

	// Full range should return 1200
	total = m.GetRowCountForRange(may1h10.UnixNano(), may1h12.Add(time.Hour).UnixNano())
	if total != 1200 {
		t.Errorf("row count for full range = %d, want 1200", total)
	}
}

func TestManifest_GetRowCountsByPartition(t *testing.T) {
	m := New("test-bucket", "logs/")

	may1h10 := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	may1h11 := time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC)

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key: "logs/dt=2026-05-01/hour=10/a.parquet", Size: 1000, RowCount: 500,
	})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key: "logs/dt=2026-05-01/hour=10/b.parquet", Size: 2000, RowCount: 300,
	})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{
		Key: "logs/dt=2026-05-01/hour=11/c.parquet", Size: 1500, RowCount: 400,
	})

	// Add a third partition that should be excluded by the range
	m.AddFile("dt=2026-05-01/hour=12", FileInfo{
		Key: "logs/dt=2026-05-01/hour=12/d.parquet", Size: 3000, RowCount: 600,
	})

	buckets := m.GetRowCountsByPartition(may1h10.UnixNano(), may1h11.Add(time.Hour).UnixNano())
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets (hour=12 excluded), got %d", len(buckets))
	}
	if buckets[0].RowCount != 800 {
		t.Errorf("bucket[0] rows = %d, want 800", buckets[0].RowCount)
	}
	if buckets[1].RowCount != 400 {
		t.Errorf("bucket[1] rows = %d, want 400", buckets[1].RowCount)
	}
}

func TestManifest_TotalRows(t *testing.T) {
	m := New("bucket", "logs/")

	// Empty manifest returns 0.
	if got := m.TotalRows(); got != 0 {
		t.Errorf("TotalRows() on empty = %d, want 0", got)
	}

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100, RowCount: 50})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "b.parquet", Size: 200, RowCount: 75})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{Key: "c.parquet", Size: 300, RowCount: 25})

	if got := m.TotalRows(); got != 150 {
		t.Errorf("TotalRows() = %d, want 150", got)
	}
}

func TestManifest_TotalRawBytes(t *testing.T) {
	m := New("bucket", "logs/")

	// Empty manifest returns 0.
	if got := m.TotalRawBytes(); got != 0 {
		t.Errorf("TotalRawBytes() on empty = %d, want 0", got)
	}

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100, RawBytes: 1000})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "b.parquet", Size: 200, RawBytes: 2000})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{Key: "c.parquet", Size: 300, RawBytes: 3000})

	if got := m.TotalRawBytes(); got != 6000 {
		t.Errorf("TotalRawBytes() = %d, want 6000", got)
	}
}

func TestManifest_BloomMeta(t *testing.T) {
	m := New("bucket", "logs/")

	// BloomAvailable returns false for unknown partition.
	if m.BloomAvailable("dt=2026-05-01") {
		t.Error("BloomAvailable() should be false for unknown partition")
	}

	// GetBloomMeta returns nil for unknown partition.
	if got := m.GetBloomMeta("dt=2026-05-01"); got != nil {
		t.Errorf("GetBloomMeta() = %v, want nil", got)
	}

	// SetBloomMeta and GetBloomMeta round-trip.
	meta := PartitionMeta{BloomAvailable: true, BloomSize: 1024}
	m.SetBloomMeta("dt=2026-05-01", meta)

	if !m.BloomAvailable("dt=2026-05-01") {
		t.Error("BloomAvailable() should be true after SetBloomMeta")
	}

	got := m.GetBloomMeta("dt=2026-05-01")
	if got == nil {
		t.Fatal("GetBloomMeta() returned nil after SetBloomMeta")
	}
	if !got.BloomAvailable {
		t.Error("BloomAvailable in returned meta should be true")
	}
	if got.BloomSize != 1024 {
		t.Errorf("BloomSize = %d, want 1024", got.BloomSize)
	}

	// BloomAvailable returns false when BloomAvailable field is false.
	meta2 := PartitionMeta{BloomAvailable: false}
	m.SetBloomMeta("dt=2026-05-02", meta2)
	if m.BloomAvailable("dt=2026-05-02") {
		t.Error("BloomAvailable() should be false when BloomAvailable=false in meta")
	}
}

func TestManifest_LabelIndex(t *testing.T) {
	m := newTestManifest()

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:  "logs/dt=2026-05-01/hour=10/a.parquet",
		Size: 1000,
		Labels: map[string][]string{
			"service.name": {"api", "worker"},
			"level":        {"error"},
		},
	})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:  "logs/dt=2026-05-01/hour=10/b.parquet",
		Size: 2000,
		Labels: map[string][]string{
			"service.name": {"api"},
			"level":        {"info"},
		},
	})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{
		Key:  "logs/dt=2026-05-01/hour=11/c.parquet",
		Size: 3000,
		Labels: map[string][]string{
			"service.name": {"worker"},
			"level":        {"warn"},
		},
	})

	// Exact match: service.name=api should return files a and b
	keys := m.GetFileKeysByLabel("service.name", "api")
	if len(keys) != 2 {
		t.Fatalf("service.name=api: got %d keys, want 2", len(keys))
	}
	if !keys["logs/dt=2026-05-01/hour=10/a.parquet"] || !keys["logs/dt=2026-05-01/hour=10/b.parquet"] {
		t.Errorf("service.name=api: wrong keys %v", keys)
	}

	// Exact match: service.name=worker should return files a and c
	keys = m.GetFileKeysByLabel("service.name", "worker")
	if len(keys) != 2 {
		t.Fatalf("service.name=worker: got %d keys, want 2", len(keys))
	}
	if !keys["logs/dt=2026-05-01/hour=10/a.parquet"] || !keys["logs/dt=2026-05-01/hour=11/c.parquet"] {
		t.Errorf("service.name=worker: wrong keys %v", keys)
	}

	// Exact match: level=error should return only file a
	keys = m.GetFileKeysByLabel("level", "error")
	if len(keys) != 1 {
		t.Fatalf("level=error: got %d keys, want 1", len(keys))
	}
	if !keys["logs/dt=2026-05-01/hour=10/a.parquet"] {
		t.Errorf("level=error: wrong keys %v", keys)
	}

	// Non-existent value returns nil
	keys = m.GetFileKeysByLabel("service.name", "nonexistent")
	if keys != nil {
		t.Errorf("nonexistent value: got %v, want nil", keys)
	}

	// Non-existent field returns nil
	keys = m.GetFileKeysByLabel("unknown_field", "api")
	if keys != nil {
		t.Errorf("unknown field: got %v, want nil", keys)
	}

	// After removing a file, index is updated
	m.RemoveFile("dt=2026-05-01/hour=10", "logs/dt=2026-05-01/hour=10/a.parquet")
	keys = m.GetFileKeysByLabel("service.name", "api")
	if len(keys) != 1 {
		t.Fatalf("after remove, service.name=api: got %d keys, want 1", len(keys))
	}
	if !keys["logs/dt=2026-05-01/hour=10/b.parquet"] {
		t.Errorf("after remove: wrong keys %v", keys)
	}

	// worker should now only be in file c
	keys = m.GetFileKeysByLabel("service.name", "worker")
	if len(keys) != 1 {
		t.Fatalf("after remove, service.name=worker: got %d keys, want 1", len(keys))
	}
}

func TestManifest_LabelIndex_SaveLoadRoundTrip(t *testing.T) {
	m := newTestManifest()

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{
		Key:  "logs/dt=2026-05-01/hour=10/a.parquet",
		Size: 1000,
		Labels: map[string][]string{
			"service.name": {"api"},
		},
	})

	path := t.TempDir() + "/manifest.json"
	if err := m.SaveTo(path); err != nil {
		t.Fatal(err)
	}

	m2 := newTestManifest()
	if err := m2.LoadFrom(path); err != nil {
		t.Fatal(err)
	}

	keys := m2.GetFileKeysByLabel("service.name", "api")
	if len(keys) != 1 {
		t.Fatalf("after load, service.name=api: got %d keys, want 1", len(keys))
	}
	if !keys["logs/dt=2026-05-01/hour=10/a.parquet"] {
		t.Error("label index not rebuilt after LoadFrom")
	}
}
