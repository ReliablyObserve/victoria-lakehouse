package manifest

import (
	"io"
	"log/slog"
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

func newTestManifest() *Manifest {
	return New("test-bucket", "logs/", testLogger())
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
