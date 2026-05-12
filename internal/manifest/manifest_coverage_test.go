package manifest

import (
	"testing"
	"time"
)

// TestExportedExtractPartition tests the exported wrapper ExtractPartition.
func TestExportedExtractPartition(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"logs/dt=2026-05-02/hour=10/file.parquet", "dt=2026-05-02/hour=10"},
		{"dt=2026-01-01/file.parquet", "dt=2026-01-01"},
		{"nopartition/file.parquet", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got := ExtractPartition(tt.key)
		if got != tt.want {
			t.Errorf("ExtractPartition(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

// TestExportedParsePartitionTime tests the exported wrapper ParsePartitionTime.
func TestExportedParsePartitionTime(t *testing.T) {
	tests := []struct {
		partition string
		wantHour  int
		wantErr   bool
	}{
		{"dt=2026-05-02/hour=10", 10, false},
		{"dt=2026-01-15/hour=00", 0, false},
		{"dt=2026-05-02", 0, false},
		{"hour=10", 0, true},
		{"invalid", 0, true},
		{"dt=not-a-date", 0, true},
	}
	for _, tt := range tests {
		got, err := ParsePartitionTime(tt.partition)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParsePartitionTime(%q) expected error", tt.partition)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParsePartitionTime(%q) error: %v", tt.partition, err)
			continue
		}
		if got.Hour() != tt.wantHour {
			t.Errorf("ParsePartitionTime(%q) hour = %d, want %d", tt.partition, got.Hour(), tt.wantHour)
		}
	}
}

// TestFilesForPartition tests the FilesForPartition method.
func TestFilesForPartition(t *testing.T) {
	m := New("test-bucket", "logs/")

	// Empty partition returns empty slice.
	files := m.FilesForPartition("dt=2026-05-02/hour=10")
	if len(files) != 0 {
		t.Errorf("expected 0 files for empty partition, got %d", len(files))
	}

	// Add files to a partition.
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "b.parquet", Size: 200})
	m.AddFile("dt=2026-05-02/hour=11", FileInfo{Key: "c.parquet", Size: 300})

	files = m.FilesForPartition("dt=2026-05-02/hour=10")
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}

	// Verify it returns a copy (modifying the returned slice shouldn't affect manifest).
	files[0].Key = "modified"
	original := m.FilesForPartition("dt=2026-05-02/hour=10")
	if original[0].Key == "modified" {
		t.Error("FilesForPartition should return a copy, not the original")
	}

	// Non-existent partition returns empty.
	none := m.FilesForPartition("dt=2026-12-31/hour=00")
	if len(none) != 0 {
		t.Errorf("expected 0 files for non-existent partition, got %d", len(none))
	}
}

// TestGetPartitions_DateOnlyPartitions verifies handling of date-only partitions
// (without hour= part).
func TestGetPartitions_DateOnlyPartitions(t *testing.T) {
	m := New("bucket", "logs/")

	m.mu.Lock()
	m.files = map[string][]FileInfo{
		"dt=2026-05-01": {{Key: "a.parquet", Size: 100}},
	}
	m.totalFiles = 1
	m.mu.Unlock()

	parts := m.GetPartitions("", "")
	if len(parts) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(parts))
	}
	if parts[0].Date != "2026-05-01" {
		t.Errorf("date = %q, want 2026-05-01", parts[0].Date)
	}
	// No hour part means no hours in the summary.
	if len(parts[0].Hours) != 0 {
		t.Errorf("expected 0 hours for date-only partition, got %v", parts[0].Hours)
	}
}

// TestSaveTo_MarshalError exercises the code path where json.Marshal would fail.
// In practice this can't happen with the manifest struct, but we test the
// surrounding code paths (successful save after AddFile).
func TestSaveTo_AfterMultipleAddRemove(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "b.parquet", Size: 200})
	m.RemoveFile("dt=2026-05-01/hour=10", "a.parquet")

	savePath := t.TempDir() + "/manifest.json"
	if err := m.SaveTo(savePath); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}

	m2 := New("bucket", "logs/")
	if err := m2.LoadFrom(savePath); err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if m2.TotalFiles() != 1 {
		t.Errorf("TotalFiles = %d, want 1", m2.TotalFiles())
	}
	if m2.TotalBytes() != 200 {
		t.Errorf("TotalBytes = %d, want 200", m2.TotalBytes())
	}
}

// TestLoadFrom_ZeroTimeBounds verifies loading with zero time bounds.
func TestLoadFrom_ZeroTimeBounds(t *testing.T) {
	m := New("bucket", "logs/")

	// Add file with invalid partition so times remain zero.
	m.AddFile("invalid-partition", FileInfo{Key: "x.parquet", Size: 50})

	savePath := t.TempDir() + "/manifest.json"
	if err := m.SaveTo(savePath); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}

	m2 := New("bucket", "logs/")
	if err := m2.LoadFrom(savePath); err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	// The manifest should have been loaded with 1 file.
	if m2.TotalFiles() != 1 {
		t.Errorf("TotalFiles = %d, want 1", m2.TotalFiles())
	}
	if m2.TotalBytes() != 50 {
		t.Errorf("TotalBytes = %d, want 50", m2.TotalBytes())
	}
}

// TestAddFile_UpdatesMinMaxTime tests that AddFile properly updates min/max times.
func TestAddFile_UpdatesMinMaxTime_Extended(t *testing.T) {
	m := New("bucket", "logs/")

	// First file sets initial times.
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	if m.MinTime().IsZero() {
		t.Fatal("MinTime should not be zero after adding file")
	}

	expected := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)
	if m.MinTime().UnixNano() != expected.UnixNano() {
		t.Errorf("MinTime = %v, want %v", m.MinTime(), expected)
	}

	// Add earlier file - should update min.
	m.AddFile("dt=2026-04-01/hour=00", FileInfo{Key: "b.parquet", Size: 200})
	expectedMin := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	if m.MinTime().UnixNano() != expectedMin.UnixNano() {
		t.Errorf("MinTime = %v, want %v", m.MinTime(), expectedMin)
	}

	// Add later file - should update max.
	m.AddFile("dt=2026-12-31/hour=23", FileInfo{Key: "c.parquet", Size: 300})
	expectedMax := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) // hour=23 + 1 hour
	if m.MaxTime().UnixNano() != expectedMax.UnixNano() {
		t.Errorf("MaxTime = %v, want %v", m.MaxTime(), expectedMax)
	}
}

// TestGetFilesForRange_SortedOutput verifies files are returned sorted by key.
func TestGetFilesForRange_SortedOutput(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "z_file.parquet", Size: 100})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a_file.parquet", Size: 200})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "m_file.parquet", Size: 300})

	start := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC).UnixNano()
	end := time.Date(2026, 5, 1, 11, 0, 0, 0, time.UTC).UnixNano()

	files := m.GetFilesForRange(start, end)
	if len(files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(files))
	}
	if files[0].Key != "a_file.parquet" {
		t.Errorf("files[0].Key = %q, want a_file.parquet", files[0].Key)
	}
	if files[1].Key != "m_file.parquet" {
		t.Errorf("files[1].Key = %q, want m_file.parquet", files[1].Key)
	}
	if files[2].Key != "z_file.parquet" {
		t.Errorf("files[2].Key = %q, want z_file.parquet", files[2].Key)
	}
}
