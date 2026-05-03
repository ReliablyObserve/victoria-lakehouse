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

func newTestManifest() *Manifest {
	return New("test-bucket", "logs/", testLogger())
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
