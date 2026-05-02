package manifest

import (
	"testing"
	"time"
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
	l := testLogger()
	m := New("bucket", "logs/", l)

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
	l := testLogger()
	m := New("bucket", "logs/", l)

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
	l := testLogger()
	m := New("bucket", "logs/", l)

	files := m.GetFilesForRange(0, 1000)
	if len(files) != 0 {
		t.Errorf("expected 0 files from empty manifest, got %d", len(files))
	}
}

func TestAddFile_UpdatesMinMax(t *testing.T) {
	l := testLogger()
	m := New("bucket", "logs/", l)

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
	l := testLogger()
	m := New("bucket", "logs/", l)

	m.AddFile("invalid-partition", FileInfo{Key: "f.parquet", Size: 100})
	if m.TotalFiles() != 1 {
		t.Errorf("totalFiles = %d, want 1", m.TotalFiles())
	}
	if !m.MinTime().IsZero() {
		t.Error("minTime should be zero for invalid partition")
	}
}

func TestPartitionCount(t *testing.T) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-01/hour=10", FileInfo{Key: "b.parquet", Size: 100})
	m.AddFile("dt=2026-05-01/hour=11", FileInfo{Key: "c.parquet", Size: 100})

	if m.PartitionCount() != 2 {
		t.Errorf("partitionCount = %d, want 2", m.PartitionCount())
	}
}
