package manifest

import (
	"testing"
	"time"
)

func TestGetFilesForRange_BinarySearchCorrectness(t *testing.T) {
	m := &Manifest{}
	m.files = make(map[string][]FileInfo)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		pt := base.Add(time.Duration(i) * time.Hour)
		key := formatPartitionKey(pt)
		m.files[key] = []FileInfo{
			{Key: key + "/file.parquet", Size: 1000},
		}
	}
	m.rebuildIndex()

	start := base.Add(50 * time.Hour)
	end := start.Add(3 * time.Hour)
	startNs := start.UnixNano()
	endNs := end.UnixNano()

	files := m.GetFilesForRange(startNs, endNs)

	if len(files) < 3 {
		t.Fatalf("expected at least 3 files for 3-hour window, got %d", len(files))
	}
	if len(files) > 4 {
		t.Fatalf("expected at most 4 files, got %d", len(files))
	}
}

func TestGetFilesForRange_EmptyManifest(t *testing.T) {
	m := &Manifest{}
	m.files = make(map[string][]FileInfo)
	m.rebuildIndex()

	files := m.GetFilesForRange(0, time.Now().UnixNano())
	if len(files) != 0 {
		t.Fatalf("expected 0 files from empty manifest, got %d", len(files))
	}
}

func TestGetFilesForRange_SinglePartition(t *testing.T) {
	m := &Manifest{}
	m.files = make(map[string][]FileInfo)
	pt := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	key := formatPartitionKey(pt)
	m.files[key] = []FileInfo{
		{Key: key + "/a.parquet", Size: 500},
		{Key: key + "/b.parquet", Size: 600},
	}
	m.rebuildIndex()

	start := pt.Add(-30 * time.Minute)
	end := pt.Add(90 * time.Minute)
	files := m.GetFilesForRange(start.UnixNano(), end.UnixNano())
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}

	files = m.GetFilesForRange(0, pt.Add(-1*time.Hour).UnixNano())
	if len(files) != 0 {
		t.Fatalf("expected 0 files for range before partition, got %d", len(files))
	}
}

func TestRebuildIndex_SortsPartitions(t *testing.T) {
	m := &Manifest{}
	m.files = make(map[string][]FileInfo)

	times := []time.Time{
		time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	for _, pt := range times {
		key := formatPartitionKey(pt)
		m.files[key] = []FileInfo{{Key: key + "/f.parquet"}}
	}
	m.rebuildIndex()

	if len(m.sortedPartitions) != 3 {
		t.Fatalf("expected 3 sorted partitions, got %d", len(m.sortedPartitions))
	}
	for i := 1; i < len(m.sortedPartitions); i++ {
		if !m.sortedPartitions[i].start.After(m.sortedPartitions[i-1].start) {
			t.Errorf("partitions not sorted at index %d", i)
		}
	}
}

func formatPartitionKey(t time.Time) string {
	return "dt=" + t.Format("2006-01-02") + "/hour=" + t.Format("15")
}
