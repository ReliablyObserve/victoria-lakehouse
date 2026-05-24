package manifest

import (
	"fmt"
	"testing"
	"time"
)

// partitionForIndex maps a flat index to a valid "dt=YYYY-MM-DD/hour=HH" partition.
// Base date is 2025-01-01; each day has 24 hours, so index i maps to
// day=i/24, hour=i%24.
func partitionForIndex(i int) string {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	day := i / 24
	hour := i % 24
	date := base.AddDate(0, 0, day)
	return fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), hour)
}

// TestManifest_LargePartitionCount adds files across 1000 distinct partitions
// and verifies AllFiles returns all of them and GetFilesForRange narrows correctly.
func TestManifest_LargePartitionCount(t *testing.T) {
	m := New("stress-bucket", "logs/")

	const totalPartitions = 1000

	// Add 1 file per partition. Partitions span indices 0..999.
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < totalPartitions; i++ {
		partition := partitionForIndex(i)
		day := i / 24
		hour := i % 24
		partStart := base.AddDate(0, 0, day).Add(time.Duration(hour) * time.Hour)
		m.AddFile(partition, FileInfo{
			Key:       fmt.Sprintf("logs/%s/file-%04d.parquet", partition, i),
			Size:      1024,
			MinTimeNs: partStart.UnixNano(),
			MaxTimeNs: partStart.Add(time.Hour - 1).UnixNano(),
		})
	}

	// AllFiles must return exactly totalPartitions partitions.
	all := m.AllFiles()
	if len(all) != totalPartitions {
		t.Fatalf("AllFiles: got %d partitions, want %d", len(all), totalPartitions)
	}

	totalFiles := 0
	for _, files := range all {
		totalFiles += len(files)
	}
	if totalFiles != totalPartitions {
		t.Fatalf("AllFiles total files: got %d, want %d", totalFiles, totalPartitions)
	}

	// A narrow range covering exactly the first 3 partitions (hours 0-2 of day 0)
	// should return only 3 files.
	queryStart := base.UnixNano()
	queryEnd := base.Add(3 * time.Hour).UnixNano()

	files := m.GetFilesForRange(queryStart, queryEnd)
	if len(files) != 3 {
		t.Fatalf("GetFilesForRange narrow range: got %d files, want 3", len(files))
	}

	// Verify the returned files belong to the expected partitions.
	expected := map[string]bool{
		partitionForIndex(0): false,
		partitionForIndex(1): false,
		partitionForIndex(2): false,
	}
	for _, f := range files {
		p := extractPartition(f.Key)
		if _, ok := expected[p]; ok {
			expected[p] = true
		} else {
			t.Errorf("unexpected partition %q in result", p)
		}
	}
	for p, found := range expected {
		if !found {
			t.Errorf("partition %q missing from GetFilesForRange result", p)
		}
	}
}

// TestManifest_LargeFileCountPerPartition adds 500 files to a single partition
// and verifies FilesForPartition returns all of them. It also checks that
// adding another 500 is fast (not O(n²)).
func TestManifest_LargeFileCountPerPartition(t *testing.T) {
	m := New("stress-bucket", "logs/")

	const partition = "dt=2025-01-01/hour=10"
	const initialFiles = 500

	partStart := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)

	for i := 0; i < initialFiles; i++ {
		m.AddFile(partition, FileInfo{
			Key:       fmt.Sprintf("logs/%s/file-%06d.parquet", partition, i),
			Size:      1024,
			MinTimeNs: partStart.UnixNano(),
			MaxTimeNs: partStart.Add(time.Hour - 1).UnixNano(),
		})
	}

	files := m.FilesForPartition(partition)
	if len(files) != initialFiles {
		t.Fatalf("FilesForPartition: got %d files, want %d", len(files), initialFiles)
	}

	// Adding another 500 files should complete in < 100ms (not O(n²)).
	start := time.Now()
	for i := initialFiles; i < initialFiles*2; i++ {
		m.AddFile(partition, FileInfo{
			Key:       fmt.Sprintf("logs/%s/file-%06d.parquet", partition, i),
			Size:      1024,
			MinTimeNs: partStart.UnixNano(),
			MaxTimeNs: partStart.Add(time.Hour - 1).UnixNano(),
		})
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("adding 500 more files took %v, want < 100ms (O(n²) suspected)", elapsed)
	}

	files = m.FilesForPartition(partition)
	if len(files) != initialFiles*2 {
		t.Fatalf("FilesForPartition after growth: got %d files, want %d", len(files), initialFiles*2)
	}
}

// TestManifest_GetFilesForRange_Efficiency adds 10000 files across 500 partitions
// (20 files each) and verifies that a range query covering 10 partitions returns
// the right count and completes in < 50ms.
func TestManifest_GetFilesForRange_Efficiency(t *testing.T) {
	m := New("stress-bucket", "logs/")

	const (
		totalPartitions = 500
		filesPerPart    = 20
	)

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < totalPartitions; i++ {
		partition := partitionForIndex(i)
		day := i / 24
		hour := i % 24
		partStart := base.AddDate(0, 0, day).Add(time.Duration(hour) * time.Hour)

		for j := 0; j < filesPerPart; j++ {
			m.AddFile(partition, FileInfo{
				Key:       fmt.Sprintf("logs/%s/file-%06d.parquet", partition, j),
				Size:      1024,
				MinTimeNs: partStart.UnixNano(),
				MaxTimeNs: partStart.Add(time.Hour - 1).UnixNano(),
			})
		}
	}

	// Verify total file count.
	if got := m.TotalFiles(); got != totalPartitions*filesPerPart {
		t.Fatalf("TotalFiles: got %d, want %d", got, totalPartitions*filesPerPart)
	}

	// Query a 10-hour window starting at partition index 100 (i.e. partitions 100-109).
	// Each of those spans exactly 1 hour; 10 partitions × 20 files = 200 files expected.
	qIdx := 100
	qDay := qIdx / 24
	qHour := qIdx % 24
	queryStart := base.AddDate(0, 0, qDay).Add(time.Duration(qHour) * time.Hour)
	queryEnd := queryStart.Add(10 * time.Hour)

	start := time.Now()
	files := m.GetFilesForRange(queryStart.UnixNano(), queryEnd.UnixNano())
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Errorf("GetFilesForRange over 10 partitions took %v, want < 50ms", elapsed)
	}

	// Expect exactly 10 partitions × 20 files = 200 files.
	if len(files) != 200 {
		t.Fatalf("GetFilesForRange returned %d files, want 200", len(files))
	}
}

// TestManifest_RemoveFiles_LargeCount adds 1000 files across 1000 partitions,
// removes 500, and verifies correct residual count and absence from queries.
func TestManifest_RemoveFiles_LargeCount(t *testing.T) {
	m := New("stress-bucket", "logs/")

	const totalFiles = 1000
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	type fileRef struct {
		partition string
		key       string
	}
	refs := make([]fileRef, 0, totalFiles)

	for i := 0; i < totalFiles; i++ {
		partition := partitionForIndex(i)
		day := i / 24
		hour := i % 24
		partStart := base.AddDate(0, 0, day).Add(time.Duration(hour) * time.Hour)
		key := fmt.Sprintf("logs/%s/file-%06d.parquet", partition, i)
		m.AddFile(partition, FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: partStart.UnixNano(),
			MaxTimeNs: partStart.Add(time.Hour - 1).UnixNano(),
		})
		refs = append(refs, fileRef{partition: partition, key: key})
	}

	if got := m.TotalFiles(); got != totalFiles {
		t.Fatalf("before remove: TotalFiles = %d, want %d", got, totalFiles)
	}

	// Remove the first 500 files.
	const removeCount = 500
	for i := 0; i < removeCount; i++ {
		m.RemoveFile(refs[i].partition, refs[i].key)
	}

	if got := m.TotalFiles(); got != totalFiles-removeCount {
		t.Fatalf("after remove: TotalFiles = %d, want %d", got, totalFiles-removeCount)
	}

	// Removed files must not appear in any query. Pick a spot-check: first removed file.
	removed := refs[0]
	all := m.AllFiles()
	for partition, files := range all {
		for _, f := range files {
			if f.Key == removed.key {
				t.Errorf("removed file %q still present in partition %q", removed.key, partition)
			}
		}
	}

	// The first removed partition (index 0) should have no files.
	if files := m.FilesForPartition(refs[0].partition); len(files) != 0 {
		t.Errorf("removed partition still has %d files", len(files))
	}

	// Retained files (indices 500-999) must still be queryable.
	// Use a wide range covering all 1000 original partitions.
	lastIdx := totalFiles - 1
	lastDay := lastIdx / 24
	lastHour := lastIdx % 24
	queryEnd := base.AddDate(0, 0, lastDay).Add(time.Duration(lastHour+1) * time.Hour)

	files := m.GetFilesForRange(base.UnixNano(), queryEnd.UnixNano())
	if len(files) != totalFiles-removeCount {
		t.Fatalf("GetFilesForRange after remove: got %d files, want %d", len(files), totalFiles-removeCount)
	}
}
