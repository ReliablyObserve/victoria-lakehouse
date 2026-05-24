package parquets3

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// sfTestTime is the base time for the test partition "dt=2026-05-24/hour=10".
var sfTestTime = time.Date(2026, 5, 24, 10, 0, 0, 0, time.UTC)

// sfStartNs and sfEndNs define the time range within the test partition.
var sfStartNs = sfTestTime.UnixNano()
var sfEndNs = sfTestTime.Add(time.Hour).UnixNano() - 1

// addSFTestFiles populates the manifest with files in a single partition.
func addSFTestFiles(s *Storage, keys []string) {
	partition := "dt=2026-05-24/hour=10"
	for _, key := range keys {
		s.manifest.AddFile(partition, manifest.FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: sfStartNs,
			MaxTimeNs: sfEndNs,
		})
	}
}

// filterSpecificFiles replicates the file-selection logic inside
// QuerySpecificFiles without calling queryFile (which requires S3).
func filterSpecificFiles(s *Storage, fileKeys []string, startNs, endNs int64) []manifest.FileInfo {
	if len(fileKeys) == 0 {
		return nil
	}
	keySet := make(map[string]bool, len(fileKeys))
	for _, k := range fileKeys {
		keySet[k] = true
	}
	allFiles := s.manifest.GetFilesForRange(startNs, endNs)
	var files []manifest.FileInfo
	for _, f := range allFiles {
		if keySet[f.Key] {
			files = append(files, f)
		}
	}
	return files
}

func TestQuerySpecificFiles_EmptyKeys_ReturnsNil(t *testing.T) {
	s := newMinimalStorage()
	addSFTestFiles(s, []string{"file-a.parquet", "file-b.parquet"})

	err := s.QuerySpecificFiles(context.Background(), nil, sfStartNs, sfEndNs, "", nil, noopWriteBlock)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	err = s.QuerySpecificFiles(context.Background(), []string{}, sfStartNs, sfEndNs, "", nil, noopWriteBlock)
	if err != nil {
		t.Fatalf("expected nil error for empty slice, got %v", err)
	}
}

func TestQuerySpecificFiles_NoMatchingFiles_ReturnsNil(t *testing.T) {
	s := newMinimalStorage()
	addSFTestFiles(s, []string{"file-a.parquet", "file-b.parquet"})

	err := s.QuerySpecificFiles(context.Background(), []string{"nonexistent.parquet"}, sfStartNs, sfEndNs, "", nil, noopWriteBlock)
	if err != nil {
		t.Fatalf("expected nil error for no matching files, got %v", err)
	}
}

func TestQuerySpecificFiles_FiltersCorrectly(t *testing.T) {
	s := newMinimalStorage()

	allKeys := []string{
		"file-a.parquet",
		"file-b.parquet",
		"file-c.parquet",
		"file-d.parquet",
		"file-e.parquet",
	}
	addSFTestFiles(s, allKeys)

	// Verify the filtering logic: 5 manifest files, request 2 keys, get 2 matches.
	requestedKeys := []string{"file-b.parquet", "file-d.parquet"}
	matched := filterSpecificFiles(s, requestedKeys, sfStartNs, sfEndNs)

	if len(matched) != 2 {
		t.Fatalf("expected 2 matched files, got %d", len(matched))
	}
	matchedKeys := make(map[string]bool)
	for _, f := range matched {
		matchedKeys[f.Key] = true
	}
	if !matchedKeys["file-b.parquet"] || !matchedKeys["file-d.parquet"] {
		t.Errorf("unexpected matched files: %v", matched)
	}

	// Also verify full manifest has 5 files.
	allFiles := s.manifest.GetFilesForRange(sfStartNs, sfEndNs)
	if len(allFiles) != 5 {
		t.Fatalf("expected 5 total manifest files, got %d", len(allFiles))
	}
}

func TestQuerySpecificFiles_AllKeys(t *testing.T) {
	s := newMinimalStorage()

	allKeys := []string{
		"file-a.parquet",
		"file-b.parquet",
		"file-c.parquet",
	}
	addSFTestFiles(s, allKeys)

	// Request all keys -- all should match.
	matched := filterSpecificFiles(s, allKeys, sfStartNs, sfEndNs)
	if len(matched) != 3 {
		t.Errorf("expected 3 matched files, got %d", len(matched))
	}
}

func TestQuerySpecificFiles_DuplicateKeys(t *testing.T) {
	s := newMinimalStorage()

	allKeys := []string{
		"file-a.parquet",
		"file-b.parquet",
		"file-c.parquet",
	}
	addSFTestFiles(s, allKeys)

	// Pass duplicate keys -- deduplication should produce only 2 unique matches.
	duplicateKeys := []string{
		"file-a.parquet",
		"file-a.parquet",
		"file-a.parquet",
		"file-b.parquet",
		"file-b.parquet",
	}

	matched := filterSpecificFiles(s, duplicateKeys, sfStartNs, sfEndNs)
	if len(matched) != 2 {
		t.Errorf("expected 2 matched files (deduped from 5 input keys), got %d", len(matched))
	}
}

func TestQuerySpecificFiles_NoGoroutineLeak(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()

	for i := 0; i < 100; i++ {
		s := newMinimalStorage()
		addSFTestFiles(s, []string{"file-a.parquet", "file-b.parquet"})
		// Call with no matching keys to avoid S3 download attempts.
		_ = s.QuerySpecificFiles(context.Background(), []string{"nonexistent.parquet"}, sfStartNs, sfEndNs, "", nil, noopWriteBlock)
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()

	if after > baseline+5 {
		t.Errorf("goroutine leak: baseline=%d, after=%d (delta=%d)", baseline, after, after-baseline)
	}
}

func TestQuerySpecificFiles_NoMemoryLeak(t *testing.T) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := 0; i < 1000; i++ {
		s := newMinimalStorage()
		addSFTestFiles(s, []string{"file-a.parquet", "file-b.parquet", "file-c.parquet"})
		// Exercise the full filtering path without hitting S3.
		_ = filterSpecificFiles(s, []string{"file-a.parquet", "file-c.parquet"}, sfStartNs, sfEndNs)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	const maxGrowthBytes = 10 * 1024 * 1024
	if growth > maxGrowthBytes {
		t.Errorf("memory leak: heap grew by %d bytes (limit %d)", growth, maxGrowthBytes)
	}
}
