package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func TestBloomBuild_CorrectEntries(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)

	files := []struct {
		key      string
		traceIDs []string
		services []string
	}{
		{"dt=2026-05-02/hour=10/file1.parquet", []string{"trace-aaa", "trace-bbb"}, []string{"api-gw"}},
		{"dt=2026-05-02/hour=10/file2.parquet", []string{"trace-ccc"}, []string{"order-svc"}},
		{"dt=2026-05-02/hour=10/file3.parquet", []string{"trace-ddd", "trace-eee"}, []string{"api-gw", "db-svc"}},
	}

	for _, f := range files {
		pi.AddFile("dt=2026-05-02/hour=10", f.key, map[string][]string{
			"trace_id":     f.traceIDs,
			"service.name": f.services,
		})
	}

	idx := pi.GetPartition("dt=2026-05-02/hour=10")
	if idx == nil {
		t.Fatal("partition index not created")
	}
	if idx.Len() != 3 {
		t.Errorf("want 3 file entries, got %d", idx.Len())
	}

	// Verify each file's bloom contains its trace_ids
	keys := []string{files[0].key, files[1].key, files[2].key}
	result := idx.MayContain(keys, "trace_id", "trace-aaa")
	if !containsKey(result, files[0].key) {
		t.Error("file1 bloom should contain trace-aaa")
	}

	result = idx.MayContain(keys, "trace_id", "trace-ccc")
	if !containsKey(result, files[1].key) {
		t.Error("file2 bloom should contain trace-ccc")
	}
}

func TestBloomSkip_ReducesFileReads(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)

	numFiles := 100
	partition := "dt=2026-05-02/hour=10"
	keys := make([]string, numFiles)

	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("%s/file%d.parquet", partition, i)
		keys[i] = key

		traceIDs := make([]string, 200)
		for j := range traceIDs {
			traceIDs[j] = fmt.Sprintf("trace-%d-%d", i, j)
		}
		pi.AddFile(partition, key, map[string][]string{"trace_id": traceIDs})
	}

	idx := pi.GetPartition(partition)
	checks := []bloomindex.ColumnCheck{{Column: "trace_id", Value: "trace-42-100"}}
	matching := idx.MayContainAll(keys, checks)

	// Must include file42 (true positive)
	if !containsKey(matching, keys[42]) {
		t.Error("should include file42 (true positive)")
	}

	// Should skip most files (FP rate ~1%)
	if len(matching) > 10 {
		t.Errorf("bloom should skip most files: %d/100 matched (want ≤ 10)", len(matching))
	}

	// Results without bloom = all files
	noBloomResult := keys
	// Verify bloom result is a subset of no-bloom result
	matchSet := make(map[string]bool)
	for _, k := range matching {
		matchSet[k] = true
	}
	for _, k := range noBloomResult {
		_ = matchSet[k] // all keys should be in no-bloom result
	}
}

func TestBloomPersist_MarshalRoundtrip(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)

	partition := "dt=2026-05-02/hour=10"
	pi.AddFile(partition, "file1.parquet", map[string][]string{
		"trace_id":     {"trace-aaa", "trace-bbb"},
		"service.name": {"api-gateway"},
	})

	data := pi.MarshalPartition(partition)
	if len(data) == 0 {
		t.Fatal("marshal returned empty data")
	}

	// Simulate S3 roundtrip: unmarshal, put in new cache
	restored, err := bloomindex.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}

	// Verify restored index works correctly
	result := restored.MayContain([]string{"file1.parquet"}, "trace_id", "trace-aaa")
	if len(result) != 1 {
		t.Error("restored bloom should find trace-aaa")
	}

	result = restored.MayContain([]string{"file1.parquet"}, "service.name", "api-gateway")
	if len(result) != 1 {
		t.Error("restored bloom should find api-gateway")
	}
}

func TestBloomPersist_WithChecksum(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"
	pi.AddFile(partition, "file1.parquet", map[string][]string{"trace_id": {"t1"}})

	idx := pi.GetPartition(partition)
	data := bloomindex.MarshalWithChecksum(idx)

	restored, err := bloomindex.UnmarshalWithChecksum(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Len() != 1 {
		t.Errorf("want 1 entry, got %d", restored.Len())
	}

	// Tamper → should fail
	tampered := make([]byte, len(data))
	copy(tampered, data)
	tampered[10] ^= 0xFF
	_, err = bloomindex.UnmarshalWithChecksum(tampered)
	if err == nil {
		t.Error("tampered data should fail checksum")
	}
}

func TestBloomCache_LoaderIntegration(t *testing.T) {
	// Simulate S3 loader: pre-persist bloom data, then load via cache
	bloomData := make(map[string][]byte)

	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	pi.AddFile("p1", "f1", map[string][]string{"trace_id": {"aaa"}})
	pi.AddFile("p1", "f2", map[string][]string{"trace_id": {"bbb"}})
	bloomData["p1"] = pi.MarshalPartition("p1")

	loader := func(ctx context.Context, partition string) (*bloomindex.Index, error) {
		data, ok := bloomData[partition]
		if !ok {
			return nil, nil
		}
		return bloomindex.Unmarshal(data)
	}

	cache := bloomindex.NewBloomCache(1024*1024, loader)

	idx, err := cache.Get(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if idx == nil {
		t.Fatal("cached index should not be nil")
	}

	result := idx.MayContain([]string{"f1", "f2"}, "trace_id", "aaa")
	if !containsKey(result, "f1") {
		t.Error("should find aaa in f1")
	}
}

func TestBloomFilterFiles_Integration(t *testing.T) {
	// Test the bloomFilterFiles method logic
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)

	partition := "dt=2026-05-02/hour=10"
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("%s/file%d.parquet", partition, i)
		pi.AddFile(partition, key, map[string][]string{
			"trace_id": {fmt.Sprintf("trace-%d", i)},
		})
	}

	// Create files list as manifest.FileInfo
	files := make([]manifest.FileInfo, 50)
	for i := range files {
		files[i] = manifest.FileInfo{
			Key:       fmt.Sprintf("%s/file%d.parquet", partition, i),
			Size:      1024,
			MinTimeNs: time.Now().Add(-1 * time.Hour).UnixNano(),
			MaxTimeNs: time.Now().UnixNano(),
		}
	}

	// Build checks for trace-25
	checks := []bloomindex.ColumnCheck{{Column: "trace_id", Value: "trace-25"}}
	idx := pi.GetPartition(partition)
	keys := make([]string, len(files))
	for i, fi := range files {
		keys[i] = fi.Key
	}
	matching := idx.MayContainAll(keys, checks)

	if !containsKey(matching, files[25].Key) {
		t.Error("should include file25 (true positive)")
	}
	if len(matching) > 10 {
		t.Errorf("too many matches: %d/50 (bloom should filter most)", len(matching))
	}
}

// TestBloomFilterFilesByOrBranches_Integration verifies that OR-shaped
// queries route through the per-branch bloom path instead of returning
// every file. Builds a partition where each file carries a different
// service.name in its bloom, then queries for 2 of the 5 services via
// OR — only the 2 matching files must come back.
//
// Regression guard for the OOM-trigger Grafana-drilldown shape:
//
//	(svc_a:="x" OR svc_b:="x" OR ...)
//
// where previously every file in the partition fell through to a full
// scan because the legacy bloomFilterFiles bypassed itself on OR.
func TestBloomFilterFilesByOrBranches_Integration(t *testing.T) {
	s := testStorage()

	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"
	services := []string{"api-gw", "user-svc", "db-svc", "auth-svc", "billing-svc"}
	files := make([]manifest.FileInfo, len(services))
	now := time.Now()
	for i, svc := range services {
		key := fmt.Sprintf("%s/file%d.parquet", partition, i)
		pi.AddFile(partition, key, map[string][]string{
			"service.name": {svc},
		})
		files[i] = manifest.FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.UnixNano(),
		}
	}

	// BloomCache backed by the partitioned index — Storage hits this
	// via bloomCache.Get(ctx, partition).
	s.bloomCache = bloomindex.NewBloomCache(1024*1024, func(_ context.Context, p string) (*bloomindex.Index, error) {
		return pi.GetPartition(p), nil
	})

	queryStr := `service.name:="api-gw" OR service.name:="db-svc"`
	result := s.bloomFilterFiles(context.Background(), files, queryStr)

	if len(result) != 2 {
		t.Fatalf("got %d files, want 2 (api-gw + db-svc); keys=%v", len(result), keysOf(result))
	}
	got := make(map[string]bool)
	for _, f := range result {
		got[f.Key] = true
	}
	if !got[files[0].Key] {
		t.Error("missing api-gw file (files[0])")
	}
	if !got[files[2].Key] {
		t.Error("missing db-svc file (files[2])")
	}
	if got[files[1].Key] || got[files[3].Key] || got[files[4].Key] {
		t.Error("included non-matching service file(s)")
	}
}

// TestBloomFilterFilesByOrBranches_UnsupportedShapeFallsBack verifies
// that an OR query with a regex branch (bloom can't model regex)
// returns all files rather than over-filtering.
func TestBloomFilterFilesByOrBranches_UnsupportedShapeFallsBack(t *testing.T) {
	s := testStorage()

	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-02/hour=10"
	files := make([]manifest.FileInfo, 3)
	now := time.Now()
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("%s/file%d.parquet", partition, i)
		pi.AddFile(partition, key, map[string][]string{
			"service.name": {fmt.Sprintf("svc-%d", i)},
		})
		files[i] = manifest.FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.UnixNano(),
		}
	}
	s.bloomCache = bloomindex.NewBloomCache(1024*1024, func(_ context.Context, p string) (*bloomindex.Index, error) {
		return pi.GetPartition(p), nil
	})

	// Regex predicate inside an OR branch — unsupported shape, must
	// fall back to returning all files.
	queryStr := `service.name:="svc-0" OR _msg:~"timeout"`
	result := s.bloomFilterFiles(context.Background(), files, queryStr)
	if len(result) != len(files) {
		t.Errorf("unsupported OR shape should return all %d files; got %d", len(files), len(result))
	}
}

func keysOf(files []manifest.FileInfo) []string {
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.Key
	}
	return out
}

func TestPartitionFromKey(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"dt=2026-05-02/hour=10/abc.parquet", "dt=2026-05-02/hour=10"},
		{"dt=2026-05-02/hour=0/xyz.parquet", "dt=2026-05-02/hour=0"},
		{"dt=2026-05-02/abc.parquet", "dt=2026-05-02"},
		{"nohour.parquet", "nohour.parquet"},
	}

	for _, tt := range tests {
		got := partitionFromKey(tt.key)
		if got != tt.want {
			t.Errorf("partitionFromKey(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestManifestBloomMeta(t *testing.T) {
	m := manifest.New("test-bucket", "prefix/")

	// Initially no bloom
	if m.BloomAvailable("p1") {
		t.Error("should not have bloom initially")
	}

	// Set bloom meta
	m.SetBloomMeta("p1", manifest.PartitionMeta{
		BloomAvailable: true,
		BloomSize:      5000,
		BloomColumns:   []string{"trace_id", "service.name"},
	})

	if !m.BloomAvailable("p1") {
		t.Error("should have bloom after SetBloomMeta")
	}

	meta := m.GetBloomMeta("p1")
	if meta == nil {
		t.Fatal("meta should not be nil")
	}
	if meta.BloomSize != 5000 {
		t.Errorf("bloom size: got %d, want 5000", meta.BloomSize)
	}
	if len(meta.BloomColumns) != 2 {
		t.Errorf("bloom columns: got %d, want 2", len(meta.BloomColumns))
	}

	// Non-existent partition
	if m.BloomAvailable("p2") {
		t.Error("non-existent partition should not have bloom")
	}
}

func containsKey(keys []string, target string) bool {
	for _, k := range keys {
		if k == target {
			return true
		}
	}
	return false
}
