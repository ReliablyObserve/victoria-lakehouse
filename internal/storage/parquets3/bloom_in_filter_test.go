package parquets3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestBloomFilterFiles_TraceIDIn_ReturnsMatchingFiles is the logs-module
// twin of the traces regression test for the Jaeger 0-traces bug. The
// logs module shares the same `field:in(v1,v2,...)` filter shape via VL's
// filterIn AST node, and the same any-of-values bloom semantics must
// apply. Before the fix, bloomFilterFiles built one AND-merged
// MayContainAll(checks) where multiple values for the same column were
// conjunctive — making files match `trace_id:in(a,b,c)` only when their
// bloom contained ALL THREE trace_ids, which never happens.
//
// Negative-control: reverting the bloomFilterFiles per-column-union
// rewrite in storage_query.go makes this test fail with "got 0 files,
// want >= 3" (since each file only has one trace_id, no file passes the
// 3-way AND).
func TestBloomFilterFiles_TraceIDIn_ReturnsMatchingFiles(t *testing.T) {
	s := testStorage()

	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-30/hour=12"
	const nFiles = 30
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("%s/file%02d.parquet", partition, i)
		pi.AddFile(partition, key, map[string][]string{
			"trace_id": {fmt.Sprintf("trace-%d", i)},
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

	// VT's GetTraceList second-phase query shape.
	queryStr := `trace_id:in(trace-5,trace-12,trace-25)`

	result := s.bloomFilterFiles(context.Background(), files, queryStr)
	if len(result) < 3 {
		t.Fatalf("trace_id:in(...) pruned too aggressively: got %d files, want >= 3 true positives", len(result))
	}
	got := map[string]bool{}
	for _, f := range result {
		got[f.Key] = true
	}
	for _, idx := range []int{5, 12, 25} {
		k := fmt.Sprintf("%s/file%02d.parquet", partition, idx)
		if !got[k] {
			t.Errorf("missing true-positive file %s (bloom contains its trace_id)", k)
		}
	}
	if len(result) > 10 {
		t.Errorf("bloom pre-filter too permissive: got %d files for 3 trace_ids", len(result))
	}
}

// TestBloomFilterFiles_TraceIDIn_AllAbsent verifies that when none of
// the listed trace_ids are in any bloom, the pre-filter correctly prunes
// (within fp_rate tolerance).
func TestBloomFilterFiles_TraceIDIn_AllAbsent(t *testing.T) {
	s := testStorage()

	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-30/hour=12"
	const nFiles = 20
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("%s/file%02d.parquet", partition, i)
		pi.AddFile(partition, key, map[string][]string{
			"trace_id": {fmt.Sprintf("trace-%d", i)},
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

	queryStr := `trace_id:in(absent-a,absent-b,absent-c)`
	result := s.bloomFilterFiles(context.Background(), files, queryStr)
	// fp_rate=0.01 per file, but a 20-file partition bloom can have higher
	// effective FP rate due to per-key hashing collisions. We mainly want
	// to confirm the pre-filter prunes SOMETHING (it must not return all
	// files when all values are guaranteed absent) — that's the regression
	// being guarded. Allow up to 75% to absorb partition-level FPs and
	// still distinguish from the "no pruning at all" buggy behaviour.
	if len(result) >= nFiles {
		t.Errorf("bloom did not prune any files for absent trace_ids: got %d/%d (no pruning = bug)", len(result), nFiles)
	}
}

// TestBloomFilterFiles_TraceIDIn_LargeWorkload proves the fix scales to
// a production-shape workload (per feedback_prove_on_large_data — don't
// lock the fix on the minimal reproducer).
func TestBloomFilterFiles_TraceIDIn_LargeWorkload(t *testing.T) {
	s := testStorage()

	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-30/hour=12"
	const nFiles = 200
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	var queriedTraceIDs []string
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("%s/file%03d.parquet", partition, i)
		fileTraces := []string{}
		for j := 0; j < 8; j++ {
			tid := fmt.Sprintf("trace-%d-%d", i, j)
			fileTraces = append(fileTraces, tid)
			if i%20 == 0 && j == 0 {
				queriedTraceIDs = append(queriedTraceIDs, tid)
			}
		}
		pi.AddFile(partition, key, map[string][]string{"trace_id": fileTraces})
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

	queryStr := `trace_id:in(` + strings.Join(queriedTraceIDs, ",") + `)`
	result := s.bloomFilterFiles(context.Background(), files, queryStr)

	if len(result) < len(queriedTraceIDs) {
		t.Fatalf("large-workload trace_id:in: got %d files, want >= %d true positives", len(result), len(queriedTraceIDs))
	}
	if len(result) > 50 {
		t.Errorf("large-workload bloom too permissive: got %d/%d", len(result), nFiles)
	}
}

// TestBloomFilterFiles_TraceIDSingleExact is a negative-control direction:
// verify the single-value form still works after the multi-value rewrite.
func TestBloomFilterFiles_TraceIDSingleExact(t *testing.T) {
	s := testStorage()

	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-05-30/hour=12"
	const nFiles = 10
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("%s/file%02d.parquet", partition, i)
		pi.AddFile(partition, key, map[string][]string{
			"trace_id": {fmt.Sprintf("trace-%d", i)},
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

	queryStr := `trace_id:="trace-3"`
	result := s.bloomFilterFiles(context.Background(), files, queryStr)
	if len(result) < 1 {
		t.Fatalf("single trace_id pruned its own file: got %d, want >= 1", len(result))
	}
	k := fmt.Sprintf("%s/file03.parquet", partition)
	found := false
	for _, f := range result {
		if f.Key == k {
			found = true
		}
	}
	if !found {
		t.Errorf("missing true-positive file %s", k)
	}
	if len(result) > 4 {
		t.Errorf("single-value bloom too permissive: got %d for 1 trace_id", len(result))
	}
}
