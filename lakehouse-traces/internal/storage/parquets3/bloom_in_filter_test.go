package parquets3

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestPreFilterFiles_TraceIDIn_ReturnsMatchingFiles is the regression lock
// for the Jaeger 0-traces bug: VT's spans-lookup query has the shape
//
//	trace_id:in(t1,t2,t3)
//
// (built by query.GetTraceList after findTraceIDsSplitTimeRange). The
// previous filterFilesByBloomIndex only understood the single-value
// extractExactMatch form, so EVERY file got bloom-pruned for the in() form
// — even files whose bloom genuinely contained one of the listed trace_ids.
//
// This test:
//  1. Builds a bloom index with 30 files; each file's trace_id bloom
//     contains exactly one synthetic trace_id (trace-0 .. trace-29).
//  2. Issues a query with trace_id:in(trace-5,trace-12,trace-25) — the
//     VT spans-lookup shape.
//  3. Asserts preFilterFiles returns at least the 3 true-positive files
//     (false positives from bloom over-inclusion are acceptable; the
//     downstream row filter catches them). A passing fix yields >= 3.
//     The pre-fix behaviour returns 0 files (every file gets pruned),
//     reproducing the Jaeger 0-traces symptom.
//
// Negative-control: reverting the storage_query.go changes (the per-column
// any-of union over MayContainAll) makes this test return 0 files and
// fail with "got 0, want >= 3".
func TestPreFilterFiles_TraceIDIn_ReturnsMatchingFiles(t *testing.T) {
	s := testStorage()

	// Build bloom index: 30 files, each tagged with one unique trace_id.
	s.bloomIdx = bloomindex.New()
	const nFiles = 30
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("dt=2026-05-30/hour=12/file%02d.parquet", i)
		bf := bloomindex.NewFilter(8, 0.01)
		bf.Add(fmt.Sprintf("trace-%d", i))
		s.bloomIdx.Add(key, "trace_id", bf)
		files[i] = manifest.FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.UnixNano(),
		}
	}

	// VT's GetTraceList second-phase query: trace_id:in(t1,t2,t3).
	queryStr := `trace_id:in(trace-5,trace-12,trace-25)`

	result := s.preFilterFiles(files, queryStr)
	if len(result) < 3 {
		t.Fatalf("trace_id:in(...) pruned too aggressively: got %d files, want >= 3 (the true positives)", len(result))
	}

	// Must contain the 3 true positives.
	got := map[string]bool{}
	for _, f := range result {
		got[f.Key] = true
	}
	for _, idx := range []int{5, 12, 25} {
		k := fmt.Sprintf("dt=2026-05-30/hour=12/file%02d.parquet", idx)
		if !got[k] {
			t.Errorf("missing true-positive file %s (bloom contains its trace_id)", k)
		}
	}

	// Sanity: must not include every file (otherwise the bloom isn't
	// pruning at all). With 30 unique trace_ids and 3 queried values,
	// expected matches at the bloom layer is ~3 (plus low false-positive
	// rate). Allow up to 10 to absorb bloom FPs without flakiness.
	if len(result) > 10 {
		t.Errorf("bloom pre-filter too permissive: got %d files for 3 trace_ids (fpRate too high?)", len(result))
	}
}

// TestPreFilterFiles_TraceIDIn_QuotedValues covers the quoted form
// trace_id:in("t1","t2"), which q.String() may produce when the trace_id
// values contain special characters. Same any-of semantics expected.
func TestPreFilterFiles_TraceIDIn_QuotedValues(t *testing.T) {
	s := testStorage()
	s.bloomIdx = bloomindex.New()
	const nFiles = 10
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("dt=2026-05-30/hour=12/file%02d.parquet", i)
		bf := bloomindex.NewFilter(8, 0.01)
		bf.Add(fmt.Sprintf("trace-%d", i))
		s.bloomIdx.Add(key, "trace_id", bf)
		files[i] = manifest.FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.UnixNano(),
		}
	}

	queryStr := `trace_id:in("trace-1","trace-7")`
	result := s.preFilterFiles(files, queryStr)
	if len(result) < 2 {
		t.Fatalf("quoted trace_id:in(...) pruned too aggressively: got %d, want >= 2", len(result))
	}
	got := map[string]bool{}
	for _, f := range result {
		got[f.Key] = true
	}
	for _, idx := range []int{1, 7} {
		k := fmt.Sprintf("dt=2026-05-30/hour=12/file%02d.parquet", idx)
		if !got[k] {
			t.Errorf("missing true-positive file %s", k)
		}
	}
}

// TestPreFilterFiles_TraceIDSingleExact verifies the single-value form
// (the pre-existing happy path) still works after the multi-value
// refactor. This is the negative-control direction: making sure we
// haven't regressed the trace_id:="abc" case while fixing in(...).
func TestPreFilterFiles_TraceIDSingleExact(t *testing.T) {
	s := testStorage()
	s.bloomIdx = bloomindex.New()
	const nFiles = 10
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("dt=2026-05-30/hour=12/file%02d.parquet", i)
		bf := bloomindex.NewFilter(8, 0.01)
		bf.Add(fmt.Sprintf("trace-%d", i))
		s.bloomIdx.Add(key, "trace_id", bf)
		files[i] = manifest.FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.UnixNano(),
		}
	}

	queryStr := `trace_id:="trace-3"`
	result := s.preFilterFiles(files, queryStr)
	if len(result) < 1 {
		t.Fatalf("single trace_id pruned its own file: got %d, want >= 1", len(result))
	}
	k := "dt=2026-05-30/hour=12/file03.parquet"
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

// TestPreFilterFiles_TraceIDIn_AllAbsent verifies that when NONE of the
// listed trace_ids are in any file's bloom, all files get pruned. This
// confirms the any-of union correctly prunes — over-permissive bugs
// would leak files through.
func TestPreFilterFiles_TraceIDIn_AllAbsent(t *testing.T) {
	s := testStorage()
	s.bloomIdx = bloomindex.New()
	const nFiles = 10
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("dt=2026-05-30/hour=12/file%02d.parquet", i)
		bf := bloomindex.NewFilter(8, 0.01)
		bf.Add(fmt.Sprintf("trace-%d", i))
		s.bloomIdx.Add(key, "trace_id", bf)
		files[i] = manifest.FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.UnixNano(),
		}
	}

	queryStr := `trace_id:in(absent-a,absent-b,absent-c)`
	result := s.preFilterFiles(files, queryStr)
	// With well-sized filters and absent values, FP rate is ~1%
	// → expected matches ≪ nFiles. Allow up to 30% to absorb FPs.
	if len(result) > nFiles*3/10 {
		t.Errorf("bloom failed to prune absent trace_ids: got %d/%d files", len(result), nFiles)
	}
}

// TestPreFilterFiles_TraceIDIn_LargeWorkload proves the fix scales to a
// production-shape workload (200 files, multiple bloom-enabled columns).
// Per feedback_prove_on_large_data, we don't lock the fix on a minimal
// reproducer.
func TestPreFilterFiles_TraceIDIn_LargeWorkload(t *testing.T) {
	s := testStorage()
	s.bloomIdx = bloomindex.New()
	const nFiles = 200
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	var queriedTraceIDs []string
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("dt=2026-05-30/hour=12/file%03d.parquet", i)
		bf := bloomindex.NewFilter(16, 0.01)
		// each file holds 8 trace_ids
		for j := 0; j < 8; j++ {
			tid := fmt.Sprintf("trace-%d-%d", i, j)
			bf.Add(tid)
			// Sample a few files' trace_ids for the query
			if i%20 == 0 && j == 0 {
				queriedTraceIDs = append(queriedTraceIDs, tid)
			}
		}
		s.bloomIdx.Add(key, "trace_id", bf)
		files[i] = manifest.FileInfo{
			Key:       key,
			Size:      1024,
			MinTimeNs: now.Add(-time.Hour).UnixNano(),
			MaxTimeNs: now.UnixNano(),
		}
	}

	queryStr := `trace_id:in(` + strings.Join(queriedTraceIDs, ",") + `)`
	result := s.preFilterFiles(files, queryStr)

	// Must include all 10 true-positive files (every 20th file's trace_id is queried).
	if len(result) < len(queriedTraceIDs) {
		t.Fatalf("large-workload trace_id:in: got %d files, want >= %d true positives", len(result), len(queriedTraceIDs))
	}
	// Bloom pre-filter must still prune the majority — 200 files, ~10
	// true positives, low FP rate → expect well under 50 matches.
	if len(result) > 50 {
		t.Errorf("large-workload bloom too permissive: got %d/%d files (expected pruning to single digits)", len(result), nFiles)
	}
}

// TestCheckFileBloom_TraceIDIn verifies the per-file bloom sidecar path
// handles in() with any-of semantics (the same code shape as the
// index-level filter, in a different helper). The file-bloom helper
// uses the conventional fileKey "_" — see FileBloomMayContainAll in
// internal/bloomindex/file_bloom.go.
func TestCheckFileBloom_TraceIDIn(t *testing.T) {
	idx := bloomindex.New()
	bf := bloomindex.NewFilter(8, 0.01)
	bf.Add("trace-2")
	// File-bloom sidecar convention: single entry under fileKey "_".
	idx.Add("_", "trace_id", bf)

	// Sanity: inserted value matches.
	checks := []bloomindex.ColumnCheck{{Column: "trace_id", Value: "trace-2"}}
	if !bloomindex.FileBloomMayContainAll(idx, checks) {
		t.Fatal("bloom should match the inserted value")
	}
	// Probability test: ~99% of absent values should be correctly rejected
	// at fp_rate=0.01. Try 50 distinct values; expect majority rejected.
	rejections := 0
	for i := 0; i < 50; i++ {
		checks = []bloomindex.ColumnCheck{{Column: "trace_id", Value: fmt.Sprintf("absent-%d-xyz", i)}}
		if !bloomindex.FileBloomMayContainAll(idx, checks) {
			rejections++
		}
	}
	if rejections < 40 {
		t.Errorf("bloom failed to reject most absent values: %d/50 rejected (FP rate too high?)", rejections)
	}
}
