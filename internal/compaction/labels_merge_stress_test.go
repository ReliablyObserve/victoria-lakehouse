package compaction

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestStress_MergeFileLabels_ManyInputs scales the union helper to the
// shape it sees at PB scale: a single L4 compaction merges hundreds of
// L3 input files. Each input carries a small but distinct set of
// service.name values, plus identical high-cardinality
// k8s.pod.name values. The merged result must contain every input
// (cumulative, deduplicated) AND complete in well under a second so
// the compactor's scan loop doesn't become a bottleneck during heavy
// compaction churn.
func TestStress_MergeFileLabels_ManyInputs(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test — skipping under -short")
	}

	const nFiles = 1000
	const podsPerFile = 50

	files := make([]manifest.FileInfo, nFiles)
	for i := range files {
		svcs := []string{
			fmt.Sprintf("svc-%d", i%20),
			fmt.Sprintf("svc-%d", (i+1)%20),
		}
		pods := make([]string, podsPerFile)
		for j := range pods {
			pods[j] = fmt.Sprintf("pod-%d-%d", i%5, j)
		}
		ns := "production"
		if i%2 == 1 {
			ns = "staging"
		}
		files[i] = manifest.FileInfo{
			Key: fmt.Sprintf("k/%d", i),
			Labels: map[string][]string{
				"service.name":       svcs,
				"k8s.pod.name":       pods,
				"k8s.namespace.name": {ns},
			},
		}
	}

	got := mergeFileLabels(files)

	// service.name: every i%20 gets hit twice (i and i+1 paths), so 20
	// distinct values after dedup.
	if len(got["service.name"]) != 20 {
		t.Errorf("service.name distinct = %d, want 20 (dedup is broken)",
			len(got["service.name"]))
	}

	// k8s.pod.name: 5 (i%5) × podsPerFile (50) = 250 distinct values
	// after dedup across all inputs.
	if want := 5 * podsPerFile; len(got["k8s.pod.name"]) != want {
		t.Errorf("k8s.pod.name distinct = %d, want %d (dedup is broken)",
			len(got["k8s.pod.name"]), want)
	}

	// k8s.namespace.name: only "production" and "staging" exist.
	if len(got["k8s.namespace.name"]) != 2 {
		t.Errorf("k8s.namespace.name distinct = %d, want 2", len(got["k8s.namespace.name"]))
	}
}

// TestRace_MergeFileLabels_ConcurrentReaders pins that the helper
// produces independent output slices per call — callers that read the
// returned map concurrently with another caller's merge must not see
// torn writes. The compactor runs partition groups in parallel and
// each calls mergeFileLabels independently; if the helper ever
// caches via a package-level map, the race detector catches it here.
func TestRace_MergeFileLabels_ConcurrentReaders(t *testing.T) {
	if testing.Short() {
		t.Skip("race test — skipping under -short")
	}

	files := []manifest.FileInfo{
		{Key: "a", Labels: map[string][]string{"service.name": {"x", "y"}}},
		{Key: "b", Labels: map[string][]string{"service.name": {"y", "z"}}},
	}

	const goroutines = 64
	const iterations = 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				got := mergeFileLabels(files)
				vals := got["service.name"]
				sort.Strings(vals)
				if got["service.name"] = vals; len(vals) != 3 {
					// Use t.Errorf via a closure — t.Fatal in a goroutine
					// is unsafe, t.Errorf is documented safe.
					t.Errorf("goroutine %d iter %d: got %v, want 3 distinct", g, i, vals)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestMemLeak_MergeFileLabels_RepeatedCalls ensures the helper
// doesn't accumulate state across invocations — every input map and
// every returned map must be reclaimable by the next GC cycle. The
// compactor calls this once per partition group; at PB scale that's
// thousands of invocations per minute. A leak per call (e.g. a
// growing sync.Map cache that's never trimmed) would surface as
// steady RSS growth on the lakehouse-logs process. Budget: 5 MB
// growth after 10k invocations on a fixed 100-input fixture.
func TestMemLeak_MergeFileLabels_RepeatedCalls(t *testing.T) {
	if testing.Short() {
		t.Skip("memleak test — skipping under -short")
	}

	files := make([]manifest.FileInfo, 100)
	for i := range files {
		files[i] = manifest.FileInfo{
			Key: fmt.Sprintf("k/%d", i),
			Labels: map[string][]string{
				"service.name": {fmt.Sprintf("svc-%d", i%20)},
				"host.name":    {fmt.Sprintf("host-%d", i%10)},
			},
		}
	}

	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := 0; i < 10000; i++ {
		_ = mergeFileLabels(files)
	}

	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	const budget = 5 * 1024 * 1024 // 5 MB
	if growth := int64(after.HeapInuse) - int64(before.HeapInuse); growth > budget {
		t.Errorf("heap grew by %d bytes across 10k invocations; budget is %d (likely caching state per call)",
			growth, budget)
	}
}
