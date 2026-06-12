package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// newRecompactSchedulerOwned is newRecompactScheduler with a caller-supplied
// ownership resolver, so the trigger tests can exercise the not-owner (403) gate.
func newRecompactSchedulerOwned(m *manifest.Manifest, pool *mockPool, currentFP string, own *OwnershipResolver) *Scheduler {
	return NewScheduler(SchedulerConfig{
		Manifest:                 m,
		Pool:                     pool,
		Ownership:                own,
		Policy:                   NewLevelPolicy(10, 10, 0), // L0/L1 thresholds unreachable here
		Prefix:                   "logs/",
		Mode:                     config.ModeLogs,
		Interval:                 time.Minute,
		RowGroupSize:             1000,
		CompressionLevel:         1,
		MaxConcurrent:            4,
		CurrentSchemaFingerprint: currentFP,
	})
}

// TestForceCompactPartition_E2E_FragmentedClears is the manual-trigger end-to-end
// proof tied to the hint system. Two CURRENT-fingerprint L2 files are fragmented
// (two files stuck at the top level) — the level policy never re-picks them, so
// ComputeCompactionStats lists the partition as exactly one "fragmented" candidate.
// ForceCompactPartition (level 0 → derived from the hint) merges them into one L3
// file through the REAL Compactor.Compact path, the inputs vanish from manifest and
// pool, rows are conserved, and the candidate clears (1 → 0) — proving the trigger
// resolves the very work the hints surface.
func TestForceCompactPartition_E2E_FragmentedClears(t *testing.T) {
	const partition = "dt=2026-06-03/hour=00"
	m := manifest.New("test", "logs/")
	pool := newMockPool()
	// Current fingerprint (v2) → not stale; two files at L2 → fragmented.
	inputKeys := seedL2(t, m, pool, partition, "v2", 2)

	// Precondition: the hint system sees exactly one fragmented candidate here.
	before := m.ComputeCompactionStats("v2", nil)
	if len(before.Candidates) != 1 {
		t.Fatalf("candidates before = %d, want 1 (fragmented)", len(before.Candidates))
	}
	if before.Candidates[0].Partition != partition {
		t.Fatalf("candidate partition = %q, want %q", before.Candidates[0].Partition, partition)
	}
	if before.FragmentedPartitions != 1 {
		t.Errorf("fragmented_partitions = %d, want 1", before.FragmentedPartitions)
	}

	// Precondition: the level policy ALONE must not pick this partition, so the only
	// thing that can compact it is the forced trigger.
	pt, err := manifest.ParsePartitionTime(partition)
	if err != nil {
		t.Fatalf("parse partition time: %v", err)
	}
	if _, eligible := NewLevelPolicy(10, 10, 0).Eligible(m.FilesForPartition(partition), pt); eligible {
		t.Fatal("precondition failed: level policy considered the partition eligible")
	}

	sched := newRecompactScheduler(m, pool, "v2")

	result, err := sched.ForceCompactPartition(context.Background(), partition, 0)
	if err != nil {
		t.Fatalf("ForceCompactPartition: %v", err)
	}
	if result.OutputLevel != 3 {
		t.Errorf("output level = L%d, want L3", result.OutputLevel)
	}
	if result.RowsMerged != 2 {
		t.Errorf("rows merged = %d, want 2 (rows conserved)", result.RowsMerged)
	}
	if len(result.InputFiles) != 2 {
		t.Errorf("input files = %d, want 2", len(result.InputFiles))
	}

	// Manifest: exactly one file now, at L3.
	files := m.FilesForPartition(partition)
	if len(files) != 1 {
		t.Fatalf("files after = %d, want 1", len(files))
	}
	if files[0].CompactionLevel != 3 {
		t.Errorf("output level = L%d, want L3", files[0].CompactionLevel)
	}

	// Inputs gone from BOTH manifest and pool.
	for _, k := range inputKeys {
		for _, f := range files {
			if f.Key == k {
				t.Errorf("input %q still in manifest", k)
			}
		}
		if data, _ := pool.Download(context.Background(), k); data != nil {
			t.Errorf("input %q still in pool", k)
		}
	}

	// The candidate must clear: a lone current-fingerprint top-level file is neither
	// stale nor fragmented, so the hint list is now empty.
	after := m.ComputeCompactionStats("v2", nil)
	if len(after.Candidates) != 0 {
		t.Errorf("candidates after = %d, want 0 (trigger resolved the fragmentation)", len(after.Candidates))
	}
	if after.FragmentedPartitions != 0 {
		t.Errorf("fragmented_partitions after = %d, want 0", after.FragmentedPartitions)
	}
}

// TestForceCompactPartition_Errors covers the trigger's guard rails: an unknown
// partition, a partition with a single (non-compactable) file, and a draining
// scheduler must all return errors and leave the manifest untouched — no panic, no
// partial rewrite.
func TestForceCompactPartition_Errors(t *testing.T) {
	t.Run("unknown partition", func(t *testing.T) {
		m := manifest.New("test", "logs/")
		sched := newRecompactScheduler(m, newMockPool(), "v2")
		if _, err := sched.ForceCompactPartition(context.Background(), "dt=2099-01-01/hour=00", 0); err == nil {
			t.Fatal("want error for unknown partition, got nil")
		}
	})

	t.Run("fewer than two files", func(t *testing.T) {
		const partition = "dt=2026-06-04/hour=00"
		m := manifest.New("test", "logs/")
		pool := newMockPool()
		seedL2(t, m, pool, partition, "v1", 1) // single file — nothing to merge
		sched := newRecompactScheduler(m, pool, "v2")
		if _, err := sched.ForceCompactPartition(context.Background(), partition, 0); err == nil {
			t.Fatal("want error for single-file partition, got nil")
		}
		// The one file must be left untouched.
		if got := len(m.FilesForPartition(partition)); got != 1 {
			t.Errorf("files = %d, want 1 (untouched)", got)
		}
	})

	t.Run("draining", func(t *testing.T) {
		const partition = "dt=2026-06-05/hour=00"
		m := manifest.New("test", "logs/")
		pool := newMockPool()
		seedL2(t, m, pool, partition, "v1", 2)
		sched := newRecompactScheduler(m, pool, "v2")
		sched.draining.Store(true)
		if _, err := sched.ForceCompactPartition(context.Background(), partition, 0); err == nil {
			t.Fatal("want error while draining, got nil")
		}
		// Nothing compacted while draining.
		if got := len(m.FilesForPartition(partition)); got != 2 {
			t.Errorf("files = %d, want 2 (no compaction while draining)", got)
		}
	})
}

// TestRecompactHandler exercises the HTTP contract of POST /lakehouse/compaction/
// recompact: method guard (405), disabled compaction (503), malformed/missing body
// (400), the HRW ownership gate (403), and a successful forced recompaction (200
// with the result JSON). The ownership and success cases run the real scheduler so
// the gate and the compaction path are both genuinely exercised.
func TestRecompactHandler(t *testing.T) {
	post := func(h http.HandlerFunc, body string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		h(rr, httptest.NewRequest(http.MethodPost, "/lakehouse/compaction/recompact", strings.NewReader(body)))
		return rr
	}

	t.Run("wrong method", func(t *testing.T) {
		sched := newRecompactScheduler(manifest.New("test", "logs/"), newMockPool(), "v2")
		rr := httptest.NewRecorder()
		RecompactHandler(sched)(rr, httptest.NewRequest(http.MethodGet, "/lakehouse/compaction/recompact", nil))
		if rr.Code != http.StatusMethodNotAllowed {
			t.Errorf("GET status = %d, want 405", rr.Code)
		}
	})

	t.Run("compaction disabled", func(t *testing.T) {
		rr := post(RecompactHandler(nil), `{"partition":"dt=2026-06-03/hour=00"}`)
		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("nil-scheduler status = %d, want 503", rr.Code)
		}
	})

	t.Run("malformed json", func(t *testing.T) {
		sched := newRecompactScheduler(manifest.New("test", "logs/"), newMockPool(), "v2")
		rr := post(RecompactHandler(sched), `{not json`)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("bad-json status = %d, want 400", rr.Code)
		}
	})

	t.Run("missing partition", func(t *testing.T) {
		sched := newRecompactScheduler(manifest.New("test", "logs/"), newMockPool(), "v2")
		rr := post(RecompactHandler(sched), `{"level":2}`)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("missing-partition status = %d, want 400", rr.Code)
		}
	})

	t.Run("not owner", func(t *testing.T) {
		// A two-peer ring; find a partition this pod does NOT own, then assert the
		// trigger refuses it with 403 (so two pods never both rewrite a partition).
		own := NewOwnershipResolver("self", staticPeers("self", "peer-b"))
		var notOwned string
		for i := 0; i < 4000 && notOwned == ""; i++ {
			p := fmt.Sprintf("dt=2026-06-01/hour=%02d/p=%d", i%24, i)
			if !own.OwnsPartition(p) {
				notOwned = p
			}
		}
		if notOwned == "" {
			t.Skip("could not find a non-owned partition in the test ring")
		}
		sched := newRecompactSchedulerOwned(manifest.New("test", "logs/"), newMockPool(), "v2", own)
		rr := post(RecompactHandler(sched), fmt.Sprintf(`{"partition":%q}`, notOwned))
		if rr.Code != http.StatusForbidden {
			t.Errorf("not-owner status = %d, want 403", rr.Code)
		}
	})

	t.Run("success", func(t *testing.T) {
		const partition = "dt=2026-06-06/hour=00"
		m := manifest.New("test", "logs/")
		pool := newMockPool()
		seedL2(t, m, pool, partition, "v2", 2) // fragmented, owned by the single pod
		sched := newRecompactScheduler(m, pool, "v2")

		rr := post(RecompactHandler(sched), fmt.Sprintf(`{"partition":%q}`, partition))
		if rr.Code != http.StatusOK {
			t.Fatalf("success status = %d, want 200 (body=%s)", rr.Code, rr.Body.String())
		}
		var got struct {
			Partition   string `json:"partition"`
			OutputLevel int    `json:"output_level"`
			InputFiles  int    `json:"input_files"`
			RowsMerged  int    `json:"rows_merged"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if got.Partition != partition {
			t.Errorf("partition = %q, want %q", got.Partition, partition)
		}
		if got.OutputLevel != 3 {
			t.Errorf("output_level = %d, want 3", got.OutputLevel)
		}
		if got.InputFiles != 2 {
			t.Errorf("input_files = %d, want 2", got.InputFiles)
		}
		if got.RowsMerged != 2 {
			t.Errorf("rows_merged = %d, want 2", got.RowsMerged)
		}
		// The partition is now a single L3 file — re-triggering must fail (nothing to merge).
		if rr2 := post(RecompactHandler(sched), fmt.Sprintf(`{"partition":%q}`, partition)); rr2.Code != http.StatusBadRequest {
			t.Errorf("re-trigger status = %d, want 400 (single file left)", rr2.Code)
		}
	})
}
