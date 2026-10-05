package compaction

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func TestForceCompactPartition_PerTenant(t *testing.T) {
	// Guards the per-tenant split of the manual trigger: the tenant with two
	// files is merged, the tenant with one is left alone. On the old code the
	// partition-wide selection rewrote the lone tenant's file 1 -> 1.
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-5 * time.Hour))
		w.add("1001/0", p, 0, 2, 3, nil)
		lone := w.add("1002/0", p, 0, 1, 3, nil)
		sched := w.shippedScheduler()
		rows := w.rows()
		res, err := sched.ForceCompactPartition(context.Background(), p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.InputFiles) != 2 || len(res.OutputFiles) != 1 || res.RowsMerged != 6 {
			t.Fatalf("result: inputs=%d outputs=%d rows=%d", len(res.InputFiles), len(res.OutputFiles), res.RowsMerged)
		}
		live := w.keys(p)
		if len(live) != 2 {
			t.Fatalf("live files %v, want the merged output plus the lone file", live)
		}
		found := false
		for _, k := range live {
			if k == lone[0] {
				found = true
			}
		}
		if !found {
			t.Fatalf("lone-file tenant was rewritten: %v", live)
		}
		if w.rows() != rows {
			t.Fatalf("rows %d -> %d", rows, w.rows())
		}
	})
}

func TestForceCompactPartition_AllLoneIsAnError(t *testing.T) {
	// Guards merged == 0: nothing to merge must not report success or write.
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-5 * time.Hour))
		w.add("1001/0", p, 0, 1, 3, nil)
		w.add("1002/0", p, 1, 1, 3, nil)
		before := w.keys(p)
		_, err := w.shippedScheduler().ForceCompactPartition(context.Background(), p, 0)
		if err == nil || !strings.Contains(err.Error(), "fewer than 2") {
			t.Fatalf("err = %v, want one naming 'fewer than 2'", err)
		}
		if got := w.keys(p); strings.Join(got, ",") != strings.Join(before, ",") {
			t.Fatalf("objects changed: %v -> %v", before, got)
		}
		if _, err := w.shippedScheduler().ForceCompactPartition(context.Background(), "dt=2020-01-01/hour=00", 0); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("missing partition: %v", err)
		}
	})
}

func TestForceCompactPartition_RespectsFreeze(t *testing.T) {
	// Guards s.freeze.frozen in the manual path: tiered files are excluded,
	// so a tenant whose only pair is tiered is not merged, and a tenant with
	// two live files plus tiered ones merges just the live two.
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-5 * time.Hour))
		tiered := w.add("1001/0", p, 0, 2, 2, func(fi *manifest.FileInfo) { fi.StorageClass = "STANDARD_IA" })
		w.add("1002/0", p, 0, 2, 2, nil)
		tiered = append(tiered, w.add("1002/0", p, 0, 2, 2, func(fi *manifest.FileInfo) { fi.StorageClass = "DEEP_ARCHIVE" })...)
		sched := w.shippedScheduler()
		res, err := sched.ForceCompactPartition(context.Background(), p, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.InputFiles) != 2 {
			t.Fatalf("inputs %v, want only 1002/0's two live files", res.InputFiles)
		}
		live := map[string]bool{}
		for _, k := range w.keys(p) {
			live[k] = true
		}
		for _, k := range tiered {
			if !live[k] {
				t.Fatalf("tiered object %s was rewritten", k)
			}
		}
		// Now only tiered pairs and one merged file: nothing left to do.
		if _, err := sched.ForceCompactPartition(context.Background(), p, 0); err == nil {
			t.Fatal("second force over tiered files must fail with nothing to merge")
		}
	})
}

func TestRecompactHandler_PerTenant(t *testing.T) {
	// Guards the HTTP trigger end to end on top of the per-tenant force: 200
	// with the merge counts for a mergeable tenant, 400 when every tenant is a
	// single file.
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-5 * time.Hour))
		w.add("1001/0", p, 0, 3, 2, nil)
		w.add("1002/0", p, 0, 1, 2, nil)
		h := RecompactHandler(w.shippedScheduler())
		body, _ := json.Marshal(RecompactRequest{Partition: p})
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodPost, "/lakehouse/compaction/recompact", bytes.NewReader(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		var out map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		if out["input_files"].(float64) != 3 || out["output_files"].(float64) != 1 || out["rows_merged"].(float64) != 6 {
			t.Fatalf("response %v", out)
		}
		rec = httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodPost, "/lakehouse/compaction/recompact", bytes.NewReader(body)))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("second call status %d, want 400 (one file per tenant): %s", rec.Code, rec.Body.String())
		}
	})
}

type compactedEvent struct {
	added   []manifest.FileInfo
	removed []string
}

// TestScan_OnCompactedGetsOnlyOutputs: the pmeta feed and the peer push must
// hear about exactly the merge's outputs as added and exactly its inputs as
// removed. The old code re-announced every file of the partition on each
// merge: O(tenants) per merge, O(tenants^2) per hour.
func TestScan_OnCompactedGetsOnlyOutputs(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		in1 := w.add("1001/0", p, 0, 10, 2, nil)
		in2 := w.add("1002/0", p, 0, 10, 2, nil)
		bystander := w.add("1003/0", p, 1, 1, 2, nil)
		var mu sync.Mutex
		var events []compactedEvent
		sched := w.shippedScheduler()
		sched.onCompacted = func(added []manifest.FileInfo, removed []string, _ map[string]map[string][]string) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, compactedEvent{added, removed})
		}
		n, err := sched.Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 || len(events) != 2 {
			t.Fatalf("compactions=%d events=%d, want 2/2 (both tenants merge in one scan)", n, len(events))
		}
		want := map[string][]string{"1001/0/": in1, "1002/0/": in2}
		for _, ev := range events {
			if len(ev.added) != 1 {
				t.Fatalf("added %d entries, want exactly the output", len(ev.added))
			}
			out := ev.added[0]
			if !strings.Contains(out.Key, "compacted-L1-") || out.RowCount != 20 {
				t.Fatalf("added entry is not the merged output: %+v", out)
			}
			var inputs []string
			for prefix, keys := range want {
				if strings.HasPrefix(out.Key, prefix) {
					inputs = keys
				}
			}
			a, b := append([]string(nil), ev.removed...), append([]string(nil), inputs...)
			sort.Strings(a)
			sort.Strings(b)
			if strings.Join(a, ",") != strings.Join(b, ",") {
				t.Fatalf("removed %v, want exactly the merged inputs %v", a, b)
			}
			for _, k := range bystander {
				if k == out.Key {
					t.Fatal("bystander announced")
				}
			}
		}
	})
}

// tierAWorld builds a sweep that is the secondary owner of a partition whose
// primary attempted long ago, over the world's manifest and pool.
func tierAWorld(w *planWorld, freeze *LifecycleFreeze, onCompacted func([]manifest.FileInfo, []string, map[string]map[string][]string)) *OrphanSweep {
	lp := &listingPool{mockPool: w.pool, mtimes: map[string]time.Time{}}
	ranker := func(s string) uint64 {
		if strings.HasPrefix(s, "pod-A") {
			return 100
		}
		return 50
	}
	rB := NewOwnershipResolver("pod-B", staticPeers("pod-A", "pod-B")).WithHashFunc(ranker)
	return NewOrphanSweep(OrphanSweepConfig{
		Manifest: w.m, Pool: lp, Ownership: rB, Policy: shippedPolicy(), Lister: lp, Freeze: freeze,
		Prefix: string(w.mode) + "/", Mode: w.mode, Interval: time.Minute,
		RowGroupSize: 1000, CompressionLevel: 1, TierAStalenessMultiplier: 3, OnCompacted: onCompacted,
	})
}

func TestTierA_PerTenantPlanning(t *testing.T) {
	// Guards Tier A using the per-(tenant, partition) planner. Two tenants with
	// one L1 file each in a 72 h old partition: nothing to steal (the old
	// partition-wide count made the rollup eligible and stole it, rewriting
	// each lone file). One tenant with 12 L0 files: stolen, and only that
	// tenant's files are touched and announced.
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-72 * time.Hour))
		w.add("1001/0", p, 1, 1, 2, nil)
		w.add("1002/0", p, 1, 1, 2, nil)
		w.m.MarkAttempt(p, time.Now().Add(-10*time.Minute))
		before := w.keys(p)
		if got, err := tierAWorld(w, nil, nil).RunTierA(context.Background()); err != nil || got != 0 {
			t.Fatalf("lone files: stolen=%d err=%v, want 0", got, err)
		}
		if strings.Join(w.keys(p), ",") != strings.Join(before, ",") {
			t.Fatalf("objects changed: %v -> %v", before, w.keys(p))
		}

		inputs := w.add("1003/0", p, 0, 12, 2, nil)
		var ev compactedEvent
		got, err := tierAWorld(w, nil, func(a []manifest.FileInfo, r []string, _ map[string]map[string][]string) { ev = compactedEvent{a, r} }).RunTierA(context.Background())
		if err != nil || got != 1 {
			t.Fatalf("12 L0: stolen=%d err=%v, want 1", got, err)
		}
		if len(w.keys(p)) != 3 {
			t.Fatalf("keys after steal: %v, want 1001, 1002 untouched and one merged output", w.keys(p))
		}
		if len(ev.added) != 1 || !strings.HasPrefix(ev.added[0].Key, "1003/0/") || len(ev.removed) != len(inputs) {
			t.Fatalf("OnCompacted added=%v removed=%d, want only 1003/0's output and its 12 inputs", ev.added, len(ev.removed))
		}
	})
}

func TestTierA_RespectsFreeze(t *testing.T) {
	// Guards o.cfg.Freeze.frozen in Tier A: a steal must not rewrite tiered
	// objects, whether the class is recorded on the entry or implied by a
	// lifecycle rule (a 3-day IA rule freezes a 72 h old partition).
	bothModes(t, func(t *testing.T, mode config.Mode) {
		p := partitionAt(time.Now().Add(-72 * time.Hour))
		for name, tc := range map[string]struct {
			mutate func(*manifest.FileInfo)
			freeze *LifecycleFreeze
		}{
			"recorded class": {func(fi *manifest.FileInfo) { fi.StorageClass = "GLACIER" }, nil},
			"lifecycle rule": {nil, &LifecycleFreeze{ExtraRules: rules(3, delete.ClassStandardIA)}},
		} {
			w := newPlanWorld(t, mode)
			w.add("1001/0", p, 0, 12, 2, tc.mutate)
			w.m.MarkAttempt(p, time.Now().Add(-10*time.Minute))
			before := w.keys(p)
			if got, _ := tierAWorld(w, tc.freeze, nil).RunTierA(context.Background()); got != 0 {
				t.Fatalf("%s: tiered files stolen: %d", name, got)
			}
			if strings.Join(w.keys(p), ",") != strings.Join(before, ",") {
				t.Fatalf("%s: tiered objects rewritten", name)
			}
			// Control: the same files without the freeze are stolen.
			ctl := newPlanWorld(t, mode)
			ctl.add("1001/0", p, 0, 12, 2, nil)
			ctl.m.MarkAttempt(p, time.Now().Add(-10*time.Minute))
			if got, _ := tierAWorld(ctl, nil, nil).RunTierA(context.Background()); got != 1 {
				t.Fatalf("%s: control not stolen: %d", name, got)
			}
		}
	})
}

// TestScan_HeldFilesDoNotCountTowardThresholds guards the planner's held-key
// exclusion: a file a delete rewrite has swapped in but not yet recorded is
// left out BEFORE counting, so ten L0 files with one held are nine and wait;
// the held file is merged once it is released. Without the exclusion the plan
// reaches the threshold and merges nine files that did not.
func TestScan_HeldFilesDoNotCountTowardThresholds(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		keys := w.add("1001/0", p, 0, 10, 2, nil)
		w.m.Hold(keys[0])
		s := w.shippedScheduler()
		if n, err := s.Scan(context.Background()); err != nil || n != 0 {
			t.Fatalf("nine live L0 files: merges=%d err=%v, want 0", n, err)
		}
		w.m.Release(keys[0])
		if n, err := s.Scan(context.Background()); err != nil || n != 1 {
			t.Fatalf("after release: merges=%d err=%v, want 1", n, err)
		}
	})
}
