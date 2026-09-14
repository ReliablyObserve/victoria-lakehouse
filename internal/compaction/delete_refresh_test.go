package compaction

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil/storageinvariants"
)

// The periodic manifest refresh rebuilds the manifest from the bucket listing.
// These tests run it in every window a compaction or a rewrite leaves behind
// in which an object exists in the bucket without belonging in the manifest.

// refresh runs the periodic refresh against the world's bucket.
func (w *raceWorld) refresh(t *testing.T) {
	t.Helper()
	listStart := time.Now()
	var objects []manifest.ListedObject
	for _, k := range w.pool.Keys() {
		objects = append(objects, manifest.ListedObject{Key: k, Size: int64(len(w.pool.get(k)))})
	}
	if !w.manifest.ApplyListing(objects, listStart) {
		t.Fatal("the refresh was rejected by the cliff guard")
	}
}

// assertVisible checks what a query sees — the rows of MANIFESTED objects, less
// those an active tombstone hides: every kept row exactly once, no deleted row.
// Unlike assert it tolerates objects awaiting deletion, which is the point.
func (w *raceWorld) assertVisible(t *testing.T, stage string) {
	t.Helper()
	counts := map[string]int{}
	active := w.store.Active()
	for _, files := range w.manifest.AllFiles() {
		for _, fi := range files {
			data := w.pool.get(fi.Key)
			if data == nil {
				t.Fatalf("%s: manifested key %s has no object behind it", stage, fi.Key)
			}
			rows, err := readLogRows(data)
			if err != nil {
				t.Fatalf("%s: read %s: %v", stage, fi.Key, err)
			}
			if fi.RowCount != int64(len(rows)) {
				t.Fatalf("%s: %s is registered with %d rows but holds %d", stage, fi.Key, fi.RowCount, len(rows))
			}
			for i := range rows {
				hidden := false
				for j := range active {
					if active[j].MatchesFields(delete.LogRowFields(&rows[i]), rows[i].TimestampUnixNano) {
						hidden = true
						break
					}
				}
				if !hidden {
					counts[rows[i].Body]++
				}
			}
		}
	}
	for body := range w.kept {
		if counts[body] != 1 {
			t.Fatalf("%s: kept row %q is visible %d times", stage, body, counts[body])
		}
	}
	for body := range w.deleted {
		if counts[body] > 0 {
			t.Fatalf("%s: deleted row %q is visible", stage, body)
		}
	}
	// Every object outside the manifest must be one the manifest knows it let
	// go of — never an object a refresh could adopt.
	storageinvariants.Assert(t, stage, storageinvariants.State{
		Manifest: w.manifest, Bucket: w.pool.mockPool, Tombstones: tombstoneViewsOf(w.store),
		AwaitingDeletion: storageinvariants.AwaitingDeletionIn(w.manifest),
	})
}

func TestDeleteRefresh_MergedSourceWhoseDeleteFailedIsNotReadopted(t *testing.T) {
	w := newRaceWorld(t)
	stuck := w.files[0].Key
	w.pool.setFailDelete(func(k string) bool { return k == stuck })

	if _, err := w.compactor(time.Hour).Compact(context.Background(), racePartition, w.files, 0); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if w.pool.get(stuck) == nil {
		t.Fatal("fixture: the merged source must survive its failed delete")
	}
	// Both sources stay retired: the stuck one because its object is still
	// there and owed, the other because a listing that began before its delete
	// can still report it. Only the stuck one is owed — the retired set tracks
	// the outstanding deletes through Reclaim, not through membership.
	rk := w.manifest.RetiredKeys()
	if len(rk) != 2 {
		t.Fatalf("retired keys = %+v, want both merged sources held", rk)
	}
	owed, landed := 0, 0
	for _, r := range rk {
		switch {
		case r.Reclaim:
			owed++
			if r.Key != stuck {
				t.Fatalf("the delete owed is %s, want the stuck source %s", r.Key, stuck)
			}
		case r.Deleted:
			landed++
		}
	}
	if owed != 1 || landed != 1 {
		t.Fatalf("retired keys = %+v, want exactly one delete owed and one landed", rk)
	}

	w.refresh(t)
	if w.manifest.HasKey(stuck) {
		t.Fatalf("the refresh re-adopted %s, which the compaction merged", stuck)
	}
	w.assertVisible(t, "refresh after the failed source delete")

	// The scheduler's reclaim retries the delete once it can succeed.
	w.pool.setFailDelete(nil)
	if deleted, failed := w.manifest.ReclaimRetired(context.Background(), w.pool.Delete, 0); deleted != 1 || failed != 0 {
		t.Fatalf("reclaim deleted=%d failed=%d, want the one stuck source", deleted, failed)
	}
	w.refresh(t)
	w.converge(t)
	w.refresh(t)
	w.assert(t, "after the reclaim")
}

func TestDeleteRefresh_RewriteDiscardedAfterCompactionWhoseDeleteFailed(t *testing.T) {
	w := newRaceWorld(t)
	isReplacement := func(k string) bool {
		return !strings.Contains(k, "compacted-") && k != w.files[0].Key && k != w.files[1].Key
	}
	reached, release := w.pool.gate(isReplacement)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.scheduler().RunOnce(context.Background())
	}()
	<-reached
	if _, err := w.compactor(time.Hour).Compact(context.Background(), racePartition, w.files, 0); err != nil {
		t.Fatalf("compaction should win the race and publish: %v", err)
	}
	w.pool.setFailDelete(isReplacement) // the abandoned replacement's delete fails
	release()
	wg.Wait()

	w.refresh(t)
	for _, k := range w.pool.Keys() {
		if isReplacement(k) && w.manifest.HasKey(k) {
			t.Fatalf("the refresh adopted the discarded replacement %s next to the compacted output", k)
		}
	}
	w.assertVisible(t, "refresh after the failed discard")

	w.pool.setFailDelete(nil)
	w.converge(t)
	w.refresh(t)
	w.assert(t, "after the discard was retried")
	if w.store.Count() != 0 {
		t.Fatalf("the tombstone should retire once the abandoned replacement is gone, %d remain", w.store.Count())
	}
}

func TestDeleteRefresh_RefreshBetweenCompactionUploadAndPublish(t *testing.T) {
	w := newRaceWorld(t)
	var output string
	w.pool.onUpload(func(k string) bool { return strings.Contains(k, "compacted-") }, func(k string) {
		output = k
		w.refresh(t)
		if w.manifest.HasKey(k) {
			t.Errorf("the refresh adopted the unpublished compaction output %s next to its sources", k)
		}
		w.assertVisible(t, "refresh between the compaction's upload and publish")
	})

	res, err := w.compactor(time.Hour).Compact(context.Background(), racePartition, w.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if output == "" || output != res.OutputFile {
		t.Fatalf("fixture: the hook must see the output upload, saw %q want %q", output, res.OutputFile)
	}
	if fi, ok := w.manifest.GetFileByKey(output); !ok || fi.RowCount != res.RowsMerged {
		t.Fatalf("the published output must carry the compactor's own entry (rows %d), got %+v ok=%v", res.RowsMerged, fi, ok)
	}
	w.refresh(t)
	w.assertVisible(t, "after the publish")
}

// TestDeleteRefresh_ListingThatBeganBeforeTheCompactionIsNotReadopted drives the
// whole compaction — merge, publish, delete both sources, all deletes landing —
// against a bucket listing that BEGAN BEFORE it and is applied after. S3 read
// the sources before they were deleted, so its answer still names them; the
// refresh applying it must not put them back next to the output that already
// holds their rows.
//
// This is the case a landed delete cannot settle on its own, and it is the one
// that happens every time in production: deletes normally succeed.
func TestDeleteRefresh_ListingThatBeganBeforeTheCompactionIsNotReadopted(t *testing.T) {
	w := newRaceWorld(t)

	// The LIST begins and pages the bucket as it is: both sources, no output.
	listStart := time.Now()
	var stale []manifest.ListedObject
	for _, k := range w.pool.Keys() {
		stale = append(stale, manifest.ListedObject{Key: k, Size: int64(len(w.pool.get(k)))})
	}
	time.Sleep(2 * time.Millisecond)

	res, err := w.compactor(time.Hour).Compact(context.Background(), racePartition, w.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	for _, f := range w.files {
		if w.pool.get(f.Key) != nil {
			t.Fatalf("fixture: source %s should have been deleted by the compaction", f.Key)
		}
	}

	// The LIST's pages arrive and the refresh applies them.
	if !w.manifest.ApplyListing(stale, listStart) {
		t.Fatal("the refresh was rejected by the cliff guard")
	}
	for _, f := range w.files {
		if w.manifest.HasKey(f.Key) {
			fi, _ := w.manifest.GetFileByKey(f.Key)
			t.Errorf("the refresh re-admitted the deleted source %s next to %s; entry now %+v", f.Key, res.OutputFile, fi)
		}
	}
	if !w.manifest.HasKey(res.OutputFile) {
		t.Errorf("merged output %s missing from the manifest", res.OutputFile)
	}
	w.assertVisible(t, "refresh from a listing that began before the compaction")

	// A listing that began after the deletes settles them: the guards go.
	w.refresh(t)
	if rk := w.manifest.RetiredKeys(); len(rk) != 0 {
		t.Errorf("a listing that began after the deletes must forget the guards, still held: %+v", rk)
	}
	w.assertVisible(t, "after the settling refresh")
}
