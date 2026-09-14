package manifest

import (
	"testing"
	"time"
)

// A bucket listing describes the bucket as it was when the listing ran, not as
// it is when the refresh applies it. Every publish that replaces an object and
// then deletes it therefore races one specific listing: the one that began
// before the publish and lands after the delete. It still reports the deleted
// object, and the refresh applying it would adopt that object back — next to
// whatever replaced it, as a bare entry with no row count and no aggregates.
//
// The retired set is what closes that window, and the window does NOT close
// when the delete lands: it closes when a listing that began after the
// retirement comes back without the key. These tests drive the manifest exactly
// as the two publishers do — compaction (ReplaceFiles + ConfirmDeleted per
// source, see internal/compaction/compactor.go) and the delete rewriter
// (ReplaceFile + ConfirmDeleted, see internal/delete/intents.go) — for both
// outcomes of the delete, and pin that the record outlives the delete.

const (
	slPartition = "dt=2026-05-01/hour=10"
	slSrcA      = "logs/dt=2026-05-01/hour=10/src-a.parquet"
	slSrcB      = "logs/dt=2026-05-01/hour=10/src-b.parquet"
	slMerged    = "logs/dt=2026-05-01/hour=10/compacted-L1-0001.parquet"
)

// slWorld is a manifest holding two flushed files, already in its steady state:
// one accepted listing has been applied, so a later refresh is judged against a
// manifest that has listed the bucket.
func slWorld(t *testing.T) *Manifest {
	t.Helper()
	m := New("test-bucket", "logs/")
	m.AddFile(slPartition, FileInfo{Key: slSrcA, Size: 1000, RowCount: 10, MinTimeNs: 1, MaxTimeNs: 2})
	m.AddFile(slPartition, FileInfo{Key: slSrcB, Size: 1000, RowCount: 10, MinTimeNs: 1, MaxTimeNs: 2})
	if !m.ApplyListing([]ListedObject{{Key: slSrcA, Size: 1000}, {Key: slSrcB, Size: 1000}}, time.Now()) {
		t.Fatal("fixture: baseline listing rejected")
	}
	time.Sleep(2 * time.Millisecond)
	return m
}

// slStaleListing is the in-flight LIST: it begins now and pages through the
// bucket as it is at this instant — both sources, no merged output yet.
func slStaleListing() (time.Time, []ListedObject) {
	start := time.Now()
	time.Sleep(2 * time.Millisecond)
	return start, []ListedObject{{Key: slSrcA, Size: 1000}, {Key: slSrcB, Size: 1000}}
}

// slPublishMerge is compaction's publish step (compactor.go: ReplaceFiles).
func slPublishMerge(t *testing.T, m *Manifest) {
	t.Helper()
	if !m.ReplaceFiles(slPartition, []string{slSrcA, slSrcB}, FileInfo{
		Key: slMerged, Size: 1500, RowCount: 20, CompactionLevel: 1, MinTimeNs: 1, MaxTimeNs: 2,
	}) {
		t.Fatal("fixture: publish refused")
	}
}

func slAssertOnlyTheMergedOutput(t *testing.T, m *Manifest, stage string) {
	t.Helper()
	for _, k := range []string{slSrcA, slSrcB} {
		if m.HasKey(k) {
			fi, _ := m.GetFileByKey(k)
			t.Errorf("%s: the refresh re-admitted the compacted-away source %s, whose rows are already in %s; entry now %+v",
				stage, k, slMerged, fi)
		}
	}
	if !m.HasKey(slMerged) {
		t.Errorf("%s: merged output %s missing from the manifest", stage, slMerged)
	}
	if got := m.TotalFiles(); got != 1 {
		t.Errorf("%s: TotalFiles=%d, want 1", stage, got)
	}
	if got := m.TotalRows(); got != 20 {
		t.Errorf("%s: TotalRows=%d, want 20", stage, got)
	}
}

// TestStaleListing_CompactedSourcesNotReadmitted_DeleteFailed: the sources'
// deletes failed, so their objects really are still in the bucket and their
// deletes are still owed. The retired set keeps them out of the refresh and the
// scheduler's reclaim retries.
func TestStaleListing_CompactedSourcesNotReadmitted_DeleteFailed(t *testing.T) {
	m := slWorld(t)
	listStart, stale := slStaleListing()
	slPublishMerge(t, m)

	if !m.ApplyListing(stale, listStart) {
		t.Fatal("refresh rejected by the cliff guard")
	}
	slAssertOnlyTheMergedOutput(t, m, "source delete failed")
	if got := len(m.RetiredKeys()); got != 2 {
		t.Errorf("retired keys = %d, want both sources still held for the retry", got)
	}
}

// TestStaleListing_CompactedSourcesNotReadmitted_DeleteSucceeded is the normal
// case, and the one a landed-delete-forgets-the-key implementation gets wrong:
// both deletes succeed, and the listing that began before the publish is
// applied afterwards. The objects are gone, but S3 read them before they were,
// so the answer still names them.
func TestStaleListing_CompactedSourcesNotReadmitted_DeleteSucceeded(t *testing.T) {
	m := slWorld(t)
	listStart, stale := slStaleListing()
	slPublishMerge(t, m)
	m.ConfirmDeleted(slSrcA) // compactor.go: pool.Delete returned success
	m.ConfirmDeleted(slSrcB)

	if !m.ApplyListing(stale, listStart) {
		t.Fatal("refresh rejected by the cliff guard")
	}
	slAssertOnlyTheMergedOutput(t, m, "source delete succeeded")
}

// TestStaleListing_RewrittenSourceNotReadmitted_DeleteSucceeded: the same race
// on the delete rewriter's path, where re-admitting the source does not merely
// double-count rows — it serves back the rows a delete request removed.
func TestStaleListing_RewrittenSourceNotReadmitted_DeleteSucceeded(t *testing.T) {
	const (
		p    = "dt=2026-05-01/hour=11"
		src  = "logs/dt=2026-05-01/hour=11/src.parquet"
		repl = "logs/dt=2026-05-01/hour=11/src-rewrite-abcd1234.parquet"
	)
	m := New("test-bucket", "logs/")
	m.AddFile(p, FileInfo{Key: src, Size: 1000, RowCount: 10, MinTimeNs: 1, MaxTimeNs: 2})
	if !m.ApplyListing([]ListedObject{{Key: src, Size: 1000}}, time.Now()) {
		t.Fatal("fixture: baseline listing rejected")
	}
	time.Sleep(2 * time.Millisecond)

	listStart := time.Now()
	stale := []ListedObject{{Key: src, Size: 1000}}
	time.Sleep(2 * time.Millisecond)

	// The rewrite drops three rows and publishes the smaller replacement.
	if !m.ReplaceFile(p, src, FileInfo{Key: repl, Size: 800, RowCount: 7, MinTimeNs: 1, MaxTimeNs: 2}) {
		t.Fatal("fixture: publish refused")
	}
	m.ConfirmDeleted(src) // intents.go: the superseded object's delete landed

	if !m.ApplyListing(stale, listStart) {
		t.Fatal("refresh rejected by the cliff guard")
	}
	if m.HasKey(src) {
		t.Errorf("the refresh re-admitted the rewritten-away source %s: the rows the delete removed are served again", src)
	}
	if got := m.TotalRows(); got != 7 {
		t.Errorf("TotalRows=%d, want 7 (the deleted rows must stay deleted)", got)
	}
}

// TestStaleListing_RecordIsForgottenByTheNextListing: the guard is not held
// forever. A listing that BEGAN AFTER the retirement and came back without the
// key proves the object is gone — no listing still in flight can report it —
// and the record is dropped.
func TestStaleListing_RecordIsForgottenByTheNextListing(t *testing.T) {
	m := slWorld(t)
	slPublishMerge(t, m)
	m.ConfirmDeleted(slSrcA)
	m.ConfirmDeleted(slSrcB)
	if got := len(m.RetiredKeys()); got != 2 {
		t.Fatalf("retired keys = %d right after the deletes, want 2 held as guards", got)
	}
	time.Sleep(2 * time.Millisecond)

	// A fresh listing: it began after the retirements and the objects are gone.
	if !m.ApplyListing([]ListedObject{{Key: slMerged, Size: 1500}}, time.Now()) {
		t.Fatal("refresh rejected by the cliff guard")
	}
	if got := m.RetiredKeys(); len(got) != 0 {
		t.Errorf("a listing that began after the retirement and did not report the keys must forget them, still held: %+v", got)
	}
	slAssertOnlyTheMergedOutput(t, m, "after the settling listing")
}

// TestStaleListing_RejectedListingSettlesNothing: the cliff guard refuses a
// listing that would drop most of the manifest. Such a listing proves nothing
// about any key, so it must not retire the guards either — otherwise the next
// in-flight listing walks straight into the window this file is about.
func TestStaleListing_RejectedListingSettlesNothing(t *testing.T) {
	m := New("test-bucket", "logs/")
	// Enough files that losing them all trips the cliff guard, which only
	// rejects a listing that drops more than half of a non-empty manifest.
	var baseline []ListedObject
	for _, k := range []string{slSrcA, slSrcB, slMerged, "logs/dt=2026-05-01/hour=10/other.parquet"} {
		m.AddFile(slPartition, FileInfo{Key: k, Size: 1000, RowCount: 10, MinTimeNs: 1, MaxTimeNs: 2})
		baseline = append(baseline, ListedObject{Key: k, Size: 1000})
	}
	if !m.ApplyListing(baseline, time.Now()) {
		t.Fatal("fixture: baseline listing rejected")
	}
	time.Sleep(2 * time.Millisecond)

	m.Retire(slSrcA, slMerged, true)
	m.ConfirmDeleted(slSrcA)
	time.Sleep(2 * time.Millisecond)

	// A transient LIST hiccup: it began after the retirement, so it would
	// otherwise settle the guard, but it came back empty and is rejected.
	if m.ApplyListing(nil, time.Now()) {
		t.Fatal("fixture: the cliff guard must reject a listing that loses every file")
	}
	if got := m.RetiredKeys(); len(got) != 1 || got[0].Key != slSrcA {
		t.Errorf("a REJECTED listing settles nothing, so the guard must stay; retired = %+v", got)
	}
}
