package delete

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// --- the supersession path, inside this package ---------------------------------

// supersedingManifest removes the source from the manifest the moment the
// rewriter looks up its partition: the window between the rewrite's
// "is the source still there" check and its swap, where a concurrent
// compaction merging the same file lands.
type supersedingManifest struct {
	*lhmanifest.Manifest
	fired bool
}

func (m *supersedingManifest) PartitionForKey(key string) (string, bool) {
	p, ok := m.Manifest.PartitionForKey(key)
	if ok && !m.fired {
		m.fired = true
		m.RemoveFile(p, key)
	}
	return p, ok
}

func TestRewrite_SourceSupersededAtPublishIsDiscardedNotRegistered(t *testing.T) {
	f := newRewriteFixture(t)
	sm := &supersedingManifest{Manifest: f.manifest}
	f.sched.manifest = sm

	before := f.pool.Count()
	results := f.sched.RunOnce(context.Background())

	if len(results) != 0 {
		t.Fatalf("a superseded rewrite must not be reported as done: %+v", results)
	}
	// The replacement was uploaded and then discarded; the source object is
	// untouched (the "compaction" that took it owns its deletion).
	if got := f.pool.Count(); got != before {
		t.Fatalf("bucket holds %d objects, want the original %d — the discarded replacement leaked", got, before)
	}
	for _, k := range f.pool.Keys() {
		if f.manifest.HasKey(k) {
			t.Fatalf("nothing may be registered: %s", k)
		}
	}
}

func TestPublishRewrite_ReplaceRefusedAfterConcurrentRemovalIsSuperseded(t *testing.T) {
	m := newTestManifest(t, map[string]int64{fixtureKey: 5})
	sm := &supersedingManifest{Manifest: m}
	res := &RewriteResult{OldKey: fixtureKey, NewKey: "logs/dt=2026-03-01/hour=07/n.parquet", RowsKept: 3, RowsRemoved: 2, BytesAfter: 5}
	if _, err := publishRewrite(sm, res); !errors.Is(err, errSourceSuperseded) {
		t.Fatalf("a swap refused because the source vanished must be superseded, got %v", err)
	}

	m0 := newTestManifest(t, map[string]int64{fixtureKey: 5})
	sm0 := &supersedingManifest{Manifest: m0}
	res0 := &RewriteResult{OldKey: fixtureKey, RowsKept: 0, RowsRemoved: 5}
	if _, err := publishRewrite(sm0, res0); !errors.Is(err, errSourceSuperseded) {
		t.Fatalf("a removal refused because the source vanished must be superseded, got %v", err)
	}
}

// --- discovery -------------------------------------------------------------------

func TestDiscoverAffectedKeys_AddsOverlappingFilesOnce(t *testing.T) {
	hour := time.Date(2026, 3, 1, 7, 0, 0, 0, time.UTC)
	m := lhmanifest.New("b", "")
	listed := "logs/dt=2026-03-01/hour=07/listed.parquet"
	late := "logs/dt=2026-03-01/hour=07/late.parquet"
	for _, k := range []string{listed, late} {
		m.AddFile("dt=2026-03-01/hour=07", lhmanifest.FileInfo{
			Key: k, RowCount: 1, MinTimeNs: hour.Add(time.Minute).UnixNano(), MaxTimeNs: hour.Add(2 * time.Minute).UnixNano(),
		})
	}
	store := NewTombstoneStore()
	store.Add(Tombstone{Tenants: []TenantRef{{}}, ID: "ts", Mode: "permanent", StartNs: hour.UnixNano(), EndNs: hour.Add(time.Hour).UnixNano(),
		AffectedKeys: []string{listed}, Reaped: map[string]bool{}})
	s := NewRewriteScheduler(RewriteSchedulerConfig{Store: store, Rewriter: NewRewriter(newMockRewriterPool(), "logs/", 100, "logs"),
		Detector: NewStorageClassDetector(nil), Manifest: m})

	if n := s.discoverAffectedKeys("ts"); n != 1 {
		t.Fatalf("discovered %d files, want the 1 unlisted overlapping file", n)
	}
	got, _ := store.Get("ts")
	if !containsString(got.AffectedKeys, late) || got.Handled(late) {
		t.Fatalf("the late file must be listed as pending: %+v", got)
	}
	if n := s.discoverAffectedKeys("ts"); n != 0 {
		t.Fatalf("a second discovery must add nothing, added %d", n)
	}
	if n := s.discoverAffectedKeys("absent"); n != 0 {
		t.Fatalf("discovery on an unknown tombstone must add nothing, added %d", n)
	}
	if containsString(nil, "x") {
		t.Fatal("nothing is contained in an empty list")
	}
}

// --- rewriter edge paths -----------------------------------------------------------

type deleteFailPool struct{ *mockRewriterPool }

func (d deleteFailPool) Delete(context.Context, string) error { return errInjected }

func TestRewriter_CommitAndDiscardEdgePaths(t *testing.T) {
	pool := newMockRewriterPool()
	rw := NewRewriter(pool, "logs/", 0, "") // defaults applied
	ctx := context.Background()

	if err := rw.Commit(ctx, nil); err != nil {
		t.Errorf("committing nothing is not an error: %v", err)
	}
	if err := rw.Commit(ctx, &RewriteResult{OldKey: "k"}); err != nil {
		t.Errorf("a rewrite that removed nothing has nothing to commit: %v", err)
	}

	failing := NewRewriter(deleteFailPool{pool}, "logs/", 100, "logs")
	if err := failing.Commit(ctx, &RewriteResult{OldKey: "k", RowsRemoved: 1, Published: true}); err == nil {
		t.Error("a failed delete of the superseded object must be reported")
	}

	pool.Put("logs/new.parquet", []byte("x"))
	rw.Discard(ctx, nil)
	rw.Discard(ctx, &RewriteResult{NewKey: "logs/new.parquet", Published: true})
	if !pool.Has("logs/new.parquet") {
		t.Fatal("a published replacement must never be discarded")
	}
	rw.Discard(ctx, &RewriteResult{NewKey: ""})
	rw.Discard(ctx, &RewriteResult{NewKey: "logs/new.parquet"})
	if pool.Has("logs/new.parquet") {
		t.Fatal("an unpublished replacement must be discarded")
	}
	// A failed discard is logged, not fatal (the scheduler's durable path
	// retries it; see TestRewriteRefresh_*).
	failing.Discard(ctx, &RewriteResult{NewKey: "logs/other.parquet"})
}

func TestFooterHelpers_ToleratesUnreadableBytes(t *testing.T) {
	junk := []byte("not a parquet file")
	if got := footerBloomBytes(junk); got != 0 {
		t.Errorf("footerBloomBytes(junk) = %d, want 0", got)
	}
	if got := columnBytesFromFooter(junk); got != nil {
		t.Errorf("columnBytesFromFooter(junk) = %v, want nil", got)
	}
	if got := sourceSlotMapping(junk); got != nil {
		t.Errorf("sourceSlotMapping(junk) = %v, want nil", got)
	}
}

func TestRowFields_AgreeWithTheMapForm(t *testing.T) {
	lr := &schema.LogRow{ServiceName: "web", Body: "b", LogAttributes: map[string]string{"k": "v"}}
	lm := logRowToMap(lr)
	for _, f := range LogRowFields(lr) {
		if lm[f.Name] != f.Value {
			t.Errorf("log field %s = %q, map has %q", f.Name, f.Value, lm[f.Name])
		}
	}
	if len(LogRowFields(lr)) != len(lm) {
		t.Error("LogRowFields and logRowToMap disagree on the field set")
	}

	tr := &schema.TraceRow{ServiceName: "svc", SpanName: "op", SpanKind: 2, StatusCode: 1, DurationNs: 5,
		SpanAttributes: map[string]string{"a": "b"}, ScopeAttributes: map[string]string{"c": "d"}}
	tm := traceRowToMap(tr)
	for _, f := range TraceRowFields(tr) {
		if tm[f.Name] != f.Value {
			t.Errorf("trace field %s = %q, map has %q", f.Name, f.Value, tm[f.Name])
		}
	}
	for _, k := range []string{"span.kind", "status.code", "duration_ns", "a", "c"} {
		if _, ok := tm[k]; !ok {
			t.Errorf("trace map lacks %s", k)
		}
	}
}

// --- tombstone store edges ---------------------------------------------------------

func TestTombstoneFilter_MatchAllHasNoFilter(t *testing.T) {
	for _, q := range []string{"", "*"} {
		ts := Tombstone{Tenants: []TenantRef{{}}, Query: q}
		if ts.Filter() != nil {
			t.Errorf("query %q is match-all and has no filter", q)
		}
	}
}

func TestMergeLoaded_KeepsTheEarliestCreationAndTheUnionOfKeys(t *testing.T) {
	store := NewTombstoneStore()
	later := time.Now()
	earlier := later.Add(-time.Hour)
	store.mu.Lock()
	store.mergeLoadedLocked(Tombstone{Tenants: []TenantRef{{}}, ID: "x", CreatedAt: later, AffectedKeys: []string{"a", "only-on-disk"}})
	store.mergeLoadedLocked(Tombstone{Tenants: []TenantRef{{}}, ID: "x", CreatedAt: earlier, AffectedKeys: []string{"a", "b"}, Reaped: map[string]bool{"a": true, "b": false}})
	store.mergeLoadedLocked(Tombstone{Tenants: []TenantRef{{}}, ID: "x", CreatedAt: time.Time{}})
	store.mu.Unlock()

	got, _ := store.Get("x")
	if !got.CreatedAt.Equal(earlier) {
		t.Errorf("CreatedAt = %v, want the earliest %v (it bounds the un-delete window conservatively)", got.CreatedAt, earlier)
	}
	// Each copy discovered a file the other did not; keeping only one list
	// would let the tombstone retire with the other file unhandled.
	want := map[string]bool{"a": true, "only-on-disk": true, "b": true}
	if len(got.AffectedKeys) != len(want) {
		t.Errorf("AffectedKeys = %v, want the union %v", got.AffectedKeys, want)
	}
	for _, k := range got.AffectedKeys {
		if !want[k] {
			t.Errorf("unexpected key %s in the merged list", k)
		}
	}
	if !got.Reaped["a"] || got.Reaped["b"] {
		t.Errorf("Reaped = %v, want only the true marks carried", got.Reaped)
	}
}

func TestPersistence_DiskFailureIsCountedNotFatal(t *testing.T) {
	// A regular file where the directory should be: MkdirAll fails.
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewTombstoneStore()
	store.EnablePersistence(PersistenceConfig{Dir: blocker})
	store.Add(Tombstone{Tenants: []TenantRef{{}}, ID: "ts", Query: "*"}) // must not panic
	if store.Count() != 1 {
		t.Fatal("a disk failure must not lose the in-memory tombstone")
	}
	if err := store.PersistToDisk(blocker); err == nil {
		t.Error("PersistToDisk into a non-directory must fail")
	}
}

func TestSelfCheck_NilInputs(t *testing.T) {
	if got := SelfCheck(nil, nil); got != nil {
		t.Errorf("a nil store has nothing to check, got %v", got)
	}
	store := NewTombstoneStore()
	store.EnablePersistence(PersistenceConfig{Dir: t.TempDir()})
	store.Add(Tombstone{Tenants: []TenantRef{{}}, ID: "ts", AffectedKeys: []string{"k"}})
	if got := SelfCheck(store, nil); len(got) != 0 {
		t.Errorf("without a manifest only persistence can be checked, got %v", got)
	}
}

// --- the publish post-condition ----------------------------------------------------

// lyingManifest reports a successful swap without performing it.
type lyingManifest struct{ *lhmanifest.Manifest }

func (m *lyingManifest) ReplaceFile(string, string, lhmanifest.FileInfo) bool { return true }

// mergeAfterSwapManifest performs the swap, then immediately removes the new
// key — a compaction merging the replacement the instant it was published.
type mergeAfterSwapManifest struct{ *lhmanifest.Manifest }

func (m *mergeAfterSwapManifest) ReplaceFile(partition, oldKey string, fi lhmanifest.FileInfo) bool {
	ok := m.Manifest.ReplaceFile(partition, oldKey, fi)
	if ok {
		m.RemoveFile(partition, fi.Key)
	}
	return ok
}

func TestPublishRewrite_ReportedSwapThatLeftTheOldKeyIsAFailure(t *testing.T) {
	m := &lyingManifest{Manifest: newTestManifest(t, map[string]int64{fixtureKey: 5})}
	res := &RewriteResult{OldKey: fixtureKey, NewKey: "logs/dt=2026-03-01/hour=07/n.parquet", RowsKept: 3, RowsRemoved: 2, BytesAfter: 5}
	if _, err := publishRewrite(m, res); err == nil {
		t.Fatal("a swap that left the old key registered must be reported")
	}
	if res.Published {
		t.Fatal("committing now would delete an object the manifest still points at")
	}
}

// TestPublishRewrite_ReplacementMergedRightAfterTheSwapIsStillPublished: once
// the swap lands, a compaction may take the replacement at once. That is a
// successful publish; treating it as a failure would discard an object that was
// registered (and may already be merged elsewhere).
func TestPublishRewrite_ReplacementMergedRightAfterTheSwapIsStillPublished(t *testing.T) {
	m := &mergeAfterSwapManifest{Manifest: newTestManifest(t, map[string]int64{fixtureKey: 5})}
	res := &RewriteResult{OldKey: fixtureKey, NewKey: "logs/dt=2026-03-01/hour=07/n.parquet", RowsKept: 3, RowsRemoved: 2, BytesAfter: 5}
	fi, err := publishRewrite(m, res)
	if err != nil {
		t.Fatalf("a publish whose replacement was merged immediately afterwards is still a publish: %v", err)
	}
	if !res.Published || fi == nil || fi.Key != res.NewKey {
		t.Fatalf("publish result = %+v, published=%v", fi, res.Published)
	}
}
