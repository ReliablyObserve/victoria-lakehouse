package delete

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

func TestMergeSupersessions(t *testing.T) {
	t0 := time.Now()
	prepared := Supersession{NewKey: "n1", State: SupersessionPrepared, At: t0}
	published := Supersession{NewKey: "n1", State: SupersessionPublished, At: t0.Add(time.Second)}
	discarded := Supersession{NewKey: "n1", State: SupersessionDiscarded, At: t0.Add(time.Second)}
	laterAttempt := Supersession{NewKey: "n2", State: SupersessionPrepared, At: t0.Add(time.Minute)}

	if got := mergeSupersessions(nil, nil, nil); got != nil {
		t.Fatalf("nothing to merge: %v", got)
	}
	for _, tc := range []struct {
		name   string
		a, b   Supersession
		reaped bool
		want   *Supersession
	}{
		{"further progress wins (b)", prepared, published, false, &published},
		{"further progress wins (a)", published, prepared, false, &published},
		{"discarded beats prepared", prepared, discarded, false, &discarded},
		{"a later attempt with another replacement wins", prepared, laterAttempt, false, &laterAttempt},
		{"an earlier attempt does not replace a later one", laterAttempt, prepared, false, &laterAttempt},
		{"a prepared record of a source already reaped is stale", prepared, prepared, true, nil},
		{"a published record of a reaped source stays until its delete lands", published, prepared, true, &published},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reaped := map[string]bool{}
			if tc.reaped {
				reaped["src"] = true
			}
			got := mergeSupersessions(map[string]Supersession{"src": tc.a}, map[string]Supersession{"src": tc.b}, reaped)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("want the record dropped, got %v", got)
				}
				return
			}
			if got["src"] != *tc.want {
				t.Fatalf("got %+v, want %+v", got["src"], *tc.want)
			}
		})
	}
}

// TestRestore_NeverUndoesAFinishedRewriteFromAStaleCopy is the merge rule's
// reason to exist: an S3 copy that fell behind still holds "prepared", while
// the disk copy has the rewrite finished (source reaped, record cleared).
// Resolving "prepared" would retire the live replacement.
func TestRestore_NeverUndoesAFinishedRewriteFromAStaleCopy(t *testing.T) {
	s3 := newMockS3Pool()
	dir := t.TempDir()
	cfg := PersistenceConfig{Dir: dir, Pool: s3, Prefix: "logs/"}

	stale := NewTombstoneStore()
	stale.EnablePersistence(PersistenceConfig{Pool: s3, Prefix: "logs/"})
	stale.Add(Tombstone{ID: "t", Mode: "permanent", AffectedKeys: []string{"src"},
		Superseded: map[string]Supersession{"src": {NewKey: "repl", State: SupersessionPrepared, At: time.Now()}}})

	fresh := NewTombstoneStore()
	fresh.EnablePersistence(PersistenceConfig{Dir: dir})
	fresh.Add(Tombstone{ID: "t", Mode: "permanent", AffectedKeys: []string{"src", "repl", "other"},
		Reaped: map[string]bool{"src": true}, Clean: map[string]bool{"repl": true}})

	restored := NewTombstoneStore()
	if _, err := restored.Restore(context.Background(), cfg); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, _ := restored.Get("t")
	if len(got.Superseded) != 0 {
		t.Fatalf("a stale prepared record survived the merge: %+v", got.Superseded)
	}

	m := lhmanifest.New("b", "")
	m.AddFile("dt=2026-03-01/hour=07", lhmanifest.FileInfo{Key: "logs/dt=2026-03-01/hour=07/repl.parquet", Size: 1})
	if n := ResolveInterruptedRewrites(restored, m); n != 0 {
		t.Fatalf("nothing is interrupted, resolved %d", n)
	}
}

func TestTombstoneValidate_RejectsMalformedRewriteRecords(t *testing.T) {
	base := Tombstone{ID: "t", Query: "*", StartNs: 0, EndNs: 1, Mode: "permanent"}
	bad := base
	bad.Superseded = map[string]Supersession{"src": {NewKey: "n", State: "half-done"}}
	if err := bad.Validate(); err == nil {
		t.Fatal("an unknown rewrite state must be rejected")
	}
	bad = base
	bad.Superseded = map[string]Supersession{"src": {NewKey: "n\xff", State: SupersessionPrepared}}
	if err := bad.Validate(); err == nil {
		t.Fatal("a replacement key that is not UTF-8 must be rejected")
	}
	bad = base
	bad.Clean = map[string]bool{"\xfe": true}
	if err := bad.Validate(); err == nil {
		t.Fatal("a clean key that is not UTF-8 must be rejected")
	}
	good := base
	good.Superseded = map[string]Supersession{"src": {NewKey: "n", State: SupersessionPublished}}
	good.Clean = map[string]bool{"n": true}
	if err := good.Validate(); err != nil {
		t.Fatalf("a well-formed record is valid: %v", err)
	}
}

func TestTombstone_HandledReapedAndClean(t *testing.T) {
	ts := Tombstone{AffectedKeys: []string{"src", "repl"}}
	ts.MarkReaped("src")
	ts.SetClean("repl", true)
	if !ts.Handled("src") || !ts.Handled("repl") || ts.Handled("other") || !ts.FullyReaped() {
		t.Fatalf("bookkeeping = reaped %v clean %v", ts.Reaped, ts.Clean)
	}
	// A clean file merged away later becomes reaped, not both.
	ts.MarkReaped("repl")
	if ts.Clean["repl"] || !ts.Reaped["repl"] {
		t.Fatalf("merged-away clean file: reaped %v clean %v", ts.Reaped, ts.Clean)
	}
	ts.SetClean("x", true)
	ts.SetClean("x", false)
	if ts.Clean["x"] {
		t.Fatal("SetClean(false) must clear the mark")
	}
	// An unfinished rewrite keeps the tombstone working.
	ts.Superseded = map[string]Supersession{"src": {NewKey: "repl", State: SupersessionPublished}}
	if ts.FullyReaped() {
		t.Fatal("a tombstone with an unsettled rewrite is not fully reaped")
	}
}

func TestTryRemove(t *testing.T) {
	store := NewTombstoneStore()
	if err := store.TryRemove("missing"); !errors.Is(err, ErrTombstoneNotFound) {
		t.Fatalf("missing id: %v", err)
	}
	store.Add(Tombstone{ID: "busy", Mode: "permanent",
		Superseded: map[string]Supersession{"src": {NewKey: "n", State: SupersessionPublished}}})
	if err := store.TryRemove("busy"); !errors.Is(err, ErrRewriteInProgress) {
		t.Fatalf("an un-delete mid-rewrite must be refused: %v", err)
	}
	if _, ok := store.Get("busy"); !ok {
		t.Fatal("a refused un-delete must leave the tombstone")
	}
	store.Add(Tombstone{ID: "idle", Mode: "hide"})
	if err := store.TryRemove("idle"); err != nil {
		t.Fatalf("an idle tombstone is removable: %v", err)
	}
	if _, ok := store.Get("idle"); ok {
		t.Fatal("TryRemove must remove the tombstone")
	}
}

func TestHandler_UndeleteMidRewriteIsAConflict(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{ID: "busy", Mode: "permanent",
		Superseded: map[string]Supersession{"src": {NewKey: "n", State: SupersessionPrepared}}})
	h := NewHandler(store, &mockManifest{}, nil, nil, "logs")
	mux := http.NewServeMux()
	h.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/delete/logsql/tombstone/busy", nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 while the rewrite is unfinished", rec.Code)
	}
}

func TestClaimKey(t *testing.T) {
	store := NewTombstoneStore()
	if !store.claimKey("k") {
		t.Fatal("first claim succeeds")
	}
	if store.claimKey("k") {
		t.Fatal("a second claim of the same key must fail")
	}
	store.releaseKey("k")
	if !store.claimKey("k") {
		t.Fatal("a released key can be claimed again")
	}
}

func TestResolveInterruptedRewrites_EachState(t *testing.T) {
	const part = "dt=2026-03-01/hour=07"
	key := func(n string) string { return "logs/" + part + "/" + n + ".parquet" }

	if ResolveInterruptedRewrites(nil, nil) != 0 {
		t.Fatal("nil arguments resolve nothing")
	}

	m := lhmanifest.New("b", "")
	m.AddFile(part, lhmanifest.FileInfo{Key: key("undone-src"), Size: 1})
	m.AddFile(part, lhmanifest.FileInfo{Key: key("published-src"), Size: 1})
	m.AddFile(part, lhmanifest.FileInfo{Key: key("promoted-src"), Size: 1})
	// The snapshot captured a swap whose record never reached "published".
	m.AddFile(part, lhmanifest.FileInfo{Key: key("swapped-src"), Size: 1})
	m.ReplaceFile(part, key("swapped-src"), lhmanifest.FileInfo{Key: key("swapped-repl"), Size: 1})
	// ...and a replacement a later compaction already merged.
	m.Retire(key("promoted-repl"), key("compacted"), true)

	store := NewTombstoneStore()
	store.Add(Tombstone{ID: "t", Mode: "permanent", Superseded: map[string]Supersession{
		key("undone-src"):    {NewKey: key("undone-repl"), State: SupersessionPrepared},
		key("swapped-src"):   {NewKey: key("swapped-repl"), State: SupersessionPrepared},
		key("published-src"): {NewKey: key("published-repl"), State: SupersessionPublished},
		key("discarded-src"): {NewKey: key("discarded-repl"), State: SupersessionDiscarded},
		key("promoted-src"):  {NewKey: key("promoted-repl"), State: SupersessionPrepared},
		key("all-rows-src"):  {State: SupersessionPrepared},
	}})
	before := metrics.DeleteRewriteInterrupted.Get("undone")

	if n := ResolveInterruptedRewrites(store, m); n != 6 {
		t.Fatalf("resolved %d records, want 6", n)
	}

	if !m.IsRetired(key("undone-repl")) || !m.HasKey(key("undone-src")) {
		t.Fatal("prepared: the replacement is retired and the source stays")
	}
	if m.HasKey(key("swapped-repl")) || !m.IsRetired(key("swapped-repl")) || m.IsRetired(key("swapped-src")) {
		t.Fatal("prepared with the swap in the snapshot: the swap is undone, the source adoptable again")
	}
	if m.HasKey(key("published-src")) || !m.IsRetired(key("published-src")) {
		t.Fatal("published: the source is retired")
	}
	if !m.IsRetired(key("discarded-repl")) {
		t.Fatal("discarded: the abandoned replacement is retired")
	}
	if m.HasKey(key("promoted-src")) {
		t.Fatal("a replacement already merged proves its publish landed: the source is retired, not restored")
	}

	got, _ := store.Get("t")
	if got.Superseded[key("undone-src")].State != SupersessionDiscarded {
		t.Fatalf("an undone rewrite is recorded as discarded until its replacement is deleted: %+v", got.Superseded)
	}
	if got.Superseded[key("promoted-src")].State != SupersessionPublished || !got.Reaped[key("promoted-src")] {
		t.Fatalf("a promoted rewrite is recorded as published with its source reaped: %+v", got)
	}
	if metrics.DeleteRewriteInterrupted.Get("undone") <= before {
		t.Error("resolutions are counted by outcome")
	}
}

// TestResumeRewrites_RetriesUntilTheDeleteLands pins the durable retry queue:
// a superseded object whose delete keeps failing keeps its record — and the
// tombstone active — across passes, and the record clears on the first pass
// whose delete succeeds.
func TestResumeRewrites_RetriesUntilTheDeleteLands(t *testing.T) {
	f := newRewriteFixture(t)
	f.fault.failDeleteOn = f.key
	f.sched.RunOnce(context.Background())

	ts, ok := f.store.Get("ts-fixture")
	if !ok || ts.Superseded[f.key].State != SupersessionPublished {
		t.Fatalf("a failed superseded-object delete must leave a published record: %+v ok=%v", ts, ok)
	}
	if !f.manifest.IsRetired(f.key) {
		t.Fatal("the superseded source stays retired while its delete is owed")
	}

	f.fault.failDeleteOn = f.key // fails again on the next pass
	f.sched.RunOnce(context.Background())
	if ts, _ := f.store.Get("ts-fixture"); len(ts.Superseded) != 1 {
		t.Fatalf("the record must survive a second failed delete: %+v", ts.Superseded)
	}

	f.sched.RunOnce(context.Background())
	if _, still := f.store.Get("ts-fixture"); still {
		t.Fatal("once the delete lands the record clears and the tombstone retires")
	}
	if f.pool.Has(f.key) || f.manifest.IsRetired(f.key) {
		t.Fatal("the superseded object must be gone and forgotten")
	}
	f.assertConverged(t, "after the retried delete")
}
