package delete

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil/storageinvariants"
)

// TestRewriteCrashMatrix injects a failure at each step of the rewrite and
// asserts the same thing every time: the state left behind is consistent, no
// kept row is lost, no deleted row is resurrected, and a retry converges.
//
// The three steps are prepare (write the replacement), publish (swap the
// manifest entry) and commit (delete the superseded object). A failure leaves
// a record on the tombstone and the object it names retired in the manifest,
// so the retry settles it — without the orphan sweep, which a manifest refresh
// would otherwise beat to the object. Crashes (the process dying, not a step
// failing) are covered with restarts and refreshes by
// TestRewriteCrashMatrix_WithManifestRefresh.
func TestRewriteCrashMatrix(t *testing.T) {
	cases := []struct {
		name string
		// arm breaks one step.
		arm func(f *rewriteFixture)
		// afterFirstRun describes the state the crash is expected to leave.
		sourceSurvives bool
		// rewritten says whether the FIRST run is expected to complete.
		rewritten bool
	}{
		{
			name: "prepare fails: replacement never written",
			arm: func(f *rewriteFixture) {
				f.fault.failUpload = true
			},
			sourceSurvives: true,
			rewritten:      false,
		},
		{
			name: "publish fails: replacement written but manifest never learns",
			arm: func(f *rewriteFixture) {
				f.wrapped.failReplace = true
			},
			sourceSurvives: true,
			rewritten:      false,
		},
		{
			name: "commit fails: manifest swapped but superseded object survives",
			arm: func(f *rewriteFixture) {
				f.fault.failDeleteOn = f.key
			},
			sourceSurvives: true,
			rewritten:      true,
		},
		{
			name:           "no fault: the happy path",
			arm:            func(f *rewriteFixture) {},
			sourceSurvives: false,
			rewritten:      true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRewriteFixture(t)
			tc.arm(f)

			results := f.sched.RunOnce(context.Background())

			if tc.rewritten && len(results) != 1 {
				t.Fatalf("expected the rewrite to complete, got %d results", len(results))
			}
			if !tc.rewritten && len(results) != 0 {
				t.Fatalf("expected the rewrite to be abandoned, got %d results", len(results))
			}
			if got := f.pool.Has(f.key); got != tc.sourceSurvives {
				t.Fatalf("source object present = %v, want %v", got, tc.sourceSurvives)
			}

			// The critical claim: whatever the failure left behind, no KEPT row
			// is unreachable. That is weaker than the full invariant set,
			// because a failed delete legitimately leaves an unmanifested
			// object behind for the retry — but a kept row that exists in no
			// readable object is unrecoverable, and that must never happen.
			assertKeptRowsReadable(t, f, tc.name)

			// Retry. Every failure must be recoverable by simply running again.
			// A failing step is armed to fire ONCE, so the second run is clean
			// and must settle everything the first left behind, with no orphan
			// sweep involved.
			f.sched.RunOnce(context.Background())
			f.assertConverged(t, tc.name+" / after retry")
		})
	}
}

// TestRewriteCrash_TombstoneBookkeepingLost covers the window between the
// manifest swap and recording the key as reaped: the process dies before the
// tombstone is updated, so on restart the tombstone still lists a key that no
// longer exists anywhere.
func TestRewriteCrash_TombstoneBookkeepingLost(t *testing.T) {
	f := newRewriteFixture(t)

	results := f.sched.RunOnce(context.Background())
	if len(results) != 1 {
		t.Fatalf("expected 1 rewrite, got %d", len(results))
	}

	// Rewind the tombstone to its pre-rewrite state, as a restart from an older
	// persisted copy would.
	f.store.Add(Tombstone{
		Tenants:      []TenantRef{{}},
		ID:           "ts-fixture",
		Query:        `severity_text:="error"`,
		StartNs:      0,
		EndNs:        10000,
		AffectedKeys: []string{f.key},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "permanent",
		Reaped:       map[string]bool{},
	})

	// The self-check must see and classify the disagreement rather than let it
	// sit silently until a query returns the wrong rows.
	found := SelfCheck(f.store, f.manifest)
	if len(found) == 0 {
		t.Fatal("self-check found nothing; a tombstone pointing at a key the manifest lost must be reported")
	}
	var sawPending bool
	for _, inc := range found {
		if inc.Kind == "pending_key_missing_from_manifest" && inc.Key == f.key {
			sawPending = true
		}
	}
	if !sawPending {
		t.Fatalf("expected pending_key_missing_from_manifest, got %v", found)
	}

	// And the next tick must converge rather than retry a download forever.
	f.sched.RunOnce(context.Background())
	if _, still := f.store.Get("ts-fixture"); still {
		t.Fatal("the scheduler must recognise the key as already superseded and complete the tombstone")
	}
	f.assertConverged(t, "recovered from lost bookkeeping")
	if rest := SelfCheck(f.store, f.manifest); len(rest) > 1 {
		t.Fatalf("self-check still reports disagreements after recovery: %v", rest)
	}
}

// TestRewriteCrash_ManifestAheadOfTombstones is the opposite skew: the manifest
// snapshot is newer than the tombstone copy, so a key recorded as reaped is
// still listed.
func TestRewriteCrash_ManifestAheadOfTombstones(t *testing.T) {
	f := newRewriteFixture(t)
	ts, _ := f.store.Get("ts-fixture")
	ts.Reaped = map[string]bool{f.key: true}
	f.store.Add(ts)

	found := SelfCheck(f.store, f.manifest)
	var saw bool
	for _, inc := range found {
		if inc.Kind == "reaped_key_still_manifested" && inc.Key == f.key {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("expected reaped_key_still_manifested, got %v", found)
	}
	// The rows stay hidden by the query-time filter in the meantime, which is
	// why this is reported rather than repaired here.
	if len(f.store.ForRange(0, 10000)) == 0 {
		t.Fatal("the tombstone must still be consulted by queries while the skew persists")
	}
}

func TestSelfCheck_ReportsMissingDurability(t *testing.T) {
	store := NewTombstoneStore()
	found := SelfCheck(store, nil)
	if len(found) != 1 || found[0].Kind != "persistence_disabled" {
		t.Fatalf("a store with no durable target must be reported, got %v", found)
	}

	store.EnablePersistence(PersistenceConfig{Dir: t.TempDir()})
	if found := SelfCheck(store, nil); len(found) != 0 {
		t.Fatalf("expected no findings once persistence is armed, got %v", found)
	}
}

// --- concurrency -------------------------------------------------------------

// TestRewriteConcurrency_RaceWithReadersAndCompaction runs a rewrite against a
// reader, a second rewrite of the same file and a compaction-shaped manifest
// mutation. Under -race this is the check that the manifest swap, the tombstone
// store and the bucket are all safe to touch concurrently; the assertions
// afterwards are the check that concurrency cannot corrupt the state.
func TestRewriteConcurrency_RaceWithReadersAndCompaction(t *testing.T) {
	f := newRewriteFixture(t)

	// A second scheduler over the same store, manifest and bucket: two rewrite
	// loops racing the same tombstone inside one process. (Separate pods have
	// separate manifests; that case is not covered by this test — see the
	// multi-instance bounds in docs/operations.md.)
	other := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          f.store,
		Rewriter:       NewRewriter(f.pool, "logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       f.manifest,
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reader: the query path's manifest lookups plus the tombstone lookup every
	// query does.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = f.manifest.GetFilesForRange(0, 1<<62)
			_ = f.store.ForRange(0, 10000)
			_ = f.manifest.TotalRows()
		}
	}()

	// Compaction-shaped writer: touches the same partition's aggregates.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key := fmt.Sprintf("logs/dt=2026-03-01/hour=07/compacted-%02d.parquet", i)
			f.manifest.AddFile("dt=2026-03-01/hour=07", lhmanifest.FileInfo{
				Key: key, Size: 1, RowCount: 1, MinTimeNs: 1, MaxTimeNs: 2,
			})
			f.manifest.RemoveFile("dt=2026-03-01/hour=07", key)
		}
	}()

	// Two rewriters racing the same tombstone.
	wg.Add(2)
	for _, s := range []*RewriteScheduler{f.sched, other} {
		go func(s *RewriteScheduler) {
			defer wg.Done()
			s.RunOnce(context.Background())
		}(s)
	}

	// Let the rewrites finish, then stop the readers.
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Losing the race must not corrupt anything: whichever rewriter got there
	// first published its replacement, the other found the key already
	// superseded (or failed cleanly and retried).
	f.sched.RunOnce(context.Background())
	f.assertConverged(t, "after concurrent rewrites, reads and compaction")
}

// --- property-style -----------------------------------------------------------

// TestRewriteProperties_RandomSequences drives random delete/rewrite/restart
// sequences and checks the invariants after every step.
//
// The value over the hand-written cases is ordering: a hand-written test picks
// one interleaving, and the bugs here lived in the interleavings nobody picked
// (a restart between the swap and the bookkeeping; a second delete landing on a
// file a previous delete had already replaced).
func TestRewriteProperties_RandomSequences(t *testing.T) {
	const seeds = 24
	for seed := int64(0); seed < seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			runRandomDeleteSequence(t, rng)
		})
	}
}

func runRandomDeleteSequence(t *testing.T, rng *rand.Rand) {
	t.Helper()

	const files = 3
	severities := []string{"info", "warn", "error", "debug"}

	pool := newMockRewriterPool()
	rowsByKey := map[string]int64{}
	// aliveBodies tracks, independently of the code under test, which rows
	// should still exist. It is the oracle every assertion compares against.
	aliveBodies := map[string]string{} // body -> severity
	for i := 0; i < files; i++ {
		key := fmt.Sprintf("logs/dt=2026-04-%02d/hour=00/f%02d.parquet", i+1, i)
		n := 3 + rng.Intn(4)
		rows := make([]schema.LogRow, 0, n)
		for j := 0; j < n; j++ {
			sev := severities[rng.Intn(len(severities))]
			body := fmt.Sprintf("f%d-r%d", i, j)
			rows = append(rows, schema.LogRow{
				TimestampUnixNano: int64(1000 + j*100),
				Body:              body,
				SeverityText:      sev,
				ServiceName:       "svc",
			})
			aliveBodies[body] = sev
		}
		pool.Put(key, buildTestParquet(t, rows))
		rowsByKey[key] = int64(len(rows))
	}

	m := newTestManifest(t, rowsByKey)
	store := NewTombstoneStore()
	newSched := func() *RewriteScheduler {
		return NewRewriteScheduler(RewriteSchedulerConfig{
			Store:          store,
			Rewriter:       NewRewriter(pool, "logs/", 1000, "logs"),
			Detector:       NewStorageClassDetector(nil),
			RewriteDelay:   0,
			AllowedClasses: []string{"STANDARD"},
			Manifest:       m,
		})
	}
	sched := newSched()

	check := func(stage string) {
		t.Helper()
		storageinvariants.Assert(t, stage, storageinvariants.State{
			Manifest: m, Bucket: pool, Tombstones: tombstoneViews(store),
		})
		scanned := scanLogRows(t, pool)
		if got, want := storageinvariants.ManifestRows(m), int64(len(scanned)); got != want {
			t.Fatalf("%s: manifest claims %d rows, scan finds %d", stage, got, want)
		}
		present := map[string]bool{}
		for i := range scanned {
			present[scanned[i].Body] = true
		}
		for body := range aliveBodies {
			if !present[body] {
				t.Fatalf("%s: row %q that no delete covered is gone", stage, body)
			}
		}
		for body := range present {
			if _, ok := aliveBodies[body]; !ok {
				t.Fatalf("%s: deleted row %q came back", stage, body)
			}
		}
	}

	check("initial")

	for step := 0; step < 8; step++ {
		switch rng.Intn(4) {
		case 0, 1:
			// Issue a delete over a random severity, covering every file that
			// still exists.
			sev := severities[rng.Intn(len(severities))]
			keys := pool.Keys()
			if len(keys) == 0 {
				continue
			}
			store.Add(Tombstone{
				Tenants:      []TenantRef{{}},
				ID:           fmt.Sprintf("ts-%d", step),
				Query:        fmt.Sprintf(`severity_text:=%q`, sev),
				StartNs:      0,
				EndNs:        1 << 40,
				AffectedKeys: keys,
				CreatedAt:    time.Now().Add(-time.Hour),
				Mode:         "permanent",
				Reaped:       map[string]bool{},
			})
			// The oracle: those rows are on their way out. They stay readable
			// until the rewrite runs, so remove them from the oracle only when
			// the scheduler has actually run — do it here and run immediately.
			sched.RunOnce(context.Background())
			for body, s := range aliveBodies {
				if s == sev {
					delete(aliveBodies, body)
				}
			}
			check(fmt.Sprintf("step %d: delete %s", step, sev))

		case 2:
			// Restart: a fresh scheduler over the same store and manifest.
			sched = newSched()
			sched.RunOnce(context.Background())
			check(fmt.Sprintf("step %d: restart", step))

		case 3:
			// Idle tick.
			sched.RunOnce(context.Background())
			check(fmt.Sprintf("step %d: idle tick", step))
		}
	}
}

// --- helpers ------------------------------------------------------------------

// assertKeptRowsReadable checks the weakest property that must survive every
// crash: every row a delete did NOT cover is still in some object.
func assertKeptRowsReadable(t *testing.T, f *rewriteFixture, stage string) {
	t.Helper()
	present := map[string]bool{}
	for _, row := range scanLogRows(t, f.pool) {
		present[row.Body] = true
	}
	for body := range f.keptBodies {
		if !present[body] {
			t.Fatalf("%s: kept row %q exists in no object — unrecoverable", stage, body)
		}
	}
}
