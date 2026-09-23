package delete

import (
	"bytes"
	"context"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A replacement key is a directory plus 8 hex characters. The odds of drawing
// one that is already in use are tiny but not zero, and the consequence is not
// tiny: an upload to a key the manifest already serves overwrites a live file
// with a rewritten copy of a different file's rows. So the key is CLAIMED
// before anything is written, the draw is repeated on a collision, and the
// manifest refuses to publish onto a key it already has.
//
// These tests force the collision, which is the only way to exercise the guard.

// withReplacementIDs makes newReplacementID hand out the given ids in order,
// repeating the last one forever.
func withReplacementIDs(t *testing.T, ids ...string) {
	t.Helper()
	old := newReplacementID
	i := 0
	newReplacementID = func() string {
		id := ids[i]
		if i < len(ids)-1 {
			i++
		}
		return id
	}
	t.Cleanup(func() { newReplacementID = old })
}

// collisionWorld is one source file to rewrite plus one bystander file whose
// key the rewrite will try to take.
type collisionWorld struct {
	pool      *mockRewriterPool
	manifest  *lhmanifest.Manifest
	store     *TombstoneStore
	sched     *RewriteScheduler
	source    string
	bystander string
	// bystanderBytes is the object as it was before the rewrite ran.
	bystanderBytes []byte
}

const collisionID = "deadbeef"

func newCollisionWorld(t *testing.T) *collisionWorld {
	t.Helper()
	const dir = "logs/dt=2026-03-01/hour=07/"
	source := dir + "src-0001.parquet"
	bystander := dir + collisionID + ".parquet"

	srcRows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep-a", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "drop-a", SeverityText: "error", ServiceName: "web"},
	}
	otherRows := []schema.LogRow{
		{TimestampUnixNano: 3000, Body: "bystander-1", SeverityText: "info", ServiceName: "db"},
		{TimestampUnixNano: 4000, Body: "bystander-2", SeverityText: "info", ServiceName: "db"},
	}

	pool := newMockRewriterPool()
	pool.Put(source, buildTestParquet(t, srcRows))
	pool.Put(bystander, buildTestParquet(t, otherRows))

	m := newTestManifest(t, map[string]int64{
		source:    int64(len(srcRows)),
		bystander: int64(len(otherRows)),
	})

	store := NewTombstoneStore()
	store.Add(Tombstone{
		Tenants: []TenantRef{{}},
		ID:      "ts-collision", Query: `severity_text:="error"`,
		StartNs: 0, EndNs: 1 << 40, AffectedKeys: []string{source},
		CreatedAt: time.Now().Add(-2 * time.Hour), Mode: "permanent", Reaped: map[string]bool{},
	})

	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       NewRewriter(pool, "logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       m,
	})

	return &collisionWorld{
		pool: pool, manifest: m, store: store, sched: sched,
		source: source, bystander: bystander,
		bystanderBytes: mustGet(t, pool, bystander),
	}
}

func (w *collisionWorld) assertBystanderIntact(t *testing.T, stage string) {
	t.Helper()
	got, ok := w.pool.Get(w.bystander)
	if !ok {
		t.Fatalf("%s: the bystander object %s is gone", stage, w.bystander)
	}
	if !bytes.Equal(got, w.bystanderBytes) {
		t.Fatalf("%s: the bystander object %s was overwritten (%d bytes, was %d)",
			stage, w.bystander, len(got), len(w.bystanderBytes))
	}
	if !w.manifest.HasKey(w.bystander) {
		t.Fatalf("%s: the manifest stopped serving the bystander %s", stage, w.bystander)
	}
	rows := readRowsAt(t, w.pool, w.bystander)
	if len(rows) != 2 || rows[0].Body != "bystander-1" {
		t.Fatalf("%s: the bystander object holds %d rows starting %q", stage, len(rows), bodyOf(rows))
	}
}

func readRowsAt(t *testing.T, pool *mockRewriterPool, key string) []schema.LogRow {
	t.Helper()
	single := newMockRewriterPool()
	single.Put(key, mustGet(t, pool, key))
	return scanLogRows(t, single)
}

func bodyOf(rows []schema.LogRow) string {
	if len(rows) == 0 {
		return ""
	}
	return rows[0].Body
}

// TestRewrite_ReplacementKeyCollisionRedrawsAndNeverOverwrites: the first draw
// names a file the manifest already serves. The claim rejects it, the scheduler
// draws again, and the live file is never touched.
func TestRewrite_ReplacementKeyCollisionRedrawsAndNeverOverwrites(t *testing.T) {
	w := newCollisionWorld(t)
	withReplacementIDs(t, collisionID, "0badc0de")

	before := metrics.DeleteRewriteKeyCollisions.Get()
	results := w.sched.RunOnce(context.Background())
	if len(results) != 1 {
		t.Fatalf("expected the rewrite to finish on the second key, got %d results", len(results))
	}
	if got := results[0].NewKey; got == w.bystander {
		t.Fatalf("the rewrite published onto the bystander's key %s", got)
	}
	if metrics.DeleteRewriteKeyCollisions.Get() == before {
		t.Error("the collision was not counted")
	}
	w.assertBystanderIntact(t, "after the redraw")
	if w.pool.Has(w.source) {
		t.Errorf("the superseded source %s was not deleted although the rewrite finished", w.source)
	}
	if !w.manifest.HasKey(results[0].NewKey) {
		t.Errorf("the manifest does not serve the replacement %s", results[0].NewKey)
	}
}

// TestRewrite_ReplacementKeyCollisionThatNeverClearsAbandonsTheRewrite: every
// draw collides. The rewrite must give up with everything untouched rather than
// write over the file it keeps hitting.
func TestRewrite_ReplacementKeyCollisionThatNeverClearsAbandonsTheRewrite(t *testing.T) {
	w := newCollisionWorld(t)
	withReplacementIDs(t, collisionID)

	results := w.sched.RunOnce(context.Background())
	if len(results) != 0 {
		t.Fatalf("the rewrite reported %d results although it could not find a free key", len(results))
	}
	w.assertBystanderIntact(t, "after the exhausted redraws")
	if !w.pool.Has(w.source) {
		t.Fatalf("the source %s was deleted although no replacement was ever written", w.source)
	}
	ts, ok := w.store.Get("ts-collision")
	if !ok {
		t.Fatal("the tombstone retired although its file was never rewritten")
	}
	if ts.Reaped[w.source] {
		t.Errorf("%s was recorded as handled although the rewrite never ran", w.source)
	}
	if len(ts.Superseded) != 0 {
		t.Errorf("a rewrite record was left behind for a rewrite that never started: %+v", ts.Superseded)
	}
}

// TestClaimPending_RejectsEveryKeyAlreadyInUse is the claim's own contract: a
// registered, retired or already-claimed key is never handed out, so two
// writers can never be told they own the same key.
func TestClaimPending_RejectsEveryKeyAlreadyInUse(t *testing.T) {
	const registered = "logs/dt=2026-03-01/hour=07/registered.parquet"
	const retired = "logs/dt=2026-03-01/hour=07/retired.parquet"
	const claimed = "logs/dt=2026-03-01/hour=07/claimed.parquet"

	m := newTestManifest(t, map[string]int64{registered: 1, retired: 1})
	m.Retire(retired, "", true)
	if !m.ClaimPending(claimed) {
		t.Fatal("fixture: the first claim of a free key must succeed")
	}

	for _, tc := range []struct{ name, key string }{
		{"registered", registered},
		{"retired", retired},
		{"already claimed", claimed},
		{"empty", ""},
	} {
		if m.ClaimPending(tc.key) {
			t.Errorf("ClaimPending(%s) succeeded for a key that is %s", tc.key, tc.name)
		}
	}

	// And the claim is given back when nothing was written.
	m.ReleasePending(claimed)
	if !m.ClaimPending(claimed) {
		t.Error("a released claim must be claimable again")
	}
}
