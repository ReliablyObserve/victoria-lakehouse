package delete

import (
	"context"
	"errors"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil/storageinvariants"
)

// --- fixture -----------------------------------------------------------------

// rewriteFixture is one file in one bucket with one manifest entry and one
// tombstone over it: the smallest setup in which the rewrite can go wrong.
type rewriteFixture struct {
	pool     *mockRewriterPool
	fault    *faultPool
	manifest *lhmanifest.Manifest
	wrapped  *failingManifest
	store    *TombstoneStore
	sched    *RewriteScheduler
	key      string
	rows     []schema.LogRow
	// keptBodies / deletedBodies are the ground truth the invariants are
	// checked against: no kept body may ever disappear, no deleted body may
	// ever come back.
	keptBodies    map[string]bool
	deletedBodies map[string]bool
}

const fixtureKey = "logs/dt=2026-03-01/hour=07/src-0001.parquet"

func newRewriteFixture(t *testing.T) *rewriteFixture {
	t.Helper()

	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep-a", SeverityText: "info", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "drop-a", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 3000, Body: "keep-b", SeverityText: "info", ServiceName: "api"},
		{TimestampUnixNano: 4000, Body: "drop-b", SeverityText: "error", ServiceName: "api"},
		{TimestampUnixNano: 5000, Body: "keep-c", SeverityText: "warn", ServiceName: "web"},
	}

	pool := newMockRewriterPool()
	pool.Put(fixtureKey, buildTestParquet(t, rows))
	fault := newFaultPool(pool)

	m := newTestManifest(t, map[string]int64{fixtureKey: int64(len(rows))})
	wrapped := wrapManifest(m)

	store := NewTombstoneStore()
	store.Add(Tombstone{
		ID:           "ts-fixture",
		Query:        `severity_text:="error"`,
		StartNs:      0,
		EndNs:        10000,
		AffectedKeys: []string{fixtureKey},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "permanent",
		Reaped:       map[string]bool{},
	})

	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       NewRewriter(fault, "logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		Manifest:       wrapped,
	})

	return &rewriteFixture{
		pool: pool, fault: fault, manifest: m, wrapped: wrapped,
		store: store, sched: sched, key: fixtureKey, rows: rows,
		keptBodies:    map[string]bool{"keep-a": true, "keep-b": true, "keep-c": true},
		deletedBodies: map[string]bool{"drop-a": true, "drop-b": true},
	}
}

func (f *rewriteFixture) state() storageinvariants.State {
	return storageinvariants.State{Manifest: f.manifest, Bucket: f.pool, Tombstones: tombstoneViews(f.store)}
}

// assertConverged is the invariant set every scheduler run must leave behind,
// checked as a whole rather than one assertion per test:
//
//	I1  every manifest entry has an object behind it
//	I2  every object is manifested (nothing for the orphan sweep to reclaim)
//	I3  no entry claims rows or aggregates it cannot have
//	I4  no fully reaped tombstone is still Active()
//	R1  the manifest's summed RowCount equals a full scan of the bucket
//	R2  every kept row is still readable
//	R3  no deleted row is readable
func (f *rewriteFixture) assertConverged(t *testing.T, stage string) {
	t.Helper()
	storageinvariants.Assert(t, stage, f.state())

	scanned := scanLogRows(t, f.pool)
	if got, want := storageinvariants.ManifestRows(f.manifest), int64(len(scanned)); got != want {
		t.Fatalf("%s: manifest claims %d rows, a full scan finds %d", stage, got, want)
	}

	present := map[string]bool{}
	for i := range scanned {
		present[scanned[i].Body] = true
	}
	for body := range f.keptBodies {
		if !present[body] {
			t.Fatalf("%s: kept row %q is gone from storage", stage, body)
		}
	}
	for body := range f.deletedBodies {
		if present[body] {
			t.Fatalf("%s: deleted row %q came back", stage, body)
		}
	}
}

// --- Bug 1: the rewrite never reached the manifest ---------------------------

func TestRewrite_PublishesReplacementIntoManifest(t *testing.T) {
	f := newRewriteFixture(t)

	results := f.sched.RunOnce(context.Background())
	if len(results) != 1 {
		t.Fatalf("expected 1 rewrite result, got %d", len(results))
	}
	res := results[0]

	if f.manifest.HasKey(f.key) {
		t.Fatalf("manifest still points at the superseded key %s — this is the original bug: "+
			"queries 404 on it and either skip the kept rows or synthesise the deleted ones", f.key)
	}
	if !f.manifest.HasKey(res.NewKey) {
		t.Fatalf("manifest does not know the replacement %s — the orphan sweep would delete it, "+
			"taking the kept rows with it", res.NewKey)
	}

	fi, ok := f.manifest.GetFileByKey(res.NewKey)
	if !ok {
		t.Fatal("replacement entry missing")
	}
	if fi.RowCount != res.RowsKept {
		t.Fatalf("replacement RowCount = %d, want RowsKept = %d", fi.RowCount, res.RowsKept)
	}
	if fi.RowCount != 3 {
		t.Fatalf("replacement RowCount = %d, want 3 kept rows", fi.RowCount)
	}
	if fi.Size != res.BytesAfter || fi.Size <= 0 {
		t.Fatalf("replacement Size = %d, want BytesAfter = %d", fi.Size, res.BytesAfter)
	}
	// Time bounds must describe the KEPT rows. The dropped rows sat at 2000 and
	// 4000, so an inherited bound would still be [1000, 5000] here by accident —
	// what matters is that they are recomputed and consistent.
	if fi.MinTimeNs != 1000 || fi.MaxTimeNs != 5000 {
		t.Fatalf("replacement time bounds = [%d, %d], want [1000, 5000]", fi.MinTimeNs, fi.MaxTimeNs)
	}

	f.assertConverged(t, "after first run")
}

func TestRewrite_LabelAggregatesExcludeDeletedRows(t *testing.T) {
	f := newRewriteFixture(t)

	results := f.sched.RunOnce(context.Background())
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	fi, ok := f.manifest.GetFileByKey(results[0].NewKey)
	if !ok {
		t.Fatal("replacement entry missing")
	}

	// `stats count() by (severity_text)` is answered from LabelAggregates
	// WITHOUT opening the file. Carrying the old file's aggregates forward
	// would keep reporting the deleted rows in every dashboard that uses one.
	agg := fi.LabelAggregates["severity_text"]
	if agg == nil {
		t.Fatal("replacement has no severity_text aggregate; the count-pushdown path would fall back forever")
	}
	if n := agg["error"]; n != 0 {
		t.Fatalf("aggregate still counts %d deleted error rows", n)
	}
	if n := agg["info"]; n != 2 {
		t.Fatalf("aggregate counts %d info rows, want 2", n)
	}

	var sum int64
	for _, c := range agg {
		sum += c
	}
	if sum != fi.RowCount {
		t.Fatalf("aggregate sums to %d but RowCount is %d", sum, fi.RowCount)
	}
}

func TestRewrite_AllRowsRemovedDropsTheEntry(t *testing.T) {
	f := newRewriteFixture(t)
	// Widen the tombstone to cover every row.
	ts, _ := f.store.Get("ts-fixture")
	ts.Query = "*"
	f.store.Add(ts)
	f.keptBodies = map[string]bool{}
	for i := range f.rows {
		f.deletedBodies[f.rows[i].Body] = true
	}

	results := f.sched.RunOnce(context.Background())
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].RowsKept != 0 {
		t.Fatalf("expected 0 rows kept, got %d", results[0].RowsKept)
	}
	if results[0].NewKey != "" {
		t.Fatalf("no replacement object should be written when nothing survives, got %s", results[0].NewKey)
	}
	if f.manifest.HasKey(f.key) {
		t.Fatal("manifest must drop the entry when the whole file is deleted")
	}
	if f.pool.Count() != 0 {
		t.Fatalf("bucket should be empty, holds %d objects", f.pool.Count())
	}
	f.assertConverged(t, "all rows removed")
}

func TestRewrite_RefusedWithoutAManifest(t *testing.T) {
	f := newRewriteFixture(t)
	// A scheduler with no manifest cannot publish, so rewriting would orphan
	// the replacement and strand the manifest on a deleted key. Refusing is the
	// safe state: the rows stay hidden by the query-time filter.
	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          f.store,
		Rewriter:       NewRewriter(f.pool, "logs/", 1000, "logs"),
		Detector:       NewStorageClassDetector(nil),
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
	})

	before := metrics.DeleteRewriteSkippedNoManifest.Get()
	results := sched.RunOnce(context.Background())

	if len(results) != 0 {
		t.Fatalf("expected no rewrites without a manifest, got %d", len(results))
	}
	if metrics.DeleteRewriteSkippedNoManifest.Get() <= before {
		t.Error("expected the refusal to be counted")
	}
	// The refusal must leave storage exactly as it was: the source object
	// intact, the manifest pointing at it, the tombstone still active so the
	// query-time filter keeps hiding the rows. That is the SAFE state — the
	// rows are hidden but not yet removed — and it is what makes refusing
	// better than rewriting blind.
	if !f.pool.Has(f.key) {
		t.Fatal("the source object must be untouched")
	}
	if !f.manifest.HasKey(f.key) {
		t.Fatal("the manifest entry must be untouched")
	}
	if _, still := f.store.Get("ts-fixture"); !still {
		t.Fatal("the tombstone must stay active so the query filter keeps hiding the rows")
	}
	storageinvariants.Assert(t, "refused without manifest", f.state())
	if got, want := storageinvariants.ManifestRows(f.manifest), int64(len(f.rows)); got != want {
		t.Fatalf("manifest claims %d rows, want the untouched %d", got, want)
	}
}

func TestRewrite_CompletesTheTombstoneAndStopsRunning(t *testing.T) {
	f := newRewriteFixture(t)

	f.sched.RunOnce(context.Background())
	if _, still := f.store.Get("ts-fixture"); still {
		t.Fatal("a fully reaped tombstone must be retired, not left active forever")
	}

	// A second tick must find nothing to do and must not disturb storage.
	keysBefore := f.pool.Keys()
	if n := len(f.sched.RunOnce(context.Background())); n != 0 {
		t.Fatalf("second run did %d rewrites, want 0", n)
	}
	if got := f.pool.Keys(); len(got) != len(keysBefore) {
		t.Fatalf("second run changed the bucket: %v -> %v", keysBefore, got)
	}
	f.assertConverged(t, "after idempotent second run")
}

func TestRewrite_SelfHealsKeyAlreadySuperseded(t *testing.T) {
	f := newRewriteFixture(t)

	// Simulate a crash between the manifest swap and the reaped bookkeeping:
	// the key is gone from the manifest but the tombstone still lists it as
	// pending. Retrying its download would fail forever.
	f.manifest.RemoveFile(extractPartition(f.key), f.key)
	_ = f.pool.Delete(context.Background(), f.key)
	f.keptBodies = map[string]bool{}
	f.deletedBodies = map[string]bool{}

	before := metrics.DeleteRewriteErrors.Get()
	f.sched.RunOnce(context.Background())

	if metrics.DeleteRewriteErrors.Get() != before {
		t.Error("a key that is already superseded must not be counted as a rewrite error")
	}
	if _, still := f.store.Get("ts-fixture"); still {
		t.Fatal("the tombstone should have been completed by the self-healing path")
	}
	f.assertConverged(t, "self-healed")
}

// --- publishRewrite unit coverage -------------------------------------------

func TestPublishRewrite_RejectsAFailedSwap(t *testing.T) {
	f := newRewriteFixture(t)
	f.wrapped.failReplace = true

	res := &RewriteResult{
		OldKey:      f.key,
		NewKey:      "logs/dt=2026-03-01/hour=07/new.parquet",
		RowsKept:    3,
		RowsRemoved: 2,
		BytesAfter:  100,
	}
	fi, err := publishRewrite(f.wrapped, res)
	if err == nil {
		t.Fatal("a swap that did not take effect must be reported, not assumed")
	}
	if res.Published {
		t.Fatal("a failed publish must not mark the result published — Commit would then delete the only copy")
	}
	if fi != nil {
		t.Fatalf("a failed publish must not report a registered entry, got %+v", fi)
	}
}

func TestPublishRewrite_RequiresAManifest(t *testing.T) {
	if _, err := publishRewrite(nil, &RewriteResult{OldKey: "k", RowsRemoved: 1}); err == nil {
		t.Fatal("publishing without a manifest must be an error")
	}
}

// TestPublishRewrite_RefusesASupersededSource covers the concurrent-compaction
// case: the source left the manifest between the rewrite's read and its
// publish. Registering the replacement anyway would put the kept rows in the
// manifest twice (the replacement and the compacted output) and bring the
// deleted rows back through the compacted copy.
func TestPublishRewrite_RefusesASupersededSource(t *testing.T) {
	m := lhmanifest.New("b", "")
	res := &RewriteResult{
		OldKey:      fixtureKey,
		NewKey:      "logs/dt=2026-03-01/hour=07/new.parquet",
		RowsKept:    1,
		RowsRemoved: 1,
		BytesAfter:  10,
	}
	fi, err := publishRewrite(m, res)
	if !errors.Is(err, errSourceSuperseded) {
		t.Fatalf("publish of a source that is not registered must report supersession, got %v", err)
	}
	if fi != nil || res.Published {
		t.Fatal("a superseded rewrite must not be registered or marked published")
	}
	if m.HasKey(res.NewKey) {
		t.Fatal("the replacement must not enter the manifest")
	}

	// The every-row-removed path has the same guard.
	res0 := &RewriteResult{OldKey: fixtureKey, RowsKept: 0, RowsRemoved: 3}
	if _, err := publishRewrite(m, res0); !errors.Is(err, errSourceSuperseded) {
		t.Fatalf("an all-rows-removed publish of an unregistered source must report supersession, got %v", err)
	}
}

// TestPublishRewrite_RefusalWithTheSourceStillPresentIsAFailure pins the
// classification the scheduler depends on. Only a verifiably-gone source may be
// treated as superseded (key marked reaped, tombstone follows the rows). A
// refusal while the source is still registered is a failure to retry — marking
// that key reaped would retire the tombstone with its rows still in the source.
func TestPublishRewrite_RefusalWithTheSourceStillPresentIsAFailure(t *testing.T) {
	f := newRewriteFixture(t)
	f.wrapped.failReplace = true

	res := &RewriteResult{OldKey: f.key, NewKey: "logs/dt=2026-03-01/hour=07/n.parquet", RowsKept: 3, RowsRemoved: 2, BytesAfter: 5}
	_, err := publishRewrite(f.wrapped, res)
	if err == nil || errors.Is(err, errSourceSuperseded) {
		t.Fatalf("a refusal with the source still registered must be a plain failure, got %v", err)
	}

	f.wrapped.failRemove = true
	res0 := &RewriteResult{OldKey: f.key, RowsKept: 0, RowsRemoved: 5}
	_, err = publishRewrite(f.wrapped, res0)
	if err == nil || errors.Is(err, errSourceSuperseded) {
		t.Fatalf("a refused removal with the source still registered must be a plain failure, got %v", err)
	}
}

// --- Manifest.ReplaceFile ----------------------------------------------------

func TestManifestReplaceFile_IsASingleSwap(t *testing.T) {
	m := newTestManifest(t, map[string]int64{fixtureKey: 5})

	newFI := lhmanifest.FileInfo{Key: "logs/dt=2026-03-01/hour=07/new.parquet", Size: 10, RowCount: 3, MinTimeNs: 1, MaxTimeNs: 2}
	if !m.ReplaceFile("dt=2026-03-01/hour=07", fixtureKey, newFI) {
		t.Fatal("ReplaceFile should report that it replaced an existing entry")
	}
	if m.HasKey(fixtureKey) {
		t.Fatal("old key still present")
	}
	if !m.HasKey(newFI.Key) {
		t.Fatal("new key absent")
	}
	if got := storageinvariants.ManifestRows(m); got != 3 {
		t.Fatalf("manifest rows = %d, want 3", got)
	}

	// Replacing a key that is not there changes nothing: the swap is
	// conditional so a rewrite racing a compaction can never register a second
	// copy of the source's rows.
	if m.ReplaceFile("dt=2026-03-01/hour=07", "logs/dt=2026-03-01/hour=07/absent.parquet",
		lhmanifest.FileInfo{Key: "logs/dt=2026-03-01/hour=07/n2.parquet", RowCount: 1}) {
		t.Fatal("ReplaceFile should report false when the old key was absent")
	}
	if m.HasKey("logs/dt=2026-03-01/hour=07/n2.parquet") {
		t.Fatal("a refused swap must not register the replacement")
	}
}

func TestManifestPartitionForKey(t *testing.T) {
	m := newTestManifest(t, map[string]int64{fixtureKey: 1})
	p, ok := m.PartitionForKey(fixtureKey)
	if !ok || p != "dt=2026-03-01/hour=07" {
		t.Fatalf("PartitionForKey = %q, %v", p, ok)
	}
	if _, ok := m.PartitionForKey("nope"); ok {
		t.Fatal("unknown key must report false")
	}
}

// --- rewrite metadata fidelity ----------------------------------------------

func TestRewrite_PreservesTheDedicatedSlotFooterBinding(t *testing.T) {
	// The read path resolves ded_sNN columns by each file's OWN footer KV and
	// skips the raw slot column when the key is absent — a replacement that
	// dropped the binding would serve its kept rows with the promoted
	// attributes missing.
	src := buildTestParquetWithSlots(t, []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep", SeverityText: "info"},
		{TimestampUnixNano: 2000, Body: "drop", SeverityText: "error"},
	}, `{"ded_s01":"tenant.tier"}`)

	pool := newMockRewriterPool()
	key := "logs/dt=2026-03-02/hour=01/slots.parquet"
	pool.Put(key, src)

	rw := NewRewriter(pool, "logs/", 100, "logs")
	res, err := rw.RewriteFile(context.Background(), key, []Tombstone{
		{ID: "t", Query: `severity_text:="error"`, StartNs: 0, EndNs: 9000},
	})
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}
	newData := mustGet(t, pool, res.NewKey)
	if got := string(sourceSlotMapping(newData)); got != `{"ded_s01":"tenant.tier"}` {
		t.Fatalf("replacement lost the dedicated-slot binding: %q", got)
	}
}

func TestRewrite_RecordsFooterDerivedSizes(t *testing.T) {
	f := newRewriteFixture(t)
	results := f.sched.RunOnce(context.Background())
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	fi, _ := f.manifest.GetFileByKey(results[0].NewKey)
	if len(fi.ColumnBytes) == 0 {
		t.Error("replacement has no per-column byte accounting; the per-field storage report goes blank after any delete")
	}
	if fi.RawBytes <= 0 {
		t.Error("replacement has no RawBytes; its compression ratio is unreportable")
	}
	// RawBytes must describe the kept rows only — the whole file's raw size
	// would overstate it.
	full := schema.EstimateRawBytesLogs(f.rows)
	if fi.RawBytes >= full {
		t.Errorf("RawBytes %d not reduced below the pre-delete %d", fi.RawBytes, full)
	}
}

func TestRewrittenFileInfo_InheritsWhatARewriteCannotChange(t *testing.T) {
	old := lhmanifest.FileInfo{
		Key:               "old",
		Bucket:            "other-bucket",
		SchemaFingerprint: "fp-1",
		CompactionLevel:   2,
		StorageClass:      "STANDARD",
		ClassSource:       "head",
		Labels:            map[string][]string{"service.name": {"web", "api"}},
		ColumnStats:       map[string]lhmanifest.ColumnMinMax{"x": {}},
	}
	res := &RewriteResult{
		NewKey: "new", RowsKept: 2, BytesAfter: 7, RawBytes: 9,
		Labels: map[string][]string{"service.name": {"web"}},
	}

	fi := rewrittenFileInfo(old, res)
	for _, tc := range []struct{ name, got, want string }{
		{"bucket", fi.Bucket, old.Bucket},
		{"schema fingerprint", fi.SchemaFingerprint, old.SchemaFingerprint},
		{"storage class", fi.StorageClass, old.StorageClass},
		{"class source", fi.ClassSource, old.ClassSource},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want inherited %q", tc.name, tc.got, tc.want)
		}
	}
	if fi.CompactionLevel != old.CompactionLevel {
		t.Errorf("compaction level = %d, want inherited %d", fi.CompactionLevel, old.CompactionLevel)
	}
	// Labels are RECOMPUTED, never inherited. They feed the pmeta field
	// catalog, which field_values serves verbatim: the old file's "api" value
	// belonged only to rows the rewrite removed, and inheriting it would put
	// the deleted value straight back into every dropdown.
	if got := fi.Labels["service.name"]; len(got) != 1 || got[0] != "web" {
		t.Errorf("labels = %v, want the recomputed [web] — an inherited superset resurrects deleted values", got)
	}
	// Column stats are inherited: a superset min/max is the safe direction
	// for range pruning, and stats are never served as values.
	if len(fi.ColumnStats) != 1 {
		t.Error("column stats must be inherited")
	}
	if fi.RowCount != 2 || fi.Size != 7 || fi.RawBytes != 9 {
		t.Errorf("recomputed fields not taken from the result: %+v", fi)
	}
	if fi.CreatedAt.IsZero() {
		t.Error("the replacement is a new object and needs a creation time")
	}
}

func TestExtractPartition(t *testing.T) {
	for _, tc := range []struct{ key, want string }{
		{"logs/dt=2026-01-01/hour=10/a.parquet", "dt=2026-01-01/hour=10"},
		{"1002/0/logs/dt=2026-01-01/hour=10/a.parquet", "dt=2026-01-01/hour=10"},
		{"logs/nopartition.parquet", "unknown"},
	} {
		if got := extractPartition(tc.key); got != tc.want {
			t.Errorf("extractPartition(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}
