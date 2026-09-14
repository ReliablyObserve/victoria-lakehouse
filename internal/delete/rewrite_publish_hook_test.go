package delete

import (
	"context"
	"errors"
	"sync"
	"testing"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// publishCall records one OnPublished invocation, plus the state of the bucket
// at the moment it fired.
type publishCall struct {
	added             []lhmanifest.FileInfo
	removed           []string
	blooms            map[string]map[string][]string
	oldObjectAtNotify bool
}

func recordPublishes(f *rewriteFixture) *[]publishCall {
	var mu sync.Mutex
	calls := &[]publishCall{}
	f.sched.onPublished = func(added []lhmanifest.FileInfo, removed []string, blooms map[string]map[string][]string) {
		mu.Lock()
		defer mu.Unlock()
		*calls = append(*calls, publishCall{
			added:             added,
			removed:           removed,
			blooms:            blooms,
			oldObjectAtNotify: f.pool.Has(f.key),
		})
	}
	return calls
}

// TestRewrite_OnPublishedFeedsFacetsAndPeersBeforeTheOldObjectGoes is the
// hand-off compaction's OnCompacted already does for its outputs. Without it
// the pmeta field catalog keeps the superseded file's values (a deleted value
// resurfaces in field_values once the tombstone is retired), and peers keep the
// superseded key and never learn the replacement.
func TestRewrite_OnPublishedFeedsFacetsAndPeersBeforeTheOldObjectGoes(t *testing.T) {
	f := newRewriteFixture(t)
	calls := recordPublishes(f)

	results := f.sched.RunOnce(context.Background())
	if len(results) != 1 {
		t.Fatalf("expected 1 rewrite, got %d", len(results))
	}
	res := results[0]

	if len(*calls) != 1 {
		t.Fatalf("OnPublished fired %d times, want once", len(*calls))
	}
	c := (*calls)[0]

	if len(c.removed) != 1 || c.removed[0] != f.key {
		t.Errorf("removed = %v, want [%s]", c.removed, f.key)
	}
	if len(c.added) != 1 || c.added[0].Key != res.NewKey {
		t.Fatalf("added = %v, want the replacement %s", c.added, res.NewKey)
	}
	if c.added[0].RowCount != res.RowsKept {
		t.Errorf("added RowCount = %d, want RowsKept %d", c.added[0].RowCount, res.RowsKept)
	}
	if len(c.blooms[res.NewKey]) == 0 {
		t.Error("the replacement's bloom values were not handed to the facet feed")
	}

	// Firing before the superseded object is deleted means a peer learns the
	// new key while the old one can still be read.
	if !c.oldObjectAtNotify {
		t.Error("OnPublished fired after the superseded object was deleted; peers would 404 on it in the gap")
	}
	f.assertConverged(t, "after publish hook")
}

// TestRewrite_PublishedLabelsExcludeValuesOnlyTheDeletedRowsHad pins the label
// set handed to the manifest and the facet feed. The pmeta catalog serves those
// values verbatim, so a superset inherited from the old file would put a
// deleted value straight back into every dropdown.
func TestRewrite_PublishedLabelsExcludeValuesOnlyTheDeletedRowsHad(t *testing.T) {
	f := newRewriteFixture(t)
	calls := recordPublishes(f)

	f.sched.RunOnce(context.Background())
	if len(*calls) != 1 || len((*calls)[0].added) != 1 {
		t.Fatalf("expected one published replacement, got %v", *calls)
	}
	fi := (*calls)[0].added[0]

	for _, v := range fi.Labels["severity_text"] {
		if v == "error" {
			t.Fatalf("published labels still carry %q, a value only the deleted rows had: %v", v, fi.Labels["severity_text"])
		}
	}
	got := map[string]bool{}
	for _, v := range fi.Labels["severity_text"] {
		got[v] = true
	}
	if !got["info"] || !got["warn"] {
		t.Errorf("published labels lost values the kept rows carry: %v", fi.Labels["severity_text"])
	}

	stored, ok := f.manifest.GetFileByKey(fi.Key)
	if !ok {
		t.Fatal("replacement not in the manifest")
	}
	for _, v := range stored.Labels["severity_text"] {
		if v == "error" {
			t.Fatalf("manifest labels still carry the deleted value: %v", stored.Labels["severity_text"])
		}
	}
}

func TestRewrite_OnPublishedWhenEveryRowIsRemoved(t *testing.T) {
	f := newRewriteFixture(t)
	ts, _ := f.store.Get("ts-fixture")
	ts.Query = "*"
	f.store.Add(ts)
	calls := recordPublishes(f)

	f.sched.RunOnce(context.Background())

	if len(*calls) != 1 {
		t.Fatalf("OnPublished fired %d times, want once", len(*calls))
	}
	c := (*calls)[0]
	if len(c.added) != 0 {
		t.Errorf("no replacement exists, but added = %v", c.added)
	}
	if len(c.removed) != 1 || c.removed[0] != f.key {
		t.Errorf("removed = %v, want [%s]", c.removed, f.key)
	}
	if c.blooms != nil {
		t.Errorf("no replacement, so no bloom values, got %v", c.blooms)
	}
}

func TestRewrite_OnPublishedIsNotFiredWhenPublishFails(t *testing.T) {
	f := newRewriteFixture(t)
	f.wrapped.failReplace = true
	calls := recordPublishes(f)

	f.sched.RunOnce(context.Background())

	// A failed publish means the manifest does not know the replacement.
	// Telling the facets or the peers about it would put them ahead of the
	// manifest they are supposed to mirror.
	if len(*calls) != 0 {
		t.Fatalf("OnPublished fired %d times for a publish that did not take effect", len(*calls))
	}
}

func TestNotifyPublished_GuardsAgainstUnpublishedResults(t *testing.T) {
	f := newRewriteFixture(t)
	calls := recordPublishes(f)

	f.sched.notifyPublished(nil, nil)
	f.sched.notifyPublished(nil, &RewriteResult{OldKey: "k", RowsRemoved: 1})
	if len(*calls) != 0 {
		t.Fatalf("an unpublished result must not be announced, got %v", *calls)
	}

	// A scheduler without the hook is valid (single instance, no pmeta).
	f.sched.onPublished = nil
	f.sched.notifyPublished(nil, &RewriteResult{OldKey: "k", RowsRemoved: 1, Published: true})
}

// --- injected writers ---------------------------------------------------------

func TestRewriter_UsesTheInjectedWriters(t *testing.T) {
	pool := newMockRewriterPool()
	key := "logs/dt=2026-09-02/hour=01/src.parquet"
	pool.Put(key, buildTestParquet(t, []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep", SeverityText: "info"},
		{TimestampUnixNano: 2000, Body: "drop", SeverityText: "error"},
	}))

	var gotRows int
	var gotRGS, gotLevel int
	rw := NewRewriter(pool, "logs/", 123, "logs", WithParquetWriters(ParquetWriters{
		Logs: func(rows []schema.LogRow, rowGroupSize, level int) ([]byte, error) {
			gotRows, gotRGS, gotLevel = len(rows), rowGroupSize, level
			return buildTestParquet(t, rows), nil
		},
		Traces: func([]schema.TraceRow, int, int) ([]byte, error) {
			t.Fatal("the traces writer must not be used for a logs rewrite")
			return nil, nil
		},
		CompressionLevel: 7,
	}))
	if !rw.HasProductionWriters() {
		t.Fatal("both writers are set")
	}

	res, err := rw.RewriteFile(context.Background(), key, []Tombstone{
		{ID: "t", Query: `severity_text:="error"`, StartNs: 0, EndNs: 9000},
	})
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}
	if gotRows != 1 || gotRGS != 123 || gotLevel != 7 {
		t.Fatalf("writer got rows=%d rowGroupSize=%d level=%d, want 1/123/7", gotRows, gotRGS, gotLevel)
	}
	if res.RowsKept != 1 {
		t.Fatalf("kept %d rows, want 1", res.RowsKept)
	}
}

func TestRewriter_UsesTheInjectedTraceWriter(t *testing.T) {
	pool := newMockRewriterPool()
	key := "traces/dt=2026-09-02/hour=01/src.parquet"
	pool.Put(key, buildTestTraceParquet(t, []schema.TraceRow{
		{TimestampUnixNano: 1000, TraceID: "a", SpanID: "1", ServiceName: "keep"},
		{TimestampUnixNano: 2000, TraceID: "b", SpanID: "2", ServiceName: "drop"},
	}))

	var called bool
	rw := NewRewriter(pool, "traces/", 100, "traces", WithParquetWriters(ParquetWriters{
		Traces: func(rows []schema.TraceRow, _, _ int) ([]byte, error) {
			called = true
			return buildTestTraceParquet(t, rows), nil
		},
	}))
	if rw.HasProductionWriters() {
		t.Error("only one writer is set; this must not count as a production configuration")
	}
	if _, err := rw.RewriteFile(context.Background(), key, []Tombstone{
		{ID: "t", Query: `service.name:="drop"`, StartNs: 0, EndNs: 9000},
	}); err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}
	if !called {
		t.Fatal("the injected traces writer was not used")
	}
}

// TestRewriteCrash_WriterFails extends the crash matrix to the step before the
// upload: producing the replacement's bytes. A failure there must leave the
// source intact and manifested, upload nothing, and retry cleanly.
func TestRewriteCrash_WriterFails(t *testing.T) {
	f := newRewriteFixture(t)

	var failures int
	f.sched.rewriter = NewRewriter(f.fault, "logs/", 1000, "logs", WithParquetWriters(ParquetWriters{
		Logs: func(rows []schema.LogRow, _, _ int) ([]byte, error) {
			if failures == 0 {
				failures++
				return nil, errors.New("encoder failed")
			}
			return buildTestParquet(t, rows), nil
		},
		Traces: func([]schema.TraceRow, int, int) ([]byte, error) { return nil, nil },
	}))

	keysBefore := f.pool.Keys()
	if n := len(f.sched.RunOnce(context.Background())); n != 0 {
		t.Fatalf("a writer failure must abandon the rewrite, got %d results", n)
	}
	if got := f.pool.Keys(); len(got) != len(keysBefore) {
		t.Fatalf("a writer failure must upload nothing: %v -> %v", keysBefore, got)
	}
	if !f.manifest.HasKey(f.key) {
		t.Fatal("the source must stay manifested after a writer failure")
	}
	assertKeptRowsReadable(t, f, "writer failed")

	f.sched.RunOnce(context.Background())
	f.assertConverged(t, "writer failed / after retry")
}
