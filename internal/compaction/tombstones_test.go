package compaction

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// compactionTombstoneFixture is two small log files in one partition, both
// listed by one permanent tombstone, ready to be merged.
type compactionTombstoneFixture struct {
	pool     *mockPool
	manifest *manifest.Manifest
	store    *delete.TombstoneStore
	files    []manifest.FileInfo
	keys     []string
}

func newCompactionTombstoneFixture(t *testing.T, mode string) *compactionTombstoneFixture {
	t.Helper()

	pool := newMockPool()
	m := manifest.New("test-bucket", "")
	partition := "dt=2026-07-01/hour=03"

	var keys []string
	var files []manifest.FileInfo
	for i, batch := range [][]schema.LogRow{
		{
			{TimestampUnixNano: 1000, Body: "keep-1", SeverityText: "info", ServiceName: "web"},
			{TimestampUnixNano: 1100, Body: "drop-1", SeverityText: "error", ServiceName: "web"},
		},
		{
			{TimestampUnixNano: 2000, Body: "keep-2", SeverityText: "info", ServiceName: "api"},
			{TimestampUnixNano: 2100, Body: "drop-2", SeverityText: "error", ServiceName: "api"},
		},
	} {
		key := partition + "/src-" + string(rune('a'+i)) + ".parquet"
		data, err := writeCompactedLogs(batch, 100, 1)
		if err != nil {
			t.Fatalf("write source parquet: %v", err)
		}
		pool.put(key, data)
		fi := manifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: int64(len(batch)),
			MinTimeNs: batch[0].TimestampUnixNano,
			MaxTimeNs: batch[len(batch)-1].TimestampUnixNano,
		}
		m.AddFile(partition, fi)
		files = append(files, fi)
		keys = append(keys, key)
	}

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID:           "ts-compact",
		Query:        `severity_text:="error"`,
		StartNs:      0,
		EndNs:        1 << 40,
		AffectedKeys: keys,
		CreatedAt:    time.Now().Add(-time.Hour),
		Mode:         mode,
		Reaped:       map[string]bool{},
	})

	return &compactionTombstoneFixture{pool: pool, manifest: m, store: store, files: files, keys: keys}
}

func (f *compactionTombstoneFixture) compactor(store *delete.TombstoneStore) *Compactor {
	return NewCompactor(CompactorConfig{
		Pool:         f.pool,
		Manifest:     f.manifest,
		Prefix:       "",
		Mode:         config.ModeLogs,
		RowGroupSize: 100,
		Tombstones:   store,
	})
}

// TestCompaction_DropsTombstonedRows is the second half of the delete story:
// compaction rewrites every row it touches, so carrying tombstoned rows forward
// copies deleted bytes into a brand-new key the tombstone's AffectedKeys list
// has never heard of.
func TestCompaction_DropsTombstonedRows(t *testing.T) {
	f := newCompactionTombstoneFixture(t, "permanent")

	before := metrics.DeleteCompactionRowsRemoved.Get()
	res, err := f.compactor(f.store).Compact(context.Background(), "dt=2026-07-01/hour=03", f.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if res.RowsMerged != 2 {
		t.Fatalf("merged %d rows, want the 2 that survive the tombstone", res.RowsMerged)
	}
	if metrics.DeleteCompactionRowsRemoved.Get() != before+2 {
		t.Errorf("expected 2 suppressed rows to be counted, before=%d after=%d",
			before, metrics.DeleteCompactionRowsRemoved.Get())
	}

	// The manifest entry must describe the output, not the inputs: RowCount is
	// answered from metadata by the timestamp-only fast path and by the 404
	// recovery's synthetic blocks, so an over-count resurrects deleted rows.
	fi, ok := f.manifest.GetFileByKey(res.OutputFile)
	if !ok {
		t.Fatalf("compacted output %s not in the manifest", res.OutputFile)
	}
	if fi.RowCount != 2 {
		t.Fatalf("compacted entry claims %d rows, output holds 2", fi.RowCount)
	}
	if agg := fi.LabelAggregates["severity_text"]; agg["error"] != 0 {
		t.Errorf("aggregate still counts %d deleted error rows", agg["error"])
	}

	// And the bytes are actually gone.
	rows, err := readLogRows(f.pool.get(res.OutputFile))
	if err != nil {
		t.Fatalf("read compacted output: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("output holds %d rows, want 2", len(rows))
	}
	for i := range rows {
		if rows[i].SeverityText == "error" {
			t.Fatalf("tombstoned row %q was copied into the compacted output", rows[i].Body)
		}
	}
}

// TestCompaction_ReapsSourceKeysAndCompletesTheTombstone covers the bookkeeping:
// once compaction has merged the sources away, the rewriter must not keep
// chasing keys whose objects no longer exist.
func TestCompaction_ReapsSourceKeysAndCompletesTheTombstone(t *testing.T) {
	f := newCompactionTombstoneFixture(t, "permanent")

	before := metrics.DeleteCompactionKeysReaped.Get()
	if _, err := f.compactor(f.store).Compact(context.Background(), "dt=2026-07-01/hour=03", f.files, 0); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if metrics.DeleteCompactionKeysReaped.Get() != before+uint64(len(f.keys)) {
		t.Errorf("expected %d keys to be reaped, counter moved by %d",
			len(f.keys), metrics.DeleteCompactionKeysReaped.Get()-before)
	}
	if _, still := f.store.Get("ts-compact"); still {
		t.Fatal("every affected key was merged away, so the tombstone must be completed rather than left active")
	}
}

func TestCompaction_HideModeTombstoneIsNeverReaped(t *testing.T) {
	f := newCompactionTombstoneFixture(t, "hide")

	res, err := f.compactor(f.store).Compact(context.Background(), "dt=2026-07-01/hour=03", f.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// The rows are still dropped from the output — hide mode only means the
	// user did not ask for a rewrite, not that compaction should copy them.
	if res.RowsMerged != 2 {
		t.Fatalf("merged %d rows, want 2", res.RowsMerged)
	}
	// But the tombstone stands: it is a standing suppression instruction, and
	// retiring it would un-hide anything it still covers elsewhere.
	ts, ok := f.store.Get("ts-compact")
	if !ok {
		t.Fatal("a hide-mode tombstone must never be auto-completed")
	}
	for _, k := range f.keys {
		if ts.Reaped[k] {
			t.Errorf("hide-mode tombstone must not record %s as reaped", k)
		}
	}
}

func TestCompaction_WithoutATombstoneStoreIsUnchanged(t *testing.T) {
	f := newCompactionTombstoneFixture(t, "permanent")

	res, err := f.compactor(nil).Compact(context.Background(), "dt=2026-07-01/hour=03", f.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.RowsMerged != 4 {
		t.Fatalf("a compactor with no tombstone store must merge all %d rows, got %d", 4, res.RowsMerged)
	}
}

func TestCompaction_TombstoneOutsideTheTimeRangeIsIgnored(t *testing.T) {
	f := newCompactionTombstoneFixture(t, "permanent")
	ts, _ := f.store.Get("ts-compact")
	// A tombstone whose window does not overlap the data must not suppress
	// anything, no matter what its query says.
	ts.StartNs, ts.EndNs = 1<<50, 1<<51
	f.store.Add(ts)

	res, err := f.compactor(f.store).Compact(context.Background(), "dt=2026-07-01/hour=03", f.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.RowsMerged != 4 {
		t.Fatalf("merged %d rows, want all 4 (the tombstone's window does not overlap)", res.RowsMerged)
	}
}

func TestMarkKeysReaped_HonoursNeverDeletePrefixes(t *testing.T) {
	store := delete.NewTombstoneStore()
	protected := "logs/_tombstones/x.parquet"
	normal := "logs/dt=2026-07-01/hour=00/a.parquet"
	store.Add(delete.Tombstone{
		ID:           "ts",
		Mode:         "permanent",
		AffectedKeys: []string{protected, normal},
		Reaped:       map[string]bool{},
	})

	markKeysReaped(store, []string{protected, normal}, defaultNeverDeletePrefixes())

	ts, ok := store.Get("ts")
	if !ok {
		t.Fatal("tombstone should still be active: one of its keys is protected, so it is not fully reaped")
	}
	if ts.Reaped[protected] {
		t.Error("a key under a never-delete prefix is not compaction's to reap")
	}
	if !ts.Reaped[normal] {
		t.Error("the unprotected key should be reaped")
	}
}

func TestMarkKeysReaped_NilStoreAndEmptyInputAreNoOps(t *testing.T) {
	markKeysReaped(nil, []string{"a"}, nil)
	store := delete.NewTombstoneStore()
	markKeysReaped(store, nil, nil)
	markKeysReaped(store, []string{"logs/_meta/x"}, defaultNeverDeletePrefixes())
	if store.Count() != 0 {
		t.Error("no tombstone should have been created")
	}
}

// TestNeverDeletePrefixesMatchTheTombstoneStore pins the two definitions
// together. The orphan sweep protects "_tombstones/" by substring; the delete
// package builds its keys from its own constant. If either moves without the
// other, the sweep starts deleting live tombstone records.
func TestNeverDeletePrefixesMatchTheTombstoneStore(t *testing.T) {
	want := delete.TombstonePrefix("")
	var found bool
	for _, p := range defaultNeverDeletePrefixes() {
		if p == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("the never-delete list %v does not protect the tombstone prefix %q",
			defaultNeverDeletePrefixes(), want)
	}
}

func TestIsNeverDeleteKey(t *testing.T) {
	prefixes := defaultNeverDeletePrefixes()
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"logs/_tombstones/a.json", true},
		{"logs/_meta/manifest.json", true},
		{"logs/_compaction_lock", true},
		{"logs/dt=2026-01-01/hour=00/a.parquet", false},
	} {
		if got := isNeverDeleteKey(tc.key, prefixes); got != tc.want {
			t.Errorf("isNeverDeleteKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
	if isNeverDeleteKey("anything", []string{""}) {
		t.Error("an empty prefix must not protect everything")
	}
}

// --- traces mode -------------------------------------------------------------

// TestCompaction_DropsTombstonedSpans is the traces twin of
// TestCompaction_DropsTombstonedRows. The traces module shares this compactor,
// so the span path needs its own proof that a deleted span is not copied into
// the merged output.
func TestCompaction_DropsTombstonedSpans(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "")
	partition := "dt=2026-07-02/hour=05"

	var files []manifest.FileInfo
	var keys []string
	for i, batch := range [][]schema.TraceRow{
		{
			{TimestampUnixNano: 1000, TraceID: "t1", SpanID: "s1", SpanName: "GET /a", ServiceName: "api-gateway"},
			{TimestampUnixNano: 1100, TraceID: "t2", SpanID: "s2", SpanName: "POST /b", ServiceName: "order-service"},
		},
		{
			{TimestampUnixNano: 2000, TraceID: "t3", SpanID: "s3", SpanName: "GET /c", ServiceName: "api-gateway"},
			{TimestampUnixNano: 2100, TraceID: "t4", SpanID: "s4", SpanName: "POST /d", ServiceName: "order-service"},
		},
	} {
		key := partition + "/span-" + string(rune('a'+i)) + ".parquet"
		data, err := writeCompactedTraces(batch, 100, 1)
		if err != nil {
			t.Fatalf("write source parquet: %v", err)
		}
		pool.put(key, data)
		fi := manifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: int64(len(batch)),
			MinTimeNs: batch[0].TimestampUnixNano,
			MaxTimeNs: batch[len(batch)-1].TimestampUnixNano,
		}
		m.AddFile(partition, fi)
		files = append(files, fi)
		keys = append(keys, key)
	}

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID:           "ts-spans",
		Query:        `service.name:="order-service"`,
		StartNs:      0,
		EndNs:        1 << 40,
		AffectedKeys: keys,
		CreatedAt:    time.Now().Add(-time.Hour),
		Mode:         "permanent",
		Reaped:       map[string]bool{},
	})

	c := NewCompactor(CompactorConfig{
		Pool: pool, Manifest: m, Mode: config.ModeTraces, RowGroupSize: 100, Tombstones: store,
	})
	res, err := c.Compact(context.Background(), partition, files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.RowsMerged != 2 {
		t.Fatalf("merged %d spans, want the 2 that survive the tombstone", res.RowsMerged)
	}

	rows, err := readTraceRows(pool.get(res.OutputFile))
	if err != nil {
		t.Fatalf("read compacted output: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("output holds %d spans, want 2", len(rows))
	}
	for i := range rows {
		if rows[i].ServiceName == "order-service" {
			t.Fatalf("tombstoned span %s was copied into the compacted output", rows[i].SpanID)
		}
	}
	if _, still := store.Get("ts-spans"); still {
		t.Error("every affected key was merged away; the tombstone must be completed")
	}
}

func TestDropTombstonedRows_NilStoreAndEmptyInputAreNoOps(t *testing.T) {
	logs := []schema.LogRow{{TimestampUnixNano: 1, Body: "a"}}
	if got := dropTombstonedLogRows(nil, logs); len(got) != 1 {
		t.Errorf("a nil store must leave the rows alone, got %d", len(got))
	}
	if got := dropTombstonedLogRows(delete.NewTombstoneStore(), nil); got != nil {
		t.Errorf("no rows in, no rows out, got %v", got)
	}
	// A store with no tombstone covering the range must not copy the slice.
	if got := dropTombstonedLogRows(delete.NewTombstoneStore(), logs); len(got) != 1 {
		t.Errorf("an empty store must leave the rows alone, got %d", len(got))
	}

	spans := []schema.TraceRow{{TimestampUnixNano: 1, SpanID: "s"}}
	if got := dropTombstonedTraceRows(nil, spans); len(got) != 1 {
		t.Errorf("a nil store must leave the spans alone, got %d", len(got))
	}
	if got := dropTombstonedTraceRows(delete.NewTombstoneStore(), nil); got != nil {
		t.Errorf("no spans in, no spans out, got %v", got)
	}
	if got := dropTombstonedTraceRows(delete.NewTombstoneStore(), spans); len(got) != 1 {
		t.Errorf("an empty store must leave the spans alone, got %d", len(got))
	}
}

func TestTombstonedTraceRow(t *testing.T) {
	tss := []delete.Tombstone{{Query: `service.name:="x"`, StartNs: 10, EndNs: 20}}
	match := &schema.TraceRow{TimestampUnixNano: 15, ServiceName: "x"}
	if !tombstonedTraceRow(tss, match) {
		t.Error("a matching span inside the window is tombstoned")
	}
	outside := &schema.TraceRow{TimestampUnixNano: 5, ServiceName: "x"}
	if tombstonedTraceRow(tss, outside) {
		t.Error("a matching span outside the window is not tombstoned")
	}
	other := &schema.TraceRow{TimestampUnixNano: 15, ServiceName: "y"}
	if tombstonedTraceRow(tss, other) {
		t.Error("a non-matching span is not tombstoned")
	}
}

func TestContainsKey(t *testing.T) {
	keys := []string{"a", "b"}
	if !containsKey(keys, "b") {
		t.Error("a present key must be found")
	}
	if containsKey(keys, "c") {
		t.Error("an absent key must not be found")
	}
	if containsKey(nil, "a") {
		t.Error("nothing is present in an empty list")
	}
}
