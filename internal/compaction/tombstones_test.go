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
			// What the flush writer records, so the compacted entry can be
			// checked against real input metadata rather than zeros.
			RawBytes: schema.EstimateRawBytesLogs(batch),
			Labels:   schema.ExtractLogLabels(batch),
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
	// The label SET must describe the survivors too. It is fed to the pmeta
	// field catalog, which field_values serves verbatim; unioning the input
	// files' labels would put the deleted "error" value back in the dropdown.
	for _, v := range fi.Labels["severity_text"] {
		if v == "error" {
			t.Errorf("compacted labels still carry %q, a value only the dropped rows had: %v", v, fi.Labels["severity_text"])
		}
	}
	if got := fi.Labels["severity_text"]; len(got) != 1 || got[0] != "info" {
		t.Errorf("compacted severity_text labels = %v, want [info]", got)
	}
	// And the raw size is the survivors', not the sum of the inputs'.
	var inputRaw int64
	for _, src := range f.files {
		inputRaw += src.RawBytes
	}
	if inputRaw <= 0 {
		t.Fatal("fixture is wrong: the input entries carry no RawBytes to compare against")
	}
	if fi.RawBytes >= inputRaw {
		t.Errorf("compacted RawBytes %d not reduced below the inputs' %d", fi.RawBytes, inputRaw)
	}
	if fi.RawBytes <= 0 {
		t.Errorf("compacted RawBytes = %d; the survivors' size must be recorded", fi.RawBytes)
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
	res, err := f.compactor(f.store).Compact(context.Background(), "dt=2026-07-01/hour=03", f.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if metrics.DeleteCompactionKeysReaped.Get() != before+uint64(len(f.keys)) {
		t.Errorf("expected %d keys to be reaped, counter moved by %d",
			len(f.keys), metrics.DeleteCompactionKeysReaped.Get()-before)
	}
	// The tombstone was eligible, so compaction filtered its rows out of the
	// output: the sources are gone, the output is clean, nothing is left to do.
	if _, still := f.store.Get("ts-compact"); still {
		t.Fatalf("every file holding the tombstone's rows was filtered (sources merged into clean output %s); the tombstone must complete", res.OutputFile)
	}
}

// TestCompaction_HideModeRowsAreCarriedForward pins the un-delete contract. A
// hide-mode delete is reversible: removing the tombstone must make the rows
// visible again. Compaction physically dropping them would turn every later
// un-delete of that data into silent data loss.
func TestCompaction_HideModeRowsAreCarriedForward(t *testing.T) {
	f := newCompactionTombstoneFixture(t, "hide")

	res, err := f.compactor(f.store).Compact(context.Background(), "dt=2026-07-01/hour=03", f.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.RowsMerged != 4 {
		t.Fatalf("merged %d rows, want all 4 — hide mode never removes rows", res.RowsMerged)
	}
	rows, err := readLogRows(f.pool.get(res.OutputFile))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	var hidden int
	for i := range rows {
		if rows[i].SeverityText == "error" {
			hidden++
		}
	}
	if hidden != 2 {
		t.Fatalf("the output holds %d of the 2 hidden rows; an un-delete could no longer restore them", hidden)
	}

	ts, ok := f.store.Get("ts-compact")
	if !ok {
		t.Fatal("a hide-mode tombstone must never be auto-completed")
	}
	for _, k := range f.keys {
		if ts.Reaped[k] {
			t.Errorf("hide-mode tombstone must not record %s as reaped", k)
		}
	}

	// Un-delete after compaction: the rows are still there to come back.
	f.store.Remove("ts-compact")
	if f.store.Count() != 0 {
		t.Fatal("un-delete did not remove the tombstone")
	}
}

// TestCompaction_InsideTheRewriteDelayCarriesRowsForwardAndTransfersTheKey is the
// same contract for permanent and auto deletes: rewrite_delay is their un-delete
// window. It also covers the bookkeeping hole that would otherwise resurrect
// data — the sources were merged away, so without transferring the tombstone to
// the output, every listed key would look reaped, the tombstone would retire,
// and rows that were never removed would become visible again.
func TestCompaction_InsideTheRewriteDelayCarriesRowsForwardAndTransfersTheKey(t *testing.T) {
	f := newCompactionTombstoneFixture(t, "permanent")
	ts, _ := f.store.Get("ts-compact")
	ts.CreatedAt = time.Now() // just issued
	f.store.Add(ts)

	c := NewCompactor(CompactorConfig{
		Pool: f.pool, Manifest: f.manifest, Mode: config.ModeLogs, RowGroupSize: 100,
		Tombstones: f.store, TombstoneRewriteDelay: time.Hour,
	})
	res, err := c.Compact(context.Background(), "dt=2026-07-01/hour=03", f.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.RowsMerged != 4 {
		t.Fatalf("merged %d rows, want all 4 — the un-delete window has not passed", res.RowsMerged)
	}

	got, ok := f.store.Get("ts-compact")
	if !ok {
		t.Fatal("the tombstone completed while a file still holds its rows; they would reappear")
	}
	if !containsKey(got.AffectedKeys, res.OutputFile) {
		t.Fatalf("the tombstone was not transferred to the output %s: %v", res.OutputFile, got.AffectedKeys)
	}
	if got.Handled(res.OutputFile) {
		t.Fatal("the output still holds the tombstone's rows and must stay pending for the rewriter")
	}
	for _, k := range f.keys {
		if !got.Reaped[k] {
			t.Errorf("merged-away source %s should be marked reaped", k)
		}
	}
	if got.FullyReaped() {
		t.Fatal("a tombstone with a pending output must not report itself fully reaped")
	}
}

func TestReconcileTombstones_HonoursNeverDeletePrefixes(t *testing.T) {
	store := delete.NewTombstoneStore()
	protected := "logs/_tombstones/x.parquet"
	normal := "logs/dt=2026-07-01/hour=00/a.parquet"
	output := "logs/dt=2026-07-01/hour=00/compacted.parquet"
	store.Add(delete.Tombstone{
		ID:           "ts",
		Mode:         "permanent",
		AffectedKeys: []string{protected, normal},
		Reaped:       map[string]bool{},
		CreatedAt:    time.Now().Add(-time.Hour),
	})

	reconcileTombstones(store, []string{protected, normal}, output, defaultNeverDeletePrefixes(), map[string]bool{"ts": true})

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
	if !ts.Clean[output] || ts.Reaped[output] {
		t.Error("an eligible tombstone's rows were filtered, so the output is clean")
	}
}

// TestReconcileTombstones_OutputCleanOnlyForTombstonesTheMergeApplied pins the
// clean marking to what the drop step did. The tombstone becomes eligible one
// minute after the drop evaluated it (the merge ran across the rewrite_delay
// boundary): its rows were carried, so the output must stay pending and the
// tombstone active, however eligible it is by bookkeeping time.
func TestReconcileTombstones_OutputCleanOnlyForTombstonesTheMergeApplied(t *testing.T) {
	const delay = time.Hour
	src, out := "logs/dt=2026-07-09/hour=11/src.parquet", "logs/dt=2026-07-09/hour=11/out.parquet"
	tDrop := time.Now()
	newStore := func() *delete.TombstoneStore {
		store := delete.NewTombstoneStore()
		store.Add(delete.Tombstone{
			ID: "t", Query: `service.name:="leaky"`,
			StartNs: propertyHour.UnixNano(), EndNs: propertyHour.Add(time.Hour).UnixNano(),
			AffectedKeys: []string{src}, CreatedAt: tDrop.Add(-delay).Add(30 * time.Second),
			Mode: "permanent", Reaped: map[string]bool{},
		})
		return store
	}
	rows := func() []schema.LogRow {
		return []schema.LogRow{
			{TimestampUnixNano: propertyHour.Add(time.Second).UnixNano(), Body: "r1", ServiceName: "leaky"},
			{TimestampUnixNano: propertyHour.Add(2 * time.Second).UnixNano(), Body: "r2", ServiceName: "web"},
		}
	}

	store := newStore()
	kept, dropped, applied := dropTombstonedLogRows(store, rows(), tDrop, delay)
	if dropped != 0 || len(kept) != 2 || applied["t"] {
		t.Fatalf("fixture: the tombstone is not eligible at the drop, got dropped=%d applied=%v", dropped, applied)
	}
	reconcileTombstones(store, []string{src}, out, nil, applied)
	ts, active := store.Get("t")
	if !active {
		t.Fatalf("tombstone retired although its rows were carried into %s unfiltered", out)
	}
	if ts.Handled(out) || !ts.Reaped[src] || !containsKey(ts.AffectedKeys, out) {
		t.Fatalf("want source reaped and output listed as pending, got keys=%v reaped=%v", ts.AffectedKeys, ts.Reaped)
	}

	// The same merge one minute later applies the tombstone: now the output is
	// clean and, with every key handled, the tombstone retires.
	store = newStore()
	_, dropped, applied = dropTombstonedLogRows(store, rows(), tDrop.Add(time.Minute), delay)
	if dropped != 1 || !applied["t"] {
		t.Fatalf("fixture: the tombstone is eligible a minute later, got dropped=%d applied=%v", dropped, applied)
	}
	reconcileTombstones(store, []string{src}, out, nil, applied)
	if _, still := store.Get("t"); still {
		t.Fatal("an output the merge filtered is clean; the tombstone must retire")
	}
}

func TestReconcileTombstones_UntouchedTombstonesAreLeftAlone(t *testing.T) {
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID:           "other",
		Mode:         "permanent",
		AffectedKeys: []string{"logs/dt=2026-07-01/hour=00/unrelated.parquet"},
		Reaped:       map[string]bool{},
		CreatedAt:    time.Now().Add(-time.Hour),
	})

	reconcileTombstones(store, []string{"logs/dt=2026-07-01/hour=00/a.parquet"},
		"logs/dt=2026-07-01/hour=00/out.parquet", nil, map[string]bool{"other": true})

	ts, _ := store.Get("other")
	if len(ts.AffectedKeys) != 1 || len(ts.Reaped) != 0 {
		t.Fatalf("a tombstone that named none of the merged sources must not change: %+v", ts)
	}
}

func TestReconcileTombstones_NilStoreAndEmptyInputAreNoOps(t *testing.T) {
	reconcileTombstones(nil, []string{"a"}, "out", nil, nil)
	store := delete.NewTombstoneStore()
	reconcileTombstones(store, nil, "out", nil, nil)
	reconcileTombstones(store, []string{"logs/_meta/x"}, "out", defaultNeverDeletePrefixes(), nil)
	if store.Count() != 0 {
		t.Error("no tombstone should have been created")
	}
}

func TestEligibleTombstones(t *testing.T) {
	now := time.Now()
	tss := []delete.Tombstone{
		{ID: "hide", Mode: "hide", CreatedAt: now.Add(-24 * time.Hour)},
		{ID: "fresh", Mode: "permanent", CreatedAt: now},
		{ID: "old-permanent", Mode: "permanent", CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "old-auto", Mode: "auto", CreatedAt: now.Add(-2 * time.Hour)},
	}
	got := eligibleTombstones(tss, now, time.Hour)
	ids := map[string]bool{}
	for _, ts := range got {
		ids[ts.ID] = true
	}
	if ids["hide"] || ids["fresh"] || !ids["old-permanent"] || !ids["old-auto"] || len(got) != 2 {
		t.Fatalf("eligible = %v, want exactly old-permanent and old-auto", ids)
	}
	if len(tss) != 4 {
		t.Fatal("eligibleTombstones must not modify its input slice")
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
	now := time.Now()
	logs := []schema.LogRow{{TimestampUnixNano: 1, Body: "a"}}
	if got, n, _ := dropTombstonedLogRows(nil, logs, now, 0); len(got) != 1 || n != 0 {
		t.Errorf("a nil store must leave the rows alone, got %d rows, %d dropped", len(got), n)
	}
	if got, n, _ := dropTombstonedLogRows(delete.NewTombstoneStore(), nil, now, 0); got != nil || n != 0 {
		t.Errorf("no rows in, no rows out, got %v, %d dropped", got, n)
	}
	// A store with no tombstone covering the range must not copy the slice.
	if got, n, applied := dropTombstonedLogRows(delete.NewTombstoneStore(), logs, now, 0); len(got) != 1 || n != 0 || len(applied) != 0 {
		t.Errorf("an empty store must leave the rows alone, got %d rows, %d dropped", len(got), n)
	}

	spans := []schema.TraceRow{{TimestampUnixNano: 1, SpanID: "s"}}
	if got, n, _ := dropTombstonedTraceRows(nil, spans, now, 0); len(got) != 1 || n != 0 {
		t.Errorf("a nil store must leave the spans alone, got %d spans, %d dropped", len(got), n)
	}
	if got, n, _ := dropTombstonedTraceRows(delete.NewTombstoneStore(), nil, now, 0); got != nil || n != 0 {
		t.Errorf("no spans in, no spans out, got %v, %d dropped", got, n)
	}
	if got, n, applied := dropTombstonedTraceRows(delete.NewTombstoneStore(), spans, now, 0); len(got) != 1 || n != 0 || len(applied) != 0 {
		t.Errorf("an empty store must leave the spans alone, got %d spans, %d dropped", len(got), n)
	}
}

// TestDropTombstonedRows_OnlyEligibleTombstonesDrop pins the predicate at the
// row level for both signals: an ineligible tombstone (hide, or still inside
// the un-delete window) removes nothing, an eligible one removes its rows.
func TestDropTombstonedRows_OnlyEligibleTombstonesDrop(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name      string
		mode      string
		createdAt time.Time
		wantDrop  int
	}{
		{"hide never drops", "hide", now.Add(-24 * time.Hour), 0},
		{"permanent inside the delay keeps rows", "permanent", now, 0},
		{"permanent past the delay drops", "permanent", now.Add(-2 * time.Hour), 1},
		{"auto past the delay drops", "auto", now.Add(-2 * time.Hour), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := delete.NewTombstoneStore()
			store.Add(delete.Tombstone{
				ID: "ts", Query: `service.name:="gone"`, StartNs: 0, EndNs: 1 << 40,
				Mode: tc.mode, CreatedAt: tc.createdAt,
			})

			logs := []schema.LogRow{
				{TimestampUnixNano: 10, ServiceName: "gone"},
				{TimestampUnixNano: 20, ServiceName: "stays"},
			}
			_, n, applied := dropTombstonedLogRows(store, logs, now, time.Hour)
			if n != tc.wantDrop {
				t.Errorf("logs: dropped %d, want %d", n, tc.wantDrop)
			}
			// Applied means "evaluated against every row": exactly the eligible
			// tombstones, whether or not a row matched.
			if applied["ts"] != (tc.wantDrop > 0) {
				t.Errorf("logs: applied=%v, want ts applied=%v", applied, tc.wantDrop > 0)
			}
			spans := []schema.TraceRow{
				{TimestampUnixNano: 10, ServiceName: "gone", SpanID: "a"},
				{TimestampUnixNano: 20, ServiceName: "stays", SpanID: "b"},
			}
			_, n, applied = dropTombstonedTraceRows(store, spans, now, time.Hour)
			if n != tc.wantDrop {
				t.Errorf("traces: dropped %d, want %d", n, tc.wantDrop)
			}
			if applied["ts"] != (tc.wantDrop > 0) {
				t.Errorf("traces: applied=%v, want ts applied=%v", applied, tc.wantDrop > 0)
			}
		})
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
