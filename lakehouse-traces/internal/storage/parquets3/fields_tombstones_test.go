package parquets3

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Mirror of internal/storage/parquets3/fields_tombstones_test.go. The traces
// module has its own copy of the field-enumeration paths, so it needs its own
// proof that a deleted span's attributes stop showing up in the pickers.

type traceFieldsTombstoneFixture struct {
	storage *Storage
	startNs int64
	endNs   int64
}

func newTraceFieldsTombstoneFixture(t *testing.T, withCatalog bool) *traceFieldsTombstoneFixture {
	t.Helper()

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())

	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	if withCatalog {
		catalog := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		s.catalog = catalog
		bw.catalogObserver = &catalogObserver{store: catalog}
	}

	now := time.Now()
	bw.AddTraceRows([]schema.TraceRow{
		{TimestampUnixNano: now.UnixNano(), ServiceName: "api-gateway", SpanName: "GET /a", TraceID: "t1", SpanID: "s1"},
		{TimestampUnixNano: now.UnixNano(), ServiceName: "order-service", SpanName: "POST /b", TraceID: "t2", SpanID: "s2"},
		{TimestampUnixNano: now.UnixNano(), ServiceName: "api-gateway", SpanName: "GET /a", TraceID: "t3", SpanID: "s3"},
	})
	bw.triggerFlush()

	return &traceFieldsTombstoneFixture{
		storage: s,
		startNs: now.Add(-time.Hour).UnixNano(),
		endNs:   now.Add(time.Hour).UnixNano(),
	}
}

func (f *traceFieldsTombstoneFixture) addTombstone() {
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID:      "ts-trace-fields",
		Query:   `service.name:="order-service"`,
		StartNs: f.startNs,
		EndNs:   f.endNs,
		Mode:    "hide",
	})
	f.storage.SetTombstoneStore(store)
}

func traceValueStrings(vs []logstorage.ValueWithHits) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = v.Value
	}
	sort.Strings(out)
	return out
}

func TestTraceFieldValues_TombstonedValueDisappears(t *testing.T) {
	for _, withCatalog := range []bool{false, true} {
		name := "labelindex-and-scan"
		if withCatalog {
			name = "pmeta-catalog"
		}
		t.Run(name, func(t *testing.T) {
			f := newTraceFieldsTombstoneFixture(t, withCatalog)
			q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)

			before, err := f.storage.GetFieldValues(context.Background(), nil, q, "service.name", 100)
			if err != nil {
				t.Fatalf("GetFieldValues before: %v", err)
			}
			if len(traceValueStrings(before)) != 2 {
				t.Fatalf("fixture is wrong: values before the delete = %v", traceValueStrings(before))
			}

			f.addTombstone()

			after, err := f.storage.GetFieldValues(context.Background(), nil, q, "service.name", 100)
			if err != nil {
				t.Fatalf("GetFieldValues after: %v", err)
			}
			for _, v := range traceValueStrings(after) {
				if v == "order-service" {
					t.Fatalf("the deleted span's service is still enumerated: %v", traceValueStrings(after))
				}
			}
		})
	}
}

func TestTraceFieldValues_MetadataFastPathGivenUpUnderATombstone(t *testing.T) {
	f := newTraceFieldsTombstoneFixture(t, true)
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)

	before := metrics.DeleteFieldsScanFallback.Get("field_values")
	f.addTombstone()
	if _, err := f.storage.GetFieldValues(context.Background(), nil, q, "service.name", 100); err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	if metrics.DeleteFieldsScanFallback.Get("field_values") <= before {
		t.Error("giving up the metadata fast path must be counted")
	}
}

func TestTraceGetStreams_TombstonedRowsAreNotEnumerated(t *testing.T) {
	f := newTraceFieldsTombstoneFixture(t, false)
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)
	f.addTombstone()

	before := metrics.DeleteFieldsScanFallback.Get("streams")
	if _, err := f.storage.GetStreams(context.Background(), nil, q, 100); err != nil {
		t.Fatalf("GetStreams: %v", err)
	}
	if metrics.DeleteFieldsScanFallback.Get("streams") <= before {
		t.Error("GetStreams must record that it applied a tombstone")
	}

	beforeIDs := metrics.DeleteFieldsScanFallback.Get("stream_ids")
	if _, err := f.storage.GetStreamIDs(context.Background(), nil, q, 100); err != nil {
		t.Fatalf("GetStreamIDs: %v", err)
	}
	if metrics.DeleteFieldsScanFallback.Get("stream_ids") <= beforeIDs {
		t.Error("GetStreamIDs must record that it applied a tombstone")
	}
}

func TestTraceAddTombstoneProjection_IncludesTimestampAndPredicateColumns(t *testing.T) {
	s := testStorage()
	projected := map[string]bool{"span.name": true}

	s.addTombstoneProjection([]tombstone{
		{Query: `service.name:="x"`, StartNs: 0, EndNs: 10},
	}, projected)

	if !projected[timestampColumn] {
		t.Error("the timestamp column must be projected; a tombstone is time-bounded")
	}
	if !projected["service.name"] {
		t.Errorf("the tombstone's own field must be projected, got %v", projected)
	}
	if !projected["span.name"] {
		t.Error("the caller's existing projection must be preserved")
	}
}

func TestTraceRowTombstoned(t *testing.T) {
	tss := []tombstone{{Query: `service.name:="x"`, StartNs: 10, EndNs: 20}}
	fields := []logstorage.Field{{Name: "service.name", Value: "x"}}

	if !rowTombstoned(tss, fields, 15) {
		t.Error("a matching row inside the window is tombstoned")
	}
	if rowTombstoned(tss, fields, 5) {
		t.Error("a matching row outside the window is not tombstoned")
	}
	if rowTombstoned(nil, fields, 15) {
		t.Error("no tombstones means nothing is tombstoned")
	}
}

func TestTraceFieldsTombstones_NilStore(t *testing.T) {
	s := testStorage()
	if got := s.fieldsTombstones(0, 1<<62); got != nil {
		t.Errorf("a storage with no tombstone store must report none, got %v", got)
	}
}

func TestTraceRowTimestampNs_MissingColumnReadsAsZero(t *testing.T) {
	if got := rowTimestampNs(nil, -1); got != 0 {
		t.Errorf("an unprojected timestamp column must read as 0, got %d", got)
	}
}

func TestTraceAddTombstoneProjection_NoTombstonesLeavesTheProjectionAlone(t *testing.T) {
	s := testStorage()
	projected := map[string]bool{"span.name": true}
	s.addTombstoneProjection(nil, projected)
	if len(projected) != 1 {
		t.Errorf("projection widened with no tombstones: %v", projected)
	}
}

func TestTraceAddTombstoneProjection_MatchAllAndUnparseableNeedOnlyTheTimestamp(t *testing.T) {
	s := testStorage()
	for _, q := range []string{"*", `broken:=="`} {
		projected := map[string]bool{}
		s.addTombstoneProjection([]tombstone{{Query: q, StartNs: 0, EndNs: 10}}, projected)
		if !projected[timestampColumn] {
			t.Errorf("query %q: the timestamp column must be projected — every tombstone is time-bounded", q)
		}
		if len(projected) != 1 {
			t.Errorf("query %q constrains no field, got %v", q, projected)
		}
	}
}

func TestTraceFieldsTombstones_EmptyStoreAndNonOverlappingWindow(t *testing.T) {
	s := testStorage()
	store := delete.NewTombstoneStore()
	s.SetTombstoneStore(store)

	if got := s.fieldsTombstones(0, 1<<62); got != nil {
		t.Errorf("an empty store must report none, got %v", got)
	}

	store.Add(delete.Tombstone{ID: "ts", Query: "*", StartNs: 100, EndNs: 200, Mode: "hide"})
	if got := s.fieldsTombstones(1000, 2000); got != nil {
		t.Errorf("a tombstone outside the window must report none, got %v", got)
	}
	if got := s.fieldsTombstones(150, 160); len(got) != 1 {
		t.Errorf("an overlapping tombstone must be reported, got %v", got)
	}
}

// TestTraceFieldValues_TombstoneCombinesWithTheUserFilter covers the branch
// where both predicates apply: the user's filter selects spans, and the
// tombstone removes some of what it selected.
func TestTraceFieldValues_TombstoneCombinesWithTheUserFilter(t *testing.T) {
	f := newTraceFieldsTombstoneFixture(t, false)
	f.addTombstone()

	q := mustParseQueryWithTime(t, `service.name:*`, f.startNs, f.endNs)
	got, err := f.storage.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	for _, v := range traceValueStrings(got) {
		if v == "order-service" {
			t.Fatalf("the deleted span's service survived a filtered enumeration: %v", traceValueStrings(got))
		}
	}
}

// --- pmeta catalog rebuild after rows are removed (mirror) ---------------------

func TestTracePmetaOnRewritten_RebuildsTheCatalogValues(t *testing.T) {
	s := testStorage()
	s.PmetaRebuildCatalogValues([]string{"traces/dt=2026-01-01/hour=00/a.parquet"}) // no catalog: no-op

	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "traces/")
	oldKey := "traces/dt=2026-09-05/hour=02/old.parquet"
	newKey := "traces/dt=2026-09-05/hour=02/new.parquet"
	part := manifest.ExtractTenantPartition(oldKey)

	s.catalog.OnFileFlush(pmeta.FileContribution{Partition: part, FileKey: oldKey,
		Labels: map[string][]string{"service.name": {"api", "leaky"}}})
	newFI := manifest.FileInfo{Key: newKey, RowCount: 1, Labels: map[string][]string{"service.name": {"api"}}}
	s.manifest.AddFile("dt=2026-09-05/hour=02", newFI)

	s.PmetaOnRewritten([]manifest.FileInfo{newFI}, []string{oldKey}, nil)

	if got := s.catalog.FieldValues(part, "service.name", "", 0); len(got) != 1 || got[0] != "api" {
		t.Fatalf("catalog after a rewrite = %v, want only [api]; the deleted span's service must not stay enumerable", got)
	}

	unlabeled := "traces/dt=2026-09-05/hour=02/unlabeled.parquet"
	s.manifest.AddFile("dt=2026-09-05/hour=02", manifest.FileInfo{Key: unlabeled, RowCount: 3})
	before := metrics.DeleteCatalogRebuilds.Get("skipped_unlabeled_file")
	s.PmetaRebuildCatalogValues([]string{newKey})
	if metrics.DeleteCatalogRebuilds.Get("skipped_unlabeled_file") <= before {
		t.Error("a partition with an unlabeled file must be skipped and counted")
	}
}

func TestTracePartitionHourBounds(t *testing.T) {
	h := time.Date(2026, 9, 5, 14, 0, 0, 0, time.UTC)
	lo, hi := partitionHourBounds(h.Add(17*time.Minute).UnixNano(), h.Add(42*time.Minute).UnixNano())
	if lo != h.UnixNano() || hi != h.Add(time.Hour).UnixNano()-1 {
		t.Errorf("window widened to [%v, %v], want the whole hour", time.Unix(0, lo).UTC(), time.Unix(0, hi).UTC())
	}
	if lo, hi := partitionHourBounds(math.MinInt64, math.MaxInt64); lo != math.MinInt64 || hi != math.MaxInt64 {
		t.Errorf("open window became [%d, %d]", lo, hi)
	}
}

// --- field-name and stream-id paths around the tombstone gate --------------------

func TestTraceGetFieldNames_EveryAnswerPath(t *testing.T) {
	// Footer path: no catalog, empty label index, files present.
	f := newTraceFieldsTombstoneFixture(t, false)
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)
	names, err := f.storage.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatalf("GetFieldNames (footer path): %v", err)
	}
	if len(names) == 0 {
		t.Fatal("the footer path must register and return the file's field names")
	}

	// Label-index path: the footer walk above populated it.
	again, err := f.storage.GetFieldNames(context.Background(), nil, q)
	if err != nil || len(again) == 0 {
		t.Fatalf("GetFieldNames (label index path) = %v, %v", again, err)
	}

	// Catalog path.
	withCatalog := newTraceFieldsTombstoneFixture(t, true)
	qc := mustParseQueryWithTime(t, "*", withCatalog.startNs, withCatalog.endNs)
	fromCatalog, err := withCatalog.storage.GetFieldNames(context.Background(), nil, qc)
	if err != nil || len(fromCatalog) == 0 {
		t.Fatalf("GetFieldNames (catalog path) = %v, %v", fromCatalog, err)
	}

	// A window with no files and nothing indexed answers nothing.
	empty := testStorage()
	none, err := empty.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, "*", 1, 2))
	if err != nil || none != nil {
		t.Fatalf("an empty store must answer nil, got %v, %v", none, err)
	}
}

func TestTraceGetStreamIDs_EmptyWindowAndCancelledContext(t *testing.T) {
	f := newTraceFieldsTombstoneFixture(t, false)

	none, err := f.storage.GetStreamIDs(context.Background(), nil, mustParseQueryWithTime(t, "*", 1, 2), 10)
	if err != nil || none != nil {
		t.Fatalf("a window with no files must answer nil, got %v, %v", none, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)
	if _, err := f.storage.GetStreamIDs(ctx, nil, q, 10); err == nil {
		t.Fatal("a cancelled request must stop and report the cancellation")
	}
	if _, err := f.storage.GetStreams(ctx, nil, q, 10); err == nil {
		t.Fatal("a cancelled request must stop and report the cancellation")
	}

	ids, err := f.storage.GetStreamIDs(context.Background(), nil, q, 1)
	if err != nil {
		t.Fatalf("GetStreamIDs: %v", err)
	}
	if len(ids) > 1 {
		t.Fatalf("the limit must cap the answer, got %d ids", len(ids))
	}
}
