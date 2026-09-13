package parquets3

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
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
