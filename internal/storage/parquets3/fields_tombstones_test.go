package parquets3

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fieldsTombstoneFixture is a real writer flush into a mock S3 bucket, so the
// field-enumeration endpoints run against genuine Parquet files and a genuine
// manifest — the only setting in which "the value is still in the dropdown"
// can be observed.
type fieldsTombstoneFixture struct {
	storage      *Storage
	startNs      int64
	endNs        int64
	tombstoneQry string
}

func newFieldsTombstoneFixture(t *testing.T, withCatalog bool) *fieldsTombstoneFixture {
	t.Helper()

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())

	if withCatalog {
		catalog := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		s.catalog = catalog
		bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
		bw.catalogObserver = &catalogObserver{store: catalog}
		seedFieldRows(t, bw)
	} else {
		bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
		seedFieldRows(t, bw)
	}

	now := time.Now()
	return &fieldsTombstoneFixture{
		storage:      s,
		startNs:      now.Add(-time.Hour).UnixNano(),
		endNs:        now.Add(time.Hour).UnixNano(),
		tombstoneQry: `service.name:="order-service"`,
	}
}

func seedFieldRows(t *testing.T, bw *BatchWriter) {
	t.Helper()
	now := time.Now().UnixNano()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now, Body: "a", ServiceName: "api-gateway", SeverityText: "info"},
		{TimestampUnixNano: now, Body: "b", ServiceName: "order-service", SeverityText: "error"},
		{TimestampUnixNano: now, Body: "c", ServiceName: "api-gateway", SeverityText: "info"},
	})
	bw.triggerFlush()
}

func (f *fieldsTombstoneFixture) addTombstone() {
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID:      "ts-fields",
		Query:   f.tombstoneQry,
		StartNs: f.startNs,
		EndNs:   f.endNs,
		Mode:    "hide",
	})
	f.storage.SetTombstoneStore(store)
}

// TestFieldValues_TombstonedValueDisappears is the user-visible symptom: a
// hide-mode delete removed the rows from the log view, but the value it deleted
// stayed in every Grafana dropdown, every field-cardinality panel and every
// stream list — a "deleted" value the user could still see and still select.
func TestFieldValues_TombstonedValueDisappears(t *testing.T) {
	for _, withCatalog := range []bool{false, true} {
		name := "labelindex-and-scan"
		if withCatalog {
			name = "pmeta-catalog"
		}
		t.Run(name, func(t *testing.T) {
			f := newFieldsTombstoneFixture(t, withCatalog)
			q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)

			before, err := f.storage.GetFieldValues(context.Background(), nil, q, "service.name", 100)
			if err != nil {
				t.Fatalf("GetFieldValues before: %v", err)
			}
			if got, want := valueStrings(before), []string{"api-gateway", "order-service"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("fixture is wrong: values before the delete = %v, want %v", got, want)
			}

			f.addTombstone()

			after, err := f.storage.GetFieldValues(context.Background(), nil, q, "service.name", 100)
			if err != nil {
				t.Fatalf("GetFieldValues after: %v", err)
			}
			got := valueStrings(after)
			for _, v := range got {
				if v == "order-service" {
					t.Fatalf("the deleted value is still enumerated: %v", got)
				}
			}
			if want := []string{"api-gateway"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("values after the delete = %v, want %v", got, want)
			}
		})
	}
}

// TestFieldValues_MetadataFastPathGivenUpUnderATombstone pins the mechanism:
// the RAM indexes have no tombstone awareness, so the only correct thing to do
// with an overlapping tombstone is to stop trusting them.
func TestFieldValues_MetadataFastPathGivenUpUnderATombstone(t *testing.T) {
	f := newFieldsTombstoneFixture(t, true)
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)

	before := metrics.DeleteFieldsScanFallback.Get("field_values")
	f.addTombstone()
	if _, err := f.storage.GetFieldValues(context.Background(), nil, q, "service.name", 100); err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	if metrics.DeleteFieldsScanFallback.Get("field_values") <= before {
		t.Error("giving up the metadata fast path must be counted; operators need it to explain the latency change after a delete")
	}
}

// TestFieldValues_NonOverlappingTombstoneKeepsTheFastPath is the other half:
// the cost is paid only while a tombstone actually covers the window.
func TestFieldValues_NonOverlappingTombstoneKeepsTheFastPath(t *testing.T) {
	f := newFieldsTombstoneFixture(t, true)

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		ID: "ts-elsewhere", Query: f.tombstoneQry,
		StartNs: 1, EndNs: 2, Mode: "hide",
	})
	f.storage.SetTombstoneStore(store)

	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)
	before := metrics.DeleteFieldsScanFallback.Get("field_values")
	got, err := f.storage.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	if metrics.DeleteFieldsScanFallback.Get("field_values") != before {
		t.Error("a tombstone outside the window must not cost the fast path")
	}
	if want := []string{"api-gateway", "order-service"}; !reflect.DeepEqual(valueStrings(got), want) {
		t.Fatalf("values = %v, want the untouched %v", valueStrings(got), want)
	}
}

// TestGetStreams_TombstonedRowsAreNotEnumerated covers the streams/stream_ids
// pair, which share the scanner with field_values.
func TestGetStreams_TombstonedRowsAreNotEnumerated(t *testing.T) {
	f := newFieldsTombstoneFixture(t, false)
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

// TestFieldNames_HitsAreUnknownUnderATombstone documents the bound we accept.
//
// Hit counts come from the Parquet column index — no row is read — so a
// tombstone cannot be applied per row without turning a footer walk into a full
// scan of every candidate file. Reporting the raw counts anyway would hand the
// caller a number that still includes deleted rows, so the counts are reported
// as unknown (Hits=0) instead. Names stay over-inclusive until the rewrite
// lands; that is documented in docs/operations.md.
func TestFieldNames_HitsAreUnknownUnderATombstone(t *testing.T) {
	f := newFieldsTombstoneFixture(t, false)
	q := mustParseQueryWithTime(t, "*", f.startNs, f.endNs)

	before, err := f.storage.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatalf("GetFieldNames before: %v", err)
	}
	var sawHits bool
	for _, v := range before {
		if v.Hits > 0 {
			sawHits = true
		}
	}
	if !sawHits {
		t.Fatal("fixture is wrong: field names should carry real hit counts before any delete")
	}

	f.addTombstone()

	after, err := f.storage.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatalf("GetFieldNames after: %v", err)
	}
	if len(after) == 0 {
		t.Fatal("field names must still be returned; only the counts become unknown")
	}
	for _, v := range after {
		if v.Hits != 0 {
			t.Errorf("field %q reports %d hits; that count still includes the deleted rows", v.Value, v.Hits)
		}
	}
}

// --- unit coverage of the helpers -------------------------------------------

func TestFieldsTombstones_NilStoreAndEmptyRange(t *testing.T) {
	s := testStorage()
	if got := s.fieldsTombstones(0, 1<<62); got != nil {
		t.Errorf("a storage with no tombstone store must report none, got %v", got)
	}
	s.SetTombstoneStore(delete.NewTombstoneStore())
	if got := s.fieldsTombstones(0, 1<<62); got != nil {
		t.Errorf("an empty store must report none, got %v", got)
	}
}

func TestAddTombstoneProjection_IncludesTimestampAndPredicateColumns(t *testing.T) {
	s := testStorage()
	projected := map[string]bool{"body": true}

	s.addTombstoneProjection([]tombstone{
		{Query: `service.name:="x"`, StartNs: 0, EndNs: 10},
	}, projected)

	// Without the timestamp column every row reads as timestamp 0 and falls
	// outside the tombstone's range, so nothing is suppressed.
	if !projected[timestampColumn] {
		t.Error("the timestamp column must be projected; a tombstone is time-bounded")
	}
	// Without the predicate's own columns the filter evaluates against absent
	// fields and matches nothing.
	if !projected["service.name"] {
		t.Errorf("the tombstone's own field must be projected, got %v", projected)
	}
	if !projected["body"] {
		t.Error("the caller's existing projection must be preserved")
	}
}

func TestAddTombstoneProjection_NoTombstonesLeavesTheProjectionAlone(t *testing.T) {
	s := testStorage()
	projected := map[string]bool{"body": true}
	s.addTombstoneProjection(nil, projected)
	if len(projected) != 1 {
		t.Errorf("projection widened with no tombstones: %v", projected)
	}
}

func TestAddTombstoneProjection_MatchAllQueryNeedsOnlyTheTimestamp(t *testing.T) {
	s := testStorage()
	projected := map[string]bool{}
	s.addTombstoneProjection([]tombstone{{Query: "*", StartNs: 0, EndNs: 10}}, projected)
	if !projected[timestampColumn] {
		t.Error("even a match-all tombstone is time-bounded")
	}
	if len(projected) != 1 {
		t.Errorf("a match-all tombstone constrains no field, got %v", projected)
	}
}

func TestRowTombstoned(t *testing.T) {
	tss := []tombstone{{Query: `service.name:="x"`, StartNs: 10, EndNs: 20}}
	fields := fieldsFor("service.name", "x")

	if !rowTombstoned(tss, fields, 15) {
		t.Error("a matching row inside the window is tombstoned")
	}
	if rowTombstoned(tss, fields, 5) {
		t.Error("a matching row outside the window is not tombstoned")
	}
	if rowTombstoned(tss, fieldsFor("service.name", "y"), 15) {
		t.Error("a non-matching row is not tombstoned")
	}
	if rowTombstoned(nil, fields, 15) {
		t.Error("no tombstones means nothing is tombstoned")
	}
}

func TestRowTimestampNs_MissingColumnReadsAsZero(t *testing.T) {
	if got := rowTimestampNs(nil, -1); got != 0 {
		t.Errorf("an unprojected timestamp column must read as 0, got %d", got)
	}
	if got := rowTimestampNs(nil, 5); got != 0 {
		t.Errorf("an out-of-range index must read as 0, got %d", got)
	}
}

// fieldsFor builds the []logstorage.Field shape the tombstone predicate is
// evaluated against.
func fieldsFor(kv ...string) []logstorage.Field {
	out := make([]logstorage.Field, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, logstorage.Field{Name: kv[i], Value: kv[i+1]})
	}
	return out
}
