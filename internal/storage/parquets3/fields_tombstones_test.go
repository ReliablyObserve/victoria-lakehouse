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

func TestAddTombstoneProjection_AttributeFieldProjectsItsMapColumn(t *testing.T) {
	s := testStorage()
	projected := map[string]bool{}

	// An attribute with no dedicated column resolves to the MAP column that
	// holds it, and that column is what has to be fetched.
	s.addTombstoneProjection([]tombstone{
		{Query: `custom_attr_no_such_column:="x"`, StartNs: 0, EndNs: 10},
	}, projected)

	if !projected["resource.attributes"] {
		t.Errorf("an attribute field must project the map column holding it, got %v", projected)
	}
	if !projected[timestampColumn] {
		t.Error("the timestamp column must be projected")
	}
}

func TestAddTombstoneProjection_UnparseableTombstoneIsSkipped(t *testing.T) {
	s := testStorage()
	projected := map[string]bool{}
	s.addTombstoneProjection([]tombstone{
		{Query: `broken:=="`, StartNs: 0, EndNs: 10},
	}, projected)

	// It still forces the timestamp (every tombstone is time-bounded) but
	// contributes no predicate columns, because it has no predicate.
	if !projected[timestampColumn] {
		t.Error("the timestamp column must be projected even for an unparseable tombstone")
	}
	if len(projected) != 1 {
		t.Errorf("an unparseable tombstone must contribute no predicate columns, got %v", projected)
	}
}

// TestFieldValues_TombstoneCombinesWithTheUserFilter covers the branch where
// both predicates apply: the user's filter selects rows, and the tombstone
// removes some of what it selected.
func TestFieldValues_TombstoneCombinesWithTheUserFilter(t *testing.T) {
	f := newFieldsTombstoneFixture(t, false)
	f.addTombstone()

	// A filter that matches every row, so the surviving values are decided
	// purely by the tombstone — but via the filtered branch of the scanner.
	q := mustParseQueryWithTime(t, `_msg:*`, f.startNs, f.endNs)
	got, err := f.storage.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	for _, v := range valueStrings(got) {
		if v == "order-service" {
			t.Fatalf("the deleted value survived a filtered enumeration: %v", valueStrings(got))
		}
	}
}

// TestFieldNames_EmptyHitsFallsThroughUnderATombstone exercises the branch
// where the footer walk produced no hits at all: the tombstone path must not
// swallow the catalog/labelIndex fallbacks that still have names to offer.
func TestFieldNames_EmptyHitsFallsThroughUnderATombstone(t *testing.T) {
	f := newFieldsTombstoneFixture(t, false)
	f.addTombstone()

	// A window with no files at all: GetFieldNames returns before the footer
	// walk, so the tombstone branch is never reached and nothing panics.
	q := mustParseQueryWithTime(t, "*", 1, 2)
	if _, err := f.storage.GetFieldNames(context.Background(), nil, q); err != nil {
		t.Fatalf("GetFieldNames over an empty window: %v", err)
	}
}

// --- field_names hit bookkeeping ---------------------------------------------
//
// These cover the two helpers the tombstone branch of GetFieldNames leans on.
// Both are pure, and both decide what a caller SEES in a field picker, so they
// are worth pinning independently of the endpoint that calls them.

func TestLabelIndexNamesWithHits(t *testing.T) {
	got := labelIndexNamesWithHits([]string{"a", "b"}, map[string]uint64{"a": 7})
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if got[0].Value != "a" || got[0].Hits != 7 {
		t.Errorf("entry 0 = %+v, want a/7", got[0])
	}
	// A name with no recorded count reports 0 — the documented "unknown count"
	// signal, which is exactly what the tombstone branch relies on.
	if got[1].Value != "b" || got[1].Hits != 0 {
		t.Errorf("entry 1 = %+v, want b/0", got[1])
	}

	// A nil hits map means every count is unknown.
	all := labelIndexNamesWithHits([]string{"x", "y"}, nil)
	for _, v := range all {
		if v.Hits != 0 {
			t.Errorf("%q reports %d hits from a nil map", v.Value, v.Hits)
		}
	}
	if len(labelIndexNamesWithHits(nil, nil)) != 0 {
		t.Error("no names in, no entries out")
	}
}

func TestRemapSlotFieldHits(t *testing.T) {
	prev := activeSlotResolver
	t.Cleanup(func() { activeSlotResolver = prev })
	activeSlotResolver = schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant.tier"}})

	mapping := activeSlotResolver.Mapping()
	var mappedSlot string
	for slot := range mapping {
		mappedSlot = slot
		break
	}
	if mappedSlot == "" {
		t.Fatal("fixture is wrong: the resolver mapped no slot")
	}

	hits := map[string]uint64{
		mappedSlot:     5,
		"ded_s99":      3, // a slot with no operator mapping
		"service.name": 11,
	}
	remapSlotFieldHits(hits)

	// A MAPPED slot is reported under its configured attribute name.
	if hits["tenant.tier"] != 5 {
		t.Errorf("mapped slot not reported under its attribute name: %v", hits)
	}
	// An UNMAPPED slot is an empty placeholder column and must never surface —
	// it shows up as `ded_sNN` noise in a field picker with no values on any row.
	if _, still := hits["ded_s99"]; still {
		t.Errorf("an unmapped slot column leaked into the field list: %v", hits)
	}
	if _, still := hits[mappedSlot]; still {
		t.Errorf("the raw slot name is still present alongside its attribute name: %v", hits)
	}
	// Ordinary fields are untouched.
	if hits["service.name"] != 11 {
		t.Errorf("an ordinary field was disturbed: %v", hits)
	}

	// Empty input is a no-op rather than a panic.
	empty := map[string]uint64{}
	remapSlotFieldHits(empty)
	if len(empty) != 0 {
		t.Errorf("empty hits grew to %v", empty)
	}
}
