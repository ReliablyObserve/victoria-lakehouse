package parquets3

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Exactness of field_values / field_names on the cold tier, after the sampled
// label index stopped being an answer. Twin of
// lakehouse-traces/internal/storage/parquets3/field_values_exactness_test.go.

func newFieldValuesStorage(t *testing.T, pmetaCfg *config.PmetaConfig) (*Storage, *BatchWriter) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	if pmetaCfg != nil {
		s.cfg.Pmeta = *pmetaCfg
		s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog}
	}
	return s, bw
}

func levelRows(at time.Time, levels ...string) []schema.LogRow {
	rows := make([]schema.LogRow, 0, len(levels))
	for i, lvl := range levels {
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: at.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "row", ServiceName: "svc", SeverityText: lvl,
		})
	}
	return rows
}

// TestFieldValues_CatalogUnionWithAHighCardPartitionScans: a partition where
// the field is high-card has no enumerable values in the catalog. Treating that
// as "no values" and unioning the other partitions returned a strict subset;
// the whole answer must come from the rows instead.
func TestFieldValues_CatalogUnionWithAHighCardPartitionScans(t *testing.T) {
	s, bw := newFieldValuesStorage(t, &config.PmetaConfig{Enabled: true, CardinalityThreshold: 2})
	a := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	b := a.Add(2 * time.Hour)
	bw.AddLogRows(append(levelRows(a, "INFO", "ERROR"), levelRows(b, "DEBUG", "WARN", "INFO")...))
	bw.triggerFlush()

	got := fieldValueSet(t, s, a.Add(-time.Hour).UnixNano(), b.Add(time.Hour).UnixNano(), "level", 0)
	if want := []string{"DEBUG", "ERROR", "INFO", "WARN"}; !equalStrings(got, want) {
		t.Fatalf("field_values level = %v, want %v (a high-card partition must not be read as empty)", got, want)
	}
}

// TestFieldValues_CatalogMissingAFileScans: a file in range whose labels never
// reached the catalog (written before the observer was attached — the same
// shape as a file another writer flushed, or a manifest entry without labels)
// leaves the catalog incomplete for its partition; the answer must come from
// the rows.
func TestFieldValues_CatalogMissingAFileScans(t *testing.T) {
	s, bw := newFieldValuesStorage(t, &config.PmetaConfig{Enabled: true})
	obs := bw.catalogObserver
	bw.catalogObserver = nil
	at := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	bw.AddLogRows(levelRows(at, "ERROR"))
	bw.triggerFlush()
	bw.catalogObserver = obs
	bw.AddLogRows(levelRows(at.Add(time.Minute), "INFO"))
	bw.triggerFlush()

	got := fieldValueSet(t, s, at.Add(-time.Hour).UnixNano(), at.Add(time.Hour).UnixNano(), "level", 0)
	if want := []string{"ERROR", "INFO"}; !equalStrings(got, want) {
		t.Fatalf("field_values level = %v, want %v (an uncatalogued file must not be skipped)", got, want)
	}
}

// TestFieldValues_CatalogServesWhenComplete: the exactness checks must not
// cost the fast path when every partition in range is catalogued and low-card.
func TestFieldValues_CatalogServesWhenComplete(t *testing.T) {
	s, bw := newFieldValuesStorage(t, &config.PmetaConfig{Enabled: true})
	a := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	b := a.Add(2 * time.Hour)
	bw.AddLogRows(append(levelRows(a, "INFO", "ERROR"), levelRows(b, "DEBUG", "WARN")...))
	bw.triggerFlush()

	before := metrics.CatalogValueLookups.Get("catalog")
	got := fieldValueSet(t, s, a.Add(-time.Hour).UnixNano(), b.Add(time.Hour).UnixNano(), "level", 0)
	if want := []string{"DEBUG", "ERROR", "INFO", "WARN"}; !equalStrings(got, want) {
		t.Fatalf("field_values level = %v, want %v", got, want)
	}
	if metrics.CatalogValueLookups.Get("catalog") <= before {
		t.Fatal("a complete, low-card catalog did not serve the request")
	}
}

// TestFieldValues_ScanIsConfinedToTheWindow: one file holds INFO at 10:15 and
// ERROR at 10:45; a window of [10:00, 10:30] must list INFO only, with the
// in-window hit count — unfiltered and filtered alike (the filter handed to the
// scan carries no time bound of its own).
func TestFieldValues_ScanIsConfinedToTheWindow(t *testing.T) {
	s, bw := newFieldValuesStorage(t, nil)
	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	bw.AddLogRows(append(levelRows(base.Add(15*time.Minute), "INFO", "INFO"), levelRows(base.Add(45*time.Minute), "ERROR")...))
	bw.triggerFlush()
	if n := len(s.manifest.GetFilesForRange(base.UnixNano(), base.Add(time.Hour).UnixNano())); n != 1 {
		t.Fatalf("fixture: want one file straddling the window, got %d", n)
	}

	lo, hi := base.UnixNano(), base.Add(30*time.Minute).UnixNano()
	for _, query := range []string{"*", `service.name:="svc"`} {
		q := mustParseQueryWithTime(t, query, lo, hi)
		vals, err := s.GetFieldValues(context.Background(), nil, q, "level", 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(vals) != 1 || vals[0].Value != "INFO" || vals[0].Hits != 2 {
			t.Errorf("%s: field_values level over [10:00,10:30] = %+v, want [{INFO 2}]", query, vals)
		}
	}
	// A window covering the whole file still takes every row.
	got := fieldValueSet(t, s, base.UnixNano(), base.Add(time.Hour).UnixNano(), "level", 0)
	if want := []string{"ERROR", "INFO"}; !equalStrings(got, want) {
		t.Errorf("whole-hour window = %v, want %v", got, want)
	}
}

// TestStreams_ScanIsConfinedToTheWindow: streams and stream_ids use the same
// scan and must not list a stream whose rows all lie outside the window.
func TestStreams_ScanIsConfinedToTheWindow(t *testing.T) {
	s, bw := newFieldValuesStorage(t, nil)
	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: base.Add(15 * time.Minute).UnixNano(), Body: "a", ServiceName: "in-window", SeverityText: "INFO",
			Stream: `{service.name="in-window"}`, StreamID: "id-in-window"},
		{TimestampUnixNano: base.Add(45 * time.Minute).UnixNano(), Body: "b", ServiceName: "outside", SeverityText: "INFO",
			Stream: `{service.name="outside"}`, StreamID: "id-outside"},
	})
	bw.triggerFlush()
	q := mustParseQueryWithTime(t, "*", base.UnixNano(), base.Add(30*time.Minute).UnixNano())
	streams, err := s.GetStreams(context.Background(), nil, q, 0)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := s.GetStreamIDs(context.Background(), nil, q, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 || len(ids) != 1 {
		t.Errorf("streams=%v stream_ids=%v, want one of each (only the in-window row's stream)", streams, ids)
	}
}

// TestFieldNames_EmptyWindowIsEmpty: a window holding no objects has no field
// names (VictoriaLogs semantics), whatever the label index remembers.
func TestFieldNames_EmptyWindowIsEmpty(t *testing.T) {
	s := testStorage()
	soleTenantManifest(t, s)
	s.labelIndex.Add("service.name", []string{"api"})
	s.labelIndex.Add("level", []string{"INFO"})
	q := mustParseQueryWithTime(t, "*", time.Now().Add(-time.Hour).UnixNano(), time.Now().UnixNano())
	names, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		got := make([]string, 0, len(names))
		for _, n := range names {
			got = append(got, n.Value)
		}
		sort.Strings(got)
		t.Fatalf("field_names over a window with no objects = %v, want none", got)
	}
}
