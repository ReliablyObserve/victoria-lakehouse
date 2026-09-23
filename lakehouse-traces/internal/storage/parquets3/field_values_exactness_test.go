package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Twin of internal/storage/parquets3/field_values_exactness_test.go.

func newFieldValuesStorage(t *testing.T, pmetaCfg *config.PmetaConfig) (*Storage, *BatchWriter) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	if pmetaCfg != nil {
		s.cfg.Pmeta = *pmetaCfg
		s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog}
	}
	return s, bw
}

func spanRows(at time.Time, names ...string) []schema.TraceRow {
	rows := make([]schema.TraceRow, 0, len(names))
	for i, n := range names {
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: at.Add(time.Duration(i) * time.Second).UnixNano(),
			ServiceName:       "svc", SpanName: n,
			TraceID: n + at.Format("150405"), SpanID: n + at.Format("150405"),
		})
	}
	return rows
}

func TestFieldValues_CatalogUnionWithAHighCardPartitionScans(t *testing.T) {
	s, bw := newFieldValuesStorage(t, &config.PmetaConfig{Enabled: true, CardinalityThreshold: 2})
	a := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	b := a.Add(2 * time.Hour)
	bw.AddTraceRows(append(spanRows(a, "GET /a", "POST /b"), spanRows(b, "PUT /c", "DELETE /d", "GET /a")...))
	bw.triggerFlush()

	got := fieldValueSet(t, s, a.Add(-time.Hour).UnixNano(), b.Add(time.Hour).UnixNano(), "name", 0)
	if want := []string{"DELETE /d", "GET /a", "POST /b", "PUT /c"}; !equalStrings(got, want) {
		t.Fatalf("field_values name = %v, want %v (a high-card partition must not be read as empty)", got, want)
	}
}

func TestFieldValues_CatalogMissingAFileScans(t *testing.T) {
	s, bw := newFieldValuesStorage(t, &config.PmetaConfig{Enabled: true})
	obs := bw.catalogObserver
	bw.catalogObserver = nil
	at := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	bw.AddTraceRows(spanRows(at, "GET /a"))
	bw.triggerFlush()
	bw.catalogObserver = obs
	bw.AddTraceRows(spanRows(at.Add(time.Minute), "POST /b"))
	bw.triggerFlush()

	got := fieldValueSet(t, s, at.Add(-time.Hour).UnixNano(), at.Add(time.Hour).UnixNano(), "name", 0)
	if want := []string{"GET /a", "POST /b"}; !equalStrings(got, want) {
		t.Fatalf("field_values name = %v, want %v (an uncatalogued file must not be skipped)", got, want)
	}
}

func TestFieldValues_CatalogServesWhenComplete(t *testing.T) {
	s, bw := newFieldValuesStorage(t, &config.PmetaConfig{Enabled: true})
	a := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	b := a.Add(2 * time.Hour)
	bw.AddTraceRows(append(spanRows(a, "GET /a", "POST /b"), spanRows(b, "PUT /c")...))
	bw.triggerFlush()

	before := metrics.CatalogValueLookups.Get("catalog")
	got := fieldValueSet(t, s, a.Add(-time.Hour).UnixNano(), b.Add(time.Hour).UnixNano(), "name", 0)
	if want := []string{"GET /a", "POST /b", "PUT /c"}; !equalStrings(got, want) {
		t.Fatalf("field_values name = %v, want %v", got, want)
	}
	if metrics.CatalogValueLookups.Get("catalog") <= before {
		t.Fatal("a complete, low-card catalog did not serve the request")
	}
}

func TestFieldValues_ScanIsConfinedToTheWindow(t *testing.T) {
	s, bw := newFieldValuesStorage(t, nil)
	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	bw.AddTraceRows(append(spanRows(base.Add(15*time.Minute), "GET /a", "GET /a"), spanRows(base.Add(45*time.Minute), "POST /b")...))
	bw.triggerFlush()
	if n := len(s.manifest.GetFilesForRange(base.UnixNano(), base.Add(time.Hour).UnixNano())); n != 1 {
		t.Fatalf("fixture: want one file straddling the window, got %d", n)
	}

	lo, hi := base.UnixNano(), base.Add(30*time.Minute).UnixNano()
	for _, query := range []string{"*", `service.name:="svc"`} {
		q := mustParseQueryWithTime(t, query, lo, hi)
		vals, err := s.GetFieldValues(context.Background(), nil, q, "name", 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(vals) != 1 || vals[0].Value != "GET /a" || vals[0].Hits != 2 {
			t.Errorf("%s: field_values name over [10:00,10:30] = %+v, want [{GET /a 2}]", query, vals)
		}
	}
	got := fieldValueSet(t, s, base.UnixNano(), base.Add(time.Hour).UnixNano(), "name", 0)
	if want := []string{"GET /a", "POST /b"}; !equalStrings(got, want) {
		t.Errorf("whole-hour window = %v, want %v", got, want)
	}
}

// TestFieldValues_ScanWindowBoundsAreInclusive: the window is [startNs, endNs]
// inclusive at both ends. One file holds a span 1 ns before the window, one at
// each bound and one 1 ns after it; the scan lists exactly the two bound spans,
// each with one hit — unfiltered, with a user filter, and unfiltered while an
// unrelated tombstone is active (rows materialised with no filter at all).
func TestFieldValues_ScanWindowBoundsAreInclusive(t *testing.T) {
	s, bw := newFieldValuesStorage(t, nil)
	startNs := time.Date(2026, 6, 9, 10, 10, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 6, 9, 10, 20, 0, 0, time.UTC).UnixNano()
	row := func(ts int64, name string) schema.TraceRow {
		return schema.TraceRow{TimestampUnixNano: ts, ServiceName: "svc", SpanName: name, TraceID: "t-" + name, SpanID: "s-" + name}
	}
	bw.AddTraceRows([]schema.TraceRow{
		row(startNs-1, "BEFORE"), row(startNs, "AT_START"), row(endNs, "AT_END"), row(endNs+1, "AFTER"),
	})
	bw.triggerFlush()
	if n := len(s.manifest.GetFilesForRange(startNs, endNs)); n != 1 {
		t.Fatalf("fixture: want one file straddling both bounds, got %d", n)
	}

	check := func(name, query string) {
		t.Helper()
		q := mustParseQueryWithTime(t, query, startNs, endNs)
		vals, err := s.GetFieldValues(context.Background(), nil, q, "name", 0)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]uint64{}
		for _, v := range vals {
			got[v.Value] = v.Hits
		}
		if len(got) != 2 || got["AT_START"] != 1 || got["AT_END"] != 1 {
			t.Errorf("%s: field_values name over [startNs, endNs] = %v, want exactly {AT_START:1 AT_END:1}", name, got)
		}
	}
	check("unfiltered", "*")
	check("filtered", `service.name:="svc"`)

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "unrelated", Query: `service.name:="other"`,
		StartNs: startNs - int64(time.Minute), EndNs: endNs + int64(time.Minute), Mode: "hide"})
	s.SetTombstoneStore(store)
	check("unfiltered with an active tombstone", "*")
}

func TestStreams_ScanIsConfinedToTheWindow(t *testing.T) {
	s, bw := newFieldValuesStorage(t, nil)
	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	in := spanRows(base.Add(15*time.Minute), "GET /a")
	in[0].Stream, in[0].StreamID = `{resource_attr:service.name="in-window"}`, "id-in-window"
	out := spanRows(base.Add(45*time.Minute), "POST /b")
	out[0].Stream, out[0].StreamID = `{resource_attr:service.name="outside"}`, "id-outside"
	bw.AddTraceRows(append(in, out...))
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
		t.Errorf("streams=%v stream_ids=%v, want one of each (only the in-window span's stream)", streams, ids)
	}
}

// TestFieldNames_EmptyWindowIsEmpty: a window holding no objects has no field
// names, whatever the label index remembers.
func TestFieldNames_EmptyWindowIsEmpty(t *testing.T) {
	s := testStorage()
	soleTenantManifest(t, s)
	s.labelIndex.Add("service.name", []string{"api"})
	s.labelIndex.Add("span_name", []string{"GET /"})
	q := mustParseQueryWithTime(t, "*", time.Now().Add(-time.Hour).UnixNano(), time.Now().UnixNano())
	names, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatalf("field_names over a window with no objects = %v, want none", names)
	}
}
