package parquets3

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fvMutantFile registers one object (a fvPropFile) with a caller-shaped
// manifest entry, so a test controls RowCount and the label aggregates.
func fvMutantFile(t *testing.T, s *Storage, mock *mockS3Server, name string, data []byte, base time.Time, rowCount int64, agg map[string]map[string]int64) {
	t.Helper()
	key := fmt.Sprintf("traces/dt=%s/hour=%02d/%s.parquet", base.Format("2006-01-02"), base.Hour(), name)
	mock.putFile(key, data)
	s.manifest.AddFile(fmt.Sprintf("dt=%s/hour=%02d", base.Format("2006-01-02"), base.Hour()), manifest.FileInfo{
		Key: key, Size: int64(len(data)), RowCount: rowCount,
		MinTimeNs: base.UnixNano(), MaxTimeNs: base.Add(time.Minute).UnixNano(),
		LabelAggregates: agg,
	})
}

func fvMutantRows(n int, level string) []fvPropRow {
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := make([]fvPropRow, n)
	for i := range rows {
		rows[i] = fvPropRow{ts: base.Add(time.Duration(i) * time.Millisecond).UnixNano(), level: level, svc: "a",
			maps: map[string]map[string]string{}, hasMap: map[string]bool{}}
	}
	return rows
}

// A manifest entry with label aggregates but no row count cannot give the empty
// bucket (it is the row count minus the counted values): the object is scanned
// instead, and the rows without the value are not lost.
func TestFieldValuesEmpty_AggregateWithoutRowCountIsScanned(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := fvMutantRows(6, "INFO")
	for i := range rows[:2] {
		rows[i].level = ""
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "span.name", "service.name", "resource.attributes", "span.attributes", "scope.attributes"}, rows, 0)
	fvMutantFile(t, s, mock, "agg0", data, base, 0, map[string]map[string]int64{"span.name": {"INFO": 4}})
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "name", false)
	if want := map[string]uint64{"INFO": 4, "": 2}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("field_values(name) with aggregates and RowCount 0 = %v, want %v", got, want)
	}
}

// With the row count known, the aggregate path adds the rows without a value
// under "" — and equals the scan of the same object.
func TestFieldValuesEmpty_AggregateWithRowCountEqualsScan(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := fvMutantRows(6, "INFO")
	for i := range rows[:2] {
		rows[i].level = ""
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "span.name", "service.name", "resource.attributes", "span.attributes", "scope.attributes"}, rows, 0)
	fvMutantFile(t, s, mock, "agg6", data, base, 6, map[string]map[string]int64{"span.name": {"INFO": 4}})
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	agg := fvEmptyValues(t, s, "*", lo, hi, "name", false)
	scan := fvEmptyValues(t, s, "`resource_attr:service.name`:=a", lo, hi, "name", false)
	want := map[string]uint64{"INFO": 4, "": 2}
	if fmt.Sprint(agg) != fmt.Sprint(want) || fmt.Sprint(scan) != fmt.Sprint(want) {
		t.Errorf("aggregate=%v scan=%v, want %v", agg, scan, want)
	}
}

// An object that lacks the column altogether, lying inside the window: its rows
// are all "" — answered from the row count with no data page read when nothing
// narrows them, and by reading the rows when a filter or a tombstone does.
func TestFieldValuesEmpty_MissingColumnInsideWindow(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	// Enough payload that the footer read-ahead does not cover the object.
	const n = 4000
	rows := fvMutantRows(n, "")
	for i := range rows {
		if i%2 == 0 {
			rows[i].svc = "b"
		}
		rows[i].hasMap["resource.attributes"] = true
		rows[i].maps["resource.attributes"] = map[string]string{"k": fmt.Sprintf("value-%d-%d", i, i*7919%100003)}
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "service.name", "resource.attributes"}, rows, 3)
	fvMutantFile(t, s, mock, "nolevel", data, base, n+7, nil)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()

	// The manifest says one row more than the object holds: an answer from
	// the row count (no data page read) shows it, a scan of the rows does not.
	got := fvEmptyValues(t, s, "*", lo, hi, "name", false)
	if want := map[string]uint64{"": n + 7}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("unfiltered = %v, want the manifest row count %v", got, want)
	}
	filtered := fvEmptyValues(t, s, "`resource_attr:service.name`:=b", lo, hi, "name", false)
	if want := map[string]uint64{"": n / 2}; fmt.Sprint(filtered) != fmt.Sprint(want) {
		t.Errorf("filtered = %v, want %v", filtered, want)
	}
	// stream_field_values never lists the empty value.
	if sv := fvEmptyValues(t, s, "*", lo, hi, "name", true); len(sv) != 0 {
		t.Errorf("stream_field_values = %v, want none", sv)
	}
}

// A MAP attribute over a window that cuts an object: only rows inside count.
func TestFieldValuesEmpty_MapAttrStraddlingWindow(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	var rows []fvPropRow
	for i := 0; i < 10; i++ {
		rows = append(rows, fvPropRow{ts: base.Add(time.Duration(i) * time.Second).UnixNano(), level: "op", svc: "a",
			maps:   map[string]map[string]string{"span.attributes": {"sa": fmt.Sprintf("v%d", i)}},
			hasMap: map[string]bool{"span.attributes": true}})
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "span.name", "service.name", "resource.attributes", "span.attributes", "scope.attributes"}, rows, 4)
	fvMutantFile(t, s, mock, "win", data, base, 10, nil)
	lo, hi := base.Add(3*time.Second).UnixNano(), base.Add(4*time.Second).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "span_attr:sa", false)
	if want := map[string]uint64{"v3": 1, "v4": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("field_values(span_attr:sa) window [3s,4s] = %v, want %v", got, want)
	}
}

// A hide-mode delete removes a MAP attribute's values, and the rows from the
// empty bucket, on the map read path too.
func TestFieldValuesEmpty_MapAttrTombstoneApplied(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "traces/", s.cfg.Mode)
	now := time.Now().UnixNano()
	bw.stageTraceRows([]schema.TraceRow{
		{TimestampUnixNano: now, ServiceName: "api-gateway", SpanName: "a", TraceID: "t1", SpanID: "s1", SpanAttributes: map[string]string{"sa": "keep"}},
		{TimestampUnixNano: now, ServiceName: "order-service", SpanName: "b", TraceID: "t2", SpanID: "s2", SpanAttributes: map[string]string{"sa": "gone"}},
		{TimestampUnixNano: now, ServiceName: "order-service", SpanName: "c", TraceID: "t3", SpanID: "s3"},
	})
	bw.flushStagedNow()
	f := &traceFieldsTombstoneFixture{storage: s, startNs: now - int64(time.Hour), endNs: now + int64(time.Hour)}
	f.addTombstone()
	got := fvEmptyValues(t, s, "span_id:*", f.startNs, f.endNs, "span_attr:sa", false)
	if want := map[string]uint64{"keep": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("field_values(span_attr:sa) after deleting order-service = %v, want %v", got, want)
	}
}

// The aggregate path (unfiltered, object inside the window) and the scan path
// give the same empty bucket for spans without a service name.
func TestFieldValuesEmpty_TracesAggregateEqualsScan(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "traces/", config.ModeTraces)
	now := time.Now().UnixNano()
	var rows []schema.TraceRow
	for i, svc := range []string{"a", "a", "", "b", ""} {
		rows = append(rows, schema.TraceRow{TimestampUnixNano: now + int64(i), ServiceName: svc, SpanName: "op", TraceID: fmt.Sprintf("t%d", i), SpanID: fmt.Sprintf("%016x", i+1)})
	}
	bw.stageTraceRows(rows)
	bw.flushStagedNow()
	lo, hi := now-int64(time.Hour), now+int64(time.Hour)
	agg := fvEmptyValues(t, s, "*", lo, hi, "resource_attr:service.name", false)
	scan := fvEmptyValues(t, s, "span_id:*", lo, hi, "resource_attr:service.name", false)
	want := map[string]uint64{"": 2, "a": 2, "b": 1}
	if fmt.Sprint(agg) != fmt.Sprint(want) || fmt.Sprint(scan) != fmt.Sprint(want) {
		t.Errorf("aggregate=%v scan=%v, want %v", agg, scan, want)
	}
}

// fvEmptyValues answers field_values (or stream_field_values) as value -> hits.
func fvEmptyValues(t *testing.T, s *Storage, q string, lo, hi int64, field string, stream bool) map[string]uint64 {
	t.Helper()
	qq := mustParseQueryWithTime(t, q, lo, hi)
	get := s.GetFieldValues
	if stream {
		get = s.GetStreamFieldValues
	}
	vs, err := get(context.Background(), nil, qq, field, 0)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]uint64{}
	for _, v := range vs {
		m[v.Value] = v.Hits
	}
	return m
}

// An object that lacks the column and straddles the window is read row by row,
// never answered from its row count: only the rows inside the window count.
func TestFieldValuesEmpty_MissingColumnStraddlingWindowWithRowCount(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := make([]fvPropRow, 10)
	for i := range rows {
		rows[i] = fvPropRow{ts: base.Add(time.Duration(i) * time.Second).UnixNano(), svc: "a", maps: map[string]map[string]string{}, hasMap: map[string]bool{}}
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "service.name", "resource.attributes"}, rows, 4)
	key := fmt.Sprintf("traces/dt=%s/hour=%02d/straddle.parquet", base.Format("2006-01-02"), base.Hour())
	mock.putFile(key, data)
	s.manifest.AddFile(fmt.Sprintf("dt=%s/hour=%02d", base.Format("2006-01-02"), base.Hour()), manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 10,
		MinTimeNs: base.UnixNano(), MaxTimeNs: base.Add(9 * time.Second).UnixNano()})
	lo, hi := base.Add(3*time.Second).UnixNano(), base.Add(6*time.Second).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "name", false)
	if want := map[string]uint64{"": 4}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("window [3s,6s] over a 10-row object without the column = %v, want %v", got, want)
	}
}

// The MAP reader expands only the keys a request names: the keys of the target,
// of its filter and of its tombstones, in their bare and prefixed spellings.
func TestMapScanKeys_OnlyTheRequestedKeys(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	filter := parseFilterFromQuery(mustParseQueryWithTime(t, "`span_attr:user`:=u1 name:=ERROR", 0, 1))
	got := s.mapScanKeys("span_attr:lk", filter, nil)
	for _, want := range []string{"lk", "user", "name", "_time"} {
		if _, ok := got[want]; !ok {
			t.Errorf("mapScanKeys misses %q: %v", want, got)
		}
	}
	if len(got) != 6 {
		t.Errorf("mapScanKeys = %v, want exactly lk, user, name, _time and the prefixed spellings of lk and user", got)
	}
	pre := s.mapScanKeys("resource_attr:lk", nil, nil)
	if _, ok := pre["lk"]; !ok {
		t.Errorf("a prefixed field must also name its bare key: %v", pre)
	}
	if k := s.mapScanKeys("*", nil, nil); k != nil {
		t.Errorf("a wildcard cannot be bounded, got %v", k)
	}
}

// Expanding one key of two wide maps must not stringify every other key: the
// allocations stay far below one per attribute per row.
func TestFieldValuesEmpty_MapReadExpandsOnlyTheRequestedKey(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	const n, width = 300, 40
	rows := make([]fvPropRow, n)
	for i := range rows {
		attrs := map[string]string{"lk": fmt.Sprintf("v%d", i%5)}
		for k := 0; k < width; k++ {
			attrs[fmt.Sprintf("other%d", k)] = fmt.Sprintf("x%d-%d", k, i)
		}
		rows[i] = fvPropRow{ts: base.Add(time.Duration(i) * time.Millisecond).UnixNano(), level: "op", svc: "a",
			maps: map[string]map[string]string{"span.attributes": attrs}, hasMap: map[string]bool{"span.attributes": true}}
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "span.name", "service.name", "resource.attributes", "span.attributes", "scope.attributes"}, rows, 0)
	fvMutantFile(t, s, mock, "wide", data, base, n, nil)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "span_attr:lk", false)
	want := map[string]uint64{"v0": 60, "v1": 60, "v2": 60, "v3": 60, "v4": 60}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values(span_attr:lk) = %v, want %v", got, want)
	}
	q := mustParseQueryWithTime(t, "*", lo, hi)
	allocs := testing.AllocsPerRun(5, func() { _, _ = s.GetFieldValues(context.Background(), nil, q, "span_attr:lk", 0) })
	if limit := float64(n * 8); allocs > limit {
		t.Errorf("field_values(lk) over %d rows x %d keys made %.0f allocations, want at most %.0f (one key expanded)", n, width, allocs, limit)
	}
}

// One top-level span attribute key (emitted without a prefix) present in two
// MAP columns is one field: per span the first non-empty value wins, and no
// span reads as carrying it twice with one of them empty.
func TestFieldValuesEmpty_SameTopLevelKeyInTwoMaps(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := []fvPropRow{
		{ts: base.UnixNano(), level: "op", svc: "a",
			maps:   map[string]map[string]string{"span.attributes": {"flags": "1"}, "scope.attributes": {}},
			hasMap: map[string]bool{"span.attributes": true, "scope.attributes": true}},
		{ts: base.Add(time.Second).UnixNano(), level: "op", svc: "a",
			maps:   map[string]map[string]string{"span.attributes": {}, "scope.attributes": {"flags": "2"}},
			hasMap: map[string]bool{"span.attributes": true, "scope.attributes": true}},
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "span.name", "service.name", "resource.attributes", "span.attributes", "scope.attributes"}, rows, 0)
	fvMutantFile(t, s, mock, "twomaps", data, base, 2, nil)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "flags", false)
	if want := map[string]uint64{"1": 1, "2": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("field_values(flags) = %v, want %v (each span carries it once)", got, want)
	}
}

// An event or link field over spans that carry no events, and over an object
// written before the events column existed: every span is one hit of the empty
// value (the composite column is never read through the attribute maps).
func TestFieldValuesEmpty_EventFieldsOverSpansWithoutEventsOrColumn(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := fvMutantRows(6, "op")
	for i := range rows {
		rows[i].hasMap = map[string]bool{"span.attributes": true}
		rows[i].maps = map[string]map[string]string{"span.attributes": {"event_name": "decoy"}}
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "span.name", "service.name", "resource.attributes", "span.attributes", "scope.attributes"}, rows, 3)
	fvMutantFile(t, s, mock, "oldfile", data, base, 6, nil)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	for _, field := range []string{"event:event_name:0", "link:link_span_id:0"} {
		for _, q := range []string{"*", "`resource_attr:service.name`:=a"} {
			got := fvEmptyValues(t, s, q, lo, hi, field, false)
			if want := map[string]uint64{"": 6}; fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("field_values(%s) q=%s over an object without the events column = %v, want %v", field, q, got, want)
			}
		}
	}
}

// An event field combined with a MAP attribute filter or tombstone: the
// attribute columns are read next to the composite column, so the filter and the
// tombstone see the span's attribute and the event name is still the target.
func TestFieldValuesEmpty_EventTargetWithMapAttributeFilterAndTombstone(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	mk := func(i int, event, attr string) schema.TraceRow {
		var c schema.SpanSubFieldCollector
		if event != "" {
			c.Add("event:event_name:0", event)
		}
		r := schema.TraceRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(), StartTimeUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
			TraceID: fmt.Sprintf("t%d", i), SpanID: fmt.Sprintf("%016x", i+1), SpanName: "op", ServiceName: "svc",
			Stream: `{resource_attr:service.name="svc"}`,
		}
		if attr != "" {
			r.SpanAttributes = map[string]string{"k": attr}
		}
		c.Apply(&r)
		return r
	}
	rows := []schema.TraceRow{
		mk(0, "exception", "x"), mk(1, "log", "x"), mk(2, "", "x"),
		mk(3, "exception", "y"), mk(4, "retry", "y"), mk(5, "", ""),
	}
	res, err := writeTracesParquet(rows, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/ev.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	const field = "event:event_name:0"

	got := fvEmptyValues(t, s, "`span_attr:k`:=x", lo, hi, field, false)
	if want := map[string]uint64{"exception": 1, "log": 1, "": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("filter span_attr:k=x: %v, want %v", got, want)
	}
	f := &traceFieldsTombstoneFixture{storage: s, startNs: lo, endNs: hi}
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "ts-ev", Query: "`span_attr:k`:=x", StartNs: lo, EndNs: hi, Mode: "hide"})
	f.storage.SetTombstoneStore(store)
	got = fvEmptyValues(t, s, "*", lo, hi, field, false)
	if want := map[string]uint64{"exception": 1, "retry": 1, "": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("tombstone span_attr:k=x: %v, want %v", got, want)
	}
}

// One top-level span attribute key (emitted without a prefix) in two MAP
// columns with different values: the first occurrence (span, then scope) wins on
// every read, whatever Go's map order.
func TestFieldValuesEmpty_SameKeyTwoMapsIsResourceFirstOnEveryRead(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := make([]fvPropRow, 4)
	for i := range rows {
		rows[i] = fvPropRow{ts: base.Add(time.Duration(i) * time.Second).UnixNano(), level: "op", svc: "a",
			maps: map[string]map[string]string{
				"resource.attributes": {"a1": "x"},
				"span.attributes":     {"flags": "span", "b1": "x"},
				"scope.attributes":    {"flags": "scope", "c1": "x"},
			},
			hasMap: map[string]bool{"resource.attributes": true, "span.attributes": true, "scope.attributes": true}}
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "span.name", "service.name", "scope.attributes", "span.attributes", "resource.attributes"}, rows, 0)
	fvMutantFile(t, s, mock, "twok", data, base, 4, nil)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	seen := map[string]int{}
	for i := 0; i < 60; i++ {
		seen[fmt.Sprint(fvEmptyValues(t, s, "*", lo, hi, "flags", false))]++
	}
	if len(seen) != 1 {
		t.Fatalf("answers differ between reads: %v", seen)
	}
	for a := range seen {
		if a != "map[span:4]" {
			t.Errorf("answer %s: the span attribute comes first and must win", a)
		}
	}
}
