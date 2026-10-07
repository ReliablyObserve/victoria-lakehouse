package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func fvEmptyValues(t *testing.T, s *Storage, q string, lo, hi int64, field string, stream bool) map[string]uint64 {
	t.Helper()
	qq := mustParseQueryWithTime(t, q, lo, hi)
	var (
		got []valueHits
		err error
	)
	if stream {
		vs, e := s.GetStreamFieldValues(context.Background(), nil, qq, field, 0)
		err = e
		for _, v := range vs {
			got = append(got, valueHits{v.Value, v.Hits})
		}
	} else {
		vs, e := s.GetFieldValues(context.Background(), nil, qq, field, 0)
		err = e
		for _, v := range vs {
			got = append(got, valueHits{v.Value, v.Hits})
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]uint64{}
	for _, v := range got {
		m[v.v] = v.h
	}
	return m
}

type valueHits struct {
	v string
	h uint64
}

// Hot VL v1.53 (measured): stream_field_values never lists "" — it is built
// from the stream tags of GetStreams (forEachStreamField), and a stream
// without the tag contributes nothing.
func TestFieldValuesEmpty_StreamFieldValues_NoEmptyBucket(t *testing.T) {
	s, bw := newFieldValuesStorage(t, nil)
	at := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	var rows []schema.LogRow
	for i, ns := range []string{"n1", "n1", "", ""} {
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: at.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "row", ServiceName: "svc", SeverityText: "INFO", K8sNamespaceName: ns,
		})
	}
	bw.stageLogRows(rows)
	bw.flushStagedNow()
	lo, hi := at.Add(-time.Hour).UnixNano(), at.Add(time.Hour).UnixNano()
	for _, q := range []string{"*", `service.name:=svc`} {
		got := fvEmptyValues(t, s, q, lo, hi, "k8s.namespace.name", true)
		if _, bad := got[""]; bad {
			t.Errorf("stream_field_values(k8s.namespace.name) q=%s = %v; hot VL never lists an empty value here", q, got)
		}
	}
}

// The aggregate path (unfiltered, file inside window) and the scan path
// (filtered) must give the same empty bucket for the same rows.
func TestFieldValuesEmpty_EmptyBucket_AggregateEqualsScan(t *testing.T) {
	s, bw := newFieldValuesStorage(t, nil)
	at := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	var rows []schema.LogRow
	for i, ns := range []string{"n1", "n1", "", "n2", ""} {
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: at.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              "row", ServiceName: "svc", SeverityText: "INFO", K8sNamespaceName: ns,
		})
	}
	bw.stageLogRows(rows)
	bw.flushStagedNow()
	lo, hi := at.Add(-time.Hour).UnixNano(), at.Add(time.Hour).UnixNano()
	agg := fvEmptyValues(t, s, "*", lo, hi, "k8s.namespace.name", false)
	scan := fvEmptyValues(t, s, `service.name:=svc`, lo, hi, "k8s.namespace.name", false)
	want := map[string]uint64{"": 2, "n1": 2, "n2": 1}
	if fmt.Sprint(agg) != fmt.Sprint(want) || fmt.Sprint(scan) != fmt.Sprint(want) {
		t.Errorf("aggregate=%v scan=%v want %v", agg, scan, want)
	}
	// Window straddling the file: only rows in the window count.
	part := fvEmptyValues(t, s, "*", at.Add(2*time.Second).UnixNano(), at.Add(3*time.Second).UnixNano()+1, "k8s.namespace.name", false)
	if want := map[string]uint64{"": 1, "n2": 1}; fmt.Sprint(part) != fmt.Sprint(want) {
		t.Errorf("straddling window = %v, want %v", part, want)
	}
}

// One attribute key carried by different rows in different MAP columns: a row
// has it in resource.attributes, another in log.attributes. Each row carries
// the field, so there is no empty bucket at all.
func TestFieldValuesEmpty_SameKeyInTwoMaps_NoPhantomEmpty(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := []fvPropRow{
		{ts: base.UnixNano(), level: "INFO", svc: "a",
			maps:   map[string]map[string]string{"resource.attributes": {"k": "r"}, "log.attributes": {}},
			hasMap: map[string]bool{"resource.attributes": true, "log.attributes": true}},
		{ts: base.Add(time.Second).UnixNano(), level: "INFO", svc: "a",
			maps:   map[string]map[string]string{"resource.attributes": {}, "log.attributes": {"k": "l"}},
			hasMap: map[string]bool{"resource.attributes": true, "log.attributes": true}},
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "severity_text", "service.name", "resource.attributes", "log.attributes"}, rows, 0)
	key := fmt.Sprintf("logs/dt=%s/hour=%02d/two.parquet", base.Format("2006-01-02"), base.Hour())
	registerFileInMockS3(t, s, mock, key, data, base)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "k", false)
	if want := map[string]uint64{"r": 1, "l": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("field_values(k) = %v, want %v (each row carries k once)", got, want)
	}
}

// A nested LIST column before the scalars: leaf addressing must still find the
// scalar's own chunk, on the positional path and the map path.
type fvEmptyListRow struct {
	TS     int64             `parquet:"timestamp_unix_nano"`
	Events []fvEmptyEvent    `parquet:"events,list"`
	Res    map[string]string `parquet:"resource.attributes,optional"`
	Level  string            `parquet:"severity_text"`
	Svc    string            `parquet:"service.name"`
}

type fvEmptyEvent struct {
	Name  string            `parquet:"name"`
	Attrs map[string]string `parquet:"attrs,optional"`
}

func TestFieldValuesEmpty_ListColumnBeforeScalars(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	in := []fvEmptyListRow{
		{TS: base.UnixNano(), Events: []fvEmptyEvent{{Name: "e1", Attrs: map[string]string{"x": "1"}}, {Name: "e2"}}, Res: map[string]string{"rk": "a"}, Level: "INFO", Svc: "s1"},
		{TS: base.Add(time.Second).UnixNano(), Res: map[string]string{"rk": "b"}, Level: "ERROR", Svc: "s2"},
		{TS: base.Add(2 * time.Second).UnixNano(), Events: []fvEmptyEvent{{Name: "e3"}}, Level: "WARN", Svc: "s1"},
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[fvEmptyListRow](&buf)
	if _, err := w.Write(in); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("logs/dt=%s/hour=%02d/list.parquet", base.Format("2006-01-02"), base.Hour())
	registerFileInMockS3(t, s, mock, key, buf.Bytes(), base)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	for _, tc := range []struct {
		q, field string
		want     map[string]uint64
	}{
		{"*", "level", map[string]uint64{"INFO": 1, "ERROR": 1, "WARN": 1}},
		{"*", "service.name", map[string]uint64{"s1": 2, "s2": 1}},
		{`level:=ERROR`, "service.name", map[string]uint64{"s2": 1}},
		{`rk:=a`, "level", map[string]uint64{"INFO": 1}},
		{"*", "rk", map[string]uint64{"a": 1, "b": 1, "": 1}},
	} {
		got := fvEmptyValues(t, s, tc.q, lo, hi, tc.field, false)
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("field_values(%s) q=%s = %v, want %v", tc.field, tc.q, got, tc.want)
		}
	}
	// field_names hits for the scalars after the nested columns.
	qq := mustParseQueryWithTime(t, "*", lo, hi)
	names, err := s.GetFieldNames(context.Background(), nil, qq)
	if err != nil {
		t.Fatal(err)
	}
	hm := map[string]uint64{}
	for _, n := range names {
		hm[n.Value] = n.Hits
	}
	if hm["level"] != 3 || hm["service.name"] != 3 {
		t.Errorf("field_names hits = %v, want level=3 service.name=3", hm)
	}
}

// A file written before a column existed (schema evolution) straddles the
// window: its rows count under "" only inside the window.
func TestFieldValuesEmpty_MissingColumnFile_StraddlingWindow(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	var rows []fvPropRow
	for i := 0; i < 10; i++ {
		rows = append(rows, fvPropRow{ts: base.Add(time.Duration(i) * time.Second).UnixNano(), svc: "a",
			maps: map[string]map[string]string{}, hasMap: map[string]bool{}})
	}
	// No severity_text column at all.
	data := fvPropFile(t, []string{"timestamp_unix_nano", "service.name", "resource.attributes"}, rows, 4)
	key := fmt.Sprintf("logs/dt=%s/hour=%02d/old.parquet", base.Format("2006-01-02"), base.Hour())
	registerFileInMockS3(t, s, mock, key, data, base)
	lo, hi := base.Add(3*time.Second).UnixNano(), base.Add(6*time.Second).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "level", false)
	if want := map[string]uint64{"": 4}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("field_values(level) over a file without the column, window [3s,6s] = %v, want %v", got, want)
	}
}

// A MAP attribute over a window that cuts a file: only rows inside count.
func TestFieldValuesEmpty_MapAttr_StraddlingWindow(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	var rows []fvPropRow
	for i := 0; i < 10; i++ {
		rows = append(rows, fvPropRow{ts: base.Add(time.Duration(i) * time.Second).UnixNano(), level: "INFO", svc: "a",
			maps:   map[string]map[string]string{"log.attributes": {"lk": fmt.Sprintf("v%d", i)}},
			hasMap: map[string]bool{"log.attributes": true}})
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "severity_text", "service.name", "resource.attributes", "log.attributes"}, rows, 4)
	key := fmt.Sprintf("logs/dt=%s/hour=%02d/win.parquet", base.Format("2006-01-02"), base.Hour())
	registerFileInMockS3(t, s, mock, key, data, base)
	lo, hi := base.Add(3*time.Second).UnixNano(), base.Add(4*time.Second).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "lk", false)
	if want := map[string]uint64{"v3": 1, "v4": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("field_values(lk) window [3s,4s] = %v, want %v", got, want)
	}
}

// A hide-mode delete must remove a MAP attribute's values (and their rows
// from the empty bucket) on the map read path too.
func TestFieldValuesEmpty_MapAttr_TombstoneApplied(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", s.cfg.Mode)
	now := time.Now().UnixNano()
	bw.stageLogRows([]schema.LogRow{
		{TimestampUnixNano: now, Body: "a", ServiceName: "api-gateway", SeverityText: "info", LogAttributes: map[string]string{"lk": "keep"}},
		{TimestampUnixNano: now, Body: "b", ServiceName: "order-service", SeverityText: "error", LogAttributes: map[string]string{"lk": "gone"}},
		{TimestampUnixNano: now, Body: "c", ServiceName: "order-service", SeverityText: "error"},
	})
	bw.flushStagedNow()
	f := &fieldsTombstoneFixture{storage: s, startNs: now - int64(time.Hour), endNs: now + int64(time.Hour), tombstoneQry: `service.name:="order-service"`}
	f.addTombstone()
	got := fvEmptyValues(t, s, "*", f.startNs, f.endNs, "lk", false)
	if want := map[string]uint64{"keep": 1}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("field_values(lk) after deleting order-service = %v, want %v", got, want)
	}
}

// The same rows answered from the buffer (a peer) and from cold give the same
// empty bucket, for a promoted column and for a MAP attribute.
func TestFieldValuesEmpty_BufferEqualsCold_EmptyBucket(t *testing.T) {
	mk := func(at time.Duration, ns, lk string) schema.LogRow {
		r := fvcRow(at, "INFO")
		r.K8sNamespaceName = ns
		if lk != "" {
			r.LogAttributes = map[string]string{"lk": lk}
		}
		return r
	}
	rows := []schema.LogRow{mk(20*time.Minute, "n1", "a"), mk(21*time.Minute, "", ""), mk(22*time.Minute, "", "b")}
	// Buffer only: an empty cold tier, the rows on a peer.
	sb := fvcStorageNonce(t, "")
	sb.bufferBridge = fvcPeer(t, rows)
	// Cold only.
	sc := fvcStorageNonce(t, "", rows)
	lo, hi := fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano()
	for _, field := range []string{"k8s.namespace.name", "lk"} {
		for _, q := range []string{"*", "_msg:x"} {
			b := fvEmptyValues(t, sb, q, lo, hi, field, false)
			c := fvEmptyValues(t, sc, q, lo, hi, field, false)
			if fmt.Sprint(b) != fmt.Sprint(c) {
				t.Errorf("field_values(%s) q=%s buffer=%v cold=%v", field, q, b, c)
			}
		}
	}
}
