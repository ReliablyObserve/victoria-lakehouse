package parquets3

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The spans an insert peer serves are decoded with the parquet-name aliases of
// the promoted attributes (service.name next to resource_attr:service.name,
// http.method next to span_attr:http.method) so a filter spelling either
// matches. They are not fields of a span on VictoriaTraces (service.name matches
// no span on a hot instance): a field_names enumeration lists the names a span is
// stored under only, and the union of a stream-day across the peer's spans and the
// objects must not credit an alias with the objects' spans (#504). Every other
// read of the same spans still carries them.

// aliasedSpan is a span of the stream of fvcSpan(0, "GET") with a promoted span
// attribute, so the parquet-name aliases service.name and http.method exist.
func aliasedSpan(at time.Duration, name string) schema.TraceRow {
	base := fvcSpan(0, "GET")
	r := fvcSpan(at, name)
	r.Stream, r.StreamID, r.ServiceName = base.Stream, base.StreamID, "svc-GET"
	r.HTTPMethod = "GET"
	return r
}

// aliasNames are the parquet-name aliases of the promoted attributes aliasedSpan sets.
func aliasNames() []string { return []string{"service.name", "http.method"} }

func TestTraceFieldNames_PeerSpansListNoParquetAliasNames(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{aliasedSpan(1*time.Minute, "GET"), aliasedSpan(2*time.Minute, "GET")})
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{aliasedSpan(20*time.Minute, "GET"), aliasedSpan(21*time.Minute, "GET")})

	for _, filter := range []string{
		"*",
		`"resource_attr:service.name":="svc-GET"`, // the filter as a hot instance knows it
		`_stream:{resource_attr:service.name="svc-GET"}`,
		`"span_attr:http.method":=GET`,
	} {
		got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, filter))
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		for _, alias := range aliasNames() {
			if _, listed := m[alias]; listed {
				t.Errorf("%s: %s listed although it is no field of a span: %v", filter, alias, m)
			}
		}
		if m["resource_attr:service.name"] != 4 || m["span_attr:http.method"] != 4 {
			t.Errorf("%s: resource_attr:service.name/span_attr:http.method hits = %d/%d, want 4/4 (the stream-day's spans): %v", filter, m["resource_attr:service.name"], m["span_attr:http.method"], m)
		}
	}

	// stream_field_names lists the stream tags only.
	tags, err := s.GetStreamFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "resource_attr:service.name", Hits: 4}}; !reflect.DeepEqual(tags, want) {
		t.Errorf("stream_field_names = %v, want %v", tags, want)
	}
}

// The rule keys on the read being a field_names enumeration, not on the plain
// filter path: a field_names call whose query has pipes runs upstream's field_names
// pipe over the same reads, and those list no alias of a peer's spans either.
func TestTraceFieldNames_PipePathListsNoPeerAliasNames(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{aliasedSpan(1*time.Minute, "GET"), aliasedSpan(2*time.Minute, "GET")})
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{aliasedSpan(20*time.Minute, "GET"), aliasedSpan(21*time.Minute, "GET")})
	for _, query := range []string{"* | limit 100", `"span_attr:http.method":=GET | limit 100`} {
		got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, query))
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		for _, alias := range aliasNames() {
			if _, listed := m[alias]; listed {
				t.Errorf("%s: %s listed although it is no field of a span: %v", query, alias, m)
			}
		}
		if m["resource_attr:service.name"] != 4 || m["span_attr:http.method"] != 4 {
			t.Errorf("%s: resource_attr:service.name/span_attr:http.method hits = %d/%d, want 4/4: %v", query, m["resource_attr:service.name"], m["span_attr:http.method"], m)
		}
	}
}

// A field_names query that spells the Parquet dialect (service.name:=...,
// http.method:=...) matches no span of an insert peer, as on hot VictoriaTraces,
// where those are no fields: a field_names read asks the bridge for VictoriaTraces'
// names only, so the peer's spans are not credited by it. Known difference
// (#505): the objects honour that spelling as a filter convenience, so a
// field_names query in the Parquet dialect still lists them (and the alias the
// query spells) where hot answers nothing; a row query matches both layers.
func TestTraceFieldNames_AParquetNameFilterMatchesNoPeerSpan(t *testing.T) {
	rows := []schema.TraceRow{aliasedSpan(1*time.Minute, "GET"), aliasedSpan(2*time.Minute, "GET"), aliasedSpan(20*time.Minute, "GET"), aliasedSpan(21*time.Minute, "GET")}
	peerOnly := fvcStorage(t)
	peerOnly.bufferBridge = fvcPeer(t, rows)
	split := fvcStorage(t, rows[:2])
	split.bufferBridge = fvcPeer(t, rows[2:])

	for _, filter := range []string{`http.method:=GET`, `service.name:="svc-GET"`} {
		got, err := peerOnly.GetFieldNames(context.Background(), nil, fnWindow(t, filter))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Errorf("%s: spans only a peer holds are credited by a Parquet-dialect filter, as hot would not: %v", filter, got)
		}
		got, err = split.GetFieldNames(context.Background(), nil, fnWindow(t, filter))
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		if m["trace_id"] != 2 {
			t.Errorf("%s: trace_id hits = %d, want 2 (the objects' spans, none of the peer's): %v", filter, m["trace_id"], m)
		}
		// A row query still matches the peer's spans with either spelling.
		var rowsSeen atomic.Int64
		if err := split.RunQuery(context.Background(), nil, fnWindow(t, filter), func(_ uint, db *logstorage.DataBlock) { rowsSeen.Add(int64(db.RowsCount())) }); err != nil {
			t.Fatal(err)
		}
		if rowsSeen.Load() != 4 {
			t.Errorf("%s: a row query matched %d spans, want all 4", filter, rowsSeen.Load())
		}
	}
}

// Negative control: only a field_names read drops the aliases. A row read, the
// values of a field and the streams of the same spans see them as before.
func TestTracePeerSpans_OrdinaryReadsKeepTheParquetAliases(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{aliasedSpan(1*time.Minute, "GET")})
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{aliasedSpan(20*time.Minute, "GET"), aliasedSpan(21*time.Minute, "GET")})

	// A row read carries the aliases, with a wildcard and with a filter in either dialect.
	for _, filter := range []string{"*", `service.name:="svc-GET"`, `http.method:=GET`, `"span_attr:http.method":=GET`} {
		var rows atomic.Int64
		columns := make(map[string]bool)
		if err := s.RunQuery(context.Background(), nil, fnWindow(t, filter), func(_ uint, db *logstorage.DataBlock) {
			rows.Add(int64(db.RowsCount()))
			for _, c := range db.GetColumns(false) {
				columns[c.Name] = true
			}
		}); err != nil {
			t.Fatal(err)
		}
		if rows.Load() != 3 {
			t.Errorf("%s: %d rows, want 3 (the object's span and the peer's two)", filter, rows.Load())
		}
		// The object's span carries an alias only when the query spells it
		// (the file-scan rule); the peer's spans carry both always.
		if filter == "*" && (!columns["service.name"] || !columns["http.method"]) {
			t.Errorf("a row read of the peer's spans lost an alias: %v", columns)
		}
	}

	// field_values: the alias and the VictoriaTraces name list the same values.
	for _, pair := range [][2]string{{"service.name", "resource_attr:service.name"}, {"http.method", "span_attr:http.method"}} {
		a, err := s.GetFieldValues(context.Background(), nil, fnWindow(t, "*"), pair[0], 0)
		if err != nil {
			t.Fatal(err)
		}
		b, err := s.GetFieldValues(context.Background(), nil, fnWindow(t, "*"), pair[1], 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == 0 || !reflect.DeepEqual(a, b) {
			t.Errorf("field_values %s = %v, %s = %v: want the same values", pair[0], a, pair[1], b)
		}
	}

	streams, err := s.GetStreams(context.Background(), nil, fnWindow(t, "*"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 || streams[0].Hits != 3 {
		t.Errorf("streams = %v, want one stream with 3 spans", streams)
	}
}
