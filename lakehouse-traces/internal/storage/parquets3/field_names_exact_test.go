package parquets3

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

func namesToHits(vs []logstorage.ValueWithHits) map[string]uint64 {
	m := make(map[string]uint64, len(vs))
	for _, v := range vs {
		m[v.Value] = v.Hits
	}
	return m
}

// fnSpan is fvcSpan with a span attribute.
func fnSpan(at time.Duration, name, method string) schema.TraceRow {
	r := fvcSpan(at, name)
	if method != "" {
		r.SpanAttributes = map[string]string{"custom.attr": method}
	}
	return r
}

func fnWindow(t *testing.T, query string) *logstorage.Query {
	t.Helper()
	return mustParseQueryWithTime(t, query, fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
}

// field_names lists the names VictoriaTraces stores a span under
// (span_attr:..., resource_attr:..., event:...) and not the Parquet columns
// (#269), and credits a field with every matching span of the stream blocks
// that list it: custom.attr is set on one span of the GET stream, so that
// stream's three spans are credited and the PUT stream's two are not.
func TestTraceFieldNames_AreVictoriaTracesFieldsCreditedPerStream(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{
		fnSpan(1*time.Minute, "GET", "GET"), fnSpan(2*time.Minute, "GET", ""), fnSpan(3*time.Minute, "GET", ""),
		fnSpan(4*time.Minute, "PUT", ""), fnSpan(5*time.Minute, "PUT", ""),
	})
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if m["span_attr:custom.attr"] != 3 {
		t.Errorf("span_attr:custom.attr hits = %d, want 3: %v", m["span_attr:custom.attr"], m)
	}
	for _, name := range []string{"trace_id", "span_id", "name", "resource_attr:service.name", "_time", "_stream", "_stream_id"} {
		if m[name] != 5 {
			t.Errorf("%s hits = %d, want 5: %v", name, m[name], m)
		}
	}
	for _, column := range []string{"service.name", "span.name", "custom.attr", "timestamp_unix_nano", "span.attributes", "resource.attributes", "account_id", "project_id"} {
		if _, ok := m[column]; ok {
			t.Errorf("%q listed: a Parquet column name is not a VictoriaTraces field: %v", column, m)
		}
	}
	for i := 1; i < len(got); i++ {
		a, b := got[i-1], got[i]
		if a.Hits < b.Hits || (a.Hits == b.Hits && a.Value > b.Value) {
			t.Fatalf("answer not in upstream order at %d: %v then %v", i, a, b)
		}
	}
}

// A window held only in the local insert buffer lists its fields (#378).
func TestTraceFieldNames_AnswersAWindowHeldOnlyInTheLocalBuffer(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for i, name := range []string{"GET", "GET", "PUT"} {
		lr.MustAdd(logstorage.TenantID{}, now+int64(i), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "checkout"}, {Name: "name", Value: name}, {Name: "trace_id", Value: "t"},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.SetLocalBuffer(bs)
	q := mustParseQueryWithTime(t, "*", now-int64(time.Hour), now+int64(time.Hour))
	got, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	for _, name := range []string{"_time", "name", "trace_id", "resource_attr:service.name"} {
		if m[name] != 3 {
			t.Errorf("%s hits = %d, want 3: %v", name, m[name], m)
		}
	}
}

// Spans an insert peer still holds are merged as a query merges them.
func TestTraceFieldNames_MergesPeerBufferAndObjects(t *testing.T) {
	const nonce = "65000000aaaabbbb"
	s := fvcStorageNonce(t, nonce, []schema.TraceRow{fvcSpan(5*time.Minute, "GET"), fvcSpan(6*time.Minute, "GET")})
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{
		fvcSpan(5*time.Minute, "GET"), fvcSpan(6*time.Minute, "GET"), // in the segment's object too
		fnSpan(20*time.Minute, "PUT", "PUT"), fvcSpan(21*time.Minute, "PUT"),
	}, nonce)
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if m["trace_id"] != 4 {
		t.Errorf("trace_id hits = %d, want 4: %v", m["trace_id"], m)
	}
	if m["span_attr:custom.attr"] != 2 {
		t.Errorf("span_attr:custom.attr hits = %d, want 2 (the peer's PUT stream): %v", m["span_attr:custom.attr"], m)
	}
}

// stream_field_names credits a tag with the spans of every matching stream
// that carries it (#461).
func TestTraceGetStreamFieldNames_CreditsTheSpansOfEachStream(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{
		fvcSpan(1*time.Minute, "GET"), fvcSpan(2*time.Minute, "GET"), fvcSpan(3*time.Minute, "GET"), fvcSpan(4*time.Minute, "PUT"),
	})
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{fvcSpan(20*time.Minute, "PUT")})
	got, err := s.GetStreamFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	want := []logstorage.ValueWithHits{{Value: "resource_attr:service.name", Hits: 5}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("stream_field_names = %v, want %v", got, want)
	}
	empty := mustParseQueryWithTime(t, "*", fvcBase.Add(-48*time.Hour).UnixNano(), fvcBase.Add(-47*time.Hour).UnixNano())
	if got, _ = s.GetStreamFieldNames(context.Background(), nil, empty); len(got) != 0 {
		t.Errorf("empty window answered %v", got)
	}
}

// The answer does not depend on how the spans of a stream are spread over
// objects: one stream held in two objects credits a field one span carries
// with every span of the stream, as one block does.
func TestTraceFieldNames_DoNotDependOnTheObjectLayout(t *testing.T) {
	first := []schema.TraceRow{fnSpan(1*time.Minute, "GET", "GET"), fnSpan(2*time.Minute, "GET", "")}
	second := []schema.TraceRow{fnSpan(3*time.Minute, "GET", ""), fnSpan(4*time.Minute, "GET", "")}
	one := fvcStorage(t, append(append([]schema.TraceRow{}, first...), second...))
	two := fvcStorage(t, first, second)
	a, err := one.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := two.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("one object: %v\ntwo objects: %v", a, b)
	}
	if m := namesToHits(b); m["span_attr:custom.attr"] != 4 {
		t.Errorf("span_attr:custom.attr hits = %d, want 4", m["span_attr:custom.attr"])
	}
}
