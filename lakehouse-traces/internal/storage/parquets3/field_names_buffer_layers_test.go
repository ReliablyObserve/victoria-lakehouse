package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// One stream held in the local insert buffer AND in objects, each with a column
// the other lacks. Hot storage merges the in-memory part into the stored ones, so
// the stream is one block: the buffer and the objects are unioned per stream-day
// and every column is credited with all of the stream's matching spans.
func TestTraceFieldNames_LocalBufferAndObjectsOfOneStreamAreOneBlock(t *testing.T) {
	base := time.Now().Truncate(time.Hour)
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for i := 0; i < 2; i++ {
		lr.MustAdd(logstorage.TenantID{}, base.Add(20*time.Minute+time.Duration(i)*time.Second).UnixNano(), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "checkout"}, {Name: "span_attr:bufonly", Value: "b"}, {Name: "name", Value: "GET"}, {Name: "trace_id", Value: "tb"},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces // as the traces binary runs
	s.SetLocalBuffer(bs)

	var streamID, stream string
	probe := mustParseQueryWithTime(t, "*", base.UnixNano(), base.Add(time.Hour).UnixNano())
	if err := s.RunQuery(context.Background(), nil, probe, func(_ uint, db *logstorage.DataBlock) {
		for _, c := range db.GetColumns(false) {
			switch c.Name {
			case "_stream_id":
				streamID = c.Values[0]
			case "_stream":
				stream = c.Values[0]
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if streamID == "" || stream == "" {
		t.Fatalf("the buffer's block carries no stream id/stream: %q %q", streamID, stream)
	}

	obj := func(at time.Duration) schema.TraceRow {
		ts := base.Add(at).UnixNano()
		return schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: ptr(ts), TraceID: "to", SpanID: "so", SpanName: "GET",
			ServiceName: "checkout", Stream: stream, StreamID: streamID,
			SpanAttributes: map[string]string{"custom.attr": "v"},
		}
	}
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	fvcUpload(t, bw, "", []schema.TraceRow{obj(1 * time.Minute), obj(2 * time.Minute), obj(3 * time.Minute)})

	q := mustParseQueryWithTime(t, "*", base.UnixNano(), base.Add(time.Hour).UnixNano())
	got, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	want := map[string]uint64{"trace_id": 5, "_time": 5, "span_attr:bufonly": 5, "span_attr:custom.attr": 5}
	for name, hits := range want {
		if m[name] != hits {
			t.Errorf("* %s hits = %d, want %d: %v", name, m[name], hits, m)
		}
	}
	for _, tc := range []struct {
		filter string
		rows   uint64
	}{{"trace_id:=to", 3}, {"trace_id:=tb", 2}} {
		got, err = s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, tc.filter, base.UnixNano(), base.Add(time.Hour).UnixNano()))
		if err != nil {
			t.Fatal(err)
		}
		m = namesToHits(got)
		for _, name := range []string{"trace_id", "span_attr:bufonly", "span_attr:custom.attr"} {
			if m[name] != tc.rows {
				t.Errorf("%s: %s hits = %d, want %d: %v", tc.filter, name, m[name], tc.rows, m)
			}
		}
	}
}

// A filter selects spans of the insert buffer too: the requests the buffer holds
// are parts of one stream, and a part none of whose spans match still lists its
// columns for the stream (see the logs twin).
func TestTraceFieldNames_FilterKeepsColumnsOfNonMatchingPartsInTheLocalBuffer(t *testing.T) {
	for _, withObject := range []bool{false, true} {
		base := time.Now().Truncate(time.Hour)
		bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		add := func(gen string, withField bool, at time.Duration) {
			lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
			for i := 0; i < 2; i++ {
				fields := []logstorage.Field{{Name: "resource_attr:service.name", Value: "checkout"}, {Name: "span_attr:gen", Value: gen}, {Name: "name", Value: "GET"}, {Name: "trace_id", Value: "t"}}
				if withField {
					fields = append(fields, logstorage.Field{Name: "span_attr:onlya", Value: "a"})
				}
				lr.MustAdd(logstorage.TenantID{}, base.Add(at+time.Duration(i)*time.Second).UnixNano(), fields, 1)
			}
			bs.MustAddRows(lr)
			logstorage.PutLogRows(lr)
			bs.DebugFlush()
		}
		add("g0", true, 20*time.Minute)
		add("g1", false, 30*time.Minute)

		mock := newMockS3Server()
		s := testStorageWithS3(t, mock.url())
		s.cfg.Mode = config.ModeTraces
		s.SetLocalBuffer(bs)
		if withObject {
			bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
			other := fvcSpan(0, "PUT")
			other.TimestampUnixNano = base.Add(1 * time.Minute).UnixNano()
			other.StartTimeUnixNano = ptr(other.TimestampUnixNano)
			fvcUpload(t, bw, "", []schema.TraceRow{other})
		}
		q := mustParseQueryWithTime(t, "`span_attr:gen`:=g1", base.UnixNano(), base.Add(time.Hour).UnixNano())
		got, err := s.GetFieldNames(context.Background(), nil, q)
		mock.close()
		bs.Close()
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		if m["span_attr:onlya"] != 2 || m["span_attr:gen"] != 2 || m["trace_id"] != 2 {
			t.Errorf("withObject=%v: span_attr:onlya/span_attr:gen/trace_id hits = %d/%d/%d, want 2/2/2: %v", withObject, m["span_attr:onlya"], m["span_attr:gen"], m["trace_id"], m)
		}
	}
}
