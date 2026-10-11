package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// One stream held in the local insert buffer AND in objects, each with a column
// the other lacks. The buffer's rows are an in-memory part that hot storage has
// not merged into the stored blocks, so the stream is two blocks: each column is
// credited with the rows of its own layer, the shared ones with all of them.
func TestFieldNames_LocalBufferAndObjectsOfOneStreamAreTwoBlocks(t *testing.T) {
	base := time.Now().Truncate(time.Hour)
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 2; i++ {
		lr.MustAdd(logstorage.TenantID{}, base.Add(20*time.Minute+time.Duration(i)*time.Second).UnixNano(), []logstorage.Field{
			{Name: "service.name", Value: "checkout"}, {Name: "bufonly", Value: "b"}, {Name: "_msg", Value: "event"},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.SetLocalBuffer(bs)

	// The stream id the buffer gives the stream, so the objects' rows are the
	// same stream.
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

	obj := func(at time.Duration) schema.LogRow {
		return schema.LogRow{
			TimestampUnixNano: base.Add(at).UnixNano(), Body: "event", SeverityText: "INFO",
			ServiceName: "checkout", Stream: stream, StreamID: streamID, ExceptionType: "E",
		}
	}
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", s.cfg.Mode)
	fvcUpload(t, bw, "", []schema.LogRow{obj(1 * time.Minute), obj(2 * time.Minute), obj(3 * time.Minute)})

	q := mustParseQueryWithTime(t, "*", base.UnixNano(), base.Add(time.Hour).UnixNano())
	got, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	want := map[string]uint64{"_msg": 5, "_time": 5, "bufonly": 2, "exception.type": 3}
	for name, hits := range want {
		if m[name] != hits {
			t.Errorf("%s hits = %d, want %d: %v", name, m[name], hits, m)
		}
	}
	// A filter keeps both layers' columns: the object rows are the only matching
	// ones, and the buffer's column still belongs to the stream's buffer block.
	got, err = s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, "exception.type:=E", base.UnixNano(), base.Add(time.Hour).UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	if m = namesToHits(got); m["_msg"] != 3 || m["exception.type"] != 3 {
		t.Errorf("filtered _msg/exception.type hits = %d/%d, want 3/3: %v", m["_msg"], m["exception.type"], m)
	}
	if _, listed := m["bufonly"]; listed {
		t.Errorf("bufonly listed although no matching row is in the buffer block: %v", m)
	}
}

// A filter selects rows of the insert buffer too: the requests the buffer holds
// are parts of one stream, and a part none of whose rows match the filter still
// lists its columns for the stream. The buffer's engine is therefore asked for
// every row of the window and the filter is applied to the rows afterwards, with
// the buffer alone in the window and next to an object of another stream.
func TestFieldNames_FilterKeepsColumnsOfNonMatchingPartsInTheLocalBuffer(t *testing.T) {
	for _, withObject := range []bool{false, true} {
		base := time.Now().Truncate(time.Hour)
		bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		add := func(gen string, withField bool, at time.Duration) {
			lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
			for i := 0; i < 2; i++ {
				fields := []logstorage.Field{{Name: "service.name", Value: "checkout"}, {Name: "gen", Value: gen}, {Name: "_msg", Value: "event"}}
				if withField {
					fields = append(fields, logstorage.Field{Name: "onlya", Value: "a"})
				}
				lr.MustAdd(logstorage.TenantID{}, base.Add(at+time.Duration(i)*time.Second).UnixNano(), fields, 1)
			}
			bs.MustAddRows(lr)
			logstorage.PutLogRows(lr)
			bs.DebugFlush()
		}
		add("g0", true, 20*time.Minute)  // a request whose rows carry onlya ...
		add("g1", false, 30*time.Minute) // ... and a later one whose rows do not

		mock := newMockS3Server()
		s := testStorageWithS3(t, mock.url())
		s.SetLocalBuffer(bs)
		if withObject {
			bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", s.cfg.Mode)
			other := fvcRow(0, "INFO")
			other.TimestampUnixNano = base.Add(1 * time.Minute).UnixNano()
			fvcUpload(t, bw, "", []schema.LogRow{other})
		}
		q := mustParseQueryWithTime(t, "gen:=g1", base.UnixNano(), base.Add(time.Hour).UnixNano())
		got, err := s.GetFieldNames(context.Background(), nil, q)
		mock.close()
		bs.Close()
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		if m["onlya"] != 2 || m["gen"] != 2 || m["_msg"] != 2 {
			t.Errorf("withObject=%v: onlya/gen/_msg hits = %d/%d/%d, want 2/2/2: %v", withObject, m["onlya"], m["gen"], m["_msg"], m)
		}
	}
}
