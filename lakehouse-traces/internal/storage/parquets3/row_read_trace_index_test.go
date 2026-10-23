package parquets3

// Trace-by-ID reads the footer index of every file first (LookupTraceIndex) and
// uses the bound it aggregates for the span fetch. A footer that cannot be read
// leaves its file's entry unknown: the bound of the files that could be read may
// be narrower than the trace, so the lookup reports a miss and VictoriaTraces'
// span scan, which fails when it cannot read the file, answers instead (issue
// #503: the aggregate of the readable files used to be returned as if complete).

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

func traceIndexFixture(t *testing.T) (*faultS3, *Storage, []string) {
	t.Helper()
	fs := newFaultS3(t)
	s := testStorageWithS3(t, fs.srv.URL)
	s.cfg.Mode = config.ModeTraces
	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	keys := []string{"traces/dt=2026-06-01/hour=10/ti-0.parquet", "traces/dt=2026-06-01/hour=10/ti-1.parquet", "traces/dt=2026-06-01/hour=10/ti-2.parquet"}
	entries := [][]TraceIndexEntry{
		{{TraceID: "trace-xyz", StartNs: 1_000, EndNs: 5_000}},
		{{TraceID: "trace-xyz", StartNs: 500, EndNs: 3_000}}, // the earliest start of the trace is here
		{{TraceID: "trace-other", StartNs: 9_000, EndNs: 9_500}},
	}
	for i, k := range keys {
		registerFileInMockS3(t, s, fs.mock, k, writeParquetWithTraceIndex(t, entries[i], 4), base)
	}
	return fs, s, keys
}

func TestLookupTraceIndex_UnreadableFooterBeatsTheAggregateOfTheOthers(t *testing.T) {
	t.Run("control: every footer readable", func(t *testing.T) {
		_, s, _ := traceIndexFixture(t)
		start, end, found, err := s.LookupTraceIndex(context.Background(), nil, "trace-xyz")
		if err != nil || !found || start != 500 || end != 5_000 {
			t.Fatalf("got (%d, %d, found=%v, err=%v), want (500, 5000, true, nil)", start, end, found, err)
		}
	})
	t.Run("a footer that cannot be read: a miss, so the scan answers", func(t *testing.T) {
		fs, s, keys := traceIndexFixture(t)
		fs.setFault(keys[1], accessDenied)
		start, end, found, err := s.LookupTraceIndex(context.Background(), nil, "trace-xyz")
		if err != nil || found {
			t.Fatalf("got (%d, %d, found=%v, err=%v): the bound of the readable files (1000..5000) would hide spans of the unreadable one", start, end, found, err)
		}
	})
	t.Run("a file that is gone is not an error", func(t *testing.T) {
		fs, s, keys := traceIndexFixture(t)
		fs.mock.mu.Lock()
		delete(fs.mock.files, keys[2])
		fs.mock.mu.Unlock()
		start, end, found, err := s.LookupTraceIndex(context.Background(), nil, "trace-xyz")
		if err != nil || !found || start != 500 || end != 5_000 {
			t.Fatalf("got (%d, %d, found=%v, err=%v), want (500, 5000, true, nil)", start, end, found, err)
		}
	})
}

// The VictoriaTraces index lookup through the adapter: with an unreadable file
// the lookup is not answered from the footers of the others, and the scan that
// replaces it fails instead of returning the trace without that file's spans.
func TestTraceByIDIndexQuery_UnreadableObjectFailsTheLookup(t *testing.T) {
	run := func(fs *faultS3, s *Storage) ([]map[string]string, error) {
		vtstorageadapter.Init(s)
		t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })
		qs := fmt.Sprintf(`{%s="0"} %s:=%q | stats min(_time) as _time, min(%s) as %s, max(%s) as %s | limit 10`,
			otelpb.TraceIDIndexStreamName, otelpb.TraceIDIndexFieldName, "trace-xyz",
			otelpb.TraceIDIndexStartTimeFieldName, otelpb.TraceIDIndexStartTimeFieldName,
			otelpb.TraceIDIndexEndTimeFieldName, otelpb.TraceIDIndexEndTimeFieldName)
		q := mustParseQueryWithTime(t, qs, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).UnixNano(), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).UnixNano())
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		var out []map[string]string
		err := vtstorage.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			rows := blockRowFields([]*logstorage.DataBlock{db})
			mu.Lock()
			out = append(out, rows...)
			mu.Unlock()
		})
		return out, err
	}

	fs, s, _ := traceIndexFixture(t)
	if rows, err := run(fs, s); err != nil || len(rows) != 1 {
		t.Fatalf("control: %d rows, err %v; want the one index row", len(rows), err)
	}

	fs, s, keys := traceIndexFixture(t)
	fs.setFault(keys[1], accessDenied)
	if rows, err := run(fs, s); err == nil {
		t.Fatalf("the index lookup answered %d rows with an unreadable object", len(rows))
	}
}
