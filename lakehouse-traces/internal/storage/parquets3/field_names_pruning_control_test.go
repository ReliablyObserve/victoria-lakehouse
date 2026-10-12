package parquets3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Negative control for the field_names counting read (see the logs twin): only
// a field_names enumeration gives up the pruning the row filter allows, every
// other read keeps its file pre-filter and its row-group skips.

// skippedRowGroups is every skip reason of lakehouse_parquet_row_groups_skipped_total.
func skippedRowGroups() uint64 {
	var n uint64
	for _, r := range metrics.RowGroupSkipReasons {
		n += metrics.ParquetRowGroupsSkipped.Get(r)
	}
	return n
}

// recordingS3 fronts a mock S3 server and records which Parquet objects were
// read, so a test can tell which objects a query opened.
type recordingS3 struct {
	*httptest.Server
	mu   sync.Mutex
	keys map[string]struct{}
}

func newRecordingS3(t *testing.T) *recordingS3 {
	t.Helper()
	backend := newMockS3Server()
	t.Cleanup(backend.close)
	r := &recordingS3{keys: make(map[string]struct{})}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, ".parquet") {
			r.mu.Lock()
			r.keys[req.URL.Path] = struct{}{}
			r.mu.Unlock()
		}
		backend.handler(w, req)
	}))
	t.Cleanup(r.Close)
	return r
}

// readObjects is the number of distinct Parquet objects that have been read.
func (r *recordingS3) readObjects() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

// twoObjectStream is one stream in two objects whose operation name differs: the
// filter name:="PUT-2" excludes the first object.
func twoObjectStream(t *testing.T) (*Storage, *recordingS3) {
	t.Helper()
	mock := newRecordingS3(t)
	s := testStorageWithS3(t, mock.URL)
	s.cfg.Mode = config.ModeTraces
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	a := fnSpan(1*time.Minute, "GET-1", "GET")
	b := fvcSpan(2*time.Minute, "PUT-2")
	a.TraceID, a.SpanID = "ta", "sa"
	b.TraceID, b.SpanID = "tb", "sb"
	b.Stream, b.StreamID, b.ServiceName = a.Stream, a.StreamID, a.ServiceName
	fvcUpload(t, bw, "", []schema.TraceRow{a})
	fvcUpload(t, bw, "", []schema.TraceRow{b})
	return s, mock
}

func discard(uint, *logstorage.DataBlock) {}

func TestTraceFieldNames_OnlyTheCountingReadOpensObjectsTheFilterExcludes(t *testing.T) {
	const filter = `name:="PUT-2"`
	ctx := context.Background()
	for name, run := range map[string]func(s *Storage) error{
		"RunQuery": func(s *Storage) error { return s.RunQuery(ctx, nil, fnWindow(t, filter), discard) },
		"hits shape": func(s *Storage) error {
			return s.RunQuery(ctx, nil, fnWindow(t, filter+" | stats by (_time:1h) count() hits"), discard)
		},
		"stats": func(s *Storage) error {
			return s.RunQuery(ctx, nil, fnWindow(t, filter+" | stats count() n"), discard)
		},
	} {
		s, mock := twoObjectStream(t)
		before := metrics.ParquetRowGroupsSkipped.Get("label_index") + metrics.ParquetRowGroupsSkipped.Get("column_stats")
		if err := run(s); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if n := mock.readObjects(); n != 1 {
			t.Errorf("%s: %d objects opened, want 1 (the object the filter excludes is skipped)", name, n)
		}
		if d := metrics.ParquetRowGroupsSkipped.Get("label_index") + metrics.ParquetRowGroupsSkipped.Get("column_stats") - before; d == 0 {
			t.Errorf("%s: no object-level skip recorded", name)
		}
	}
	s, mock := twoObjectStream(t)
	if _, err := s.GetFieldNames(ctx, nil, fnWindow(t, filter)); err != nil {
		t.Fatal(err)
	}
	if n := mock.readObjects(); n != 2 {
		t.Errorf("field_names count: %d objects opened, want 2 (an excluded object still lists its columns)", n)
	}
}

// rowGroupStore is two objects of four row groups of ten spans, one trace id per
// row group.
func rowGroupStore(t *testing.T) *Storage {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	pad := strings.Repeat("y", 1500)
	for f := 0; f < 2; f++ {
		var rows []schema.TraceRow
		for i := 0; i < 40; i++ {
			k := i / 10
			ts := base.Add(time.Duration(f*40+i) * time.Second).UnixNano()
			rows = append(rows, schema.TraceRow{
				TimestampUnixNano: ts, StartTimeUnixNano: ptr(ts),
				TraceID: ztid(f, k), SpanID: fmt.Sprintf("%016x", f*100+i), SpanName: fmt.Sprintf("op%d-%s%d", k%2, pad, i), ServiceName: []string{"alpha", "beta"}[f],
				Stream: fmt.Sprintf(`{resource_attr:service.name=%q}`, []string{"alpha", "beta"}[f]), StreamID: fmt.Sprintf("%048x", 3+f),
			})
		}
		res, err := writeTracesParquet(rows, 10, 3)
		if err != nil {
			t.Fatal(err)
		}
		registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/f%d.parquet", base.Format("2006-01-02"), base.Hour(), f), res.Data, base.Add(time.Duration(f)*40*time.Second))
	}
	return s
}

func TestTraceFieldNames_OnlyTheCountingReadReadsRowGroupsTheFilterExcludes(t *testing.T) {
	ctx := context.Background()
	window := func(query string) *logstorage.Query {
		now := time.Now()
		return mustParseQueryWithTime(t, query, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	}
	for _, tc := range []struct {
		filter string
		rows   int
		skips  uint64 // row groups an ordinary read skips, at least
	}{
		{"trace_id:=" + ztid(1, 0), 10, 3},            // the footer bloom of the id
		{"`resource_attr:service.name`:=beta", 40, 1}, // the footer bloom of a service
	} {
		s := rowGroupStore(t)
		before := skippedRowGroups()
		rows := 0
		if err := s.RunQuery(ctx, nil, window(tc.filter), func(_ uint, db *logstorage.DataBlock) { rows += db.RowsCount() }); err != nil {
			t.Fatal(err)
		}
		if rows != tc.rows {
			t.Fatalf("%s: RunQuery returned %d spans, want %d", tc.filter, rows, tc.rows)
		}
		if d := skippedRowGroups() - before; d < tc.skips {
			t.Errorf("%s: RunQuery skipped %d row groups, want at least %d", tc.filter, d, tc.skips)
		}

		// next to it: the field_names count of the same filter skips none
		s = rowGroupStore(t)
		before = skippedRowGroups()
		if _, err := s.GetFieldNames(ctx, nil, window(tc.filter)); err != nil {
			t.Fatal(err)
		}
		if d := skippedRowGroups() - before; d != 0 {
			t.Errorf("%s: the field_names count skipped %d row groups for the row filter's sake", tc.filter, d)
		}
	}
}
