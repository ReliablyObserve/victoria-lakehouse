package parquets3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

func tracesRunner(t *testing.T, st storage.Storage, start, end int64) func(ctx context.Context, query string) []map[string]string {
	t.Helper()
	vtstorageadapter.Init(st)
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })
	return func(ctx context.Context, query string) []map[string]string {
		t.Helper()
		vtstorageadapter.Init(st)
		q := mustParseQueryWithTime(t, query, start, end)
		qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		var out []map[string]string
		if err := vtstorage.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			r := blockRowFields([]*logstorage.DataBlock{db})
			mu.Lock()
			out = append(out, r...)
			mu.Unlock()
		}); err != nil {
			t.Fatalf("RunQuery(%s): %v", query, err)
		}
		return out
	}
}

// TestColdReview_HitsHintStarPipes (traces): the hits endpoint's timestamp-only
// hint must not narrow a read that a pipe needing every field made unprojected,
// with or without a delete.
func TestColdReview_HitsHintStarPipes(t *testing.T) {
	for _, tomb := range []bool{false, true} {
		s, start, end := coldFilteredStatsFixture(t)
		run := tracesRunner(t, s, start, end)
		if tomb {
			addHideTombstone(s, `name:="needle-exact"`, start, end)
		}
		hint := storage.WithTimestampOnlyHint(context.Background())
		for _, q := range []string{
			`* | pack_json | filter name:GET | stats by (_time:1m) count() n`,
			`* | pack_logfmt as y | filter y:GET | stats by (_time:1m) count() n`,
			`* | pack_json | stats by (_time:1m) count() n`,
		} {
			got := sumN(run(hint, q))
			want := sumN(tracesRunner(t, allColumnsStore{s}, start, end)(context.Background(), q))
			if want == 0 {
				t.Fatalf("fixture: %s matched nothing", q)
			}
			if got != want {
				t.Errorf("tomb=%v %s: hint run = %d, all-columns = %d", tomb, q, got, want)
			}
		}
	}
}

// TestColdReview_NotOrIfAroundBloomColumn (#289, traces): a NOT, an OR or a
// stats `if (...)` around a footer-bloom column must not prune row groups.
func TestColdReview_NotOrIfAroundBloomColumn(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var rows []schema.TraceRow
	for i := 0; i < 40; i++ {
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(), StartTimeUnixNano: schema.Int64Ptr(base.Add(time.Duration(i) * time.Second).UnixNano()),
			TraceID: fmt.Sprintf("trace-%d", i/10), SpanID: fmt.Sprintf("%016x", i), SpanName: "op", ServiceName: "svc",
			Stream: `{resource_attr:service.name="svc"}`, StreamID: fmt.Sprintf("%048x", 3),
		})
	}
	res, err := writeTracesParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/b.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	run := tracesRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	bg := context.Background()
	for _, c := range []struct {
		q     string
		want  int
		stats bool
	}{
		{`NOT trace_id:="trace-1"`, 30, false},
		{`NOT trace_id:="trace-1" | stats count() n`, 30, true},
		{`trace_id:="trace-1" OR trace_id:="trace-2"`, 20, false},
		{`trace_id:="trace-1" OR trace_id:="trace-2" | stats count() n`, 20, true},
		{`NOT trace_id:="trace-1" | fields trace*`, 30, false},
		{`NOT trace_id:="trace-1" | sort by (_time) | limit 100`, 30, false},
		{`* | stats count() if (trace_id:="trace-1") n`, 10, true},
		{`* | stats count() if (NOT trace_id:="trace-1") n`, 30, true},
		{`trace_id:="trace-1"`, 10, false}, // a positive lookup still works (and still prunes)
	} {
		out := run(bg, c.q)
		got := len(out)
		if c.stats {
			got = sumN(out)
		}
		if got != c.want {
			t.Errorf("%s: got %d want %d", c.q, got, c.want)
		}
	}
}

// TestColdReview_PushdownTimeShapes (traces): the count pushdown fabricates
// timestamps, so a shape whose answer depends on a row's own time must scan.
func TestColdReview_PushdownTimeShapes(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	minute := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Minute)
	var rows []schema.TraceRow
	for i := 0; i < 60; i++ {
		ts := minute.Add(time.Duration(i) * 100 * time.Millisecond)
		name := []string{"a", "b", "c"}[i%3]
		if i >= 54 {
			ts = minute.Add(10*time.Minute + time.Duration(i)*time.Second)
			name = "late"
		}
		rows = append(rows, schema.TraceRow{TimestampUnixNano: ts.UnixNano(), StartTimeUnixNano: schema.Int64Ptr(ts.UnixNano()), TraceID: fmt.Sprintf("t%d", i), SpanID: fmt.Sprintf("%016x", i),
			SpanName: name, ServiceName: "svc", StatusCode: schema.Int32Ptr(int32(i % 2)), Stream: `{resource_attr:service.name="svc"}`, StreamID: fmt.Sprintf("%048x", 3)})
	}
	res, err := writeTracesParquet(rows, 20, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/pd.parquet", minute.Format("2006-01-02"), minute.Hour()), res.Data, minute)
	fi := s.manifest.GetFilesForRange(0, 1<<62)[0]
	agg := map[string]int64{}
	early := 0
	for _, r := range rows {
		agg[r.SpanName]++
		if r.SpanName != "late" {
			early++
		}
	}
	fi.RowCount = int64(len(rows))
	fi.MinTimeNs, fi.MaxTimeNs = rows[0].TimestampUnixNano, rows[len(rows)-1].TimestampUnixNano
	fi.LabelAggregates = map[string]map[string]int64{"name": agg}
	s.manifest = manifest.New("test-bucket", "logs/")
	s.manifest.AddFile(partitionOfKey(fi.Key), fi)
	run := tracesRunner(t, s, minute.Add(-time.Hour).UnixNano(), minute.Add(time.Hour).UnixNano())
	bg := context.Background()

	served := func(q string) (int, bool) {
		before := getCounterValue(t, metrics.MetadataOnlyFiles)
		got := sumN(run(bg, q))
		return got, getCounterValue(t, metrics.MetadataOnlyFiles) != before
	}
	if _, used := served(`* | stats by (name) count() n`); !used {
		t.Fatal("control: the pushdown was not used")
	}
	a, b := minute.Format(time.RFC3339), minute.Add(5*time.Minute).Format(time.RFC3339)
	al := minute.In(time.Local)
	dr := fmt.Sprintf(`_time:day_range[%s, %s]`, al.Format("15:04"), al.Add(time.Minute).Format("15:04"))
	for _, c := range []struct {
		q    string
		want int
	}{
		{fmt.Sprintf(`* | stats by (name) count() if (_time:[%s, %s]) n`, a, b), early},
		{fmt.Sprintf(`* | stats by (name) count() if (%s) n`, dr), early},
		{fmt.Sprintf(`name:* %s | stats by (name) count() n`, dr), early},
		{`* | stats by (name) count() if (status_code:=1) n`, 30},
	} {
		got, used := served(c.q)
		if got != c.want || used {
			t.Errorf("%s: got %d (want %d), pushdown used=%v (want a scan)", c.q, got, c.want, used)
		}
	}
}
