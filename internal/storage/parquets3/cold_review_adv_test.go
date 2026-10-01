package parquets3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

func reviewRunner(t *testing.T, st storage.Storage, start, end int64) func(ctx context.Context, query string) []map[string]string {
	t.Helper()
	internalvlstorage.SetStorage(st, nil)
	t.Cleanup(func() { vlapp.SetExternalStorage(nil) })
	return func(ctx context.Context, query string) []map[string]string {
		t.Helper()
		q := mustParseQueryWithTime(t, query, start, end)
		qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		var out []map[string]string
		if err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			out = append(out, blockRowFields([]*logstorage.DataBlock{db})...)
			mu.Unlock()
		}); err != nil {
			t.Fatalf("RunQuery(%s): %v", query, err)
		}
		return out
	}
}

// tombstone shapes x stats shapes, with and without the hits timestamp-only hint.
func TestColdReview_TombstoneShapes(t *testing.T) {
	for _, tq := range []string{
		`needle-exact`,
		`_msg:="needle-exact"`,
		`{service.name="alpha"}`,
		`NOT level:=INFO`,
		`level:in(ERROR)`,
		`level:~"ERR"`,
		`repro_layer:=warm OR level:=ERROR`,
		`service.name:=beta repro_layer:=cold`,
		`host.name:=host-gamma`,
		`seq("MARKER", "from")`,
	} {
		t.Run(tq, func(t *testing.T) {
			s, start, end := coldFilteredStatsFixture(t)
			run := reviewRunner(t, s, start, end)
			bg := context.Background()
			all := len(run(bg, "*"))
			addHideTombstone(s, tq, start, end)
			rows := run(bg, "*")
			if len(rows) == 0 || len(rows) >= all {
				t.Fatalf("fixture: tombstone %q must hide some rows (all=%d visible=%d)", tq, all, len(rows))
			}
			for _, ctx := range []context.Context{bg, storage.WithTimestampOnlyHint(bg)} {
				hint := storage.IsTimestampOnly(ctx)
				if got := statsCount(t, run(ctx, `* | stats count() n`), "c"); got != len(rows) {
					t.Errorf("hint=%v * | stats count() = %d want %d", hint, got, len(rows))
				}
				if got := sumN(run(ctx, `* | stats by (_time:1m) count() n`)); got != len(rows) {
					t.Errorf("hint=%v hits-shaped sum = %d want %d", hint, got, len(rows))
				}
				g := statsGroups(run(ctx, `* | stats by (service.name) count() n`), "service.name")
				if fmtGroups(g) != fmtGroups(groupCounts(rows, "service.name")) {
					t.Errorf("hint=%v by service.name got %s want %s", hint, fmtGroups(g), fmtGroups(groupCounts(rows, "service.name")))
				}
				g = statsGroups(run(ctx, `* | stats by (repro_layer) count() n`), "repro_layer")
				if fmtGroups(g) != fmtGroups(groupCounts(rows, "repro_layer")) {
					t.Errorf("hint=%v by repro_layer got %s want %s", hint, fmtGroups(g), fmtGroups(groupCounts(rows, "repro_layer")))
				}
				u := run(ctx, `* | uniq by (service.name) with hits`)
				_ = u
			}
		})
	}
}

// pushdown fixture: uneven timestamps, file fully in range, LabelAggregates on service.name.
func reviewPushdownFixture(t *testing.T) (*Storage, int64, int64, []map[string]string, time.Time) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	minute := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Minute)
	var rows []schema.LogRow
	for i := 0; i < 60; i++ {
		ts := minute.Add(time.Duration(i) * 100 * time.Millisecond) // first 6s
		if i >= 54 {
			ts = minute.Add(10*time.Minute + time.Duration(i)*time.Second)
		}
		svc := []string{"alpha", "beta", "gamma"}[i%3]
		if i >= 54 {
			svc = "late"
		}
		rows = append(rows, schema.LogRow{TimestampUnixNano: ts.UnixNano(), Body: "b", SeverityText: []string{"INFO", "ERROR"}[i%2], ServiceName: svc,
			Stream: fmt.Sprintf(`{service.name=%q}`, svc), StreamID: fmt.Sprintf("%048x", len(svc))})
	}
	res, err := writeLogsParquet(rows, 20, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/pd.parquet", minute.Format("2006-01-02"), minute.Hour()), res.Data, minute)
	fi := s.manifest.GetFilesForRange(0, 1<<62)[0]
	agg := map[string]int64{}
	for _, r := range rows {
		agg[r.ServiceName]++
	}
	fi.RowCount = int64(len(rows))
	fi.MinTimeNs, fi.MaxTimeNs = rows[0].TimestampUnixNano, rows[len(rows)-1].TimestampUnixNano
	fi.LabelAggregates = map[string]map[string]int64{"service.name": agg}
	s.manifest = manifest.New("test-bucket", "logs/")
	s.manifest.AddFile(partitionFromKey(fi.Key), fi)
	start, end := minute.Add(-time.Hour).UnixNano(), minute.Add(time.Hour).UnixNano()
	run := reviewRunner(t, s, start, end)
	return s, start, end, run(context.Background(), "*"), minute
}

// count pushdown gate vs pipe/filter shapes that read _time per row.
func TestColdReview_PushdownTimeShapes(t *testing.T) {
	s, start, end, rows, minute := reviewPushdownFixture(t)
	run := reviewRunner(t, s, start, end)
	bg := context.Background()
	served := func(q string) ([]map[string]string, bool) {
		before := getCounterValue(t, metrics.MetadataOnlyFiles)
		out := run(bg, q)
		return out, getCounterValue(t, metrics.MetadataOnlyFiles) != before
	}
	// control
	if _, used := served(`* | stats by (service.name) count() n`); !used {
		t.Fatal("control: pushdown not used")
	}
	a := minute.Format(time.RFC3339)
	b := minute.Add(5 * time.Minute).Format(time.RFC3339)
	early := 0
	for _, r := range rows {
		if r["service.name"] != "late" {
			early++
		}
	}
	al := minute.In(time.Local)
	dr := fmt.Sprintf(`_time:day_range[%s, %s]`, al.Format("15:04"), al.Add(time.Minute).Format("15:04"))
	for _, c := range []struct {
		q    string
		want int
	}{
		{fmt.Sprintf(`* | stats by (service.name) count() if (_time:[%s, %s]) n`, a, b), early},
		{`* | stats by (service.name) count() if (level:=ERROR) n`, func() int {
			n := 0
			for _, r := range rows {
				if r["level"] == "ERROR" {
					n++
				}
			}
			return n
		}()},
		{fmt.Sprintf(`* | stats by (service.name) count() if (%s) n`, dr), early},
		{fmt.Sprintf(`service.name:* %s | stats by (service.name) count() n`, dr), early},
		{fmt.Sprintf(`service.name:in(alpha,beta,gamma,late) %s | stats by (service.name) count() n`, dr), early},
		{`* | fields service.name | copy host.name as service.name | stats by (service.name) count() n`, len(rows)},
	} {
		out, used := served(c.q)
		got := sumN(out)
		t.Logf("%s: pushdown=%v got=%d want=%d", c.q, used, got, c.want)
		if got != c.want {
			t.Errorf("%s: got %d want %d (pushdown=%v)", c.q, got, c.want, used)
		}
	}
}

// tsOnly synthetic path vs pipes that read _time per row (oracle: all columns).
func TestColdReview_SyntheticTimePipes(t *testing.T) {
	s, start, end, _, minute := reviewPushdownFixture(t)
	// drop aggregates so the count pushdown is not involved
	fi := s.manifest.GetFilesForRange(0, 1<<62)[0]
	fi.LabelAggregates = nil
	s.manifest = manifest.New("test-bucket", "logs/")
	s.manifest.AddFile(partitionFromKey(fi.Key), fi)
	bg := context.Background()
	a := minute.Format(time.RFC3339)
	b := minute.Add(5 * time.Minute).Format(time.RFC3339)
	al := minute.In(time.Local)
	dr := fmt.Sprintf(`_time:day_range[%s, %s]`, al.Format("15:04"), al.Add(time.Minute).Format("15:04"))
	for _, q := range []string{
		`* | stats count() n`,
		fmt.Sprintf(`* | stats count() if (_time:[%s, %s]) n`, a, b),
		fmt.Sprintf(`* | stats count() if (%s) n`, dr),
		`* | stats min(_time) n`,
		`* | stats max(_time) n`,
		`* | stats quantile(0.5, _time) n`,
		`* | stats by (_time:1m) count() n`,
		`* | stats by (_time:1m offset 30s) count() n`,
		`* | stats by (_time:7m) count() n`,
		`* | uniq by (_time)`,
		`* | stats count_uniq(_time) n`,
		`* | math _time / 1e9 as x | stats count() n`,
		fmt.Sprintf(`* | filter %s | stats count() n`, dr),
		`* | top 3 (_time)`,
		`* | stats by (_time:1m) count() n | stats sum(n) n`,
	} {
		run := reviewRunner(t, s, start, end)
		got := canonRows(run(bg, q))
		runAll := reviewRunner(t, allColumnsStore{s}, start, end)
		want := canonRows(runAll(bg, q))
		if got != want {
			t.Errorf("%s\n  projected: %s\n  all-cols:  %s", q, got, want)
		}
	}
	_ = storage.IsTimestampOnly
}
