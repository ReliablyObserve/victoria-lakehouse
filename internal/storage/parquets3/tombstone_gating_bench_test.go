package parquets3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// BenchmarkTombstoneGating is the perf guard of per-file tombstone gating: 8
// objects of 5,000 rows each behind a counting mock S3, answered with 0, 1, 10
// and 100 upstream-shaped tombstones (no lower bound, tenant 0). "label"
// tombstones name a value no file holds (provable clean), "older" end before
// the first file (provable by time), "user" name a map attribute that has rows
// in every file (not provable: those files are read). Every answer is the sum
// of the rows counted and is reported as "answer" so two builds are compared on
// identical answers; "gets/op" is the object reads per query.
func BenchmarkTombstoneGating(b *testing.B) { benchTombstoneGating(b, false) }

// BenchmarkTombstoneGatingCold is the same with the caches emptied before every
// query, so "gets/op" is the object reads a query really makes.
func BenchmarkTombstoneGatingCold(b *testing.B) { benchTombstoneGating(b, true) }

func benchTombstoneGating(b *testing.B, cold bool) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(b, mock.url())
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Minute)
	const files, perFile = 8, 5000
	svcs := []string{"alpha", "beta", "gamma", "delta"}
	s.manifest = manifest.New("test-bucket", "logs/")
	var first int64
	for f := 0; f < files; f++ {
		rows := make([]schema.LogRow, 0, perFile)
		for j := 0; j < perFile; j++ {
			i := f*perFile + j
			svc := svcs[i%4]
			rows = append(rows, schema.LogRow{
				TimestampUnixNano: base.Add(time.Duration(i) * 10 * time.Millisecond).UnixNano(),
				Body:              fmt.Sprintf("request %d from %s", i%97, svc),
				SeverityText:      []string{"INFO", "ERROR", "WARN"}[i%3],
				ServiceName:       svc,
				TraceID:           fmt.Sprintf("%032x", i),
				Stream:            fmt.Sprintf(`{service.name=%q}`, svc), StreamID: fmt.Sprintf("%048x", i%4),
				LogAttributes: map[string]string{"region": fmt.Sprintf("eu-%d", i%5), "user": fmt.Sprintf("u%d", i%1000)},
			})
		}
		res, err := writeLogsParquet(rows, perFile, 3)
		if err != nil {
			b.Fatal(err)
		}
		key := fmt.Sprintf("logs/dt=%s/hour=%02d/bench%02d.parquet", base.Format("2006-01-02"), base.Hour(), f)
		mock.putFile(key, res.Data)
		minT, maxT := schema.LogRowTimeBounds(rows)
		if f == 0 {
			first = minT
		}
		s.manifest.AddFile(partitionFromKey(key), manifest.FileInfo{Key: key, Size: int64(len(res.Data)), RowCount: perFile,
			MinTimeNs: minT, MaxTimeNs: maxT, Labels: extractLogLabels(rows), LabelAggregates: schema.ExtractLogLabelAggregates(rows)})
	}
	start, end := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	internalvlstorage.SetStorage(s, nil)
	defer vlapp.SetExternalStorage(nil)

	shapes := []struct {
		name, q string
		hint    bool
	}{
		{"count", `* | stats count() n`, true},
		{"hits", `* | stats by (_time:5m) count() n`, true},
		{"filtered_count", `service.name:=alpha | stats count() n`, false},
		{"count_by_service", `* | stats by (service.name) count() n`, false},
		{"field_names", `* | field_names`, false},
	}
	now := time.Now().UnixNano()
	tombstones := func(kind string, k int) []delete.Tombstone {
		out := make([]delete.Tombstone, 0, k)
		for i := 0; i < k; i++ {
			ts := delete.Tombstone{ID: fmt.Sprintf("t%d", i), Tenants: []delete.TenantRef{{}}, StartNs: -1 << 63, EndNs: now, Mode: "hide"}
			switch kind {
			case "label":
				ts.Query = fmt.Sprintf(`service.name:="gone%d"`, i)
			case "older":
				ts.Query, ts.EndNs = fmt.Sprintf(`user:="u%d"`, 900+i%100), first-1
			default:
				ts.Query = fmt.Sprintf(`user:="u%d"`, 900+i%100)
			}
			out = append(out, ts)
		}
		return out
	}
	sets := []struct {
		kind string
		k    int
	}{{"none", 0}, {"user", 1}, {"user", 10}, {"user", 100}, {"label", 1}, {"label", 10}, {"label", 100}, {"older", 1}, {"older", 100}}
	for _, set := range sets {
		store := delete.NewTombstoneStore()
		for _, ts := range tombstones(set.kind, set.k) {
			store.Add(ts)
		}
		s.SetTombstoneStore(store)
		for _, sh := range shapes {
			q, err := logstorage.ParseQuery(sh.q)
			if err != nil {
				b.Fatal(err)
			}
			q.AddTimeFilter(start, end)
			b.Run(fmt.Sprintf("%s%d/%s", set.kind, set.k, sh.name), func(b *testing.B) {
				b.ReportAllocs()
				ctx := context.Background()
				if sh.hint {
					ctx = storage.WithTimestampOnlyHint(ctx)
				}
				getsBefore := mock.gets.Load()
				var answer int64
				for i := 0; i < b.N; i++ {
					answer = 0
					if cold {
						b.StopTimer()
						s.memCache = cache.NewLRU(64 * 1024 * 1024)
						s.footerCache = NewFooterCache(0)
						b.StartTimer()
					}
					qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, nil, q, false, nil)
					var mu sync.Mutex
					if err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
						mu.Lock()
						defer mu.Unlock()
						answer += int64(db.RowsCount())
						for _, c := range db.GetColumns(false) {
							if c.Name == "n" {
								answer = answer - int64(db.RowsCount())
								for _, v := range c.Values {
									var x int64
									_, _ = fmt.Sscan(v, &x)
									answer += x
								}
							}
						}
					}); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(answer), "answer")
				b.ReportMetric(float64(mock.gets.Load()-getsBefore)/float64(b.N), "gets/op")
			})
		}
	}
}
