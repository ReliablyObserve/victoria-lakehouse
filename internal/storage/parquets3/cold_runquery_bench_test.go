package parquets3

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

func BenchmarkColdRunQuery(b *testing.B) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(b, mock.url())
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Minute)
	const n = 40000
	rows := make([]schema.LogRow, 0, n)
	svcs := []string{"alpha", "beta", "gamma", "delta"}
	for i := 0; i < n; i++ {
		svc := svcs[i%4]
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: base.Add(time.Duration(i) * 10 * time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("MARKER request %d from %s with some longer payload text to make the body column heavy %d", i%97, svc, i),
			SeverityText:      []string{"INFO", "ERROR", "WARN"}[i%3],
			ServiceName:       svc, HostName: fmt.Sprintf("host-%d", i%13),
			TraceID: fmt.Sprintf("%032x", i),
			Stream:  fmt.Sprintf(`{service.name=%q}`, svc), StreamID: fmt.Sprintf("%048x", i%4),
			LogAttributes: map[string]string{"repro_layer": []string{"cold", "warm"}[i%2], "region": fmt.Sprintf("eu-%d", i%5), "user": fmt.Sprintf("u%d", i%1000)},
		})
	}
	res, err := writeLogsParquet(rows, 5000, 3)
	if err != nil {
		b.Fatal(err)
	}
	key := fmt.Sprintf("logs/dt=%s/hour=%02d/bench.parquet", base.Format("2006-01-02"), base.Hour())
	mock.putFile(key, res.Data)
	s.manifest = manifest.New("test-bucket", "logs/")
	s.manifest.AddFile(partitionFromKey(key), manifest.FileInfo{Key: key, Size: int64(len(res.Data)), RowCount: n,
		MinTimeNs: rows[0].TimestampUnixNano, MaxTimeNs: rows[n-1].TimestampUnixNano})
	start, end := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	internalvlstorage.SetStorage(s, nil)
	defer vlapp.SetExternalStorage(nil)

	shapes := []struct{ name, q string }{
		{"reg_filter_count", `service.name:=alpha | stats count() n`},
		{"reg_filter_by", `service.name:=alpha | stats by (host.name) count() n`},
		{"reg_unfiltered_by", `* | stats by (service.name) count() n`},
		{"msg_filter_count", `MARKER | stats count() n`},
		{"unreg_filter_count", `repro_layer:=warm | stats count() n`},
		{"unreg_by", `* | stats by (region) count() n`},
		{"unfiltered_count", `* | stats count() n`},
		{"retrieval_limit", `service.name:=alpha | limit 100`},
	}
	tss := []struct {
		name string
		ts   *delete.Tombstone
	}{
		{"none", nil},
		{"tomb_reg", &delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "t", Query: `level:=ERROR`, StartNs: start, EndNs: end, Mode: "hide"}},
		{"tomb_unreg", &delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "t", Query: `user:=u7`, StartNs: start, EndNs: end, Mode: "hide"}},
		{"tomb_other_tenant", &delete.Tombstone{Tenants: []delete.TenantRef{{AccountID: 7}}, ID: "t", Query: `user:=u7`, StartNs: start, EndNs: end, Mode: "hide"}},
		{"tomb_out_of_window", &delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "t", Query: `user:=u7`, StartNs: 1, EndNs: 2, Mode: "hide"}},
	}
	for _, tc := range tss {
		store := delete.NewTombstoneStore()
		if tc.ts != nil {
			store.Add(*tc.ts)
		}
		s.SetTombstoneStore(store)
		for _, sh := range shapes {
			q, err := logstorage.ParseQuery(sh.q)
			if err != nil {
				b.Fatal(err)
			}
			q.AddTimeFilter(start, end)
			b.Run(tc.name+"/"+sh.name, func(b *testing.B) {
				b.ReportAllocs()
				var result float64
				for i := 0; i < b.N; i++ {
					result = 0
					qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
					var rowsOut int
					var mu sync.Mutex
					if err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
						mu.Lock()
						defer mu.Unlock()
						rowsOut += db.RowsCount()
						for _, c := range db.GetColumns(false) {
							if c.Name == "n" {
								for _, v := range c.Values {
									x, _ := strconv.Atoi(v)
									result += float64(x)
								}
							}
						}
					}); err != nil {
						b.Fatal(err)
					}
					if !strings.Contains(sh.q, "stats") {
						result = float64(rowsOut)
					}
				}
				b.ReportMetric(result, "answer")
			})
		}
	}
}
