package parquets3

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

func TestColdReview_HitsHintStarPipes(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var rows []schema.LogRow
	for i := 0; i < 30; i++ {
		svc := []string{"alpha", "beta", "gamma"}[i%3]
		rows = append(rows, schema.LogRow{TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(), Body: "msg " + svc, SeverityText: []string{"INFO", "ERROR"}[i%2], ServiceName: svc,
			Stream: fmt.Sprintf(`{service.name=%q}`, svc), StreamID: fmt.Sprintf("%048x", len(svc))})
	}
	res, err := writeLogsParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/h.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	start, end := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	t.Cleanup(func() { vlapp.SetExternalStorage(nil) })
	run := func(st storage.Storage, ctx context.Context, query string) int {
		internalvlstorage.SetStorage(st, nil)
		q := mustParseQueryWithTime(t, query, start, end)
		qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		n := 0
		if err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			for _, r := range blockRowFields([]*logstorage.DataBlock{db}) {
				var v int
				_, _ = fmt.Sscan(r["n"], &v)
				n += v
			}
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	hint := storage.WithTimestampOnlyHint(context.Background())
	for _, tomb := range []bool{false, true} {
		if tomb {
			st := delete.NewTombstoneStore()
			st.Add(delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "x", Query: `level:=ERROR`, StartNs: start, EndNs: end, Mode: "hide"})
			s.SetTombstoneStore(st)
		}
		for _, q := range []string{
			`* | pack_json | filter _msg:alpha | stats by (_time:1m) count() n`,
			`* | pack_logfmt as y | filter y:alpha | stats by (_time:1m) count() n`,
			`* | pack_json | stats by (_time:1m) count() n`,
		} {
			got := run(s, hint, q)
			want := run(allColumnsStore{s}, context.Background(), q)
			t.Logf("tomb=%v %-70s hint-projected=%d all-columns=%d", tomb, q, got, want)
			if got != want {
				t.Errorf("tomb=%v %s: hint=%d want %d", tomb, q, got, want)
			}
		}
	}
}
