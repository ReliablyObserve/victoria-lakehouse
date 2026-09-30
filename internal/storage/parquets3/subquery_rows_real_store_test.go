package parquets3

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// The registry rows vl.pipe.union.basic, vl.filter.in.subquery and
// vl.pipe.stream_context.basic, proved over the REAL parquets3 Storage (Parquet
// in a mock S3, the store's own filter extraction and pushdown) through the
// logs binary's adapter, reached the way the binary reaches it:
// app/vlstorage.RunQuery -> internal/vlstorage adapter -> Storage.
func TestRealStore_SubqueryRows(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())

	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	var rows []schema.LogRow
	add := func(i int, svc, level, stream string) {
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              fmt.Sprintf("line %d", i),
			SeverityText:      level,
			ServiceName:       svc,
			Stream:            fmt.Sprintf(`{service.name=%q}`, svc),
			StreamID:          stream,
		})
	}
	// 9 consecutive lines of one stream, the 5th (index 4) is the only ERROR.
	for i := 0; i < 9; i++ {
		lvl := "INFO"
		if i == 4 {
			lvl = "ERROR"
		}
		add(i, "ctx", lvl, "0000000000000000000000000000000000000000000000aa")
	}
	// A WARN line elsewhere, for the union row.
	add(20, "other", "WARN", "0000000000000000000000000000000000000000000000bb")
	// 130 lines of a busy service and 5 of a quiet one, for the in() row.
	for i := 0; i < 130; i++ {
		add(100+i, "busy", "DEBUG", "0000000000000000000000000000000000000000000000cc")
	}
	for i := 0; i < 5; i++ {
		add(300+i, "quiet", "DEBUG", "0000000000000000000000000000000000000000000000dd")
	}
	res, err := writeLogsParquet(rows, 1000, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/rows.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)

	internalvlstorage.SetStorage(s, nil)
	t.Cleanup(func() { vlapp.SetExternalStorage(nil) })

	start, end := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	run := func(query string) []map[string]string {
		q := mustParseQueryWithTime(t, query, start, end)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
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

	t.Run("vl.pipe.union.basic", func(t *testing.T) {
		got := run(`level:="ERROR" | union (level:="WARN") | stats count() as n`)
		if len(got) != 1 || got[0]["n"] != "2" {
			t.Fatalf("got %v, want one row n=2 (1 ERROR + 1 WARN)", got)
		}
	})

	t.Run("vl.filter.in.subquery", func(t *testing.T) {
		got := run(`service.name:in(* | stats by (service.name) count() as n | filter n:>100 | fields service.name) | limit 100`)
		if len(got) != 100 {
			t.Fatalf("got %d rows, want 100", len(got))
		}
		for _, r := range got {
			if r["service.name"] != "busy" {
				t.Fatalf("row from a service under the threshold: %v", r)
			}
		}
	})

	t.Run("vl.pipe.stream_context.basic", func(t *testing.T) {
		got := run(`level:="ERROR" | stream_context before 3 after 3 | limit 100`)
		var msgs []string
		for _, r := range got {
			msgs = append(msgs, r["_msg"])
		}
		sort.Strings(msgs)
		t.Logf("stream_context rows: %s", strings.Join(msgs, " | "))
		if len(got) != 7 {
			t.Fatalf("got %d rows, want 7 (the ERROR line plus 3 before and 3 after in its stream)", len(got))
		}
	})
}
