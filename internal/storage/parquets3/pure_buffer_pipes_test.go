package parquets3

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
)

// TestRunQuery_PureBufferWindow_PipesThroughAdapter is the end-to-end check for
// a window no Parquet object covers yet (for this tenant): the answer comes from
// the co-located buffer, and the storage adapter applies the query's pipes over
// it exactly once — like upstream VL/VT, and like the adapters do for Parquet
// rows. Before, the buffer ran the pipes itself: a filtered count answered 0,
// an unfiltered count answered 1, and hits came back empty.
func TestRunQuery_PureBufferWindow_PipesThroughAdapter(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open buffer: %v", err)
	}
	defer bs.Close()

	now := time.Now().UnixNano()
	tenant := logstorage.TenantID{AccountID: 3003}
	other := logstorage.TenantID{AccountID: 1001}
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 5; i++ {
		lr.MustAdd(tenant, now+int64(i), []logstorage.Field{{Name: "service.name", Value: "svc-a"}, {Name: "_msg", Value: "marker"}}, 1)
	}
	for i := 0; i < 2; i++ {
		lr.MustAdd(tenant, now+int64(10+i), []logstorage.Field{{Name: "service.name", Value: "svc-b"}, {Name: "_msg", Value: "other"}}, 1)
	}
	for i := 0; i < 3; i++ {
		lr.MustAdd(other, now+int64(20+i), []logstorage.Field{{Name: "service.name", Value: "svc-foreign"}, {Name: "_msg", Value: "marker"}}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	s := testStorage()
	s.manifest = manifest.New("test", "")
	s.localBuffer = bs

	run := func(t *testing.T, queryStr string) []string {
		t.Helper()
		q, err := logstorage.ParseQueryAtTimestamp(queryStr, now)
		if err != nil {
			t.Fatalf("parse %q: %v", queryStr, err)
		}
		q.AddTimeFilter(now-int64(time.Hour), now+int64(time.Hour))
		ids := []logstorage.TenantID{tenant}
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, ids, q, false, nil)
		var mu sync.Mutex
		var cells []string
		wb := func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			for _, c := range db.GetColumns(false) {
				for _, v := range c.Values {
					cells = append(cells, c.Name+"="+v)
				}
			}
		}
		searchFn := func(w logstorage.WriteDataBlockFunc) error {
			return s.RunQuery(context.Background(), ids, q, w)
		}
		if err := logstorage.RunQueryExternal(qctx, searchFn, wb); err != nil {
			t.Fatalf("RunQueryExternal(%q): %v", queryStr, err)
		}
		sort.Strings(cells)
		return cells
	}

	cases := []struct {
		query string
		want  string
	}{
		{`* | stats count() n`, "n=7"},
		{`_msg:="marker" | stats count() n`, "n=5"},
		{`* | stats by (service.name) count() n`, "n=2,n=5,service.name=svc-a,service.name=svc-b"},
		{`_msg:="marker" | stats by (_time:1h) count() hits`, "hits=5"},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			got := run(t, tc.query)
			var keep []string
			for _, c := range got {
				if !strings.HasPrefix(c, "_time=") {
					keep = append(keep, c)
				}
			}
			if strings.Join(keep, ",") != tc.want {
				t.Errorf("got %v, want %s", keep, tc.want)
			}
		})
	}
}
