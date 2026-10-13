package vtstorageadapter

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

// With the trace_id_idx table on, the adapter takes no shortcut: VictoriaTraces' own
// stage-1 query, the TraceQL duration join and the VTUI heatmap reach the storage with
// their index-stream selector, and the footer lookup is not asked.
func TestIdxTable_AdapterTakesNoShortcut(t *testing.T) {
	if !tracesidx.Enabled {
		t.Skip("the trace_id_idx table is off")
	}
	queries := map[string]*logstorage.Query{
		"stage 1 (Jaeger and Tempo)": vtIssuedLookup(t, "0123456789abcdef0123456789abcdef", false),
		"stage 1 with a time filter": vtIssuedLookup(t, "0123456789abcdef0123456789abcdef", true),
	}
	for name, qs := range map[string]string{
		"TraceQL duration join": `{trace_id_idx_stream!=""} AND duration:>1000000 | fields trace_id_idx | rename trace_id_idx as trace_id`,
		"heatmap":               `{trace_id_idx_stream!=""} | stats by (_time:5m) count() as count`,
	} {
		q, err := logstorage.ParseQueryAtTimestamp(qs, time.Now().UnixNano())
		if err != nil {
			t.Fatal(err)
		}
		queries[name] = q
	}
	for name, q := range queries {
		t.Run(name, func(t *testing.T) {
			var seen []string
			store := &fastpathStore{found: true, startNs: 1, endNs: 2, runQuery: func(q *logstorage.Query) { seen = append(seen, q.String()) }}
			a := &Adapter{store: store}
			if err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, func(uint, *logstorage.DataBlock) {}); err != nil {
				t.Fatal(err)
			}
			if store.got != "" {
				t.Fatalf("the footer lookup answered (%q)", store.got)
			}
			if len(seen) == 0 {
				t.Fatal("the storage saw no query")
			}
			for _, s := range seen {
				if !strings.Contains(s, "trace_id_idx_stream") {
					t.Errorf("the index-stream selector was stripped: %s", s)
				}
				if strings.Contains(s, "trace_id:=") || strings.Contains(s, "start_time_unix_nano") {
					t.Errorf("the query was rewritten into a span scan: %s", s)
				}
			}
		})
	}
}
