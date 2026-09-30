package vtstorageadapter

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
	"github.com/cespare/xxhash/v2"
)

// vtIssuedLookup builds the lookup query exactly as VictoriaTraces' Jaeger and
// Tempo GetTrace do (same format string, AddPipeOffsetLimit(0, 10), and the
// time filter Tempo adds from its start/end), so the shape matcher is pinned to
// what upstream really sends and not to a hand-written approximation.
func vtIssuedLookup(t *testing.T, traceID string, withTimeFilter bool) *logstorage.Query {
	t.Helper()
	qStr := `{` + otelpb.TraceIDIndexStreamName + `="` +
		strconv.FormatUint(xxhash.Sum64String(traceID)%otelpb.TraceIDIndexPartitionCount, 10) + `"} AND ` +
		otelpb.TraceIDIndexFieldName + `:=` + strconv.Quote(traceID) +
		` | stats min(_time) _time, min(` + otelpb.TraceIDIndexStartTimeFieldName + `) ` + otelpb.TraceIDIndexStartTimeFieldName +
		`, max(` + otelpb.TraceIDIndexEndTimeFieldName + `) ` + otelpb.TraceIDIndexEndTimeFieldName
	q, err := logstorage.ParseQueryAtTimestamp(qStr, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("parse %q: %v", qStr, err)
	}
	q.AddPipeOffsetLimit(0, 10)
	if withTimeFilter {
		q.AddTimeFilter(1_000_000_000, 2_000_000_000)
	}
	return q
}

// A user's own LogsQL that mentions trace_id_idx must reach the store with the
// user's pipes intact and must never be answered by the synthetic index row.
func TestAdapter_UserLogsQLWithTraceIDIdxIsNotHijacked(t *testing.T) {
	for _, qs := range []string{
		`trace_id_idx:=X | stats count()`,
		`trace_id_idx:=X | stats min(_time) as _time`,
		`{trace_id_idx_stream="3"} AND trace_id_idx:=X | stats count()`,
		`"trace_id_idx:=X" | stats count()`,
		`message:"a trace_id_idx:=X b"`,
		`service.name:="trace_id_idx:=X"`,
		`foo:bar | fields "trace_id_idx:=X"`,
	} {
		t.Run(qs, func(t *testing.T) {
			var seen []string
			store := &fastpathStore{
				found: true, startNs: 1, endNs: 2,
				runQuery: func(q *logstorage.Query) { seen = append(seen, q.String()) },
			}
			a := &Adapter{store: store}
			q, err := logstorage.ParseQueryAtTimestamp(qs, 1_000_000_000)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			want := q.String()
			wb := func(uint, *logstorage.DataBlock) {}
			if err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, wb); err != nil {
				t.Fatalf("RunQuery: %v", err)
			}
			if store.got != "" {
				t.Fatalf("index lookup used (%q) for a user query", store.got)
			}
			if len(seen) != 1 {
				t.Fatalf("store saw %d queries, want 1", len(seen))
			}
			if strings.Contains(seen[0], "trace_id:=") || strings.Contains(seen[0], "start_time_unix_nano") {
				t.Fatalf("query was rewritten into a span scan: %s", seen[0])
			}
			// The stream-selector strip only applies to a leading selector.
			if strings.Contains(want, "trace_id_idx_stream") && strings.HasPrefix(want, "{") {
				return
			}
			if !strings.Contains(seen[0], strings.TrimSpace(strings.SplitN(want, "|", 2)[0])) {
				t.Fatalf("store got %q, want the caller's filter %q", seen[0], want)
			}
		})
	}
}

// What VT really issues keeps the fast path (and, on a miss, the span-scan
// rewrite), with or without the time filter, for a hex and an odd trace ID.
func TestAdapter_VTIssuedLookupKeepsFastPath(t *testing.T) {
	for _, withTime := range []bool{false, true} {
		for _, id := range []string{"0123456789abcdef0123456789abcdef", "trace-with-dashes_1"} {
			t.Run(id, func(t *testing.T) {
				store := &fastpathStore{found: true, startNs: 10, endNs: 20}
				a := &Adapter{store: store}
				var emitted *logstorage.DataBlock
				q := vtIssuedLookup(t, id, withTime)
				err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q},
					func(_ uint, db *logstorage.DataBlock) { emitted = db })
				if err != nil {
					t.Fatal(err)
				}
				if store.got != id || emitted == nil {
					t.Fatalf("fast path not taken for %q (time=%v): lookup=%q emitted=%v", id, withTime, store.got, emitted != nil)
				}
			})
		}
	}
}

func TestAdapter_VTIssuedLookupMissRewritesToSpanScan(t *testing.T) {
	var seen string
	store := &fastpathStore{found: false, runQuery: func(q *logstorage.Query) { seen = q.String() }}
	a := &Adapter{store: store}
	q := vtIssuedLookup(t, "abc123", true)
	if err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, func(uint, *logstorage.DataBlock) {}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(seen, `trace_id:=abc123`) || strings.Contains(seen, "trace_id_idx") {
		t.Fatalf("miss did not fall back to the span-scan rewrite: %s", seen)
	}
}

func TestTraceIndexShapeTraceID(t *testing.T) {
	good := vtIssuedLookup(t, "abc", true).String()
	if id, ok := traceIndexShapeTraceID(good); !ok || id != "abc" {
		t.Fatalf("VT shape not recognised: %q -> %q %v", good, id, ok)
	}
	for _, bad := range []string{
		`trace_id_idx:=abc`,
		`trace_id_idx:=abc | stats count(*) as "count(*)"`,
		`"{trace_id_idx_stream=\"1\"} trace_id_idx:=abc | stats min(_time) as _time, min(start_time) as start_time, max(end_time) as end_time"`,
		good + ` | fields x`,
		`foo ` + good,
	} {
		if _, ok := traceIndexShapeTraceID(bad); ok {
			t.Errorf("non-VT shape accepted: %q", bad)
		}
	}
}
