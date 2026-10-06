package parquets3

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// TestFlush_SpanEventsLinksScopeAttrs_SurviveToCold is the issue #409 chain end
// to end, written against interfaces that exist on main too (so it fails there):
//
//	hot: the VictoriaLogs buffer, which VictoriaTraces' own storage is built from
//	flush: buffer rows -> DataBlockToTraceRows -> the production Parquet writer
//	cold: the Parquet file read back through the cold query path
//
// The event, link and scope-attribute fields of a span must be the same on both
// sides.
func TestFlush_SpanEventsLinksScopeAttrs_SurviveToCold(t *testing.T) {
	st, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for i := 0; i < 3; i++ {
		f := []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "checkout"},
			{Name: "scope_name", Value: "io.opentelemetry.http"},
			{Name: "scope_version", Value: "1.2.3"},
			{Name: "scope_attr:scope.key", Value: fmt.Sprintf("scope-%d", i)},
			{Name: "span_id", Value: fmt.Sprintf("%016x", i+1)},
			{Name: "name", Value: "POST /pay"},
			{Name: "start_time_unix_nano", Value: fmt.Sprintf("%d", base.UnixNano()+int64(i))},
			{Name: "end_time_unix_nano", Value: fmt.Sprintf("%d", base.UnixNano()+int64(i)+1000)},
			{Name: "duration", Value: "1000"},
			{Name: "_msg", Value: "-"},
		}
		// Span 0 has no events; span 1 has two events and a link; span 2 twelve events.
		nEvents := []int{0, 2, 12}[i]
		for e := 0; e < nEvents; e++ {
			s := fmt.Sprintf(":%d", e)
			f = append(f,
				logstorage.Field{Name: "event:event_time_unix_nano" + s, Value: fmt.Sprintf("%d", base.UnixNano()+int64(e))},
				logstorage.Field{Name: "event:event_name" + s, Value: "exception"},
				logstorage.Field{Name: "event:event_dropped_attributes_count" + s, Value: "0"},
				logstorage.Field{Name: "event:event_attr:exception.type" + s, Value: "IOError"},
				logstorage.Field{Name: "event:event_attr:exception.stacktrace" + s, Value: "at a.b\nat c.d"},
			)
		}
		if i == 1 {
			f = append(f,
				logstorage.Field{Name: "link:link_trace_id:0", Value: strings.Repeat("a", 32)},
				logstorage.Field{Name: "link:link_span_id:0", Value: strings.Repeat("b", 16)},
				logstorage.Field{Name: "link:link_trace_state:0", Value: "k=v"},
				logstorage.Field{Name: "link:link_dropped_attributes_count:0", Value: "0"},
				logstorage.Field{Name: "link:link_flags:0", Value: "257"},
				logstorage.Field{Name: "link:link_attr:link.reason:0", Value: "retry"},
			)
		}
		f = append(f, logstorage.Field{Name: "trace_id", Value: fmt.Sprintf("%032x", i+1)})
		lr.MustAdd(logstorage.TenantID{}, base.UnixNano()+1000+int64(i), f, 1)
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()

	// hot: what the buffer (upstream storage) answers for `*`.
	hot := map[string]map[string]string{}
	{
		q, err := logstorage.ParseQueryAtTimestamp("*", math.MaxInt64)
		if err != nil {
			t.Fatal(err)
		}
		q = q.CloneWithTimeFilter(q.GetTimestamp(), 0, math.MaxInt64)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{{}}, q, false, nil)
		var mu sync.Mutex
		var rows []*logstorage.DataBlock
		if err := st.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			rows = append(rows, cloneBlock(db))
			mu.Unlock()
		}); err != nil {
			t.Fatal(err)
		}
		for _, r := range blockRowFields(rows) {
			hot[r["span_id"]] = r
		}
		// The buffer's blocks are re-read below for the flush.
	}
	if len(hot) != 3 {
		t.Fatalf("hot answered %d spans, want 3", len(hot))
	}

	// flush: the same conversion the buffer flusher runs.
	var traceRows = flushRows(t, st)
	res, err := writeTracesParquet(traceRows, 1000, 3)
	if err != nil {
		t.Fatal(err)
	}

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/flushed.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	cold := map[string]map[string]string{}
	for _, r := range coldSelectRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())(`*`) {
		cold[r["span_id"]] = r
	}
	if len(cold) != 3 {
		t.Fatalf("cold answered %d spans, want 3", len(cold))
	}

	extra := func(r map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range r {
			if strings.HasPrefix(k, "event:") || strings.HasPrefix(k, "link:") || strings.HasPrefix(k, "scope_attr:") || k == "scope_name" || k == "scope_version" {
				out[k] = v
			}
		}
		return out
	}
	for id, h := range hot {
		he, ce := extra(h), extra(cold[id])
		if len(he) == 0 {
			t.Fatalf("hot span %s has no event/link/scope fields: the fixture is wrong", id)
		}
		for k, v := range he {
			if ce[k] != v {
				t.Errorf("span %s: cold %s = %q, hot has %q", id, k, ce[k], v)
			}
		}
		for k := range ce {
			if _, ok := he[k]; !ok {
				t.Errorf("span %s: cold has %s, hot does not", id, k)
			}
		}
	}
}

func cloneBlock(db *logstorage.DataBlock) *logstorage.DataBlock {
	cols := db.GetColumns(false)
	out := make([]logstorage.BlockColumn, len(cols))
	for i, c := range cols {
		vals := make([]string, len(c.Values))
		for j, v := range c.Values {
			vals[j] = strings.Clone(v)
		}
		out[i] = logstorage.BlockColumn{Name: strings.Clone(c.Name), Values: vals}
	}
	cp := &logstorage.DataBlock{}
	cp.SetColumns(out)
	return cp
}

// flushRows is what the buffer flusher does with a window of the buffer.
func flushRows(t *testing.T, st *membuffer.Store) []schema.TraceRow {
	t.Helper()
	q, err := logstorage.ParseQueryAtTimestamp("*", math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), 0, math.MaxInt64)
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{{}}, q, false, nil)
	var mu sync.Mutex
	var rows []schema.TraceRow
	if err := st.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		conv := vlstorage.DataBlockToTraceRows(db, logstorage.TenantID{})
		mu.Lock()
		rows = append(rows, conv...)
		mu.Unlock()
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}
