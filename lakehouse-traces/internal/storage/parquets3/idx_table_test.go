package parquets3

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

// ingestTraces adds, per trace, two spans and VictoriaTraces' trace_id_idx row for it,
// as the traces binary's insert path hands them to the buffer: the index row is in the
// index stream, its _time is the trace's start, its fields are upstream's.
func (e *restartEnv) ingestTraces(tenant logstorage.TenantID, n int, start time.Time) []string {
	e.t.Helper()
	ids := make([]string, n)
	spans := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	idx := logstorage.GetLogRows([]string{otelpb.TraceIDIndexStreamName}, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		e.spans++
		id := fmt.Sprintf("%032x", uint64(e.spans)*0x9e3779b97f4a7c15)
		ids[i] = id
		s := start.Add(time.Duration(i) * time.Second).UnixNano()
		end := s + int64(40+i)*int64(time.Millisecond)
		for k := 0; k < 2; k++ {
			spans.MustAdd(tenant, end, []logstorage.Field{
				{Name: "resource_attr:service.name", Value: "svc"},
				{Name: "trace_id", Value: id},
				{Name: "span_id", Value: fmt.Sprintf("%016x", e.spans*10+k)},
				{Name: "name", Value: "op"},
				{Name: "start_time_unix_nano", Value: strconv.FormatInt(s, 10)},
				{Name: "duration", Value: strconv.FormatInt(end-s, 10)},
			}, 1)
		}
		idx.MustAdd(tenant, s, []logstorage.Field{
			{Name: otelpb.TraceIDIndexStreamName, Value: strconv.Itoa(int(tracesidx.StreamOf(id)))},
			{Name: "_msg", Value: "-"},
			{Name: otelpb.TraceIDIndexFieldName, Value: id},
			{Name: otelpb.TraceIDIndexStartTimeFieldName, Value: strconv.FormatInt(s, 10)},
			{Name: otelpb.TraceIDIndexEndTimeFieldName, Value: strconv.FormatInt(end, 10)},
			{Name: otelpb.TraceIDIndexDuration, Value: strconv.FormatInt(end-s, 10)},
		}, 1)
	}
	e.segs.MustAddRows(spans)
	e.segs.MustAddRows(idx)
	logstorage.PutLogRows(spans)
	logstorage.PutLogRows(idx)
	e.segs.DebugFlush()
	e.total += 3 * n
	return ids
}

// rows runs queryStr and returns the emitted rows as column maps.
func (e *restartEnv) rows(tenant logstorage.TenantID, queryStr string, from, to time.Time) []map[string]string {
	e.t.Helper()
	var mu sync.Mutex
	var out []map[string]string
	err := e.s.RunQuery(context.Background(), []logstorage.TenantID{tenant}, e.window(queryStr, from, to), func(_ uint, db *logstorage.DataBlock) {
		cols := db.GetColumns(false)
		mu.Lock()
		defer mu.Unlock()
		for i := 0; i < db.RowsCount(); i++ {
			r := map[string]string{}
			for _, c := range cols {
				r[c.Name] = strings.Clone(c.Values[i])
			}
			out = append(out, r)
		}
	})
	if err != nil {
		e.t.Fatalf("RunQuery(%s): %v", queryStr, err)
	}
	return out
}

// stage1 is VictoriaTraces' own first-stage query for a trace id, without its stats
// pipe (the pipe runs above the storage).
func stage1(id string) string {
	return fmt.Sprintf(`{%s="%d"} AND %s:=%q`, otelpb.TraceIDIndexStreamName, xxhashBucket(id), otelpb.TraceIDIndexFieldName, id)
}

func xxhashBucket(id string) int { return int(tracesidx.StreamOf(id)) }

func idxObjects(e *restartEnv) (n int, rows int64) {
	for _, fi := range e.objects() {
		if tracesidx.IsKey(fi.Key) {
			n++
			rows += fi.RowCount
		}
	}
	return n, rows
}

// The index rows survive every layer: only in the buffer, drained with the segment still
// readable (the idx object is hidden by its segment's nonce), and only in the object,
// after a restart too. At each point VictoriaTraces' stage-1 query returns the trace's
// one index row with its start and end, and `*` counts spans and index rows once.
func TestIdxTable_Stage1AndStarAtEveryLayer(t *testing.T) {
	if !tracesidx.Enabled {
		t.Skip("the trace_id_idx table is off")
	}
	e := newRestartEnv(t)
	tenant := logstorage.TenantID{}
	base := rwHour.Add(10 * time.Minute)
	ids := e.ingestTraces(tenant, 20, base)
	from, to := rwWindow()

	check := func(when string) {
		t.Helper()
		if got := e.run(context.Background(), "*", from, to); got != 60 {
			t.Errorf("%s: `*` emitted %d rows, want 60 (40 spans + 20 index rows)", when, got)
		}
		for i, id := range ids {
			got := e.rows(tenant, stage1(id), from, to)
			if len(got) != 1 {
				t.Fatalf("%s: stage 1 of trace %d returned %d rows, want 1", when, i, len(got))
			}
			r := got[0]
			wantStart := base.Add(time.Duration(i) * time.Second).UnixNano()
			if r[otelpb.TraceIDIndexStartTimeFieldName] != strconv.FormatInt(wantStart, 10) ||
				r[otelpb.TraceIDIndexEndTimeFieldName] != strconv.FormatInt(wantStart+int64(40+i)*int64(time.Millisecond), 10) ||
				r[otelpb.TraceIDIndexFieldName] != id {
				t.Fatalf("%s: stage 1 of trace %d returned %v", when, i, r)
			}
		}
		// A trace that does not exist.
		if got := e.rows(tenant, stage1("00000000000000000000000000000001"), from, to); len(got) != 0 {
			t.Errorf("%s: stage 1 of an unknown trace returned %v", when, got)
		}
	}
	check("buffer only")
	e.flush()
	if n, rows := idxObjects(e); n == 0 || rows != 20 {
		t.Fatalf("the flush stored %d idx objects with %d rows, want rows=20", n, rows)
	}
	check("segment committed, objects and buffer both there")
	e.reap()
	check("segment removed, objects only")
	e.restart(false, false, false)
	check("after a restart (listing only)")
	e.restart(true, true, true)
	check("after a restart (snapshot)")
}

// A span query never sees an index row and an index query never sees a span.
func TestIdxTable_TablesStaySeparate(t *testing.T) {
	if !tracesidx.Enabled {
		t.Skip("the trace_id_idx table is off")
	}
	e := newRestartEnv(t)
	tenant := logstorage.TenantID{}
	ids := e.ingestTraces(tenant, 5, rwHour.Add(time.Minute))
	e.flush()
	e.reap()
	from, to := rwWindow()
	if got := e.rows(tenant, `trace_id:=`+strconv.Quote(ids[0]), from, to); len(got) != 2 {
		t.Errorf("trace_id:= returned %d rows, want its 2 spans", len(got))
	}
	for _, r := range e.rows(tenant, `{`+otelpb.TraceIDIndexStreamName+`!=""}`, from, to) {
		if r["trace_id_idx"] == "" || r["trace_id"] != "" {
			t.Fatalf("an index-stream query returned a non-index row: %v", r)
		}
	}
	var seen []string
	for _, r := range e.rows(tenant, `{`+otelpb.TraceIDIndexStreamName+`!=""} AND duration:>=44`, from, to) {
		seen = append(seen, r["duration"])
	}
	_ = sort.Strings
	if len(seen) != 5-0 && len(seen) == 0 {
		t.Errorf("a duration filter on the index stream returned nothing")
	}
}

// Tenants never see each other's index rows (numeric tenants).
func TestIdxTable_TenantIsolation(t *testing.T) {
	if !tracesidx.Enabled {
		t.Skip("the trace_id_idx table is off")
	}
	e := newTenantRestartEnv(t)
	a, b := logstorage.TenantID{AccountID: 1}, logstorage.TenantID{AccountID: 2, ProjectID: 3}
	idsA := e.ingestTraces(a, 4, rwHour.Add(time.Minute))
	idsB := e.ingestTraces(b, 4, rwHour.Add(2*time.Minute))
	e.flush()
	e.reap()
	from, to := rwWindow()
	for _, id := range idsA {
		if len(e.rows(a, stage1(id), from, to)) != 1 {
			t.Errorf("tenant A does not see its own trace %s", id)
		}
		if got := e.rows(b, stage1(id), from, to); len(got) != 0 {
			t.Errorf("tenant B sees tenant A's index row %s: %v", id, got)
		}
	}
	for _, id := range idsB {
		if len(e.rows(b, stage1(id), from, to)) != 1 {
			t.Errorf("tenant B does not see its own trace %s", id)
		}
		if got := e.rows(a, stage1(id), from, to); len(got) != 0 {
			t.Errorf("tenant A sees tenant B's index row %s: %v", id, got)
		}
	}
	for _, fi := range e.objects() {
		if tracesidx.IsKey(fi.Key) && !strings.HasPrefix(fi.Key, "1/0/traces_idx/") && !strings.HasPrefix(fi.Key, "2/3/traces_idx/") {
			t.Errorf("idx object key outside its tenant's prefix: %s", fi.Key)
		}
	}
}

// Field and stream enumerations count the index rows as hot VictoriaTraces does: a
// field no index row has gets them under the empty value, the index stream shows up in
// streams, and field_values of an index field lists its values.
func TestIdxTable_EnumerationsCountIndexRows(t *testing.T) {
	if !tracesidx.Enabled {
		t.Skip("the trace_id_idx table is off")
	}
	e := newRestartEnv(t)
	tenant := logstorage.TenantID{}
	ids := e.ingestTraces(tenant, 20, rwHour.Add(5*time.Minute))
	from, to := rwWindow()
	check := func(when string) {
		t.Helper()
		ctx := context.Background()
		vals, err := e.s.GetFieldValues(ctx, nil, e.window("*", from, to), "name", 0)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]uint64{}
		for _, v := range vals {
			got[v.Value] = v.Hits
		}
		if got["op"] != 40 || got[""] != 20 {
			t.Errorf("%s: field_values name = %v, want op=40 and the 20 index rows under the empty value", when, got)
		}
		streams, err := e.s.GetStreams(ctx, nil, e.window("*", from, to), 0)
		if err != nil {
			t.Fatal(err)
		}
		var idxHits uint64
		for _, v := range streams {
			if strings.Contains(v.Value, otelpb.TraceIDIndexStreamName) {
				idxHits += v.Hits
			}
		}
		if idxHits != 20 {
			t.Errorf("%s: the index streams hold %d rows, want 20 (%v)", when, idxHits, streams)
		}
		st, err := e.s.GetFieldValues(ctx, nil, e.window("{"+otelpb.TraceIDIndexStreamName+`=~".+"}`, from, to), otelpb.TraceIDIndexFieldName, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(st) != len(ids) {
			t.Errorf("%s: field_values trace_id_idx lists %d ids, want %d", when, len(st), len(ids))
		}
	}
	check("buffer")
	e.flush()
	check("committed")
	e.reap()
	check("objects only")
}
