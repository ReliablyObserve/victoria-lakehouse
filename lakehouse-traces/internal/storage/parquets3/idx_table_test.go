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

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
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
		rowsA := e.rows(a, stage1(id), from, to)
		if len(rowsA) != 1 {
			t.Errorf("tenant A does not see its own trace %s", id)
		} else if sid := rowsA[0]["_stream_id"]; !strings.HasPrefix(sid, "0000000100000000") {
			t.Errorf("the _stream_id %q of tenant 1:0's index row does not carry the tenant", sid)
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

// idxRouteOfQuery decides which tables a query reads. A wrong "spans only" would hide
// index rows, so it is given only for filters that cannot match one; anything with a
// NOT, an OR or an `if (...)` reads both.
func TestIdxTable_RouteOfQuery(t *testing.T) {
	if !tracesidx.Enabled {
		t.Skip("the trace_id_idx table is off")
	}
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	for _, tc := range []struct {
		q       string
		unknown bool
		want    idxRoute
	}{
		{`{trace_id_idx_stream="12"} AND trace_id_idx:="x"`, false, routeIdxOnly},
		{`_time:5m {trace_id_idx_stream!=""} AND duration:>5`, false, routeIdxOnly},
		// what the select handler really hands the storage: its latency-offset bound, then the caller's range
		{`_time:[1677-09-21T00:12:43.145224192Z,2026-10-13T20:00:00.016284169Z] _time:[2026-10-13T16:00:00.000000000Z,2026-10-13T17:59:59.999999999Z] {trace_id_idx_stream="897"} trace_id_idx:=5f7b21a43158a0b98328f82988d59fb7`, false, routeIdxOnly},
		{`_time:[1677-09-21T00:12:43.145224192Z,2026-10-13T20:00:00.016284169Z] _time:[2026-10-13T16:00:00.000000000Z,2026-10-13T17:59:59.999999999Z] {resource_attr:service.name="a"} trace_id:=x`, false, routeSpansOnly},
		{`{trace_id_idx_stream!=""} | stats count()`, false, routeIdxOnly},
		{`{trace_id_idx_stream!=""} OR {resource_attr:service.name="a"}`, true, routeBoth},
		{`{resource_attr:service.name="a"}`, false, routeSpansOnly},
		{`trace_id:="abc"`, false, routeSpansOnly},
		{`trace_id:="abc" | fields span_id`, false, routeSpansOnly},
		{`*`, false, routeBoth},
		{`error`, false, routeBoth},
		{`NOT trace_id:="abc"`, true, routeBoth},
		{`trace_id:="abc" OR trace_id_idx:="abc"`, true, routeBoth},
		{`_time:1h`, false, routeBoth},
		{`duration:>5`, false, routeBoth},
		// "the field is not empty" on a span-only field cannot match an index row
		{"span_id:* | stats count() n", false, routeSpansOnly},
		{"span_id:* `resource_attr:service.name`:~\"a.*\" | stats by (name) count()", false, routeSpansOnly},
		{"span_id:* duration:>50000000 | stats count() n", false, routeSpansOnly},
		{"span_id:* | fields trace_id, span_id, name | limit 1000", false, routeSpansOnly},
		// ... but not on a field an index row has, nor under a NOT or an OR
		{`trace_id_idx:* | stats count()`, false, routeBoth},
		{`duration:*`, false, routeBoth},
		{`_msg:*`, false, routeBoth},
		{`NOT span_id:*`, true, routeBoth},
		{`span_id:* OR trace_id_idx:*`, true, routeBoth},
	} {
		if got := s.idxRouteOfQuery(tc.q, tc.unknown); got != tc.want {
			t.Errorf("%q (unknown=%v): route %d, want %d", tc.q, tc.unknown, got, tc.want)
		}
	}
}

// The reads of a trace lookup do not grow with the number of idx objects once their
// footers and filters are cached: the first lookup after a restart reads each object once,
// a lookup of another id reads only what the filters let through (about 1 % per object)
// plus the object that holds the id.
func TestIdxTable_LookupReadsStayFlatAcrossObjects(t *testing.T) {
	if !tracesidx.Enabled {
		t.Skip("the trace_id_idx table is off")
	}
	e := newRestartEnv(t)
	tenant := logstorage.TenantID{}
	var ids []string
	for g := 0; g < 12; g++ { // twelve flush groups of one hour: twelve idx objects
		ids = append(ids, e.ingestTraces(tenant, 6, rwHour.Add(time.Duration(g)*time.Minute))...)
		e.flush()
	}
	e.reap()
	from, to := rwWindow()
	gets := func() int { return int(metrics.S3RequestsTotal.Get("GetObject")) }
	// first lookup: every object once
	before := gets()
	if len(e.rows(tenant, stage1(ids[3]), from, to)) != 1 {
		t.Fatal("stage 1 missed")
	}
	first := gets() - before
	// later lookups, memory and disk tiers emptied (as the harness's cold step does)
	var later int
	for i := 10; i < 60; i += 5 {
		id := ids[i]
		// the data tiers only: the locator keeps the parsed footers and filters, which is what
		// keeps these lookups flat (ClearCaches would drop it too, as a restarted process has)
		e.s.memCache.Clear()
		if e.s.diskCache != nil {
			_ = e.s.diskCache.Clear()
		}
		before = gets()
		if len(e.rows(tenant, stage1(id), from, to)) != 1 {
			t.Fatal("stage 1 missed")
		}
		later += gets() - before
	}
	t.Logf("first lookup: %d GETs for 12 objects; %d lookups later: %d GETs", first, 10, later)
	if first > 12+2 {
		t.Errorf("the first lookup made %d GETs for 12 objects", first)
	}
	if per := float64(later) / 10; per > 3.5 {
		t.Errorf("a later lookup made %.1f GETs on average: objects that cannot hold the id were read again", per)
	}
}

// The stage-2 span query of a trace lookup is bounded by the trace's exact [start, end]
// that the stage-1 lookup of the idx table returned (as on hot VictoriaTraces), and the
// storage plans the span objects of that window only: widening a trace_id query to
// "now" would put every span object after the trace's start into the plan.
func TestIdxTable_TraceIDQueryKeepsItsTimeBound(t *testing.T) {
	if !tracesidx.Enabled {
		t.Skip("the trace_id_idx table is off")
	}
	start := time.Now().Add(-6 * time.Hour).Truncate(time.Second)
	end := start.Add(90 * time.Millisecond)
	q, err := logstorage.ParseQueryAtTimestamp(`trace_id:="0123456789abcdef0123456789abcdef"`, end.UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(end.UnixNano(), start.UnixNano(), end.UnixNano())
	gotQ, gotEnd := widenTraceIDQueryToNow(q, start.UnixNano(), end.UnixNano())
	if gotEnd != end.UnixNano() {
		t.Errorf("upper bound widened from %d to %d", end.UnixNano(), gotEnd)
	}
	if s, e := gotQ.GetFilterTimeRange(); s != start.UnixNano() || e != end.UnixNano() {
		t.Errorf("the query's own time range became [%d, %d], want [%d, %d]", s, e, start.UnixNano(), end.UnixNano())
	}
}

// pmeta holds a summary of every idx object (fences and a copy of its bloom filter), so a
// lookup of an id reads the objects that can hold it and no others, with every in-process
// cache emptied (the first lookup after a restart is no different):
//   - from the summaries the flush recorded;
//   - after the bundles were lost: the lookups that touch an object recreate its summary
//     from the footer they read, one pass over the objects, then the lookups are flat again;
//   - after a restart: the summaries come back with their bundles, one GET per partition.
func TestIdxTable_PmetaSummaryAnswersMembership(t *testing.T) {
	if !tracesidx.Enabled {
		t.Skip("the trace_id_idx table is off")
	}
	e := newRestartEnv(t)
	cfg := config.PmetaConfig{Enabled: true}
	install := func(cs *pmeta.Store) {
		e.s.catalog = cs
		e.s.cfg.Pmeta = cfg
		e.bw.catalogObserver = &catalogObserver{store: cs, pool: e.s.pool}
	}
	install(newCatalogStore(cfg, "traces/"))
	tenant := logstorage.TenantID{}
	var ids []string
	const objects = 16
	for g := 0; g < objects; g++ {
		ids = append(ids, e.ingestTraces(tenant, 6, rwHour.Add(time.Duration(g)*time.Minute))...)
		e.flush()
	}
	e.reap()
	from, to := rwWindow()
	gets := func() int { return int(metrics.S3RequestsTotal.Get("GetObject")) }
	skips := func() int { return int(metrics.TraceIdxSummarySkips.Get()) }
	lookups := func(n int) (perLookup float64) {
		t.Helper()
		total := 0
		for i := 0; i < n; i++ {
			id := ids[(i*7+3)%len(ids)]
			e.s.ClearCaches() // memory and disk tiers, footer cache and the idx locator
			before := gets()
			if len(e.rows(tenant, stage1(id), from, to)) != 1 {
				t.Fatalf("stage 1 missed %s", id)
			}
			total += gets() - before
		}
		return float64(total) / float64(n)
	}

	if per := lookups(20); per > 2.5 {
		t.Errorf("with the flush's summaries a lookup made %.1f GETs on average, want about one (the object that holds the id)", per)
	}
	if skips() == 0 {
		t.Error("no object was skipped through its summary")
	}

	// the bundles are lost: a pass of lookups recreates the summaries
	install(newCatalogStore(cfg, "traces/"))
	learned := int(metrics.TraceIdxSummaryLearned.Get())
	before := gets()
	e.s.ClearCaches()
	if len(e.rows(tenant, stage1(ids[5]), from, to)) != 1 {
		t.Fatal("stage 1 missed")
	}
	if first := gets() - before; first < objects {
		t.Errorf("without summaries the first lookup made %d GETs for %d objects: it must read them (and learn them)", first, objects)
	}
	if got := int(metrics.TraceIdxSummaryLearned.Get()) - learned; got < objects {
		t.Errorf("%d summaries learned, want %d", got, objects)
	}
	if per := lookups(20); per > 2.5 {
		t.Errorf("after learning a lookup made %.1f GETs on average", per)
	}

	// restart: the persisted bundles are loaded, nothing is learned
	if n, err := e.s.catalog.PersistDirty(context.Background(), poolObjectStore{e.s.pool}); err != nil || n == 0 {
		t.Fatalf("persist: %d bundles, %v", n, err)
	}
	install(newCatalogStore(cfg, "traces/"))
	e.s.WarmCatalogFromS3(context.Background())
	learned = int(metrics.TraceIdxSummaryLearned.Get())
	if per := lookups(20); per > 2.5 {
		t.Errorf("after a restart a lookup made %.1f GETs on average", per)
	}
	if got := int(metrics.TraceIdxSummaryLearned.Get()) - learned; got != 0 {
		t.Errorf("%d summaries were learned after the bundles were loaded, want 0", got)
	}
}
