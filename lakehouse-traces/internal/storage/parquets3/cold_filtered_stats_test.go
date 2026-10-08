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
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// allColumnsStore runs every query with column projection disabled (the
// all-fields hint), giving the reference answer a projected run must equal.
type allColumnsStore struct{ *Storage }

func (a allColumnsStore) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, wb logstorage.WriteDataBlockFunc) error {
	return a.Storage.RunQuery(storage.WithAllFieldsHint(ctx), tenantIDs, q, wb)
}

func runSelect(t *testing.T, start, end int64, query string) []map[string]string {
	t.Helper()
	q := mustParseQueryWithTime(t, query, start, end)
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
	var mu sync.Mutex
	var out []map[string]string
	if err := vtstorage.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		rows := blockRowFields([]*logstorage.DataBlock{db})
		mu.Lock()
		out = append(out, rows...)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("RunQuery(%s): %v", query, err)
	}
	return out
}

// coldSelectRunner returns a function that runs a LogsQL query over st the way
// the traces binary does: VictoriaTraces' vtstorage.RunQuery -> the vtstorage
// adapter -> storage, with the pipes executed by VictoriaLogs' pipe machinery.
func coldSelectRunner(t *testing.T, st storage.Storage, start, end int64) func(query string) []map[string]string {
	t.Helper()
	vtstorageadapter.Init(st)
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })
	return func(query string) []map[string]string { return runSelect(t, start, end, query) }
}

// coldFilteredStatsFixture flushes a Parquet file (mock S3) holding spans that
// differ in every dimension a filter or a group key can use, and returns the
// storage plus the query window.
func coldFilteredStatsFixture(t *testing.T) (*Storage, int64, int64) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces

	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var rows []schema.TraceRow
	i := 0
	for _, svc := range []string{"alpha", "beta", "gamma"} {
		for _, method := range []string{"GET", "POST"} {
			for _, route := range []string{"/cold", "/warm"} {
				for k := 0; k < 4; k++ {
					name := fmt.Sprintf("%s %s", method, route)
					if k == 3 {
						name = "needle-exact"
					}
					rows = append(rows, schema.TraceRow{
						TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
						StartTimeUnixNano: schema.Int64Ptr(base.Add(time.Duration(i) * time.Second).UnixNano()),
						TraceID:           fmt.Sprintf("trace-%s-%d", svc, i%5),
						SpanID:            fmt.Sprintf("%016x", i),
						SpanName:          name,
						ServiceName:       svc,
						HostName:          "host-" + svc,
						DurationNs:        schema.Int64Ptr(int64(1000000 * (1 + i%4))),
						StatusCode:        schema.Int32Ptr(int32(i % 3)),
						HTTPMethod:        method,
						Stream:            fmt.Sprintf(`{resource_attr:service.name=%q}`, svc),
						StreamID:          fmt.Sprintf("%048x", len(svc)),
						SpanAttributes:    map[string]string{"http.route": route, "trace_state": "state-" + strings.TrimPrefix(route, "/")},
					})
					i++
				}
			}
		}
	}
	// Several row groups so the scan crosses group boundaries.
	res, err := writeTracesParquet(rows, len(rows)/3+1, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/rows.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	return s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
}

func statsCount(t *testing.T, got []map[string]string, query string) int {
	t.Helper()
	if len(got) != 1 {
		return 0
	}
	n, err := strconv.Atoi(got[0]["n"])
	if err != nil {
		t.Fatalf("%s: count %q is not a number: %v", query, got[0]["n"], err)
	}
	return n
}

// groupCounts folds rows into value->count for field (missing -> "").
func groupCounts(rows []map[string]string, field string) map[string]int {
	out := map[string]int{}
	for _, r := range rows {
		out[r[field]]++
	}
	return out
}

func statsGroups(rows []map[string]string, field string) map[string]int {
	out := map[string]int{}
	for _, r := range rows {
		n, _ := strconv.Atoi(r["n"])
		out[r[field]] += n
	}
	return out
}

func fmtGroups(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%q=%d", k, m[k]))
	}
	return strings.Join(parts, " ")
}

// TestColdFilteredStats_EqualsRowQuery is the issue #273 regression for spans:
// on cold (flushed Parquet) data a filtered `| stats count()` /
// `| stats by (f) count()` must equal what the same filter returns as a row
// query. Before the fix the projection was assembled from the query text and
// dropped a column the filter read whenever a `_time:` term or a stream
// selector sat beside it.
func TestColdFilteredStats_EqualsRowQuery(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	run := coldSelectRunner(t, s, start, end)

	filters := []struct {
		name   string
		filter string
		min    int
	}{
		{"span name exact", `name:="needle-exact"`, 1},
		{"time + span name", `_time:30m name:="needle-exact"`, 1},
		{"span name + time", `name:="needle-exact" _time:30m`, 1},
		{"time + span name + method", `_time:30m name:="needle-exact" "span_attr:http.method":=GET`, 1},
		{"service attr", `"resource_attr:service.name":=alpha`, 1},
		{"service by parquet spelling", `service.name:=alpha`, 1},
		{"stream selector alone", `{resource_attr:service.name="beta"}`, 1},
		{"stream selector + name", `{resource_attr:service.name="beta"} name:="needle-exact"`, 1},
		{"stream selector + time + name", `{resource_attr:service.name="beta"} _time:30m name:="needle-exact"`, 1},
		{"explicit stream + time", `_stream:{resource_attr:service.name="gamma"} _time:30m`, 1},
		{"span attr promoted", `"span_attr:http.method":=POST`, 1},
		{"map span attr", `"span_attr:http.route":="/cold"`, 1},
		// trace_state is stored in span.attributes but VT serves it as a bare
		// top-level field, so the projection cannot name its column.
		{"bare top-level attr", `trace_state:="state-cold"`, 1},
		{"bare top-level attr + name", `trace_state:="state-warm" name:="needle-exact"`, 1},
		{"duration range", `duration:>2000000`, 1},
		{"status code", `status_code:=2 _time:30m`, 1},
		{"trace id", `trace_id:="trace-alpha-1"`, 1},
		{"or", `name:="needle-exact" OR "span_attr:http.method":=GET "span_attr:http.route":="/cold"`, 1},
		{"not", `NOT "span_attr:http.method":=GET "span_attr:http.route":="/cold"`, 1},
		{"regex", `name:~"GET /(cold|warm)"`, 1},
		{"prefix", `name:needle*`, 1},
		{"no match", `_time:30m name:="does-not-exist"`, 0},
	}
	groupFields := []string{"name", "resource_attr:service.name", "span_attr:http.method", "span_attr:http.route", "trace_state", "status_code"}

	for _, tc := range filters {
		t.Run(tc.name, func(t *testing.T) {
			rows := run(tc.filter)
			if len(rows) < tc.min || (tc.min == 0 && len(rows) != 0) {
				t.Fatalf("row query %q returned %d rows (fixture sanity, want >= %d)", tc.filter, len(rows), tc.min)
			}

			cq := tc.filter + ` | stats count() as n`
			if got := statsCount(t, run(cq), cq); got != len(rows) {
				t.Errorf("%s = %d, the row query returned %d", cq, got, len(rows))
			}

			for _, field := range groupFields {
				gq := fmt.Sprintf(`%s | stats by (%q) count() as n`, tc.filter, field)
				got, want := statsGroups(run(gq), field), groupCounts(rows, field)
				if fmtGroups(got) != fmtGroups(want) {
					t.Errorf("%s\n  got  %s\n  want %s (from the row query)", gq, fmtGroups(got), fmtGroups(want))
				}
			}

			uq := tc.filter + ` | uniq by (name) hits`
			if got, want := len(run(uq)), len(groupCounts(rows, "name")); got != want {
				t.Errorf("%s returned %d groups, want %d", uq, got, want)
			}
			fq := tc.filter + ` | fields trace_id, name`
			if got := len(run(fq)); got != len(rows) {
				t.Errorf("%s returned %d rows, want %d", fq, got, len(rows))
			}
			sq := tc.filter + ` | filter status_code:=2 | stats count() as n`
			wantFiltered := 0
			for _, r := range rows {
				if r["status_code"] == "2" {
					wantFiltered++
				}
			}
			if got := statsCount(t, run(sq), sq); got != wantFiltered {
				t.Errorf("%s = %d, want %d", sq, got, wantFiltered)
			}
		})
	}
}

// TestColdFilteredStats_PipesMatchAllColumns runs pipe shapes over a filter
// through both the projected scan and an all-columns scan and requires the same
// answer, including the Tempo-metrics and service-graph query shapes.
func TestColdFilteredStats_PipesMatchAllColumns(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	projected := coldSelectRunner(t, s, start, end)
	reference := func(query string) []map[string]string {
		vtstorageadapter.Init(allColumnsStore{s})
		defer vtstorageadapter.Init(s)
		return runSelect(t, start, end, query)
	}

	queries := []string{
		`_time:30m name:="needle-exact" | stats by (_time:1m) quantile(0.9, duration) as n`,
		`{resource_attr:service.name="alpha"} _time:30m | stats by (name) count() as n`,
		`name:="needle-exact" | stats by ("resource_attr:service.name") sum(duration) as n`,
		`_time:30m | stats by (_time:1m) histogram(duration) as n`,
		`trace_state:="state-cold" | format "<name>-<trace_state>" as c | stats by (c) count() as n`,
		`name:="needle-exact" | math duration / 1000 as us | stats sum(us) as n`,
		`name:~"GET .*" | extract "<m> <r>" from name | stats by (m, r) count() as n`,
		`name:="needle-exact" | sort by (_time) | limit 5`,
		`name:="needle-exact" | stats by (trace_id) count() as n | sort by (trace_id)`,
		`_time:30m "span_attr:http.method":=GET | stats count_uniq(trace_id) as n`,
		`name:="needle-exact" | rename name as nm | stats by (nm) count() as n`,
		`name:="needle-exact" | filter trace_state:=state-warm | stats by (status_code) count() as n`,
		`{resource_attr:service.name="beta"} | top 2 (name)`,
	}
	canon := func(rows []map[string]string) string {
		lines := make([]string, 0, len(rows))
		for _, r := range rows {
			keys := make([]string, 0, len(r))
			for k := range r {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var sb strings.Builder
			for _, k := range keys {
				fmt.Fprintf(&sb, "%s=%s;", k, r[k])
			}
			lines = append(lines, sb.String())
		}
		sort.Strings(lines)
		return strings.Join(lines, "\n")
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			got, want := projected(q), reference(q)
			if len(want) == 0 {
				t.Fatalf("reference run of %q returned no rows (fixture sanity)", q)
			}
			if canon(got) != canon(want) {
				t.Errorf("projected answer differs from the all-columns answer\n  projected:\n%s\n  all columns:\n%s", canon(got), canon(want))
			}
		})
	}
}
