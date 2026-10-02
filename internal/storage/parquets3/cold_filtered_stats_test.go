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

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// allColumnsStore runs every query with column projection disabled (the
// all-fields hint), giving the reference answer a projected run must equal.
type allColumnsStore struct{ *Storage }

func (a allColumnsStore) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, wb logstorage.WriteDataBlockFunc) error {
	return a.Storage.RunQuery(storage.WithAllFieldsHint(ctx), tenantIDs, q, wb)
}

// coldSelectRunner returns a function that runs a LogsQL query over st the way
// the logs binary does: app/vlstorage.RunQuery -> internal/vlstorage adapter ->
// storage, with the pipes executed by VictoriaLogs' own pipe machinery.
func coldSelectRunner(t *testing.T, st storage.Storage, start, end int64) func(query string) []map[string]string {
	t.Helper()
	internalvlstorage.SetStorage(st, nil)
	t.Cleanup(func() { vlapp.SetExternalStorage(nil) })
	return func(query string) []map[string]string {
		t.Helper()
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
}

// coldFilteredStatsFixture flushes a Parquet file (mock S3) holding rows that
// differ in every dimension a filter or a group key can use, and returns the
// storage plus the query window.
func coldFilteredStatsFixture(t *testing.T) (*Storage, int64, int64) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())

	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var rows []schema.LogRow
	add := func(i int, svc, level, body, layer string) {
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
			Body:              body,
			SeverityText:      level,
			ServiceName:       svc,
			HostName:          "host-" + svc,
			Stream:            fmt.Sprintf(`{service.name=%q}`, svc),
			StreamID:          fmt.Sprintf("%048x", len(svc)),
			LogAttributes:     map[string]string{"repro_layer": layer},
		})
	}
	i := 0
	for _, svc := range []string{"alpha", "beta", "gamma"} {
		for _, level := range []string{"INFO", "ERROR"} {
			for _, layer := range []string{"cold", "warm"} {
				for k := 0; k < 3; k++ {
					add(i, svc, level, fmt.Sprintf("MARKER request %d from %s", k, svc), layer)
					i++
				}
				add(i, svc, level, "needle-exact", layer)
				i++
				add(i, svc, level, "background noise "+svc, layer)
				i++
			}
		}
	}
	// Three row groups so the scan crosses group boundaries.
	res, err := writeLogsParquet(rows, len(rows)/3+1, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/rows.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	return s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
}

func statsCount(t *testing.T, got []map[string]string, query string) int {
	t.Helper()
	if len(got) != 1 {
		return 0 // `stats count()` over zero rows emits one row with 0 or none
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

// TestColdFilteredStats_EqualsRowQuery is the issue #273 regression: on cold
// (flushed Parquet) data a filtered `| stats count()` / `| stats by (f) count()`
// must equal what the same filter returns as a row query. Before the fix the
// projection dropped `_msg` (and other referenced fields) whenever a `_time:`
// term, a stream selector or a second term sat next to a default-field filter,
// so the filter ran on a block without the column and counted 0.
func TestColdFilteredStats_EqualsRowQuery(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	run := coldSelectRunner(t, s, start, end)

	filters := []struct {
		name   string
		filter string
		min    int // rows the filter must match (non-vacuous check)
	}{
		{"word", `MARKER`, 1},
		{"phrase", `"MARKER request 1"`, 1},
		{"default field exact", `_msg:="needle-exact"`, 1},
		{"default field exact bare", `="needle-exact"`, 1},
		{"time + default field exact", `_time:30m _msg:="needle-exact"`, 1},
		{"default field exact + time", `_msg:="needle-exact" _time:30m`, 1},
		{"time + word", `_time:30m MARKER`, 1},
		{"default field exact + level", `_msg:="needle-exact" level:=ERROR`, 1},
		{"word + level", `MARKER level:=ERROR`, 1},
		{"time + word + level", `_time:30m MARKER level:=ERROR`, 1},
		{"word + service", `MARKER service.name:=alpha`, 1},
		{"stream selector + word", `{service.name="beta"} MARKER`, 1},
		{"stream selector + time + exact", `{service.name="beta"} _time:30m _msg:="needle-exact"`, 1},
		{"stream selector alone", `{service.name="gamma"}`, 1},
		{"prefix", `_msg:needle*`, 1},
		{"regex", `_msg:~"MARKER request [12]"`, 1},
		{"or", `_msg:="needle-exact" OR level:=INFO MARKER`, 1},
		{"not", `MARKER NOT level:=INFO`, 1},
		{"unregistered field", `repro_layer:=cold`, 1},
		{"unregistered field + word", `MARKER repro_layer:=warm`, 1},
		{"no match", `_time:30m _msg:="does-not-exist"`, 0},
	}
	groupFields := []string{"level", "service.name", "repro_layer", "host.name"}

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
				gq := fmt.Sprintf(`%s | stats by (%s) count() as n`, tc.filter, field)
				got, want := statsGroups(run(gq), field), groupCounts(rows, field)
				if fmtGroups(got) != fmtGroups(want) {
					t.Errorf("%s\n  got  %s\n  want %s (from the row query)", gq, fmtGroups(got), fmtGroups(want))
				}
			}

			// Other column-needing pipes over the same filter.
			uq := tc.filter + ` | uniq by (level) hits`
			if got, want := len(run(uq)), len(groupCounts(rows, "level")); got != want {
				t.Errorf("%s returned %d groups, want %d", uq, got, want)
			}
			fq := tc.filter + ` | fields level, service.name`
			if got := len(run(fq)); got != len(rows) {
				t.Errorf("%s returned %d rows, want %d", fq, got, len(rows))
			}
			sq := tc.filter + ` | filter level:=ERROR | stats count() as n`
			wantErr := 0
			for _, r := range rows {
				if r["level"] == "ERROR" {
					wantErr++
				}
			}
			if got := statsCount(t, run(sq), sq); got != wantErr {
				t.Errorf("%s = %d, want %d", sq, got, wantErr)
			}
		})
	}
}

// TestColdFilteredStats_PipesMatchAllColumns runs pipe shapes over a filter
// through both the projected scan and an all-columns scan and requires the same
// answer, for the pipes that read fields indirectly (extract, unpack_json,
// math, format, sort, ...).
func TestColdFilteredStats_PipesMatchAllColumns(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	projected := coldSelectRunner(t, s, start, end)
	// The runner installs the store globally; the reference run swaps in the
	// all-columns store for one query and puts the projected one back.
	reference := func(query string) []map[string]string {
		internalvlstorage.SetStorage(allColumnsStore{s}, nil)
		defer internalvlstorage.SetStorage(s, nil)
		q := mustParseQueryWithTime(t, query, start, end)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		var out []map[string]string
		if err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			out = append(out, blockRowFields([]*logstorage.DataBlock{db})...)
			mu.Unlock()
		}); err != nil {
			t.Fatalf("reference RunQuery(%s): %v", query, err)
		}
		return out
	}

	queries := []string{
		`_time:30m MARKER | extract "request <k> from <who>" | stats by (who) count() as n`,
		`_time:30m MARKER | extract "request <k> from <who>" from _msg | stats by (k) count() as n`,
		`MARKER | format "<service.name>/<level>" as combo | stats by (combo) count() as n`,
		`_msg:="needle-exact" | math severity_number + 1 as l | stats sum(l) as n`,
		`MARKER | sort by (service.name, _msg) | limit 5`,
		`{service.name="alpha"} _time:30m | stats by (level, repro_layer) count() as n`,
		`MARKER | stats by (service.name) count() as n | sort by (service.name)`,
		`_time:30m _msg:="needle-exact" | stats count_uniq(host.name) as n`,
		`MARKER | top 2 (service.name)`,
		`MARKER | rename service.name as svc | stats by (svc) count() as n`,
		`_time:30m MARKER | unpack_logfmt from _msg | stats count() as n`,
		`MARKER | stats by (_time:1m) count() as n`,
		`MARKER | filter repro_layer:=cold | stats by (level) count() as n`,
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
