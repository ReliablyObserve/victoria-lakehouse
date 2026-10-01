package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// partitionOfKey maps traces/dt=D/hour=H/file.parquet to dt=D/hour=H.
func partitionOfKey(key string) string {
	key = strings.TrimPrefix(key, "traces/")
	return key[:strings.LastIndex(key, "/")]
}

func sumN(rows []map[string]string) int {
	n := 0
	for _, r := range rows {
		v, _ := strconv.Atoi(r["n"])
		n += v
	}
	return n
}

func addHideTombstone(s *Storage, query string, startNs, endNs int64) {
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "ts-273", Query: query, StartNs: startNs, EndNs: endNs, Mode: "hide"})
	s.SetTombstoneStore(store)
}

// TestColdTombstones_StatsEqualRows (issue #285 and the projection regression it
// shares a cause with, traces): a hide-mode delete must hide the deleted spans
// from a `stats` answer exactly as from the row query, whatever the projection.
func TestColdTombstones_StatsEqualRows(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	run := coldSelectRunner(t, s, start, end)

	all := len(run("*"))
	addHideTombstone(s, `name:="needle-exact"`, start, end)
	visible := run("*")
	if len(visible) == 0 || len(visible) >= all {
		t.Fatalf("fixture: the tombstone must hide some but not all spans (all=%d visible=%d)", all, len(visible))
	}

	for _, f := range []string{
		`*`,
		`NOT foo_name:=x`,
		`"resource_attr:service.name":=alpha`,
		`_time:30m "span_attr:http.method":=GET`,
	} {
		t.Run(f, func(t *testing.T) {
			rows := run(f)
			if len(rows) == 0 {
				t.Fatalf("fixture: %q matched nothing", f)
			}
			for _, r := range rows {
				if r["name"] == "needle-exact" {
					t.Fatalf("row query shows a tombstoned span: %v", r)
				}
			}
			if got := statsCount(t, run(f+` | stats count() n`), f); got != len(rows) {
				t.Errorf("%s | stats count() = %d, the row query returned %d", f, got, len(rows))
			}
			if got := sumN(run(f + ` | stats by (_time:1m) count() n`)); got != len(rows) {
				t.Errorf("%s | stats by (_time:1m) count() sums to %d, want %d", f, got, len(rows))
			}
			got, want := statsGroups(run(f+` | stats by (name) count() n`), "name"), groupCounts(rows, "name")
			if fmtGroups(got) != fmtGroups(want) {
				t.Errorf("%s | stats by (name)\n  got  %s\n  want %s", f, fmtGroups(got), fmtGroups(want))
			}
		})
	}
}

// TestColdTombstones_UnregisteredTombstoneField: a tombstone on a bare VT
// top-level attribute (held in the span attribute MAP) still applies.
func TestColdTombstones_UnregisteredTombstoneField(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	run := coldSelectRunner(t, s, start, end)
	addHideTombstone(s, `trace_state:="state-warm"`, start, end)
	rows := run("*")
	if len(rows) == 0 {
		t.Fatal("fixture: everything hidden")
	}
	if got := statsCount(t, run(`* | stats count() n`), "count"); got != len(rows) {
		t.Errorf("* | stats count() = %d, want %d (tombstone on a MAP field)", got, len(rows))
	}
}

// TestColdCountPushdown_RewrittenGroupKey (M1, traces): the manifest count
// pushdown must fall through when a pipe rewrites the group key first.
func TestColdCountPushdown_RewrittenGroupKey(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	run := coldSelectRunner(t, s, start, end)
	rows := run("*")

	files := s.manifest.GetFilesForRange(start, end)
	if len(files) != 1 {
		t.Fatalf("fixture: want one file, got %d", len(files))
	}
	fi := files[0]
	fi.RowCount = int64(len(rows))
	agg := map[string]int64{}
	var minNs, maxNs int64
	for _, r := range rows {
		agg[r["name"]]++
		ts, _ := time.Parse(time.RFC3339Nano, r["_time"])
		if minNs == 0 || ts.UnixNano() < minNs {
			minNs = ts.UnixNano()
		}
		if ts.UnixNano() > maxNs {
			maxNs = ts.UnixNano()
		}
	}
	fi.MinTimeNs, fi.MaxTimeNs = minNs, maxNs
	fi.LabelAggregates = map[string]map[string]int64{"name": agg}
	s.manifest = manifest.New("test-bucket", "logs/")
	s.manifest.AddFile(partitionOfKey(fi.Key), fi)

	served := func(q string) (map[string]int, bool) {
		before := getCounterValue(t, metrics.MetadataOnlyFiles)
		out := statsGroups(run(q), "name")
		return out, getCounterValue(t, metrics.MetadataOnlyFiles) != before
	}

	got, used := served(`* | stats by (name) count() n`)
	if !used || fmtGroups(got) != fmtGroups(groupCounts(rows, "name")) {
		t.Fatalf("control: pushdown used=%v answer %s", used, fmtGroups(got))
	}
	methods := groupCounts(rows, "span_attr:http.method")
	got, used = served(`* | copy "span_attr:http.method" as name | stats by (name) count() n`)
	if used {
		t.Error("copy-rewritten group key was answered from the stored aggregates")
	}
	if fmtGroups(got) != fmtGroups(methods) {
		t.Errorf("copy as name\n  got  %s\n  want %s", fmtGroups(got), fmtGroups(methods))
	}
	got, used = served(`* | extract "<name> <r>" from trace_id | stats by (name) count() n`)
	if used {
		t.Error("extract-rewritten group key was answered from the stored aggregates")
	}
	got, used = served(`* | format "x" as name | stats by (name) count() n`)
	if used || len(got) != 1 || got["x"] != len(rows) {
		t.Errorf("format-rewritten group key: pushdown used=%v answer %s, want x=%d from a scan", used, fmtGroups(got), len(rows))
	}
}

// TestColdTier2Slot_FilterAndGroupBy (traces): a span attribute stored in a
// Tier-2 slot is found by a filter and a group key under a narrow projection.
func TestColdTier2Slot_FilterAndGroupBy(t *testing.T) {
	prev := activeSlotResolver
	t.Cleanup(func() { SetSlotResolver(prev) })
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant.tier"}}))

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var rows []schema.TraceRow
	for i := 0; i < 30; i++ {
		r := schema.TraceRow{TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(), StartTimeUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(),
			TraceID: fmt.Sprintf("t%d", i), SpanID: fmt.Sprintf("%016x", i), SpanName: "slot-span", ServiceName: "api",
			Stream: `{resource_attr:service.name="api"}`, StreamID: fmt.Sprintf("%048x", 1)}
		schema.SetTraceSlot(&r, "ded_s01", []string{"gold", "silver", "bronze"}[i%3])
		rows = append(rows, r)
	}
	res, err := writeTracesParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/slot.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	run := coldSelectRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())

	if got := statsCount(t, run(`_time:30m "tenant.tier":=gold | stats count() n`), "gold"); got != 10 {
		t.Errorf("tenant.tier:=gold | stats count() = %d, want 10", got)
	}
	got := statsGroups(run(`name:="slot-span" | stats by (tenant.tier) count() n`), "tenant.tier")
	if fmtGroups(got) != fmtGroups(map[string]int{"gold": 10, "silver": 10, "bronze": 10}) {
		t.Errorf("stats by (tenant.tier) = %s", fmtGroups(got))
	}
}

// TestQuerySpecificFiles_ProjectsFromQuery (traces).
func TestQuerySpecificFiles_ProjectsFromQuery(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	files := s.manifest.GetFilesForRange(start, end)
	if len(files) != 1 {
		t.Fatalf("fixture: want one file, got %d", len(files))
	}
	var mu sync.Mutex
	cols := map[string]bool{}
	err := s.QuerySpecificFiles(context.Background(), []string{files[0].Key}, start, end, `name:="needle-exact" | stats count() n`, nil,
		func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			for _, c := range db.GetColumns(false) {
				cols[c.Name] = true
			}
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) == 0 || cols["trace_id"] || cols["resource_attr:service.name"] {
		t.Errorf("block columns = %v, want only the projected ones (_time and name)", cols)
	}
}

func TestWithTombstoneFields(t *testing.T) {
	ts := func(q string) tombstone {
		return tombstone{ID: "x", Query: q, Mode: "hide"}
	}
	got := withTombstoneFields([]string{"_time", "level"}, []tombstone{ts(`foo:=1`), ts(`level:=x AND bar:=2`)})
	want := map[string]bool{"_time": true, "level": true, "foo": true, "bar": true}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, f := range got {
		if !want[f] {
			t.Errorf("unexpected %q in %v", f, got)
		}
	}
	if g := withTombstoneFields([]string{"*"}, []tombstone{ts(`foo:=1`)}); len(g) != 1 || g[0] != "*" {
		t.Errorf("a wildcard list must stay as is, got %v", g)
	}
	if g := withTombstoneFields([]string{"_time"}, nil); len(g) != 1 {
		t.Errorf("no tombstones must leave the list alone, got %v", g)
	}
}

func TestCountPushdownSound(t *testing.T) {
	cases := []struct {
		query string
		field string
		want  bool
	}{
		{`* | stats by (name) count()`, "name", true},
		{`* | uniq by (name)`, "name", true},
		{`* | top 3 (name)`, "name", true},
		{`* | copy trace_id as name | stats by (name) count()`, "name", false},
		{`* | extract "x <name> " from trace_id | stats by (name) count()`, "name", false},
		{`* | format "x" as name | stats by (name) count()`, "name", false},
		{`* | limit 5 | stats by (name) count()`, "name", false},
		{`* | filter status_code:=2 | stats by (name) count()`, "name", true}, // a filter pipe is folded into the filter; countPushdownFilterFields vets it,
		{`name:a | stats by (name) count()`, "name", true},
		{`*`, "name", false},
	}
	for _, c := range cases {
		q, err := logstorage.ParseQuery(c.query)
		if err != nil {
			t.Fatal(err)
		}
		if got := countPushdownSound(q, c.field); got != c.want {
			t.Errorf("countPushdownSound(%q) = %v, want %v", c.query, got, c.want)
		}
	}
}

func TestRowFilterContext(t *testing.T) {
	if rowFilterFrom(context.Background()) {
		t.Error("bare ctx must not claim a row filter")
	}
	if !rowFilterFrom(withRowFilter(context.Background(), true)) {
		t.Error("withRowFilter(true) lost")
	}
}

// TestRunQueryProjectionEquivalence_Random (traces) drives random filter + pipe
// chains through the whole RunQuery path and requires the projected answer to
// equal the answer with every column read, with and without a hide tombstone.
func TestRunQueryProjectionEquivalence_Random(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })
	run := func(st storage.Storage, q string) string {
		vtstorageadapter.Init(st)
		query := mustParseQueryWithTime(t, q, start, end)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, query, false, nil)
		var mu sync.Mutex
		var out []map[string]string
		if err := vtstorage.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			r := blockRowFields([]*logstorage.DataBlock{db})
			mu.Lock()
			out = append(out, r...)
			mu.Unlock()
		}); err != nil {
			return "error: " + err.Error()
		}
		return canonRows(out)
	}

	rng := rand.New(rand.NewSource(285))
	g := projGen{pick: rng.Intn}
	for _, tombstone := range []bool{false, true} {
		if tombstone {
			addHideTombstone(s, `name:="needle-exact"`, start, end)
		}
		checked := 0
		for i := 0; i < 100; i++ {
			q := g.query()
			parsed, err := logstorage.ParseQuery(q)
			if err != nil || logstorage.QueryHasFilterSubqueries(parsed) {
				continue
			}
			got, want := run(s, q), run(allColumnsStore{s}, q)
			if got != want {
				t.Fatalf("tombstone=%v: projected answer differs for %q\n  projected:\n%s\n  all columns:\n%s", tombstone, q, got, want)
			}
			checked++
		}
		if checked < 50 {
			t.Fatalf("tombstone=%v: only %d runnable queries; the generator no longer exercises the property", tombstone, checked)
		}
	}
}
