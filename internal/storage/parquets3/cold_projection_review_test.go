package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

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

// TestColdTombstones_StatsEqualRows (issue #285 and the projection regression
// it shares a cause with): a hide-mode delete must hide the deleted rows from a
// `stats` answer exactly as it hides them from the row query. A tombstone is
// evaluated on the projected block, so the projection has to carry the fields
// the tombstone's query references, including fields no registry column holds.
func TestColdTombstones_StatsEqualRows(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	run := coldSelectRunner(t, s, start, end)

	all := len(run("*"))
	addHideTombstone(s, `level:=ERROR`, start, end)
	visible := run("*")
	if len(visible) == 0 || len(visible) >= all {
		t.Fatalf("fixture: the tombstone must hide some but not all rows (all=%d visible=%d)", all, len(visible))
	}

	for _, f := range []string{
		`*`,
		`NOT detected_level:=debug`,
		`service.name:=alpha NOT loglevel:=x`,
		`_time:30m MARKER`,
	} {
		t.Run(f, func(t *testing.T) {
			rows := run(f)
			if len(rows) == 0 {
				t.Fatalf("fixture: %q matched nothing", f)
			}
			for _, r := range rows {
				if r["level"] == "ERROR" {
					t.Fatalf("row query shows a tombstoned row: %v", r)
				}
			}
			if got := statsCount(t, run(f+` | stats count() n`), f); got != len(rows) {
				t.Errorf("%s | stats count() = %d, the row query returned %d", f, got, len(rows))
			}
			if got := sumN(run(f + ` | stats by (_time:1m) count() n`)); got != len(rows) {
				t.Errorf("%s | stats by (_time:1m) count() sums to %d, want %d", f, got, len(rows))
			}
			got, want := statsGroups(run(f+` | stats by (service.name) count() n`), "service.name"), groupCounts(rows, "service.name")
			if fmtGroups(got) != fmtGroups(want) {
				t.Errorf("%s | stats by (service.name)\n  got  %s\n  want %s", f, fmtGroups(got), fmtGroups(want))
			}
		})
	}
}

// TestColdTombstones_UnregisteredTombstoneField: a tombstone on a field held
// only in the attribute MAP column still applies under a narrow projection.
func TestColdTombstones_UnregisteredTombstoneField(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	run := coldSelectRunner(t, s, start, end)
	addHideTombstone(s, `repro_layer:=warm`, start, end)
	rows := run("*")
	if len(rows) == 0 {
		t.Fatal("fixture: everything hidden")
	}
	if got := statsCount(t, run(`* | stats count() n`), "count"); got != len(rows) {
		t.Errorf("* | stats count() = %d, want %d (tombstone on a MAP field)", got, len(rows))
	}
}

// TestColdSyntheticPath_NeedsNoRowFilter (M2): a `_time` filter that is not a
// plain range (day_range) is evaluated per row, so the metadata-only synthetic
// timestamp series must not stand in for the rows.
func TestColdSyntheticPath_NeedsNoRowFilter(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	// Unevenly spaced rows: 55 in the first seconds of minute M, 5 half a minute
	// into M+1. An evenly spaced fabricated series would put about 37 in M.
	minute := time.Now().UTC().Add(-20 * time.Minute).Truncate(time.Minute)
	var rows []schema.LogRow
	for i := 0; i < 55; i++ {
		rows = append(rows, schema.LogRow{TimestampUnixNano: minute.Add(time.Duration(i) * 50 * time.Millisecond).UnixNano(), Body: "early", ServiceName: "api"})
	}
	for i := 0; i < 5; i++ {
		rows = append(rows, schema.LogRow{TimestampUnixNano: minute.Add(time.Minute + 30*time.Second + time.Duration(i)*time.Second).UnixNano(), Body: "late", ServiceName: "api"})
	}
	res, err := writeLogsParquet(rows, 1000, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/uneven.parquet", minute.Format("2006-01-02"), minute.Hour()), res.Data, minute.Add(time.Minute))
	// registerFileInMockS3 brackets the file by +-1m around its time argument;
	// widen it to the real span so the file counts as fully inside the query.
	files := s.manifest.GetFilesForRange(0, 1<<62)
	fi := files[0]
	fi.RowCount = int64(len(rows))
	fi.MinTimeNs, fi.MaxTimeNs = rows[0].TimestampUnixNano, rows[len(rows)-1].TimestampUnixNano
	s.manifest = manifest.New("test-bucket", "logs/")
	s.manifest.AddFile(partitionFromKey(fi.Key), fi)

	run := coldSelectRunner(t, s, minute.Add(-time.Hour).UnixNano(), minute.Add(time.Hour).UnixNano())
	// day_range is evaluated in the process's local zone.
	a := minute.In(time.Local)
	f := fmt.Sprintf(`_time:day_range[%s, %s]`, a.Format("15:04"), a.Add(time.Minute).Format("15:04"))
	want := len(run(f))
	if want != 55 {
		t.Fatalf("fixture: the row query returned %d rows for %s, want 55", want, f)
	}
	if got := statsCount(t, run(f+` | stats count() n`), f); got != want {
		t.Errorf("%s | stats count() = %d, the row query returned %d", f, got, want)
	}

	// A delete over minute M only (no field in its query): the survivors are the
	// 5 late rows, which a fabricated evenly spaced series would not reproduce.
	addHideTombstone(s, `*`, minute.UnixNano(), minute.Add(time.Minute).UnixNano()-1)
	rest := len(run("*"))
	if rest != 5 {
		t.Fatalf("fixture: the delete should leave 5 rows, the row query returned %d", rest)
	}
	if got := statsCount(t, run(`* | stats count() n`), "count"); got != rest {
		t.Errorf("* | stats count() under a time-only delete = %d, want %d", got, rest)
	}
}

// TestColdCountPushdown_RewrittenGroupKey (M1): the manifest count pushdown
// reproduces the stored distribution of ONE field, so it is only sound when the
// pipe chain groups by that stored field. A pipe that rewrites the key first
// (copy, extract) must fall through to a real scan.
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
		agg[r["service.name"]]++
	}
	for _, r := range rows {
		ts, _ := time.Parse(time.RFC3339Nano, r["_time"])
		if minNs == 0 || ts.UnixNano() < minNs {
			minNs = ts.UnixNano()
		}
		if ts.UnixNano() > maxNs {
			maxNs = ts.UnixNano()
		}
	}
	fi.MinTimeNs, fi.MaxTimeNs = minNs, maxNs
	fi.LabelAggregates = map[string]map[string]int64{"service.name": agg}
	s.manifest = manifest.New("test-bucket", "logs/")
	s.manifest.AddFile(partitionFromKey(fi.Key), fi)

	served := func(q string) (map[string]int, bool) {
		before := getCounterValue(t, metrics.MetadataOnlyFiles)
		out := statsGroups(run(q), "service.name")
		return out, getCounterValue(t, metrics.MetadataOnlyFiles) != before
	}

	// Control: the plain shape is served from the aggregates.
	got, used := served(`* | stats by (service.name) count() n`)
	if !used || fmtGroups(got) != fmtGroups(groupCounts(rows, "service.name")) {
		t.Fatalf("control: pushdown used=%v answer %s", used, fmtGroups(got))
	}

	hosts := groupCounts(rows, "host.name")
	for _, q := range []string{
		`* | copy host.name as service.name | stats by (service.name) count() n`,
		`* | extract "MARKER <service.name> " from _msg | stats by (service.name) count() n`,
		`* | format "x" as service.name | stats by (service.name) count() n`,
	} {
		got, used := served(q)
		if used {
			t.Errorf("%s was answered from the stored aggregates", q)
		}
		switch q[:12] {
		case `* | copy hos`:
			if fmtGroups(got) != fmtGroups(hosts) {
				t.Errorf("%s\n  got  %s\n  want %s", q, fmtGroups(got), fmtGroups(hosts))
			}
		case `* | format "`:
			if len(got) != 1 || got["x"] != len(rows) {
				t.Errorf("%s = %s, want x=%d", q, fmtGroups(got), len(rows))
			}
		}
	}
}

// TestColdTier2Slot_FilterAndGroupBy: a field stored in a Tier-2 spare slot
// column is found by a filter and a group key under a narrow projection, with
// the slot bound by the file's own footer.
func TestColdTier2Slot_FilterAndGroupBy(t *testing.T) {
	prev := activeSlotResolver
	t.Cleanup(func() { SetSlotResolver(prev) })
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant.tier"}}))

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var rows []schema.LogRow
	for i := 0; i < 30; i++ {
		r := schema.LogRow{TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(), Body: fmt.Sprintf("MARKER slot %d", i), ServiceName: "api",
			Stream: `{service.name="api"}`, StreamID: fmt.Sprintf("%048x", 1)}
		schema.SetLogSlot(&r, "ded_s01", []string{"gold", "silver", "bronze"}[i%3])
		rows = append(rows, r)
	}
	res, err := writeLogsParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/slot.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	run := coldSelectRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())

	if got := statsCount(t, run(`_time:30m tenant.tier:=gold | stats count() n`), "gold"); got != 10 {
		t.Errorf("tenant.tier:=gold | stats count() = %d, want 10", got)
	}
	got := statsGroups(run(`MARKER | stats by (tenant.tier) count() n`), "tenant.tier")
	if fmtGroups(got) != fmtGroups(map[string]int{"gold": 10, "silver": 10, "bronze": 10}) {
		t.Errorf("stats by (tenant.tier) = %s", fmtGroups(got))
	}
}

// TestQuerySpecificFiles_ProjectsFromQuery: the direct per-file entry point
// attaches the needed-field list the same way RunQuery does.
func TestQuerySpecificFiles_ProjectsFromQuery(t *testing.T) {
	s, start, end := coldFilteredStatsFixture(t)
	files := s.manifest.GetFilesForRange(start, end)
	if len(files) != 1 {
		t.Fatalf("fixture: want one file, got %d", len(files))
	}
	var mu sync.Mutex
	cols := map[string]bool{}
	err := s.QuerySpecificFiles(context.Background(), []string{files[0].Key}, start, end, `_msg:="needle-exact" | stats count() n`, nil,
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
	if len(cols) == 0 || cols["host.name"] || cols["service.name"] {
		t.Errorf("block columns = %v, want only the projected ones (_time and _msg)", cols)
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
		{`* | stats by (service.name) count()`, "service.name", true},
		{`* | uniq by (service.name)`, "service.name", true},
		{`* | top 3 (service.name)`, "service.name", true},
		{`* | copy host.name as service.name | stats by (service.name) count()`, "service.name", false},
		{`* | extract "x <service.name> " from _msg | stats by (service.name) count()`, "service.name", false},
		{`* | format "x" as service.name | stats by (service.name) count()`, "service.name", false},
		{`* | limit 5 | stats by (service.name) count()`, "service.name", false},
		{`* | filter level:=x | stats by (service.name) count()`, "service.name", false},
		{`service.name:a | stats by (service.name) count()`, "service.name", true},
		{`*`, "service.name", false},
	}
	for _, c := range cases {
		q, err := logstorage.ParseQuery(c.query)
		if err != nil {
			t.Fatal(err)
		}
		if got := countPushdownSound(q, logstorage.GetQueryNeededFields(q), c.field); got != c.want {
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

// TestRunQueryProjectionEquivalence_Random drives random filter + pipe chains
// through the whole RunQuery path (tombstones, the metadata-only synthetic
// timestamp path, the adapter and the pipes) and requires the projected answer
// to equal the answer with every column read. The file's timestamps are
// unevenly spaced so a fabricated series cannot pass for the real one, and every
// query runs with and without a hide tombstone.
func TestRunQueryProjectionEquivalence_Random(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	minute := time.Now().UTC().Add(-20 * time.Minute).Truncate(time.Minute)
	var rows []schema.LogRow
	for i := 0; i < 120; i++ {
		ts := minute.Add(time.Duration(i) * 40 * time.Millisecond)
		if i%6 == 5 {
			ts = minute.Add(time.Minute + time.Duration(i)*time.Second/2)
		}
		svc := []string{"alpha", "beta", "gamma"}[i%3]
		body := fmt.Sprintf("MARKER request %d from %s", i%4, svc)
		if i%5 == 0 {
			body = "needle-exact"
		}
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: ts.UnixNano(), Body: body, SeverityText: []string{"INFO", "ERROR"}[i%2], SeverityNumber: int32(9 + i%5),
			ServiceName: svc, HostName: "host-" + svc, TraceID: fmt.Sprintf("%032x", i),
			Stream: fmt.Sprintf(`{service.name=%q}`, svc), StreamID: fmt.Sprintf("%048x", len(svc)),
			LogAttributes: map[string]string{"repro_layer": []string{"cold", "warm"}[i%2], "region": "eu-" + svc[:1]},
		})
	}
	res, err := writeLogsParquet(rows, 40, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/prop.parquet", minute.Format("2006-01-02"), minute.Hour()), res.Data, minute.Add(time.Minute))
	fi := s.manifest.GetFilesForRange(0, 1<<62)[0]
	fi.RowCount = int64(len(rows))
	fi.MinTimeNs, fi.MaxTimeNs = rows[0].TimestampUnixNano, rows[len(rows)-1].TimestampUnixNano
	s.manifest = manifest.New("test-bucket", "logs/")
	s.manifest.AddFile(partitionFromKey(fi.Key), fi)

	start, end := minute.Add(-time.Hour).UnixNano(), minute.Add(time.Hour).UnixNano()
	a := minute.In(time.Local)
	extra := []string{
		fmt.Sprintf(`_time:day_range[%s, %s]`, a.Format("15:04"), a.Add(time.Minute).Format("15:04")),
		fmt.Sprintf(`_time:day_range[%s, %s]`, a.Add(time.Minute).Format("15:04"), a.Add(2*time.Minute).Format("15:04")),
	}
	run := func(st storage.Storage, q string) string {
		internalvlstorage.SetStorage(st, nil)
		query := mustParseQueryWithTime(t, q, start, end)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, query, false, nil)
		var mu sync.Mutex
		var out []map[string]string
		if err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			r := blockRowFields([]*logstorage.DataBlock{db})
			mu.Lock()
			out = append(out, r...)
			mu.Unlock()
		}); err != nil {
			return "error: " + err.Error()
		}
		return canonRows(out)
	}
	t.Cleanup(func() { vlapp.SetExternalStorage(nil) })

	rng := rand.New(rand.NewSource(285))
	g := projGen{pick: rng.Intn, extra: extra}
	for _, tombstone := range []bool{false, true} {
		if tombstone {
			addHideTombstone(s, `level:=ERROR`, start, end)
		}
		checked := 0
		for i := 0; i < 150; i++ {
			q := g.query()
			if _, err := logstorage.ParseQuery(q); err != nil || logstorage.QueryHasFilterSubqueries(mustParseQuery(t, q)) {
				continue
			}
			got, want := run(s, q), run(allColumnsStore{s}, q)
			if got != want {
				t.Fatalf("tombstone=%v: projected answer differs for %q\n  projected:\n%s\n  all columns:\n%s", tombstone, q, got, want)
			}
			checked++
		}
		if checked < 80 {
			t.Fatalf("tombstone=%v: only %d runnable queries; the generator no longer exercises the property", tombstone, checked)
		}
	}
}
