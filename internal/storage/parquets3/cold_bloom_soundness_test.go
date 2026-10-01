package parquets3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fixture: 2 files x 4 row groups x 10 rows. Row group k of file f has
// trace_id "trace-f-k" (and a hex form), body "wordFK common pad...".
func bloomFixture(t *testing.T) (*Storage, func(ctx context.Context, q string) []map[string]string) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	pad := strings.Repeat("x", 1500)
	for f := 0; f < 2; f++ {
		var rows []schema.LogRow
		for i := 0; i < 40; i++ {
			k := i / 10
			rows = append(rows, schema.LogRow{
				TimestampUnixNano: base.Add(time.Duration(f*40+i) * time.Second).UnixNano(),
				Body:              fmt.Sprintf("w%d%d common %s%d", f, k, pad, i),
				ServiceName:       []string{"alpha", "beta"}[f],
				SeverityText:      []string{"INFO", "ERROR"}[k%2],
				TraceID:           fmt.Sprintf("%032x", 0xabc000+f*16+k),
				Stream:            fmt.Sprintf(`{service.name=%q}`, []string{"alpha", "beta"}[f]),
				StreamID:          fmt.Sprintf("%048x", 3+f),
			})
		}
		res, err := writeLogsParquet(rows, 10, 3)
		if err != nil {
			t.Fatal(err)
		}
		registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/f%d.parquet", base.Format("2006-01-02"), base.Hour(), f), res.Data, base.Add(time.Duration(f)*40*time.Second))
	}
	run := reviewRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	return s, run
}

func tid(f, k int) string { return fmt.Sprintf("%032x", 0xabc000+f*16+k) }

func TestColdBloom_BloomSoundness(t *testing.T) {
	_, run := bloomFixture(t)
	bg := context.Background()
	t10, t11, t02 := tid(1, 0), tid(1, 1), tid(0, 2)
	cases := []struct {
		q    string
		want int
	}{
		{fmt.Sprintf(`trace_id:=%s`, t10), 10},
		{fmt.Sprintf(`NOT (NOT trace_id:=%s)`, t10), 10},
		{fmt.Sprintf(`NOT NOT trace_id:=%s`, t10), 10},
		{fmt.Sprintf(`-trace_id:=%s`, t10), 70},
		{fmt.Sprintf(`NOT (trace_id:=%s level:=INFO)`, t10), 70},
		{fmt.Sprintf(`NOT (service.name:=beta trace_id:=%s)`, t10), 70},
		{fmt.Sprintf(`NOT trace_id:in(%s,%s)`, t10, t11), 60},
		{fmt.Sprintf(`NOT (trace_id:=%s OR trace_id:=%s)`, t10, t02), 60},
		{fmt.Sprintf(`trace_id:=%s OR service.name:=alpha`, t10), 50},
		{fmt.Sprintf(`trace_id:=%s OR level:=ERROR`, t10), 50},
		{fmt.Sprintf(`(trace_id:=%s AND level:=INFO) OR trace_id:=%s`, t10, t02), 20},
		{fmt.Sprintf(`service.name:=beta (trace_id:=%s OR trace_id:=%s)`, t10, t02), 10},
		{fmt.Sprintf(`service.name:=beta NOT trace_id:=%s`, t10), 30},
		{fmt.Sprintf(`trace_id:in(%s)`, func() string {
			var v []string
			for i := 0; i < 200; i++ {
				v = append(v, fmt.Sprintf("%032x", 0xdead0000+i))
			}
			v = append(v, t10, t02)
			return strings.Join(v, ",")
		}()), 20},
		{`trace_id:~"abc01"`, 40},
		// known gaps (pre-existing, separate issues, not asserted here): a QUOTED trace_id prefix
		// returns 0 rows on the unprojected row path; a rewriting pipe ahead of a bloom-column
		// filter (`format "X" as trace_id | filter trace_id:=X`) is pruned on the stored column.
		{`trace_id:00000000000000000000000000abc01*`, 40},
		// pipe-level filters (string-based extraction suspicion)
		{fmt.Sprintf(`* | filter trace_id:=%s or service.name:=alpha`, t10), 50},
		{fmt.Sprintf(`* | filter not trace_id:=%s`, t10), 70},
		// token bloom (_msg words) under NOT / OR / if
		{`w10`, 10},
		{`NOT w10`, 70},
		{`-w10`, 70},
		{`w10 OR w01`, 20},
		{`common NOT w10`, 70},
	}
	for _, c := range cases {
		got := len(run(bg, c.q))
		got2 := sumN(run(bg, c.q+` | stats count() n`))
		if got != c.want || got2 != c.want {
			t.Errorf("%s: rows=%d stats=%d want %d", c.q, got, got2, c.want)
		}
	}
	for _, c := range []struct {
		q    string
		want int
	}{
		{fmt.Sprintf(`* | stats count() if (trace_id:=%s) n`, t10), 10},
		{fmt.Sprintf(`* | stats count() if (NOT trace_id:=%s) n`, t10), 70},
		{fmt.Sprintf(`* | stats count() if (trace_id:=%s) a, count() n`, t10), 80},
		{`* | stats count() if (w10) a, count() n`, 80},
		{`* | stats count() if (NOT w10) n`, 70},
		{fmt.Sprintf(`* | stats by (service.name) count() if (trace_id:=%s OR level:=ERROR) n`, t10), 50},
	} {
		if got := sumN(run(bg, c.q)); got != c.want {
			t.Errorf("%s: got %d want %d", c.q, got, c.want)
		}
	}
}

// positive row lookups must still prune row groups. (A stats pipe reads a
// projected range, which never uses the footer bloom, so it is not asserted.)
func TestColdBloom_PositivePrunes(t *testing.T) {
	_, run := bloomFixture(t)
	bg := context.Background()
	skips := func() uint64 {
		var n uint64
		for _, r := range []string{"bloom", "pushdown", "footer_prefetch", "column_stats", "label_index", "token_bloom"} {
			n += metrics.ParquetRowGroupsSkipped.Get(r)
		}
		return n + metrics.ParquetBloomChecks.Get("facet_bloom_skip") + metrics.ParquetBloomChecks.Get("file_bloom_skip")
	}
	for _, q := range []string{
		fmt.Sprintf(`trace_id:=%s`, tid(1, 0)),
		fmt.Sprintf(`trace_id:in(%s,%s)`, tid(1, 0), tid(0, 2)),
		`w10`,
	} {
		before := skips()
		var detail []string
		for _, r := range []string{"bloom", "pushdown", "footer_prefetch", "column_stats", "label_index", "token_bloom"} {
			detail = append(detail, fmt.Sprintf("%s=%d", r, metrics.ParquetRowGroupsSkipped.Get(r)))
		}
		out := run(bg, q)
		after := skips()
		var detail2 []string
		for _, r := range []string{"bloom", "pushdown", "footer_prefetch", "column_stats", "label_index", "token_bloom"} {
			detail2 = append(detail2, fmt.Sprintf("%s=%d", r, metrics.ParquetRowGroupsSkipped.Get(r)))
		}
		t.Logf("%s: rows=%d skips +%d (%v -> %v)", q, len(out), after-before, detail, detail2)
		if after == before {
			t.Errorf("%s: no pruning at all", q)
		}
	}
}

// C1: time-sensitive shapes against the count pushdown with LabelAggregates set.
func TestColdBloom_PushdownTimeShapesMore(t *testing.T) {
	s, start, end, rows, minute := reviewPushdownFixture(t)
	run := reviewRunner(t, s, start, end)
	runAll := reviewRunner(t, allColumnsStore{s}, start, end)
	bg := context.Background()
	_ = rows
	a := minute.Add(-time.Minute).Format(time.RFC3339)
	b := minute.Add(5 * time.Minute).Format(time.RFC3339)
	mid := minute.Add(5 * time.Minute).Format(time.RFC3339)
	for _, q := range []string{
		`* | stats by (service.name) count() n`,
		fmt.Sprintf(`* | stats by (service.name) count() if (_time:[%s, %s]) n`, a, b),
		`* | stats by (service.name) count() if (_time:1h offset 15m) n`,
		`_time:1h offset 25m | stats by (service.name) count() n`,
		fmt.Sprintf(`_time:>%s | stats by (service.name) count() n`, mid),
		fmt.Sprintf(`_time:<%s | stats by (service.name) count() n`, mid),
		fmt.Sprintf(`_time:[%s, %s] | stats by (service.name) count() n`, a, b),
		`* | stats by (_time:1h, service.name) count() n`,
		`* | stats by (_time:1m, service.name) count() n`,
		`* | uniq by (service.name) with hits`,
		`* | top 5 (service.name)`,
		`* | stats by (service.name) count() n, min(_time) m`,
		`* | stats by (service.name) count() n | sort by (n)`,
		`_time:week_range[Mon, Sun] | stats by (service.name) count() n`,
		`* | fields service.name, _time | stats by (service.name) count() n`,
		`* | stats by (service.name) count_uniq(_time) n`,
	} {
		before := getCounterValue(t, metrics.MetadataOnlyFiles)
		got := canonRows(run(bg, q))
		used := getCounterValue(t, metrics.MetadataOnlyFiles) != before
		want := canonRows(runAll(bg, q))
		t.Logf("pushdown=%v %s", used, q)
		if got != want {
			t.Errorf("%s (pushdown=%v)\n got  %s\n want %s", q, used, got, want)
		}
	}
	// window that covers only part of the file: pushdown must not answer it.
	pa := minute.Add(2 * time.Second).UnixNano()
	pb := minute.Add(30 * time.Minute).UnixNano()
	runPart := reviewRunner(t, s, pa, pb)
	runPartAll := reviewRunner(t, allColumnsStore{s}, pa, pb)
	before := getCounterValue(t, metrics.MetadataOnlyFiles)
	got := canonRows(runPart(bg, `* | stats by (service.name) count() n`))
	used := getCounterValue(t, metrics.MetadataOnlyFiles) != before
	want := canonRows(runPartAll(bg, `* | stats by (service.name) count() n`))
	t.Logf("partial window pushdown=%v", used)
	if got != want || used {
		t.Errorf("partial window: pushdown=%v got %s want %s", used, got, want)
	}
	_ = manifest.FileInfo{}
}

func TestColdBloom_QuotedIf(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	pad := strings.Repeat("x", 1500)
	for f := 0; f < 2; f++ {
		var rows []schema.LogRow
		for i := 0; i < 40; i++ {
			k := i / 10
			rows = append(rows, schema.LogRow{TimestampUnixNano: base.Add(time.Duration(f*40+i) * time.Second).UnixNano(), Body: fmt.Sprintf("b %s%d", pad, i),
				ServiceName: []string{"alpha", "beta"}[f], SeverityText: "INFO", TraceID: fmt.Sprintf("tr-%d-%d", f, k),
				Stream: fmt.Sprintf(`{service.name=%q}`, []string{"alpha", "beta"}[f]), StreamID: fmt.Sprintf("%048x", 3+f)})
		}
		res, _ := writeLogsParquet(rows, 10, 3)
		key := fmt.Sprintf("logs/dt=%s/hour=%02d/q%d.parquet", base.Format("2006-01-02"), base.Hour(), f)
		registerFileInMockS3(t, s, mock, key, res.Data, base.Add(time.Duration(f)*40*time.Second))
		var ids []string
		for k := 0; k < 4; k++ {
			ids = append(ids, fmt.Sprintf("tr-%d-%d", f, k))
		}
		mock.putFile(key+".bloom", bloomindex.NewFileBloomIndex(map[string][]string{"trace_id": ids}, 0.01).Marshal())
	}
	run := reviewRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	b0 := metrics.ParquetBloomChecks.Get("file_bloom_skip")
	if n := len(run(context.Background(), `trace_id:="tr-1-0"`)); n != 10 {
		t.Errorf("positive got %d", n)
	}
	t.Logf("file_bloom_skip on positive lookup: +%d", metrics.ParquetBloomChecks.Get("file_bloom_skip")-b0)
	for _, c := range []struct {
		q    string
		want int
	}{
		{`* | stats count() if (trace_id:="tr-1-0") a, count() n`, 80},
		{`* | stats count() if (service.name:="alpha") a, count() n`, 80},
		{`* | stats by (service.name) count() if (trace_id:="tr-1-0") a, count() n`, 80},
		{`NOT trace_id:="tr-1-0" | stats count() n`, 70},
		{`trace_id:="tr-1-0" OR service.name:="alpha" | stats count() n`, 50},
	} {
		if got := sumN(run(context.Background(), c.q)); got != c.want {
			t.Errorf("%s: got %d want %d", c.q, got, c.want)
		}
		if got := len(run(context.Background(), strings.Split(c.q, " | ")[0])); c.q[0] != '*' && got != c.want {
			t.Errorf("rows %s: got %d want %d", c.q, got, c.want)
		}
	}
}

// putSidecars uploads a `.bloom` sidecar per file (trace_id and service.name
// columns), the input of the file-level prune that runs before any row group is
// read. File f holds trace ids tid(f,0..3) and service alpha (f=0) or beta (f=1).
func putSidecars(t *testing.T, mock *mockS3Server, keyPrefix string, tidFn func(f, k int) string) {
	t.Helper()
	for f := 0; f < 2; f++ {
		tf := bloomindex.NewFilter(16, 0.001)
		for k := 0; k < 4; k++ {
			tf.Add(tidFn(f, k))
		}
		sf := bloomindex.NewFilter(16, 0.001)
		sf.Add([]string{"alpha", "beta"}[f])
		idx := bloomindex.New()
		idx.AddColumns("_", map[string]*bloomindex.Filter{"trace_id": tf, "service.name": sf})
		mock.putFile(fmt.Sprintf("%sf%d.parquet.bloom", keyPrefix, f), idx.Marshal())
	}
}

// TestColdBloom_FileLevelSidecars: with `.bloom` sidecars present the file-level
// prune is live; it must be off under NOT / OR / if (...) and on for a plain
// positive lookup (file 0 is skipped).
func TestColdBloom_FileLevelSidecars(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	prefix := fmt.Sprintf("logs/dt=%s/hour=%02d/", base.Format("2006-01-02"), base.Hour())
	pad := strings.Repeat("x", 1500)
	for f := 0; f < 2; f++ {
		var rows []schema.LogRow
		for i := 0; i < 40; i++ {
			k := i / 10
			rows = append(rows, schema.LogRow{
				TimestampUnixNano: base.Add(time.Duration(f*40+i) * time.Second).UnixNano(),
				Body:              fmt.Sprintf("w%d%d common %s%d", f, k, pad, i),
				ServiceName:       []string{"alpha", "beta"}[f],
				SeverityText:      []string{"INFO", "ERROR"}[k%2],
				TraceID:           tid(f, k),
				Stream:            fmt.Sprintf(`{service.name=%q}`, []string{"alpha", "beta"}[f]),
				StreamID:          fmt.Sprintf("%048x", 3+f),
			})
		}
		res, err := writeLogsParquet(rows, 10, 3)
		if err != nil {
			t.Fatal(err)
		}
		registerFileInMockS3(t, s, mock, fmt.Sprintf("%sf%d.parquet", prefix, f), res.Data, base.Add(time.Duration(f)*40*time.Second))
	}
	putSidecars(t, mock, prefix, tid)
	run := reviewRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	bg := context.Background()
	t10 := tid(1, 0)
	for _, c := range []struct {
		q    string
		want int
	}{
		{fmt.Sprintf(`trace_id:=%s`, t10), 10},
		{fmt.Sprintf(`trace_id:=%s OR service.name:=alpha`, t10), 50},
		{fmt.Sprintf(`service.name:=alpha OR trace_id:=%s`, t10), 50},
		{fmt.Sprintf(`NOT trace_id:=%s`, t10), 70},
		{fmt.Sprintf(`NOT (trace_id:=%s level:=INFO)`, t10), 70},
		{fmt.Sprintf(`* | filter trace_id:=%s or service.name:=alpha`, t10), 50},
	} {
		if got, st := len(run(bg, c.q)), sumN(run(bg, c.q+` | stats count() n`)); got != c.want || st != c.want {
			t.Errorf("%s: rows=%d stats=%d want %d", c.q, got, st, c.want)
		}
	}
	for _, c := range []struct {
		q    string
		want int
	}{
		{fmt.Sprintf(`* | stats count() if (trace_id:=%s OR service.name:=alpha) n`, t10), 50},
		{fmt.Sprintf(`* | stats count() if (NOT trace_id:=%s) n`, t10), 70},
	} {
		if got := sumN(run(bg, c.q)); got != c.want {
			t.Errorf("%s: got %d want %d", c.q, got, c.want)
		}
	}
}
