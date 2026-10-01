package parquets3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func ztid(f, k int) string { return fmt.Sprintf("%032x", 0xabc000+f*16+k) }

func TestColdBloom_BloomSoundness(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	pad := strings.Repeat("y", 1500)
	for f := 0; f < 2; f++ {
		var rows []schema.TraceRow
		for i := 0; i < 40; i++ {
			k := i / 10
			ts := base.Add(time.Duration(f*40+i) * time.Second).UnixNano()
			rows = append(rows, schema.TraceRow{
				TimestampUnixNano: ts, StartTimeUnixNano: ts,
				TraceID: ztid(f, k), SpanID: fmt.Sprintf("%016x", f*100+i), SpanName: fmt.Sprintf("op%d-%s%d", k%2, pad, i), ServiceName: []string{"alpha", "beta"}[f],
				Stream: fmt.Sprintf(`{resource_attr:service.name=%q}`, []string{"alpha", "beta"}[f]), StreamID: fmt.Sprintf("%048x", 3+f),
			})
		}
		res, err := writeTracesParquet(rows, 10, 3)
		if err != nil {
			t.Fatal(err)
		}
		registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/f%d.parquet", base.Format("2006-01-02"), base.Hour(), f), res.Data, base.Add(time.Duration(f)*40*time.Second))
	}
	run := tracesRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	bg := context.Background()
	t10, t11, t02 := ztid(1, 0), ztid(1, 1), ztid(0, 2)
	many := func() string {
		var v []string
		for i := 0; i < 200; i++ {
			v = append(v, fmt.Sprintf("%032x", 0xdead0000+i))
		}
		v = append(v, t10, t02)
		return strings.Join(v, ",")
	}()
	for _, c := range []struct {
		q    string
		want int
	}{
		{`*`, 80},
		{`"resource_attr:service.name":=alpha`, 40},
		{fmt.Sprintf(`trace_id:=%s`, t10), 10},
		{fmt.Sprintf(`trace_id:%q`, t10), 10},
		{fmt.Sprintf(`NOT (NOT trace_id:=%s)`, t10), 10},
		{fmt.Sprintf(`-trace_id:=%s`, t10), 70},
		{fmt.Sprintf(`NOT trace_id:in(%s,%s)`, t10, t11), 60},
		{fmt.Sprintf(`NOT (trace_id:=%s OR trace_id:=%s)`, t10, t02), 60},
		{fmt.Sprintf(`trace_id:=%s OR "resource_attr:service.name":=alpha`, t10), 50},
		{fmt.Sprintf(`(trace_id:=%s AND name:op0*) OR trace_id:=%s`, t10, t02), 20},
		{fmt.Sprintf(`trace_id:in(%s)`, many), 20},
		{`trace_id:~"abc01"`, 40},
		// known gap (pre-existing, separate issue): a QUOTED trace_id prefix is treated as an exact value by the _trace_idx prefilter.
		// Token bloom: a bare word under NOT must not prune.
		{`NOT zzword`, 80},
		{`-zzword`, 80},
		{`trace_id:~"^00000000000000000000000000abc01"`, 40},
		{`trace_id:00000000000000000000000000abc01*`, 40},
		{fmt.Sprintf(`* | filter trace_id:=%s or "resource_attr:service.name":=alpha`, t10), 50},
	} {
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
		{fmt.Sprintf(`* | stats count() if (trace_id:=%s) a, count() n`, t10), 80},
		{fmt.Sprintf(`* | stats count() if (NOT trace_id:=%s) n`, t10), 70},
	} {
		if got := sumN(run(bg, c.q)); got != c.want {
			t.Errorf("%s: got %d want %d", c.q, got, c.want)
		}
	}
	skips := func() uint64 {
		var n uint64
		for _, r := range []string{"bloom", "pushdown", "footer_prefetch", "column_stats", "label_index", "token_bloom"} {
			n += metrics.ParquetRowGroupsSkipped.Get(r)
		}
		return n
	}
	for _, q := range []string{fmt.Sprintf(`trace_id:%q`, t10), fmt.Sprintf(`trace_id:=%s | stats count() n`, t10), fmt.Sprintf(`trace_id:in(%s,%s)`, t10, t02)} {
		b := skips()
		out := run(bg, q)
		t.Logf("%s: rows=%d skips +%d", q, len(out), skips()-b)
	}
}

// putSidecarsT uploads a `.bloom` sidecar per file (trace_id and service.name
// columns), the input of the file-level prune that runs before any row group is
// read. File f holds trace ids ztid(f,0..3) and service alpha (f=0) or beta (f=1).
func putSidecarsT(t *testing.T, mock *mockS3Server, keyPrefix string, tidFn func(f, k int) string) {
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
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	prefix := fmt.Sprintf("traces/dt=%s/hour=%02d/", base.Format("2006-01-02"), base.Hour())
	pad := strings.Repeat("y", 1500)
	for f := 0; f < 2; f++ {
		var rows []schema.TraceRow
		for i := 0; i < 40; i++ {
			k := i / 10
			ts := base.Add(time.Duration(f*40+i) * time.Second).UnixNano()
			rows = append(rows, schema.TraceRow{
				TimestampUnixNano: ts, StartTimeUnixNano: ts,
				TraceID: ztid(f, k), SpanID: fmt.Sprintf("%016x", f*100+i), SpanName: fmt.Sprintf("op%d-%s%d", k%2, pad, i), ServiceName: []string{"alpha", "beta"}[f],
				Stream: fmt.Sprintf(`{resource_attr:service.name=%q}`, []string{"alpha", "beta"}[f]), StreamID: fmt.Sprintf("%048x", 3+f),
			})
		}
		res, err := writeTracesParquet(rows, 10, 3)
		if err != nil {
			t.Fatal(err)
		}
		registerFileInMockS3(t, s, mock, fmt.Sprintf("%sf%d.parquet", prefix, f), res.Data, base.Add(time.Duration(f)*40*time.Second))
	}
	putSidecarsT(t, mock, prefix, ztid)
	run := tracesRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	bg := context.Background()
	t10 := ztid(1, 0)
	for _, c := range []struct {
		q    string
		want int
	}{
		{fmt.Sprintf(`trace_id:=%s`, t10), 10},
		{fmt.Sprintf(`trace_id:=%s OR "resource_attr:service.name":=alpha`, t10), 50},
		{fmt.Sprintf(`"resource_attr:service.name":=alpha OR trace_id:=%s`, t10), 50},
		{fmt.Sprintf(`NOT trace_id:=%s`, t10), 70},
		{fmt.Sprintf(`NOT (trace_id:=%s)`, t10), 70},
		{fmt.Sprintf(`* | filter trace_id:=%s or "resource_attr:service.name":=alpha`, t10), 50},
	} {
		if got, st := len(run(bg, c.q)), sumN(run(bg, c.q+` | stats count() n`)); got != c.want || st != c.want {
			t.Errorf("%s: rows=%d stats=%d want %d", c.q, got, st, c.want)
		}
	}
	for _, c := range []struct {
		q    string
		want int
	}{
		{fmt.Sprintf(`* | stats count() if (trace_id:=%s OR "resource_attr:service.name":=alpha) n`, t10), 50},
		{fmt.Sprintf(`* | stats count() if (NOT trace_id:=%s) n`, t10), 70},
	} {
		if got := sumN(run(bg, c.q)); got != c.want {
			t.Errorf("%s: got %d want %d", c.q, got, c.want)
		}
	}
}

// TestColdBloom_QuotedPushdownText: the row-group pushdown predicate is read from
// the filter part of the query only; the quoted equality inside a pipe's
// `if (...)` (or after a NOT / OR) must not become a pushdown check.
func TestColdBloom_QuotedPushdownText(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	pad := strings.Repeat("y", 1500)
	for f := 0; f < 2; f++ {
		var rows []schema.TraceRow
		for i := 0; i < 40; i++ {
			k := i / 10
			ts := base.Add(time.Duration(f*40+i) * time.Second).UnixNano()
			rows = append(rows, schema.TraceRow{
				TimestampUnixNano: ts, StartTimeUnixNano: ts,
				TraceID: fmt.Sprintf("tr-%d-%d", f, k), SpanID: fmt.Sprintf("%016x", f*100+i), SpanName: fmt.Sprintf("op%d-%s%d", k%2, pad, i), ServiceName: []string{"alpha", "beta"}[f],
				Stream: fmt.Sprintf(`{resource_attr:service.name=%q}`, []string{"alpha", "beta"}[f]), StreamID: fmt.Sprintf("%048x", 3+f),
			})
		}
		res, err := writeTracesParquet(rows, 10, 3)
		if err != nil {
			t.Fatal(err)
		}
		registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/q%d.parquet", base.Format("2006-01-02"), base.Hour(), f), res.Data, base.Add(time.Duration(f)*40*time.Second))
	}
	run := tracesRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	bg := context.Background()
	t10 := "tr-1-0"
	for _, c := range []struct {
		q    string
		want int
	}{
		{fmt.Sprintf(`* | stats count() if (trace_id:=%q) a, count() n`, t10), 80},
		{fmt.Sprintf(`* | stats count() if (trace_id:=%q) n`, t10), 10},
		{fmt.Sprintf(`* | stats by ("resource_attr:service.name") count() if (trace_id:=%q) a, count() n`, t10), 80},
		{fmt.Sprintf(`* | stats count() if (NOT trace_id:=%q) n`, t10), 70},
		{fmt.Sprintf(`NOT trace_id:=%q | stats count() n`, t10), 70},
		{fmt.Sprintf(`trace_id:=%q OR "resource_attr:service.name":="alpha" | stats count() n`, t10), 50},
		{fmt.Sprintf(`* | filter trace_id:=%q or "resource_attr:service.name":="alpha" | stats count() n`, t10), 50},
	} {
		if got := sumN(run(bg, c.q)); got != c.want {
			t.Errorf("%s: got %d want %d", c.q, got, c.want)
		}
	}
}
