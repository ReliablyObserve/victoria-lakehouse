package parquets3

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// scaleSpans builds n spans of traces of 8 spans, sorted the way the writer sorts a flush
// group of many streams: by service stream, then time. A trace's spans (8 services in a
// row) therefore land in different row groups, and the time ranges of the row groups
// overlap.
func scaleSpans(n int, hour time.Time) ([]schema.TraceRow, []string, []int64) {
	rnd := rand.New(rand.NewSource(7))
	rows := make([]schema.TraceRow, 0, n)
	var ids []string
	var starts []int64
	var tid string
	var base int64
	for i := 0; i < n; i++ {
		if i%8 == 0 {
			tid = fmt.Sprintf("%016x%016x", rnd.Uint64(), rnd.Uint64())
			ids = append(ids, tid)
			base = hour.UnixNano() + rnd.Int63n(int64(time.Hour)-int64(time.Second))
			starts = append(starts, base)
		}
		svc := fmt.Sprintf("svc-%02d", i%8)
		ts := base + int64(i%8)*10_000_000
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: ts + 5_000_000, StartTimeUnixNano: schema.Int64Ptr(ts), TraceID: tid, SpanID: fmt.Sprintf("%016x", rnd.Uint64()),
			SpanName: fmt.Sprintf("op-%d", rnd.Intn(40)), ServiceName: svc, DurationNs: schema.Int64Ptr(5_000_000), SpanKind: schema.Int32Ptr(2),
			Stream: fmt.Sprintf(`{resource_attr:service.name=%q}`, svc), StreamID: fmt.Sprintf("%032x%016x", 0x7ace, i%8),
			SpanAttributes:     map[string]string{"peer": fmt.Sprintf("p%d", rnd.Intn(50)), "pad": fmt.Sprintf("%x", rnd.Uint64())},
			ResourceAttributes: map[string]string{"host": fmt.Sprintf("h%d", rnd.Intn(16))},
		})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].ServiceName != rows[j].ServiceName {
			return rows[i].ServiceName < rows[j].ServiceName
		}
		return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano
	})
	return rows, ids, starts
}

// What a trace-by-ID read of one span object costs in S3 requests when the object has many
// row groups and the process is cold. The numbers are a measurement, not a target: on this
// commit a 60-row-group object takes about 60 requests per lookup (the footer, one bloom
// read per row group whose time range holds the trace, then about six ranged reads of each
// row group that holds a span) and a 2-row-group object reads its two groups in full more
// than once. The test keeps the answer exact (every span of the trace, nothing else) and
// the request count under a ceiling that a regression of the read path would cross. It uses one
// layout (120,000 spans in 60 row groups of 2,000): a smaller object is read whole, and a second layout
// doubled the time of the race-enabled heavy job.
func TestSpanByIDReads_MultiRowGroupFile(t *testing.T) {
	if testing.Short() {
		t.Skip("builds objects of several MiB and reads them cold")
	}
	e := newRestartEnv(t)
	rows, ids, starts := scaleSpans(120_000, rwHour)
	for _, rg := range []int{2_000} {
		res, err := writeTracesParquet(rows, rg, 3)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("traces/dt=2026-10-01/hour=07/scale-%d.parquet", rg)
		e.mock.putFile(key, res.Data)
		var mn, mx int64 = 1 << 62, 0
		for i := range rows {
			mn, mx = min(mn, rows[i].TimestampUnixNano), max(mx, rows[i].TimestampUnixNano)
		}
		e.s.manifest.AddFile("dt=2026-10-01/hour=07", manifest.FileInfo{Key: key, Size: int64(len(res.Data)), RowCount: int64(len(rows)), MinTimeNs: mn, MaxTimeNs: mx})
		gets := func() int { return int(metrics.S3RequestsTotal.Get("GetObject")) }
		bytesRead := func() int { return int(metrics.S3BytesReadTotal.Get()) }
		total, totalBytes := 0, 0
		const lookups = 5
		for i := 0; i < lookups; i++ {
			e.s.ClearCaches()
			id, start := ids[(i*997)%len(ids)], starts[(i*997)%len(ids)]
			from, to := time.Unix(0, start), time.Unix(0, start+int64(100*time.Millisecond))
			g0, b0 := gets(), bytesRead()
			got := e.rows(logstorage.TenantID{}, fmt.Sprintf(`trace_id:=%q`, id), from, to)
			if len(got) != 8 {
				t.Fatalf("rg=%d: %d spans for %s, want 8", rg, len(got), id)
			}
			total += gets() - g0
			totalBytes += bytesRead() - b0
		}
		per := float64(total) / lookups
		t.Logf("file %d KiB, %d row groups: %.1f GETs and %d KiB per by-ID read (cold caches)", len(res.Data)/1024, (len(rows)+rg-1)/rg, per, totalBytes/lookups/1024)
		if per > 150 {
			t.Errorf("a by-ID read of a %d-row-group object made %.0f requests on average, want at most 150", (len(rows)+rg-1)/rg, per)
		}
		e.s.manifest.RemoveFile("dt=2026-10-01/hour=07", key)
	}
}
