package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A full-row read (a bare LogsQL filter, `* | limit N`, or VL's
// `| sort by (_time) desc | offset | limit` rewrite of the limit argument)
// projects every column, so queryFile downloads the whole object. RunQuery has
// already run prefetchFooters by then, which caches, for every object of at
// least minFileSizeForPrefetch, a handle backed by a synthetic reader holding
// only the footer. Decoding rows through that handle read the column data as
// zeros: every timestamp was 0, nothing fell inside the query window, and the
// query returned no rows for the object — silently. Projected queries opened a
// fresh ranged handle and were unaffected, which is why `| stats` and
// `| fields` still counted the rows a bare filter could not find.

const fullReadMarkerRows = 7

type fullReadFixture struct {
	s        *Storage
	file     manifest.FileInfo
	marker   string
	ingestAt time.Time
	total    int
}

// newFullReadFixture flushes one realistic tenant 0:0 object through the
// production writer: a continuous span stream (few distinct trace IDs, so the
// trace-index footer stays small enough for the footer prefetch to cache it)
// plus marker spans, exactly the shape of a default-tenant object on a live
// stack.
func newFullReadFixture(t *testing.T) *fullReadFixture {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.manifest.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	cfg := testConfig()
	cfg.Mode = config.ModeTraces
	writer := NewBatchWriter(&cfg.Insert, s.pool, s.manifest, "traces/", config.ModeTraces)
	writer.SetTenantPrefix(func(a, p uint32) string { return fmt.Sprintf("%d/%d/traces/", a, p) })

	ingestAt := time.Date(2026, 9, 14, 12, 51, 12, 0, time.UTC)
	marker := fmt.Sprintf("fullread%d", ingestAt.UnixNano())
	rows := fullReadStreamRows(rand.New(rand.NewSource(7)), ingestAt.Add(90*time.Second), 2256)
	rows = append(rows, fullReadMarkerSpans(ingestAt, marker)...)
	const partition = "dt=2026-09-14/hour=12"
	if err := writer.flushTracePartition(context.Background(), partition, rows); err != nil {
		t.Fatalf("flush: %v", err)
	}
	files := s.manifest.FilesForPartition(partition)
	if len(files) != 1 {
		t.Fatalf("flush registered %d objects, want 1", len(files))
	}
	if files[0].Size < minFileSizeForPrefetch {
		t.Fatalf("fixture object is %d bytes; it must be at least minFileSizeForPrefetch (%d) to exercise the prefetched-footer path", files[0].Size, minFileSizeForPrefetch)
	}
	return &fullReadFixture{s: s, file: files[0], marker: marker, ingestAt: ingestAt, total: len(rows)}
}

func fullReadStreamRows(rng *rand.Rand, base time.Time, n int) []schema.TraceRow {
	services := []string{"api-gateway", "user-service", "order-service", "payment-service", "inventory-service", "auth-service"}
	names := []string{"HTTP GET /api/v1/users", "HTTP POST /api/v1/orders", "DB SELECT users", "gRPC /payment.Process", "Redis GET session", "Kafka produce events"}
	hex := func(n int) string {
		const d = "0123456789abcdef"
		b := make([]byte, n)
		for i := range b {
			b[i] = d[rng.Intn(16)]
		}
		return string(b)
	}
	traceIDs := make([]string, 300)
	for i := range traceIDs {
		traceIDs[i] = hex(32)
	}
	rows := make([]schema.TraceRow, 0, n)
	for i := 0; i < n; i++ {
		svc, name := services[rng.Intn(len(services))], names[rng.Intn(len(names))]
		end := base.Add(-time.Duration(rng.Intn(120_000)) * time.Millisecond)
		start := end.Add(-time.Duration(5+rng.Intn(50)) * time.Millisecond)
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano:  end.UnixNano(),
			StartTimeUnixNano:  start.UnixNano(),
			TraceID:            traceIDs[rng.Intn(len(traceIDs))],
			SpanID:             hex(16),
			ParentSpanID:       hex(16),
			SpanName:           name,
			ServiceName:        svc,
			DurationNs:         end.Sub(start).Nanoseconds(),
			SpanKind:           int32(2 + rng.Intn(2)),
			HTTPUrl:            "http://" + svc + ":8080/api/v1/users",
			K8sPodName:         svc + "-" + hex(10),
			Stream:             fmt.Sprintf(`{name=%q,resource_attr:service.name=%q}`, name, svc),
			StreamID:           hex(32),
			ContainerID:        hex(64),
			ServiceInstanceID:  svc + "-" + hex(8),
			ResourceAttributes: map[string]string{"telemetry.sdk.language": "go", "os.type": "linux"},
			SpanAttributes:     map[string]string{"thread.id": fmt.Sprint(1 + rng.Intn(32)), "code.function": name},
		})
	}
	return rows
}

func fullReadMarkerSpans(base time.Time, marker string) []schema.TraceRow {
	rows := make([]schema.TraceRow, 0, fullReadMarkerRows)
	for i := 0; i < fullReadMarkerRows; i++ {
		start := base.Add(-time.Duration(i+1) * time.Millisecond)
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: start.Add(time.Millisecond).UnixNano(),
			StartTimeUnixNano: start.UnixNano(),
			TraceID:           fmt.Sprintf("%016x%016x", base.UnixNano(), i),
			SpanID:            fmt.Sprintf("%016x", i),
			SpanName:          marker,
			ServiceName:       marker + "-svc",
			DurationNs:        int64(time.Millisecond),
			SpanKind:          2,
			Stream:            fmt.Sprintf(`{name=%q,resource_attr:service.name=%q}`, marker, marker+"-svc"),
		})
	}
	return rows
}

// run executes queryStr for tenant 0:0 over the e2e window and returns the
// number of rows emitted and how many of them carry the marker span name.
func (f *fullReadFixture) run(t *testing.T, queryStr string) (rows, markers int) {
	t.Helper()
	startNs := f.ingestAt.Add(-15 * time.Minute).UnixNano()
	endNs := f.ingestAt.Add(5 * time.Minute).UnixNano()
	q, err := logstorage.ParseQueryAtTimestamp(queryStr, endNs)
	if err != nil {
		t.Fatalf("parse %q: %v", queryStr, err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), startNs, endNs)
	var mu sync.Mutex
	err = f.s.RunQuery(context.Background(), []logstorage.TenantID{{}}, q, func(_ uint, db *logstorage.DataBlock) {
		if db == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		rows += db.RowsCount()
		for _, v := range tsColumnValues(db, "name") {
			if v == f.marker {
				markers++
			}
		}
	})
	if err != nil {
		t.Fatalf("RunQuery %q: %v", queryStr, err)
	}
	return rows, markers
}

// TestFullRowRead_AfterFooterPrefetch is the regression for the silent empty
// answer: every full-row query shape must return the object's matching rows
// although RunQuery cached a footer-only handle for it first.
func TestFullRowRead_AfterFooterPrefetch(t *testing.T) {
	f := newFullReadFixture(t)
	for _, tc := range []struct {
		name, query       string
		wantRows, wantMkr int
	}{
		{"bare exact filter", fmt.Sprintf(`name:=%q`, f.marker), fullReadMarkerRows, fullReadMarkerRows},
		{"limit rewrite", fmt.Sprintf(`name:=%q | sort by (_time) desc | offset 0 | limit 1000`, f.marker), fullReadMarkerRows, fullReadMarkerRows},
		{"anchored regex", fmt.Sprintf(`name:~%q`, "^"+f.marker+"$"), fullReadMarkerRows, fullReadMarkerRows},
		{"every row", `*`, f.total, fullReadMarkerRows},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, markers := f.run(t, tc.query)
			if rows != tc.wantRows || markers != tc.wantMkr {
				t.Fatalf("%q: got %d rows (%d marker rows), want %d rows (%d marker rows)", tc.query, rows, markers, tc.wantRows, tc.wantMkr)
			}
		})
	}
	// The scenario is only real if the query path cached the footer before
	// the scan; guard the fixture so a future prefetch change cannot turn this
	// into a test of the uncached path.
	if !f.s.footerCache.Has(f.file.Key) {
		t.Fatal("footer cache holds no entry for the object after the queries — the prefetched-footer path was not exercised")
	}
}

// TestFullRowRead_MatchesProjectedRead pins the invariant the defect broke:
// for the same object and filter, a full-row read and a projected read agree
// on the matching rows.
func TestFullRowRead_MatchesProjectedRead(t *testing.T) {
	f := newFullReadFixture(t)
	_, full := f.run(t, fmt.Sprintf(`name:=%q`, f.marker))
	_, projected := f.run(t, fmt.Sprintf(`name:=%q | fields name, _time`, f.marker))
	if full != projected || full != fullReadMarkerRows {
		t.Fatalf("full-row read found %d marker rows, projected read %d; want both %d", full, projected, fullReadMarkerRows)
	}
}

// TestOpenParquet_FullDownloadNeverReusesCachedHandle pins the open contract
// directly: with a footer-only entry in the cache, the full-download path
// returns a fresh handle over the downloaded bytes that decodes every row.
func TestOpenParquet_FullDownloadNeverReusesCachedHandle(t *testing.T) {
	f := newFullReadFixture(t)
	if n := prefetchFooters(context.Background(), f.s.pool, []manifest.FileInfo{f.file}, f.s.footerCache, 0, f.s.footerPrefetchBytes()); n != 1 {
		t.Fatalf("prefetchFooters cached %d footers, want 1", n)
	}
	cached, ok := f.s.footerCache.Get(f.file.Key)
	if !ok {
		t.Fatal("footer cache has no entry after prefetchFooters")
	}

	pf, planned, err := f.s.openParquetFileWithPlan(context.Background(), f.file, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if planned != nil {
		t.Fatal("a full-row open must not return a planned range view")
	}
	if pf == cached.File {
		t.Fatal("full-download open returned the cached footer-only handle")
	}

	var decoded, inWindow int
	lo, hi := f.ingestAt.Add(-15*time.Minute).UnixNano(), f.ingestAt.Add(5*time.Minute).UnixNano()
	for _, rg := range pf.RowGroups() {
		r := parquet.NewGenericRowGroupReader[schema.TraceRow](rg)
		buf := make([]schema.TraceRow, 256)
		for {
			n, rerr := r.Read(buf)
			for i := 0; i < n; i++ {
				decoded++
				if ts := buf[i].TimestampUnixNano; ts >= lo && ts <= hi {
					inWindow++
				}
			}
			if rerr != nil {
				break
			}
		}
	}
	if decoded != f.total || inWindow != f.total {
		t.Fatalf("decoded %d rows (%d inside the window), want %d rows all inside it", decoded, inWindow, f.total)
	}
}

// TestFullRowRead_ConcurrentQueriesAgree runs full-row reads of the same object
// in parallel (a shared handle would race under -race and could corrupt
// decoder state) and requires every one to see all marker rows.
func TestFullRowRead_ConcurrentQueriesAgree(t *testing.T) {
	f := newFullReadFixture(t)
	query := fmt.Sprintf(`name:=%q`, f.marker)
	const workers = 6
	results := make([]int, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = f.run(t, query)
		}(i)
	}
	wg.Wait()
	for i, got := range results {
		if got != fullReadMarkerRows {
			t.Errorf("worker %d: got %d marker rows, want %d", i, got, fullReadMarkerRows)
		}
	}
}
