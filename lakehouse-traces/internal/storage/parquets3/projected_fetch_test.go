package parquets3

// Mirror of internal/storage/parquets3/projected_fetch_test.go — the
// plan-then-fetch (S3 Tier-2 items 8/9) regression suite for the traces
// module: only planned ranges on the wire, planned/window equivalence,
// and the cap fallback. Differences from the logs twin: an 8-column span
// row (so a 3-column projection stays under the 50% range-read gate) and
// explicit footer-cache priming for the GetFieldValues path (the traces
// module has no inline footer fetch).

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

type loggedRange struct {
	key  string
	off  int64
	len  int64
	full bool
}

type rangeLoggingS3Server struct {
	mu    sync.Mutex
	files map[string][]byte
	log   []loggedRange
	srv   *httptest.Server
}

func newRangeLoggingS3Server() *rangeLoggingS3Server {
	m := &rangeLoggingS3Server{files: make(map[string][]byte)}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *rangeLoggingS3Server) putFile(key string, data []byte) {
	m.mu.Lock()
	m.files[key] = data
	m.mu.Unlock()
}

func (m *rangeLoggingS3Server) requests() []loggedRange {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]loggedRange, len(m.log))
	copy(out, m.log)
	return out
}

func (m *rangeLoggingS3Server) bytesServed() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, r := range m.log {
		n += r.len
	}
	return n
}

func (m *rangeLoggingS3Server) handler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		if r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	key := parts[1]
	if r.Method == http.MethodPut {
		data, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		m.putFile(key, data)
		w.WriteHeader(http.StatusOK)
		return
	}
	m.mu.Lock()
	data, ok := m.files[key]
	m.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	rangeHdr := r.Header.Get("Range")
	if strings.HasPrefix(rangeHdr, "bytes=") {
		bounds := strings.SplitN(strings.TrimPrefix(rangeHdr, "bytes="), "-", 2)
		start, _ := strconv.ParseInt(bounds[0], 10, 64)
		end, _ := strconv.ParseInt(bounds[1], 10, 64)
		if start >= int64(len(data)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		m.mu.Lock()
		m.log = append(m.log, loggedRange{key: key, off: start, len: end - start + 1})
		m.mu.Unlock()
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(int(end-start+1)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
		return
	}
	m.mu.Lock()
	m.log = append(m.log, loggedRange{key: key, off: 0, len: int64(len(data)), full: true})
	m.mu.Unlock()
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (m *rangeLoggingS3Server) close()      { m.srv.Close() }
func (m *rangeLoggingS3Server) url() string { return m.srv.URL }

// plannedSpanRow is an 8-column span-shaped schema so a 3-column
// projection (37.5%) stays under the 50% range-read gate.
type plannedSpanRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	TraceID           string `parquet:"trace_id"`
	SpanID            string `parquet:"span_id"`
	Name              string `parquet:"span.name"`
	ServiceName       string `parquet:"service.name"`
	DurationNs        int64  `parquet:"duration_ns"`
	Stream            string `parquet:"_stream"`
	StatusMessage     string `parquet:"status.message"`
}

// makeMultiRGSpanParquet writes a multi-row-group span parquet of at least
// minBytes with ZSTD-resistant payload.
func makeMultiRGSpanParquet(t *testing.T, baseTime time.Time, minBytes, maxRowsPerRG int) []byte {
	t.Helper()
	var rows []plannedSpanRow
	i := 0
	for {
		rows = append(rows, plannedSpanRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			TraceID:           fmt.Sprintf("trace-%032x", i),
			SpanID:            fmt.Sprintf("span-%016x", i),
			Name:              fmt.Sprintf("op-%d", i%4),
			ServiceName:       fmt.Sprintf("service-%d", i%16),
			DurationNs:        int64(i%1000) * 1_000_000,
			Stream:            fmt.Sprintf(`{svc="service-%d"}`, i%16),
			StatusMessage:     fmt.Sprintf("status-%d-detail-%x-%x-%x", i, i*2654435761, i*1442695040, i*0xdeadbeef),
		})
		i++
		if i%500 == 0 {
			var buf bytes.Buffer
			w := parquet.NewGenericWriter[plannedSpanRow](&buf,
				parquet.Compression(&parquet.Zstd),
				parquet.MaxRowsPerRowGroup(int64(maxRowsPerRG)),
			)
			if _, err := w.Write(rows); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if buf.Len() >= minBytes {
				return buf.Bytes()
			}
		}
		if i > 100000 {
			t.Fatal("could not grow test parquet to minBytes")
		}
	}
}

// fieldValuesPlanCols mirrors scanProjectedFieldValues' projection for a
// GetFieldValues(target, q) call — target column plus every
// filter-referenced field (including _time from the query's time filter).
func fieldValuesPlanCols(s *Storage, target string, q *logstorage.Query) map[string]bool {
	cols := map[string]bool{}
	if m := s.registry.ResolveToParquet(target); m != nil {
		cols[m.ParquetColumn] = true
	} else if m := s.registry.ResolveFromParquet(target); m != nil {
		cols[m.ParquetColumn] = true
	} else {
		cols[target] = true
	}
	for internalName := range FilterReferencedFields(parseFilterFromQuery(q)) {
		if m := s.registry.ResolveToParquet(internalName); m != nil {
			cols[m.ParquetColumn] = true
		} else {
			cols[internalName] = true
		}
	}
	return cols
}

// staticReaderAt is a size-only ReaderAtSizer used to price plans in tests.
type staticReaderAt struct{ size int64 }

func (r *staticReaderAt) ReadAt(p []byte, off int64) (int, error) { return 0, io.EOF }
func (r *staticReaderAt) Size() int64                             { return r.size }

// TestPlanProjectedRanges_Derivation_Traces pins the plan derivation
// against the parquet footer (dictionary pages + page-index sections
// included; absent columns contribute nothing).
func TestPlanProjectedRanges_Derivation_Traces(t *testing.T) {
	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGSpanParquet(t, baseTime, 256*1024, 400)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	meta := f.Metadata()
	numRGs := len(meta.RowGroups)
	if numRGs < 2 {
		t.Fatalf("fixture must have >= 2 row groups, got %d", numRGs)
	}
	allRGs := make([]int, numRGs)
	for i := range allRGs {
		allRGs[i] = i
	}

	planCols := map[string]bool{"service.name": true, "span.name": true}
	ranges := planProjectedRanges(f, allRGs, planCols)
	if len(ranges) == 0 {
		t.Fatal("expected non-empty plan")
	}

	var want []s3reader.Range
	for _, rg := range meta.RowGroups {
		for ci := range rg.Columns {
			cc := &rg.Columns[ci]
			md := &cc.MetaData
			if !planCols[md.PathInSchema[0]] {
				continue
			}
			start := md.DataPageOffset
			if md.DictionaryPageOffset > 0 && md.DictionaryPageOffset < start {
				start = md.DictionaryPageOffset
			}
			want = append(want, s3reader.Range{Off: start, Len: md.TotalCompressedSize})
			if cc.ColumnIndexOffset > 0 && cc.ColumnIndexLength > 0 {
				want = append(want, s3reader.Range{Off: cc.ColumnIndexOffset, Len: int64(cc.ColumnIndexLength)})
			}
			if cc.OffsetIndexOffset > 0 && cc.OffsetIndexLength > 0 {
				want = append(want, s3reader.Range{Off: cc.OffsetIndexOffset, Len: int64(cc.OffsetIndexLength)})
			}
		}
	}
	if len(ranges) != len(want) {
		t.Fatalf("plan has %d ranges, want %d", len(ranges), len(want))
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("range[%d] = %+v, want %+v", i, ranges[i], want[i])
		}
	}

	if got := planProjectedRanges(f, allRGs, map[string]bool{"no_such_column": true}); len(got) != 0 {
		t.Fatalf("plan for absent column must be empty, got %v", got)
	}
	if got := planProjectedRanges(f, []int{-1, numRGs + 5}, planCols); len(got) != 0 {
		t.Fatalf("plan for out-of-bounds RGs must be empty, got %v", got)
	}
}

// TestGetFieldValues_PlannedFetch_OnlyPlannedRangesRequested_Traces is the
// traces twin of the request-log regression test: with the default planned
// mode every S3 range request of a projected GetFieldValues scan must be
// the open's head-probe/footer-tail read or inside a coalesced planned
// span — never a speculative window pull.
func TestGetFieldValues_PlannedFetch_OnlyPlannedRangesRequested_Traces(t *testing.T) {
	mock := newRangeLoggingS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	// planned mode is opt-in since the live-bench verdict (default=window).
	s.cfg.S3.ProjectedFetchMode = config.ProjectedFetchModePlanned

	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGSpanParquet(t, baseTime, 400*1024, 600)
	size := int64(len(data))

	const numFiles = 3
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("logs/dt=2026-06-01/hour=%02d/file%d.parquet", 10+i, i)
		mock.putFile(key, data)
		s.manifest.AddFile(fmt.Sprintf("dt=2026-06-01/hour=%02d", 10+i), manifest.FileInfo{
			Key:       key,
			Size:      size,
			MinTimeNs: baseTime.Add(time.Duration(i)*time.Hour - time.Minute).UnixNano(),
			MaxTimeNs: baseTime.Add(time.Duration(i)*time.Hour + time.Minute).UnixNano(),
		})
		// The traces module's projected open requires a footer-cache hit
		// (no inline footer fetch) — prime it the way prefetchFooters /
		// a previous query would have.
		cached, _, err := ParseFooterFromData(key, data)
		if err != nil {
			t.Fatalf("ParseFooterFromData: %v", err)
		}
		s.footerCache.Put(key, cached)
	}

	fetchesBefore := metrics.S3PlannedFetchesTotal.Get()

	q := mustParseQueryWithTime(t, `name:="op-1"`,
		baseTime.Add(-time.Hour).UnixNano(),
		baseTime.Add(10*time.Hour).UnixNano(),
	)
	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	if len(vals) == 0 {
		t.Fatal("expected non-empty values")
	}

	if got := metrics.S3PlannedFetchesTotal.Get(); got != fetchesBefore+numFiles {
		t.Errorf("planned fetches = %d, want %d (one armed plan per file)", got-fetchesBefore, numFiles)
	}

	fLocal, err := parquet.OpenFile(bytes.NewReader(data), size)
	if err != nil {
		t.Fatal(err)
	}
	allRGs := make([]int, len(fLocal.Metadata().RowGroups))
	for i := range allRGs {
		allRGs[i] = i
	}
	planCols := fieldValuesPlanCols(s, "service.name", q)
	effGap, _, _ := s.clampWindowKnobs(size)
	pricer := s3reader.NewPlannedFetchReaderAt(&staticReaderAt{size: size}, size, effGap, nil)
	allowedSpans, planTotal := pricer.PlanRanges(planProjectedRanges(fLocal, allRGs, planCols))
	if len(allowedSpans) == 0 {
		t.Fatal("expected a non-empty expected plan")
	}

	tailAllowance := max64(footerPrefetchSize, max64(64<<10, size/4)) + 16
	inSpans := func(off, ln int64) bool {
		for _, sp := range allowedSpans {
			if off >= sp.Off && off+ln <= sp.Off+sp.Len {
				return true
			}
		}
		return false
	}
	var planned, tail, head int
	for _, r := range mock.requests() {
		if r.full {
			t.Errorf("full-object GET for %s — projected read must never download the body", r.key)
			continue
		}
		switch {
		case r.off == 0 && r.len <= 8:
			head++
		case r.off >= size-tailAllowance:
			tail++
		case inSpans(r.off, r.len):
			planned++
		default:
			t.Errorf("S3 GET outside the plan: key=%s off=%d len=%d (a window pull?) — allowed spans %v",
				r.key, r.off, r.len, allowedSpans)
		}
	}
	t.Logf("requests: %d planned-span, %d footer/tail, %d head-probe; plan=%d spans / %d bytes per file",
		planned, tail, head, len(allowedSpans), planTotal)
	if planned == 0 {
		t.Error("no planned-span requests observed — the plan-then-fetch path did not engage")
	}

	readBufCap := max64(64<<10, size/4)
	maxAllowed := numFiles * (planTotal + footerPrefetchSize + readBufCap + 8192)
	if served := mock.bytesServed(); served > maxAllowed {
		t.Errorf("served %d bytes, want <= %d (plan + footer reads per file)", served, maxAllowed)
	} else {
		t.Logf("served %d bytes total (cap %d; full bodies would be %d)", served, maxAllowed, numFiles*int(size))
	}
}

// runFilteredQuery executes q against s and returns every emitted
// (column, value) pair count — a mode-agnostic content fingerprint.
func runFilteredQuery(t *testing.T, s *Storage, q *logstorage.Query) map[string]int {
	t.Helper()
	got := map[string]int{}
	var mu sync.Mutex
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range db.GetColumns(false) {
			for _, v := range c.Values {
				got[c.Name+"="+v]++
			}
		}
	}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	return got
}

func newPlannedEquivStorage(t *testing.T, data []byte, numFiles int, baseTime time.Time, mode string) (*Storage, *rangeLoggingS3Server) {
	t.Helper()
	mock := newRangeLoggingS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.S3.ReadAheadBytes = 2 << 20
	s.cfg.S3.ReadAheadMaxBytes = 8 << 20
	s.cfg.S3.CoalesceGapBytes = 1 << 20
	s.cfg.S3.ProjectedFetchMode = mode
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("logs/dt=2026-06-01/hour=%02d/file%d.parquet", 10+i, i)
		mock.putFile(key, data)
		s.manifest.AddFile(fmt.Sprintf("dt=2026-06-01/hour=%02d", 10+i), manifest.FileInfo{
			Key:       key,
			Size:      int64(len(data)),
			MinTimeNs: baseTime.Add(time.Duration(i)*time.Hour - time.Minute).UnixNano(),
			MaxTimeNs: baseTime.Add(time.Duration(i)*time.Hour + time.Minute).UnixNano(),
			RowCount:  1,
		})
	}
	return s, mock
}

// TestRunQuery_PlannedVsWindow_Equivalence_Traces: identical emitted
// content in both projected-fetch modes, with planned strictly cheaper on
// the wire.
func TestRunQuery_PlannedVsWindow_Equivalence_Traces(t *testing.T) {
	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGSpanParquet(t, baseTime, 400*1024, 600)
	const numFiles = 3

	sPlanned, mockPlanned := newPlannedEquivStorage(t, data, numFiles, baseTime, config.ProjectedFetchModePlanned)
	sWindow, mockWindow := newPlannedEquivStorage(t, data, numFiles, baseTime, config.ProjectedFetchModeWindow)

	startNs := baseTime.Add(-time.Hour).UnixNano()
	endNs := baseTime.Add(10 * time.Hour).UnixNano()
	queryStr := `name:="op-1" | stats by (service.name) count()`

	gotPlanned := runFilteredQuery(t, sPlanned, mustParseQueryWithTime(t, queryStr, startNs, endNs))
	gotWindow := runFilteredQuery(t, sWindow, mustParseQueryWithTime(t, queryStr, startNs, endNs))

	if len(gotPlanned) == 0 {
		t.Fatal("planned mode returned no rows")
	}
	if len(gotPlanned) != len(gotWindow) {
		t.Fatalf("content mismatch: planned %d distinct (col,value) pairs, window %d", len(gotPlanned), len(gotWindow))
	}
	for k, v := range gotWindow {
		if gotPlanned[k] != v {
			t.Fatalf("content mismatch at %q: planned=%d window=%d", k, gotPlanned[k], v)
		}
	}

	pb, wb := mockPlanned.bytesServed(), mockWindow.bytesServed()
	t.Logf("bytes on wire: planned=%d window=%d (%.1f%% reduction)", pb, wb, (1-float64(pb)/float64(wb))*100)
	if pb >= wb {
		t.Errorf("planned mode served %d bytes, want < window mode's %d", pb, wb)
	}

	// GetFieldValues equivalence (order-insensitive: Hits ties make the
	// result order run-dependent).
	q := mustParseQueryWithTime(t, `name:="op-1"`, startNs, endNs)
	vp, err := sPlanned.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues planned: %v", err)
	}
	vw, err := sWindow.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues window: %v", err)
	}
	if len(vp) == 0 || len(vp) != len(vw) {
		t.Fatalf("GetFieldValues mismatch: planned %d values, window %d", len(vp), len(vw))
	}
	asMap := func(vals []logstorage.ValueWithHits) map[string]uint64 {
		m := make(map[string]uint64, len(vals))
		for _, v := range vals {
			m[v.Value] = v.Hits
		}
		return m
	}
	mp, mw := asMap(vp), asMap(vw)
	for k, v := range mw {
		if mp[k] != v {
			t.Fatalf("GetFieldValues mismatch at %q: planned=%d window=%d", k, mp[k], v)
		}
	}
}

// TestPlannedFetch_CapFallbackToWindow_Traces: a 1-byte plan cap forces
// the window fallback — results identical, reason="cap" ticks, no plan
// armed.
func TestPlannedFetch_CapFallbackToWindow_Traces(t *testing.T) {
	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGSpanParquet(t, baseTime, 400*1024, 600)
	const numFiles = 2

	sCapped, _ := newPlannedEquivStorage(t, data, numFiles, baseTime, config.ProjectedFetchModePlanned)
	sCapped.cfg.S3.ProjectedFetchMaxBytes = 1
	sWindow, _ := newPlannedEquivStorage(t, data, numFiles, baseTime, config.ProjectedFetchModeWindow)

	capBefore := metrics.S3ProjectedFetchFallback.Get("cap")
	fetchesBefore := metrics.S3PlannedFetchesTotal.Get()

	startNs := baseTime.Add(-time.Hour).UnixNano()
	endNs := baseTime.Add(10 * time.Hour).UnixNano()
	queryStr := `name:="op-2" | stats by (service.name) count()`

	gotCapped := runFilteredQuery(t, sCapped, mustParseQueryWithTime(t, queryStr, startNs, endNs))
	gotWindow := runFilteredQuery(t, sWindow, mustParseQueryWithTime(t, queryStr, startNs, endNs))

	if len(gotCapped) == 0 {
		t.Fatal("cap-fallback query returned no rows")
	}
	if len(gotCapped) != len(gotWindow) {
		t.Fatalf("cap fallback content mismatch: %d vs %d pairs", len(gotCapped), len(gotWindow))
	}
	for k, v := range gotWindow {
		if gotCapped[k] != v {
			t.Fatalf("cap fallback mismatch at %q: capped=%d window=%d", k, gotCapped[k], v)
		}
	}

	if got := metrics.S3ProjectedFetchFallback.Get("cap"); got <= capBefore {
		t.Errorf("cap fallback counter did not move (before=%d after=%d)", capBefore, got)
	}
	if got := metrics.S3PlannedFetchesTotal.Get(); got != fetchesBefore {
		t.Errorf("a plan was armed despite the 1-byte cap (fetches %d -> %d)", fetchesBefore, got)
	}
}
