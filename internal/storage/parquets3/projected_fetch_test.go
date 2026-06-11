package parquets3

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

// ---------------------------------------------------------------------------
// rangeLoggingS3Server: a mock S3 that records EVERY request's byte range so
// tests can assert exactly which ranges a read path touched — the request-log
// regression guard for the plan-then-fetch path (no speculative window pulls).
// ---------------------------------------------------------------------------

type loggedRange struct {
	key  string
	off  int64
	len  int64
	full bool // no Range header — whole-object GET
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

// makeMultiRGFullLogParquet writes an 8-column logs parquet with several
// row groups (maxRowsPerRG bounds each) and at least minBytes of
// ZSTD-resistant payload, so the plan covers multiple row groups and the
// projected chunks are a small fraction of the file.
func makeMultiRGFullLogParquet(t *testing.T, baseTime time.Time, minBytes, maxRowsPerRG int) []byte {
	t.Helper()
	var rows []fullLogRow
	i := 0
	for {
		rows = append(rows, fullLogRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			Body:              fmt.Sprintf("row-%d-payload-%x-%x-%x-%x-%x", i, i*2654435761, i*1442695040, i*8675309, i*0xdeadbeef, i*0xbadf00d),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       fmt.Sprintf("service-%d", i%16),
			Stream:            fmt.Sprintf(`{svc="service-%d"}`, i%16),
			StreamID:          fmt.Sprintf("sid-%x", i),
			TraceID:           fmt.Sprintf("trace-%016x", i),
			SpanID:            fmt.Sprintf("span-%016x", i),
		})
		i++
		if i%500 == 0 {
			var buf bytes.Buffer
			w := parquet.NewGenericWriter[fullLogRow](&buf,
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
// filter-referenced field (which includes _time from the query's time
// filter) — used by the request-log test to compute the EXPECTED planned
// ranges independently of the implementation.
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

// TestPlanProjectedRanges_Derivation pins the plan derivation against the
// parquet footer: per projected column per row group it must cover the
// chunk's pages FROM THE DICTIONARY PAGE and include the page-index
// sections; columns absent from planCols contribute NOTHING (the
// absent-value assert), and out-of-bounds row-group ordinals are ignored.
func TestPlanProjectedRanges_Derivation(t *testing.T) {
	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGFullLogParquet(t, baseTime, 256*1024, 400)
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

	planCols := map[string]bool{"service.name": true, "severity_text": true}
	ranges := planProjectedRanges(f, allRGs, planCols)
	if len(ranges) == 0 {
		t.Fatal("expected non-empty plan")
	}

	// Rebuild the expectation straight from the footer.
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

	// Absent column → contributes nothing; unknown column name → empty plan.
	if got := planProjectedRanges(f, allRGs, map[string]bool{"no_such_column": true}); len(got) != 0 {
		t.Fatalf("plan for absent column must be empty, got %v", got)
	}
	// Out-of-bounds row-group ordinals are skipped, not a panic.
	if got := planProjectedRanges(f, []int{-1, numRGs + 5}, planCols); len(got) != 0 {
		t.Fatalf("plan for out-of-bounds RGs must be empty, got %v", got)
	}
}

// TestGetFieldValues_PlannedFetch_OnlyPlannedRangesRequested is THE
// regression test for the measured ~46 MB/query of never-read window bytes
// on column-projected reads: with projected_fetch_mode=planned (default),
// every S3 range request issued by a projected GetFieldValues scan must be
// either (a) the parquet open's head-probe/footer-tail read, or (b) inside
// one of the COALESCED PLANNED SPANS derived from the footer — never a
// speculative read-ahead window pull.
func TestGetFieldValues_PlannedFetch_OnlyPlannedRangesRequested(t *testing.T) {
	mock := newRangeLoggingS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	// planned mode is opt-in since the live-bench verdict (default=window);
	// this test exercises the planned path explicitly. S* is forced to 1 so
	// these (deliberately small) cold-footer fixtures take the
	// plan-cold-footer rung, not the whole-file warmup — the warmup routing
	// has its own test (TestPlannedOpen_SStarRouting).
	s.cfg.S3.ProjectedFetchMode = config.ProjectedFetchModePlanned
	s.cfg.S3.WholeFileThresholdBytes = 1

	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGFullLogParquet(t, baseTime, 400*1024, 600)
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
	}

	fetchesBefore := metrics.S3PlannedFetchesTotal.Get()
	plannedBytesBefore := metrics.S3PlannedFetchBytesTotal.Get()

	q := mustParseQueryWithTime(t, `level:="INFO"`,
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

	// Compute the ALLOWED regions exactly as the implementation does: the
	// coalesced plan for (target + filter) columns over every row group,
	// priced by the same gap discipline (slice 1c) the fetch uses.
	fLocal, err := parquet.OpenFile(bytes.NewReader(data), size)
	if err != nil {
		t.Fatal(err)
	}
	allRGs := make([]int, len(fLocal.Metadata().RowGroups))
	for i := range allRGs {
		allRGs[i] = i
	}
	planCols := fieldValuesPlanCols(s, "service.name", q)
	pricer := s3reader.NewPlannedFetchReaderAt(&staticReaderAt{size: size}, size, 0, nil)
	planRanges := planProjectedRanges(fLocal, allRGs, planCols)
	bestGap, _ := choosePlannedGap(pricer, planRanges, s.cfg.S3.PlannedFetchMaxInflight)
	allowedSpans, planTotal := pricer.PlanRangesAt(planRanges, bestGap)
	if len(allowedSpans) == 0 {
		t.Fatal("expected a non-empty expected plan")
	}

	// Footer-tail allowance: open probe (<=8B at 0), the optimistic
	// footer-tail read (<= max(64KB, size/4)), and the 64KB inline footer
	// prefetch all live at the object's tail.
	tailAllowance := max64(s.footerPrefetchBytes(), max64(64<<10, size/4)) + 16

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

	// Bytes-on-wire sanity: everything served must stay well under the
	// full bodies (the old waste shape). Honest per-file accounting:
	// the coalesced plan + the 64KB inline footer prefetch + the open's
	// optimistic footer-tail read (ReadBufferSize, clamped to size/4 —
	// the open re-reads the tail it can't get from the footer cache;
	// absorbing it is Tier-2 item 7, out of scope here).
	readBufCap := max64(64<<10, size/4)
	maxAllowed := numFiles * (planTotal + s.footerPrefetchBytes() + readBufCap + 8192)
	if served := mock.bytesServed(); served > maxAllowed {
		t.Errorf("served %d bytes, want <= %d (plan + footer reads per file)", served, maxAllowed)
	} else {
		t.Logf("served %d bytes total (cap %d; full bodies would be %d)", served, maxAllowed, numFiles*int(size))
	}
	if got := metrics.S3PlannedFetchBytesTotal.Get(); got-plannedBytesBefore != uint64(numFiles*planTotal) {
		t.Logf("note: planned bytes metric delta = %d (expected %d)", got-plannedBytesBefore, numFiles*planTotal)
	}
}

// staticReaderAt is a size-only ReaderAtSizer used to price plans in tests.
type staticReaderAt struct{ size int64 }

func (r *staticReaderAt) ReadAt(p []byte, off int64) (int, error) { return 0, io.EOF }
func (r *staticReaderAt) Size() int64                             { return r.size }

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

// newPlannedEquivStorage builds a storage over its own request-logging mock
// with production-shaped window knobs, numFiles copies of data registered,
// and the given projected-fetch mode.
func newPlannedEquivStorage(t *testing.T, data []byte, numFiles int, baseTime time.Time, mode string) (*Storage, *rangeLoggingS3Server) {
	t.Helper()
	mock := newRangeLoggingS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.S3.ReadAheadBytes = 2 << 20
	s.cfg.S3.ReadAheadMaxBytes = 8 << 20
	s.cfg.S3.CoalesceGapBytes = 1 << 20
	s.cfg.S3.ProjectedFetchMode = mode
	// S* = 1: the equivalence suite pins span-fetch vs window behavior on
	// small fixtures; the S* whole-file warmup has its own routing test.
	s.cfg.S3.WholeFileThresholdBytes = 1
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("logs/dt=2026-06-01/hour=%02d/file%d.parquet", 10+i, i)
		mock.putFile(key, data)
		s.manifest.AddFile(fmt.Sprintf("dt=2026-06-01/hour=%02d", 10+i), manifest.FileInfo{
			Key:       key,
			Size:      int64(len(data)),
			MinTimeNs: baseTime.Add(time.Duration(i)*time.Hour - time.Minute).UnixNano(),
			MaxTimeNs: baseTime.Add(time.Duration(i)*time.Hour + time.Minute).UnixNano(),
			RowCount:  1, // force the scan path, not manifest-only answers
		})
	}
	return s, mock
}

// TestRunQuery_PlannedVsWindow_Equivalence runs the same filtered,
// projected query through both projected-fetch modes and requires
// IDENTICAL emitted content — the planned path may only change HOW bytes
// are fetched, never WHAT the query returns. It also pins the
// bytes-on-wire win (planned strictly below the window path).
func TestRunQuery_PlannedVsWindow_Equivalence(t *testing.T) {
	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGFullLogParquet(t, baseTime, 400*1024, 600)
	const numFiles = 3

	sPlanned, mockPlanned := newPlannedEquivStorage(t, data, numFiles, baseTime, config.ProjectedFetchModePlanned)
	sWindow, mockWindow := newPlannedEquivStorage(t, data, numFiles, baseTime, config.ProjectedFetchModeWindow)

	startNs := baseTime.Add(-time.Hour).UnixNano()
	endNs := baseTime.Add(10 * time.Hour).UnixNano()
	queryStr := `level:="INFO" | stats by (service.name) count()`

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

	// GetFieldValues equivalence over the same two storages.
	q := mustParseQueryWithTime(t, `level:="INFO"`, startNs, endNs)
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
	// Order-insensitive: ties in Hits make the result order run-dependent.
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

// TestPlannedFetch_PlanCapRetired_NoCapFallback pins the slice-1b cap
// re-scope BEHAVIOR CHANGE: the old per-PLAN knob (projected_fetch_max_bytes,
// set to 1 byte — under the old code every plan exceeded it and fell back
// with reason="cap") is now deprecated and ignored. The plan must still be
// ARMED, results must match window mode, and the cap fallback counter must
// NOT move (absent-value assert) — plans are admitted via the memory
// ledger and bounded per-SPAN instead.
func TestPlannedFetch_PlanCapRetired_NoCapFallback(t *testing.T) {
	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGFullLogParquet(t, baseTime, 400*1024, 600)
	const numFiles = 2

	sPlanned, _ := newPlannedEquivStorage(t, data, numFiles, baseTime, config.ProjectedFetchModePlanned)
	sPlanned.cfg.S3.ProjectedFetchMaxBytes = 1 // the OLD per-plan cap: now a no-op
	sWindow, _ := newPlannedEquivStorage(t, data, numFiles, baseTime, config.ProjectedFetchModeWindow)

	capBefore := metrics.S3ProjectedFetchFallback.Get("cap")
	fetchesBefore := metrics.S3PlannedFetchesTotal.Get()

	startNs := baseTime.Add(-time.Hour).UnixNano()
	endNs := baseTime.Add(10 * time.Hour).UnixNano()
	queryStr := `level:="ERROR" | stats by (service.name) count()`

	gotPlanned := runFilteredQuery(t, sPlanned, mustParseQueryWithTime(t, queryStr, startNs, endNs))
	gotWindow := runFilteredQuery(t, sWindow, mustParseQueryWithTime(t, queryStr, startNs, endNs))

	if len(gotPlanned) == 0 {
		t.Fatal("planned query returned no rows")
	}
	if len(gotPlanned) != len(gotWindow) {
		t.Fatalf("content mismatch: %d vs %d pairs", len(gotPlanned), len(gotWindow))
	}
	for k, v := range gotWindow {
		if gotPlanned[k] != v {
			t.Fatalf("content mismatch at %q: planned=%d window=%d", k, gotPlanned[k], v)
		}
	}

	if got := metrics.S3ProjectedFetchFallback.Get("cap"); got != capBefore {
		t.Errorf("cap fallback ticked (%d -> %d) — the per-plan cap must be retired", capBefore, got)
	}
	if got := metrics.S3PlannedFetchesTotal.Get(); got <= fetchesBefore {
		t.Errorf("no plan was armed (fetches %d -> %d) — plans whose total exceeds the old cap must still arm", fetchesBefore, got)
	}
}

// TestChoosePlannedGap_EachCandidateCanWin is the slice-1c gap-discipline
// regression: synthetic range sets where EACH candidate gap is the
// strict-cost winner under the documented model
// (cost = ceil(spans/k)*100ms + bytes/50MBps, k=16).
func TestChoosePlannedGap_EachCandidateCanWin(t *testing.T) {
	const fileSize = 256 << 20 // big file: no gap clamping in play
	view := s3reader.NewPlannedFetchReaderAt(&staticReaderAt{size: fileSize}, fileSize, 1<<20, nil)

	mk := func(n int, stride, length int64) []s3reader.Range {
		var rs []s3reader.Range
		for i := 0; i < n; i++ {
			rs = append(rs, s3reader.Range{Off: int64(i) * stride, Len: length})
		}
		return rs
	}

	// 64KB wins: 10 ranges 200KB apart — every candidate fits one 16-wide
	// RTT wave (10 spans at 64KB; 1 span at 256KB/1MB), so the smallest
	// bytes win: the bigger gaps pay 9 x ~136KB of merged gap for ZERO
	// fewer waves.
	if gap, _ := choosePlannedGap(view, mk(10, 200<<10, 64<<10), 16); gap != 64<<10 {
		t.Errorf("sparse one-wave set: chose gap %d, want 64KB", gap)
	}

	// 256KB wins: 17 ranges 192KB apart (128KB gaps) — at 64KB gap nothing
	// merges (17 spans = 2 waves); at 256KB they merge to 1 span (1 wave)
	// for 16 x 128KB = 2MB of gap bytes (0.04s << the 0.1s wave saved).
	// 1MB merges identically but iterates later — the smaller-gap
	// tie-break keeps 256KB.
	if gap, _ := choosePlannedGap(view, mk(17, 192<<10, 64<<10), 16); gap != 256<<10 {
		t.Errorf("two-wave 192KB-stride set: chose gap %d, want 256KB", gap)
	}

	// 1MB wins: 17 ranges 344KB apart — the 280KB gaps merge ONLY at the
	// 1MB candidate (64KB/256KB keep 17 spans = 2 waves); merging buys a
	// wave (0.1s) for 16 x 280KB = 4.48MB = 0.0896s of extra bytes.
	if gap, _ := choosePlannedGap(view, mk(17, 344<<10, 64<<10), 16); gap != 1<<20 {
		t.Errorf("two-wave 344KB-stride set: chose gap %d, want 1MB", gap)
	}

	// Absent-value: k <= 0 resolves to the default 16, not a panic/zero.
	if gap, _ := choosePlannedGap(view, mk(10, 200<<10, 64<<10), 0); gap != 64<<10 {
		t.Errorf("k=0 must price with the default k, got gap %d", gap)
	}
}

// TestPlannedOpen_SStarRouting pins the slice-1d footer-cache-gated
// strategy ladder on the PLANNED projected-read path, with absent-value
// asserts on the fallback counters at every rung:
//
//	cold footer + size < S*  → ONE whole-file GET (the warmup: the footer
//	                           cache holds the key afterwards), strategy
//	                           "whole-file-warmup", NO fallback ticks;
//	cold footer + size >= S* → footer tail fetch + armed plan, strategy
//	                           "plan-cold-footer", NO no-footer tick;
//	warm footer              → armed plan, strategy "plan-warm-footer".
func TestPlannedOpen_SStarRouting(t *testing.T) {
	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGFullLogParquet(t, baseTime, 400*1024, 600)
	size := int64(len(data))
	projected := map[string]bool{"service.name": true}

	snap := func() map[string]uint64 {
		return map[string]uint64{
			"cap":       metrics.S3ProjectedFetchFallback.Get("cap"),
			"no-footer": metrics.S3ProjectedFetchFallback.Get("no-footer"),
			"error":     metrics.S3ProjectedFetchFallback.Get("error"),
		}
	}
	assertNoFallbackTicks := func(t *testing.T, before map[string]uint64, rung string) {
		t.Helper()
		after := snap()
		for reason, v := range before {
			if after[reason] != v {
				t.Errorf("%s: fallback{reason=%q} ticked (%d -> %d)", rung, reason, v, after[reason])
			}
		}
	}

	// Rung 1: cold footer, file BELOW S* → whole-file warmup.
	{
		mock := newRangeLoggingS3Server()
		defer mock.close()
		s := testStorageWithS3(t, mock.url())
		s.cfg.S3.ProjectedFetchMode = config.ProjectedFetchModePlanned
		s.cfg.S3.WholeFileThresholdBytes = int(size + 1) // file is under S*
		key := "logs/dt=2026-06-01/hour=10/warmup.parquet"
		mock.putFile(key, data)
		fi := manifest.FileInfo{Key: key, Size: size}

		before := snap()
		warmupsBefore := metrics.S3PlannedStrategy.Get("whole-file-warmup")
		f, view, err := s.openParquetFileWithPlan(context.Background(), fi, projected)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if view != nil {
			t.Error("whole-file warmup must not return a planned view")
			_ = view.Close()
		}
		if f == nil {
			t.Fatal("nil parquet file")
		}
		if got := metrics.S3PlannedStrategy.Get("whole-file-warmup"); got != warmupsBefore+1 {
			t.Errorf("whole-file-warmup strategy counter = %d, want %d", got, warmupsBefore+1)
		}
		var full int
		for _, r := range mock.requests() {
			if r.full {
				full++
			}
		}
		if full != 1 {
			t.Errorf("whole-file warmup issued %d full-object GETs, want exactly 1", full)
		}
		if !s.footerCache.Has(key) {
			t.Error("the whole-file download must warm the footer cache (the download IS the warmup)")
		}
		assertNoFallbackTicks(t, before, "whole-file-warmup")

		// The NEXT open of the same file takes the warm-footer plan route.
		warmBefore := metrics.S3PlannedStrategy.Get("plan-warm-footer")
		_, view2, err := s.openParquetFileWithPlan(context.Background(), fi, projected)
		if err != nil {
			t.Fatalf("second open: %v", err)
		}
		if view2 == nil {
			t.Error("warm-footer open must return a planned view")
		} else {
			_ = view2.Close()
		}
		if got := metrics.S3PlannedStrategy.Get("plan-warm-footer"); got != warmBefore+1 {
			t.Errorf("plan-warm-footer strategy counter = %d, want %d", got, warmBefore+1)
		}
	}

	// Rung 2: cold footer, file AT/ABOVE S* → footer fetch + plan; the
	// no-footer fallback must NOT tick (absent-value).
	{
		mock := newRangeLoggingS3Server()
		defer mock.close()
		s := testStorageWithS3(t, mock.url())
		s.cfg.S3.ProjectedFetchMode = config.ProjectedFetchModePlanned
		s.cfg.S3.WholeFileThresholdBytes = int(size) // file is >= S*
		key := "logs/dt=2026-06-01/hour=11/coldplan.parquet"
		mock.putFile(key, data)
		fi := manifest.FileInfo{Key: key, Size: size}

		before := snap()
		coldBefore := metrics.S3PlannedStrategy.Get("plan-cold-footer")
		_, view, err := s.openParquetFileWithPlan(context.Background(), fi, projected)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if view == nil {
			t.Error("cold-footer plan route must return a planned view")
		} else {
			_ = view.Close()
		}
		if got := metrics.S3PlannedStrategy.Get("plan-cold-footer"); got != coldBefore+1 {
			t.Errorf("plan-cold-footer strategy counter = %d, want %d", got, coldBefore+1)
		}
		for _, r := range mock.requests() {
			if r.full {
				t.Errorf("cold-footer plan route must not download the body (full GET of %s)", r.key)
			}
		}
		if !s.footerCache.Has(key) {
			t.Error("the footer fetch must populate the footer cache")
		}
		assertNoFallbackTicks(t, before, "plan-cold-footer")
	}
}

// TestFooterPrefetch_OversizeFooterFitsPerSignalDefault is the slice-0a
// regression for the traces-L2-footers-always-full-download bug-class: a
// footer LARGER than the old shared 64KB constant but under the new
// per-signal default must be prefetched and cached in one pass — and the
// planned open must then take the warm-footer plan route WITHOUT the
// no-footer fallback ticking (absent-value assert).
func TestFooterPrefetch_OversizeFooterFitsPerSignalDefault(t *testing.T) {
	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	// Pad the footer over 64KB via file-level key-value metadata (the same
	// mechanism that blows real traces footers up: the trace index lives
	// in footer KV). 80KB of KV puts the footer in (64KB, prefetch-default)
	// — over the old constant, under the per-signal default. The file is
	// grown past 8x the prefetch size so the footerPrefetchTail size clamp
	// (max(64KB, size/8)) does not bind — matching the live geometry,
	// where oversized footers ride large compacted files.
	data := makeMultiRGFullLogParquetWithKV(t, baseTime, 1100*1024, 2000, strings.Repeat("x", 70<<10))
	size := int64(len(data))

	// Confirm the fixture: footer length really exceeds the old 64KB.
	footerLen, err := FooterLength(data[len(data)-8:])
	if err != nil {
		t.Fatal(err)
	}
	if footerLen <= 64<<10 {
		t.Fatalf("fixture footer = %d bytes, want > 64KB to exercise the bug-class", footerLen)
	}
	if int64(footerLen+8) > defaultFooterPrefetchBytes {
		t.Fatalf("fixture footer = %d bytes, must fit the per-signal default %d", footerLen, defaultFooterPrefetchBytes)
	}

	mock := newRangeLoggingS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.S3.ProjectedFetchMode = config.ProjectedFetchModePlanned
	key := "logs/dt=2026-06-01/hour=10/bigfooter.parquet"
	mock.putFile(key, data)
	fi := manifest.FileInfo{Key: key, Size: size}

	// One prefetch pass caches the footer (no too_big second class).
	if got := prefetchFooters(context.Background(), s.pool, []manifest.FileInfo{fi}, s.footerCache, 1, s.footerPrefetchBytes()); got != 1 {
		t.Fatalf("prefetchFooters cached %d footers, want 1 (footer no longer over the prefetch size)", got)
	}
	if !s.footerCache.Has(key) {
		t.Fatal("footer not in cache after prefetch")
	}

	// The planned open now takes the warm plan route: no full-object GET,
	// and the no-footer fallback counter does NOT tick.
	noFooterBefore := metrics.S3ProjectedFetchFallback.Get("no-footer")
	_, view, err := s.openParquetFileWithPlan(context.Background(), fi, map[string]bool{"service.name": true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if view == nil {
		t.Error("expected a planned view over the prefetched footer")
	} else {
		_ = view.Close()
	}
	if got := metrics.S3ProjectedFetchFallback.Get("no-footer"); got != noFooterBefore {
		t.Errorf("fallback{reason=no-footer} ticked (%d -> %d) — the prefetched footer must serve the plan", noFooterBefore, got)
	}
	for _, r := range mock.requests() {
		if r.full {
			t.Errorf("full-object GET of %s — the oversize-footer bug-class regressed", r.key)
		}
	}
}

// makeMultiRGFullLogParquetWithKV is makeMultiRGFullLogParquet plus a
// file-level key-value metadata pad — the mechanism that inflates real
// traces footers (trace index in footer KV).
func makeMultiRGFullLogParquetWithKV(t *testing.T, baseTime time.Time, minBytes, maxRowsPerRG int, kvPad string) []byte {
	t.Helper()
	var rows []fullLogRow
	i := 0
	for {
		rows = append(rows, fullLogRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			Body:              fmt.Sprintf("row-%d-payload-%x-%x", i, i*2654435761, i*1442695040),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       fmt.Sprintf("service-%d", i%16),
			Stream:            fmt.Sprintf(`{svc="service-%d"}`, i%16),
			StreamID:          fmt.Sprintf("sid-%x", i),
			TraceID:           fmt.Sprintf("trace-%016x", i),
			SpanID:            fmt.Sprintf("span-%016x", i),
		})
		i++
		if i%500 == 0 {
			var buf bytes.Buffer
			w := parquet.NewGenericWriter[fullLogRow](&buf,
				parquet.Compression(&parquet.Zstd),
				parquet.MaxRowsPerRowGroup(int64(maxRowsPerRG)),
				parquet.KeyValueMetadata("test_footer_pad", kvPad),
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
