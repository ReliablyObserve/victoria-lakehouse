package parquets3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// BenchmarkFieldValues_Level measures an unfiltered whole-range
// `field_values?field=level` over the parity corpus shape: 24 hourly
// partitions of 400 rows, levels spread uniformly. pmeta=on is the default
// deployment (answered from the catalog); pmeta=off is the degraded mode
// (answered by a column-projected scan). window=whole covers every file;
// window=cut starts and ends mid-hour, so two files straddle the window and the
// scan reads their timestamp column to confine values to it. The label index is
// seeded by a first query, as in production.
func BenchmarkFieldValues_Level(b *testing.B) {
	base := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
	windows := []struct {
		name   string
		lo, hi time.Time
	}{
		{"whole", base.Add(-time.Hour), base.Add(25 * time.Hour)},
		{"cut", base.Add(30 * time.Minute), base.Add(23*time.Hour + 30*time.Minute)},
	}
	for _, pmetaOn := range []bool{true, false} {
		for _, w := range windows {
			benchFieldValuesLevel(b, pmetaOn, w.name, base, w.lo, w.hi)
		}
	}
}

func benchFieldValuesLevel(b *testing.B, pmetaOn bool, window string, base, lo, hi time.Time) {
	b.Run(fmt.Sprintf("pmeta=%v/window=%s", pmetaOn, window), func(b *testing.B) {
		mock := newMockS3Server()
		b.Cleanup(mock.close)
		s := testStorageWithS3(b, mock.url())
		bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
		if pmetaOn {
			s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
			s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
			bw.catalogObserver = &catalogObserver{store: s.catalog}
		}
		levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
		rows := make([]schema.LogRow, 0, 24*400)
		for h := 0; h < 24; h++ {
			for i := 0; i < 400; i++ {
				rows = append(rows, schema.LogRow{
					TimestampUnixNano: base.Add(time.Duration(h)*time.Hour + time.Duration(i)*time.Second).UnixNano(),
					Body:              "row", ServiceName: "svc", SeverityText: levels[(h+i)%len(levels)],
				})
			}
		}
		bw.AddLogRows(rows)
		bw.triggerFlush()

		q, err := logstorage.ParseQuery("*")
		if err != nil {
			b.Fatal(err)
		}
		q.AddTimeFilter(lo.UnixNano(), hi.UnixNano())
		if err := s.RunQuery(context.Background(), nil, q, func(uint, *logstorage.DataBlock) {}); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vals, err := s.GetFieldValues(context.Background(), nil, q, "level", 0)
			if err != nil {
				b.Fatal(err)
			}
			if len(vals) != len(levels) {
				b.Fatalf("got %d level values, want %d", len(vals), len(levels))
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Field-metadata performance matrix (docs/perf/field-metadata-cells.md).
//
// Cells: endpoint {field_values level, field_values service.name, field_names,
// streams} × pmeta {on, off} × layout {flushed small files, compacted by the
// real compactor, flushed but the last ten minutes still unflushed on a peer
// insert instance (served through the buffer bridge)} × window {whole hours,
// cut mid-hour, narrow 30 s, across the hour edge} × filter {none,
// service.name:="svc-a"} × S3 first-byte latency {0, 100 ms}.
//
// Every iteration starts with cold object caches (memCache + footerCache are
// replaced; the label index and the pmeta catalog are kept, as on a running
// node), and every answer is validated against the dataset's known truth:
//   - set_ok:  the exact value (or name) set of the rows in the window;
//   - hits_ok: set_ok AND every hit count equals the number of rows.
// Only set-valid iterations carry a latency; invalid ones are counted, not
// timed. The S3 mock counts GETs, body bytes, LISTs, and the distinct row
// groups and pages whose bytes a GET returned.
//
// Two entry points share the harness:
//   - BenchmarkFieldMetadata: go-bench view (custom metrics per cell);
//   - TestFieldMetadataMatrix: env-gated JSONL emitter used by
//     scripts/bench/field_metadata/run.sh to run the matrix interleaved on two
//     builds (before/after) and aggregate p50/p90 + validity per cell.
// ---------------------------------------------------------------------------

// fmS3 is a deterministic in-process S3 with toxiproxy-like first-byte latency
// on every read and Parquet-structure accounting of what each read returned.
type fmS3 struct {
	mu      sync.RWMutex
	files   map[string][]byte
	layouts map[string]*fmLayout
	srv     *httptest.Server

	latency atomic.Int64 // ns slept before the first byte of every read

	gets, bytesServed, lists atomic.Int64

	touchMu sync.Mutex
	rgs     map[string]struct{}
	pages   map[string]struct{}
}

type fmSpan struct{ lo, hi int64 } // [lo, hi)

// fmLayout is where an object's row groups and pages live in its bytes.
type fmLayout struct {
	rowGroups [][]fmSpan // per row group: its column-chunk spans
	pages     []fmSpan   // every dictionary and data page
}

func newFmS3() *fmS3 {
	m := &fmS3{
		files:   make(map[string][]byte),
		layouts: make(map[string]*fmLayout),
		rgs:     make(map[string]struct{}),
		pages:   make(map[string]struct{}),
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *fmS3) url() string { return m.srv.URL }
func (m *fmS3) close()      { m.srv.Close() }

func (m *fmS3) resetCounters() {
	m.gets.Store(0)
	m.bytesServed.Store(0)
	m.lists.Store(0)
	m.touchMu.Lock()
	m.rgs = make(map[string]struct{})
	m.pages = make(map[string]struct{})
	m.touchMu.Unlock()
}

func (m *fmS3) touched() (rgs, pages int64) {
	m.touchMu.Lock()
	defer m.touchMu.Unlock()
	return int64(len(m.rgs)), int64(len(m.pages))
}

func (m *fmS3) handler(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}
	switch r.Method {
	case http.MethodPut:
		data, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		m.mu.Lock()
		m.files[key] = data
		delete(m.layouts, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	case http.MethodDelete:
		m.mu.Lock()
		delete(m.files, key)
		delete(m.layouts, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if d := m.latency.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	if key == "" {
		m.serveList(w, r)
		return
	}
	m.mu.RLock()
	data, ok := m.files[key]
	m.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		return
	}
	lo, hi := int64(0), int64(len(data))
	status := http.StatusOK
	if rh := r.Header.Get("Range"); strings.HasPrefix(rh, "bytes=") {
		bounds := strings.SplitN(strings.TrimPrefix(rh, "bytes="), "-", 2)
		start, _ := strconv.ParseInt(bounds[0], 10, 64)
		end, _ := strconv.ParseInt(bounds[1], 10, 64)
		if start >= int64(len(data)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		lo, hi = start, end+1
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	}
	m.account(key, data, lo, hi)
	w.Header().Set("Content-Length", strconv.FormatInt(hi-lo, 10))
	w.WriteHeader(status)
	_, _ = w.Write(data[lo:hi])
}

// serveList answers ListObjectsV2 with every stored key under the prefix.
func (m *fmS3) serveList(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("list-type") != "2" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	m.lists.Add(1)
	prefix := r.URL.Query().Get("prefix")
	m.mu.RLock()
	keys := make([]string, 0, len(m.files))
	for k := range m.files {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(`<?xml version="1.0"?><ListBucketResult>`)
	for _, k := range keys {
		fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>%d</Size></Contents>`, k, len(m.files[k]))
	}
	m.mu.RUnlock()
	b.WriteString(`<IsTruncated>false</IsTruncated></ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, b.String())
}

func (m *fmS3) account(key string, data []byte, lo, hi int64) {
	m.gets.Add(1)
	m.bytesServed.Add(hi - lo)
	l := m.layoutFor(key, data)
	if l == nil {
		return
	}
	over := func(s fmSpan) bool { return s.lo < hi && lo < s.hi }
	m.touchMu.Lock()
	defer m.touchMu.Unlock()
	for i, rg := range l.rowGroups {
		for _, sp := range rg {
			if over(sp) {
				m.rgs[key+"#"+strconv.Itoa(i)] = struct{}{}
				break
			}
		}
	}
	for _, p := range l.pages {
		if over(p) {
			m.pages[key+"@"+strconv.FormatInt(p.lo, 10)] = struct{}{}
		}
	}
}

// layoutFor parses (once per object) where the object's row groups and pages
// are. A non-Parquet object (a pmeta bundle, a sidecar) has no layout.
func (m *fmS3) layoutFor(key string, data []byte) *fmLayout {
	m.mu.RLock()
	l, ok := m.layouts[key]
	m.mu.RUnlock()
	if ok {
		return l
	}
	l = parseFmLayout(data)
	m.mu.Lock()
	m.layouts[key] = l
	m.mu.Unlock()
	return l
}

func parseFmLayout(data []byte) *fmLayout {
	if len(data) < 8 || !bytes.HasSuffix(data, []byte("PAR1")) {
		return nil
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	md := f.Metadata()
	_, offsets, _ := f.ReadPageIndex()
	l := &fmLayout{rowGroups: make([][]fmSpan, len(md.RowGroups))}
	for i, rg := range md.RowGroups {
		for j, cc := range rg.Columns {
			cm := cc.MetaData
			start := cm.DataPageOffset
			if cm.DictionaryPageOffset > 0 && cm.DictionaryPageOffset < start {
				start = cm.DictionaryPageOffset
				l.pages = append(l.pages, fmSpan{cm.DictionaryPageOffset, cm.DataPageOffset})
			}
			l.rowGroups[i] = append(l.rowGroups[i], fmSpan{start, start + cm.TotalCompressedSize})
			idx := i*len(rg.Columns) + j
			if idx < len(offsets) && len(offsets[idx].PageLocations) > 0 {
				for _, pl := range offsets[idx].PageLocations {
					l.pages = append(l.pages, fmSpan{pl.Offset, pl.Offset + int64(pl.CompressedPageSize)})
				}
			} else {
				// No offset index: the chunk's data pages count as one unit.
				l.pages = append(l.pages, fmSpan{cm.DataPageOffset, start + cm.TotalCompressedSize})
			}
		}
	}
	return l
}

// Dataset: a quiet hour (fewer than 100 distinct values per field per file)
// and a busy hour (150 distinct service.name values in every file, 700 in the
// hour). 12 flushes of 2000 rows per hour. Values that exist only outside the
// cut window [10:30, 11:30] expose answers that are not window-confined:
// TRACE and svc-early live in 10:00-10:10, FATAL in 11:50-12:00. trace_id is
// absent on svc-a rows, so a filtered field_names differs from an unfiltered one.
const (
	fmSlotsPerHour = 12
	fmRowsPerSlot  = 2000
	fmFilterSvc    = "svc-a"
)

var (
	fmBase    = time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	fmLevels  = []string{"INFO", "WARN", "ERROR", "DEBUG"}
	fmQuiet   = []string{"svc-a", "svc-b", "svc-c", "svc-d", "svc-e", "svc-f", "svc-g", "svc-h"}
	fmSevNums = map[string]int32{"TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17, "FATAL": 21}
)

func fmStream(svc string) string { return `{service.name="` + svc + `"}` }

// fmSlotRows returns the rows of one 5-minute flush slot (hour 0 quiet, 1 busy).
func fmSlotRows(hour, slot int) []schema.LogRow {
	rng := rand.New(rand.NewSource(int64(hour*1000 + slot))) //nolint:gosec // deterministic dataset, not security
	start := fmBase.Add(time.Duration(hour)*time.Hour + time.Duration(slot)*5*time.Minute)
	rows := make([]schema.LogRow, 0, fmRowsPerSlot)
	for i := 0; i < fmRowsPerSlot; i++ {
		var svc, level string
		if hour == 0 {
			svc = fmQuiet[i%len(fmQuiet)]
			if slot < 2 && i%10 == 0 {
				svc = "svc-early"
			}
			level = fmLevels[(i+slot)%len(fmLevels)]
			if slot < 2 && i%50 == 7 {
				level = "TRACE"
			}
		} else {
			svc = fmt.Sprintf("svc-busy-%03d", slot*50+i%150)
			if i%20 == 0 {
				svc = fmFilterSvc
			}
			level = fmLevels[(i*3+slot)%len(fmLevels)]
			if slot >= 10 && i%50 == 7 {
				level = "FATAL"
			}
		}
		if svc == fmFilterSvc && level == "DEBUG" {
			level = "INFO" // the filtered value set differs from the unfiltered one
		}
		traceID := ""
		if svc != fmFilterSvc {
			traceID = fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64())
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(fmStream(svc)))
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: start.Add(time.Duration(i) * 150 * time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("GET /api/v1/items/%d status=%d dur=%dms", rng.Intn(100000), 200+100*rng.Intn(4), rng.Intn(500)),
			SeverityText:      level,
			SeverityNumber:    fmSevNums[level],
			ServiceName:       svc,
			TraceID:           traceID,
			Stream:            fmStream(svc),
			StreamID:          fmt.Sprintf("%016x", h.Sum64()),
		})
	}
	return rows
}

func fmAllRows() []schema.LogRow {
	var all []schema.LogRow
	for h := 0; h < 2; h++ {
		for s := 0; s < fmSlotsPerHour; s++ {
			all = append(all, fmSlotRows(h, s)...)
		}
	}
	return all
}

type fmWindow struct {
	name   string
	lo, hi int64 // inclusive, as VictoriaLogs' _time filter
}

func fmWindows() []fmWindow {
	return []fmWindow{
		{"whole", fmBase.UnixNano(), fmBase.Add(2*time.Hour).UnixNano() - 1},
		// Bounds sit between rows (rows are 150 ms apart): the row oracle's
		// row-group time pruning is end-exclusive (rgMin < endNs in
		// rowGroupMatchesTimeRange), so a row exactly at endNs that starts a
		// row group would be missing from the oracle but not from the scan.
		{"cut", fmBase.Add(30*time.Minute + 75*time.Millisecond).UnixNano(), fmBase.Add(90*time.Minute + 75*time.Millisecond).UnixNano()},
		// 30 s inside one flush slot: cuts one flushed file, and one compacted
		// object, of which it needs a sliver (a small call on a big object).
		{"narrow", fmBase.Add(80*time.Minute + 10*time.Second + 75*time.Millisecond).UnixNano(), fmBase.Add(80*time.Minute + 40*time.Second + 75*time.Millisecond).UnixNano()},
		// Across the hour boundary: flushed slots 09-11 of the first hour and
		// 00 of the second lie wholly inside (answerable from their label
		// counts), slot 01 is cut; both compacted hour objects are cut.
		{"edge", fmBase.Add(45*time.Minute - 75*time.Millisecond).UnixNano(), fmBase.Add(67*time.Minute + 30*time.Second + 75*time.Millisecond).UnixNano()},
	}
}

var (
	fmEndpoints = []string{"fv_level", "fv_service", "field_names", "streams"}
	fmFilters   = []string{"none", "svc"}
)

func fmQuery(tb testing.TB, filter string, w fmWindow) *logstorage.Query {
	expr := "*"
	if filter == "svc" {
		expr = `service.name:="` + fmFilterSvc + `"`
	}
	q, err := logstorage.ParseQuery(expr)
	if err != nil {
		tb.Fatal(err)
	}
	q.AddTimeFilter(w.lo, w.hi)
	return q
}

// fmTruth is the exact answer of a value endpoint, computed from the generator.
func fmTruth(rows []schema.LogRow, endpoint, filter string, w fmWindow) map[string]uint64 {
	out := make(map[string]uint64)
	for i := range rows {
		r := &rows[i]
		if r.TimestampUnixNano < w.lo || r.TimestampUnixNano > w.hi {
			continue
		}
		if filter == "svc" && r.ServiceName != fmFilterSvc {
			continue
		}
		switch endpoint {
		case "fv_level":
			out[r.SeverityText]++
		case "fv_service":
			out[r.ServiceName]++
		case "streams":
			out[r.Stream]++
		}
	}
	return out
}

// fmFieldNamesTruth is field_names' exact answer from the generator: every row
// carries _msg, _stream, _stream_id, _time, level, service.name and
// severity_number; trace_id only when set. Hits are row counts.
func fmFieldNamesTruth(rows []schema.LogRow, filter string, w fmWindow) map[string]uint64 {
	out := make(map[string]uint64)
	for i := range rows {
		r := &rows[i]
		if r.TimestampUnixNano < w.lo || r.TimestampUnixNano > w.hi {
			continue
		}
		if filter == "svc" && r.ServiceName != fmFilterSvc {
			continue
		}
		for _, n := range []string{"_msg", "_stream", "_stream_id", "_time", "level", "service.name", "severity_number"} {
			out[n]++
		}
		if r.TraceID != "" {
			out["trace_id"]++
		}
	}
	return out
}

// fmRowOracle reads the rows the query returns through the row path and counts,
// per field, the rows carrying a non-empty value (field_names' answer, keyed
// "") and, per field, each value's rows (cross-checks the generator truth).
func fmRowOracle(tb testing.TB, s *Storage, filter string, w fmWindow) map[string]map[string]uint64 {
	var mu sync.Mutex
	names := make(map[string]uint64)
	values := make(map[string]map[string]uint64)
	err := s.RunQuery(context.Background(), nil, fmQuery(tb, filter, w), func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range db.GetColumns(false) {
			for _, v := range c.Values {
				if v == "" {
					continue
				}
				names[c.Name]++
				if values[c.Name] == nil {
					values[c.Name] = make(map[string]uint64)
				}
				values[c.Name][v]++
			}
		}
	})
	if err != nil {
		tb.Fatal(err)
	}
	values[""] = names
	return values
}

// fmBridgeErrors is the number of peer answers the buffer bridge dropped.
func fmBridgeErrors() uint64 {
	var n uint64
	for _, reason := range []string{"request", "status", "scope", "decode"} {
		n += metrics.BufferBridgeErrors.Get(reason)
	}
	return n
}

// fmDiff names the first values whose hits differ from the truth
// (value=got/want), so a non-exact iteration in CI says what was wrong.
func fmDiff(got, truth map[string]uint64) string {
	keys := make([]string, 0, len(got)+len(truth))
	for k := range truth {
		keys = append(keys, k)
	}
	for k := range got {
		if _, ok := truth[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	n := 0
	for _, k := range keys {
		if got[k] == truth[k] {
			continue
		}
		if n == 5 {
			b.WriteString(" …")
			break
		}
		fmt.Fprintf(&b, "%s=%d/%d ", k, got[k], truth[k])
		n++
	}
	return strings.TrimSpace(b.String())
}

func fmEqualCounts(a, b map[string]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, n := range a {
		if m, ok := b[k]; !ok || m != n {
			return false
		}
	}
	return true
}

func fmDigest(t map[string]uint64) string {
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		_, _ = fmt.Fprintf(h, "%s=%d;", k, t[k])
	}
	return fmt.Sprintf("%d:%016x", len(t), h.Sum64())
}

// fmEnv is one (layout, pmeta) deployment of the dataset.
type fmEnv struct {
	s      *Storage
	mock   *fmS3
	layout string
	pmeta  bool
	files  int
	truth  map[string]map[string]uint64 // fmTruthKey -> exact answer
}

func fmTruthKey(endpoint, filter, window string) string {
	return endpoint + "/" + filter + "/" + window
}

// fmPeer is an insert instance holding unflushed rows: it answers
// /internal/buffer/query for the default tenant with the rows inside the
// requested [start, end], as the buffer handler does.
//
// The bridge timeout is generous: a shared CI runner can stall past the
// production default while the fake streams thousands of rows, and a timed-out
// peer is dropped whole (counted in lakehouse_buffer_bridge_errors_total).
func fmPeer(tb testing.TB, rows []schema.LogRow) *BufferBridge {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.ParseInt(r.URL.Query().Get("start"), 10, 64)
		end, _ := strconv.ParseInt(r.URL.Query().Get("end"), 10, 64)
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		enc := json.NewEncoder(w)
		for i := range rows {
			if ts := rows[i].TimestampUnixNano; ts >= start && ts <= end {
				_ = enc.Encode(&rows[i])
			}
		}
	}))
	tb.Cleanup(srv.Close)
	bridge := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 60 * time.Second}, config.ModeLogs)
	bridge.SetEndpoints([]string{srv.URL})
	return bridge
}

func buildFmEnv(tb testing.TB, layout string, pmetaOn bool) *fmEnv {
	tb.Helper()
	mock := newFmS3()
	tb.Cleanup(mock.close)
	s := testStorageWithS3(tb, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	if pmetaOn {
		s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
		s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog}
	}
	var unflushed []schema.LogRow
	for h := 0; h < 2; h++ {
		for slot := 0; slot < fmSlotsPerHour; slot++ {
			// layout=peer: the last ten minutes are not flushed yet; they sit
			// in another insert instance's buffer, reachable only through the
			// buffer bridge (the FATAL level exists only there).
			if layout == "peer" && h == 1 && slot >= fmSlotsPerHour-2 {
				unflushed = append(unflushed, fmSlotRows(h, slot)...)
				continue
			}
			bw.AddLogRows(fmSlotRows(h, slot))
			bw.triggerFlush()
		}
	}
	if layout == "peer" {
		s.bufferBridge = fmPeer(tb, unflushed)
	}
	ctx := context.Background()
	if layout == "compacted" {
		for h := 0; h < 2; h++ {
			partition := partitionFromNano(fmBase.Add(time.Duration(h) * time.Hour).UnixNano())
			inputs := s.manifest.FilesForPartition(partition)
			c := compaction.NewCompactor(compaction.CompactorConfig{
				Pool:             s.pool,
				Manifest:         s.manifest,
				Prefix:           "logs/",
				Mode:             config.ModeLogs,
				RowGroupSize:     s.cfg.Insert.RowGroupSize,
				CompressionLevel: s.cfg.Insert.CompressionLevel,
				CompactionConfig: s.cfg.Compaction,
			})
			res, err := c.Compact(ctx, partition, inputs, 0)
			if err != nil {
				tb.Fatalf("compact %s: %v", partition, err)
			}
			removed := make([]string, 0, len(inputs))
			for _, fi := range inputs {
				removed = append(removed, fi.Key)
			}
			// What the scheduler's OnCompacted hook does in cmd/lakehouse-logs.
			s.PmetaOnCompacted(s.manifest.FilesForPartition(partition), removed, res.OutputBlooms)
		}
	}
	e := &fmEnv{s: s, mock: mock, layout: layout, pmeta: pmetaOn, truth: make(map[string]map[string]uint64)}
	whole := fmWindows()[0]
	e.files = len(s.manifest.GetFilesForRange(whole.lo, whole.hi))

	// Seed the label index the way production does: a first query opens files.
	if err := s.RunQuery(ctx, nil, fmQuery(tb, "none", whole), func(uint, *logstorage.DataBlock) {}); err != nil {
		tb.Fatal(err)
	}

	all := fmAllRows()
	cols := map[string]string{"fv_level": "level", "fv_service": "service.name", "streams": "_stream"}
	for _, w := range fmWindows() {
		for _, f := range fmFilters {
			oracle := fmRowOracle(tb, s, f, w)
			for _, ep := range fmEndpoints {
				// The generator is the truth; the row path must agree with it
				// (a harness self-check, not a measured cell).
				t, got := fmFieldNamesTruth(all, f, w), oracle[""]
				if ep != "field_names" {
					t, got = fmTruth(all, ep, f, w), oracle[cols[ep]]
				}
				if !fmEqualCounts(got, t) {
					if layout != "peer" {
						tb.Fatalf("row oracle disagrees with the generator for %s/%s/%s: %v vs %v", ep, f, w.name, got, t)
					}
					// Over the buffer bridge the row path itself is under test:
					// a build whose bridge drops fields is a finding, not a
					// harness defect. The generator stays the truth.
					tb.Logf("row path over the buffer bridge disagrees with the generator for %s/%s/%s: %v vs %v", ep, f, w.name, got, t)
				}
				e.truth[fmTruthKey(ep, f, w.name)] = t
			}
		}
	}
	return e
}

type fmResult struct {
	dur                        time.Duration
	setOK, hitsOK              bool
	diff                       string // first differences from the truth, when not exact
	values                     int
	gets, bytes, lists         int64
	rowGroups, pages, catalogs int64
}

// run executes one iteration of a cell against cold object caches.
func (e *fmEnv) run(tb testing.TB, endpoint, filter string, w fmWindow, latency time.Duration) fmResult {
	s := e.s
	s.memCache = cache.NewLRU(64 * 1024 * 1024)
	s.footerCache = NewFooterCache(1000)
	q := fmQuery(tb, filter, w)
	e.mock.resetCounters()
	e.mock.latency.Store(int64(latency))
	cat0 := metrics.CatalogValueLookups.Get("catalog")
	br0 := fmBridgeErrors()
	ctx := context.Background()

	var (
		vals []logstorage.ValueWithHits
		err  error
	)
	start := time.Now()
	switch endpoint {
	case "fv_level":
		vals, err = s.GetFieldValues(ctx, nil, q, "level", 0)
	case "fv_service":
		vals, err = s.GetFieldValues(ctx, nil, q, "service.name", 0)
	case "field_names":
		vals, err = s.GetFieldNames(ctx, nil, q)
	case "streams":
		vals, err = s.GetStreams(ctx, nil, q, 0)
	}
	dur := time.Since(start)
	e.mock.latency.Store(0)
	if err != nil {
		tb.Fatalf("%s: %v", endpoint, err)
	}

	r := fmResult{dur: dur, values: len(vals)}
	r.gets, r.bytes, r.lists = e.mock.gets.Load(), e.mock.bytesServed.Load(), e.mock.lists.Load()
	r.rowGroups, r.pages = e.mock.touched()
	r.catalogs = int64(metrics.CatalogValueLookups.Get("catalog") - cat0) //nolint:gosec // a per-iteration counter delta

	truth := e.truth[fmTruthKey(endpoint, filter, w.name)]
	got := make(map[string]uint64, len(vals))
	dup := false
	for _, v := range vals {
		if _, ok := got[v.Value]; ok {
			dup = true
		}
		got[v.Value] = v.Hits
	}
	r.setOK = !dup && len(got) == len(truth)
	for k := range truth {
		if _, ok := got[k]; !ok {
			r.setOK = false
			break
		}
	}
	r.hitsOK = r.setOK && fmEqualCounts(got, truth)
	if !r.hitsOK {
		r.diff = fmDiff(got, truth)
		if n := fmBridgeErrors() - br0; n > 0 {
			r.diff += fmt.Sprintf(" (buffer bridge errors: %d)", n)
		}
	}
	return r
}

func fmLatencies() []time.Duration {
	spec := os.Getenv("FM_LATENCIES_MS")
	if spec == "" {
		spec = "0,100"
	}
	var out []time.Duration
	for _, p := range strings.Split(spec, ",") {
		if ms, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
			out = append(out, time.Duration(ms)*time.Millisecond)
		}
	}
	return out
}

func buildFmEnvs(tb testing.TB) []*fmEnv {
	var envs []*fmEnv
	for _, layout := range []string{"flushed", "compacted", "peer"} {
		for _, pm := range []bool{true, false} {
			envs = append(envs, buildFmEnv(tb, layout, pm))
		}
	}
	return envs
}

// fmCellName is the cell's name in both the benchmark and the JSONL records.
func fmCellName(ep string, e *fmEnv, w fmWindow, filter string, lat time.Duration) string {
	return fmt.Sprintf("%s/pmeta=%v/layout=%s/window=%s/filter=%s/s3=%dms", ep, e.pmeta, e.layout, w.name, filter, lat.Milliseconds())
}

// BenchmarkFieldMetadata reports, per cell, ns/op plus S3 GETs, bytes, row
// groups and pages per op and the fraction of iterations whose answer was
// exact (valid = values+hits, set_valid = values only). Narrow with -bench,
// e.g. -bench 'FieldMetadata/fv_service/pmeta=true/.*/s3=0ms'.
func BenchmarkFieldMetadata(b *testing.B) {
	envs := buildFmEnvs(b)
	for _, ep := range fmEndpoints {
		for _, e := range envs {
			for _, w := range fmWindows() {
				for _, f := range fmFilters {
					for _, lat := range fmLatencies() {
						b.Run(fmCellName(ep, e, w, f, lat), func(b *testing.B) {
							benchFmCell(b, e, ep, f, w, lat)
						})
					}
				}
			}
		}
	}
}

func benchFmCell(b *testing.B, e *fmEnv, ep, filter string, w fmWindow, lat time.Duration) {
	var gets, bytesRead, rgs, pages, valid, setValid float64
	var elapsed time.Duration
	for i := 0; i < b.N; i++ {
		r := e.run(b, ep, filter, w, lat)
		elapsed += r.dur
		gets += float64(r.gets)
		bytesRead += float64(r.bytes)
		rgs += float64(r.rowGroups)
		pages += float64(r.pages)
		if r.hitsOK {
			valid++
		}
		if r.setOK {
			setValid++
		}
	}
	n := float64(b.N)
	// ns/op of the framework includes the per-iteration cache reset and the
	// validation; call_ns/op is the measured call alone.
	b.ReportMetric(float64(elapsed.Nanoseconds())/n, "call_ns/op")
	b.ReportMetric(gets/n, "gets/op")
	b.ReportMetric(bytesRead/n, "s3B/op")
	b.ReportMetric(rgs/n, "rgs/op")
	b.ReportMetric(pages/n, "pages/op")
	b.ReportMetric(valid/n, "valid")
	b.ReportMetric(setValid/n, "set_valid")
}

// TestFieldMetadata_ExactInBothLayouts runs every value cell once, at 0 ms S3,
// on the same rows flushed as small files, compacted into hour objects, and
// flushed except the last ten minutes still unflushed on a peer, and
// requires the exact answer (values and hits) in every layout, window and
// filter. Answers from label counts must also cost no S3 read. field_names is
// not exact yet (it credits all-null columns and ignores the window) and is
// measured by the matrix only.
func TestFieldMetadata_ExactInBothLayouts(t *testing.T) {
	if testing.Short() {
		t.Skip("builds four deployments of 48k rows")
	}
	for _, e := range buildFmEnvs(t) {
		for _, ep := range fmEndpoints {
			if ep == "field_names" {
				continue
			}
			for _, w := range fmWindows() {
				for _, f := range fmFilters {
					r := e.run(t, ep, f, w, 0)
					name := fmCellName(ep, e, w, f, 0)
					if !r.hitsOK {
						t.Errorf("%s: not exact (values ok: %v)", name, r.setOK)
					}
					// Wholly contained objects and a low-card field: the answer
					// is the objects' label counts, read from the manifest.
					if ep == "fv_level" && f == "none" && w.name == "whole" && r.gets != 0 {
						t.Errorf("%s: %d S3 GETs, want 0 (label counts)", name, r.gets)
					}
				}
			}
		}
	}
}

// fmRecord is one JSONL line of TestFieldMetadataMatrix.
type fmRecord struct {
	Build     string `json:"build"`
	Round     string `json:"round"`
	Cell      string `json:"cell"`
	Endpoint  string `json:"endpoint"`
	Pmeta     bool   `json:"pmeta"`
	Layout    string `json:"layout"`
	Window    string `json:"window"`
	Filter    string `json:"filter"`
	LatencyMs int64  `json:"s3_latency_ms"`
	Iter      int    `json:"iter"`
	Ns        int64  `json:"ns"`
	SetOK     bool   `json:"set_ok"`
	HitsOK    bool   `json:"hits_ok"`
	Values    int    `json:"values"`
	Truth     string `json:"truth"`
	Files     int    `json:"files"`
	Gets      int64  `json:"gets"`
	Bytes     int64  `json:"bytes"`
	Lists     int64  `json:"lists"`
	RowGroups int64  `json:"row_groups"`
	Pages     int64  `json:"pages"`
	Catalog   int64  `json:"catalog_answers"`
	Diff      string `json:"diff,omitempty"`
}

// TestFieldMetadataMatrix runs every cell FM_ITERS times (after FM_WARMUP
// unrecorded iterations) and appends one JSONL record per measured iteration
// to FM_MATRIX_OUT. Skipped unless FM_MATRIX_OUT is set. FM_BUILD labels the
// build (before/after), FM_ROUND the interleaving round, FM_LATENCIES_MS the
// S3 first-byte latencies (default "0,100").
func TestFieldMetadataMatrix(t *testing.T) {
	out := os.Getenv("FM_MATRIX_OUT")
	if out == "" {
		t.Skip("set FM_MATRIX_OUT to run the field-metadata performance matrix")
	}
	iters, warmup := 2, 1
	if v, err := strconv.Atoi(os.Getenv("FM_ITERS")); err == nil && v > 0 {
		iters = v
	}
	if v, err := strconv.Atoi(os.Getenv("FM_WARMUP")); err == nil && v >= 0 {
		warmup = v
	}
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)

	envs := buildFmEnvs(t)
	for _, ep := range fmEndpoints {
		for _, e := range envs {
			for _, w := range fmWindows() {
				for _, flt := range fmFilters {
					for _, lat := range fmLatencies() {
						for i := 0; i < warmup; i++ {
							e.run(t, ep, flt, w, lat)
						}
						for i := 0; i < iters; i++ {
							r := e.run(t, ep, flt, w, lat)
							rec := fmRecord{
								Build: os.Getenv("FM_BUILD"), Round: os.Getenv("FM_ROUND"),
								Cell:     fmCellName(ep, e, w, flt, lat),
								Endpoint: ep, Pmeta: e.pmeta, Layout: e.layout, Window: w.name, Filter: flt,
								LatencyMs: lat.Milliseconds(), Iter: i, Ns: r.dur.Nanoseconds(),
								SetOK: r.setOK, HitsOK: r.hitsOK, Values: r.values,
								Truth: fmDigest(e.truth[fmTruthKey(ep, flt, w.name)]), Files: e.files,
								Gets: r.gets, Bytes: r.bytes, Lists: r.lists, RowGroups: r.rowGroups, Pages: r.pages,
								Catalog: r.catalogs, Diff: r.diff,
							}
							if err := enc.Encode(rec); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
			}
		}
	}
}

// TestFieldMetadataMatrixVL answers the same endpoint × window × filter cells
// from an in-process upstream VictoriaLogs storage (hot, local disk, page
// cache warm) holding the same rows, validated against the same truth — the
// reference both for latency and for what an exact answer is. Skipped unless
// FM_MATRIX_OUT is set; records carry build "vl" and layout "vl-hot".
func TestFieldMetadataMatrixVL(t *testing.T) {
	out := os.Getenv("FM_MATRIX_OUT")
	if out == "" {
		t.Skip("set FM_MATRIX_OUT to run the field-metadata performance matrix")
	}
	iters, warmup := 2, 1
	if v, err := strconv.Atoi(os.Getenv("FM_ITERS")); err == nil && v > 0 {
		iters = v
	}
	if v, err := strconv.Atoi(os.Getenv("FM_WARMUP")); err == nil && v >= 0 {
		warmup = v
	}
	const decade = 10 * 365 * 24 * time.Hour
	vs := logstorage.MustOpenStorage(t.TempDir(), &logstorage.StorageConfig{
		Retention: decade, FutureRetention: decade, MaxBackfillAge: decade,
	})
	defer vs.MustClose()
	all := fmAllRows()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := range all {
		r := &all[i]
		fields := []logstorage.Field{
			{Name: "service.name", Value: r.ServiceName}, // the stream field
			{Name: "_msg", Value: r.Body},
			{Name: "level", Value: r.SeverityText},
			{Name: "severity_number", Value: strconv.Itoa(int(r.SeverityNumber))},
		}
		if r.TraceID != "" {
			fields = append(fields, logstorage.Field{Name: "trace_id", Value: r.TraceID})
		}
		lr.MustAdd(logstorage.TenantID{}, r.TimestampUnixNano, fields, 1)
	}
	vs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	vs.DebugFlush()

	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	ctx := context.Background()
	tenants := []logstorage.TenantID{{}}
	for _, ep := range fmEndpoints {
		for _, w := range fmWindows() {
			for _, flt := range fmFilters {
				truth := fmFieldNamesTruth(all, flt, w)
				if ep != "field_names" {
					truth = fmTruth(all, ep, flt, w)
				}
				call := func() (time.Duration, []logstorage.ValueWithHits) {
					qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, tenants, fmQuery(t, flt, w), false, nil)
					var (
						vals []logstorage.ValueWithHits
						err  error
					)
					start := time.Now()
					switch ep {
					case "fv_level":
						vals, err = vs.GetFieldValues(qctx, "level", "", 0)
					case "fv_service":
						vals, err = vs.GetFieldValues(qctx, "service.name", "", 0)
					case "field_names":
						vals, err = vs.GetFieldNames(qctx, "")
					case "streams":
						vals, err = vs.GetStreams(qctx, 0)
					}
					d := time.Since(start)
					if err != nil {
						t.Fatalf("vl %s: %v", ep, err)
					}
					return d, vals
				}
				for i := 0; i < warmup; i++ {
					call()
				}
				for i := 0; i < iters; i++ {
					d, vals := call()
					got := make(map[string]uint64, len(vals))
					for _, v := range vals {
						got[v.Value] = v.Hits
					}
					setOK := len(got) == len(vals) && len(got) == len(truth)
					for k := range truth {
						if _, ok := got[k]; !ok {
							setOK = false
						}
					}
					rec := fmRecord{
						Build: "vl", Round: os.Getenv("FM_ROUND"),
						Cell:     fmt.Sprintf("vl.%s/window=%s/filter=%s", ep, w.name, flt),
						Endpoint: "vl." + ep, Layout: "vl-hot", Window: w.name, Filter: flt, Iter: i, Ns: d.Nanoseconds(),
						SetOK: setOK, HitsOK: setOK && fmEqualCounts(got, truth), Values: len(vals), Truth: fmDigest(truth),
					}
					if err := enc.Encode(rec); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}
