package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Traces subset of the field-metadata performance matrix
// (internal/storage/parquets3/field_values_bench_test.go on the logs side,
// docs/perf/field-metadata-cells.md). Cells: endpoint {field_values name,
// field_values resource_attr:service.name, streams} × pmeta {on, off} × window
// {whole hours, cut mid-hour} × filter {none, service.name:="svc-a"} × S3
// first-byte latency {0, 100 ms}, over flushed small files (compacted layouts
// and field_names are measured on the logs side only). Every iteration starts
// with cold object caches and is validated against the generator's truth
// (set_ok: exact value set; hits_ok: and exact hit counts).
//
// TestFieldMetadataMatrixTraces is skipped unless FM_MATRIX_OUT is set; it
// appends the same JSONL records as the logs harness, so
// scripts/bench/field_metadata/aggregate.py reads both.

// fmtS3 is the traces twin of the logs harness's instrumented S3: first-byte
// latency on every read, GET and body-byte counters.
type fmtS3 struct {
	mu      sync.RWMutex
	files   map[string][]byte
	srv     *httptest.Server
	latency atomic.Int64

	gets, bytesServed atomic.Int64
}

func newFmtS3() *fmtS3 {
	m := &fmtS3{files: make(map[string][]byte)}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *fmtS3) handler(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) < 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	key := parts[1]
	switch r.Method {
	case http.MethodPut:
		data, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		m.mu.Lock()
		m.files[key] = data
		m.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		return
	case http.MethodDelete:
		m.mu.Lock()
		delete(m.files, key)
		m.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if d := m.latency.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	m.mu.RLock()
	data, ok := m.files[key]
	m.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
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
	m.gets.Add(1)
	m.bytesServed.Add(hi - lo)
	w.Header().Set("Content-Length", strconv.FormatInt(hi-lo, 10))
	w.WriteHeader(status)
	_, _ = w.Write(data[lo:hi])
}

// Dataset: a quiet hour (8 services, 6 span names; svc-early and the span name
// "warmup" only in 10:00-10:10) and a busy hour (150 distinct services in every
// file, 700 in the hour; span name "shutdown" only in 11:50-12:00). 12 flushes
// of 2000 spans per hour.
const (
	fmtSlotsPerHour = 12
	fmtRowsPerSlot  = 2000
	fmtFilterSvc    = "svc-a"
)

var (
	fmtBase  = time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	fmtNames = []string{"GET /a", "POST /b", "PUT /c", "DELETE /d", "GET /e", "SELECT"}
)

func fmtStream(svc string) string { return `{resource_attr:service.name="` + svc + `"}` }

func fmtSlotRows(hour, slot int) []schema.TraceRow {
	start := fmtBase.Add(time.Duration(hour)*time.Hour + time.Duration(slot)*5*time.Minute)
	rows := make([]schema.TraceRow, 0, fmtRowsPerSlot)
	for i := 0; i < fmtRowsPerSlot; i++ {
		svc := fmt.Sprintf("svc-%c", 'a'+i%8)
		name := fmtNames[(i+slot)%len(fmtNames)]
		if hour == 0 {
			if slot < 2 && i%10 == 0 {
				svc = "svc-early"
			}
			if slot < 2 && i%50 == 7 {
				name = "warmup"
			}
		} else {
			svc = fmt.Sprintf("svc-busy-%03d", slot*50+i%150)
			if i%20 == 0 {
				svc = fmtFilterSvc
			}
			if slot >= 10 && i%50 == 7 {
				name = "shutdown"
			}
		}
		if svc == fmtFilterSvc && name == "SELECT" {
			name = "GET /a" // the filtered value set differs from the unfiltered one
		}
		ts := start.Add(time.Duration(i) * 150 * time.Millisecond).UnixNano()
		id := fmt.Sprintf("%02d%02d%04d", hour, slot, i)
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: ts,
			TraceID: "t" + id, SpanID: "s" + id,
			SpanName: name, ServiceName: svc, DurationNs: int64(i%500) * 1000,
			Stream: fmtStream(svc), StreamID: "id-" + svc,
		})
	}
	return rows
}

type fmtWindow struct {
	name   string
	lo, hi int64
}

func fmtWindows() []fmtWindow {
	return []fmtWindow{
		{"whole", fmtBase.UnixNano(), fmtBase.Add(2*time.Hour).UnixNano() - 1},
		{"cut", fmtBase.Add(30*time.Minute + 75*time.Millisecond).UnixNano(), fmtBase.Add(90*time.Minute + 75*time.Millisecond).UnixNano()},
	}
}

func fmtTruth(rows []schema.TraceRow, endpoint, filter string, w fmtWindow) map[string]uint64 {
	out := make(map[string]uint64)
	for i := range rows {
		r := &rows[i]
		if r.TimestampUnixNano < w.lo || r.TimestampUnixNano > w.hi {
			continue
		}
		if filter == "svc" && r.ServiceName != fmtFilterSvc {
			continue
		}
		switch endpoint {
		case "fv_name":
			out[r.SpanName]++
		case "fv_service":
			out[r.ServiceName]++
		case "streams":
			out[r.Stream]++
		}
	}
	return out
}

type fmtEnv struct {
	s     *Storage
	mock  *fmtS3
	pmeta bool
	files int
	rows  []schema.TraceRow
}

func buildFmtEnv(t *testing.T, pmetaOn bool) *fmtEnv {
	t.Helper()
	mock := newFmtS3()
	t.Cleanup(mock.srv.Close)
	s := testStorageWithS3(t, mock.srv.URL)
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	if pmetaOn {
		s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
		s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog}
	}
	e := &fmtEnv{s: s, mock: mock, pmeta: pmetaOn}
	for h := 0; h < 2; h++ {
		for slot := 0; slot < fmtSlotsPerHour; slot++ {
			rows := fmtSlotRows(h, slot)
			e.rows = append(e.rows, rows...)
			bw.AddTraceRows(rows)
			bw.triggerFlush()
		}
	}
	whole := fmtWindows()[0]
	e.files = len(s.manifest.GetFilesForRange(whole.lo, whole.hi))
	// Seed the label index the way production does: a first query opens files.
	if err := s.RunQuery(context.Background(), nil, fmtQuery(t, "none", whole), func(uint, *logstorage.DataBlock) {}); err != nil {
		t.Fatal(err)
	}
	return e
}

func fmtQuery(t *testing.T, filter string, w fmtWindow) *logstorage.Query {
	expr := "*"
	if filter == "svc" {
		expr = `service.name:="` + fmtFilterSvc + `"`
	}
	q, err := logstorage.ParseQuery(expr)
	if err != nil {
		t.Fatal(err)
	}
	q.AddTimeFilter(w.lo, w.hi)
	return q
}

type fmtRecord struct {
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
	Files     int    `json:"files"`
	Gets      int64  `json:"gets"`
	Bytes     int64  `json:"bytes"`
	Catalog   int64  `json:"catalog_answers"`
}

func (e *fmtEnv) run(t *testing.T, endpoint, filter string, w fmtWindow, latency time.Duration) fmtRecord {
	s := e.s
	s.memCache = cache.NewLRU(64 * 1024 * 1024)
	s.footerCache = NewFooterCache(1000)
	q := fmtQuery(t, filter, w)
	e.mock.gets.Store(0)
	e.mock.bytesServed.Store(0)
	e.mock.latency.Store(int64(latency))
	cat0 := metrics.CatalogValueLookups.Get("catalog")
	ctx := context.Background()
	var (
		vals []logstorage.ValueWithHits
		err  error
	)
	start := time.Now()
	switch endpoint {
	case "fv_name":
		vals, err = s.GetFieldValues(ctx, nil, q, "name", 0)
	case "fv_service":
		vals, err = s.GetFieldValues(ctx, nil, q, "resource_attr:service.name", 0)
	case "streams":
		vals, err = s.GetStreams(ctx, nil, q, 0)
	}
	dur := time.Since(start)
	e.mock.latency.Store(0)
	if err != nil {
		t.Fatalf("%s: %v", endpoint, err)
	}
	truth := fmtTruth(e.rows, endpoint, filter, w)
	got := make(map[string]uint64, len(vals))
	setOK := true
	for _, v := range vals {
		if _, dup := got[v.Value]; dup {
			setOK = false
		}
		got[v.Value] = v.Hits
	}
	setOK = setOK && len(got) == len(truth)
	hitsOK := setOK
	for k, n := range truth {
		h, ok := got[k]
		if !ok {
			setOK, hitsOK = false, false
			break
		}
		if h != n {
			hitsOK = false
		}
	}
	return fmtRecord{
		Endpoint: endpoint, Pmeta: e.pmeta, Layout: "flushed", Window: w.name, Filter: filter,
		LatencyMs: latency.Milliseconds(), Ns: dur.Nanoseconds(), SetOK: setOK, HitsOK: hitsOK,
		Values: len(vals), Files: e.files, Gets: e.mock.gets.Load(), Bytes: e.mock.bytesServed.Load(),
		Catalog: int64(metrics.CatalogValueLookups.Get("catalog") - cat0), //nolint:gosec // a per-iteration counter delta
	}
}

func TestFieldMetadataMatrixTraces(t *testing.T) {
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
	spec := os.Getenv("FM_LATENCIES_MS")
	if spec == "" {
		spec = "0,100"
	}
	var lats []time.Duration
	for _, p := range strings.Split(spec, ",") {
		if ms, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
			lats = append(lats, time.Duration(ms)*time.Millisecond)
		}
	}
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)

	envs := []*fmtEnv{buildFmtEnv(t, true), buildFmtEnv(t, false)}
	for _, ep := range []string{"fv_name", "fv_service", "streams"} {
		for _, e := range envs {
			for _, w := range fmtWindows() {
				for _, flt := range []string{"none", "svc"} {
					for _, lat := range lats {
						for i := 0; i < warmup; i++ {
							e.run(t, ep, flt, w, lat)
						}
						for i := 0; i < iters; i++ {
							rec := e.run(t, ep, flt, w, lat)
							rec.Build, rec.Round, rec.Iter = os.Getenv("FM_BUILD"), os.Getenv("FM_ROUND"), i
							rec.Cell = fmt.Sprintf("traces.%s/pmeta=%v/layout=flushed/window=%s/filter=%s/s3=%dms", ep, e.pmeta, w.name, flt, lat.Milliseconds())
							rec.Endpoint = "traces." + ep
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
