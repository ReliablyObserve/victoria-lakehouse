package parquets3

// Cold-read profile harness (perf/cold-filtered-read, Phase 1).
//
// Deterministic, in-process: a latency-injecting S3 mock serves Parquet files
// written by the production writer (writeLogsParquet: 10k-row groups, zstd 3,
// footer token blooms, column blooms). Each query shape runs through the real
// VL query engine (vlapp.RunQuery -> external storage -> Storage.RunQuery) and
// reports wall time, S3 GETs/bytes, bytes per column, max in-flight GETs, the
// sequential round-trip chain (critical path), row-group counters, and
// allocations. CPU profiles are written per shape at 0 ms.
//
// Run: LH_COLD_PROFILE=1 PROFILE_LAT_MS=50 PROFILE_OUT=/tmp/x.jsonl \
//      GOWORK=off go test -run TestColdProfile -count=1 -timeout 60m ./internal/storage/parquets3/

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// ---------------------------------------------------------------- S3 mock

type profReq struct {
	key        string
	off, n     int64
	start, end time.Time
}

type profS3 struct {
	mu                    sync.Mutex
	files                 map[string][]byte
	srv                   *httptest.Server
	latency               time.Duration
	log                   []profReq
	inflight, maxInflight atomic.Int64
}

func newProfS3(lat time.Duration) *profS3 {
	m := &profS3{files: map[string][]byte{}, latency: lat}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *profS3) handle(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	cur := m.inflight.Add(1)
	defer m.inflight.Add(-1)
	for {
		old := m.maxInflight.Load()
		if cur <= old || m.maxInflight.CompareAndSwap(old, cur) {
			break
		}
	}
	if m.latency > 0 {
		time.Sleep(m.latency) // time to first byte: one round trip
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	key := parts[1]
	m.mu.Lock()
	data, ok := m.files[key]
	m.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>`)
		return
	}
	off, end := int64(0), int64(len(data))-1
	if rh := r.Header.Get("Range"); strings.HasPrefix(rh, "bytes=") {
		b := strings.SplitN(strings.TrimPrefix(rh, "bytes="), "-", 2)
		off, _ = strconv.ParseInt(b[0], 10, 64)
		if b[1] != "" {
			end, _ = strconv.ParseInt(b[1], 10, 64)
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, end, len(data)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-off+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
	}
	if r.Method != http.MethodHead {
		_, _ = w.Write(data[off : end+1])
	}
	m.mu.Lock()
	m.log = append(m.log, profReq{key: key, off: off, n: end - off + 1, start: start, end: time.Now()})
	m.mu.Unlock()
}

func (m *profS3) reset() []profReq {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.log
	m.log = nil
	m.maxInflight.Store(0)
	return l
}

// chainDepth is the longest chain of GETs where each starts after the previous
// one ended: the number of sequential round trips on the critical path.
func chainDepth(reqs []profReq) int {
	if len(reqs) == 0 {
		return 0
	}
	sort.Slice(reqs, func(i, j int) bool { return reqs[i].start.Before(reqs[j].start) })
	depth := make([]int, len(reqs))
	best := 0
	for i := range reqs {
		depth[i] = 1
		for j := 0; j < i; j++ {
			if !reqs[j].end.After(reqs[i].start) && depth[j]+1 > depth[i] {
				depth[i] = depth[j] + 1
			}
		}
		if depth[i] > best {
			best = depth[i]
		}
	}
	return best
}

// ---------------------------------------------------------------- byte attribution

type colSpan struct {
	name     string
	off, end int64 // [off, end)
}

func fileSpans(t testing.TB, data []byte) []colSpan {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	md := f.Metadata()
	var out []colSpan
	for _, rg := range md.RowGroups {
		for _, c := range rg.Columns {
			name := c.MetaData.PathInSchema[0]
			off := c.MetaData.DataPageOffset
			if c.MetaData.DictionaryPageOffset > 0 && c.MetaData.DictionaryPageOffset < off {
				off = c.MetaData.DictionaryPageOffset
			}
			out = append(out, colSpan{name, off, off + c.MetaData.TotalCompressedSize})
			if c.MetaData.BloomFilterOffset > 0 {
				ln := int64(c.MetaData.BloomFilterLength)
				if ln <= 0 {
					ln = 4096
				}
				out = append(out, colSpan{"~bloom", c.MetaData.BloomFilterOffset, c.MetaData.BloomFilterOffset + ln})
			}
			if c.ColumnIndexOffset > 0 {
				out = append(out, colSpan{"~colindex", c.ColumnIndexOffset, c.ColumnIndexOffset + int64(c.ColumnIndexLength)})
			}
			if c.OffsetIndexOffset > 0 {
				out = append(out, colSpan{"~offindex", c.OffsetIndexOffset, c.OffsetIndexOffset + int64(c.OffsetIndexLength)})
			}
		}
	}
	flen := int64(uint32(data[len(data)-8]) | uint32(data[len(data)-7])<<8 | uint32(data[len(data)-6])<<16 | uint32(data[len(data)-5])<<24)
	out = append(out, colSpan{"~footer", int64(len(data)) - 8 - flen, int64(len(data))})
	return out
}

func attribute(reqs []profReq, spans map[string][]colSpan) map[string]int64 {
	res := map[string]int64{}
	for _, r := range reqs {
		covered := int64(0)
		for _, s := range spans[r.key] {
			lo, hi := max64(r.off, s.off), min64(r.off+r.n, s.end)
			if hi > lo {
				res[s.name] += hi - lo
				covered += hi - lo
			}
		}
		if r.n > covered {
			res["~other"] += r.n - covered
		}
	}
	return res
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------- dataset

var profSvc = []string{"api", "web", "db", "auth", "queue", "cache", "search", "billing"}

// traceRows: n spans over 50 minutes ending 2 minutes before end; 10 services,
// 40 span names, 1/7 spans carry span_attr repro_layer=big (others "cold"),
// http.method dedicated, random trace/span ids, 6 spans per trace.
func traceRows(n int, end time.Time) []schema.TraceRow {
	rnd := rand.New(rand.NewSource(295))
	tEnd := end.Add(-2 * time.Minute).UnixNano()
	span := int64(50 * time.Minute)
	out := make([]schema.TraceRow, 0, n)
	var tid string
	for i := 0; i < n; i++ {
		if i%6 == 0 {
			tid = fmt.Sprintf("%016x%016x", rnd.Uint64(), rnd.Uint64())
		}
		ts := tEnd - span + int64(i)*span/int64(n)
		svc := profSvc[rnd.Intn(len(profSvc))]
		name := fmt.Sprintf("op-%d", rnd.Intn(40))
		layer := "cold"
		if i%7 == 0 {
			layer = "big"
		}
		out = append(out, schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts - 1_000_000), TraceID: tid, SpanID: fmt.Sprintf("%016x", rnd.Uint64()),
			SpanName: name, ServiceName: svc, DurationNs: schema.Int64Ptr(int64(1+rnd.Intn(900)) * 1_000_000), SpanKind: schema.Int32Ptr(2),
			HTTPMethod: []string{"GET", "GET", "POST", "PUT"}[rnd.Intn(4)], HTTPStatusCode: []string{"200", "200", "500"}[rnd.Intn(3)],
			Stream:             fmt.Sprintf(`{name=%q,resource_attr:service.name=%q}`, name, svc),
			StreamID:           fmt.Sprintf("%032x%016x", 0x7ace, rnd.Intn(320)),
			SpanAttributes:     map[string]string{"repro_layer": layer, "peer": fmt.Sprintf("p%d", rnd.Intn(50))},
			ResourceAttributes: map[string]string{"host": fmt.Sprintf("h%d", rnd.Intn(16))},
		})
	}
	return out
}

// ---------------------------------------------------------------- shapes

type profShape struct {
	name   string
	q      string
	tsHint bool // hits endpoint hint
	tomb   string
}

func profShapes(anchor time.Time) []profShape {
	tr := func(d time.Duration) string {
		return fmt.Sprintf("_time:[%s, %s]", anchor.Add(-d).Format(time.RFC3339), anchor.Format(time.RFC3339))
	}
	return []profShape{
		{name: "T01 name:=op-3 | count", q: `name:=op-3 | stats count() n`},
		{name: "T02 span_attr repro_layer | by(name) (unreg)", q: `"span_attr:repro_layer":=big | stats by (name) count() n`},
		{name: "T03 _time:30m http.method:=POST | count", q: tr(30*time.Minute) + ` "span_attr:http.method":=POST | stats count() n`},
		{name: "T04 * | count", q: `* | stats count() n`},
		{name: "T05 * | by(service)", q: `* | stats by ("resource_attr:service.name") count() n`},
		{name: "T06 trace_id lookup", q: `trace_id:=TRACEID | stats count() n`},
		{name: "T07 duration:>800ms | count", q: `duration:>800ms | stats count() n`},
		{name: "T08 query=* limit 1000", q: `* | sort by (_time) desc | limit 1000`},
		{name: "T09 * | count +del(unreg)", q: `* | stats count() n`, tomb: `"span_attr:peer":=p7`},
	}
}

// ---------------------------------------------------------------- counters

func counterSnapshot() map[string]float64 {
	var b bytes.Buffer
	metrics.WritePrometheus(&b, false)
	out := map[string]float64{}
	for _, l := range strings.Split(b.String(), "\n") {
		if !strings.HasPrefix(l, "lakehouse_parquet_") && !strings.HasPrefix(l, "lakehouse_s3_") && !strings.HasPrefix(l, "lakehouse_s3") {
			continue
		}
		i := strings.LastIndexByte(l, ' ')
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(l[i+1:], 64)
		if err == nil {
			out[l[:i]] = v
		}
	}
	return out
}

func counterDelta(a, b map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for k, v := range b {
		if d := v - a[k]; d != 0 && !strings.Contains(k, "_seconds") && !strings.Contains(k, "_bucket") {
			out[k] = d
		}
	}
	return out
}

// ---------------------------------------------------------------- runner

type profResult struct {
	Build, Layout, Shape, Query string
	LatMs                       int
	Answer                      string
	FirstMs                     float64
	P50Ms, P95Ms                float64
	Gets                        int
	Bytes                       int64
	MaxInflight                 int64
	Chain                       int
	ColBytes                    map[string]int64
	Counters                    map[string]float64
	AllocMB                     float64
	Mallocs                     uint64
	Files, RowGroups            int
	Truth                       string
	TotalFileBytes              int64
}

//nolint:gocyclo // a measurement driver: one linear script over layouts, shapes and metrics
func TestColdReadProfile(t *testing.T) {
	if os.Getenv("LH_COLD_PROFILE") == "" {
		t.Skip("set LH_COLD_PROFILE=1")
	}
	latMs, _ := strconv.Atoi(os.Getenv("PROFILE_LAT_MS"))
	reps, _ := strconv.Atoi(os.Getenv("PROFILE_REPS"))
	if reps <= 0 {
		reps = 9
	}
	build := os.Getenv("PROFILE_BUILD")
	layout := os.Getenv("PROFILE_LAYOUT") // "A" (10 files x 15k) or "B" (2 x 75k)
	if layout == "" {
		layout = "A"
	}
	only := os.Getenv("PROFILE_ONLY")
	cpuDir := os.Getenv("PROFILE_CPU_DIR")
	outPath := os.Getenv("PROFILE_OUT")
	exportDir := os.Getenv("PROFILE_EXPORT")
	// Fixed position inside the hour so the dt/hour file split (and therefore
	// every byte/GET count) is identical across runs and builds.
	anchor := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour).Add(40 * time.Minute)

	mock := newProfS3(time.Duration(latMs) * time.Millisecond)
	defer mock.srv.Close()

	cfg := testConfig() // production defaults (config.Default())
	cfg.Mode = config.ModeTraces
	if fm := os.Getenv("PROFILE_FETCH"); fm != "" {
		cfg.S3.ProjectedFetchMode = fm
	}
	// External mode: the same files on a real S3 (RustFS behind a probe-verified
	// toxiproxy), so LH and ClickHouse read identical objects on identical infra.
	extEP := os.Getenv("PROFILE_S3_ENDPOINT")
	pool := testPool(t, mock.srv.URL)
	var upPool *s3reader.ClientPool
	if extEP != "" {
		mk := func(ep string) *s3reader.ClientPool {
			p, err := s3reader.NewClientPool(context.Background(), &config.S3Config{Bucket: "perf", Region: "us-east-1",
				Endpoint: ep, ForcePathStyle: true, AccessKey: "minioadmin", SecretKey: "minioadmin",
				MaxConnections: 128, Timeout: 30 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			return p
		}
		pool = mk(extEP)
		upPool = mk(os.Getenv("PROFILE_S3_DIRECT"))
	}
	s := &Storage{
		cfg:         cfg,
		pool:        pool,
		manifest:    manifest.New("test-bucket", "traces/"),
		registry:    schema.NewRegistry(schema.TracesProfile),
		memCache:    cache.NewLRU(1 << 20), // no data cache: every rep reads S3
		sfGroup:     cache.NewGroup(),
		labelIndex:  cache.NewLabelIndex(),
		discovery:   discovery.New("", nil, "", "", "9428", 5*time.Second),
		footerCache: NewFooterCache(0),
		dlSem:       make(chan struct{}, 16),
	}

	// Steady state: a running node has a label index (persisted or built by an
	// earlier query). An empty one makes the FIRST query rebuild it from data
	// pages on the query path (measured separately: PROFILE_COLD_LABELS=1).
	if os.Getenv("PROFILE_COLD_LABELS") == "" {
		s.labelIndex.Add("service.name", nil)
	}
	rows := traceRows(60000, anchor)
	perFile := 10000
	if layout == "B" {
		perFile = 30000
	}
	spans := map[string][]colSpan{}
	var totalBytes int64
	rgCount := 0
	for i, n := 0, 0; i < len(rows); n++ {
		j := i + perFile
		if j > len(rows) {
			j = len(rows)
		}
		// split at hour boundaries like the flusher's dt/hour partitions
		h := time.Unix(0, rows[i].TimestampUnixNano).UTC().Truncate(time.Hour)
		k := i
		for k < j && time.Unix(0, rows[k].TimestampUnixNano).UTC().Truncate(time.Hour).Equal(h) {
			k++
		}
		chunk := rows[i:k]
		res, err := writeTracesParquet(chunk, cfg.Insert.RowGroupSize, cfg.Insert.CompressionLevel)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("traces/dt=%s/hour=%02d/f%03d.parquet", h.Format("2006-01-02"), h.Hour(), n)
		mock.files[key] = res.Data
		if upPool != nil {
			if err := upPool.Upload(context.Background(), key, res.Data); err != nil {
				t.Fatal(err)
			}
		}
		spans[key] = fileSpans(t, res.Data)
		totalBytes += int64(len(res.Data))
		rgCount += (len(chunk) + cfg.Insert.RowGroupSize - 1) / cfg.Insert.RowGroupSize
		minT, maxT := schema.TraceRowTimeBounds(chunk)
		s.manifest.AddFile(partitionFromNano(chunk[0].TimestampUnixNano), manifest.FileInfo{
			Key: key, Size: int64(len(res.Data)), RowCount: int64(len(chunk)), MinTimeNs: minT, MaxTimeNs: maxT,
			RawBytes: res.RawBytes, Labels: extractTraceLabels(chunk), LabelAggregates: schema.ExtractTraceLabelAggregates(chunk),
			ColumnBytes: res.ColumnBytes,
		})
		if exportDir != "" {
			p := filepath.Join(exportDir, "traces", key)
			_ = os.MkdirAll(filepath.Dir(p), 0o755)
			_ = os.WriteFile(p, res.Data, 0o644)
		}
		i = k
	}
	t.Logf("layout %s: files=%d rowgroups=%d bytes=%d", layout, len(mock.files), rgCount, totalBytes)
	if exportDir != "" {
		_ = os.WriteFile(filepath.Join(exportDir, "anchor.txt"), []byte(strconv.FormatInt(anchor.UnixNano(), 10)), 0o644)
	}
	truth := profTruth(rows, anchor)

	start, end := anchor.Add(-6*time.Hour).UnixNano(), anchor.Add(time.Minute).UnixNano()
	internalvlstorage.SetStorage(s, nil)
	defer vlapp.SetExternalStorage(nil)

	var out *os.File
	if outPath != "" {
		var err error
		out, err = os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = out.Close() }()
	}

	runOnce := func(sh profShape, q *logstorage.Query) (string, error) {
		ctx := context.Background()
		if sh.tsHint {
			ctx = storage.WithTimestampOnlyHint(ctx)
		}
		qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		var lines []string
		rowsOut := 0
		err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			cols := db.GetColumns(false)
			n := db.RowsCount()
			rowsOut += n
			if strings.Contains(sh.q, "limit") {
				return
			}
			for i := 0; i < n; i++ {
				var parts []string
				for _, c := range cols {
					parts = append(parts, c.Name+"="+c.Values[i])
				}
				sort.Strings(parts)
				lines = append(lines, strings.Join(parts, ","))
			}
		})
		if strings.Contains(sh.q, "limit") {
			return fmt.Sprintf("rows=%d", rowsOut), err
		}
		sort.Strings(lines)
		ans := strings.Join(lines, ";")
		if os.Getenv("PROFILE_FULLANS") != "" {
			t.Logf("FULLANS %s: %s", sh.name, ans) // the whole answer, to diff two builds
		}
		if len(ans) > 300 {
			ans = fmt.Sprintf("groups=%d hash=%x", len(lines), fnv32(ans))
		}
		return ans, err
	}

	for _, sh := range profShapes(anchor) {
		if only != "" {
			hit := false
			for _, o := range strings.Split(only, ",") {
				if strings.HasPrefix(sh.name, o+" ") || (!strings.Contains(only, ",") && strings.Contains(sh.name, o)) {
					hit = true
				}
			}
			if !hit {
				continue
			}
		}
		store := delete.NewTombstoneStore()
		if sh.tomb != "" {
			store.Add(profTombstone(sh.tomb, start, end))
		}
		s.SetTombstoneStore(store)
		q, err := logstorage.ParseQuery(strings.ReplaceAll(sh.q, "TRACEID", rows[len(rows)/2].TraceID))
		if err != nil {
			t.Fatal(err)
		}
		q.AddTimeFilter(start, end)

		// first run: cold footer cache for this shape's files is NOT reset
		// (footers stay warm across shapes, as on a running node); data never cached.
		s.footerCache = NewFooterCache(0)
		mock.reset()
		t0 := time.Now()
		var dumpTimer *time.Timer
		if d := os.Getenv("PROFILE_DUMP"); d != "" {
			dumpTimer = time.AfterFunc(5*time.Second, func() {
				f, _ := os.Create(d)
				_ = pprof.Lookup("goroutine").WriteTo(f, 2)
				_ = f.Close()
			})
		}
		ans, err := runOnce(sh, q)
		if dumpTimer != nil {
			dumpTimer.Stop()
		}
		if err != nil {
			t.Fatalf("%s: %v", sh.name, err)
		}
		first := time.Since(t0)
		mock.reset()

		var walls []float64
		var reqs []profReq
		var maxIn int64
		var ms0, ms1 runtime.MemStats
		c0 := counterSnapshot()
		runtime.GC()
		runtime.ReadMemStats(&ms0)
		for r := 0; r < reps; r++ {
			t1 := time.Now()
			a2, err := runOnce(sh, q)
			if err != nil || a2 != ans {
				t.Fatalf("%s: unstable answer %q vs %q (%v)", sh.name, a2, ans, err)
			}
			walls = append(walls, float64(time.Since(t1).Microseconds())/1000)
			if r == 0 {
				maxIn = mock.maxInflight.Load()
				reqs = mock.reset()
			}
		}
		runtime.ReadMemStats(&ms1)
		c1 := counterSnapshot()
		mock.reset()
		sort.Float64s(walls)
		var bytesRead int64
		for _, r := range reqs {
			bytesRead += r.n
		}
		extGets := 0
		if extEP != "" {
			// first warm rep only is not isolated here; use per-rep average of the GetObject counter
			d := counterDelta(c0, c1)
			for k, v := range d {
				if strings.Contains(k, "lakehouse_s3_requests_total") && strings.Contains(k, "GetObject") {
					extGets = int(v) / reps
				}
			}
		}
		res := profResult{
			Build: build, Layout: layout, Shape: sh.name, Query: sh.q, LatMs: latMs, Answer: ans,
			FirstMs: float64(first.Microseconds()) / 1000,
			P50Ms:   walls[len(walls)/2], P95Ms: walls[(len(walls)*95)/100-boolInt((len(walls)*95)%100 == 0)],
			Gets: len(reqs) + extGets, Bytes: bytesRead, MaxInflight: maxIn, Chain: chainDepth(append([]profReq(nil), reqs...)),
			ColBytes: attribute(reqs, spans), Counters: counterDelta(c0, c1),
			AllocMB: float64(ms1.TotalAlloc-ms0.TotalAlloc) / float64(reps) / (1 << 20),
			Mallocs: (ms1.Mallocs - ms0.Mallocs) / uint64(reps),
			Files:   len(mock.files), RowGroups: rgCount, TotalFileBytes: totalBytes,
		}
		if want, ok := truth[strings.Fields(sh.name)[0]]; ok {
			res.Truth = "ok"
			if ans != want {
				res.Truth = "WRONG want " + want
				t.Errorf("%s: answer %s, ground truth %s", sh.name, ans, want)
			}
		}
		for k, v := range res.Counters {
			res.Counters[k] = v / float64(reps)
		}
		t.Logf("%-42s p50=%7.1fms first=%7.1fms gets=%3d bytes=%9d chain=%2d inflight=%2d alloc=%6.1fMB ans=%s",
			sh.name, res.P50Ms, res.FirstMs, res.Gets, res.Bytes, res.Chain, res.MaxInflight, res.AllocMB, ans)
		if out != nil {
			b, _ := json.Marshal(res)
			_, _ = out.Write(append(b, '\n'))
		}

		if cpuDir != "" {
			f, err := os.Create(filepath.Join(cpuDir, fmt.Sprintf("%s-%s-%s.pprof", build, layout, strings.Fields(sh.name)[0])))
			if err != nil {
				t.Fatal(err)
			}
			_ = pprof.StartCPUProfile(f)
			deadline := time.Now().Add(3 * time.Second)
			for n := 0; n < 3 || time.Now().Before(deadline); n++ {
				if _, err := runOnce(sh, q); err != nil {
					t.Fatal(err)
				}
			}
			pprof.StopCPUProfile()
			_ = f.Close()
			mock.reset()
		}
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func profTombstone(q string, start, end int64) delete.Tombstone {
	return delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "prof", Query: q, StartNs: start, EndNs: end, Mode: "hide"}
}

// profTruth computes the exact answers of the count shapes from the rows.
func profTruth(rows []schema.TraceRow, anchor time.Time) map[string]string {
	cnt := func(pred func(r *schema.TraceRow) bool) string {
		n := 0
		for i := range rows {
			if pred(&rows[i]) {
				n++
			}
		}
		return "n=" + strconv.Itoa(n)
	}
	a30, an := anchor.Add(-30*time.Minute).UnixNano(), anchor.UnixNano()
	tid := rows[len(rows)/2].TraceID
	return map[string]string{
		"T01": cnt(func(r *schema.TraceRow) bool { return r.SpanName == "op-3" }),
		"T03": cnt(func(r *schema.TraceRow) bool {
			return r.HTTPMethod == "POST" && r.TimestampUnixNano >= a30 && r.TimestampUnixNano <= an
		}),
		"T04": cnt(func(r *schema.TraceRow) bool { return true }),
		"T06": cnt(func(r *schema.TraceRow) bool { return r.TraceID == tid }),
		"T07": cnt(func(r *schema.TraceRow) bool { return schema.Int64Value(r.DurationNs) > 800_000_000 }),
		"T09": cnt(func(r *schema.TraceRow) bool { return r.SpanAttributes["peer"] != "p7" }),
	}
}
