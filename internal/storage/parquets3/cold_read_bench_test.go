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
//      GOWORK=off go test -run TestColdReadProfile -count=1 -timeout 60m ./internal/storage/parquets3/

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
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
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

var profWords = []string{"connection", "timeout", "refused", "retry", "upstream", "handled", "request", "payload", "cache", "miss", "hit", "latency", "slow", "query", "ok"}
var profSvc = []string{"api", "web", "db", "auth", "queue", "cache", "search", "billing"}
var profLvl = []string{"info", "info", "info", "warn", "error", "debug"}

// bigmarkRows mirrors the #295 proof seed (seed2.py): `rows` bulk rows over
// 50 minutes ending 2 minutes before end, 1/7 carry BIGMARK, 14 random words,
// plus 90 marker rows (MARKER273 / needle273) 8 minutes before end.
func bigmarkRows(rows int, end time.Time) []schema.LogRow {
	rnd := rand.New(rand.NewSource(273))
	tEnd := end.Add(-2 * time.Minute).UnixNano()
	span := int64(50 * time.Minute)
	out := make([]schema.LogRow, 0, rows+90)
	for i := 0; i < rows; i++ {
		ts := tEnd - span + int64(i)*span/int64(rows)
		var sb strings.Builder
		if i%7 == 0 {
			sb.WriteString("BIGMARK")
		} else {
			sb.WriteString("bulk")
		}
		for k := 0; k < 14; k++ {
			sb.WriteByte(' ')
			sb.WriteString(profWords[rnd.Intn(len(profWords))])
		}
		fmt.Fprintf(&sb, " id=%d sess=%012x", i, rnd.Int63()&0xffffffffffff)
		svc := profSvc[rnd.Intn(len(profSvc))]
		lvl := profLvl[rnd.Intn(len(profLvl))]
		host := fmt.Sprintf("host-%d", rnd.Intn(16))
		tid := fmt.Sprintf("%016x%016x", rnd.Uint64(), rnd.Uint64())
		status := []string{"200", "200", "200", "404", "500"}[rnd.Intn(5)]
		dur := strconv.Itoa(1 + rnd.Intn(900))
		out = append(out, schema.LogRow{
			TimestampUnixNano: ts, Body: sb.String(), SeverityText: lvl, ServiceName: svc, HostName: host, TraceID: tid,
			Stream:        fmt.Sprintf(`{repro_layer="big",service.name=%q}`, svc),
			StreamID:      fmt.Sprintf("%032x%016x", 0xb16, len(svc)*7919+int(svc[0])),
			LogAttributes: map[string]string{"repro_layer": "big", "status": status, "dur_ms": dur},
		})
	}
	t0 := end.Add(-8 * time.Minute).UnixNano()
	svc3 := []string{"alpha", "beta", "gamma"}
	for i := 0; i < 90; i++ {
		body := fmt.Sprintf("MARKER273 request %d from %s", i, svc3[i%3])
		if i%9 == 0 {
			body = "needle273"
		}
		lvl := "info"
		if i%2 == 1 {
			lvl = "error"
		}
		out = append(out, schema.LogRow{
			TimestampUnixNano: t0 + int64(i)*2_000_000_000, Body: body, SeverityText: lvl, ServiceName: svc3[i%3],
			TraceID:       fmt.Sprintf("cc%02d", i%5) + strings.Repeat("a", 28),
			Stream:        fmt.Sprintf(`{repro_layer="cold",service.name=%q}`, svc3[i%3]),
			StreamID:      fmt.Sprintf("%032x%016x", 0xc01d, i%3),
			LogAttributes: map[string]string{"repro_layer": "cold"},
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].TimestampUnixNano < out[j].TimestampUnixNano })
	return out
}

// ---------------------------------------------------------------- shapes

type profShape struct {
	name   string
	q      string
	tsHint bool // hits endpoint hint
	tomb   string
	kind   string // "" = RunQuery; field_names, field_values, streams
}

func profShapes(anchor time.Time) []profShape {
	tr := func(d time.Duration) string {
		return fmt.Sprintf("_time:[%s, %s]", anchor.Add(-d).Format(time.RFC3339), anchor.Format(time.RFC3339))
	}
	return []profShape{
		{name: "L01 _time:50m BIGMARK level:=error | count", q: tr(50*time.Minute) + ` BIGMARK level:=error | stats count() n`},
		{name: "L02 BIGMARK | count", q: `BIGMARK | stats count() n`},
		{name: "L03 {stream} BIGMARK | count", q: `{service.name="api",repro_layer="big"} BIGMARK | stats count() n`},
		{name: "L04 BIGMARK | by(service.name)", q: `BIGMARK | stats by (service.name) count() n`},
		{name: "L05 BIGMARK | by(level,host.name)", q: `BIGMARK | stats by (level, host.name) count() n`},
		{name: "L06 BIGMARK | by(repro_layer)", q: `BIGMARK | stats by (repro_layer) count() n`},
		{name: "L07 * | by(repro_layer)", q: `* | stats by (repro_layer) count() n`},
		{name: "L08 _msg:=needle273 | count", q: tr(30*time.Minute) + ` _msg:="needle273" | stats count() n`},
		{name: "L09 level:=error | count", q: `level:=error | stats count() n`},
		{name: "L10 level:=error | by(service.name)", q: `level:=error | stats by (service.name) count() n`},
		{name: "L11 status:=500 | count (unreg filter)", q: `status:=500 | stats count() n`},
		{name: "L12 status:=500 | by(dur_ms) (unreg+by)", q: `status:=500 | stats by (dur_ms) count() n`},
		{name: "L13 * | count", q: `* | stats count() n`},
		{name: "L14 * | by(service.name)", q: `* | stats by (service.name) count() n`},
		{name: "L15 hits *", q: `* | stats by (_time:5m) count() hits`, tsHint: true},
		{name: "L16 hits BIGMARK", q: `BIGMARK | stats by (_time:5m) count() hits`, tsHint: true},
		{name: "L17 query=* limit 1000", q: `* | sort by (_time) desc | limit 1000`},
		{name: "L18 BIGMARK limit 1000", q: `BIGMARK | sort by (_time) desc | limit 1000`},
		{name: "L19 * | limit 1000 (no sort)", q: `* | limit 1000`},
		{name: "L20 hits * (no hint, as wired today)", q: `* | stats by (_time:5m) count() hits`},
		{name: "L21 trace_id:=X | count (log-to-trace correlation)", q: `trace_id:=TRACEID | stats count() n`},
		{name: "F1 field_names *", q: `*`, kind: "field_names"},
		{name: "F2 field_values level", q: `*`, kind: "field_values"},
		{name: "F3 streams *", q: `*`, kind: "streams"},
		{name: "F4 field_names BIGMARK", q: `BIGMARK`, kind: "field_names"},
		{name: "D1 * | count +del(reg host.name)", q: `* | stats count() n`, tomb: `host.name:=host-3`},
		{name: "D2 * | count +del(unreg dur_ms)", q: `* | stats count() n`, tomb: `dur_ms:=500`},
		{name: "D3 BIGMARK | count +del(reg)", q: `BIGMARK | stats count() n`, tomb: `host.name:=host-3`},
		{name: "D4 BIGMARK | count +del(unreg)", q: `BIGMARK | stats count() n`, tomb: `dur_ms:=500`},
		{name: "D5 * | by(service.name) +del(unreg)", q: `* | stats by (service.name) count() n`, tomb: `dur_ms:=500`},
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
	if a, err := strconv.ParseInt(os.Getenv("PROFILE_ANCHOR_NS"), 10, 64); err == nil && a > 0 {
		anchor = time.Unix(0, a).UTC()
	}

	mock := newProfS3(time.Duration(latMs) * time.Millisecond)
	defer mock.srv.Close()

	cfg := testConfig() // production defaults (config.Default()), logs mode
	if fm := os.Getenv("PROFILE_FETCH"); fm != "" {
		cfg.S3.ProjectedFetchMode = fm
	}
	if rm := os.Getenv("PROFILE_READMODE"); rm != "" {
		cfg.S3.ParquetReadMode = rm
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
		manifest:    manifest.New("test-bucket", "logs/"),
		registry:    schema.NewRegistry(schema.LogsProfile),
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
	rows := bigmarkRows(150000, anchor)
	perFile := 15000
	if layout == "B" {
		perFile = 75045
	}
	if nf, err := strconv.Atoi(os.Getenv("PROFILE_FILES")); err == nil && nf > 0 {
		perFile = (150090 + nf - 1) / nf
		layout = layout + strconv.Itoa(nf)
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
		res, err := writeLogsParquet(chunk, cfg.Insert.RowGroupSize, cfg.Insert.CompressionLevel)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("logs/dt=%s/hour=%02d/f%03d.parquet", h.Format("2006-01-02"), h.Hour(), n)
		mock.files[key] = res.Data
		if upPool != nil {
			if err := upPool.Upload(context.Background(), key, res.Data); err != nil {
				t.Fatal(err)
			}
		}
		spans[key] = fileSpans(t, res.Data)
		totalBytes += int64(len(res.Data))
		rgCount += (len(chunk) + cfg.Insert.RowGroupSize - 1) / cfg.Insert.RowGroupSize
		minT, maxT := schema.LogRowTimeBounds(chunk)
		s.manifest.AddFile(partitionFromKey(key), manifest.FileInfo{
			Key: key, Size: int64(len(res.Data)), RowCount: int64(len(chunk)), MinTimeNs: minT, MaxTimeNs: maxT,
			RawBytes: res.RawBytes, Labels: extractLogLabels(chunk), LabelAggregates: schema.ExtractLogLabelAggregates(chunk),
			ColumnBytes: res.ColumnBytes,
		})
		if exportDir != "" {
			p := filepath.Join(exportDir, "logs", key)
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
		if sh.kind != "" {
			var vals []logstorage.ValueWithHits
			var err error
			switch sh.kind {
			case "field_names":
				vals, err = vlapp.GetFieldNames(qctx, "")
			case "field_values":
				vals, err = vlapp.GetFieldValues(qctx, "level", "", 1000)
			case "streams":
				vals, err = vlapp.GetStreams(qctx, 1000)
			}
			var parts []string
			for _, v := range vals {
				parts = append(parts, fmt.Sprintf("%s=%d", v.Value, v.Hits))
			}
			sort.Strings(parts)
			ans := strings.Join(parts, ";")
			if len(ans) > 300 {
				ans = fmt.Sprintf("values=%d hash=%x", len(parts), fnv32(ans))
			}
			return ans, err
		}
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
		if os.Getenv("PROFILE_DUMPREQ") != "" {
			sort.Slice(reqs, func(i, j int) bool { return reqs[i].start.Before(reqs[j].start) })
			for _, r := range reqs {
				if !strings.HasSuffix(r.key, os.Getenv("PROFILE_DUMPREQ")) {
					continue
				}
				a := attribute([]profReq{r}, spans)
				t.Logf("  REQ %s off=%d len=%d t=%.2fms cols=%v", r.key, r.off, r.n, float64(r.start.Sub(reqs[0].start).Microseconds())/1000, a)
			}
		}
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
func profTruth(rows []schema.LogRow, anchor time.Time) map[string]string {
	hasWord := func(body, w string) bool {
		for _, f := range strings.FieldsFunc(body, func(r rune) bool { return r == ' ' || r == '=' }) {
			if f == w {
				return true
			}
		}
		return false
	}
	cnt := func(pred func(r *schema.LogRow) bool) string {
		n := 0
		for i := range rows {
			if pred(&rows[i]) {
				n++
			}
		}
		return "n=" + strconv.Itoa(n)
	}
	a50, a30, an := anchor.Add(-50*time.Minute).UnixNano(), anchor.Add(-30*time.Minute).UnixNano(), anchor.UnixNano()
	big := func(r *schema.LogRow) bool { return hasWord(r.Body, "BIGMARK") }
	tid := rows[len(rows)/2].TraceID
	return map[string]string{
		"L01": cnt(func(r *schema.LogRow) bool {
			return big(r) && r.SeverityText == "error" && r.TimestampUnixNano >= a50 && r.TimestampUnixNano <= an
		}),
		"L02": cnt(big),
		"L08": cnt(func(r *schema.LogRow) bool {
			return r.Body == "needle273" && r.TimestampUnixNano >= a30 && r.TimestampUnixNano <= an
		}),
		"L09": cnt(func(r *schema.LogRow) bool { return r.SeverityText == "error" }),
		"L21": cnt(func(r *schema.LogRow) bool { return r.TraceID == tid }),
		"L11": cnt(func(r *schema.LogRow) bool { return r.LogAttributes["status"] == "500" }),
		"L13": cnt(func(r *schema.LogRow) bool { return true }),
		"D1":  cnt(func(r *schema.LogRow) bool { return r.HostName != "host-3" }),
		"D2":  cnt(func(r *schema.LogRow) bool { return r.LogAttributes["dur_ms"] != "500" }),
		"D3":  cnt(func(r *schema.LogRow) bool { return big(r) && r.HostName != "host-3" }),
		"D4":  cnt(func(r *schema.LogRow) bool { return big(r) && r.LogAttributes["dur_ms"] != "500" }),
	}
}
