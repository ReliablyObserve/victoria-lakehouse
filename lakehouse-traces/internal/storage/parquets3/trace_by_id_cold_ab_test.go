package parquets3

// Trace-by-ID cold A/B harness (field-presence footer KV, design D8). Skipped
// unless TBID_AB_OUT is set.
//
// A cold trace-by-ID reads every object's footer (the _trace_idx prefilter) and
// then the row groups of the object holding the trace. Whatever a writer adds to
// the footer is paid there, so this measures the query that is most sensitive to
// footer growth: wall time, S3 GETs and bytes, with a fresh Storage (empty
// footer cache) per run, S3 latency injected and verified by a probe, every
// answer validated against the spans written.
//
// Run the same file against two builds and compare the JSONL:
// TBID_BODY_BYTES adds an incompressible message of that size to every span, so the
// footer is a production-like small share of the object (a footer larger than the
// prefetch tail, max(64 KiB, size/8) up to 640 KiB, is fetched in a second read).
//   TBID_AB_OUT=/tmp/x.jsonl TBID_BUILD=main TBID_STREAMS=1000 TBID_REPEATS=5 \
//     GOWORK=off go test -run TestTraceByIDColdAB -count=1 ./internal/storage/parquets3/

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	vlapp "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

type tbidRecord struct {
	Build        string  `json:"build"`
	Round        string  `json:"round"`
	Streams      int     `json:"streams"`
	NamesPerSpan int     `json:"names_per_stream"`
	Objects      int     `json:"objects"`
	ObjectBytes  int64   `json:"object_bytes"`
	FooterBytes  int64   `json:"footer_bytes"`
	LatencyMs    int64   `json:"s3_latency_ms"`
	ProbeMs      float64 `json:"probe_ms"`
	Iter         int     `json:"iter"`
	Ns           int64   `json:"ns"`
	Gets         int     `json:"gets"`
	Bytes        int64   `json:"bytes"`
	Chain        int     `json:"chain"` // longest chain of data GETs each starting after the previous ended: the round trips on the critical path
	Spans        int     `json:"spans"`
	WantSpans    int     `json:"want_spans"`
	Valid        bool    `json:"valid"`
}

// tbidSpans builds nSpans spans in 2-span traces over `streams` services; each
// service draws its attribute names from its own pool of namesPerStream names out
// of a universe of `universe` distinct attribute names.
func tbidSpans(rng *rand.Rand, anchor time.Time, nSpans, streams, namesPerStream, universe, bodyBytes int) []schema.TraceRow {
	pools := make([][]string, streams)
	for s := range pools {
		pools[s] = make([]string, namesPerStream)
		for j := range pools[s] {
			pools[s][j] = fmt.Sprintf("attr.k%d", (s*7919+j*131)%universe)
		}
	}
	rows := make([]schema.TraceRow, nSpans)
	var tid string
	for i := range rows {
		if i%2 == 0 {
			tid = fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64())
		}
		s := rng.Intn(streams)
		ts := anchor.Add(time.Duration(i) * 100 * time.Millisecond).UnixNano()
		attrs := make(map[string]string, 8)
		for len(attrs) < 8 {
			attrs[pools[s][rng.Intn(namesPerStream)]] = "v"
		}
		rows[i] = schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: ts - 1_000_000, TraceID: tid, SpanID: fmt.Sprintf("%016x", rng.Uint64()),
			SpanName: fmt.Sprintf("op-%d", s%40), ServiceName: fmt.Sprintf("svc-%d", s), DurationNs: int64(1+rng.Intn(900)) * 1_000_000,
			SpanKind: 2, Stream: fmt.Sprintf(`{resource_attr:service.name="svc-%d"}`, s), StreamID: fmt.Sprintf("%032x%016x", 0x7ace, s),
			SpanAttributes: attrs,
		}
		if bodyBytes > 0 { // incompressible text, so objects are as big as production ones relative to their footers
			b := make([]byte, bodyBytes/2)
			_, _ = rng.Read(b)
			rows[i].Body = fmt.Sprintf("%x", b)
		}
	}
	return rows
}

func tbidProbe(url string) float64 {
	var best time.Duration
	for i := 0; i < 5; i++ {
		t0 := time.Now()
		resp, err := http.Get(url + "/probe-bucket/probe")
		if err == nil {
			_ = resp.Body.Close()
		}
		if d := time.Since(t0); i == 0 || d < best {
			best = d
		}
	}
	return float64(best.Microseconds()) / 1000
}

func TestTraceByIDColdAB(t *testing.T) {
	out := os.Getenv("TBID_AB_OUT")
	if out == "" {
		t.Skip("set TBID_AB_OUT to run the trace-by-ID cold A/B")
	}
	atoi := func(k string, def int) int {
		if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
			return v
		}
		return def
	}
	streams, names, repeats := atoi("TBID_STREAMS", 1000), atoi("TBID_NAMES", 300), atoi("TBID_REPEATS", 5)
	universe, bodyBytes := atoi("TBID_UNIVERSE", 2000), atoi("TBID_BODY_BYTES", 0)
	nObjs, spansPerObj := atoi("TBID_OBJECTS", 3), atoi("TBID_SPANS", 24000)
	var lats []time.Duration
	for _, f := range strings.Split(os.Getenv("TBID_LATENCIES_MS"), ",") {
		if v, err := strconv.Atoi(strings.TrimSpace(f)); err == nil {
			lats = append(lats, time.Duration(v)*time.Millisecond)
		}
	}
	if len(lats) == 0 {
		lats = []time.Duration{0, 20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}
	}

	mock := newCountS3()
	t.Cleanup(mock.Close)
	rng := rand.New(rand.NewSource(5))
	anchor := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	type obj struct {
		fi   manifest.FileInfo
		part string
	}
	var objs []obj
	var objBytes, footerBytes int64
	var target string
	var wantSpans int
	for i := 0; i < nObjs; i++ {
		rows := tbidSpans(rng, anchor.Add(time.Duration(i)*time.Hour), spansPerObj, streams, names, universe, bodyBytes)
		res, err := writeTracesParquet(rows, 10000, 3)
		if err != nil {
			t.Fatal(err)
		}
		if os.Getenv("TBID_LAYOUT") == "compacted" {
			// The same spans as the compaction writer writes them: what a cold
			// trace-by-ID mostly reads.
			tbidInstallComputers()
			merged := append([]schema.TraceRow(nil), rows...)
			sort.SliceStable(merged, func(a, b int) bool { return merged[a].TimestampUnixNano < merged[b].TimestampUnixNano })
			data, err := compaction.WriteTraces(merged, 10000, 3)
			if err != nil {
				t.Fatal(err)
			}
			res = &flushResult{Data: data, RawBytes: res.RawBytes, ColumnBytes: res.ColumnBytes}
		}
		key := fmt.Sprintf("traces/dt=2026-06-01/hour=%02d/f%03d.parquet", 10+i, i)
		mock.Put("test-bucket", key, res.Data)
		minT, maxT := schema.TraceRowTimeBounds(rows)
		fi := manifest.FileInfo{
			Key: key, Size: int64(len(res.Data)), RowCount: int64(len(rows)), MinTimeNs: minT, MaxTimeNs: maxT,
			RawBytes: res.RawBytes, Labels: extractTraceLabels(rows), LabelAggregates: schema.ExtractTraceLabelAggregates(rows),
			ColumnBytes: res.ColumnBytes,
		}
		if os.Getenv("TBID_HINT") != "0" { // 0: a listing-adopted file, no recorded footer-tail length
			tbidFooterHint(&fi, res.Data)
		}
		objs = append(objs, obj{part: partitionFromNano(rows[0].TimestampUnixNano), fi: fi})
		objBytes += int64(len(res.Data))
		d := res.Data
		footerBytes += int64(uint32(d[len(d)-8]) | uint32(d[len(d)-7])<<8 | uint32(d[len(d)-6])<<16 | uint32(d[len(d)-5])<<24)
		if i == nObjs/2 {
			target = rows[len(rows)/2].TraceID
			for _, r := range rows {
				if r.TraceID == target {
					wantSpans++
				}
			}
		}
	}
	lo, hi := anchor.Add(-time.Hour).UnixNano(), anchor.Add(time.Duration(nObjs+1)*time.Hour).UnixNano()

	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)

	run := func() (int, error) {
		s := testStorageWithS3(t, mock.URL())
		s.cfg.S3.ReadAheadBytes = 2 << 20
		s.cfg.S3.ReadAheadMaxBytes = 8 << 20
		s.cfg.S3.CoalesceGapBytes = 1 << 20
		s.cfg.S3.ProjectedFetchMode = config.ProjectedFetchModePlanned
		s.cfg.S3.WholeFileThresholdBytes = 1
		for _, o := range objs {
			s.manifest.AddFile(o.part, o.fi)
		}
		internalvlstorage.SetStorage(s, nil)
		defer vlapp.SetExternalStorage(nil)
		q := mustParseQueryWithTime(t, fmt.Sprintf(`trace_id:=%q`, target), lo, hi)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		n := 0
		err := vlapp.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			n += db.RowsCount()
			mu.Unlock()
		})
		return n, err
	}
	for _, lat := range lats {
		mock.SetLatency(lat)
		probe := tbidProbe(mock.URL())
		if _, err := run(); err != nil { // warm the process, not a cache
			t.Fatal(err)
		}
		for it := 0; it < repeats; it++ {
			mock.Reset()
			t0 := time.Now()
			n, err := run()
			ns := time.Since(t0).Nanoseconds()
			if err != nil {
				t.Fatal(err)
			}
			var gets int
			var bytesRead int64
			var dataGets []countReq
			for _, r := range mock.Reset() {
				if strings.HasPrefix(r.Op, "GET") && r.Class == "data" {
					dataGets = append(dataGets, r)
					gets++
					bytesRead += r.Out
					if os.Getenv("TBID_DUMP") != "" && it == 0 {
						fmt.Fprintf(os.Stderr, "GET lat=%dms %s out=%d\n", lat.Milliseconds(), r.Key, r.Out)
					}
				}
			}
			rec := tbidRecord{Build: os.Getenv("TBID_BUILD"), Round: os.Getenv("TBID_ROUND"), Streams: streams, NamesPerSpan: names,
				Objects: nObjs, ObjectBytes: objBytes, FooterBytes: footerBytes, LatencyMs: lat.Milliseconds(), ProbeMs: probe,
				Iter: it, Ns: ns, Gets: gets, Bytes: bytesRead, Chain: chainOf(dataGets), Spans: n, WantSpans: wantSpans, Valid: n == wantSpans && wantSpans > 0}
			if err := enc.Encode(rec); err != nil {
				t.Fatal(err)
			}
		}
	}
}
