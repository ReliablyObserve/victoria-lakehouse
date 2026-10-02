// durbench: ingest and exact-row verification probe for Lakehouse logs and
// traces, used by scripts/bench/compaction. Every row carries a run id and a
// unique sequence number; the acknowledged sequence ranges are written to a
// file so a later "verify" can list acknowledged rows that are missing,
// returned twice, or present without an acknowledgement.
//
//	durbench -mode ingest -signal logs   -url http://127.0.0.1:39701 -run R -total 40 -batch 40 -conc 1 -late 72h -account 1001 -acks R.acks
//	durbench -mode verify -signal logs   -url http://127.0.0.1:39701 -run R -account 1001 -acks R.acks
//
// Logs go to /insert/jsonline with the run id as a stream field; traces go to
// /insert/opentelemetry/v1/traces as OTLP JSON with run and seq span attributes.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	mode    = flag.String("mode", "ingest", "ingest | verify")
	signal  = flag.String("signal", "logs", "logs | traces")
	target  = flag.String("url", "http://127.0.0.1:39201", "lakehouse URL")
	dur     = flag.Duration("dur", 60*time.Second, "ingest duration")
	conc    = flag.Int("conc", 16, "concurrent writers")
	batch   = flag.Int("batch", 500, "rows per request")
	rate    = flag.Int("rate", 0, "target rows/s in total (0 = as fast as possible)")
	run     = flag.String("run", "", "run id")
	acks    = flag.String("acks", "", "file of acknowledged seq ranges (written by ingest, read by verify)")
	lateBy  = flag.Duration("late", 0, "stamp rows this far in the past")
	account = flag.String("account", "0", "AccountID header")
	streams = flag.Int("streams", 20, "distinct service names")
	total   = flag.Int64("total", 0, "stop after this many rows (0 = run for -dur)")
)

var client = &http.Client{Timeout: 30 * time.Second}

func main() {
	flag.Parse()
	if *run == "" {
		*run = fmt.Sprintf("r%d", time.Now().UnixNano())
	}
	switch *mode {
	case "ingest":
		ingest()
	case "verify":
		verify()
	default:
		fmt.Fprintln(os.Stderr, "unknown mode")
		os.Exit(2)
	}
}

func body(seq0 int64, n int) ([]byte, string, string) {
	now := time.Now().Add(-*lateBy)
	var b bytes.Buffer
	if *signal == "logs" {
		for i := 0; i < n; i++ {
			seq := seq0 + int64(i)
			ts := now.Add(time.Duration(i) * time.Microsecond).UTC().Format(time.RFC3339Nano)
			fmt.Fprintf(&b, `{"_time":%q,"_msg":"durbench row seq=%d user=u%d path=/api/v1/items/%d status=200 took=%dms","run":%q,"seq":"%d","service.name":"svc-%d","level":"info"}`+"\n",
				ts, seq, seq%997, seq%5000, seq%300, *run, seq, seq%int64(*streams))
		}
		return b.Bytes(), "/insert/jsonline?_stream_fields=service.name,run", "application/x-ndjson"
	}
	type kv struct {
		Key   string         `json:"key"`
		Value map[string]any `json:"value"`
	}
	sv := func(s string) map[string]any { return map[string]any{"stringValue": s} }
	bySvc := map[int64][]map[string]any{}
	for i := 0; i < n; i++ {
		seq := seq0 + int64(i)
		st := now.Add(time.Duration(i) * time.Microsecond).UnixNano()
		// Deterministic per (run, seq) so the same spans can be sent to two stores.
		h := fnv.New64a()
		_, _ = h.Write([]byte(*run))
		tid := fmt.Sprintf("%016x%016x", h.Sum64(), uint64(seq/4)) // 4 spans per trace
		bySvc[seq%int64(*streams)] = append(bySvc[seq%int64(*streams)], map[string]any{
			"traceId": tid, "spanId": fmt.Sprintf("%016x", uint64(seq)+1), "parentSpanId": parentOf(seq), "name": fmt.Sprintf("GET /api/v1/items/%d", seq%50), "kind": 2,
			"startTimeUnixNano": strconv.FormatInt(st, 10), "endTimeUnixNano": strconv.FormatInt(st+int64(seq%300)*1e6, 10),
			"attributes": []kv{{"run", sv(*run)}, {"seq", sv(strconv.FormatInt(seq, 10))}, {"http.method", sv("GET")}},
		})
	}
	var rs []map[string]any
	for svc, spans := range bySvc {
		rs = append(rs, map[string]any{
			"resource":   map[string]any{"attributes": []kv{{"service.name", sv(fmt.Sprintf("svc-%d", svc))}}},
			"scopeSpans": []map[string]any{{"scope": map[string]any{"name": "durbench"}, "spans": spans}},
		})
	}
	_ = json.NewEncoder(&b).Encode(map[string]any{"resourceSpans": rs})
	return b.Bytes(), "/insert/opentelemetry/v1/traces", "application/json"
}

// parentOf links the 4 spans of a trace: the first is the root.
func parentOf(seq int64) string {
	if seq%4 == 0 {
		return ""
	}
	return fmt.Sprintf("%016x", uint64(seq-seq%4)+1)
}

func ingest() {
	var next atomic.Int64
	var okRows, failReq, okReq, okBytes atomic.Int64
	var mu sync.Mutex
	var acked [][2]int64
	var lats []time.Duration
	deadline := time.Now().Add(*dur)
	var tick <-chan time.Time
	var tokens chan struct{}
	if *rate > 0 {
		reqPerSec := float64(*rate) / float64(*batch)
		tokens = make(chan struct{}, *conc)
		t := time.NewTicker(time.Duration(float64(time.Second) / reqPerSec))
		tick = t.C
		go func() {
			for range tick {
				select {
				case tokens <- struct{}{}:
				default:
				}
			}
		}()
	}
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if *total > 0 && next.Load() >= *total {
					return
				}
				if tokens != nil {
					select {
					case <-tokens:
					case <-time.After(time.Until(deadline)):
						return
					}
				}
				seq0 := next.Add(int64(*batch)) - int64(*batch)
				if *total > 0 && seq0 >= *total {
					return
				}
				data, path, ct := body(seq0, *batch)
				req, _ := http.NewRequest("POST", *target+path, bytes.NewReader(data))
				req.Header.Set("Content-Type", ct)
				req.Header.Set("AccountID", *account)
				t0 := time.Now()
				resp, err := client.Do(req)
				if err != nil {
					failReq.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= 300 {
					failReq.Add(1)
					continue
				}
				el := time.Since(t0)
				okReq.Add(1)
				okRows.Add(int64(*batch))
				okBytes.Add(int64(len(data)))
				mu.Lock()
				acked = append(acked, [2]int64{seq0, seq0 + int64(*batch)})
				lats = append(lats, el)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	el := time.Since(start).Seconds()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p := func(q float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(q*float64(len(lats)-1))]
	}
	if *acks != "" {
		f, _ := os.Create(*acks)
		w := bufio.NewWriter(f)
		for _, a := range acked {
			fmt.Fprintf(w, "%d %d\n", a[0], a[1])
		}
		_ = w.Flush()
		_ = f.Close()
	}
	fmt.Printf("{\"signal\":%q,\"run\":%q,\"rows_acked\":%d,\"req_ok\":%d,\"req_failed\":%d,\"seconds\":%.1f,\"rows_per_s\":%.0f,\"p50_ms\":%.1f,\"p99_ms\":%.1f,\"bytes_acked\":%d}\n",
		*signal, *run, okRows.Load(), okReq.Load(), failReq.Load(), el, float64(okRows.Load())/el,
		float64(p(0.5).Microseconds())/1000, float64(p(0.99).Microseconds())/1000, okBytes.Load())
}

func verify() {
	f, err := os.Open(*acks)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	want := map[int64]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var a, b int64
		fmt.Sscanf(sc.Text(), "%d %d", &a, &b)
		for s := a; s < b; s++ {
			want[s] = true
		}
	}
	runField, seqField := "run", "_msg"
	if *signal == "traces" {
		runField, seqField = "span_attr:run", "span_attr:seq"
	}
	q := fmt.Sprintf(`%q:=%q | fields %q`, runField, *run, seqField)
	if *signal == "logs" {
		// A stream selector: a field filter plus a projection misses cold rows on main.
		q = fmt.Sprintf(`{run=%q} | fields _msg`, *run)
	}
	u := *target + "/select/logsql/query?" + url.Values{"query": {q}, "start": {"0"}, "end": {strconv.FormatInt(time.Now().Add(48*time.Hour).Unix(), 10)}, "disable_latency_offset": {"true"}}.Encode()
	req, _ := http.NewRequest("GET", u, nil)
	req.Header.Set("AccountID", *account)
	resp, err := (&http.Client{Timeout: 5 * time.Minute}).Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	got := map[int64]int{}
	rd := bufio.NewReaderSize(resp.Body, 1<<20)
	for {
		line, err := rd.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var m map[string]string
			if json.Unmarshal(line, &m) == nil {
				v := m[seqField]
				if i := strings.Index(v, "seq="); i >= 0 {
					v = v[i+4:]
					if j := strings.IndexByte(v, ' '); j >= 0 {
						v = v[:j]
					}
				}
				if s, e := strconv.ParseInt(v, 10, 64); e == nil {
					got[s]++
				}
			}
		}
		if err != nil {
			break
		}
	}
	missing, dup, extra := []int64{}, 0, 0
	for s := range want {
		if got[s] == 0 {
			missing = append(missing, s)
		}
	}
	for s, n := range got {
		if n > 1 {
			dup += n - 1
		}
		if !want[s] {
			extra++
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	rng := ""
	if len(missing) > 0 {
		rng = fmt.Sprintf("%d..%d", missing[0], missing[len(missing)-1])
	}
	fmt.Printf("{\"signal\":%q,\"run\":%q,\"status\":%d,\"acked\":%d,\"returned_distinct\":%d,\"missing_acked\":%d,\"missing_range\":%q,\"duplicates\":%d,\"unacked_present\":%d}\n",
		*signal, *run, resp.StatusCode, len(want), len(got), len(missing), rng, dup, extra)
}
