//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"

	im "github.com/ReliablyObserve/victoria-lakehouse/tests/ingestmatrix"
)

// The ingest parity matrix: every write protocol the pinned VictoriaLogs and
// VictoriaTraces accept, sent unchanged to the hot binary and to Lakehouse.
//
// For every (case, tenant form) the test proves, in order:
//
//	ingest    both answer the same status and body (or both refuse the payload);
//	counters  the upstream rows-ingested series moves on both, and Lakehouse's
//	          own insert counter moves;
//	buffer    Lakehouse's rows equal hot's rows, field for field, straight after
//	          the write (the unflushed buffer);
//	parquet   after the flush the tenant's Parquet objects hold exactly those
//	          rows (read with a plain Parquet reader), and once the rows have left
//	          the insert buffer (so the read is answered from Parquet) Lakehouse
//	          still equals hot.
//
// The case table, the payload builders and the tenant forms live in
// tests/ingestmatrix; tests/conformance/ingest_matrix_test.go ties the table to
// the vendored upstream routes and to the registry rows that cite this file.

var (
	hotVLURL  = envOrDefault("HOT_VL_URL", "http://localhost:29429")
	hotVTURL  = envOrDefault("HOT_VT_URL", "http://localhost:10428")
	lhSyslog  = map[string]string{"tcp": envOrDefault("LH_SYSLOG_TCP_ADDR", "127.0.0.1:29514"), "udp": envOrDefault("LH_SYSLOG_UDP_ADDR", "127.0.0.1:29515")}
	hotSyslog = map[string]string{"tcp": envOrDefault("HOT_VL_SYSLOG_TCP_ADDR", "127.0.0.1:29516"), "udp": envOrDefault("HOT_VL_SYSLOG_UDP_ADDR", "127.0.0.1:29517")}
	lhGRPC    = envOrDefault("LH_TRACES_GRPC_ADDR", "127.0.0.1:20517")
	hotGRPC   = envOrDefault("HOT_VT_GRPC_ADDR", "127.0.0.1:10517")
)

// ingestEnd is one side of the comparison: the hot upstream binary or Lakehouse.
type ingestEnd struct {
	name string
	base string
	hot  bool
	// syslog and grpc addresses of this side.
	syslog map[string]string
	grpc   string
}

func ingestEnds(sig im.Signal) (hot, lh ingestEnd) {
	if sig == im.Traces {
		return ingestEnd{name: "hot VT", base: hotVTURL, hot: true, grpc: hotGRPC},
			ingestEnd{name: "lakehouse-traces", base: tracesBaseURL, grpc: lhGRPC}
	}
	return ingestEnd{name: "hot VL", base: hotVLURL, hot: true, syslog: hotSyslog},
		ingestEnd{name: "lakehouse-logs", base: logsBaseURL, syslog: lhSyslog}
}

// tenantHeaders is how a side names the tenant. Hot VL/VT only knows numeric
// tenants, so for the alias form it receives the numbers the alias resolves to;
// Lakehouse receives the alias itself.
func (e ingestEnd) tenantHeaders(p im.Params) map[string]string {
	if !e.hot && p.Form == im.Alias {
		return map[string]string{"X-Scope-OrgID": p.Tenant.OrgID}
	}
	return map[string]string{
		"AccountID": strconv.FormatUint(uint64(p.Tenant.Account), 10),
		"ProjectID": strconv.FormatUint(uint64(p.Tenant.Project), 10),
	}
}

var ingestClient = &http.Client{Timeout: 60 * time.Second}

type ingestAnswer struct {
	status int
	body   string
	err    error
}

func (e ingestEnd) send(c im.Case, p im.Params, r im.Request) ingestAnswer {
	u := e.base + r.Path
	if r.Query != "" {
		u += "?" + r.Query
	}
	req, err := http.NewRequest(r.Method, u, bytes.NewReader(r.Body))
	if err != nil {
		return ingestAnswer{err: err}
	}
	for k, v := range r.Header {
		req.Header.Set(k, v)
	}
	if !c.NoTenantHeaders {
		for k, v := range e.tenantHeaders(p) {
			req.Header.Set(k, v)
		}
	}
	resp, err := ingestClient.Do(req)
	if err != nil {
		return ingestAnswer{err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	body := b
	if c.NormalizeResponse != nil {
		body = c.NormalizeResponse(b)
	}
	return ingestAnswer{status: resp.StatusCode, body: string(body)}
}

func (e ingestEnd) sendLines(c im.Case, lines [][]byte) error {
	network := string(c.Transport)
	addr := e.syslog[network]
	switch c.Transport {
	case im.TCP:
		conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		for _, l := range lines {
			if _, err := conn.Write(append(append([]byte(nil), l...), '\n')); err != nil {
				return err
			}
		}
		return nil
	case im.UDP:
		conn, err := net.Dial("udp", addr)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()
		for _, l := range lines {
			if _, err := conn.Write(l); err != nil {
				return err
			}
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	}
	return fmt.Errorf("transport %s has no line sender", c.Transport)
}

// metricValue reads one exact series from /metrics; ok is false when the series
// has not been created yet (upstream creates counters lazily on first use).
func metricValue(t *testing.T, base, series string) (float64, bool) {
	t.Helper()
	resp, err := ingestClient.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("GET %s/metrics: %v", base, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, series+" ") {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series+" ")), 64)
			if err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return v, true
		}
	}
	return 0, false
}

func metricOrZero(t *testing.T, base, series string) float64 {
	v, _ := metricValue(t, base, series)
	return v
}

// gapRewrites turns a Lakehouse row into the form hot returns, for each known gap
// declared in tests/ingestmatrix (Gaps). A gap is not a skip: the cell still compares
// every other field, and a gap applies only when the rewritten row then equals a hot
// row exactly. A cell applies only the gaps it declares (Case.Gaps), and fails when it
// declares one it no longer observes: the issue is fixed, so the declaration must go
// and the registry row go back to pass. A divergence without a declared gap fails the cell.
var gapRewrites = map[string]func(row map[string]any) bool{
	"cold-read-adds-severity-number": func(m map[string]any) bool {
		if v, ok := m["severity_number"]; ok && fmt.Sprint(v) == "0" {
			delete(m, "severity_number")
			return true
		}
		return false
	},
	"cold-read-renames-severity-text-to-level": func(m map[string]any) bool {
		lv, hasLevel := m["level"]
		_, hasText := m["severity_text"]
		if hasLevel && !hasText {
			delete(m, "level")
			m["severity_text"] = lv
			return true
		}
		return false
	},
	"traces-default-msg-value": func(m map[string]any) bool {
		if v, ok := m["_msg"]; ok && fmt.Sprint(v) == defaultMsgVL {
			m["_msg"] = "-"
			return true
		}
		return false
	},
}

const defaultMsgVL = "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"

// applyKnownGaps rewrites the Lakehouse rows that match a hot row once the cell's
// declared gaps are applied, and records which gaps it observed. A gap that only
// exists on the Parquet read applies only once the cell has rows in Parquet (s.flushed,
// decided from S3 on every read, never from the test phase).
func (r *matrixRun) applyKnownGaps(s *caseState, hot, lh []string) []string {
	hotSet := map[string]bool{}
	for _, h := range hot {
		hotSet[h] = true
	}
	out := make([]string, len(lh))
	for i, row := range lh {
		out[i] = row
		if hotSet[row] {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(row), &m) != nil {
			continue
		}
		var applied []string
		for _, id := range s.c.Gaps {
			g, ok := im.GapByID(id)
			if !ok || rewriteOf(id) == nil || (g.AfterFlush && !s.flushed) {
				continue
			}
			if rewriteOf(id)(m) {
				applied = append(applied, id)
			}
		}
		if len(applied) == 0 {
			continue
		}
		canon, _ := json.Marshal(m)
		if hotSet[string(canon)] {
			out[i] = string(canon)
			for _, id := range applied {
				s.gapHits[id]++
			}
		}
	}
	sort.Strings(out)
	return out
}

func hasGap(c im.Case, id string) bool {
	for _, g := range c.Gaps {
		if g == id {
			return true
		}
	}
	return false
}

func rewriteOf(id string) func(map[string]any) bool { return gapRewrites[id] }

type matrixRun struct {
	sig      im.Signal
	hot, lh  ingestEnd
	runID    string
	base     time.Time
	ingestAt time.Time
	states   []*caseState
	// pq caches what each Parquet object under the matrix tenants holds, by S3 key.
	pq  map[string]*pqObject
	s3c *s3.Client
	// lakehouse insert counters before the run
	lostBefore, rejectedBefore float64
}

type caseState struct {
	c  im.Case
	f  im.Form
	p  im.Params
	ok bool // ingest succeeded on both sides; later phases run only then
	// counters before the case was sent
	hotBefore, lhBefore, lhRowsBefore float64
	// gapHits counts, per declared known gap, the row comparisons that needed it.
	gapHits                   map[string]int
	preflushDefaultMessageGap bool
	// flushed: at least one Parquet row of this cell has been seen in S3. Monotonic.
	flushed bool
	// hotRows is captured at the actual preflush observation, never reconstructed
	// from Lakehouse's own query reader.
	hotRows      []map[string]string
	visible      bool
	samples      int
	lastSample   time.Time
	maxSampleGap time.Duration
}

func (s *caseState) name() string { return s.c.ID + "/" + string(s.f) }

func newMatrixRun(t *testing.T, sig im.Signal) *matrixRun {
	t.Helper()
	hot, lh := ingestEnds(sig)
	for _, e := range []ingestEnd{hot, lh} {
		waitForHealth(t, e.base, 60*time.Second)
	}
	runID := strconv.FormatInt(time.Now().UnixNano()/1e6, 36)
	r := &matrixRun{pq: map[string]*pqObject{}, sig: sig, hot: hot, lh: lh, runID: runID, base: time.Now().UTC().Truncate(time.Second), ingestAt: time.Now()}
	for _, c := range im.CasesFor(sig) {
		for _, f := range c.Forms {
			r.states = append(r.states, &caseState{c: c, f: f, p: im.NewParams(c, f, runID, r.base), gapHits: map[string]int{}})
		}
	}
	r.lostBefore = metricOrZero(t, lh.base, "lakehouse_insert_rows_lost_total")
	r.rejectedBefore = metricOrZero(t, lh.base, "lakehouse_insert_rejected_total")
	return r
}

// readRows runs the case's read-back query against one side and returns the
// rows as canonical JSON strings, sorted.
func (r *matrixRun) readRows(t *testing.T, e ingestEnd, s *caseState) ([]string, error) {
	t.Helper()
	form := url.Values{}
	form.Set("query", s.c.ReadQuery(s.p))
	form.Set("start", strconv.FormatInt(r.base.Add(-10*time.Minute).UnixNano(), 10))
	form.Set("end", strconv.FormatInt(time.Now().Add(5*time.Minute).UnixNano(), 10))
	form.Set("limit", "1000")
	if r.sig == im.Traces {
		form.Set("disable_latency_offset", "true")
	}
	req, err := http.NewRequest("POST", e.base+"/select/logsql/query", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range e.tenantHeaders(s.p) {
		req.Header.Set(k, v)
	}
	resp, err := ingestClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: status %d: %s", e.name, resp.StatusCode, string(b))
	}
	var rows []string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return nil, fmt.Errorf("%s: bad NDJSON line %q: %v", e.name, line, err)
		}
		canon, _ := json.Marshal(m) // map keys are sorted
		rows = append(rows, string(canon))
	}
	sort.Strings(rows)
	return rows, nil
}

// waitSame polls both sides until both returned want rows and the rows are
// identical, or the deadline passes; the last difference is the failure.
func (r *matrixRun) waitSame(t *testing.T, s *caseState, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last string
	// Rows that have the right count but different fields are not a timing
	// problem for long: give the buffer-to-Parquet handoff a short grace, then fail.
	var diffSince time.Time
	for {
		r.sampleEstablished(t)
		hot, herr := r.readRows(t, r.hot, s)
		lh, lerr := r.readRows(t, r.lh, s)
		if herr == nil && lerr == nil {
			// Cold-read gaps apply from the moment the cell has rows in Parquet, which
			// is decided from S3 after the read (a flush between the read and the check
			// only allows a gap that was not needed).
			r.refreshParquet(t, s)
			// Declared known gaps are applied before anything is compared, so a gap
			// that changes the number of rows (a span returned twice) is handled too.
			lh = r.applyKnownGaps(s, hot, lh)
		}
		switch {
		case herr != nil:
			last = herr.Error()
		case lerr != nil:
			last = lerr.Error()
		case len(hot) != want:
			last = fmt.Sprintf("%s returned %d rows, want %d", r.hot.name, len(hot), want)
		case len(lh) != want:
			last = fmt.Sprintf("%s returned %d rows, want %d\nhot rows:\n%s\nlakehouse rows:\n%s", r.lh.name, len(lh), want, strings.Join(hot, "\n"), strings.Join(lh, "\n"))
		default:
			if diff := firstRowDiff(hot, lh); diff != "" {
				last = diff
				if diffSince.IsZero() {
					diffSince = time.Now()
				}
				if time.Since(diffSince) > 15*time.Second {
					t.Fatalf("rows have the right count but differ from hot (persisting 15s): %s", last)
				}
			} else {
				if len(s.hotRows) != 0 {
					return
				}
				for _, row := range hot {
					var fields map[string]string
					if err := json.Unmarshal([]byte(row), &fields); err != nil {
						t.Fatalf("hot row fields: %v", err)
					}
					s.hotRows = append(s.hotRows, fields)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("rows differ from hot after %s: %s", within, last)
		}
		time.Sleep(time.Second)
	}
}

// Once visible, a cell may never use arrival polling to recover a missing or
// wrong answer. Sample all established cells throughout subsequent ingestion,
// the flush wait and the stability interval.
func (r *matrixRun) sampleEstablished(t *testing.T) {
	t.Helper()
	for _, s := range r.states {
		if !s.visible {
			continue
		}
		hot, herr := r.readRows(t, r.hot, s)
		lh, lerr := r.readRows(t, r.lh, s)
		if herr != nil || lerr != nil {
			t.Fatalf("handoff sample %s: hot %v lakehouse %v", s.name(), herr, lerr)
		}
		r.refreshParquet(t, s)
		lh = r.applyKnownGaps(s, hot, lh)
		if err := im.CheckSample(hot, lh, s.c.Rows); err != nil {
			t.Fatalf("handoff sample %s: %v", s.name(), err)
		}
		s.samples++
		now := time.Now()
		if !s.lastSample.IsZero() && now.Sub(s.lastSample) > s.maxSampleGap {
			s.maxSampleGap = now.Sub(s.lastSample)
		}
		s.lastSample = now
	}
}

func firstRowDiff(hot, lh []string) string {
	for i := range hot {
		if hot[i] == lh[i] {
			continue
		}
		var a, b map[string]any
		_ = json.Unmarshal([]byte(hot[i]), &a)
		_ = json.Unmarshal([]byte(lh[i]), &b)
		var parts []string
		keys := map[string]bool{}
		for k := range a {
			keys[k] = true
		}
		for k := range b {
			keys[k] = true
		}
		var ks []string
		for k := range keys {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			av, aok := a[k]
			bv, bok := b[k]
			if aok && bok && fmt.Sprint(av) == fmt.Sprint(bv) {
				continue
			}
			parts = append(parts, fmt.Sprintf("  field %q: hot=%v (present %v) lakehouse=%v (present %v)", k, av, aok, bv, bok))
		}
		return fmt.Sprintf("row %d differs:\n%s", i, strings.Join(parts, "\n"))
	}
	return ""
}

// pqObject is what one Parquet object under a matrix tenant holds, per marker.
type pqObject struct {
	cells map[string]*pqCell
}

// pqCell counts one cell's rows in one Parquet object. spans maps span_id to the
// number of rows carrying it (traces only); trace-index rows have no span_id.
type pqCell struct {
	rows     int
	spanRows int
	spans    map[string]int
	raw      []im.RawParquetRow
}

func (r *matrixRun) markers() map[string]bool {
	m := map[string]bool{}
	for _, s := range r.states {
		m[s.p.Marker] = true
	}
	return m
}

// refreshParquet scans the Parquet objects of the cell's tenant that were written
// since the run started and have not been read yet, drops cache entries of objects
// that are gone (compaction), and marks the cell flushed once any of its rows is
// in Parquet.
func (r *matrixRun) refreshParquet(t *testing.T, s *caseState) {
	t.Helper()
	if r.s3c == nil {
		r.s3c = newS3Client(t)
	}
	client := r.s3c
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("%d/%d/%s/", s.p.Tenant.Account, s.p.Tenant.Project, r.sig)
	live := map[string]bool{}
	// Scan all fresh objects in this isolated fixture bucket, including foreign
	// tenant prefixes: querying the intended tenant alone cannot detect copying.
	pg := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(s3Bucket)})
	for pg.HasMorePages() {
		page, err := pg.NextPage(ctx)
		if err != nil {
			t.Fatalf("list s3://%s/%s: %v", s3Bucket, prefix, err)
		}
		for _, o := range page.Contents {
			key := aws.ToString(o.Key)
			if !strings.HasSuffix(key, ".parquet") || o.LastModified == nil || o.LastModified.Before(r.ingestAt.Add(-time.Second)) {
				continue
			}
			live[key] = true
			if _, done := r.pq[key]; done {
				continue
			}
			obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s3Bucket), Key: aws.String(key)})
			if err != nil {
				t.Fatalf("get s3://%s/%s: %v", s3Bucket, key, err)
			}
			data, err := io.ReadAll(obj.Body)
			_ = obj.Body.Close()
			if err != nil {
				t.Fatalf("read s3://%s/%s: %v", s3Bucket, key, err)
			}
			r.pq[key] = &pqObject{cells: scanParquetObject(t, key, data, r.markers())}
			for _, state := range r.states {
				if cell := r.pq[key].cells[state.p.Marker]; cell != nil && cell.rows > 0 {
					wantPrefix := fmt.Sprintf("%d/%d/%s/", state.p.Tenant.Account, state.p.Tenant.Project, r.sig)
					if !strings.HasPrefix(key, wantPrefix) {
						t.Fatalf("marker %s copied into foreign prefix %s, want %s", state.p.Marker, key, wantPrefix)
					}
				}
			}
		}
	}
	for key := range r.pq {
		if !live[key] {
			delete(r.pq, key)
		}
	}
	if rows, _, _ := r.pqCounts(s); rows > 0 {
		s.flushed = true
	}
}

// pqCounts totals a cell over the tenant's cached objects: all rows carrying the
// marker, span rows (traces: rows with a span_id) and distinct span ids.
func (r *matrixRun) pqCounts(s *caseState) (rows, spanRows, distinctSpans int) {
	prefix := fmt.Sprintf("%d/%d/%s/", s.p.Tenant.Account, s.p.Tenant.Project, r.sig)
	spans := map[string]bool{}
	for key, o := range r.pq {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if c := o.cells[s.p.Marker]; c != nil {
			rows += c.rows
			spanRows += c.spanRows
			for id := range c.spans {
				spans[id] = true
			}
		}
	}
	return rows, spanRows, len(spans)
}

// parquetExact reports whether the cell's Parquet content is exactly what was
// written: logs, Rows rows; traces, Rows span rows with Rows distinct span ids
// (trace-index rows excluded). More is a writer that duplicates, fewer is not flushed yet.
func (r *matrixRun) parquetExact(s *caseState) (bool, string) {
	rows, spanRows, distinct := r.pqCounts(s)
	if !s.c.Rejected && len(s.hotRows) != s.c.Rows {
		return false, "raw Parquet truth has no complete captured hot rows"
	}
	prefix := fmt.Sprintf("%d/%d/%s/", s.p.Tenant.Account, s.p.Tenant.Project, r.sig)
	for key, obj := range r.pq {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		cell := obj.cells[s.p.Marker]
		if cell == nil {
			continue
		}
		for _, raw := range cell.raw {
			if err := raw.CheckTenant(s.p.Tenant); err != nil {
				return false, fmt.Sprintf("%s: %v", key, err)
			}
			if r.sig == im.Traces && (len(raw["span_id"]) != 1 || raw["span_id"][0] == "") {
				continue
			}
			var expected map[string]string
			for _, hot := range s.hotRows {
				if r.sig == im.Traces && len(raw["span_id"]) == 1 && raw["span_id"][0] == hot["span_id"] {
					expected = hot
					break
				}
				if r.sig == im.Logs && len(raw["body"]) == 1 && raw["body"][0] == hot["_msg"] {
					expected = hot
					break
				}
			}
			// #332 is an existing ingest-value divergence, independently observed
			// before flush. Its physical truth is the exact native VL default, not
			// an absent body or an arbitrary replacement. All other fields still
			// compare directly with the captured hot row.
			if hasGap(s.c, "traces-default-msg-value") && s.preflushDefaultMessageGap && expected["_msg"] == "-" && len(raw["body"]) == 1 && raw["body"][0] == defaultMsgVL {
				copyExpected := make(map[string]string, len(expected))
				for k, v := range expected {
					copyExpected[k] = v
				}
				copyExpected["_msg"] = defaultMsgVL
				expected = copyExpected
			}
			if err := raw.CheckHot(r.sig, s.p.Tenant, expected); err != nil {
				return false, fmt.Sprintf("%s: %v", key, err)
			}
		}
	}
	if r.sig == im.Traces {
		return spanRows == s.c.Rows && distinct == s.c.Rows, fmt.Sprintf("%d span rows, %d distinct span ids, %d rows carrying trace_id (want %d span rows and %d distinct span ids)", spanRows, distinct, rows, s.c.Rows, s.c.Rows)
	}
	return rows == s.c.Rows, fmt.Sprintf("%d rows carrying the marker (want exactly %d)", rows, s.c.Rows)
}

// bufferHeld returns how many of the cell's rows Lakehouse's insert buffer
// holds now, read through the endpoint select pods read the buffer with
// (/internal/buffer/query): rows that carry the cell's marker, and for traces
// span rows only (VictoriaTraces' trace-index rows carry the trace id too).
// While the buffer holds them, a read is answered from the buffer with
// upstream's engine, whatever S3 already holds: a drained segment stays
// readable, and its objects are left out of the scan, for its grace period
// (2 x manifest.refresh_interval + 30 s, 90 s in the e2e stack).
func (r *matrixRun) bufferHeld(t *testing.T, s *caseState) int {
	t.Helper()
	params := url.Values{
		"start":        {strconv.FormatInt(s.p.Base.Add(-10*time.Minute).UnixNano(), 10)},
		"end":          {strconv.FormatInt(s.p.Base.Add(time.Minute).UnixNano(), 10)},
		"mode":         {string(r.sig)},
		"tenant_scope": {"v1"},
		"account_id":   {strconv.FormatUint(uint64(s.p.Tenant.Account), 10)},
		"project_id":   {strconv.FormatUint(uint64(s.p.Tenant.Project), 10)},
	}
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(r.lh.base + "/internal/buffer/query?" + params.Encode())
	if err != nil {
		t.Fatalf("buffer query on %s: %v", r.lh.base, err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("buffer query on %s: status %d, %v: %s", r.lh.base, resp.StatusCode, err, body)
	}
	held := 0
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.Contains(line, s.p.Marker) {
			continue
		}
		if r.sig == im.Traces {
			var row struct {
				SpanID string `json:"span_id"`
			}
			if json.Unmarshal([]byte(line), &row) != nil || row.SpanID == "" {
				continue
			}
		}
		held++
	}
	return held
}

// waitBufferHolds waits (up to 30 s, for upstream to make fresh rows
// searchable) until the insert buffer holds all want rows of the cell.
func (r *matrixRun) waitBufferHolds(t *testing.T, s *caseState, want int, what string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		held := r.bufferHeld(t, s)
		if held == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the insert buffer holds %d of the cell's %d rows", what, held, want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// waitLeftBuffer waits until Lakehouse's insert buffer no longer holds a row of
// the cell, so that a read is answered from Parquet: until then the comparison
// after the flush would read the buffer, and a gap that exists only on the
// Parquet read would go unobserved.
func (r *matrixRun) waitLeftBuffer(t *testing.T, s *caseState) {
	t.Helper()
	deadline := time.Now().Add(240 * time.Second)
	for {
		held := r.bufferHeld(t, s)
		if held == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d rows of %s still in the Lakehouse insert buffer 240s after their Parquet objects were exact", held, s.name())
		}
		time.Sleep(2 * time.Second)
	}
}

// waitParquetExact polls S3 until the cell's Parquet content is exact, or fails with
// the counts at the deadline. An exact content that later grows or shrinks is caught
// by the stable step.
func (r *matrixRun) waitParquetExact(t *testing.T, s *caseState) {
	t.Helper()
	deadline := r.ingestAt.Add(360 * time.Second)
	for {
		r.sampleEstablished(t)
		r.refreshParquet(t, s)
		ok, what := r.parquetExact(s)
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Parquet content never became exact within 360s of the write: %s", what)
		}
		time.Sleep(2 * time.Second)
	}
}

func scanParquetObject(t *testing.T, key string, data []byte, markers map[string]bool) map[string]*pqCell {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open %s as Parquet: %v", key, err)
	}
	var names []string
	for _, path := range f.Schema().Columns() {
		names = append(names, strings.Join(path, "."))
	}
	cells := map[string]*pqCell{}
	rawRows, err := im.ReadRawParquet(data)
	if err != nil {
		t.Fatalf("raw Parquet %s: %v", key, err)
	}
	rawIndex := 0
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 256)
		for {
			k, err := rows.ReadRows(buf)
			for _, row := range buf[:k] {
				raw := rawRows[rawIndex]
				rawIndex++
				var marker, spanID string
				for _, v := range row {
					if v.Kind() != parquet.ByteArray {
						continue
					}
					str := string(v.ByteArray())
					name := ""
					if c := v.Column(); c >= 0 && c < len(names) {
						name = names[c]
					}
					switch {
					case name == "span_id":
						spanID = str
					case markers[str]:
						marker = str
					default:
						if i := strings.Index(str, "ingm"); i >= 0 {
							j := i
							for j < len(str) && (str[j] >= 'a' && str[j] <= 'z' || str[j] >= '0' && str[j] <= '9') {
								j++
							}
							if markers[str[i:j]] {
								marker = str[i:j]
							}
						}
					}
				}
				if marker == "" {
					continue
				}
				c := cells[marker]
				if c == nil {
					c = &pqCell{spans: map[string]int{}}
					cells[marker] = c
				}
				c.rows++
				c.raw = append(c.raw, raw)
				if spanID != "" {
					c.spanRows++
					c.spans[spanID]++
				}
			}
			if err != nil {
				if err != io.EOF {
					t.Fatalf("read rows of %s: %v", key, err)
				}
				break
			}
		}
		_ = rows.Close()
	}
	return cells
}

// queryCount runs a LogsQL query against one binary for one tenant and returns the
// number of rows.
func queryCount(t *testing.T, base string, sig im.Signal, tn im.Tenant, query string, from time.Time) int {
	t.Helper()
	form := url.Values{}
	form.Set("query", query)
	form.Set("start", strconv.FormatInt(from.UnixNano(), 10))
	form.Set("end", strconv.FormatInt(time.Now().Add(5*time.Minute).UnixNano(), 10))
	form.Set("limit", "10000")
	if sig == im.Traces {
		form.Set("disable_latency_offset", "true")
	}
	req, err := http.NewRequest("POST", base+"/select/logsql/query", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("AccountID", strconv.FormatUint(uint64(tn.Account), 10))
	req.Header.Set("ProjectID", strconv.FormatUint(uint64(tn.Project), 10))
	resp, err := ingestClient.Do(req)
	if err != nil {
		t.Fatalf("query %s: %v", base, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query %s: status %d: %s", base, resp.StatusCode, string(b))
	}
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(l) != "" {
			n++
		}
	}
	return n
}

// runIngestMatrix is the body of TestIngestMatrix_Logs and TestIngestMatrix_Traces.
func runIngestMatrix(t *testing.T, sig im.Signal) {
	r := newMatrixRun(t, sig)
	t.Logf("run %s: %d case/form cells, marker base %s", r.runID, len(r.states), r.base.Format(time.RFC3339))
	r.ingestAt = time.Now()

	// 1. ingest: the same payload to hot and to Lakehouse, answers compared; then,
	// right after this cell's own write, the ingest counters. The counters are
	// per-protocol series of the whole process, not per tenant, so the check is a
	// lower bound: they moved by at least the rows this cell wrote (a continuous
	// writer on the same stack can only add to it).
	for _, s := range r.states {
		s := s
		r.sampleEstablished(t)
		t.Run("ingest/"+s.name(), func(t *testing.T) {
			s.hotBefore = metricOrZero(t, r.hot.base, s.c.Counter)
			s.lhBefore = metricOrZero(t, r.lh.base, s.c.Counter)
			s.lhRowsBefore = metricOrZero(t, r.lh.base, "lakehouse_insert_rows_total")
			switch s.c.Transport {
			case im.HTTP:
				for _, req := range s.c.Build(s.p) {
					h := r.hot.send(s.c, s.p, req)
					l := r.lh.send(s.c, s.p, req)
					if h.err != nil || l.err != nil {
						t.Fatalf("%s %s: hot error %v, lakehouse error %v", req.Method, req.Path, h.err, l.err)
					}
					if s.c.Rejected {
						if h.status < 400 {
							t.Fatalf("%s %s: hot VL/VT accepted a payload this case expects it to refuse (status %d); the case table is out of date", req.Method, req.Path, h.status)
						}
					} else if h.status >= 300 {
						t.Fatalf("%s %s: hot rejected the payload (status %d: %s); the payload builder is wrong", req.Method, req.Path, h.status, h.body)
					}
					if h.status != l.status || h.body != l.body {
						t.Fatalf("%s %s: answers differ\n  hot        status=%d body=%q\n  lakehouse  status=%d body=%q", req.Method, req.Path, h.status, h.body, l.status, l.body)
					}
				}
			case im.TCP, im.UDP:
				lines := s.c.Lines(s.p)
				if err := r.hot.sendLines(s.c, lines); err != nil {
					t.Fatalf("hot %s listener: %v", s.c.Transport, err)
				}
				if err := r.lh.sendLines(s.c, lines); err != nil {
					t.Fatalf("lakehouse %s listener: %v (the listener is opt-in: -syslog.listenAddr.%s)", s.c.Transport, err, s.c.Transport)
				}
			case im.GRPC:
				h := im.GRPCExport(context.Background(), r.hot.grpc, s.p)
				l := im.GRPCExport(context.Background(), r.lh.grpc, s.p)
				if h.Code != "OK" {
					t.Fatalf("hot gRPC export failed: %s (%v)", h.Code, h.Err)
				}
				if h.Code != l.Code || string(h.Response) != string(l.Response) {
					t.Fatalf("gRPC answers differ\n  hot        code=%s response=%x\n  lakehouse  code=%s response=%x err=%v", h.Code, h.Response, l.Code, l.Response, l.Err)
				}
			}
			s.ok = true
			if s.c.Rejected {
				t.Log("payload refused with the same answer on both sides; no rows to count")
				return
			}
			want := float64(s.c.Rows)
			deadline := time.Now().Add(60 * time.Second)
			for {
				r.sampleEstablished(t)
				hd := metricOrZero(t, r.hot.base, s.c.Counter) - s.hotBefore
				ld := metricOrZero(t, r.lh.base, s.c.Counter) - s.lhBefore
				rd := metricOrZero(t, r.lh.base, "lakehouse_insert_rows_total") - s.lhRowsBefore
				if hd >= want && ld >= want && rd >= want {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("ingest counters did not move by at least %v after this cell's write: hot %s +%v, lakehouse %s +%v, lakehouse_insert_rows_total +%v", want, s.c.Counter, hd, s.c.Counter, ld, rd)
				}
				time.Sleep(time.Second)
			}
		})
		t.Run("buffer/"+s.name(), func(t *testing.T) {
			if !s.ok {
				t.Fatal("the ingest step of this cell failed")
			}
			// The preflush read must be answered from the insert buffer: the
			// buffer holds every row of the cell before and after it. Whether
			// S3 already has the rows does not decide it, because a drained
			// segment keeps serving its rows (and its objects stay out of the
			// scan) through its grace period.
			r.waitBufferHolds(t, s, s.c.Rows, "preflush observation missed")
			r.waitSame(t, s, s.c.Rows, 60*time.Second)
			if held := r.bufferHeld(t, s); held != s.c.Rows {
				t.Fatalf("preflush observation overlapped the buffer handoff: the insert buffer holds %d of %d rows", held, s.c.Rows)
			}
			r.refreshParquet(t, s)
			s.visible = true
			s.preflushDefaultMessageGap = s.gapHits["traces-default-msg-value"] > 0
			t.Logf("preflush observed: %d exact hot/Lakehouse rows, all held by the insert buffer", s.c.Rows)
		})
	}

	// 3. parquet: wait until the tenant's Parquet holds exactly the cell's rows
	// (traces: exactly Rows span rows with Rows distinct span ids, trace-index rows
	// excluded), then compare with hot again.
	for _, s := range r.states {
		s := s
		t.Run("parquet/"+s.name(), func(t *testing.T) {
			if !s.ok {
				t.Fatalf("not run: the ingest step of this cell failed")
			}
			if s.c.Rejected {
				// Nothing was stored; both sides must still agree on that.
				r.waitSame(t, s, 0, 30*time.Second)
				return
			}
			r.waitParquetExact(t, s)
			r.waitLeftBuffer(t, s)
			r.waitSame(t, s, s.c.Rows, 90*time.Second)
		})
	}

	// 4. stable: repeated strict samples, with no eventual-equality retries.
	t.Run("stable", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Second)
		cycles := 0
		for {
			r.sampleEstablished(t)
			for _, s := range r.states {
				if !s.visible || s.c.Rejected {
					continue
				}
				r.refreshParquet(t, s)
				if ok, what := r.parquetExact(s); !ok {
					t.Fatalf("Parquet changed for %s: %s", s.name(), what)
				}
			}
			cycles++
			if time.Now().After(deadline) && cycles >= 2 {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		for _, s := range r.states {
			t.Logf("%s: %d strict samples, including %d stability cycles, observed maximum sample gap %s (no claim between samples)", s.name(), s.samples, cycles, s.maxSampleGap)
		}
	})

	// 5. known gaps must still be real: a cell that declares a gap it never
	// observed is fixed, and the declaration has to go.
	for _, s := range r.states {
		s := s
		t.Run("known_gaps/"+s.name(), func(t *testing.T) {
			for _, id := range s.c.Gaps {
				g, ok := im.GapByID(id)
				if !ok {
					t.Fatalf("cell declares unknown gap %q (tests/ingestmatrix Gaps)", id)
				}
				if s.gapHits[id] == 0 {
					t.Fatalf("known gap %s (%s) was not observed in this cell: if the issue is fixed, delete %q from the case's Gaps in tests/ingestmatrix, set the registry row back to expect: pass and update docs/ingest-parity.md", id, g.Issue, id)
				}
				t.Logf("KNOWN GAP %s: needed in %d row comparison(s); tracked in %s", id, s.gapHits[id], g.Issue)
			}
		})
	}

	// 6. health and isolation of the whole run.
	t.Run("cross_tenant_isolation", r.checkCrossTenantIsolation)
	t.Run("storage_health", func(t *testing.T) {
		if v := metricOrZero(t, r.lh.base, "lakehouse_insert_rows_lost_total") - r.lostBefore; v != 0 {
			t.Fatalf("lakehouse_insert_rows_lost_total moved by %v during the matrix", v)
		}
		if v := metricOrZero(t, r.lh.base, "lakehouse_insert_rejected_total") - r.rejectedBefore; v != 0 {
			t.Fatalf("lakehouse_insert_rejected_total moved by %v during the matrix", v)
		}
	})
	t.Run("other_signal_unaffected", func(t *testing.T) {
		otherBase, other := tracesBaseURL, im.Traces
		if sig == im.Traces {
			otherBase, other = logsBaseURL, im.Logs
		}
		from := r.base.Add(-10 * time.Minute)
		for _, tn := range tenantsOf(r.states) {
			var terms []string
			wantOwn := 0
			for _, s := range r.states {
				if s.p.Tenant == tn {
					terms = append(terms, fmt.Sprintf(`(_msg:%s OR trace_id:=%q)`, s.p.Marker, s.p.Marker))
					wantOwn += s.c.Rows
				}
			}
			q := strings.Join(terms, " OR ")
			// The query must be able to find the rows: it returns them from the binary
			// they were written to, so an empty answer from the other binary means something.
			if got := queryCount(t, r.lh.base, sig, tn, q, from); got < wantOwn {
				t.Fatalf("self-check: the isolation query finds %d rows for tenant %d:%d on the binary that holds them, want at least %d", got, tn.Account, tn.Project, wantOwn)
			}
			if got := queryCount(t, otherBase, other, tn, q, from); got != 0 {
				t.Fatalf("the %s binary holds %d rows written through the %s matrix for tenant %d:%d", other, got, sig, tn.Account, tn.Project)
			}
		}
	})
}

func tenantsOf(states []*caseState) []im.Tenant {
	seen := map[im.Tenant]bool{}
	var out []im.Tenant
	for _, s := range states {
		if !seen[s.p.Tenant] {
			seen[s.p.Tenant] = true
			out = append(out, s.p.Tenant)
		}
	}
	return out
}

// TestIngestMatrix_Logs runs every VictoriaLogs ingest protocol against hot VL
// and lakehouse-logs, in both tenant forms where upstream has both.
func TestIngestMatrix_Logs(t *testing.T) {
	t.Parallel() // independent binaries and tenants: the flush waits overlap with the traces run
	runIngestMatrix(t, im.Logs)
}

// TestIngestMatrix_Traces runs every VictoriaTraces ingest protocol against hot
// VT and lakehouse-traces, in both tenant forms where upstream has both.
func TestIngestMatrix_Traces(t *testing.T) {
	t.Parallel()
	runIngestMatrix(t, im.Traces)
}

// This is one actual multitenant native payload per signal, with distinct
// markers for each encoded tenant and explicit negative reads under the other
// tenant's numeric and alias spellings. Non-OTLP trace IDs exercise the native
// string domain instead of forcing it through OTLP's hex schema.
func TestIngestMatrix_NativeTenantControls(t *testing.T) {
	t.Parallel()
	for _, sig := range []im.Signal{im.Logs, im.Traces} {
		t.Run(string(sig), func(t *testing.T) {
			t.Parallel()
			r := newMatrixRun(t, sig)
			r.states = nil
			var c im.Case
			for _, candidate := range im.CasesFor(sig) {
				if candidate.ID == "multitenant_native" {
					c = candidate
				}
			}
			var params []im.Params
			for _, form := range []im.Form{im.Numeric, im.Alias} {
				p := im.NewParams(c, form, r.runID+"domain", r.base)
				if sig == im.Traces {
					p.Marker = "ingmnative" + r.runID + "/" + string(form)
				}
				params = append(params, p)
				r.states = append(r.states, &caseState{c: c, p: p, f: form, gapHits: map[string]int{}, ok: true})
			}
			request := im.MixedNativeRequest(sig, params)
			hot := r.hot.send(c, params[0], request)
			lh := r.lh.send(c, params[0], request)
			if hot.err != nil || lh.err != nil || hot.status >= 300 || hot.status != lh.status || hot.body != lh.body {
				t.Fatalf("mixed native: hot=%+v lakehouse=%+v", hot, lh)
			}
			for _, s := range r.states {
				r.waitBufferHolds(t, s, c.Rows, "mixed native preflush missed")
				r.waitSame(t, s, c.Rows, 60*time.Second)
				if held := r.bufferHeld(t, s); held != c.Rows {
					t.Fatalf("mixed native preflush overlapped the buffer handoff: the insert buffer holds %d of %d rows", held, c.Rows)
				}
				r.refreshParquet(t, s)
				s.preflushDefaultMessageGap = s.gapHits["traces-default-msg-value"] > 0
				s.visible = true
				t.Logf("mixed native preflush %s: %d exact rows, all held by the insert buffer", s.f, c.Rows)
			}
			for _, s := range r.states {
				r.waitParquetExact(t, s)
				r.waitLeftBuffer(t, s)
				r.sampleEstablished(t)
				t.Logf("mixed native persisted %s: exact fields/time/tenant, samples=%d max_gap=%s", s.f, s.samples, s.maxSampleGap)
				for _, other := range params {
					if other.Tenant == s.p.Tenant {
						continue
					}
					for _, form := range []im.Form{im.Numeric, im.Alias} {
						if form == im.Alias && other.Tenant.OrgID == "" {
							continue
						}
						foreign := *s
						foreign.p.Tenant = other.Tenant
						foreign.p.Form = form
						for _, side := range []ingestEnd{r.hot, r.lh} {
							rows, err := r.readRows(t, side, &foreign)
							if err != nil || len(rows) != 0 {
								t.Fatalf("mixed native tenant isolation %s/%s: rows=%v err=%v", side.name, form, rows, err)
							}
						}
					}
				}
			}
		})
	}
}

// TestIngestMatrix_Probes sends the non-data ingest routes (readiness, health,
// the Elasticsearch, Datadog and Splunk compatibility stubs) to hot and to
// Lakehouse and requires the same status and body.
func TestIngestMatrix_Probes(t *testing.T) {
	for _, pr := range im.Probes() {
		pr := pr
		t.Run(string(pr.Signal)+pr.Path, func(t *testing.T) {
			hot, lh := ingestEnds(pr.Signal)
			get := func(e ingestEnd) ingestAnswer {
				req, _ := http.NewRequest(pr.Method, e.base+pr.Path, nil)
				resp, err := ingestClient.Do(req)
				if err != nil {
					return ingestAnswer{err: err}
				}
				defer func() { _ = resp.Body.Close() }()
				b, _ := io.ReadAll(resp.Body)
				return ingestAnswer{status: resp.StatusCode, body: normalizeProbe(string(b))}
			}
			h, l := get(hot), get(lh)
			if h.err != nil || l.err != nil {
				t.Fatalf("hot error %v, lakehouse error %v", h.err, l.err)
			}
			if h.status != l.status || h.body != l.body {
				t.Fatalf("GET %s answers differ\n  hot        status=%d body=%q\n  lakehouse  status=%d body=%q", pr.Path, h.status, h.body, l.status, l.body)
			}
		})
	}
}

// normalizeProbe collapses whitespace: the upstream stubs are indented raw
// strings and only their content matters.
func normalizeProbe(s string) string { return strings.Join(strings.Fields(s), " ") }

// TestIngestMatrix_RouteGaps sends the observed route gaps to hot and to Lakehouse
// and requires exactly the documented statuses. It fails when Lakehouse starts
// answering like hot: the gap is closed and its entry has to go.
func TestIngestMatrix_RouteGaps(t *testing.T) {
	for _, g := range im.RouteGaps() {
		g := g
		t.Run(string(g.Signal)+g.Route, func(t *testing.T) {
			hot, lh := ingestEnds(g.Signal)
			status := func(e ingestEnd) int {
				req, _ := http.NewRequest(g.Method, e.base+g.Path, nil)
				req.Header.Set("Content-Type", "application/octet-stream")
				resp, err := ingestClient.Do(req)
				if err != nil {
					t.Fatalf("%s: %v", e.name, err)
				}
				_ = resp.Body.Close()
				return resp.StatusCode
			}
			if h := status(hot); h != g.HotStatus {
				t.Fatalf("%s %s: hot answers %d, the gap entry says %d", g.Method, g.Path, h, g.HotStatus)
			}
			l := status(lh)
			if l == g.HotStatus {
				t.Fatalf("%s %s: Lakehouse now answers %d like hot: the gap is closed; delete the RouteGaps entry %q, set the registry row %s to expect: pass and update docs/ingest-parity.md (%s)", g.Method, g.Path, l, g.ID, im.RouteGapRowID(g), g.Issue)
			}
			if l != g.LHStatus {
				t.Fatalf("%s %s: Lakehouse answers %d, the gap entry says %d (hot %d)", g.Method, g.Path, l, g.LHStatus, g.HotStatus)
			}
		})
	}
}

func (r *matrixRun) checkCrossTenantIsolation(t *testing.T) {
	for _, s := range r.states {
		for _, tenant := range tenantsOf(r.states) {
			if tenant.Account == s.p.Tenant.Account && tenant.Project == s.p.Tenant.Project {
				continue
			}
			forms := []im.Form{im.Numeric}
			if tenant.OrgID != "" {
				forms = append(forms, im.Alias)
			}
			for _, form := range forms {
				other := *s
				other.p.Tenant = tenant
				other.p.Form = form
				for _, side := range []ingestEnd{r.hot, r.lh} {
					rows, err := r.readRows(t, side, &other)
					if err != nil {
						t.Fatal(err)
					}
					if len(rows) != 0 {
						t.Fatalf("%s marker %s leaked to other tenant %d:%d (%s): %v", side.name, s.p.Marker, tenant.Account, tenant.Project, form, rows)
					}
				}
			}
			other := *s
			other.p.Tenant = tenant
			r.refreshParquet(t, &other)
			if rows, _, _ := r.pqCounts(&other); rows != 0 {
				t.Fatalf("marker %s stored %d rows in other tenant prefix %d:%d", s.p.Marker, rows, tenant.Account, tenant.Project)
			}
		}
	}
}
