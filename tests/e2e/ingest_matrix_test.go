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
//	          rows (read with a plain Parquet reader), and Lakehouse still equals hot.
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
	"traces-flushed-spans-lack-msg": func(m map[string]any) bool {
		if _, ok := m["_msg"]; !ok {
			m["_msg"] = "-"
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
// declared gaps are applied, and records which gaps it observed.
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
			if !ok || rewriteOf(id) == nil || (g.AfterFlush && !r.afterFlush) {
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
	// Set-level gap: the same span returned more than once. Collapse exact
	// duplicates only when that makes the set equal to hot's.
	if r.afterFlush && hasGap(s.c, "traces-trace-id-path-returns-flushed-spans-twice") && len(out) > len(hot) {
		var dedup []string
		for i, row := range out {
			if i == 0 || row != out[i-1] {
				dedup = append(dedup, row)
			}
		}
		if len(dedup) == len(hot) && firstRowDiff(hot, dedup) == "" {
			s.gapHits["traces-trace-id-path-returns-flushed-spans-twice"]++
			out = dedup
		}
	}
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
	// afterFlush: the tenants' Parquet exists; known gaps that only exist on the
	// cold read apply from here on.
	afterFlush bool
	states     []*caseState
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
	gapHits map[string]int
}

func (s *caseState) name() string { return s.c.ID + "/" + string(s.f) }

func newMatrixRun(t *testing.T, sig im.Signal) *matrixRun {
	t.Helper()
	hot, lh := ingestEnds(sig)
	for _, e := range []ingestEnd{hot, lh} {
		waitForHealth(t, e.base, 60*time.Second)
	}
	runID := strconv.FormatInt(time.Now().UnixNano()/1e6, 36)
	r := &matrixRun{sig: sig, hot: hot, lh: lh, runID: runID, base: time.Now().UTC().Truncate(time.Second), ingestAt: time.Now()}
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
		hot, herr := r.readRows(t, r.hot, s)
		lh, lerr := r.readRows(t, r.lh, s)
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
			lh = r.applyKnownGaps(s, hot, lh)
			if diff := firstRowDiff(hot, lh); diff != "" {
				last = diff
				if diffSince.IsZero() {
					diffSince = time.Now()
				}
				if time.Since(diffSince) > 15*time.Second {
					t.Fatalf("rows have the right count but differ from hot (persisting 15s): %s", last)
				}
			} else {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("rows differ from hot after %s: %s", within, last)
		}
		time.Sleep(2 * time.Second)
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

// runIngestMatrix is the body of TestIngestMatrix_Logs and TestIngestMatrix_Traces.
func runIngestMatrix(t *testing.T, sig im.Signal) {
	r := newMatrixRun(t, sig)
	t.Logf("run %s: %d case/form cells, marker base %s", r.runID, len(r.states), r.base.Format(time.RFC3339))
	r.ingestAt = time.Now()

	// 1. ingest: the same payload to hot and to Lakehouse, answers compared.
	for _, s := range r.states {
		s := s
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
		})
	}

	// 2. counters: the ingest counters moved on both sides.
	for _, s := range r.states {
		s := s
		t.Run("counters/"+s.name(), func(t *testing.T) {
			if !s.ok {
				t.Fatalf("not run: the ingest step of this cell failed")
			}
			if s.c.Rejected {
				t.Log("payload refused on both sides (verified in the ingest step); no rows to count")
				return
			}
			want := float64(s.c.Rows)
			deadline := time.Now().Add(60 * time.Second)
			for {
				hd := metricOrZero(t, r.hot.base, s.c.Counter) - s.hotBefore
				ld := metricOrZero(t, r.lh.base, s.c.Counter) - s.lhBefore
				rd := metricOrZero(t, r.lh.base, "lakehouse_insert_rows_total") - s.lhRowsBefore
				if hd >= want && ld >= want && rd >= want {
					return
				}
				if time.Now().After(deadline) {
					t.Fatalf("ingest counters did not move by %v: hot %s +%v, lakehouse %s +%v, lakehouse_insert_rows_total +%v", want, s.c.Counter, hd, s.c.Counter, ld, rd)
				}
				time.Sleep(time.Second)
			}
		})
	}

	// 3. buffer: straight after the write, before any flush, Lakehouse serves
	// exactly the rows hot serves.
	for _, s := range r.states {
		s := s
		t.Run("buffer/"+s.name(), func(t *testing.T) {
			if !s.ok {
				t.Fatalf("not run: the ingest step of this cell failed")
			}
			r.waitSame(t, s, s.c.Rows, 60*time.Second)
		})
	}

	// 4. flush: wait for each tenant's Parquet, then compare again and read the
	// Parquet itself.
	flushed := false
	t.Run("flush", func(t *testing.T) {
		client := newS3Client(t)
		deadline := time.Now().Add(300 * time.Second)
		for {
			pending := 0
			for _, tn := range tenantsOf(r.states) {
				if !scopeHasObjectSince(t, client, s3Bucket, fmt.Sprintf("%d/%d/%s/", tn.Account, tn.Project, r.sig), r.ingestAt) {
					pending++
				}
			}
			if pending == 0 {
				flushed = true
				r.afterFlush = true
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d tenant(s) never flushed Parquet within 300s", pending)
			}
			time.Sleep(5 * time.Second)
		}
	})
	for _, s := range r.states {
		s := s
		t.Run("parquet/"+s.name(), func(t *testing.T) {
			if !s.ok || !flushed {
				t.Fatalf("not run: the ingest step (ok=%v) or the flush wait (flushed=%v) of this cell failed", s.ok, flushed)
			}
			if s.c.Rejected {
				// Nothing was stored; both sides must still agree on that.
				r.waitSame(t, s, 0, 30*time.Second)
				return
			}
			got := countParquetRows(t, newS3Client(t), s.p.Tenant, r.sig, s.p.Marker, r.ingestAt)
			if r.sig == im.Traces {
				if got < s.c.Rows {
					t.Fatalf("Parquet holds %d rows carrying trace_id %s, want at least %d spans", got, s.p.Marker, s.c.Rows)
				}
			} else if got != s.c.Rows {
				t.Fatalf("Parquet holds %d rows carrying marker %s, want exactly %d", got, s.p.Marker, s.c.Rows)
			}
			// After the flush Lakehouse must still equal hot: no dip and no
			// duplicate while the buffer hands the rows over.
			r.waitSame(t, s, s.c.Rows, 90*time.Second)
			time.Sleep(10 * time.Second)
			r.waitSame(t, s, s.c.Rows, 30*time.Second)
		})
	}

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
	t.Run("storage_health", func(t *testing.T) {
		if v := metricOrZero(t, r.lh.base, "lakehouse_insert_rows_lost_total") - r.lostBefore; v != 0 {
			t.Fatalf("lakehouse_insert_rows_lost_total moved by %v during the matrix", v)
		}
		if v := metricOrZero(t, r.lh.base, "lakehouse_insert_rejected_total") - r.rejectedBefore; v != 0 {
			t.Fatalf("lakehouse_insert_rejected_total moved by %v during the matrix", v)
		}
	})
	t.Run("other_signal_unaffected", func(t *testing.T) {
		otherBase := tracesBaseURL
		other := im.Traces
		if sig == im.Traces {
			otherBase, other = logsBaseURL, im.Logs
		}
		var markers []string
		for _, s := range r.states {
			markers = append(markers, strconv.Quote(s.p.Marker))
		}
		form := url.Values{}
		form.Set("query", "_msg:in("+strings.Join(markers, ",")+") OR trace_id:in("+strings.Join(markers, ",")+")")
		form.Set("start", strconv.FormatInt(r.base.Add(-10*time.Minute).UnixNano(), 10))
		form.Set("end", strconv.FormatInt(time.Now().Add(5*time.Minute).UnixNano(), 10))
		if other == im.Traces {
			form.Set("disable_latency_offset", "true")
		}
		for _, tn := range []im.Tenant{im.NumericTenant, im.AliasTenant} {
			req, _ := http.NewRequest("POST", otherBase+"/select/logsql/query", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("AccountID", strconv.FormatUint(uint64(tn.Account), 10))
			req.Header.Set("ProjectID", strconv.FormatUint(uint64(tn.Project), 10))
			resp, err := ingestClient.Do(req)
			if err != nil {
				t.Fatalf("query the %s binary: %v", other, err)
			}
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("query the %s binary: status %d: %s", other, resp.StatusCode, string(b))
			}
			if strings.TrimSpace(string(b)) != "" {
				t.Fatalf("the %s binary returned rows written through the %s matrix:\n%s", other, sig, string(b))
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

// countParquetRows reads the tenant's Parquet objects written since `since`
// with a plain Parquet reader (the same way DuckDB or pyarrow would) and counts
// the rows that carry marker in any string column.
func countParquetRows(t *testing.T, client *s3.Client, tn im.Tenant, sig im.Signal, marker string, since time.Time) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("%d/%d/%s/", tn.Account, tn.Project, sig)
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(s3Bucket), Prefix: aws.String(prefix)})
	total := 0
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatalf("list s3://%s/%s: %v", s3Bucket, prefix, err)
		}
		for _, o := range page.Contents {
			key := aws.ToString(o.Key)
			if !strings.HasSuffix(key, ".parquet") || o.LastModified == nil || o.LastModified.Before(since.Add(-time.Second)) {
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
			total += countMarkerRows(t, key, data, marker)
		}
	}
	return total
}

func countMarkerRows(t *testing.T, key string, data []byte, marker string) int {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open %s as Parquet: %v", key, err)
	}
	n := 0
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 256)
		for {
			k, err := rows.ReadRows(buf)
			for _, row := range buf[:k] {
				for _, v := range row {
					if v.Kind() == parquet.ByteArray && strings.Contains(string(v.ByteArray()), marker) {
						n++
						break
					}
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
	return n
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
