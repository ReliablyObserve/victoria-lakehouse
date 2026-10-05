package ingestmatrix

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var testBase = time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC)

func paramsFor(c Case, f Form) Params { return NewParams(c, f, "unit", testBase) }

func TestRowIDsAreUniqueAndSorted(t *testing.T) {
	ids := RowIDs()
	seen := map[string]bool{}
	for i, id := range ids {
		if seen[id] {
			t.Errorf("duplicate row id %s", id)
		}
		seen[id] = true
		if i > 0 && ids[i-1] >= id {
			t.Errorf("row ids not sorted: %s then %s", ids[i-1], id)
		}
	}
	for _, want := range []string{"vl.ingest.jsonline.numeric", "vl.ingest.jsonline.alias", "vl.ingest.syslog_udp_rfc5424.numeric", "vt.ingest.otlp_traces_grpc.numeric"} {
		if !seen[want] {
			t.Errorf("missing row id %s", want)
		}
	}
}

func TestNewParams(t *testing.T) {
	logs := CasesFor(Logs)[0]
	p := paramsFor(logs, Alias)
	if p.Tenant != AliasTenant {
		t.Errorf("alias form must use the alias tenant, got %+v", p.Tenant)
	}
	if !strings.HasPrefix(p.Marker, "ingmunit") || strings.ContainsAny(p.Marker, "_-. ") {
		t.Errorf("a log marker must be one LogsQL word, got %q", p.Marker)
	}
	if got := logs.ReadQuery(p); got != p.Marker {
		t.Errorf("default read query = %q, want the marker", got)
	}
	var tr Case
	for _, c := range CasesFor(Traces) {
		if c.Read.Query != "" {
			tr = c
			break
		}
	}
	tp := paramsFor(tr, Numeric)
	if len(tp.Marker) != 32 {
		t.Errorf("a trace marker is a 32-hex trace id, got %q", tp.Marker)
	}
	if q := tr.ReadQuery(tp); q != `trace_id:="`+tp.Marker+`"` {
		t.Errorf("trace read query = %q", q)
	}
	if paramsFor(logs, Numeric).Marker == p.Marker {
		t.Errorf("markers of different forms must differ")
	}
	if tp.RowTime(3, 3) != testBase || tp.RowTime(1, 3) != testBase.Add(-2*time.Second) {
		t.Errorf("rows are one second apart and end at Base")
	}
	if TenantFor(Numeric) != NumericTenant || Level(1) != "INFO" || Level(4) != "INFO" || Level(3) != "ERROR" {
		t.Errorf("tenant or level helpers wrong")
	}
	if Surface(Traces) != "vt" || Surface(Logs) != "vl" {
		t.Errorf("surface mapping wrong")
	}
}

func TestExercisedCoversRoutesFlagsAndProbes(t *testing.T) {
	logs := Exercised(Logs)
	for _, want := range []string{"route:/insert/jsonline", "route:/internal/insert", "flag:syslog.listenAddr.tcp", "flag:syslog.listenAddr.udp", "route:/insert/ready"} {
		if logs[want] == "" {
			t.Errorf("logs: %s is not exercised", want)
		}
	}
	traces := Exercised(Traces)
	for _, want := range []string{"route:/insert/opentelemetry/v1/traces", "flag:otlpGRPCListenAddr", "route:/insert/ready", "route:/insert/native"} {
		if traces[want] == "" {
			t.Errorf("traces: %s is not exercised", want)
		}
	}
	if _, ok := GapByID("no-such-gap"); ok {
		t.Errorf("GapByID invented a gap")
	}
}

// Every HTTP case builds requests whose declared rows add up to the case's rows,
// with a path under one of the case's routes and a non-empty body.
func TestHTTPCasesBuildConsistentRequests(t *testing.T) {
	for _, c := range AllCases() {
		if c.Transport != HTTP {
			continue
		}
		for _, f := range c.Forms {
			reqs := c.Build(paramsFor(c, f))
			rows := 0
			for _, r := range reqs {
				rows += r.Rows
				if len(r.Body) == 0 || r.Method != "POST" {
					t.Errorf("%s/%s: empty body or non-POST request %+v", c.ID, f, r.Path)
				}
				if !contains(c.Routes, r.Path) {
					t.Errorf("%s/%s: request path %s is not among the case's routes %v", c.ID, f, r.Path, c.Routes)
				}
			}
			want := c.Rows
			if c.Rejected {
				want = rows // a refused payload still carries rows; upstream refuses it
			}
			if rows != want || (!c.Rejected && rows != c.Rows) {
				t.Errorf("%s/%s: requests carry %d rows, case says %d", c.ID, f, rows, c.Rows)
			}
		}
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func caseByID(t *testing.T, sig Signal, id string) Case {
	t.Helper()
	for _, c := range CasesFor(sig) {
		if c.ID == id {
			return c
		}
	}
	t.Fatalf("no %s case %s", sig, id)
	return Case{}
}

func TestJSONBodiesAreValidJSON(t *testing.T) {
	for _, tc := range []struct {
		sig   Signal
		id    string
		lines bool
	}{{Logs, "jsonline", true}, {Logs, "elasticsearch_bulk", true}, {Logs, "loki_json", false}, {Logs, "otlp_logs_json", false}, {Traces, "otlp_traces_json", false}} {
		c := caseByID(t, tc.sig, tc.id)
		for _, r := range c.Build(paramsFor(c, Numeric)) {
			if tc.lines {
				for _, l := range bytes.Split(bytes.TrimSpace(r.Body), []byte("\n")) {
					var v map[string]any
					if err := json.Unmarshal(l, &v); err != nil {
						t.Errorf("%s: invalid JSON line %q: %v", tc.id, l, err)
					}
				}
				continue
			}
			var v map[string]any
			if err := json.Unmarshal(r.Body, &v); err != nil {
				t.Errorf("%s: invalid JSON body: %v\n%s", tc.id, err, r.Body)
			}
		}
	}
	for _, id := range []string{"splunk_event", "datadog_logs"} {
		c := caseByID(t, Logs, id)
		for _, r := range c.Build(paramsFor(c, Numeric)) {
			var v any
			if err := json.Unmarshal(r.Body, &v); err != nil {
				t.Errorf("%s: invalid JSON body for %s: %v", id, r.Path, err)
			}
		}
	}
}

func TestNativeBodiesRoundTripThroughUpstreamDecoder(t *testing.T) {
	for _, tc := range []struct {
		sig Signal
		id  string
	}{{Logs, "native"}, {Logs, "multitenant_native"}, {Logs, "internal_insert"}, {Traces, "native"}, {Traces, "multitenant_native"}} {
		c := caseByID(t, tc.sig, tc.id)
		p := paramsFor(c, Numeric)
		body := c.Build(p)[0].Body
		n := 0
		for len(body) > 0 {
			var r logstorage.InsertRow
			tail, err := r.UnmarshalInplace(body)
			if err != nil {
				t.Fatalf("%s: upstream cannot decode row %d: %v", tc.id, n, err)
			}
			body = tail
			n++
			if r.TenantID.AccountID != p.Tenant.Account || r.TenantID.ProjectID != p.Tenant.Project {
				t.Errorf("%s: row tenant %v, want %+v", tc.id, r.TenantID, p.Tenant)
			}
			if tc.sig == Traces {
				found := false
				for _, f := range r.Fields {
					if f.Name == "trace_id" && f.Value == p.Marker {
						found = true
					}
				}
				if !found {
					t.Errorf("%s: span row has no trace_id=%s", tc.id, p.Marker)
				}
			}
		}
		if n != c.Rows {
			t.Errorf("%s: %d rows, want %d", tc.id, n, c.Rows)
		}
	}
}

func TestLokiProtobufDecodes(t *testing.T) {
	c := caseByID(t, Logs, "loki_protobuf")
	p := paramsFor(c, Numeric)
	raw, err := snappy.Decode(nil, c.Build(p)[0].Body)
	if err != nil {
		t.Fatalf("not snappy: %v", err)
	}
	streams, lines := 0, 0
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 || num != 1 || typ != protowire.BytesType {
			t.Fatalf("unexpected tag %d/%d", num, typ)
		}
		raw = raw[n:]
		stream, n := protowire.ConsumeBytes(raw)
		if n < 0 {
			t.Fatalf("bad stream bytes")
		}
		raw = raw[n:]
		streams++
		if strings.Contains(string(stream), p.Marker) {
			lines++
		}
	}
	if streams != c.Rows || lines != c.Rows {
		t.Errorf("streams=%d lines carrying the marker=%d, want %d", streams, lines, c.Rows)
	}
}

func TestOTLPProtobufBodiesUnmarshal(t *testing.T) {
	lc := caseByID(t, Logs, "otlp_logs_protobuf")
	var lreq collogs.ExportLogsServiceRequest
	if err := proto.Unmarshal(lc.Build(paramsFor(lc, Numeric))[0].Body, &lreq); err != nil {
		t.Fatalf("logs: %v", err)
	}
	if got := len(lreq.ResourceLogs[0].ScopeLogs[0].LogRecords); got != lc.Rows {
		t.Errorf("log records = %d, want %d", got, lc.Rows)
	}
	tc := caseByID(t, Traces, "otlp_traces_protobuf")
	var treq coltrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(tc.Build(paramsFor(tc, Numeric))[0].Body, &treq); err != nil {
		t.Fatalf("traces: %v", err)
	}
	spans := treq.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != tc.Rows {
		t.Fatalf("spans = %d, want %d", len(spans), tc.Rows)
	}
	for _, s := range spans[1:] {
		if !bytes.Equal(s.ParentSpanId, spans[0].SpanId) {
			t.Errorf("child span must name the root as its parent")
		}
	}
}

func TestSyslogLinesCarryTheMarkerAndDialect(t *testing.T) {
	for _, tc := range []struct {
		id      string
		rfc5424 bool
	}{{"syslog_tcp_rfc3164", false}, {"syslog_udp_rfc3164", false}, {"syslog_tcp_rfc5424", true}, {"syslog_udp_rfc5424", true}} {
		c := caseByID(t, Logs, tc.id)
		p := paramsFor(c, Numeric)
		lines := c.Lines(p)
		if len(lines) != c.Rows {
			t.Fatalf("%s: %d lines, want %d", tc.id, len(lines), c.Rows)
		}
		for _, l := range lines {
			s := string(l)
			if !strings.Contains(s, p.Marker) {
				t.Errorf("%s: line %q lacks the marker", tc.id, s)
			}
			// RFC5424 has a version digit right after <PRI>.
			afterPri := s[strings.Index(s, ">")+1:]
			if got := strings.HasPrefix(afterPri, "1 "); got != tc.rfc5424 {
				t.Errorf("%s: dialect mismatch in %q", tc.id, s)
			}
		}
	}
}

func TestNormalizersAndHelpers(t *testing.T) {
	es := caseByID(t, Logs, "elasticsearch_bulk")
	got := es.NormalizeResponse([]byte(`{"took":12,"errors":false,"items":[]}`))
	if strings.Contains(string(got), "took") || !strings.Contains(string(got), "errors") {
		t.Errorf("took must be dropped and the rest kept: %s", got)
	}
	if got := es.NormalizeResponse([]byte("not json")); string(got) != "not json" {
		t.Errorf("a non-JSON body must pass through unchanged: %s", got)
	}
	if TraceID("a") == TraceID("b") || len(TraceID("a")) != 32 {
		t.Errorf("TraceID must be a 32-hex digest of the seed")
	}
	if len(Probes()) == 0 || len(Exclusions()) == 0 {
		t.Errorf("probes and exclusions must not be empty")
	}
	if grpcCode(nil) == "" {
		t.Errorf("grpcCode(nil) must name a code")
	}
}
