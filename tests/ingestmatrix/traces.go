package ingestmatrix

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const spans3 = 3

var msgGap = []string{"traces-default-msg-value"}

func tracesCases() []Case {
	return []Case{
		{
			ID: "otlp_traces_protobuf", Signal: Traces, Gaps: msgGap, Title: "/insert/opentelemetry/v1/traces: OTLP/HTTP traces, protobuf", Transport: HTTP,
			Forms: bothForms, Rows: spans3, Routes: []string{"/insert/opentelemetry/v1/traces"},
			Counter: `vt_rows_ingested_total{type="opentelemetry_traces_otlphttp_protobuf"}`, Build: buildOTLPTracesProtobuf,
			Read: ReadSpec{Query: `trace_id:="%MARKER%"`},
		},
		{
			ID: "native", Signal: Traces, Gaps: msgGap, Title: "/insert/native: VictoriaTraces native binary protocol (span rows)", Transport: HTTP,
			Forms: bothForms, Rows: spans3, Routes: []string{"/insert/native"},
			Counter: `vt_rows_ingested_total{type="nativeinsert"}`, Build: buildTraceNativeAt("/insert/native"),
			Read: ReadSpec{Query: `trace_id:="%MARKER%"`},
		},
		{
			ID: "multitenant_native", Signal: Traces, Gaps: msgGap, Title: "/insert/multitenant/native: native protocol, tenant inside each span row", Transport: HTTP,
			Forms: []Form{Numeric}, FormNote: "the tenant is carried by each row of the payload and request tenant headers are ignored upstream, so there is no header or alias form",
			Rows: spans3, Routes: []string{"/insert/multitenant/native"}, NoTenantHeaders: true,
			Counter: `vt_rows_ingested_total{type="nativemultitenant"}`, Build: buildTraceNativeAt("/insert/multitenant/native"),
			Read: ReadSpec{Query: `trace_id:="%MARKER%"`},
		},
		{
			ID: "otlp_traces_json", Signal: Traces, Title: "/insert/opentelemetry/v1/traces: OTLP/HTTP traces, JSON", Transport: HTTP,
			Forms: bothForms, Rows: spans3, Routes: []string{"/insert/opentelemetry/v1/traces"},
			Counter: `vt_rows_ingested_total{type="opentelemetry_traces_otlphttp_json"}`, Build: buildOTLPTracesJSON,
			Read: ReadSpec{Query: `trace_id:="%MARKER%"`},
		},
		{
			ID: "otlp_traces_grpc", Signal: Traces, Gaps: msgGap, Title: "OTLP/gRPC trace export (-otlpGRPCListenAddr)", Transport: GRPC,
			Forms: []Form{Numeric}, FormNote: "gRPC carries the tenant as AccountID/ProjectID call metadata; upstream has no OrgID alias on this listener",
			Rows: spans3, Flags: []string{"otlpGRPCListenAddr"},
			Counter: `vt_rows_ingested_total{type="opentelemetry_traces_otlpgrpc"}`,
			Read:    ReadSpec{Query: `trace_id:="%MARKER%"`},
		},
	}
}

// TraceID derives the 32-hex trace id of a case run from its marker seed, so
// every case/form/run writes its own trace.
func TraceID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:16])
}

func spanID(trace string, i int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", trace, i)))
	return hex.EncodeToString(sum[:8])
}

// otlpSpans builds the three spans of a run: a root and two children, one
// second apart, ending at Base. The structure is the same for every transport.
func otlpSpans(p Params) *coltrace.ExportTraceServiceRequest {
	var spans []*tracepb.Span
	rootID, _ := hex.DecodeString(spanID(p.Marker, 1))
	tid, _ := hex.DecodeString(p.Marker)
	for i := 1; i <= spans3; i++ {
		sid, _ := hex.DecodeString(spanID(p.Marker, i))
		start := p.RowTime(i, spans3)
		s := &tracepb.Span{
			TraceId:           tid,
			SpanId:            sid,
			Name:              fmt.Sprintf("ingest-matrix op %d", i),
			Kind:              tracepb.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: uint64(start.UnixNano()),
			EndTimeUnixNano:   uint64(start.Add(250 * time.Millisecond).UnixNano()),
			Attributes: []*commonpb.KeyValue{
				strAttr("row", strconv.Itoa(i)), strAttr("env", "e2e"), strAttr("http.method", "GET"),
			},
			Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
		}
		if i > 1 {
			s.ParentSpanId = rootID
		}
		spans = append(spans, s)
	}
	return &coltrace.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:   &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.name", svcName)}},
		ScopeSpans: []*tracepb.ScopeSpans{{Scope: &commonpb.InstrumentationScope{Name: "ingest-matrix"}, Spans: spans}},
	}}}
}

func buildOTLPTracesProtobuf(p Params) []Request {
	body, err := proto.Marshal(otlpSpans(p))
	if err != nil {
		panic(err)
	}
	return []Request{{Method: "POST", Path: "/insert/opentelemetry/v1/traces", Header: map[string]string{"Content-Type": ctProto}, Body: body, Rows: spans3}}
}

func buildOTLPTracesJSON(p Params) []Request {
	var spans []string
	for i := 1; i <= spans3; i++ {
		start := p.RowTime(i, spans3)
		parent := ""
		if i > 1 {
			parent = fmt.Sprintf(`"parentSpanId":%s,`, jsonString(spanID(p.Marker, 1)))
		}
		spans = append(spans, fmt.Sprintf(
			`{"traceId":%s,"spanId":%s,%s"name":%s,"kind":2,"startTimeUnixNano":%s,"endTimeUnixNano":%s,"attributes":[{"key":"row","value":{"stringValue":%s}},{"key":"env","value":{"stringValue":"e2e"}},{"key":"http.method","value":{"stringValue":"GET"}}],"status":{"code":1}}`,
			jsonString(p.Marker), jsonString(spanID(p.Marker, i)), parent, jsonString(fmt.Sprintf("ingest-matrix op %d", i)),
			jsonString(strconv.FormatInt(start.UnixNano(), 10)), jsonString(strconv.FormatInt(start.Add(250*time.Millisecond).UnixNano(), 10)),
			jsonString(strconv.Itoa(i))))
	}
	body := `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"` + svcName + `"}}]},"scopeSpans":[{"scope":{"name":"ingest-matrix"},"spans":[` + strings.Join(spans, ",") + `]}]}]}`
	return []Request{{Method: "POST", Path: "/insert/opentelemetry/v1/traces", Header: map[string]string{"Content-Type": ctJSON}, Body: []byte(body), Rows: spans3}}
}

// GRPCResult is the answer of one OTLP/gRPC export.
type GRPCResult struct {
	// Code is the gRPC status code name ("OK", "Unavailable", ...).
	Code string
	// Response is the marshalled ExportTraceServiceResponse ("" on error).
	Response []byte
	Err      error
}

// GRPCExport sends the case's spans to an OTLP/gRPC listener (plaintext) with
// the tenant as AccountID/ProjectID call metadata, as upstream documents.
func GRPCExport(ctx context.Context, addr string, p Params) GRPCResult {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return GRPCResult{Code: "dial", Err: err}
	}
	defer func() { _ = conn.Close() }()
	ctx = metadata.AppendToOutgoingContext(ctx,
		"AccountID", strconv.FormatUint(uint64(p.Tenant.Account), 10),
		"ProjectID", strconv.FormatUint(uint64(p.Tenant.Project), 10))
	resp, err := coltrace.NewTraceServiceClient(conn).Export(ctx, otlpSpans(p))
	if err != nil {
		return GRPCResult{Code: grpcCode(err), Err: err}
	}
	b, _ := proto.Marshal(resp)
	return GRPCResult{Code: "OK", Response: b}
}

func grpcCode(err error) string { return status.Code(err).String() }

// traceNativeRows builds the span rows of a run as vtagent sends them over the
// native protocol: upstream's own logstorage.InsertRow with the VictoriaTraces
// span field names, stream tags {name, resource_attr:service.name}.
func traceNativeRows(p Params, tenant Tenant) []byte {
	var out []byte
	for i := 1; i <= spans3; i++ {
		name := fmt.Sprintf("ingest-matrix op %d", i)
		start := p.RowTime(i, spans3)
		st := logstorage.GetStreamTags()
		st.Add("name", name)
		st.Add("resource_attr:service.name", svcName)
		r := logstorage.InsertRow{
			TenantID:            logstorage.TenantID{AccountID: tenant.Account, ProjectID: tenant.Project},
			StreamTagsCanonical: string(st.MarshalCanonical(nil)),
			Timestamp:           start.UnixNano(),
		}
		logstorage.PutStreamTags(st)
		fields := []logstorage.Field{
			{Name: "name", Value: name},
			{Name: "resource_attr:service.name", Value: svcName},
			{Name: "trace_id", Value: p.Marker},
			{Name: "span_id", Value: spanID(p.Marker, i)},
			{Name: "kind", Value: "2"},
			{Name: "start_time_unix_nano", Value: strconv.FormatInt(start.UnixNano(), 10)},
			{Name: "end_time_unix_nano", Value: strconv.FormatInt(start.Add(250*time.Millisecond).UnixNano(), 10)},
			{Name: "duration", Value: "250000000"},
			{Name: "status_code", Value: "1"},
			{Name: "span_attr:row", Value: strconv.Itoa(i)},
			{Name: "span_attr:env", Value: "e2e"},
		}
		if i > 1 {
			fields = append(fields, logstorage.Field{Name: "parent_span_id", Value: spanID(p.Marker, 1)})
		}
		r.Fields = fields
		out = r.Marshal(out)
	}
	return out
}

func buildTraceNativeAt(path string) func(Params) []Request {
	return func(p Params) []Request {
		return []Request{{Method: "POST", Path: path, Query: "version=v1",
			Header: map[string]string{"Content-Type": "application/octet-stream"}, Body: traceNativeRows(p, p.Tenant), Rows: spans3}}
	}
}
