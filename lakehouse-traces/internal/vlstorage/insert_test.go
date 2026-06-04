package vlstorage

import (
	"errors"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type mockTraceWriter struct {
	rows     []schema.TraceRow
	writeErr error
}

func (m *mockTraceWriter) MustAddTraceRows(rows []schema.TraceRow) {
	m.rows = append(m.rows, rows...)
}

func (m *mockTraceWriter) CanWriteData() error {
	return m.writeErr
}

func makeLogRows(t *testing.T, fields ...logstorage.Field) *logstorage.LogRows {
	t.Helper()
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, fields, -1)
	return lr
}

func TestVTInsertAdapter_Legacy_MustAddRows_BasicFields(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "trace_id", Value: "abc123"},
		logstorage.Field{Name: "span_id", Value: "def456"},
		logstorage.Field{Name: "span.name", Value: "GET /api/users"},
		logstorage.Field{Name: "service.name", Value: "api-gateway"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]
	if row.TraceID != "abc123" {
		t.Errorf("TraceID = %q, want %q", row.TraceID, "abc123")
	}
	if row.SpanID != "def456" {
		t.Errorf("SpanID = %q, want %q", row.SpanID, "def456")
	}
	if row.SpanName != "GET /api/users" {
		t.Errorf("SpanName = %q, want %q", row.SpanName, "GET /api/users")
	}
	if row.ServiceName != "api-gateway" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "api-gateway")
	}
	if row.TimestampUnixNano != 1_000_000_000 {
		t.Errorf("Timestamp = %d, want %d", row.TimestampUnixNano, 1_000_000_000)
	}
}

func TestVTInsertAdapter_Legacy_MustAddRows_AllPromotedFields(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "trace_id", Value: "t1"},
		logstorage.Field{Name: "span_id", Value: "s1"},
		logstorage.Field{Name: "parent_span_id", Value: "p1"},
		logstorage.Field{Name: "span.name", Value: "op"},
		logstorage.Field{Name: "service.name", Value: "svc"},
		logstorage.Field{Name: "duration_ns", Value: "5000000"},
		logstorage.Field{Name: "start_time_unix_nano", Value: "999000000"},
		logstorage.Field{Name: "status.code", Value: "2"},
		logstorage.Field{Name: "status.message", Value: "OK"},
		logstorage.Field{Name: "span.kind", Value: "3"},
		logstorage.Field{Name: "http.method", Value: "POST"},
		logstorage.Field{Name: "http.status_code", Value: "201"},
		logstorage.Field{Name: "http.url", Value: "https://api.example.com/users"},
		logstorage.Field{Name: "db.system", Value: "postgresql"},
		logstorage.Field{Name: "db.statement", Value: "SELECT 1"},
		logstorage.Field{Name: "k8s.namespace.name", Value: "prod"},
		logstorage.Field{Name: "k8s.pod.name", Value: "api-pod-1"},
		logstorage.Field{Name: "k8s.deployment.name", Value: "api"},
		logstorage.Field{Name: "k8s.node.name", Value: "node-1"},
		logstorage.Field{Name: "deployment.environment", Value: "production"},
		logstorage.Field{Name: "cloud.region", Value: "us-east-1"},
		logstorage.Field{Name: "host.name", Value: "host-1"},
		logstorage.Field{Name: "scope.name", Value: "mylib"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	stringChecks := []struct {
		name string
		got  string
		want string
	}{
		{"TraceID", row.TraceID, "t1"},
		{"SpanID", row.SpanID, "s1"},
		{"ParentSpanID", row.ParentSpanID, "p1"},
		{"SpanName", row.SpanName, "op"},
		{"ServiceName", row.ServiceName, "svc"},
		{"StatusMessage", row.StatusMessage, "OK"},
		{"HTTPMethod", row.HTTPMethod, "POST"},
		{"HTTPStatusCode", row.HTTPStatusCode, "201"},
		{"HTTPUrl", row.HTTPUrl, "https://api.example.com/users"},
		{"DBSystem", row.DBSystem, "postgresql"},
		{"DBStatement", row.DBStatement, "SELECT 1"},
		{"K8sNamespaceName", row.K8sNamespaceName, "prod"},
		{"K8sPodName", row.K8sPodName, "api-pod-1"},
		{"K8sDeploymentName", row.K8sDeploymentName, "api"},
		{"K8sNodeName", row.K8sNodeName, "node-1"},
		{"DeployEnv", row.DeployEnv, "production"},
		{"CloudRegion", row.CloudRegion, "us-east-1"},
		{"HostName", row.HostName, "host-1"},
		{"ScopeName", row.ScopeName, "mylib"},
	}
	for _, c := range stringChecks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}

	if row.DurationNs != 5_000_000 {
		t.Errorf("DurationNs = %d, want %d", row.DurationNs, 5_000_000)
	}
	if row.StartTimeUnixNano != 999_000_000 {
		t.Errorf("StartTimeUnixNano = %d, want %d", row.StartTimeUnixNano, 999_000_000)
	}
	if row.StatusCode != 2 {
		t.Errorf("StatusCode = %d, want %d", row.StatusCode, 2)
	}
	if row.SpanKind != 3 {
		t.Errorf("SpanKind = %d, want %d", row.SpanKind, 3)
	}
}

func TestVTInsertAdapter_Legacy_MustAddRows_UnpromotedGoToSpanAttributes(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "trace_id", Value: "t1"},
		logstorage.Field{Name: "custom.tag", Value: "custom_value"},
		logstorage.Field{Name: "rpc.system", Value: "grpc"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	if row.SpanAttributes == nil {
		t.Fatal("SpanAttributes should not be nil")
	}
	if row.SpanAttributes["custom.tag"] != "custom_value" {
		t.Errorf("custom.tag = %q, want %q", row.SpanAttributes["custom.tag"], "custom_value")
	}
	if row.SpanAttributes["rpc.system"] != "grpc" {
		t.Errorf("rpc.system = %q, want %q", row.SpanAttributes["rpc.system"], "grpc")
	}
}

func TestVTInsertAdapter_Legacy_MustAddRows_EmptyRows(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 0 {
		t.Errorf("expected 0 rows for empty LogRows, got %d", len(w.rows))
	}
}

func TestVTInsertAdapter_Legacy_MustAddRows_MultipleRows(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	for i := 0; i < 100; i++ {
		lr.MustAdd(logstorage.TenantID{}, int64(i)*1_000_000_000,
			[]logstorage.Field{{Name: "trace_id", Value: "t1"}}, -1)
	}
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 100 {
		t.Errorf("expected 100 rows, got %d", len(w.rows))
	}
}

func TestVTInsertAdapter_Legacy_MustAddRows_StreamPreserved(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	streamFields := []string{"service.name", "k8s.namespace.name"}
	lr := logstorage.GetLogRows(streamFields, nil, nil, nil, "")
	lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, []logstorage.Field{
		{Name: "trace_id", Value: "t1"},
		{Name: "service.name", Value: "api"},
		{Name: "k8s.namespace.name", Value: "prod"},
	}, -1)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	if row.Stream == "" {
		t.Error("Stream should not be empty when stream fields are set")
	}
	if row.ServiceName != "api" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "api")
	}
}

func TestVTInsertAdapter_Legacy_CanWriteData_Healthy(t *testing.T) {
	a := &vtInsertAdapter{writer: &mockTraceWriter{}}
	if err := a.CanWriteData(); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestVTInsertAdapter_Legacy_CanWriteData_Unhealthy(t *testing.T) {
	a := &vtInsertAdapter{writer: &mockTraceWriter{writeErr: errors.New("s3 unavailable")}}
	err := a.CanWriteData()
	if err == nil {
		t.Error("expected error, got nil")
	}
	if err.Error() != "s3 unavailable" {
		t.Errorf("error = %q, want %q", err.Error(), "s3 unavailable")
	}
}

func TestVTInsertAdapter_Legacy_MustAddRows_EmptyFieldIgnored(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "", Value: "GET /health"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	if w.rows[0].SpanName != "" {
		t.Errorf("SpanName = %q, want empty (field name '' is ignored)", w.rows[0].SpanName)
	}
}

func TestVTInsertAdapter_Legacy_MustAddRows_NumericParsingErrors(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "duration_ns", Value: "not-a-number"},
		logstorage.Field{Name: "status.code", Value: "invalid"},
		logstorage.Field{Name: "span.kind", Value: "bad"},
		logstorage.Field{Name: "start_time_unix_nano", Value: "nope"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]
	if row.DurationNs != 0 {
		t.Errorf("DurationNs should be 0 on parse error, got %d", row.DurationNs)
	}
	if row.StatusCode != 0 {
		t.Errorf("StatusCode should be 0 on parse error, got %d", row.StatusCode)
	}
	if row.SpanKind != 0 {
		t.Errorf("SpanKind should be 0 on parse error, got %d", row.SpanKind)
	}
	if row.StartTimeUnixNano != 0 {
		t.Errorf("StartTimeUnixNano should be 0 on parse error, got %d", row.StartTimeUnixNano)
	}
}

func TestVTInsertAdapter_DropsTraceIDIndexRow(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	// Mirror what VT's vtinsert/insertutil/index_helper.go emits per trace.
	lr := makeLogRows(t,
		logstorage.Field{Name: otelpb.TraceIDIndexStreamName, Value: "42"},
		logstorage.Field{Name: "_msg", Value: "-"},
		logstorage.Field{Name: otelpb.TraceIDIndexFieldName, Value: "0123456789abcdef0123456789abcdef"},
		logstorage.Field{Name: otelpb.TraceIDIndexStartTimeFieldName, Value: "1000000000"},
		logstorage.Field{Name: otelpb.TraceIDIndexEndTimeFieldName, Value: "2000000000"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 0 {
		t.Fatalf("expected VT trace_id_idx row to be dropped, got %d rows persisted", len(w.rows))
	}
	if kind, drop := vtInternalRowKind(&logstorage.InsertRow{
		Fields: []logstorage.Field{{Name: otelpb.TraceIDIndexFieldName, Value: "x"}},
	}); kind != vtInternalKindTraceIDIdx || !drop {
		t.Errorf("vtInternalRowKind on trace_id_idx: got (%q, drop=%v), want (%q, drop=true)",
			kind, drop, vtInternalKindTraceIDIdx)
	}
}

// TestVTInsertAdapter_KeepsServiceGraphRow pins the post-Phase-B
// drop policy: service_graph rows ARE persisted (so the upstream
// /select/jaeger/api/dependencies reader can find them), but the
// counter still ticks under their kind for parity-check accounting.
func TestVTInsertAdapter_KeepsServiceGraphRow(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	// Mirror VT's service-graph emitter (app/victoria-traces/servicegraph).
	lr := makeLogRows(t,
		logstorage.Field{Name: otelpb.ServiceGraphStreamName, Value: "-"},
		logstorage.Field{Name: otelpb.ServiceGraphParentFieldName, Value: "frontend"},
		logstorage.Field{Name: otelpb.ServiceGraphChildFieldName, Value: "backend"},
		logstorage.Field{Name: otelpb.ServiceGraphCallCountFieldName, Value: "7"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected service_graph row to be persisted, got %d rows", len(w.rows))
	}
	// Classification still returns the kind so metrics + parity can
	// see service_graph activity, but drop=false so the writer keeps it.
	if kind, drop := vtInternalRowKind(&logstorage.InsertRow{
		Fields: []logstorage.Field{{Name: otelpb.ServiceGraphStreamName, Value: "-"}},
	}); kind != vtInternalKindServiceGraph || drop {
		t.Errorf("vtInternalRowKind on service_graph: got (%q, drop=%v), want (%q, drop=false)",
			kind, drop, vtInternalKindServiceGraph)
	}
}

func TestVTInternalRowKind_SpanRowReturnsEmpty(t *testing.T) {
	// A normal span row carries trace_id/span_id/etc. and must NOT be
	// classified as VT-internal — otherwise span data would be dropped.
	r := &logstorage.InsertRow{
		Fields: []logstorage.Field{
			{Name: "trace_id", Value: "abc"},
			{Name: "span_id", Value: "def"},
			{Name: "span.name", Value: "GET /"},
		},
	}
	if kind, drop := vtInternalRowKind(r); kind != "" || drop {
		t.Errorf("vtInternalRowKind on span row: got (%q, drop=%v), want (\"\", drop=false)",
			kind, drop)
	}
}

func TestUnmarshalStreamTags_InvalidData(t *testing.T) {
	st := logstorage.GetStreamTags()
	defer logstorage.PutStreamTags(st)

	err := unmarshalStreamTags(st, "\x00\x01\x02invalid")
	if err == nil {
		t.Error("expected error for invalid canonical data, got nil")
	}
}

func TestMapFieldToTraceRow_AllCases(t *testing.T) {
	row := &schema.TraceRow{}

	mapFieldToTraceRow(row, "", "span op")
	mapFieldToTraceRow(row, "trace_id", "t1")
	mapFieldToTraceRow(row, "span_id", "s1")
	mapFieldToTraceRow(row, "parent_span_id", "p1")
	mapFieldToTraceRow(row, "span.name", "op-override")
	mapFieldToTraceRow(row, "service.name", "svc")
	mapFieldToTraceRow(row, "duration_ns", "42000")
	mapFieldToTraceRow(row, "start_time_unix_nano", "1000")
	mapFieldToTraceRow(row, "status.code", "1")
	mapFieldToTraceRow(row, "status.message", "cancelled")
	mapFieldToTraceRow(row, "span.kind", "2")
	mapFieldToTraceRow(row, "http.method", "GET")
	mapFieldToTraceRow(row, "http.status_code", "200")
	mapFieldToTraceRow(row, "http.url", "http://localhost")
	mapFieldToTraceRow(row, "db.system", "mysql")
	mapFieldToTraceRow(row, "db.statement", "INSERT INTO t")
	mapFieldToTraceRow(row, "k8s.namespace.name", "ns")
	mapFieldToTraceRow(row, "k8s.pod.name", "pod")
	mapFieldToTraceRow(row, "k8s.deployment.name", "dep")
	mapFieldToTraceRow(row, "k8s.node.name", "node")
	mapFieldToTraceRow(row, "deployment.environment", "staging")
	mapFieldToTraceRow(row, "cloud.region", "eu-west-1")
	mapFieldToTraceRow(row, "host.name", "host1")
	mapFieldToTraceRow(row, "scope.name", "scope1")
	mapFieldToTraceRow(row, "custom_attr", "custom_val")

	stringChecks := []struct {
		name string
		got  string
		want string
	}{
		{"SpanName", row.SpanName, "op-override"},
		{"TraceID", row.TraceID, "t1"},
		{"SpanID", row.SpanID, "s1"},
		{"ParentSpanID", row.ParentSpanID, "p1"},
		{"ServiceName", row.ServiceName, "svc"},
		{"StatusMessage", row.StatusMessage, "cancelled"},
		{"HTTPMethod", row.HTTPMethod, "GET"},
		{"HTTPStatusCode", row.HTTPStatusCode, "200"},
		{"HTTPUrl", row.HTTPUrl, "http://localhost"},
		{"DBSystem", row.DBSystem, "mysql"},
		{"DBStatement", row.DBStatement, "INSERT INTO t"},
		{"K8sNamespaceName", row.K8sNamespaceName, "ns"},
		{"K8sPodName", row.K8sPodName, "pod"},
		{"K8sDeploymentName", row.K8sDeploymentName, "dep"},
		{"K8sNodeName", row.K8sNodeName, "node"},
		{"DeployEnv", row.DeployEnv, "staging"},
		{"CloudRegion", row.CloudRegion, "eu-west-1"},
		{"HostName", row.HostName, "host1"},
		{"ScopeName", row.ScopeName, "scope1"},
	}
	for _, c := range stringChecks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}

	if row.DurationNs != 42000 {
		t.Errorf("DurationNs = %d, want %d", row.DurationNs, 42000)
	}
	if row.StartTimeUnixNano != 1000 {
		t.Errorf("StartTimeUnixNano = %d, want %d", row.StartTimeUnixNano, 1000)
	}
	if row.StatusCode != 1 {
		t.Errorf("StatusCode = %d, want %d", row.StatusCode, 1)
	}
	if row.SpanKind != 2 {
		t.Errorf("SpanKind = %d, want %d", row.SpanKind, 2)
	}
	if row.SpanAttributes["custom_attr"] != "custom_val" {
		t.Errorf("custom_attr = %q, want %q", row.SpanAttributes["custom_attr"], "custom_val")
	}
}

// --- VT OTLP path tests (vtInsertAdapter with prefixed field names) ---

func TestVTInsertAdapter_MustAddRows_OTLPFields(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: otelpb.TraceIDField, Value: "abc123"},
		logstorage.Field{Name: otelpb.SpanIDField, Value: "span456"},
		logstorage.Field{Name: otelpb.ParentSpanIDField, Value: "parent789"},
		logstorage.Field{Name: otelpb.NameField, Value: "HTTP GET /users"},
		logstorage.Field{Name: otelpb.KindField, Value: "2"},
		logstorage.Field{Name: otelpb.DurationField, Value: "15000000"},
		logstorage.Field{Name: otelpb.StartTimeUnixNanoField, Value: "1700000000000000000"},
		logstorage.Field{Name: otelpb.EndTimeUnixNanoField, Value: "1700000015000000000"},
		logstorage.Field{Name: otelpb.StatusCodeField, Value: "1"},
		logstorage.Field{Name: otelpb.StatusMessageField, Value: "OK"},
		logstorage.Field{Name: otelpb.InstrumentationScopeName, Value: "my-instrumentation"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	checks := []struct {
		name string
		got  string
		want string
	}{
		{"TraceID", row.TraceID, "abc123"},
		{"SpanID", row.SpanID, "span456"},
		{"ParentSpanID", row.ParentSpanID, "parent789"},
		{"SpanName", row.SpanName, "HTTP GET /users"},
		{"StatusMessage", row.StatusMessage, "OK"},
		{"ScopeName", row.ScopeName, "my-instrumentation"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
	if row.SpanKind != 2 {
		t.Errorf("SpanKind = %d, want 2", row.SpanKind)
	}
	if row.DurationNs != 15_000_000 {
		t.Errorf("DurationNs = %d, want 15000000", row.DurationNs)
	}
	if row.StartTimeUnixNano != 1_700_000_000_000_000_000 {
		t.Errorf("StartTimeUnixNano = %d, want 1700000000000000000", row.StartTimeUnixNano)
	}
	if row.StatusCode != 1 {
		t.Errorf("StatusCode = %d, want 1", row.StatusCode)
	}
}

func TestVTInsertAdapter_MustAddRows_ResourceAttrs(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: otelpb.TraceIDField, Value: "t1"},
		logstorage.Field{Name: otelpb.NameField, Value: "op"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "service.name", Value: "payment-svc"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "k8s.namespace.name", Value: "production"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "k8s.pod.name", Value: "payment-abc123"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "k8s.deployment.name", Value: "payment"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "k8s.node.name", Value: "node-pool-a-1"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "deployment.environment", Value: "production"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "cloud.region", Value: "us-east-1"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "host.name", Value: "ip-10-0-1-42"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "custom.resource", Value: "custom-val"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	checks := []struct {
		name string
		got  string
		want string
	}{
		{"ServiceName", row.ServiceName, "payment-svc"},
		{"K8sNamespaceName", row.K8sNamespaceName, "production"},
		{"K8sPodName", row.K8sPodName, "payment-abc123"},
		{"K8sDeploymentName", row.K8sDeploymentName, "payment"},
		{"K8sNodeName", row.K8sNodeName, "node-pool-a-1"},
		{"DeployEnv", row.DeployEnv, "production"},
		{"CloudRegion", row.CloudRegion, "us-east-1"},
		{"HostName", row.HostName, "ip-10-0-1-42"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}

	if row.ResourceAttributes == nil {
		t.Fatal("ResourceAttributes should not be nil")
	}
	if row.ResourceAttributes["custom.resource"] != "custom-val" {
		t.Errorf("ResourceAttributes[custom.resource] = %q, want %q",
			row.ResourceAttributes["custom.resource"], "custom-val")
	}
}

func TestVTInsertAdapter_MustAddRows_SpanAttrs(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: otelpb.TraceIDField, Value: "t1"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "http.method", Value: "POST"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "http.status_code", Value: "201"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "http.url", Value: "https://api.example.com/orders"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "db.system", Value: "redis"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "db.statement", Value: "GET session:abc"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "rpc.system", Value: "grpc"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	checks := []struct {
		name string
		got  string
		want string
	}{
		{"HTTPMethod", row.HTTPMethod, "POST"},
		{"HTTPStatusCode", row.HTTPStatusCode, "201"},
		{"HTTPUrl", row.HTTPUrl, "https://api.example.com/orders"},
		{"DBSystem", row.DBSystem, "redis"},
		{"DBStatement", row.DBStatement, "GET session:abc"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}

	if row.SpanAttributes == nil {
		t.Fatal("SpanAttributes should not be nil")
	}
	if row.SpanAttributes["rpc.system"] != "grpc" {
		t.Errorf("SpanAttributes[rpc.system] = %q, want %q",
			row.SpanAttributes["rpc.system"], "grpc")
	}
}

func TestVTInsertAdapter_MustAddRows_OTLPMetadataStored(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: otelpb.TraceIDField, Value: "t1"},
		logstorage.Field{Name: otelpb.EndTimeUnixNanoField, Value: "99999"},
		logstorage.Field{Name: otelpb.InstrumentationScopeVersion, Value: "1.0"},
		logstorage.Field{Name: otelpb.TraceStateField, Value: "congo=t"},
		logstorage.Field{Name: otelpb.FlagsField, Value: "1"},
		logstorage.Field{Name: otelpb.DroppedAttributesCountField, Value: "0"},
		logstorage.Field{Name: otelpb.DroppedEventsCountField, Value: "0"},
		logstorage.Field{Name: otelpb.DroppedLinksCountField, Value: "0"},
		logstorage.Field{Name: otelpb.InstrumentationScopeAttrPrefix + "key", Value: "val"},
		logstorage.Field{Name: otelpb.EventPrefix + "0.name", Value: "exception"},
		logstorage.Field{Name: otelpb.LinkPrefix + "0.trace_id", Value: "linked"},
		logstorage.Field{Name: "_msg", Value: "-"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	if row.TraceID != "t1" {
		t.Errorf("TraceID = %q, want %q", row.TraceID, "t1")
	}

	// _msg is NOT in this list: VL's MustAdd extracts it as the body field,
	// so it doesn't appear in ForEachRow's Fields slice.
	storedKeys := []string{
		otelpb.EndTimeUnixNanoField,
		otelpb.InstrumentationScopeVersion,
		otelpb.TraceStateField,
		otelpb.FlagsField,
		otelpb.DroppedAttributesCountField,
		otelpb.DroppedEventsCountField,
		otelpb.DroppedLinksCountField,
	}
	for _, key := range storedKeys {
		if _, ok := row.SpanAttributes[key]; !ok {
			t.Errorf("OTLP metadata field %q not found in SpanAttributes", key)
		}
	}
	if len(row.SpanAttributes) != len(storedKeys) {
		t.Errorf("SpanAttributes should have %d entries, got %d: %v",
			len(storedKeys), len(row.SpanAttributes), row.SpanAttributes)
	}
	if len(row.ResourceAttributes) != 0 {
		t.Errorf("ResourceAttributes should be empty, got %v", row.ResourceAttributes)
	}
}

func TestVTInsertAdapter_IsLocalStorage(t *testing.T) {
	a := &vtInsertAdapter{writer: &mockTraceWriter{}}
	if !a.IsLocalStorage() {
		t.Error("IsLocalStorage() should return true")
	}
}

func TestVTInsertAdapter_CanWriteData(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		a := &vtInsertAdapter{writer: &mockTraceWriter{}}
		if err := a.CanWriteData(); err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	})
	t.Run("unhealthy", func(t *testing.T) {
		a := &vtInsertAdapter{writer: &mockTraceWriter{writeErr: errors.New("disk full")}}
		if err := a.CanWriteData(); err == nil {
			t.Error("expected error, got nil")
		}
	})
}

func TestVTInsertAdapter_MustAddRows_FullOTLPSpan(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: otelpb.TraceIDField, Value: "aabbccdd11223344"},
		logstorage.Field{Name: otelpb.SpanIDField, Value: "1122334455"},
		logstorage.Field{Name: otelpb.ParentSpanIDField, Value: "0011223344"},
		logstorage.Field{Name: otelpb.NameField, Value: "gRPC /payment.Process"},
		logstorage.Field{Name: otelpb.KindField, Value: "3"},
		logstorage.Field{Name: otelpb.DurationField, Value: "250000000"},
		logstorage.Field{Name: otelpb.StartTimeUnixNanoField, Value: "1700000000000000000"},
		logstorage.Field{Name: otelpb.EndTimeUnixNanoField, Value: "1700000250000000000"},
		logstorage.Field{Name: otelpb.StatusCodeField, Value: "2"},
		logstorage.Field{Name: otelpb.StatusMessageField, Value: "internal error"},
		logstorage.Field{Name: otelpb.InstrumentationScopeName, Value: "payment-instrumentation"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "service.name", Value: "payment-service"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "deployment.environment", Value: "production"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "cloud.region", Value: "eu-west-1"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "host.name", Value: "ip-10-0-3-88"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "k8s.namespace.name", Value: "production"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "k8s.pod.name", Value: "payment-xyz789"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "k8s.deployment.name", Value: "payment"},
		logstorage.Field{Name: otelpb.ResourceAttrPrefix + "k8s.node.name", Value: "node-pool-b-1"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "http.method", Value: "POST"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "http.status_code", Value: "500"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "http.url", Value: "https://payment.internal/process"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "rpc.system", Value: "grpc"},
		logstorage.Field{Name: otelpb.SpanAttrPrefixField + "rpc.method", Value: "Process"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	if row.TraceID != "aabbccdd11223344" {
		t.Errorf("TraceID = %q", row.TraceID)
	}
	if row.SpanID != "1122334455" {
		t.Errorf("SpanID = %q", row.SpanID)
	}
	if row.ParentSpanID != "0011223344" {
		t.Errorf("ParentSpanID = %q", row.ParentSpanID)
	}
	if row.SpanName != "gRPC /payment.Process" {
		t.Errorf("SpanName = %q", row.SpanName)
	}
	if row.SpanKind != 3 {
		t.Errorf("SpanKind = %d", row.SpanKind)
	}
	if row.DurationNs != 250_000_000 {
		t.Errorf("DurationNs = %d", row.DurationNs)
	}
	if row.StartTimeUnixNano != 1_700_000_000_000_000_000 {
		t.Errorf("StartTimeUnixNano = %d", row.StartTimeUnixNano)
	}
	if row.StatusCode != 2 {
		t.Errorf("StatusCode = %d", row.StatusCode)
	}
	if row.StatusMessage != "internal error" {
		t.Errorf("StatusMessage = %q", row.StatusMessage)
	}
	if row.ScopeName != "payment-instrumentation" {
		t.Errorf("ScopeName = %q", row.ScopeName)
	}
	if row.ServiceName != "payment-service" {
		t.Errorf("ServiceName = %q", row.ServiceName)
	}
	if row.DeployEnv != "production" {
		t.Errorf("DeployEnv = %q", row.DeployEnv)
	}
	if row.CloudRegion != "eu-west-1" {
		t.Errorf("CloudRegion = %q", row.CloudRegion)
	}
	if row.HostName != "ip-10-0-3-88" {
		t.Errorf("HostName = %q", row.HostName)
	}
	if row.K8sNamespaceName != "production" {
		t.Errorf("K8sNamespaceName = %q", row.K8sNamespaceName)
	}
	if row.K8sPodName != "payment-xyz789" {
		t.Errorf("K8sPodName = %q", row.K8sPodName)
	}
	if row.K8sDeploymentName != "payment" {
		t.Errorf("K8sDeploymentName = %q", row.K8sDeploymentName)
	}
	if row.K8sNodeName != "node-pool-b-1" {
		t.Errorf("K8sNodeName = %q", row.K8sNodeName)
	}
	if row.HTTPMethod != "POST" {
		t.Errorf("HTTPMethod = %q", row.HTTPMethod)
	}
	if row.HTTPStatusCode != "500" {
		t.Errorf("HTTPStatusCode = %q", row.HTTPStatusCode)
	}
	if row.HTTPUrl != "https://payment.internal/process" {
		t.Errorf("HTTPUrl = %q", row.HTTPUrl)
	}
	if row.SpanAttributes == nil {
		t.Fatal("SpanAttributes should not be nil")
	}
	if row.SpanAttributes["rpc.system"] != "grpc" {
		t.Errorf("SpanAttributes[rpc.system] = %q", row.SpanAttributes["rpc.system"])
	}
	if row.SpanAttributes["rpc.method"] != "Process" {
		t.Errorf("SpanAttributes[rpc.method] = %q", row.SpanAttributes["rpc.method"])
	}
}

func TestVTInsertAdapter_MustAddRows_EmptyRows(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(w.rows))
	}
}

func BenchmarkLogRowsToTraceRows(b *testing.B) {
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	for i := 0; i < 1000; i++ {
		lr.MustAdd(logstorage.TenantID{}, int64(i)*1_000_000_000, []logstorage.Field{
			{Name: "trace_id", Value: "abc123def456"},
			{Name: "span_id", Value: "span123"},
			{Name: "service.name", Value: "benchmark-svc"},
			{Name: "span.name", Value: "GET /api/benchmark"},
			{Name: "duration_ns", Value: "5000000"},
			{Name: "k8s.namespace.name", Value: "prod"},
			{Name: "custom_field_1", Value: "value1"},
			{Name: "custom_field_2", Value: "value2"},
		}, -1)
	}
	defer logstorage.PutLogRows(lr)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows := logRowsToTraceRows(lr)
		if len(rows) != 1000 {
			b.Fatalf("expected 1000 rows, got %d", len(rows))
		}
	}
}

// TestSetInsertStorage_NoPanic exercises SetInsertStorage (previously 0%).
func TestSetInsertStorage_NoPanic(t *testing.T) {
	w := &mockTraceWriter{}
	// Should not panic — just registers the adapter with vtinsert.
	SetInsertStorage(w)
}
