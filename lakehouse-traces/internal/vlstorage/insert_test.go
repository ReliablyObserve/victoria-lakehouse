package vlstorage

import (
	"errors"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

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

func TestInsertAdapter_MustAddRows_BasicFields(t *testing.T) {
	w := &mockTraceWriter{}
	a := &insertAdapter{writer: w}

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

func TestInsertAdapter_MustAddRows_AllPromotedFields(t *testing.T) {
	w := &mockTraceWriter{}
	a := &insertAdapter{writer: w}

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

func TestInsertAdapter_MustAddRows_UnpromotedGoToSpanAttributes(t *testing.T) {
	w := &mockTraceWriter{}
	a := &insertAdapter{writer: w}

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

func TestInsertAdapter_MustAddRows_EmptyRows(t *testing.T) {
	w := &mockTraceWriter{}
	a := &insertAdapter{writer: w}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 0 {
		t.Errorf("expected 0 rows for empty LogRows, got %d", len(w.rows))
	}
}

func TestInsertAdapter_MustAddRows_MultipleRows(t *testing.T) {
	w := &mockTraceWriter{}
	a := &insertAdapter{writer: w}

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

func TestInsertAdapter_MustAddRows_StreamPreserved(t *testing.T) {
	w := &mockTraceWriter{}
	a := &insertAdapter{writer: w}

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

func TestInsertAdapter_CanWriteData_Healthy(t *testing.T) {
	a := &insertAdapter{writer: &mockTraceWriter{}}
	if err := a.CanWriteData(); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestInsertAdapter_CanWriteData_Unhealthy(t *testing.T) {
	a := &insertAdapter{writer: &mockTraceWriter{writeErr: errors.New("s3 unavailable")}}
	err := a.CanWriteData()
	if err == nil {
		t.Error("expected error, got nil")
	}
	if err.Error() != "s3 unavailable" {
		t.Errorf("error = %q, want %q", err.Error(), "s3 unavailable")
	}
}

func TestInsertAdapter_MustAddRows_SpanNameFromEmptyField(t *testing.T) {
	w := &mockTraceWriter{}
	a := &insertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "", Value: "GET /health"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	if w.rows[0].SpanName != "GET /health" {
		t.Errorf("SpanName = %q, want %q", w.rows[0].SpanName, "GET /health")
	}
}

func TestInsertAdapter_MustAddRows_NumericParsingErrors(t *testing.T) {
	w := &mockTraceWriter{}
	a := &insertAdapter{writer: w}

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
