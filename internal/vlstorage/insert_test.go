package vlstorage

import (
	"errors"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// mockLogWriter captures calls to MustAddLogRows for assertion.
type mockLogWriter struct {
	rows     []schema.LogRow
	writeErr error
}

func (m *mockLogWriter) MustAddLogRows(rows []schema.LogRow) {
	m.rows = append(m.rows, rows...)
}

func (m *mockLogWriter) CanWriteData() error {
	return m.writeErr
}

func makeLogRows(t *testing.T, fields ...logstorage.Field) *logstorage.LogRows {
	t.Helper()
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, fields, -1)
	return lr
}

func TestInsertAdapter_MustAddRows_BasicFields(t *testing.T) {
	w := &mockLogWriter{}
	a := &insertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "_msg", Value: "hello world"},
		logstorage.Field{Name: "_level", Value: "info"},
		logstorage.Field{Name: "service.name", Value: "api-gateway"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]
	if row.Body != "hello world" {
		t.Errorf("Body = %q, want %q", row.Body, "hello world")
	}
	if row.SeverityText != "info" {
		t.Errorf("SeverityText = %q, want %q", row.SeverityText, "info")
	}
	if row.ServiceName != "api-gateway" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "api-gateway")
	}
	if row.TimestampUnixNano != 1_000_000_000 {
		t.Errorf("Timestamp = %d, want %d", row.TimestampUnixNano, 1_000_000_000)
	}
}

func TestInsertAdapter_MustAddRows_AllPromotedFields(t *testing.T) {
	w := &mockLogWriter{}
	a := &insertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "_msg", Value: "test"},
		logstorage.Field{Name: "service.name", Value: "svc"},
		logstorage.Field{Name: "trace_id", Value: "abc123"},
		logstorage.Field{Name: "span_id", Value: "def456"},
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

	checks := []struct {
		name string
		got  string
		want string
	}{
		{"Body", row.Body, "test"},
		{"ServiceName", row.ServiceName, "svc"},
		{"TraceID", row.TraceID, "abc123"},
		{"SpanID", row.SpanID, "def456"},
		{"K8sNamespaceName", row.K8sNamespaceName, "prod"},
		{"K8sPodName", row.K8sPodName, "api-pod-1"},
		{"K8sDeploymentName", row.K8sDeploymentName, "api"},
		{"K8sNodeName", row.K8sNodeName, "node-1"},
		{"DeployEnv", row.DeployEnv, "production"},
		{"CloudRegion", row.CloudRegion, "us-east-1"},
		{"HostName", row.HostName, "host-1"},
		{"ScopeName", row.ScopeName, "mylib"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}
}

func TestInsertAdapter_MustAddRows_UnpromotedGoToAttributes(t *testing.T) {
	w := &mockLogWriter{}
	a := &insertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "_msg", Value: "test"},
		logstorage.Field{Name: "custom_field", Value: "custom_value"},
		logstorage.Field{Name: "another.field", Value: "another_value"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	if row.LogAttributes == nil {
		t.Fatal("LogAttributes should not be nil")
	}
	if row.LogAttributes["custom_field"] != "custom_value" {
		t.Errorf("custom_field = %q, want %q", row.LogAttributes["custom_field"], "custom_value")
	}
	if row.LogAttributes["another.field"] != "another_value" {
		t.Errorf("another.field = %q, want %q", row.LogAttributes["another.field"], "another_value")
	}
}

func TestInsertAdapter_MustAddRows_EmptyRows(t *testing.T) {
	w := &mockLogWriter{}
	a := &insertAdapter{writer: w}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 0 {
		t.Errorf("expected 0 rows for empty LogRows, got %d", len(w.rows))
	}
}

func TestInsertAdapter_MustAddRows_MultipleRows(t *testing.T) {
	w := &mockLogWriter{}
	a := &insertAdapter{writer: w}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	for i := 0; i < 100; i++ {
		lr.MustAdd(logstorage.TenantID{}, int64(i)*1_000_000_000,
			[]logstorage.Field{{Name: "_msg", Value: "msg"}}, -1)
	}
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 100 {
		t.Errorf("expected 100 rows, got %d", len(w.rows))
	}
}

func TestInsertAdapter_MustAddRows_StreamPreserved(t *testing.T) {
	w := &mockLogWriter{}
	a := &insertAdapter{writer: w}

	streamFields := []string{"service.name", "k8s.namespace.name"}
	lr := logstorage.GetLogRows(streamFields, nil, nil, nil, "")
	lr.MustAdd(logstorage.TenantID{}, 1_000_000_000, []logstorage.Field{
		{Name: "_msg", Value: "test"},
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
	a := &insertAdapter{writer: &mockLogWriter{}}
	if err := a.CanWriteData(); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestInsertAdapter_CanWriteData_Unhealthy(t *testing.T) {
	a := &insertAdapter{writer: &mockLogWriter{writeErr: errors.New("s3 unavailable")}}
	err := a.CanWriteData()
	if err == nil {
		t.Error("expected error, got nil")
	}
	if err.Error() != "s3 unavailable" {
		t.Errorf("error = %q, want %q", err.Error(), "s3 unavailable")
	}
}

func TestInsertAdapter_MustAddRows_NoMsgField(t *testing.T) {
	w := &mockLogWriter{}
	a := &insertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "service.name", Value: "api"},
		logstorage.Field{Name: "custom", Value: "value"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]
	if row.Body != "" {
		t.Errorf("Body should be empty when no _msg field, got %q", row.Body)
	}
}

func BenchmarkLogRowsToSchemaRows(b *testing.B) {
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	for i := 0; i < 1000; i++ {
		lr.MustAdd(logstorage.TenantID{}, int64(i)*1_000_000_000, []logstorage.Field{
			{Name: "_msg", Value: "benchmark log message"},
			{Name: "service.name", Value: "benchmark-svc"},
			{Name: "k8s.namespace.name", Value: "prod"},
			{Name: "custom_field_1", Value: "value1"},
			{Name: "custom_field_2", Value: "value2"},
		}, -1)
	}
	defer logstorage.PutLogRows(lr)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows := logRowsToSchemaRows(lr)
		if len(rows) != 1000 {
			b.Fatalf("expected 1000 rows, got %d", len(rows))
		}
	}
}
