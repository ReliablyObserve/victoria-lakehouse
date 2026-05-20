package vlstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestVerifyTraceInsert_AllPromotedFields verifies that every promoted trace field
// is correctly mapped from a VL LogRow to the corresponding TraceRow column.
func TestVerifyTraceInsert_AllPromotedFields(t *testing.T) {
	tests := []struct {
		fieldName string
		value     string
		check     func(t *testing.T, row schema.TraceRow)
	}{
		{
			fieldName: "trace_id",
			value:     "tid-001",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.TraceID != "tid-001" {
					t.Errorf("TraceID = %q, want %q", row.TraceID, "tid-001")
				}
			},
		},
		{
			fieldName: "span_id",
			value:     "sid-001",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.SpanID != "sid-001" {
					t.Errorf("SpanID = %q, want %q", row.SpanID, "sid-001")
				}
			},
		},
		{
			fieldName: "parent_span_id",
			value:     "psid-001",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.ParentSpanID != "psid-001" {
					t.Errorf("ParentSpanID = %q, want %q", row.ParentSpanID, "psid-001")
				}
			},
		},
		{
			fieldName: "span.name",
			value:     "verify-op",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.SpanName != "verify-op" {
					t.Errorf("SpanName = %q, want %q", row.SpanName, "verify-op")
				}
			},
		},
		{
			fieldName: "service.name",
			value:     "verify-svc",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.ServiceName != "verify-svc" {
					t.Errorf("ServiceName = %q, want %q", row.ServiceName, "verify-svc")
				}
			},
		},
		{
			fieldName: "duration_ns",
			value:     "7654321",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.DurationNs != 7_654_321 {
					t.Errorf("DurationNs = %d, want %d", row.DurationNs, 7_654_321)
				}
			},
		},
		{
			fieldName: "status.code",
			value:     "2",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.StatusCode != 2 {
					t.Errorf("StatusCode = %d, want %d", row.StatusCode, 2)
				}
			},
		},
		{
			fieldName: "status.message",
			value:     "verify-msg",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.StatusMessage != "verify-msg" {
					t.Errorf("StatusMessage = %q, want %q", row.StatusMessage, "verify-msg")
				}
			},
		},
		{
			fieldName: "span.kind",
			value:     "4",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.SpanKind != 4 {
					t.Errorf("SpanKind = %d, want %d", row.SpanKind, 4)
				}
			},
		},
		{
			fieldName: "http.method",
			value:     "DELETE",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.HTTPMethod != "DELETE" {
					t.Errorf("HTTPMethod = %q, want %q", row.HTTPMethod, "DELETE")
				}
			},
		},
		{
			fieldName: "http.status_code",
			value:     "404",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.HTTPStatusCode != "404" {
					t.Errorf("HTTPStatusCode = %q, want %q", row.HTTPStatusCode, "404")
				}
			},
		},
		{
			fieldName: "http.url",
			value:     "https://verify.example.com/path",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.HTTPUrl != "https://verify.example.com/path" {
					t.Errorf("HTTPUrl = %q, want %q", row.HTTPUrl, "https://verify.example.com/path")
				}
			},
		},
		{
			fieldName: "db.system",
			value:     "sqlite",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.DBSystem != "sqlite" {
					t.Errorf("DBSystem = %q, want %q", row.DBSystem, "sqlite")
				}
			},
		},
		{
			fieldName: "db.statement",
			value:     "SELECT * FROM verify",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.DBStatement != "SELECT * FROM verify" {
					t.Errorf("DBStatement = %q, want %q", row.DBStatement, "SELECT * FROM verify")
				}
			},
		},
		{
			fieldName: "k8s.namespace.name",
			value:     "verify-ns",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.K8sNamespaceName != "verify-ns" {
					t.Errorf("K8sNamespaceName = %q, want %q", row.K8sNamespaceName, "verify-ns")
				}
			},
		},
		{
			fieldName: "k8s.pod.name",
			value:     "verify-pod-1",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.K8sPodName != "verify-pod-1" {
					t.Errorf("K8sPodName = %q, want %q", row.K8sPodName, "verify-pod-1")
				}
			},
		},
		{
			fieldName: "k8s.deployment.name",
			value:     "verify-deploy",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.K8sDeploymentName != "verify-deploy" {
					t.Errorf("K8sDeploymentName = %q, want %q", row.K8sDeploymentName, "verify-deploy")
				}
			},
		},
		{
			fieldName: "k8s.node.name",
			value:     "verify-node-1",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.K8sNodeName != "verify-node-1" {
					t.Errorf("K8sNodeName = %q, want %q", row.K8sNodeName, "verify-node-1")
				}
			},
		},
		{
			fieldName: "deployment.environment",
			value:     "verify-env",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.DeployEnv != "verify-env" {
					t.Errorf("DeployEnv = %q, want %q", row.DeployEnv, "verify-env")
				}
			},
		},
		{
			fieldName: "cloud.region",
			value:     "ap-southeast-2",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.CloudRegion != "ap-southeast-2" {
					t.Errorf("CloudRegion = %q, want %q", row.CloudRegion, "ap-southeast-2")
				}
			},
		},
		{
			fieldName: "host.name",
			value:     "verify-host-42",
			check: func(t *testing.T, row schema.TraceRow) {
				t.Helper()
				if row.HostName != "verify-host-42" {
					t.Errorf("HostName = %q, want %q", row.HostName, "verify-host-42")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.fieldName, func(t *testing.T) {
			w := &mockTraceWriter{}
			a := &vtInsertAdapter{writer: w}

			lr := makeLogRows(t, logstorage.Field{Name: tc.fieldName, Value: tc.value})
			defer logstorage.PutLogRows(lr)

			a.MustAddRows(lr)

			if len(w.rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(w.rows))
			}
			tc.check(t, w.rows[0])
		})
	}
}

// TestVerifyTraceInsert_StartTimePreserved verifies that start_time_unix_nano
// is mapped to StartTimeUnixNano on the TraceRow.
func TestVerifyTraceInsert_StartTimePreserved(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	const wantNano = int64(1_700_000_000_000_000_000)

	lr := makeLogRows(t, logstorage.Field{Name: "start_time_unix_nano", Value: "1700000000000000000"})
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	if w.rows[0].StartTimeUnixNano != wantNano {
		t.Errorf("StartTimeUnixNano = %d, want %d", w.rows[0].StartTimeUnixNano, wantNano)
	}
}

// TestVerifyTraceInsert_UnknownFieldGoesToSpanAttributes verifies that fields
// not matching any promoted column are stored in SpanAttributes.
func TestVerifyTraceInsert_UnknownFieldGoesToSpanAttributes(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "trace_id", Value: "t-custom"},
		logstorage.Field{Name: "custom.verify.field", Value: "custom-value"},
		logstorage.Field{Name: "arbitrary_key", Value: "arbitrary-value"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	if row.SpanAttributes == nil {
		t.Fatal("SpanAttributes should not be nil for unknown fields")
	}
	if got := row.SpanAttributes["custom.verify.field"]; got != "custom-value" {
		t.Errorf("SpanAttributes[custom.verify.field] = %q, want %q", got, "custom-value")
	}
	if got := row.SpanAttributes["arbitrary_key"]; got != "arbitrary-value" {
		t.Errorf("SpanAttributes[arbitrary_key] = %q, want %q", got, "arbitrary-value")
	}
}

// TestVerifyTraceInsert_EmptyRows verifies that zero input rows produce zero output.
func TestVerifyTraceInsert_EmptyRows(t *testing.T) {
	w := &mockTraceWriter{}
	a := &vtInsertAdapter{writer: w}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 0 {
		t.Errorf("expected 0 rows for empty input, got %d", len(w.rows))
	}
}
