package vlstorage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestVerifyInsert_AllPromotedFields is a table-driven test that exercises
// every individually promoted field, verifying the mapping from VL field name
// to the correct schema.LogRow column.
func TestVerifyInsert_AllPromotedFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fieldName string
		value     string
		check     func(row schema.LogRow) string
		wantField string
	}{
		{
			fieldName: "",
			value:     "the log body",
			check:     func(r schema.LogRow) string { return r.Body },
			wantField: "Body",
		},
		{
			fieldName: "level",
			value:     "error",
			check:     func(r schema.LogRow) string { return r.SeverityText },
			wantField: "SeverityText",
		},
		{
			fieldName: "service.name",
			value:     "payment-service",
			check:     func(r schema.LogRow) string { return r.ServiceName },
			wantField: "ServiceName",
		},
		{
			fieldName: "trace_id",
			value:     "4bf92f3577b34da6a3ce929d0e0e4736",
			check:     func(r schema.LogRow) string { return r.TraceID },
			wantField: "TraceID",
		},
		{
			fieldName: "span_id",
			value:     "00f067aa0ba902b7",
			check:     func(r schema.LogRow) string { return r.SpanID },
			wantField: "SpanID",
		},
		{
			fieldName: "k8s.namespace.name",
			value:     "production",
			check:     func(r schema.LogRow) string { return r.K8sNamespaceName },
			wantField: "K8sNamespaceName",
		},
		{
			fieldName: "k8s.pod.name",
			value:     "payment-service-abc123",
			check:     func(r schema.LogRow) string { return r.K8sPodName },
			wantField: "K8sPodName",
		},
		{
			fieldName: "k8s.deployment.name",
			value:     "payment-service",
			check:     func(r schema.LogRow) string { return r.K8sDeploymentName },
			wantField: "K8sDeploymentName",
		},
		{
			fieldName: "k8s.node.name",
			value:     "ip-10-0-1-42.ec2.internal",
			check:     func(r schema.LogRow) string { return r.K8sNodeName },
			wantField: "K8sNodeName",
		},
		{
			fieldName: "deployment.environment",
			value:     "production",
			check:     func(r schema.LogRow) string { return r.DeployEnv },
			wantField: "DeployEnv",
		},
		{
			fieldName: "cloud.region",
			value:     "eu-west-1",
			check:     func(r schema.LogRow) string { return r.CloudRegion },
			wantField: "CloudRegion",
		},
		{
			fieldName: "host.name",
			value:     "worker-node-3",
			check:     func(r schema.LogRow) string { return r.HostName },
			wantField: "HostName",
		},
		{
			fieldName: "scope.name",
			value:     "github.com/my/lib",
			check:     func(r schema.LogRow) string { return r.ScopeName },
			wantField: "ScopeName",
		},
	}

	for _, tc := range cases {
		t.Run(tc.wantField, func(t *testing.T) {
			t.Parallel()

			row := schema.LogRow{}
			mapFieldToRow(&row, tc.fieldName, tc.value)

			got := tc.check(row)
			if got != tc.value {
				t.Errorf("%s: got %q, want %q", tc.wantField, got, tc.value)
			}
		})
	}
}

// TestVerifyInsert_UnknownFieldGoesToLogAttributes verifies that fields not in
// the promoted set land in LogAttributes, not in any typed column.
func TestVerifyInsert_UnknownFieldGoesToLogAttributes(t *testing.T) {
	t.Parallel()

	w := &mockLogWriter{}
	a := &insertAdapter{writer: w}

	lr := makeLogRows(t,
		logstorage.Field{Name: "http.method", Value: "POST"},
		logstorage.Field{Name: "user.id", Value: "u-42"},
		logstorage.Field{Name: "request.duration_ms", Value: "123"},
	)
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(w.rows))
	}
	row := w.rows[0]

	if row.LogAttributes == nil {
		t.Fatal("LogAttributes must not be nil for rows with unknown fields")
	}

	unknowns := map[string]string{
		"http.method":         "POST",
		"user.id":             "u-42",
		"request.duration_ms": "123",
	}
	for k, want := range unknowns {
		got, ok := row.LogAttributes[k]
		if !ok {
			t.Errorf("LogAttributes missing key %q", k)
			continue
		}
		if got != want {
			t.Errorf("LogAttributes[%q] = %q, want %q", k, got, want)
		}
	}

	// Verify none of the promoted typed columns were populated.
	if row.Body != "" {
		t.Errorf("Body should be empty, got %q", row.Body)
	}
	if row.SeverityText != "" {
		t.Errorf("SeverityText should be empty, got %q", row.SeverityText)
	}
	if row.ServiceName != "" {
		t.Errorf("ServiceName should be empty, got %q", row.ServiceName)
	}
}

// TestVerifyInsert_ResourceAttributeDualStorage verifies that each field with
// dual storage (promoted column + ResourceAttributes map) is written to both
// locations simultaneously.
func TestVerifyInsert_ResourceAttributeDualStorage(t *testing.T) {
	t.Parallel()

	dualFields := []struct {
		name     string
		value    string
		promoted func(r schema.LogRow) string
	}{
		{"service.name", "api-gw", func(r schema.LogRow) string { return r.ServiceName }},
		{"k8s.namespace.name", "staging", func(r schema.LogRow) string { return r.K8sNamespaceName }},
		{"k8s.pod.name", "api-pod-7", func(r schema.LogRow) string { return r.K8sPodName }},
		{"k8s.deployment.name", "api-dep", func(r schema.LogRow) string { return r.K8sDeploymentName }},
		{"k8s.node.name", "node-99", func(r schema.LogRow) string { return r.K8sNodeName }},
		{"deployment.environment", "staging", func(r schema.LogRow) string { return r.DeployEnv }},
		{"cloud.region", "ap-southeast-1", func(r schema.LogRow) string { return r.CloudRegion }},
		{"host.name", "host-abc", func(r schema.LogRow) string { return r.HostName }},
	}

	for _, tc := range dualFields {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			row := schema.LogRow{}
			mapFieldToRow(&row, tc.name, tc.value)

			// Check promoted column.
			if got := tc.promoted(row); got != tc.value {
				t.Errorf("promoted field %q = %q, want %q", tc.name, got, tc.value)
			}

			// Check ResourceAttributes map.
			if row.ResourceAttributes == nil {
				t.Fatalf("ResourceAttributes must not be nil for dual-storage field %q", tc.name)
			}
			if got := row.ResourceAttributes[tc.name]; got != tc.value {
				t.Errorf("ResourceAttributes[%q] = %q, want %q", tc.name, got, tc.value)
			}
		})
	}
}

// TestVerifyInsert_TimestampPreserved verifies that nanosecond timestamps are
// passed through the insert adapter without truncation or modification.
func TestVerifyInsert_TimestampPreserved(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ts   int64
	}{
		{"zero", 0},
		{"one_second", 1_000_000_000},
		{"nanosecond_precision", 1_716_220_800_123_456_789},
		{"large_value", 9_223_372_036_854_775_807}, // math.MaxInt64
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := &mockLogWriter{}
			a := &insertAdapter{writer: w}

			lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
			lr.MustAdd(logstorage.TenantID{}, tc.ts,
				[]logstorage.Field{{Name: "_msg", Value: "ts-test"}}, -1)
			defer logstorage.PutLogRows(lr)

			a.MustAddRows(lr)

			if len(w.rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(w.rows))
			}
			if got := w.rows[0].TimestampUnixNano; got != tc.ts {
				t.Errorf("TimestampUnixNano = %d, want %d", got, tc.ts)
			}
		})
	}
}

// TestVerifyInsert_EmptyRows verifies that an empty LogRows input produces no
// schema rows and does not call MustAddLogRows.
func TestVerifyInsert_EmptyRows(t *testing.T) {
	t.Parallel()

	w := &mockLogWriter{}
	a := &insertAdapter{writer: w}

	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)

	a.MustAddRows(lr)

	if len(w.rows) != 0 {
		t.Errorf("expected 0 rows for empty input, got %d", len(w.rows))
	}
}

// TestVerifyInsert_SeverityNumberParsing verifies correct int32 parsing for the
// severity_number field: valid decimal integers are stored; invalid or empty
// values leave SeverityNumber at zero.
func TestVerifyInsert_SeverityNumberParsing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		input   string
		wantNum int32
	}{
		{"valid_zero", "0", 0},
		{"valid_positive", "9", 9},
		{"valid_max_severity", "24", 24},
		{"valid_large", "2147483647", 2147483647},
		{"empty_string", "", 0},
		{"non_numeric", "critical", 0},
		{"float_string", "9.5", 0},
		{"negative", "-1", -1},
		{"overflow", "99999999999", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			row := schema.LogRow{}
			mapFieldToRow(&row, "severity_number", tc.input)

			if row.SeverityNumber != tc.wantNum {
				t.Errorf("SeverityNumber for input %q = %d, want %d",
					tc.input, row.SeverityNumber, tc.wantNum)
			}
		})
	}
}
