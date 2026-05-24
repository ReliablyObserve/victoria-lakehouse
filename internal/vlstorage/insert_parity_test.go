package vlstorage

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestMapFieldToRow_PromotedFieldsNotDuplicated is a regression test that
// verifies promoted fields go ONLY to their promoted columns and NOT to
// ResourceAttributes MAP. A previous bug caused dual-storage where promoted
// fields were written to both the promoted column AND ResourceAttributes,
// inflating Parquet file sizes and causing field_names to return duplicates.
func TestMapFieldToRow_PromotedFieldsNotDuplicated(t *testing.T) {
	t.Parallel()

	promotedFields := []struct {
		name     string
		value    string
		promoted func(r schema.LogRow) string
	}{
		{"service.name", "api-gw", func(r schema.LogRow) string { return r.ServiceName }},
		{"k8s.namespace.name", "production", func(r schema.LogRow) string { return r.K8sNamespaceName }},
		{"k8s.pod.name", "api-pod-1", func(r schema.LogRow) string { return r.K8sPodName }},
		{"k8s.deployment.name", "api", func(r schema.LogRow) string { return r.K8sDeploymentName }},
		{"k8s.node.name", "node-1", func(r schema.LogRow) string { return r.K8sNodeName }},
		{"deployment.environment", "staging", func(r schema.LogRow) string { return r.DeployEnv }},
		{"cloud.region", "eu-west-1", func(r schema.LogRow) string { return r.CloudRegion }},
		{"host.name", "host-abc", func(r schema.LogRow) string { return r.HostName }},
		{"trace_id", "4bf92f3577b34da6a3ce929d0e0e4736", func(r schema.LogRow) string { return r.TraceID }},
		{"span_id", "00f067aa0ba902b7", func(r schema.LogRow) string { return r.SpanID }},
		{"scope.name", "github.com/my/lib", func(r schema.LogRow) string { return r.ScopeName }},
	}

	for _, tc := range promotedFields {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			row := schema.LogRow{}
			mapFieldToRow(&row, tc.name, tc.value)

			// Promoted column MUST have the value.
			if got := tc.promoted(row); got != tc.value {
				t.Errorf("promoted column %q = %q, want %q", tc.name, got, tc.value)
			}

			// ResourceAttributes MUST NOT have the value (regression guard).
			if row.ResourceAttributes != nil {
				if _, exists := row.ResourceAttributes[tc.name]; exists {
					t.Errorf("REGRESSION: promoted field %q was duplicated into ResourceAttributes", tc.name)
				}
			}

			// LogAttributes MUST NOT have the value either.
			if row.LogAttributes != nil {
				if _, exists := row.LogAttributes[tc.name]; exists {
					t.Errorf("REGRESSION: promoted field %q was duplicated into LogAttributes", tc.name)
				}
			}
		})
	}
}

// TestMapFieldToRow_UnknownFieldsGoToLogAttributes verifies that fields not in
// the promoted set land in LogAttributes MAP, never in promoted columns or
// ResourceAttributes.
func TestMapFieldToRow_UnknownFieldsGoToLogAttributes(t *testing.T) {
	t.Parallel()

	unknownFields := []struct {
		name  string
		value string
	}{
		{"cloud.provider", "aws"},
		{"container.id", "abc123def456"},
		{"telemetry.sdk.name", "opentelemetry"},
		{"telemetry.sdk.language", "go"},
		{"os.type", "linux"},
		{"process.pid", "12345"},
		{"http.method", "POST"},
		{"user.id", "u-42"},
		{"custom.business.metric", "revenue"},
	}

	row := schema.LogRow{}
	for _, f := range unknownFields {
		mapFieldToRow(&row, f.name, f.value)
	}

	if row.LogAttributes == nil {
		t.Fatal("LogAttributes must not be nil when unknown fields are present")
	}

	for _, f := range unknownFields {
		got, ok := row.LogAttributes[f.name]
		if !ok {
			t.Errorf("unknown field %q not found in LogAttributes", f.name)
			continue
		}
		if got != f.value {
			t.Errorf("LogAttributes[%q] = %q, want %q", f.name, got, f.value)
		}
	}

	// Verify ResourceAttributes was not populated.
	if len(row.ResourceAttributes) > 0 {
		t.Errorf("ResourceAttributes should be nil/empty for unknown fields, got %v", row.ResourceAttributes)
	}
}

// TestMapFieldToRow_OTELResourceAttributes tests a comprehensive set of OTEL
// standard resource attributes to verify each one gets placed correctly:
// promoted ones in promoted columns, non-promoted ones in LogAttributes MAP.
func TestMapFieldToRow_OTELResourceAttributes(t *testing.T) {
	t.Parallel()

	type attrTest struct {
		name      string
		value     string
		promoted  bool // true if this field has a promoted column
		checkFunc func(r schema.LogRow) string
	}

	attrs := []attrTest{
		// Promoted OTEL resource attributes
		{"service.name", "payment-service", true, func(r schema.LogRow) string { return r.ServiceName }},
		{"deployment.environment", "production", true, func(r schema.LogRow) string { return r.DeployEnv }},
		{"cloud.region", "us-east-1", true, func(r schema.LogRow) string { return r.CloudRegion }},
		{"host.name", "worker-3", true, func(r schema.LogRow) string { return r.HostName }},
		{"k8s.namespace.name", "prod", true, func(r schema.LogRow) string { return r.K8sNamespaceName }},
		{"k8s.pod.name", "payment-svc-abc", true, func(r schema.LogRow) string { return r.K8sPodName }},
		{"k8s.deployment.name", "payment-svc", true, func(r schema.LogRow) string { return r.K8sDeploymentName }},
		{"k8s.node.name", "ip-10-0-1-42", true, func(r schema.LogRow) string { return r.K8sNodeName }},

		// Non-promoted OTEL resource attributes (must go to LogAttributes)
		{"service.version", "1.2.3", false, nil},
		{"service.namespace", "payments", false, nil},
		{"telemetry.sdk.name", "opentelemetry", false, nil},
		{"telemetry.sdk.language", "go", false, nil},
		{"telemetry.sdk.version", "1.30.0", false, nil},
		{"cloud.provider", "aws", false, nil},
		{"cloud.account.id", "123456789012", false, nil},
		{"cloud.availability_zone", "us-east-1a", false, nil},
		{"container.id", "abc123", false, nil},
		{"container.name", "payment-container", false, nil},
		{"container.image.name", "payment:latest", false, nil},
		{"os.type", "linux", false, nil},
		{"os.description", "Ubuntu 22.04", false, nil},
		{"process.pid", "12345", false, nil},
		{"process.command", "/app/server", false, nil},
		{"host.arch", "amd64", false, nil},
		{"k8s.cluster.name", "prod-east", false, nil},
		{"k8s.container.name", "payment", false, nil},
	}

	row := schema.LogRow{}
	for _, a := range attrs {
		mapFieldToRow(&row, a.name, a.value)
	}

	for _, a := range attrs {
		if a.promoted {
			// Must be in promoted column.
			got := a.checkFunc(row)
			if got != a.value {
				t.Errorf("promoted field %q = %q, want %q", a.name, got, a.value)
			}
			// Must NOT be duplicated in LogAttributes.
			if row.LogAttributes != nil {
				if _, exists := row.LogAttributes[a.name]; exists {
					t.Errorf("REGRESSION: promoted OTEL field %q was duplicated into LogAttributes", a.name)
				}
			}
			// Must NOT be duplicated in ResourceAttributes.
			if row.ResourceAttributes != nil {
				if _, exists := row.ResourceAttributes[a.name]; exists {
					t.Errorf("REGRESSION: promoted OTEL field %q was duplicated into ResourceAttributes", a.name)
				}
			}
		} else {
			// Must be in LogAttributes.
			if row.LogAttributes == nil {
				t.Fatalf("LogAttributes must not be nil")
			}
			got, ok := row.LogAttributes[a.name]
			if !ok {
				t.Errorf("non-promoted OTEL field %q not found in LogAttributes", a.name)
				continue
			}
			if got != a.value {
				t.Errorf("LogAttributes[%q] = %q, want %q", a.name, got, a.value)
			}
		}
	}
}

// TestMapFieldToRow_NonOTELFields verifies that arbitrary non-OTEL fields
// (custom business fields) go to LogAttributes MAP and never to promoted
// columns or ResourceAttributes.
func TestMapFieldToRow_NonOTELFields(t *testing.T) {
	t.Parallel()

	customFields := map[string]string{
		"custom.field":                  "value1",
		"business.metric":               "revenue",
		"app.version":                   "2.1.0",
		"request.id":                    "req-abc-123",
		"correlation_id":                "corr-xyz-789",
		"user.email":                    "test@example.com",
		"org.id":                        "org-42",
		"feature.flag.enabled":          "true",
		"payment.amount":                "99.99",
		"order.status":                  "completed",
		"db.query.duration_ms":          "42",
		"cache.hit":                     "false",
		"with spaces":                   "should work",
		"UPPERCASE.FIELD":               "should work too",
		"deeply.nested.field.name.here": "deep value",
	}

	row := schema.LogRow{}
	for name, value := range customFields {
		mapFieldToRow(&row, name, value)
	}

	if row.LogAttributes == nil {
		t.Fatal("LogAttributes must not be nil when custom fields are present")
	}

	for name, want := range customFields {
		got, ok := row.LogAttributes[name]
		if !ok {
			t.Errorf("custom field %q not found in LogAttributes", name)
			continue
		}
		if got != want {
			t.Errorf("LogAttributes[%q] = %q, want %q", name, got, want)
		}
	}

	// Verify no promoted columns were accidentally populated.
	if row.ServiceName != "" {
		t.Errorf("ServiceName should be empty, got %q", row.ServiceName)
	}
	if row.K8sNamespaceName != "" {
		t.Errorf("K8sNamespaceName should be empty, got %q", row.K8sNamespaceName)
	}
	if row.HostName != "" {
		t.Errorf("HostName should be empty, got %q", row.HostName)
	}
	if row.CloudRegion != "" {
		t.Errorf("CloudRegion should be empty, got %q", row.CloudRegion)
	}

	// ResourceAttributes must not be populated.
	if len(row.ResourceAttributes) > 0 {
		t.Errorf("ResourceAttributes should be empty for custom fields, got %v", row.ResourceAttributes)
	}
}
