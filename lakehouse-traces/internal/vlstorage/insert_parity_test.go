package vlstorage

import (
	"testing"

	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Regression tests for VT field name parity in the insert path.
// These tests verify that promoted resource/span attributes go ONLY
// to their promoted columns and are NOT duplicated in the MAP fields.
// They also verify that non-promoted attrs correctly land in the MAPs.

// --- Resource attribute tests ---

// TestTraceMapResourceAttr_PromotedFieldsNotInMAP verifies that all promoted
// resource attributes (service.name, k8s.pod.name, etc.) are routed to their
// dedicated TraceRow columns and do NOT leak into ResourceAttributes MAP.
// This was a root cause bug where promoted fields appeared in both places.
func TestTraceMapResourceAttr_PromotedFieldsNotInMAP(t *testing.T) {
	promotedResourceAttrs := map[string]struct {
		value   string
		checker func(row schema.TraceRow) string
	}{
		"service.name":           {"my-svc", func(r schema.TraceRow) string { return r.ServiceName }},
		"k8s.namespace.name":     {"production", func(r schema.TraceRow) string { return r.K8sNamespaceName }},
		"k8s.pod.name":           {"api-pod-1", func(r schema.TraceRow) string { return r.K8sPodName }},
		"k8s.deployment.name":    {"api-deploy", func(r schema.TraceRow) string { return r.K8sDeploymentName }},
		"k8s.node.name":          {"node-a-1", func(r schema.TraceRow) string { return r.K8sNodeName }},
		"deployment.environment": {"prod", func(r schema.TraceRow) string { return r.DeployEnv }},
		"cloud.region":           {"us-east-1", func(r schema.TraceRow) string { return r.CloudRegion }},
		"host.name":              {"ip-10-0-1-42", func(r schema.TraceRow) string { return r.HostName }},
	}

	for key, tc := range promotedResourceAttrs {
		t.Run(key, func(t *testing.T) {
			row := schema.TraceRow{}
			mapResourceAttr(&row, key, tc.value)

			// Promoted column MUST have the value.
			if got := tc.checker(row); got != tc.value {
				t.Errorf("promoted column for %q = %q, want %q", key, got, tc.value)
			}

			// ResourceAttributes MAP MUST NOT have the value.
			if row.ResourceAttributes != nil {
				if v, ok := row.ResourceAttributes[key]; ok {
					t.Errorf("promoted key %q leaked into ResourceAttributes MAP with value %q", key, v)
				}
			}
		})
	}

	// Also test that when ALL promoted keys are set at once, none appear in MAP.
	t.Run("all_promoted_at_once", func(t *testing.T) {
		row := schema.TraceRow{}
		for key, tc := range promotedResourceAttrs {
			mapResourceAttr(&row, key, tc.value)
		}

		if len(row.ResourceAttributes) > 0 {
			t.Errorf("ResourceAttributes MAP should be empty when only promoted keys are set, got %v", row.ResourceAttributes)
		}
	})
}

// TestTraceMapResourceAttr_NonPromotedGoToMAP verifies that resource attributes
// NOT in the promoted set are correctly placed in the ResourceAttributes MAP.
func TestTraceMapResourceAttr_NonPromotedGoToMAP(t *testing.T) {
	nonPromotedKeys := []struct {
		key   string
		value string
	}{
		{"cloud.provider", "aws"},
		{"container.id", "abc123def"},
		{"os.type", "linux"},
		{"process.runtime.name", "go"},
		{"telemetry.sdk.name", "opentelemetry"},
		{"custom.resource.tag", "custom-value"},
	}

	for _, tc := range nonPromotedKeys {
		t.Run(tc.key, func(t *testing.T) {
			row := schema.TraceRow{}
			mapResourceAttr(&row, tc.key, tc.value)

			if row.ResourceAttributes == nil {
				t.Fatalf("ResourceAttributes MAP should not be nil for non-promoted key %q", tc.key)
			}
			if got := row.ResourceAttributes[tc.key]; got != tc.value {
				t.Errorf("ResourceAttributes[%q] = %q, want %q", tc.key, got, tc.value)
			}
		})
	}
}

// TestTraceMapResourceAttr_MixedPromotedAndNonPromoted verifies that when both
// promoted and non-promoted resource attrs arrive together, promoted go to
// columns and non-promoted go to MAP without cross-contamination.
func TestTraceMapResourceAttr_MixedPromotedAndNonPromoted(t *testing.T) {
	row := schema.TraceRow{}

	// Promoted
	mapResourceAttr(&row, "service.name", "my-svc")
	mapResourceAttr(&row, "k8s.namespace.name", "prod")

	// Non-promoted
	mapResourceAttr(&row, "cloud.provider", "aws")
	mapResourceAttr(&row, "container.id", "ctr-123")

	if row.ServiceName != "my-svc" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "my-svc")
	}
	if row.K8sNamespaceName != "prod" {
		t.Errorf("K8sNamespaceName = %q, want %q", row.K8sNamespaceName, "prod")
	}

	if row.ResourceAttributes == nil {
		t.Fatal("ResourceAttributes MAP should not be nil")
	}
	if len(row.ResourceAttributes) != 2 {
		t.Errorf("ResourceAttributes MAP should have exactly 2 entries, got %d: %v",
			len(row.ResourceAttributes), row.ResourceAttributes)
	}
	if row.ResourceAttributes["cloud.provider"] != "aws" {
		t.Errorf("ResourceAttributes[cloud.provider] = %q, want %q",
			row.ResourceAttributes["cloud.provider"], "aws")
	}
	if row.ResourceAttributes["container.id"] != "ctr-123" {
		t.Errorf("ResourceAttributes[container.id] = %q, want %q",
			row.ResourceAttributes["container.id"], "ctr-123")
	}

	// Promoted keys MUST NOT appear in MAP.
	for _, key := range []string{"service.name", "k8s.namespace.name"} {
		if _, ok := row.ResourceAttributes[key]; ok {
			t.Errorf("promoted key %q leaked into ResourceAttributes MAP", key)
		}
	}
}

// --- Span attribute tests ---

// TestTraceMapSpanAttr_PromotedFieldsNotInMAP verifies that all promoted
// span attributes (http.method, db.system, etc.) are routed to their
// dedicated TraceRow columns and do NOT leak into SpanAttributes MAP.
func TestTraceMapSpanAttr_PromotedFieldsNotInMAP(t *testing.T) {
	promotedSpanAttrs := map[string]struct {
		value   string
		checker func(row schema.TraceRow) string
	}{
		"http.method":      {"GET", func(r schema.TraceRow) string { return r.HTTPMethod }},
		"http.status_code": {"200", func(r schema.TraceRow) string { return r.HTTPStatusCode }},
		"http.url":         {"https://example.com", func(r schema.TraceRow) string { return r.HTTPUrl }},
		"db.system":        {"postgresql", func(r schema.TraceRow) string { return r.DBSystem }},
		"db.statement":     {"SELECT 1", func(r schema.TraceRow) string { return r.DBStatement }},
	}

	for key, tc := range promotedSpanAttrs {
		t.Run(key, func(t *testing.T) {
			row := schema.TraceRow{}
			mapSpanAttr(&row, key, tc.value)

			// Promoted column MUST have the value.
			if got := tc.checker(row); got != tc.value {
				t.Errorf("promoted column for %q = %q, want %q", key, got, tc.value)
			}

			// SpanAttributes MAP MUST NOT have the value.
			if row.SpanAttributes != nil {
				if v, ok := row.SpanAttributes[key]; ok {
					t.Errorf("promoted key %q leaked into SpanAttributes MAP with value %q", key, v)
				}
			}
		})
	}

	// All promoted at once.
	t.Run("all_promoted_at_once", func(t *testing.T) {
		row := schema.TraceRow{}
		for key, tc := range promotedSpanAttrs {
			mapSpanAttr(&row, key, tc.value)
		}

		if len(row.SpanAttributes) > 0 {
			t.Errorf("SpanAttributes MAP should be empty when only promoted keys are set, got %v", row.SpanAttributes)
		}
	})
}

// TestTraceMapSpanAttr_NonPromotedGoToMAP verifies that span attributes
// NOT in the promoted set are correctly placed in the SpanAttributes MAP.
func TestTraceMapSpanAttr_NonPromotedGoToMAP(t *testing.T) {
	nonPromotedKeys := []struct {
		key   string
		value string
	}{
		{"rpc.system", "grpc"},
		{"rpc.method", "Process"},
		{"messaging.system", "kafka"},
		{"net.peer.name", "db-host-1"},
		{"custom.span.tag", "custom-value"},
		{"enduser.id", "user-42"},
	}

	for _, tc := range nonPromotedKeys {
		t.Run(tc.key, func(t *testing.T) {
			row := schema.TraceRow{}
			mapSpanAttr(&row, tc.key, tc.value)

			if row.SpanAttributes == nil {
				t.Fatalf("SpanAttributes MAP should not be nil for non-promoted key %q", tc.key)
			}
			if got := row.SpanAttributes[tc.key]; got != tc.value {
				t.Errorf("SpanAttributes[%q] = %q, want %q", tc.key, got, tc.value)
			}
		})
	}
}

// --- Full OTEL span attribute coverage ---

// TestTraceMapFieldToRow_OTELSpanAttributes tests the full OTLP insert path
// with prefixed field names from VT's OTLP parser, ensuring each field is
// correctly routed to TraceRow columns via mapFieldToTraceRow.
func TestTraceMapFieldToRow_OTELSpanAttributes(t *testing.T) {
	row := schema.TraceRow{}

	// Core span fields via VT OTLP constants
	mapFieldToTraceRow(&row, otelpb.TraceIDField, "abc123")
	mapFieldToTraceRow(&row, otelpb.SpanIDField, "span456")
	mapFieldToTraceRow(&row, otelpb.ParentSpanIDField, "parent789")
	mapFieldToTraceRow(&row, otelpb.NameField, "HTTP GET /api")
	mapFieldToTraceRow(&row, otelpb.KindField, "2")
	mapFieldToTraceRow(&row, otelpb.DurationField, "15000000")
	mapFieldToTraceRow(&row, otelpb.StartTimeUnixNanoField, "1700000000000000000")
	mapFieldToTraceRow(&row, otelpb.StatusCodeField, "1")
	mapFieldToTraceRow(&row, otelpb.StatusMessageField, "OK")
	mapFieldToTraceRow(&row, otelpb.InstrumentationScopeName, "my-scope")

	// Resource attrs (prefixed)
	mapFieldToTraceRow(&row, otelpb.ResourceAttrPrefix+"service.name", "payment-svc")
	mapFieldToTraceRow(&row, otelpb.ResourceAttrPrefix+"k8s.namespace.name", "production")
	mapFieldToTraceRow(&row, otelpb.ResourceAttrPrefix+"k8s.pod.name", "payment-abc")
	mapFieldToTraceRow(&row, otelpb.ResourceAttrPrefix+"k8s.deployment.name", "payment")
	mapFieldToTraceRow(&row, otelpb.ResourceAttrPrefix+"k8s.node.name", "node-1")
	mapFieldToTraceRow(&row, otelpb.ResourceAttrPrefix+"deployment.environment", "prod")
	mapFieldToTraceRow(&row, otelpb.ResourceAttrPrefix+"cloud.region", "us-east-1")
	mapFieldToTraceRow(&row, otelpb.ResourceAttrPrefix+"host.name", "ip-10-0-1-1")
	mapFieldToTraceRow(&row, otelpb.ResourceAttrPrefix+"cloud.provider", "aws")

	// Span attrs (prefixed)
	mapFieldToTraceRow(&row, otelpb.SpanAttrPrefixField+"http.method", "POST")
	mapFieldToTraceRow(&row, otelpb.SpanAttrPrefixField+"http.status_code", "201")
	mapFieldToTraceRow(&row, otelpb.SpanAttrPrefixField+"http.url", "https://api.example.com")
	mapFieldToTraceRow(&row, otelpb.SpanAttrPrefixField+"db.system", "redis")
	mapFieldToTraceRow(&row, otelpb.SpanAttrPrefixField+"db.statement", "GET key")
	mapFieldToTraceRow(&row, otelpb.SpanAttrPrefixField+"rpc.system", "grpc")

	// Verify core fields
	if row.TraceID != "abc123" {
		t.Errorf("TraceID = %q, want %q", row.TraceID, "abc123")
	}
	if row.SpanID != "span456" {
		t.Errorf("SpanID = %q, want %q", row.SpanID, "span456")
	}
	if row.ParentSpanID != "parent789" {
		t.Errorf("ParentSpanID = %q, want %q", row.ParentSpanID, "parent789")
	}
	if row.SpanName != "HTTP GET /api" {
		t.Errorf("SpanName = %q, want %q", row.SpanName, "HTTP GET /api")
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
	if row.StatusMessage != "OK" {
		t.Errorf("StatusMessage = %q, want %q", row.StatusMessage, "OK")
	}
	if row.ScopeName != "my-scope" {
		t.Errorf("ScopeName = %q, want %q", row.ScopeName, "my-scope")
	}

	// Verify promoted resource attrs in columns, NOT in MAP
	resourceChecks := map[string]struct {
		got  string
		want string
	}{
		"ServiceName":       {row.ServiceName, "payment-svc"},
		"K8sNamespaceName":  {row.K8sNamespaceName, "production"},
		"K8sPodName":        {row.K8sPodName, "payment-abc"},
		"K8sDeploymentName": {row.K8sDeploymentName, "payment"},
		"K8sNodeName":       {row.K8sNodeName, "node-1"},
		"DeployEnv":         {row.DeployEnv, "prod"},
		"CloudRegion":       {row.CloudRegion, "us-east-1"},
		"HostName":          {row.HostName, "ip-10-0-1-1"},
	}
	for name, c := range resourceChecks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", name, c.got, c.want)
		}
	}

	// Non-promoted resource attr MUST be in MAP
	if row.ResourceAttributes == nil {
		t.Fatal("ResourceAttributes MAP should not be nil")
	}
	if row.ResourceAttributes["cloud.provider"] != "aws" {
		t.Errorf("ResourceAttributes[cloud.provider] = %q, want %q",
			row.ResourceAttributes["cloud.provider"], "aws")
	}
	// Only 1 entry in ResourceAttributes (cloud.provider)
	if len(row.ResourceAttributes) != 1 {
		t.Errorf("ResourceAttributes should have 1 entry (cloud.provider), got %d: %v",
			len(row.ResourceAttributes), row.ResourceAttributes)
	}

	// Verify promoted span attrs in columns, NOT in MAP
	if row.HTTPMethod != "POST" {
		t.Errorf("HTTPMethod = %q, want %q", row.HTTPMethod, "POST")
	}
	if row.HTTPStatusCode != "201" {
		t.Errorf("HTTPStatusCode = %q, want %q", row.HTTPStatusCode, "201")
	}
	if row.HTTPUrl != "https://api.example.com" {
		t.Errorf("HTTPUrl = %q, want %q", row.HTTPUrl, "https://api.example.com")
	}
	if row.DBSystem != "redis" {
		t.Errorf("DBSystem = %q, want %q", row.DBSystem, "redis")
	}
	if row.DBStatement != "GET key" {
		t.Errorf("DBStatement = %q, want %q", row.DBStatement, "GET key")
	}

	// Non-promoted span attr MUST be in MAP
	if row.SpanAttributes == nil {
		t.Fatal("SpanAttributes MAP should not be nil")
	}
	if row.SpanAttributes["rpc.system"] != "grpc" {
		t.Errorf("SpanAttributes[rpc.system] = %q, want %q",
			row.SpanAttributes["rpc.system"], "grpc")
	}
	// Only 1 entry in SpanAttributes (rpc.system)
	if len(row.SpanAttributes) != 1 {
		t.Errorf("SpanAttributes should have 1 entry (rpc.system), got %d: %v",
			len(row.SpanAttributes), row.SpanAttributes)
	}
}

// TestTraceMapFieldToRow_IgnoredOTELFields ensures VT OTLP fields that are
// intentionally dropped (end_time, flags, dropped counts, scope version)
// do not pollute any TraceRow column or MAP.
func TestTraceMapFieldToRow_IgnoredOTELFields(t *testing.T) {
	row := schema.TraceRow{}

	ignoredFields := []struct {
		name  string
		value string
	}{
		{otelpb.EndTimeUnixNanoField, "9999"},
		{otelpb.InstrumentationScopeVersion, "1.0"},
		{otelpb.TraceStateField, "congo=t"},
		{otelpb.FlagsField, "1"},
		{otelpb.DroppedAttributesCountField, "0"},
		{otelpb.DroppedEventsCountField, "0"},
		{otelpb.DroppedLinksCountField, "0"},
		{otelpb.InstrumentationScopeAttrPrefix + "lib.version", "2.0"},
		{otelpb.EventPrefix + "0.name", "exception"},
		{otelpb.LinkPrefix + "0.trace_id", "linked-trace"},
		{"_msg", "-"},
	}

	for _, f := range ignoredFields {
		mapFieldToTraceRow(&row, f.name, f.value)
	}

	if len(row.SpanAttributes) != 0 {
		t.Errorf("SpanAttributes should be empty for ignored fields, got %v", row.SpanAttributes)
	}
	if len(row.ResourceAttributes) != 0 {
		t.Errorf("ResourceAttributes should be empty for ignored fields, got %v", row.ResourceAttributes)
	}
	if row.SpanName != "" {
		t.Errorf("SpanName should be empty, got %q", row.SpanName)
	}
}

// TestTraceMapFieldToRow_LegacyFlatNames verifies that legacy flat field names
// (from jsonline insert path) still work correctly. This is the fallback path.
func TestTraceMapFieldToRow_LegacyFlatNames(t *testing.T) {
	row := schema.TraceRow{}

	mapFieldToTraceRow(&row, "service.name", "legacy-svc")
	mapFieldToTraceRow(&row, "http.method", "DELETE")
	mapFieldToTraceRow(&row, "db.system", "mysql")
	mapFieldToTraceRow(&row, "k8s.namespace.name", "legacy-ns")
	mapFieldToTraceRow(&row, "custom.flat.field", "custom-val")

	if row.ServiceName != "legacy-svc" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "legacy-svc")
	}
	if row.HTTPMethod != "DELETE" {
		t.Errorf("HTTPMethod = %q, want %q", row.HTTPMethod, "DELETE")
	}
	if row.DBSystem != "mysql" {
		t.Errorf("DBSystem = %q, want %q", row.DBSystem, "mysql")
	}
	if row.K8sNamespaceName != "legacy-ns" {
		t.Errorf("K8sNamespaceName = %q, want %q", row.K8sNamespaceName, "legacy-ns")
	}
	// Non-promoted flat field goes to SpanAttributes (default)
	if row.SpanAttributes == nil || row.SpanAttributes["custom.flat.field"] != "custom-val" {
		t.Errorf("SpanAttributes[custom.flat.field] = %q, want %q",
			row.SpanAttributes["custom.flat.field"], "custom-val")
	}
}
