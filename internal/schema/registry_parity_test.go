package schema

import (
	"strings"
	"testing"
)

// TestLogsProfile_AllFieldsFlat is a regression test that verifies ALL
// LogsProfile promoted fields have InternalName == ParquetColumn (flat names,
// no prefix). VictoriaLogs uses flat field names for logs: "service.name",
// NOT "resource_attr:service.name". If anyone accidentally adds a prefix
// to a logs field, this test will catch it.
func TestLogsProfile_AllFieldsFlat(t *testing.T) {
	t.Parallel()

	// Fields that have a deliberate mapping (InternalName != ParquetColumn)
	// because VL uses different internal names than the Parquet column names.
	allowedRenames := map[string]string{
		"timestamp_unix_nano": "_time",
		"body":                "_msg",
		"severity_text":       "level",
	}

	for _, m := range LogsProfile.Promoted {
		// Skip known renamed fields.
		if expected, ok := allowedRenames[m.ParquetColumn]; ok {
			if m.InternalName != expected {
				t.Errorf("field %q: InternalName = %q, want %q (known rename)",
					m.ParquetColumn, m.InternalName, expected)
			}
			continue
		}

		// All other fields MUST have InternalName == ParquetColumn (flat, no prefix).
		if m.InternalName != m.ParquetColumn {
			t.Errorf("REGRESSION: logs field %q has InternalName %q (expected flat name matching ParquetColumn)",
				m.ParquetColumn, m.InternalName)
		}

		// Must NOT contain any prefix pattern.
		for _, prefix := range []string{"resource_attr:", "log_attr:", "span_attr:", "scope_attr:"} {
			if strings.HasPrefix(m.InternalName, prefix) {
				t.Errorf("REGRESSION: logs field %q has prefixed InternalName %q (logs must use flat names)",
					m.ParquetColumn, m.InternalName)
			}
		}
	}
}

// TestTracesProfile_ResourceFieldsPrefixed is a regression test that verifies
// TracesProfile resource-origin fields have "resource_attr:" prefix in their
// InternalName. VictoriaTraces uses prefixed field names to distinguish
// resource attributes from span attributes.
func TestTracesProfile_ResourceFieldsPrefixed(t *testing.T) {
	t.Parallel()

	// Resource attribute fields that must have resource_attr: prefix in traces.
	resourceFields := map[string]string{
		"service.name":          "resource_attr:service.name",
		"deployment.environment": "resource_attr:deployment.environment",
		"cloud.region":          "resource_attr:cloud.region",
		"host.name":             "resource_attr:host.name",
		"k8s.namespace.name":    "resource_attr:k8s.namespace.name",
		"k8s.deployment.name":   "resource_attr:k8s.deployment.name",
		"k8s.node.name":         "resource_attr:k8s.node.name",
	}

	for _, m := range TracesProfile.Promoted {
		expected, isResourceField := resourceFields[m.ParquetColumn]
		if !isResourceField {
			continue
		}

		if m.InternalName != expected {
			t.Errorf("traces resource field %q: InternalName = %q, want %q",
				m.ParquetColumn, m.InternalName, expected)
		}

		if !strings.HasPrefix(m.InternalName, "resource_attr:") {
			t.Errorf("REGRESSION: traces resource field %q missing resource_attr: prefix, got %q",
				m.ParquetColumn, m.InternalName)
		}
	}
}

// TestTracesProfile_SpanFieldsPrefixed is a regression test that verifies
// TracesProfile span-origin fields have "span_attr:" prefix in their
// InternalName.
func TestTracesProfile_SpanFieldsPrefixed(t *testing.T) {
	t.Parallel()

	// Span attribute fields that must have span_attr: prefix in traces.
	spanFields := map[string]string{
		"http.method":      "span_attr:http.method",
		"http.status_code": "span_attr:http.status_code",
		"http.url":         "span_attr:http.url",
		"db.system":        "span_attr:db.system",
		"db.statement":     "span_attr:db.statement",
	}

	for _, m := range TracesProfile.Promoted {
		expected, isSpanField := spanFields[m.ParquetColumn]
		if !isSpanField {
			continue
		}

		if m.InternalName != expected {
			t.Errorf("traces span field %q: InternalName = %q, want %q",
				m.ParquetColumn, m.InternalName, expected)
		}

		if !strings.HasPrefix(m.InternalName, "span_attr:") {
			t.Errorf("REGRESSION: traces span field %q missing span_attr: prefix, got %q",
				m.ParquetColumn, m.InternalName)
		}
	}
}

// TestLogsProfile_OTELSemanticCoverage verifies LogsProfile covers the core
// OTEL log resource and log attributes that VictoriaLogs expects. If a field
// is removed from LogsProfile, this test will catch it.
func TestLogsProfile_OTELSemanticCoverage(t *testing.T) {
	t.Parallel()

	r := NewRegistry(LogsProfile)

	// Core OTEL fields that must be promoted in the logs profile.
	requiredFields := []struct {
		internalName  string
		parquetColumn string
	}{
		{"service.name", "service.name"},
		{"deployment.environment", "deployment.environment"},
		{"cloud.region", "cloud.region"},
		{"host.name", "host.name"},
		{"k8s.namespace.name", "k8s.namespace.name"},
		{"k8s.pod.name", "k8s.pod.name"},
		{"k8s.deployment.name", "k8s.deployment.name"},
		{"k8s.node.name", "k8s.node.name"},
		{"trace_id", "trace_id"},
		{"span_id", "span_id"},
	}

	for _, rf := range requiredFields {
		m := r.ResolveToParquet(rf.internalName)
		if m == nil {
			t.Errorf("required OTEL field %q not found in LogsProfile", rf.internalName)
			continue
		}
		if m.ParquetColumn != rf.parquetColumn {
			t.Errorf("field %q: ParquetColumn = %q, want %q",
				rf.internalName, m.ParquetColumn, rf.parquetColumn)
		}
		if m.Origin != OriginPromoted {
			t.Errorf("field %q: Origin = %d, want OriginPromoted", rf.internalName, m.Origin)
		}

		// Verify flat naming (no prefix) for logs.
		if strings.Contains(m.InternalName, ":") {
			t.Errorf("REGRESSION: logs field %q has prefixed InternalName %q",
				rf.internalName, m.InternalName)
		}
	}

	// Verify core VL-specific fields are present.
	vlFields := []struct {
		internalName  string
		parquetColumn string
	}{
		{"_time", "timestamp_unix_nano"},
		{"_msg", "body"},
		{"level", "severity_text"},
		{"_stream", "_stream"},
		{"_stream_id", "_stream_id"},
	}

	for _, vf := range vlFields {
		m := r.ResolveToParquet(vf.internalName)
		if m == nil {
			t.Errorf("required VL field %q not found in LogsProfile", vf.internalName)
			continue
		}
		if m.ParquetColumn != vf.parquetColumn {
			t.Errorf("VL field %q: ParquetColumn = %q, want %q",
				vf.internalName, m.ParquetColumn, vf.parquetColumn)
		}
	}
}

// TestLogsVsTraces_SameFieldDifferentNames is a regression test that verifies
// the core difference between logs and traces profiles: the same Parquet
// column name (e.g., "service.name") maps to a FLAT InternalName in logs
// but a PREFIXED InternalName in traces.
func TestLogsVsTraces_SameFieldDifferentNames(t *testing.T) {
	t.Parallel()

	logsReg := NewRegistry(LogsProfile)
	tracesReg := NewRegistry(TracesProfile)

	// Fields that exist in both profiles with different internal names.
	sharedFields := []struct {
		parquetColumn     string
		wantLogsInternal  string
		wantTracesInternal string
	}{
		{"service.name", "service.name", "resource_attr:service.name"},
		{"deployment.environment", "deployment.environment", "resource_attr:deployment.environment"},
		{"cloud.region", "cloud.region", "resource_attr:cloud.region"},
		{"host.name", "host.name", "resource_attr:host.name"},
		{"k8s.namespace.name", "k8s.namespace.name", "resource_attr:k8s.namespace.name"},
		{"k8s.deployment.name", "k8s.deployment.name", "resource_attr:k8s.deployment.name"},
		{"k8s.node.name", "k8s.node.name", "resource_attr:k8s.node.name"},
	}

	for _, sf := range sharedFields {
		logsM := logsReg.ResolveFromParquet(sf.parquetColumn)
		if logsM == nil {
			t.Errorf("logs: ResolveFromParquet(%q) = nil", sf.parquetColumn)
			continue
		}
		if logsM.InternalName != sf.wantLogsInternal {
			t.Errorf("logs %q: InternalName = %q, want %q",
				sf.parquetColumn, logsM.InternalName, sf.wantLogsInternal)
		}

		tracesM := tracesReg.ResolveFromParquet(sf.parquetColumn)
		if tracesM == nil {
			t.Errorf("traces: ResolveFromParquet(%q) = nil", sf.parquetColumn)
			continue
		}
		if tracesM.InternalName != sf.wantTracesInternal {
			t.Errorf("traces %q: InternalName = %q, want %q",
				sf.parquetColumn, tracesM.InternalName, sf.wantTracesInternal)
		}
	}
}
