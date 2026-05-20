package schema

import (
	"testing"
)

// TestVerifySchema_LogsProfile_AllPromotedFields verifies all promoted fields
// exist in the logs profile with correct types and origins.
func TestVerifySchema_LogsProfile_AllPromotedFields(t *testing.T) {
	r := NewRegistry(LogsProfile)

	tests := []struct {
		internal   string
		parquet    string
		wantType   FieldType
		wantOrigin FieldOrigin
	}{
		{"_time", "timestamp_unix_nano", TypeTimestampNano, OriginPromoted},
		{"_msg", "body", TypeString, OriginPromoted},
		{"level", "severity_text", TypeString, OriginPromoted},
		{"severity_number", "severity_number", TypeInt32, OriginPromoted},
		{"service.name", "service.name", TypeString, OriginPromoted},
		{"k8s.namespace.name", "k8s.namespace.name", TypeString, OriginPromoted},
		{"k8s.pod.name", "k8s.pod.name", TypeString, OriginPromoted},
		{"trace_id", "trace_id", TypeString, OriginPromoted},
		{"span_id", "span_id", TypeString, OriginPromoted},
		{"k8s.deployment.name", "k8s.deployment.name", TypeString, OriginPromoted},
		{"k8s.node.name", "k8s.node.name", TypeString, OriginPromoted},
		{"deployment.environment", "deployment.environment", TypeString, OriginPromoted},
		{"cloud.region", "cloud.region", TypeString, OriginPromoted},
		{"host.name", "host.name", TypeString, OriginPromoted},
		{"_stream", "_stream", TypeString, OriginPromoted},
		{"_stream_id", "_stream_id", TypeString, OriginPromoted},
		{"scope.name", "scope.name", TypeString, OriginPromoted},
	}

	for _, tt := range tests {
		t.Run(tt.internal, func(t *testing.T) {
			m := r.ResolveToParquet(tt.internal)
			if m == nil {
				t.Fatalf("ResolveToParquet(%q) = nil, want field mapping", tt.internal)
			}
			if m.ParquetColumn != tt.parquet {
				t.Errorf("ParquetColumn = %q, want %q", m.ParquetColumn, tt.parquet)
			}
			if m.Type != tt.wantType {
				t.Errorf("Type = %d, want %d", m.Type, tt.wantType)
			}
			if m.Origin != tt.wantOrigin {
				t.Errorf("Origin = %d, want OriginPromoted (%d)", m.Origin, tt.wantOrigin)
			}
		})
	}
}

// TestVerifySchema_TracesProfile_AllPromotedFields verifies all promoted fields
// exist in the traces profile with correct types and origins.
func TestVerifySchema_TracesProfile_AllPromotedFields(t *testing.T) {
	r := NewRegistry(TracesProfile)

	tests := []struct {
		internal   string
		parquet    string
		wantType   FieldType
		wantOrigin FieldOrigin
	}{
		{"_time", "timestamp_unix_nano", TypeTimestampNano, OriginPromoted},
		{"start_time", "start_time_unix_nano", TypeTimestampNano, OriginPromoted},
		{"trace_id", "trace_id", TypeString, OriginPromoted},
		{"span_id", "span_id", TypeString, OriginPromoted},
		{"parent_span_id", "parent_span_id", TypeString, OriginPromoted},
		{"name", "span.name", TypeString, OriginPromoted},
		{"kind", "span.kind", TypeInt32, OriginPromoted},
		{"status_code", "status.code", TypeInt32, OriginPromoted},
		{"status_message", "status.message", TypeString, OriginPromoted},
		{"duration", "duration_ns", TypeInt64, OriginPromoted},
		{"resource_attr:service.name", "service.name", TypeString, OriginPromoted},
		{"scope_attr:otel.library.name", "scope.name", TypeString, OriginPromoted},
		{"_stream", "_stream", TypeString, OriginPromoted},
		{"_stream_id", "_stream_id", TypeString, OriginPromoted},
	}

	for _, tt := range tests {
		t.Run(tt.internal, func(t *testing.T) {
			m := r.ResolveToParquet(tt.internal)
			if m == nil {
				t.Fatalf("ResolveToParquet(%q) = nil, want field mapping", tt.internal)
			}
			if m.ParquetColumn != tt.parquet {
				t.Errorf("ParquetColumn = %q, want %q", m.ParquetColumn, tt.parquet)
			}
			if m.Type != tt.wantType {
				t.Errorf("Type = %d, want %d", m.Type, tt.wantType)
			}
			if m.Origin != tt.wantOrigin {
				t.Errorf("Origin = %d, want OriginPromoted (%d)", m.Origin, tt.wantOrigin)
			}
		})
	}
}

// TestVerifySchema_BloomEnabled_Logs verifies that high-cardinality fields
// have HasBloom=true in the logs profile.
func TestVerifySchema_BloomEnabled_Logs(t *testing.T) {
	r := NewRegistry(LogsProfile)

	bloomFields := []string{
		"service.name", "trace_id",
		"host.name", "k8s.namespace.name", "k8s.pod.name",
		"k8s.deployment.name", "deployment.environment",
	}
	for _, name := range bloomFields {
		t.Run(name, func(t *testing.T) {
			m := r.ResolveToParquet(name)
			if m == nil {
				t.Fatalf("ResolveToParquet(%q) = nil", name)
			}
			if !m.HasBloom {
				t.Errorf("field %q: HasBloom = false, want true", name)
			}
		})
	}

	// Verify non-bloom fields do NOT have bloom enabled.
	nonBloomFields := []string{"_msg", "level", "severity_number", "span_id"}
	for _, name := range nonBloomFields {
		t.Run("no_bloom_"+name, func(t *testing.T) {
			m := r.ResolveToParquet(name)
			if m == nil {
				t.Fatalf("ResolveToParquet(%q) = nil", name)
			}
			if m.HasBloom {
				t.Errorf("field %q: HasBloom = true, want false", name)
			}
		})
	}

	// Verify exact bloom count.
	bloomCount := 0
	for _, m := range r.PromotedColumns() {
		if m.HasBloom {
			bloomCount++
		}
	}
	if bloomCount != 7 {
		t.Errorf("logs profile: bloom column count = %d, want 7", bloomCount)
	}
}

// TestVerifySchema_BloomEnabled_Traces verifies that high-cardinality fields
// have HasBloom=true in the traces profile.
func TestVerifySchema_BloomEnabled_Traces(t *testing.T) {
	r := NewRegistry(TracesProfile)

	// trace_id is looked up by its internal name directly.
	m := r.ResolveToParquet("trace_id")
	if m == nil {
		t.Fatal("ResolveToParquet(trace_id) = nil")
	}
	if !m.HasBloom {
		t.Error("traces profile: trace_id HasBloom = false, want true")
	}

	// service.name in traces profile is stored under resource_attr:service.name internal name.
	m = r.ResolveToParquet("resource_attr:service.name")
	if m == nil {
		t.Fatal("ResolveToParquet(resource_attr:service.name) = nil")
	}
	if !m.HasBloom {
		t.Error("traces profile: service.name (resource_attr:service.name) HasBloom = false, want true")
	}

	// span.name is looked up by its parquet column name.
	m = r.ResolveFromParquet("span.name")
	if m == nil {
		t.Fatal("ResolveFromParquet(span.name) = nil")
	}
	if !m.HasBloom {
		t.Error("traces profile: span.name HasBloom = false, want true")
	}

	// Verify exact bloom count.
	bloomCount := 0
	for _, col := range r.PromotedColumns() {
		if col.HasBloom {
			bloomCount++
		}
	}
	if bloomCount != 3 {
		t.Errorf("traces profile: bloom column count = %d, want 3 (trace_id, service.name, span.name)", bloomCount)
	}
}

// TestVerifySchema_LogsProfile_MapColumns verifies resource.attributes and
// log.attributes are present as MAP columns in the logs profile.
func TestVerifySchema_LogsProfile_MapColumns(t *testing.T) {
	r := NewRegistry(LogsProfile)

	mc := r.MapColumns()
	if len(mc) != 2 {
		t.Fatalf("MapColumns() len = %d, want 2", len(mc))
	}

	expected := map[string]bool{
		"resource.attributes": false,
		"log.attributes":      false,
	}
	for _, col := range mc {
		if _, ok := expected[col]; !ok {
			t.Errorf("unexpected MAP column: %q", col)
		}
		expected[col] = true
	}
	for col, found := range expected {
		if !found {
			t.Errorf("MAP column %q missing from logs profile", col)
		}
	}
}

// TestVerifySchema_TracesProfile_MapColumns verifies resource.attributes,
// span.attributes, and scope.attributes are present as MAP columns in the traces profile.
func TestVerifySchema_TracesProfile_MapColumns(t *testing.T) {
	r := NewRegistry(TracesProfile)

	mc := r.MapColumns()
	if len(mc) != 3 {
		t.Fatalf("MapColumns() len = %d, want 3", len(mc))
	}

	expected := map[string]bool{
		"resource.attributes": false,
		"span.attributes":     false,
		"scope.attributes":    false,
	}
	for _, col := range mc {
		if _, ok := expected[col]; !ok {
			t.Errorf("unexpected MAP column: %q", col)
		}
		expected[col] = true
	}
	for col, found := range expected {
		if !found {
			t.Errorf("MAP column %q missing from traces profile", col)
		}
	}
}
