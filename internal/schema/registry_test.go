package schema

import "testing"

func TestLogsRegistry_PromotedLookup(t *testing.T) {
	r := NewRegistry(LogsProfile)

	tests := []struct {
		internal string
		wantCol  string
	}{
		{"_time", "timestamp_unix_nano"},
		{"_msg", "body"},
		{"level", "severity_text"},
		{"service.name", "service.name"},
		{"k8s.namespace.name", "k8s.namespace.name"},
		{"trace_id", "trace_id"},
		{"_stream", "_stream"},
		{"scope.name", "scope.name"},
	}
	for _, tt := range tests {
		m := r.ResolveToParquet(tt.internal)
		if m == nil {
			t.Errorf("ResolveToParquet(%q) = nil", tt.internal)
			continue
		}
		if m.ParquetColumn != tt.wantCol {
			t.Errorf("ResolveToParquet(%q).ParquetColumn = %q, want %q", tt.internal, m.ParquetColumn, tt.wantCol)
		}
		if m.Origin != OriginPromoted {
			t.Errorf("ResolveToParquet(%q).Origin = %d, want OriginPromoted", tt.internal, m.Origin)
		}
	}
}

func TestTracesRegistry_PromotedLookup(t *testing.T) {
	r := NewRegistry(TracesProfile)

	m := r.ResolveToParquet("resource_attr:service.name")
	if m == nil {
		t.Fatal("ResolveToParquet(resource_attr:service.name) = nil")
	}
	if m.ParquetColumn != "service.name" {
		t.Errorf("got column %q, want service.name", m.ParquetColumn)
	}

	m = r.ResolveToParquet("name")
	if m == nil {
		t.Fatal("ResolveToParquet(name) = nil")
	}
	if m.ParquetColumn != "span.name" {
		t.Errorf("got column %q, want span.name", m.ParquetColumn)
	}
}

func TestRegistry_MAPFallback(t *testing.T) {
	r := NewRegistry(LogsProfile)

	m := r.ResolveToParquet("resource_attr:custom.field")
	if m == nil {
		t.Fatal("resource_attr: prefix should resolve to MAP")
	}
	if m.Origin != OriginResourceMap {
		t.Errorf("Origin = %d, want OriginResourceMap", m.Origin)
	}
	if m.MapKey != "custom.field" {
		t.Errorf("MapKey = %q, want custom.field", m.MapKey)
	}

	m = r.ResolveToParquet("span_attr:http.method")
	if m == nil {
		t.Fatal("span_attr: prefix should resolve to MAP")
	}
	if m.Origin != OriginSpanAttrMap {
		t.Errorf("Origin = %d, want OriginSpanAttrMap", m.Origin)
	}
}

func TestRegistry_UnknownField_DottedConvention(t *testing.T) {
	r := NewRegistry(LogsProfile)

	m := r.ResolveToParquet("my.custom.field")
	if m == nil {
		t.Fatal("unknown dotted field should fall back to first MAP column")
	}
	if m.MapColumn != "resource.attributes" {
		t.Errorf("MapColumn = %q, want resource.attributes", m.MapColumn)
	}
	if m.MapKey != "my.custom.field" {
		t.Errorf("MapKey = %q, want my.custom.field", m.MapKey)
	}
}

func TestRegistry_ReverseResolve(t *testing.T) {
	r := NewRegistry(LogsProfile)

	m := r.ResolveFromParquet("body")
	if m == nil {
		t.Fatal("ResolveFromParquet(body) = nil")
	}
	if m.InternalName != "_msg" {
		t.Errorf("InternalName = %q, want _msg", m.InternalName)
	}

	m = r.ResolveFromParquet("nonexistent")
	if m != nil {
		t.Error("nonexistent column should return nil")
	}
}

func TestRegistry_TimestampColumn(t *testing.T) {
	r := NewRegistry(LogsProfile)
	if col := r.TimestampColumn(); col != "timestamp_unix_nano" {
		t.Errorf("TimestampColumn = %q, want timestamp_unix_nano", col)
	}
}

func TestRegistry_BloomColumns(t *testing.T) {
	r := NewRegistry(LogsProfile)
	blooms := 0
	for _, m := range r.PromotedColumns() {
		if m.HasBloom {
			blooms++
		}
	}
	if blooms != 2 {
		t.Errorf("expected 2 bloom columns (service.name, trace_id), got %d", blooms)
	}
}
