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

func TestRegistry_StreamFields_Logs(t *testing.T) {
	r := NewRegistry(LogsProfile)
	fields := r.StreamFields()
	if len(fields) != 3 {
		t.Errorf("expected 3 log stream fields, got %d", len(fields))
	}

	expected := map[string]bool{
		"service.name":       true,
		"k8s.namespace.name": true,
		"k8s.pod.name":       true,
	}
	for _, f := range fields {
		if !expected[f] {
			t.Errorf("unexpected stream field: %q", f)
		}
	}
}

func TestRegistry_StreamFields_Traces(t *testing.T) {
	r := NewRegistry(TracesProfile)
	fields := r.StreamFields()
	if len(fields) != 2 {
		t.Errorf("expected 2 trace stream fields, got %d", len(fields))
	}

	expected := map[string]bool{
		"resource_attr:service.name": true,
		"name":                       true,
	}
	for _, f := range fields {
		if !expected[f] {
			t.Errorf("unexpected stream field: %q", f)
		}
	}
}

func TestRegistry_MapColumns_Logs(t *testing.T) {
	r := NewRegistry(LogsProfile)
	mc := r.MapColumns()
	if len(mc) != 2 {
		t.Errorf("expected 2 map columns for logs, got %d", len(mc))
	}
}

func TestRegistry_MapColumns_Traces(t *testing.T) {
	r := NewRegistry(TracesProfile)
	mc := r.MapColumns()
	if len(mc) != 3 {
		t.Errorf("expected 3 map columns for traces, got %d", len(mc))
	}
}

func TestRegistry_LogAttrPrefix(t *testing.T) {
	r := NewRegistry(LogsProfile)
	m := r.ResolveToParquet("log_attr:custom.field")
	if m == nil {
		t.Fatal("log_attr: prefix should resolve to MAP")
	}
	if m.Origin != OriginLogAttrMap {
		t.Errorf("Origin = %d, want OriginLogAttrMap", m.Origin)
	}
	if m.MapKey != "custom.field" {
		t.Errorf("MapKey = %q, want custom.field", m.MapKey)
	}
}

func TestRegistry_ScopeAttrPrefix(t *testing.T) {
	r := NewRegistry(TracesProfile)
	m := r.ResolveToParquet("scope_attr:custom.field")
	if m == nil {
		t.Fatal("scope_attr: prefix should resolve to MAP")
	}
	if m.Origin != OriginScopeAttrMap {
		t.Errorf("Origin = %d, want OriginScopeAttrMap", m.Origin)
	}
}

func TestRegistry_ExtraPromoted(t *testing.T) {
	extra := []ExtraPromoted{
		{Name: "http.status_code", Type: "string", Bloom: true},
		{Name: "customer_id", Type: "string", Bloom: true},
	}
	r := NewRegistry(LogsProfile, extra...)

	m := r.ResolveToParquet("http.status_code")
	if m == nil {
		t.Fatal("extra promoted http.status_code should resolve")
	}
	if m.Origin != OriginPromoted {
		t.Errorf("Origin = %d, want OriginPromoted", m.Origin)
	}
	if !m.HasBloom {
		t.Error("HasBloom should be true")
	}
	if m.ParquetColumn != "http.status_code" {
		t.Errorf("ParquetColumn = %q", m.ParquetColumn)
	}

	m2 := r.ResolveToParquet("customer_id")
	if m2 == nil {
		t.Fatal("extra promoted customer_id should resolve")
	}
	if !m2.HasBloom {
		t.Error("customer_id HasBloom should be true")
	}
}

func TestRegistry_ExtraPromotedReverseResolve(t *testing.T) {
	extra := []ExtraPromoted{
		{Name: "http.status_code", Type: "string", Bloom: false},
	}
	r := NewRegistry(LogsProfile, extra...)

	m := r.ResolveFromParquet("http.status_code")
	if m == nil {
		t.Fatal("reverse resolve should find extra promoted")
	}
	if m.InternalName != "http.status_code" {
		t.Errorf("InternalName = %q", m.InternalName)
	}
}

func TestRegistry_ExtraPromotedList(t *testing.T) {
	extra := []ExtraPromoted{
		{Name: "http.status_code", Type: "string", Bloom: true},
		{Name: "customer_id", Type: "string", Bloom: false},
	}
	r := NewRegistry(LogsProfile, extra...)

	got := r.ExtraPromoted()
	if len(got) != 2 {
		t.Fatalf("ExtraPromoted() len = %d, want 2", len(got))
	}
	if got[0].Name != "http.status_code" {
		t.Errorf("got[0].Name = %q", got[0].Name)
	}
}

func TestRegistry_IsPromoted(t *testing.T) {
	r := NewRegistry(LogsProfile)

	if !r.IsPromoted("_time") {
		t.Error("_time should be promoted")
	}
	if !r.IsPromoted("service.name") {
		t.Error("service.name should be promoted")
	}
	if r.IsPromoted("random.field") {
		t.Error("random.field should not be promoted")
	}
}

func TestRegistry_IsPromoted_WithExtra(t *testing.T) {
	extra := []ExtraPromoted{
		{Name: "customer_id", Type: "string", Bloom: true},
	}
	r := NewRegistry(LogsProfile, extra...)

	if !r.IsPromoted("customer_id") {
		t.Error("extra promoted customer_id should be promoted")
	}
	if !r.IsPromoted("_time") {
		t.Error("default promoted _time should still be promoted")
	}
	if r.IsPromoted("unknown") {
		t.Error("unknown should not be promoted")
	}
}

func TestRegistry_ExtraPromotedOverridesDefault(t *testing.T) {
	extra := []ExtraPromoted{
		{Name: "service.name", Type: "string", Bloom: false},
	}
	r := NewRegistry(LogsProfile, extra...)

	m := r.ResolveToParquet("service.name")
	if m == nil {
		t.Fatal("service.name should resolve")
	}
	if m.HasBloom {
		t.Error("extra promoted should override default bloom setting")
	}
}

func TestRegistry_NoExtraPromoted(t *testing.T) {
	r := NewRegistry(LogsProfile)
	got := r.ExtraPromoted()
	if len(got) != 0 {
		t.Errorf("ExtraPromoted() should be empty, got %d", len(got))
	}
}

func TestRegistry_DottedConvention_SpanAttrs(t *testing.T) {
	profile := Profile{
		Promoted:     []FieldMapping{},
		MapColumns:   []string{"span.attributes"},
		StreamFields: []string{},
	}
	r := NewRegistry(profile)
	m := r.ResolveToParquet("my.custom.field")
	if m == nil {
		t.Fatal("dotted field should resolve to MAP fallback")
	}
	if m.Origin != OriginSpanAttrMap {
		t.Errorf("Origin = %d, want OriginSpanAttrMap (%d)", m.Origin, OriginSpanAttrMap)
	}
	if m.MapColumn != "span.attributes" {
		t.Errorf("MapColumn = %q, want span.attributes", m.MapColumn)
	}
}

func TestRegistry_DottedConvention_LogAttrs(t *testing.T) {
	profile := Profile{
		Promoted:     []FieldMapping{},
		MapColumns:   []string{"log.attributes"},
		StreamFields: []string{},
	}
	r := NewRegistry(profile)
	m := r.ResolveToParquet("custom.field")
	if m == nil {
		t.Fatal("dotted field should resolve to MAP fallback")
	}
	if m.Origin != OriginLogAttrMap {
		t.Errorf("Origin = %d, want OriginLogAttrMap (%d)", m.Origin, OriginLogAttrMap)
	}
}

func TestRegistry_DottedConvention_ScopeAttrs(t *testing.T) {
	profile := Profile{
		Promoted:     []FieldMapping{},
		MapColumns:   []string{"scope.attributes"},
		StreamFields: []string{},
	}
	r := NewRegistry(profile)
	m := r.ResolveToParquet("custom.field")
	if m == nil {
		t.Fatal("should resolve")
	}
	if m.Origin != OriginScopeAttrMap {
		t.Errorf("Origin = %d, want OriginScopeAttrMap", m.Origin)
	}
}

func TestRegistry_DottedConvention_UnknownMapColumn(t *testing.T) {
	profile := Profile{
		Promoted:     []FieldMapping{},
		MapColumns:   []string{"custom.attributes"},
		StreamFields: []string{},
	}
	r := NewRegistry(profile)
	m := r.ResolveToParquet("field")
	if m == nil {
		t.Fatal("should resolve to fallback")
	}
	if m.Origin != OriginResourceMap {
		t.Errorf("Origin = %d, want OriginResourceMap (default)", m.Origin)
	}
}

func TestRegistry_NoMapColumns(t *testing.T) {
	profile := Profile{
		Promoted:     []FieldMapping{},
		MapColumns:   []string{},
		StreamFields: []string{},
	}
	r := NewRegistry(profile)
	m := r.ResolveToParquet("unknown.field")
	if m != nil {
		t.Error("should return nil when no MAP columns and no promoted match")
	}
}
