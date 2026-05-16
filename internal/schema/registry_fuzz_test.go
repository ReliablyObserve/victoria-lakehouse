package schema

import (
	"testing"
)

func FuzzFormatValue_String(f *testing.F) {
	f.Add("hello world")
	f.Add("")
	f.Add("special chars: \x00\n\t\\\"")
	f.Add("{json: true}")
	f.Add("unicode: ąéñ日本語")

	f.Fuzz(func(t *testing.T, input string) {
		result := TypeString.FormatValue(input)
		if result != input {
			t.Errorf("TypeString.FormatValue(%q) = %q, want pass-through", input, result)
		}
	})
}

func FuzzFormatValue_TimestampNano(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(1714650000000000000))
	f.Add(int64(-1000000000))
	f.Add(int64(9223372036854775807))
	f.Add(int64(-9223372036854775808))

	f.Fuzz(func(t *testing.T, ns int64) {
		result := TypeTimestampNano.FormatValue(ns)
		if result == "" {
			t.Error("timestamp format should never return empty string")
		}
	})
}

func FuzzFormatValue_Int64(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(42))
	f.Add(int64(-1))
	f.Add(int64(9223372036854775807))

	f.Fuzz(func(t *testing.T, n int64) {
		result := TypeInt64.FormatValue(n)
		if result == "" {
			t.Error("int64 format should never return empty string")
		}
	})
}

func FuzzFormatValue_Float64(f *testing.F) {
	f.Add(float64(0))
	f.Add(float64(3.14159))
	f.Add(float64(-1e308))
	f.Add(float64(1e308))

	f.Fuzz(func(t *testing.T, n float64) {
		result := TypeFloat64.FormatValue(n)
		if result == "" {
			t.Error("float64 format should never return empty string")
		}
	})
}

func FuzzResolveToParquet(f *testing.F) {
	f.Add("service.name")
	f.Add("resource_attr:service.name")
	f.Add("span_attr:http.method")
	f.Add("scope_attr:otel.library.name")
	f.Add("log_attr:custom.field")
	f.Add("unknown_field")
	f.Add("")
	f.Add("resource_attr:")
	f.Add("span_attr:")
	f.Add("scope_attr:")
	f.Add("log_attr:")
	f.Add("a:b:c:d")
	f.Add("_time")
	f.Add("_msg")
	f.Add("_stream")
	f.Add("_stream_id")

	registry := NewRegistry(LogsProfile)

	f.Fuzz(func(t *testing.T, fieldName string) {
		m := registry.ResolveToParquet(fieldName)
		if m == nil {
			t.Skipf("nil mapping for %q (no map columns)", fieldName)
		}
		if m.ParquetColumn == "" {
			t.Errorf("ResolveToParquet(%q) returned mapping with empty ParquetColumn", fieldName)
		}
		if m.InternalName != fieldName {
			t.Errorf("ResolveToParquet(%q).InternalName = %q, want original", fieldName, m.InternalName)
		}
	})
}

func FuzzResolveToParquet_Traces(f *testing.F) {
	f.Add("trace_id")
	f.Add("span_id")
	f.Add("resource_attr:service.name")
	f.Add("span_attr:http.status_code")
	f.Add("scope_attr:otel.library.name")
	f.Add("unknown.field")
	f.Add("")
	f.Add("_stream")
	f.Add("_stream_id")

	registry := NewRegistry(TracesProfile)

	f.Fuzz(func(t *testing.T, fieldName string) {
		m := registry.ResolveToParquet(fieldName)
		if m == nil {
			t.Skipf("nil mapping for %q", fieldName)
		}
		if m.ParquetColumn == "" {
			t.Errorf("ResolveToParquet(%q) returned mapping with empty ParquetColumn", fieldName)
		}
	})
}

func FuzzParseFieldType(f *testing.F) {
	f.Add("string")
	f.Add("int32")
	f.Add("int64")
	f.Add("float64")
	f.Add("bool")
	f.Add("timestamp_nano")
	f.Add("")
	f.Add("UNKNOWN")
	f.Add("INT32")

	f.Fuzz(func(t *testing.T, s string) {
		ft := ParseFieldType(s)
		if ft < TypeString || ft > TypeBool {
			t.Errorf("ParseFieldType(%q) returned out-of-range FieldType: %d", s, ft)
		}
	})
}
