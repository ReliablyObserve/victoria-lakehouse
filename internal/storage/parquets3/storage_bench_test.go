package parquets3

import (
	"testing"
)

func BenchmarkExtractExactMatch(b *testing.B) {
	query := `service.name:="api-gw" AND trace_id:="abc123"`
	field := "service.name"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractExactMatch(query, field)
	}
}

func BenchmarkExtractExactMatch_NoMatch(b *testing.B) {
	query := `some query without the field`
	field := "service.name"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractExactMatch(query, field)
	}
}

func BenchmarkIsPrintable_Short(b *testing.B) {
	data := []byte("hello world")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isPrintable(data)
	}
}

func BenchmarkIsPrintable_Long(b *testing.B) {
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte('a' + i%26)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isPrintable(data)
	}
}
