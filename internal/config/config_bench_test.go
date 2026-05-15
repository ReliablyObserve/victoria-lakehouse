package config

import (
	"testing"
)

func BenchmarkParseSizeBytes(b *testing.B) {
	inputs := []string{"512MB", "50GB", "1TB", "256KB", "1024"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseSizeBytes(inputs[i%len(inputs)])
	}
}

func BenchmarkValidate(b *testing.B) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cfg.Validate()
	}
}

func BenchmarkDefault(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Default()
	}
}
