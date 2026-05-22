package schema

import (
	"testing"
	"time"
)

func TestFastFormatTimestampNano(t *testing.T) {
	cases := []int64{
		0,
		1700000000000000000,
		1716393600123456789,
		time.Date(2026, 5, 22, 13, 45, 30, 123456789, time.UTC).UnixNano(),
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(2099, 12, 31, 23, 59, 59, 999999999, time.UTC).UnixNano(),
	}
	for _, ns := range cases {
		want := time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
		got := fastFormatTimestampNano(ns)
		if got != want {
			t.Errorf("fastFormatTimestampNano(%d) = %q, want %q", ns, got, want)
		}
	}
}

func TestFastFormatInt64(t *testing.T) {
	cases := []int64{0, 1, -1, 42, 999, 1000, -1000, 1234567890, -9223372036854775807}
	for _, n := range cases {
		want := TypeInt64.FormatValue(n)
		got := fastFormatInt64(n)
		if got != want {
			t.Errorf("fastFormatInt64(%d) = %q, want %q", n, got, want)
		}
	}
}

var benchResult string

func BenchmarkFormatTimestampNano_Fast(b *testing.B) {
	ns := int64(1716393600123456789)
	for b.Loop() {
		benchResult = fastFormatTimestampNano(ns)
	}
}

func BenchmarkFormatTimestampNano_Stdlib(b *testing.B) {
	ns := int64(1716393600123456789)
	for b.Loop() {
		benchResult = time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
	}
}

func BenchmarkFormatInt64_Fast(b *testing.B) {
	for b.Loop() {
		benchResult = fastFormatInt64(42)
	}
}

func BenchmarkFormatInt64_Strconv(b *testing.B) {
	for b.Loop() {
		benchResult = TypeInt64.FormatValue(int64(42))
	}
}

func BenchmarkFormatField_Mixed(b *testing.B) {
	r := NewRegistry(LogsProfile)
	vals := []struct {
		name string
		val  any
	}{
		{"_time", int64(1716393600123456789)},
		{"_msg", "this is a test log message"},
		{"level", "error"},
		{"severity_number", int64(3)},
		{"service.name", "api-gateway"},
	}
	b.ResetTimer()
	for b.Loop() {
		for _, v := range vals {
			benchResult = r.FormatField(v.name, v.val)
		}
	}
}
