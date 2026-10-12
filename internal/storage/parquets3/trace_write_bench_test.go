package parquets3

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// benchTraceRows builds n OTLP-shaped spans: the span timestamp is its end,
// start = end - duration, a few services and operations, parent links inside a
// trace, status and kind set.
func benchTraceRows(n int) []schema.TraceRow {
	rng := rand.New(rand.NewSource(7))
	rows := make([]schema.TraceRow, n)
	base := int64(1_780_000_000_000_000_000)
	for i := range rows {
		dur := int64(rng.Intn(200_000_000)) + 1000
		end := base + int64(i)*5_000_000 + int64(rng.Intn(1000))
		rows[i] = schema.TraceRow{
			TimestampUnixNano: end,
			StartTimeUnixNano: schema.Int64Ptr(end - dur),
			TraceID:           fmt.Sprintf("%032x", i/8),
			SpanID:            fmt.Sprintf("%016x", i),
			ParentSpanID:      fmt.Sprintf("%016x", i-i%8),
			SpanName:          fmt.Sprintf("op-%d", rng.Intn(40)),
			ServiceName:       fmt.Sprintf("svc-%d", rng.Intn(12)),
			DurationNs:        schema.Int64Ptr(dur),
			StatusCode:        schema.Int32Ptr(int32(rng.Intn(3))),
			SpanKind:          schema.Int32Ptr(int32(rng.Intn(5))),
			HTTPMethod:        "GET",
			HTTPStatusCode:    "200",
		}
	}
	return rows
}

// BenchmarkWriteTracesParquet is the flush write path of a traces object; the
// reported B/op of the object is the output size (s3_bytes/op).
func BenchmarkWriteTracesParquet(b *testing.B) {
	rows := benchTraceRows(10000)
	b.ReportAllocs()
	b.ResetTimer()
	var size int
	for i := 0; i < b.N; i++ {
		res, err := writeTracesParquet(rows, 10000, 7)
		if err != nil {
			b.Fatal(err)
		}
		size = len(res.Data)
	}
	b.ReportMetric(float64(size), "object-bytes")
}
