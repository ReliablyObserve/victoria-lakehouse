package compaction

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func benchCompactionTraceRows(n int) []schema.TraceRow {
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

// BenchmarkWriteTraces is the compaction (and delete rewrite) write path of a
// traces object.
func BenchmarkWriteTraces(b *testing.B) {
	rows := benchCompactionTraceRows(20000)
	b.ReportAllocs()
	b.ResetTimer()
	var size int
	for i := 0; i < b.N; i++ {
		data, err := WriteTraces(rows, 10000, 7)
		if err != nil {
			b.Fatal(err)
		}
		size = len(data)
	}
	b.ReportMetric(float64(size), "object-bytes")
}

// BenchmarkReadMergeWriteTraces reads two plain-writer objects, merges and
// rewrites them: the whole compaction CPU path of a traces partition.
func BenchmarkReadMergeWriteTraces(b *testing.B) {
	rows := benchCompactionTraceRows(20000)
	var inputs [][]byte
	for part := 0; part < 2; part++ {
		var half []schema.TraceRow
		for i := part; i < len(rows); i += 2 {
			half = append(half, rows[i])
		}
		data, err := WriteTraces(half, 10000, 3)
		if err != nil {
			b.Fatal(err)
		}
		inputs = append(inputs, data)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var merged []schema.TraceRow
		for _, in := range inputs {
			r, err := readTraceRows(in)
			if err != nil {
				b.Fatal(err)
			}
			merged = append(merged, r...)
		}
		if _, err := WriteTraces(merged, 10000, 7); err != nil {
			b.Fatal(err)
		}
	}
}
