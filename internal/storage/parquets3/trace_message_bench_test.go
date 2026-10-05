package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The control has no native message, matching legacy trace objects. Check every
// converted row so an omitted field cannot masquerade as faster conversion.
func BenchmarkTraceMessageLegacyControl(b *testing.B) {
	row := schema.TraceRow{TimestampUnixNano: 123, TraceID: "trace-control", SpanID: "span-control", SpanName: "control"}
	b.ReportAllocs()
	for b.Loop() {
		fields := traceRowToFields(&row, nil)
		var seen [3]bool
		for _, f := range fields {
			switch f.name {
			case "trace_id":
				if seen[0] || f.value != row.TraceID {
					b.Fatal("trace ID changed")
				}
				seen[0] = true
			case "span_id":
				if seen[1] || f.value != row.SpanID {
					b.Fatal("span ID changed")
				}
				seen[1] = true
			case "name":
				if seen[2] || f.value != row.SpanName {
					b.Fatal("operation name changed")
				}
				seen[2] = true
			case "_msg":
				if f.value != "" {
					b.Fatal("legacy native message fabricated")
				}
			}
		}
		if !seen[0] || !seen[1] || !seen[2] {
			b.Fatalf("identity or operation field missing: %v", seen)
		}
	}
}
