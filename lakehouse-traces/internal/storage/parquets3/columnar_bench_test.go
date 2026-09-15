package parquets3

import (
	"bytes"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// BenchmarkReadRowGroupColumnar_Wildcard100k measures the shape a cold Jaeger
// span list actually produces: 100k spans, no projection, every column of the
// real TraceRow schema read through the columnar fast path. Twin of the logs
// module's benchmark of the same name; it guards the cost of the per-column
// field-name resolution and the NULL skip.
func BenchmarkReadRowGroupColumnar_Wildcard100k(b *testing.B) {
	f := writeWildcardBenchFile(b, 100_000)
	reg := schema.NewRegistry(schema.TracesProfile)
	rg := f.RowGroups()[0]
	wantCols := allLeafColumns(f)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		readRowGroupColumnar(f, rg, wantCols, reg, 0, int64(1)<<62, nil, nil, nil)
	}
}

// writeWildcardBenchFile writes numRows real schema.TraceRow spans into one row
// group, with attributes present on some spans and absent on others so the NULL
// and empty-cell paths are both exercised.
func writeWildcardBenchFile(b *testing.B, numRows int) *parquet.File {
	b.Helper()
	rows := make([]schema.TraceRow, numRows)
	for i := range rows {
		rows[i] = schema.TraceRow{
			AccountID:          7,
			ProjectID:          42,
			TimestampUnixNano:  int64(1716393600000000000 + i*1000000),
			StartTimeUnixNano:  int64(1716393600000000000 + i*1000000),
			TraceID:            "abc123def456abc123def456abc123de",
			SpanID:             "span789abcdef012",
			SpanName:           "GET /api/v1/things",
			ServiceName:        "api-gateway",
			DurationNs:         1_500_000,
			HTTPMethod:         "GET",
			K8sNamespaceName:   "production",
			Stream:             `{resource_attr:service.name="api-gateway"}`,
			ResourceAttributes: map[string]string{"custom.res": "r1"},
			SpanAttributes:     map[string]string{"custom.span": "s1"},
		}
		if i%2 == 0 {
			rows[i].URLFull = "https://x/y"
			rows[i].ClientAddress = "10.0.0.1"
		}
	}
	res, err := writeTracesParquet(rows, numRows, 3)
	if err != nil {
		b.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(res.Data), int64(len(res.Data)))
	if err != nil {
		b.Fatal(err)
	}
	return f
}

// BenchmarkMapAttrFieldName measures the per-attribute cost of the shared naming
// rule on the MAP hot loop (one call per attribute per span): prefix
// concatenation, the reserved-name classification, and interning. The
// "intern_only" sub-benchmark is the same loop without the classification, so
// the difference between the two is exactly what routing MAP keys through the
// single rule costs.
func BenchmarkMapAttrFieldName(b *testing.B) {
	keys := []string{"custom.key", "http.route", "user.id", "k8s.container.name"}
	b.Run("rule", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _ = mapAttrFieldNameWithPrefix("span_attr:", keys[i&3], nil)
		}
	})
	b.Run("intern_only", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_ = bytesutil.InternString("span_attr:" + keys[i&3])
		}
	})
}
