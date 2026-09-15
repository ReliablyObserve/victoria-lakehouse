package parquets3

import (
	"bytes"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func BenchmarkProjectedFieldsToDataBlock(b *testing.B) {
	s := &Storage{
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	rows := makeTestRows(1000, 5)
	startNs := int64(0)
	endNs := int64(1 << 62)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s.projectedFieldsToDataBlock(rows, startNs, endNs, nil)
	}
}

func BenchmarkProjectedFieldsToDataBlock_WithMaps(b *testing.B) {
	s := &Storage{
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	rows := makeTestRowsWithMaps(1000, 5, 10)
	startNs := int64(0)
	endNs := int64(1 << 62)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		s.projectedFieldsToDataBlock(rows, startNs, endNs, nil)
	}
}

func BenchmarkTypedRowsToDataBlock(b *testing.B) {
	s := &Storage{
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	logRows := make([]schema.LogRow, 1000)
	for i := range logRows {
		logRows[i] = schema.LogRow{
			TimestampUnixNano: int64(1716393600000000000 + i*1000000),
			Body:              "test log message body content here",
			SeverityText:      "INFO",
			SeverityNumber:    int32(9),
			ServiceName:       "api-gateway",
			K8sNamespaceName:  "production",
			K8sPodName:        "api-gateway-7b8c9d-xkq2v",
			TraceID:           "abc123def456",
			SpanID:            "span789",
		}
	}

	startNs := int64(0)
	endNs := int64(1 << 62)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		typedRowsToDataBlock(s, logRows, startNs, endNs, logRowToFields)
	}
}

func makeTestRows(numRows, numCols int) [][]field {
	rows := make([][]field, numRows)
	for i := range rows {
		fields := make([]field, numCols)
		fields[0] = field{name: "timestamp_unix_nano", value: int64(1716393600000000000 + i*1000000)}
		fields[1] = field{name: "body", value: "test log message"}
		fields[2] = field{name: "severity_text", value: "INFO"}
		fields[3] = field{name: "service.name", value: "api-gateway"}
		if numCols > 4 {
			fields[4] = field{name: "trace_id", value: "abc123def456"}
		}
		rows[i] = fields
	}
	return rows
}

func writeTestParquetFile(b *testing.B, numRows int) (*parquet.File, *bytes.Reader) {
	b.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf)
	rows := make([]schema.LogRow, numRows)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: int64(1716393600000000000 + i*1000000),
			Body:              "test log message body content here",
			SeverityText:      "INFO",
			SeverityNumber:    int32(9),
			ServiceName:       "api-gateway",
			K8sNamespaceName:  "production",
			K8sPodName:        "api-gateway-7b8c9d-xkq2v",
			TraceID:           "abc123def456",
			SpanID:            "span789",
		}
	}
	if _, err := w.Write(rows); err != nil {
		b.Fatal(err)
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}
	reader := bytes.NewReader(buf.Bytes())
	f, err := parquet.OpenFile(reader, int64(buf.Len()))
	if err != nil {
		b.Fatal(err)
	}
	return f, reader
}

func BenchmarkReadRowGroup_Columnar(b *testing.B) {
	f, _ := writeTestParquetFile(b, 1000)
	reg := schema.NewRegistry(schema.LogsProfile)
	wantCols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"severity_text":       true,
		"service.name":        true,
		"trace_id":            true,
	}
	rg := f.RowGroups()[0]
	startNs := int64(0)
	endNs := int64(1 << 62)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		readRowGroupColumnar(f, rg, wantCols, reg, startNs, endNs, nil, nil)
	}
}

func BenchmarkReadRowGroup_RowOriented(b *testing.B) {
	f, _ := writeTestParquetFile(b, 1000)
	s := &Storage{registry: schema.NewRegistry(schema.LogsProfile)}
	wantCols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"severity_text":       true,
		"service.name":        true,
		"trace_id":            true,
	}
	rg := f.RowGroups()[0]
	startNs := int64(0)
	endNs := int64(1 << 62)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, _ := readRowGroupProjectedBitmap(f, rg, wantCols, nil)
		s.projectedFieldsToDataBlock(rows, startNs, endNs, nil)
	}
}

func makeTestRowsWithMaps(numRows, numPromoted, numMapEntries int) [][]field {
	rows := make([][]field, numRows)
	for i := range rows {
		fields := make([]field, 0, numPromoted+1)
		fields = append(fields, field{name: "timestamp_unix_nano", value: int64(1716393600000000000 + i*1000000)})
		fields = append(fields, field{name: "body", value: "test log message"})
		fields = append(fields, field{name: "severity_text", value: "INFO"})
		fields = append(fields, field{name: "service.name", value: "api-gateway"})
		fields = append(fields, field{name: "trace_id", value: "abc123def456"})

		m := make(map[string]string, numMapEntries)
		for j := 0; j < numMapEntries; j++ {
			m["key_"+string(rune('a'+j))] = "value_" + string(rune('a'+j))
		}
		fields = append(fields, field{name: "resource.attributes", value: m})
		rows[i] = fields
	}
	return rows
}

// BenchmarkReadRowGroupColumnar_Wildcard100k measures the shape a cold Grafana
// log list actually produces: 100k rows, no projection, every column of the real
// LogRow schema read through the columnar fast path. This is the benchmark that
// guards the cost of the per-column field-name resolution and the NULL skip.
func BenchmarkReadRowGroupColumnar_Wildcard100k(b *testing.B) {
	f := writeWildcardBenchFile(b, 100_000)
	reg := schema.NewRegistry(schema.LogsProfile)
	rg := f.RowGroups()[0]
	wantCols := allLeafColumns(f)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		readRowGroupColumnar(f, rg, wantCols, reg, 0, int64(1)<<62, nil, nil)
	}
}

// writeWildcardBenchFile writes numRows real schema.LogRow rows into one row
// group, with attributes present on some rows and absent on others so the NULL
// and empty-cell paths are both exercised.
func writeWildcardBenchFile(b *testing.B, numRows int) *parquet.File {
	b.Helper()
	rows := make([]schema.LogRow, numRows)
	for i := range rows {
		rows[i] = schema.LogRow{
			AccountID:         7,
			ProjectID:         42,
			TimestampUnixNano: int64(1716393600000000000 + i*1000000),
			Body:              "test log message body content here",
			SeverityText:      "INFO",
			SeverityNumber:    int32(9),
			ServiceName:       "api-gateway",
			K8sNamespaceName:  "production",
			K8sPodName:        "api-gateway-7b8c9d-xkq2v",
			TraceID:           "abc123def456",
			SpanID:            "span789",
			Stream:            `{service.name="api-gateway"}`,
			LogAttributes:     map[string]string{"custom.key": "v1"},
		}
		if i%2 == 0 {
			rows[i].ContainerID = "ctr-abc"
			rows[i].ExceptionType = "TimeoutError"
		}
	}
	res, err := writeLogsParquet(rows, numRows, 3)
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
// rule on the MAP hot loop (one call per attribute per row): prefix
// concatenation, the reserved-name classification, and interning. The
// "intern_only" sub-benchmark is the same loop without the classification, so
// the difference between the two is exactly what routing MAP keys through the
// single rule costs.
func BenchmarkMapAttrFieldName(b *testing.B) {
	keys := []string{"custom.key", "http.route", "user.id", "k8s.container.name"}
	b.Run("rule", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_, _ = mapAttrFieldNameWithPrefix("", keys[i&3])
		}
	})
	b.Run("intern_only", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_ = bytesutil.InternString("" + keys[i&3])
		}
	})
}
