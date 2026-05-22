package parquets3

import (
	"testing"

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
		s.projectedFieldsToDataBlock(rows, startNs, endNs)
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
		s.projectedFieldsToDataBlock(rows, startNs, endNs)
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
