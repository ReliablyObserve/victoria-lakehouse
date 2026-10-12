package parquets3

import (
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tokenbloom"
)

// presenceBenchLogRows is a flush-sized batch shaped like production logs: 100
// streams, about 30 non-empty fields per row (the fixed columns, a few dedicated
// ones and ten attributes).
func presenceBenchLogRows(n int) []schema.LogRow {
	base := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	rows := make([]schema.LogRow, n)
	for i := range rows {
		s := i % 100
		rows[i] = schema.LogRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("GET /api/v1/items/%d status=200 dur=%dms", i, i%500),
			SeverityText:      "INFO", SeverityNumber: 9,
			ServiceName: fmt.Sprintf("svc-%d", s), TraceID: fmt.Sprintf("%032x", i), SpanID: fmt.Sprintf("%016x", i),
			K8sNamespaceName: "prod", K8sPodName: fmt.Sprintf("pod-%d", s), K8sDeploymentName: "dep", K8sNodeName: "node-1",
			DeployEnv: "prod", CloudRegion: "eu-1", HostName: fmt.Sprintf("host-%d", s%10),
			Stream: fmt.Sprintf(`{service.name="svc-%d"}`, s), StreamID: fmt.Sprintf("%048x", s+1),
			ContainerID: "c", K8sClusterName: "cl", CloudProvider: "aws",
			ResourceAttributes: presenceBenchAttrs("res", 5),
			LogAttributes:      presenceBenchAttrs("log", 5),
		}
	}
	return rows
}

// presenceBenchTraceRows: one span in eventEvery carries an event (0: none).
func presenceBenchTraceRows(n, eventEvery int) []schema.TraceRow {
	base := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		s := i % 100
		ts := base.Add(time.Duration(i) * time.Millisecond).UnixNano()
		rows[i] = schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: ts - 1000, TraceID: fmt.Sprintf("%032x", i/8), SpanID: fmt.Sprintf("%016x", i),
			ParentSpanID: fmt.Sprintf("%016x", i/2), SpanName: "GET /items", ServiceName: fmt.Sprintf("svc-%d", s),
			DurationNs: 1000 + int64(i%500), StatusCode: 1, SpanKind: 2, HTTPMethod: "GET", HTTPStatusCode: "200",
			Stream: fmt.Sprintf(`{resource_attr:service.name="svc-%d"}`, s), StreamID: fmt.Sprintf("%048x", s+1),
			K8sNamespaceName: "prod", DeployEnv: "prod", CloudRegion: "eu-1", HostName: "h",
			ResourceAttributes: presenceBenchAttrs("res", 3),
			SpanAttributes:     presenceBenchAttrs("span", 8),
		}
		if eventEvery > 0 && i%eventEvery == 0 {
			rows[i].EventsJSON = `[{"event_name":"exception","event_time_unix_nano":"1","event_attr:exception.type":"IOError"}]`
		}
	}
	return rows
}

const presenceBenchRows = 20000

// presenceBenchAttrs are n attributes whose key strings are freshly allocated
// (a parser does not intern them), so a cache keyed on string identity cannot help.
func presenceBenchAttrs(prefix string, n int) map[string]string {
	m := make(map[string]string, n)
	for i := 0; i < n; i++ {
		m[fmt.Sprintf("%s.attr.%d", prefix, i)] = "v"
	}
	return m
}

// BenchmarkFieldPresence_Extract is the cost of computing and encoding the facet
// of one flush; BenchmarkFlushWrite is writing the same rows (which includes the
// facet), so extract/write is the share the facet adds.
func BenchmarkFieldPresence_Extract(b *testing.B) {
	logs, traces := presenceBenchLogRows(presenceBenchRows), presenceBenchTraceRows(presenceBenchRows, 20)
	b.Run("logs", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			presenceOfLogRows(logs, nil).Finish()
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/presenceBenchRows, "ns/row")
	})
	b.Run("traces", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			presenceOfTraceRows(traces, nil).Finish()
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/presenceBenchRows, "ns/row")
	})
	// The worst case: every span carries an event, whose JSON is parsed for its
	// field names.
	b.Run("traces_event_on_every_span", func(b *testing.B) {
		every := presenceBenchTraceRows(presenceBenchRows, 1)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			presenceOfTraceRows(every, nil).Finish()
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/presenceBenchRows, "ns/row")
	})
}

// BenchmarkTokenBloom_RowGroupOptions is the body token bloom of a whole object
// (what compaction now adds to its output, #344).
func BenchmarkTokenBloom_RowGroupOptions(b *testing.B) {
	logs := presenceBenchLogRows(presenceBenchRows)
	for i := 0; i < b.N; i++ {
		_ = tokenbloom.RowGroupOptions(len(logs), 10000, func(i int) string { return logs[i].Body })
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/presenceBenchRows, "ns/row")
}

func BenchmarkFlushWrite(b *testing.B) {
	logs, traces := presenceBenchLogRows(presenceBenchRows), presenceBenchTraceRows(presenceBenchRows, 20)
	b.Run("logs", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := writeLogsParquet(logs, 10000, 3); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/presenceBenchRows, "ns/row")
	})
	b.Run("traces", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := writeTracesParquet(traces, 10000, 3); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/presenceBenchRows, "ns/row")
	})
}

// BenchmarkCompactWrite is the compaction output writer (also the delete
// rewriter's) over the same rows: facet and body token blooms included.
func BenchmarkCompactWrite(b *testing.B) {
	fieldpresence.SetComputers(FieldPresenceComputers())
	b.Cleanup(func() { fieldpresence.SetComputers(fieldpresence.Computers{}) })
	logs, traces := presenceBenchLogRows(presenceBenchRows), presenceBenchTraceRows(presenceBenchRows, 20)
	b.Run("logs", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := compaction.WriteLogs(logs, 10000, 3); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/presenceBenchRows, "ns/row")
	})
	b.Run("traces", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := compaction.WriteTraces(traces, 10000, 3); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/presenceBenchRows, "ns/row")
	})
}
