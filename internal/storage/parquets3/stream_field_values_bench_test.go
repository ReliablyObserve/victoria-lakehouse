package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// BenchmarkStreamFieldValues measures stream_field_values on cold objects:
// 20k rows in 35 streams (5 services x 7 namespaces), unfiltered (the stream
// column's label aggregate answers it) and with a level filter (the rows are
// read). Gets and bytes per call are reported with the time.
func BenchmarkStreamFieldValues(b *testing.B) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(b, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	at := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	var rows []schema.LogRow
	lv := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	for i := 0; i < 20000; i++ {
		svc, ns := fmt.Sprintf("svc%d", i%5), fmt.Sprintf("ns%d", i%7)
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: at.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              "row body text", ServiceName: svc, SeverityText: lv[i%4], K8sNamespaceName: ns,
			Stream: fmt.Sprintf(`{service.name=%q,k8s.namespace.name=%q}`, svc, ns),
		})
	}
	bw.stageLogRows(rows)
	bw.flushStagedNow()
	lo, hi := at.Add(-time.Hour).UnixNano(), at.Add(time.Hour).UnixNano()
	for _, sh := range []struct{ name, q, field string }{
		{"service_all", "*", "service.name"},
		{"service_levelfilter", "level:=ERROR", "service.name"},
		{"namespace_all", "*", "k8s.namespace.name"},
	} {
		b.Run(sh.name, func(b *testing.B) {
			q := mustParseQueryWithTimeB(b, sh.q, lo, hi)
			if _, err := s.GetStreamFieldValues(context.Background(), nil, q, sh.field, 0); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			b.ReportAllocs()
			g0, by0 := mock.gets.Load(), mock.bytesServed.Load()
			for i := 0; i < b.N; i++ {
				if _, err := s.GetStreamFieldValues(context.Background(), nil, q, sh.field, 0); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(mock.gets.Load()-g0)/float64(b.N), "gets/op")
			b.ReportMetric(float64(mock.bytesServed.Load()-by0)/float64(b.N), "s3B/op")
		})
	}
}
