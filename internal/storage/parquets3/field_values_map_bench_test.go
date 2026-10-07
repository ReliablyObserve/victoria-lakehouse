package parquets3

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func BenchmarkFieldValuesMapAttr_FieldValues(b *testing.B) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(b, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	at := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	var rows []schema.LogRow
	lv := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	for i := 0; i < 20000; i++ {
		ns := ""
		if i%3 != 0 {
			ns = fmt.Sprintf("ns%d", i%7)
		}
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: at.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              "row body text", ServiceName: fmt.Sprintf("svc%d", i%5), SeverityText: lv[i%4],
			K8sNamespaceName:   ns,
			ResourceAttributes: map[string]string{"rk": fmt.Sprintf("r%d", i%11), "host.id": fmt.Sprintf("h%d", i%13)},
			LogAttributes:      map[string]string{"lk": fmt.Sprintf("l%d", i%11), "user": fmt.Sprintf("u%d", i%17)},
		})
	}
	bw.stageLogRows(rows)
	bw.flushStagedNow()
	fvEmptyColumnSizes(b, mock)
	lo, hi := at.Add(-time.Hour).UnixNano(), at.Add(time.Hour).UnixNano()
	for _, sh := range []struct{ name, q, field string }{
		{"level_all", "*", "level"},
		{"level_svcfilter", "service.name:=svc1", "level"},
		{"svc_levelfilter", "level:=ERROR", "service.name"},
		{"ns_levelfilter", "level:=ERROR", "k8s.namespace.name"},
		{"lk_all", "*", "lk"},
		{"lk_levelfilter", "level:=ERROR", "lk"},
	} {
		b.Run(sh.name, func(b *testing.B) {
			q := mustParseQueryWithTimeB(b, sh.q, lo, hi)
			g0, by0 := mock.gets.Load(), mock.bytesServed.Load()
			vs, err := s.GetFieldValues(context.Background(), nil, q, sh.field, 0)
			if err != nil {
				b.Fatal(err)
			}
			g1, by1 := mock.gets.Load(), mock.bytesServed.Load()
			sort.Slice(vs, func(i, j int) bool { return vs[i].Value < vs[j].Value })
			b.Logf("answer=%v firstcall_gets=%d firstcall_bytes=%d", vs, g1-g0, by1-by0)
			b.ResetTimer()
			b.ReportAllocs()
			g0, by0 = mock.gets.Load(), mock.bytesServed.Load()
			for i := 0; i < b.N; i++ {
				if _, err := s.GetFieldValues(context.Background(), nil, q, sh.field, 0); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(mock.gets.Load()-g0)/float64(b.N), "gets/op")
			b.ReportMetric(float64(mock.bytesServed.Load()-by0)/float64(b.N), "s3B/op")
		})
	}
}

func mustParseQueryWithTimeB(b *testing.B, qs string, lo, hi int64) *logstorage.Query {
	q, err := logstorage.ParseQuery(qs)
	if err != nil {
		b.Fatal(err)
	}
	q.AddTimeFilter(lo, hi)
	return q
}
