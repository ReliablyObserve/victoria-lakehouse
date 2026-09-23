package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// BenchmarkFieldValues_Level measures an unfiltered whole-range
// `field_values?field=level` over the parity corpus shape: 24 hourly
// partitions of 400 rows, levels spread uniformly. pmeta=on is the default
// deployment (answered from the catalog); pmeta=off is the degraded mode
// (answered by a column-projected scan). The label index is seeded by a first
// query, as in production.
func BenchmarkFieldValues_Level(b *testing.B) {
	for _, pmetaOn := range []bool{true, false} {
		b.Run(fmt.Sprintf("pmeta=%v", pmetaOn), func(b *testing.B) {
			mock := newMockS3Server()
			b.Cleanup(mock.close)
			s := testStorageWithS3(b, mock.url())
			bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
			if pmetaOn {
				s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
				s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
				bw.catalogObserver = &catalogObserver{store: s.catalog}
			}
			base := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
			levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
			rows := make([]schema.LogRow, 0, 24*400)
			for h := 0; h < 24; h++ {
				for i := 0; i < 400; i++ {
					rows = append(rows, schema.LogRow{
						TimestampUnixNano: base.Add(time.Duration(h)*time.Hour + time.Duration(i)*time.Second).UnixNano(),
						Body:              "row", ServiceName: "svc", SeverityText: levels[(h+i)%len(levels)],
					})
				}
			}
			bw.AddLogRows(rows)
			bw.triggerFlush()

			q, err := logstorage.ParseQuery("*")
			if err != nil {
				b.Fatal(err)
			}
			q.AddTimeFilter(base.Add(-time.Hour).UnixNano(), base.Add(25*time.Hour).UnixNano())
			if err := s.RunQuery(context.Background(), nil, q, func(uint, *logstorage.DataBlock) {}); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vals, err := s.GetFieldValues(context.Background(), nil, q, "level", 0)
				if err != nil {
					b.Fatal(err)
				}
				if len(vals) != len(levels) {
					b.Fatalf("got %d level values, want %d", len(vals), len(levels))
				}
			}
		})
	}
}
