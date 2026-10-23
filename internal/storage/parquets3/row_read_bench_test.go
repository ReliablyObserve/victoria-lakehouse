package parquets3

// Benchmarks of the cold read path touched by #503: a wildcard row read and a
// field_values scan over twelve objects (mock S3 over HTTP). Compare with main
// using benchstat; the allocation counts carry the claim, the end-to-end time
// is noisy.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func benchColdFixture(b *testing.B) (*Storage, int64, int64) {
	b.Helper()
	mock := newMockS3Server()
	b.Cleanup(mock.close)
	s := testStorageWithS3(b, mock.url())
	s.cfg.Query.FileWorkers = 8
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	for i := 0; i < 12; i++ {
		rows := make([]logRow, 2000)
		for j := range rows {
			rows[j] = logRow{
				TimestampUnixNano: now.Add(time.Duration(i*2000+j) * time.Microsecond).UnixNano(),
				Body:              fmt.Sprintf("file=%d row=%d payload", i, j),
				SeverityText:      []string{"INFO", "WARN", "ERROR"}[j%3],
				ServiceName:       fmt.Sprintf("svc-%d", j%7),
			}
		}
		key := fmt.Sprintf("logs/dt=2026-05-10/hour=14/bench-%02d.parquet", i)
		data := writeParquetToBytes(b, rows)
		mock.putFile(key, data)
		s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{Key: key, Size: int64(len(data)), MinTimeNs: now.Add(-time.Minute).UnixNano(), MaxTimeNs: now.Add(time.Minute).UnixNano()})
	}
	return s, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()
}

// BenchmarkRunQuery_ColdScan is a wildcard row read over twelve objects: the file
// worker loop, processOneFile and the per-block budget gate.
func BenchmarkRunQuery_ColdScan(b *testing.B) {
	s, lo, hi := benchColdFixture(b)
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		b.Fatal(err)
	}
	q.AddTimeFilter(lo, hi)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var rows atomic.Int64
		if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) { rows.Add(int64(db.RowsCount())) }); err != nil {
			b.Fatal(err)
		}
		if rows.Load() != 12*2000 {
			b.Fatalf("rows = %d", rows.Load())
		}
	}
}

// BenchmarkGetFieldValues_ColdScan is a field_values scan over the same objects:
// scanFieldValuesParallel and scanProjectedFieldValues.
func BenchmarkGetFieldValues_ColdScan(b *testing.B) {
	s, lo, hi := benchColdFixture(b)
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		b.Fatal(err)
	}
	q.AddTimeFilter(lo, hi)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
		if err != nil || len(vals) != 7 {
			b.Fatalf("values = %d, err %v", len(vals), err)
		}
	}
}
