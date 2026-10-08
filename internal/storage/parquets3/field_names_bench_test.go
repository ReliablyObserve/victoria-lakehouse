package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// BenchmarkGetFieldNames_ObjectReads is the cost guard of field_names: one call
// over five 200 KiB objects on cold caches, reporting the S3 requests and the
// bytes served per call (the footer walk read one footer per object; the answer
// from the rows reads the columns of every object). A change that reads more
// than this shows up in `benchstat`, and docs/operations.md states the cost.
func BenchmarkGetFieldNames_ObjectReads(b *testing.B) {
	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	data := makeLargeParquet(&testing.T{}, baseTime, 200*1024) // the helper takes a *testing.T and only uses it to fail
	b.ReportAllocs()
	var reqs, served int64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mock := newInstrumentedS3Server()
		s := testStorageWithS3(b, mock.url())
		for f := 0; f < 5; f++ {
			key := fmt.Sprintf("logs/dt=2026-05-28/hour=%02d/file%d.parquet", 10+f, f)
			mock.putFile(key, data)
			s.manifest.AddFile(fmt.Sprintf("dt=2026-05-28/hour=%02d", 10+f), manifest.FileInfo{
				Key: key, Size: int64(len(data)),
				MinTimeNs: baseTime.Add(time.Duration(f)*time.Hour - time.Minute).UnixNano(),
				MaxTimeNs: baseTime.Add(time.Duration(f)*time.Hour + time.Minute).UnixNano(),
			})
		}
		q := mustParseQueryWithTimeB(b, `*`, baseTime.Add(-time.Hour).UnixNano(), baseTime.Add(10*time.Hour).UnixNano())
		b.StartTimer()
		if _, err := s.GetFieldNames(context.Background(), nil, q); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		reqs += mock.rangeReqs.Load() + mock.fullReqs.Load()
		served += mock.bytesServed.Load()
		mock.close()
		b.StartTimer()
	}
	b.ReportMetric(float64(reqs)/float64(b.N), "s3req/op")
	b.ReportMetric(float64(served)/float64(b.N), "s3bytes/op")
}
