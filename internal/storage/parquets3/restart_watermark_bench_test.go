package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// Cost of the buffer watermark on the query path (#272). The common case is
// every selected object having exact bounds: the computation must stay a
// single pass over the files. The inferred cases measure the pass that decides
// which objects could change the watermark (none are read: the manifest
// already holds exact bounds or the objects sit below the floor).
//
// Twin of lakehouse-traces/internal/storage/parquets3/restart_watermark_bench_test.go.

func rwBenchFiles(n int, inferred bool) []manifest.FileInfo {
	files := make([]manifest.FileInfo, n)
	for i := range files {
		h := rwHour.Add(-time.Duration(i%48) * time.Hour)
		fi := manifest.FileInfo{
			Key:       fmt.Sprintf("%d/0/logs/dt=%s/hour=%02d/f%d.parquet", i%4, h.Format("2006-01-02"), h.Hour(), i),
			Size:      1 << 20,
			RowCount:  1000,
			MinTimeNs: h.UnixNano() + 1,
			MaxTimeNs: h.UnixNano() + int64(30*time.Minute),
		}
		if inferred {
			fi.MinTimeNs, fi.MaxTimeNs, fi.BoundsInferred = h.UnixNano(), h.UnixNano()+int64(time.Hour)-1, true
		}
		files[i] = fi
	}
	return files
}

func BenchmarkBufferWatermarksFor(b *testing.B) {
	old := nowFn
	nowFn = func() time.Time { return rwHour.Add(10 * 24 * time.Hour) } // everything is below the retention floor
	defer func() { nowFn = old }()
	for _, n := range []int{10, 1000} {
		for _, tc := range []struct {
			name     string
			inferred bool
		}{{"exact", false}, {"inferred-below-floor", true}} {
			b.Run(fmt.Sprintf("%s/files=%d", tc.name, n), func(b *testing.B) {
				s := &Storage{manifest: manifest.New("test-bucket", "logs/"), cfg: testConfig()}
				files := rwBenchFiles(n, tc.inferred)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = s.bufferWatermarksFor(ctx, 0, files)
				}
			})
		}
	}
}

func BenchmarkWithExactBounds_AllExact(b *testing.B) {
	s := &Storage{manifest: manifest.New("test-bucket", "logs/")}
	files := rwBenchFiles(1000, false)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.withExactBounds(ctx, files)
	}
}

type rwNopBuf struct{}

func (rwNopBuf) RunQuery(*logstorage.QueryContext, logstorage.WriteDataBlockFunc) error { return nil }
func (rwNopBuf) Close()                                                                 {}

// BenchmarkRunQueryCount is a whole `stats count()` over N exact objects with a
// buffer attached: the watermark computation inside a real query.
func BenchmarkRunQueryCount(b *testing.B) {
	for _, n := range []int{10, 200} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			s := testStorage()
			for _, fi := range rwBenchFiles(n, false) {
				s.manifest.AddFile(manifest.ExtractPartition(fi.Key), fi)
			}
			s.localBuffer = rwNopBuf{}
			q, err := logstorage.ParseQuery("* | stats count() n")
			if err != nil {
				b.Fatal(err)
			}
			q.AddTimeFilter(rwHour.Add(-49*time.Hour).UnixNano(), rwHour.Add(time.Hour).UnixNano())
			ctx := storage.WithTimestampOnlyHint(context.Background())
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
