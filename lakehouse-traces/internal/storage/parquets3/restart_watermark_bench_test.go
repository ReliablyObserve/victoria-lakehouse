package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// Cost of the buffer watermark on the query path (#272). The common case is
// every selected object having exact bounds: the computation must stay a
// single pass over the files. The inferred cases measure the pass that decides
// which objects could change the watermark (none are read: the manifest
// already holds exact bounds or the objects sit below the floor).
//
// Twin of internal/storage/parquets3/restart_watermark_bench_test.go (the logs module).

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
