package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

func viewBenchFiles(n int) []manifest.FileInfo {
	files := make([]manifest.FileInfo, n)
	for i := range files {
		h := rwHour.Add(-time.Duration(i%48) * time.Hour)
		nonce := fmt.Sprintf("%08x%08x", uint32(h.Unix()), uint32(i%7))
		key := fmt.Sprintf("%d/0/traces/dt=%s/hour=%02d/%s-%x.parquet", i%4, h.Format("2006-01-02"), h.Hour(), nonce, i)
		if i%5 == 0 {
			key = fmt.Sprintf("%d/0/traces/dt=%s/hour=%02d/%016x.parquet", i%4, h.Format("2006-01-02"), h.Hour(), i) // a compaction output
		}
		files[i] = manifest.FileInfo{
			Key: key, Size: 1 << 20, RowCount: 1000,
			MinTimeNs: h.UnixNano() + 1, MaxTimeNs: h.UnixNano() + int64(30*time.Minute),
		}
	}
	return files
}

// BenchmarkBufferViewExclude is the cost the buffer view adds to every query:
// one pass over the selected objects, comparing the nonce of each key with the
// live segments'.
func BenchmarkBufferViewExclude(b *testing.B) {
	for _, n := range []int{10, 1000, 100000} {
		b.Run(fmt.Sprintf("files=%d/live=4", n), func(b *testing.B) {
			files := viewBenchFiles(n)
			v := &bufferView{nonces: map[string]struct{}{}}
			for i := 0; i < 4; i++ {
				v.nonces[fmt.Sprintf("%08x%08x", uint32(rwHour.Add(-time.Duration(i)*time.Hour).Unix()), uint32(i))] = struct{}{}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = v.exclude(files)
			}
		})
	}
}

// BenchmarkRunQueryCount is a whole `stats count()` over N exact objects with an
// (empty) insert buffer attached: the buffer view inside a real query.
func BenchmarkRunQueryCount(b *testing.B) {
	for _, n := range []int{10, 200} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			s := testStorage()
			for _, fi := range viewBenchFiles(n) {
				s.manifest.AddFile(manifest.ExtractPartition(fi.Key), fi)
			}
			st, err := membuffer.Open(membuffer.Config{Path: b.TempDir()})
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()
			s.localBuffer = snapshotBuffer{st.Snapshot()}
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
