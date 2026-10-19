package parquets3

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// BenchmarkTombstoneProofSplit is the per-query cost of the proof itself: the
// split of files x tombstones into clean and dirty files, with the labels the
// proof decides on and no S3 read. "clean" tombstones name a value no file holds
// (every tombstone is evaluated for every file), "dirty" ones a value every file
// holds (the first tombstone decides), "map" ones a map attribute (no term).
func BenchmarkTombstoneProofSplit(b *testing.B) {
	s := testStorageWithS3(b, "http://127.0.0.1:1")
	for _, files := range []int{1000, 10000, 100000} {
		fis := make([]manifest.FileInfo, files)
		for i := range fis {
			fis[i] = manifest.FileInfo{
				Key:       fmt.Sprintf("logs/dt=2026-06-01/hour=10/f%06d.parquet", i),
				MinTimeNs: 1000, MaxTimeNs: 2000, RowCount: 100,
				Labels: map[string][]string{"service.name": {"alpha", "beta"}, "severity_text": {"INFO"}},
			}
		}
		for _, k := range []int{10, 100} {
			for _, kind := range []string{"clean", "dirty", "map"} {
				tss := make([]tombstone, k)
				for i := range tss {
					q := fmt.Sprintf(`service.name:="gone%d"`, i)
					switch kind {
					case "dirty":
						q = `service.name:="alpha"`
					case "map":
						q = fmt.Sprintf(`user:="u%d"`, i)
					}
					tss[i] = tombstone{ID: fmt.Sprintf("t%d", i), Query: q, StartNs: -1 << 63, EndNs: 1<<63 - 1}
				}
				sink := newTombstoneSink(resolveTenantScope(nil), tss, delete.DefaultKeyTenant, false, func([]tombstone) logstorage.WriteDataBlockFunc { return nil })
				b.Run(fmt.Sprintf("files=%d/tombstones=%d/%s", files, k, kind), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						s.newCleanLane(fis, sink, nil, nil)
					}
				})
			}
		}
	}
}
