package compaction

import "testing"

// BenchmarkWriteLogs is the compaction (and delete rewrite) write path of a logs
// object: 20k rows, 10k-row groups, zstd 3. Since the output carries the body
// token blooms a flush writes (#344), its time and object-bytes are the measured
// cost of that fix; BenchmarkWriteLogsParquet in internal/storage/parquets3 is
// the flush side of the same encoding.
func BenchmarkWriteLogs(b *testing.B) {
	rows := benchMergeRows(0, 20000, false)
	b.ReportAllocs()
	b.ResetTimer()
	var size int
	for i := 0; i < b.N; i++ {
		data, err := WriteLogs(rows, 10000, 3)
		if err != nil {
			b.Fatal(err)
		}
		size = len(data)
	}
	b.ReportMetric(float64(size), "object-bytes")
}
