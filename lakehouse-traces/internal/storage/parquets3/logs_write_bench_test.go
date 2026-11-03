package parquets3

import "testing"

// BenchmarkWriteLogsParquet is the flush write path of a logs object (10k rows,
// 10k-row groups, zstd 3 as shipped); object-bytes is the output size. Together
// with BenchmarkWriteTracesParquet it is the allocation and time guard of the
// shared writer: moving the encoding must not make a flush slower or heavier.
func BenchmarkWriteLogsParquet(b *testing.B) {
	rows := goldenLogRows(10000)
	b.ReportAllocs()
	b.ResetTimer()
	var size int
	for i := 0; i < b.N; i++ {
		res, err := writeLogsParquet(rows, 10000, 3)
		if err != nil {
			b.Fatal(err)
		}
		size = len(res.Data)
	}
	b.ReportMetric(float64(size), "object-bytes")
}
