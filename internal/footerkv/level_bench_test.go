package footerkv

import (
	"fmt"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// BenchmarkLevels compares the zstd levels EncodeCompressed could use on a
// payload shaped like a trace index (hex trace IDs plus nanosecond times). The
// level the package uses was chosen from this benchmark.
func BenchmarkLevels(b *testing.B) {
	for _, lv := range []zstd.EncoderLevel{zstd.SpeedFastest, zstd.SpeedDefault, zstd.SpeedBetterCompression} {
		enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(lv), zstd.WithEncoderConcurrency(1))
		raw := indexLikePayload(9500)
		b.Run(fmt.Sprintf("level=%s", lv), func(b *testing.B) {
			var out []byte
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				out = enc.EncodeAll(raw, nil)
			}
			b.ReportMetric(float64(len(out))/float64(len(raw)), "ratio")
		})
	}
}
