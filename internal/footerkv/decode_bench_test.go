package footerkv

import (
	"testing"
)

// BenchmarkDecodeSteps splits the cost of reading one index-sized value into
// its base64 and zstd halves.
func BenchmarkDecodeSteps(b *testing.B) {
	raw := indexLikePayload(9500)
	plain := Encode(raw)
	comp := EncodeCompressed(raw)
	b.Run("plain-base64-only", func(b *testing.B) {
		b.SetBytes(int64(len(plain)))
		for i := 0; i < b.N; i++ {
			if _, ok := Decode(plain); !ok {
				b.Fatal()
			}
		}
	})
	b.Run("base64+zstd", func(b *testing.B) {
		b.SetBytes(int64(len(comp)))
		for i := 0; i < b.N; i++ {
			if _, ok := DecodeCompressed(comp); !ok {
				b.Fatal()
			}
		}
	})
}
