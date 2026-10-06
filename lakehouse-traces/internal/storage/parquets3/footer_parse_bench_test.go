package parquets3

import (
	"testing"
)

// BenchmarkWholeObjectFooter is the per-file cost of the footer bookkeeping on
// the whole-object download path when the footer is ALREADY cached (every warm
// repeat of a facets-style query): "reparse" is ParseFooterFromData + Put as the
// path did before (a second footer parse over a copy of the tail, then a Put
// that replaces the identical entry); "cached" is parseObjectFor, which keeps
// the entry it has. Both include parquet.OpenFile of the downloaded bytes, the
// handle the caller reads from. Compare with
//
//	go test -run '^$' -bench WholeObjectFooter -benchmem -count=10 | benchstat
func BenchmarkWholeObjectFooter(b *testing.B) {
	data := logsObject(b, 9000, 3000)
	fc := NewFooterCache(0)
	cached, _, err := ParseFooterFromData("k", data)
	if err != nil {
		b.Fatal(err)
	}
	fc.Put("k", cached)

	b.Run("reparse", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c, f, err := ParseFooterFromData("k", data)
			if err != nil || f == nil {
				b.Fatal(err)
			}
			fc.Put("k", c)
		}
	})
	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c, f, fresh, err := parseObjectFor(fc, "k", data)
			if err != nil || f == nil {
				b.Fatal(err)
			}
			if fresh {
				fc.Put("k", c)
			}
		}
	})
}
