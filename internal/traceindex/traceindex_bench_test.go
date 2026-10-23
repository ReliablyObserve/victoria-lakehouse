package traceindex

import (
	"fmt"
	"testing"
)

func benchEntries(n int) []Entry {
	in := make([]Entry, n)
	for i := range in {
		in[i] = Entry{TraceID: fmt.Sprintf("%032x", i*7919), Partition: uint16(i), StartNs: int64(i), EndNs: int64(i) + 1000}
	}
	return in
}

// BenchmarkMarshal and BenchmarkUnmarshal measure the codec on valid writer
// output (the 100 and 100k trace shapes of a typical flush and a large
// compaction) so a validation change shows up as a regression (#768).
func BenchmarkMarshal(b *testing.B) {
	for _, n := range []int{100, 100_000} {
		in := benchEntries(n)
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if len(Marshal(in)) == 0 {
					b.Fatal("empty")
				}
			}
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for _, n := range []int{100, 100_000} {
		raw := Marshal(benchEntries(n))
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				if _, err := Unmarshal(raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
