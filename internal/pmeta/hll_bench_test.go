package pmeta

import (
	"fmt"
	"testing"
)

func BenchmarkHLLAdd(b *testing.B) {
	vals := make([]string, 4096)
	for i := range vals {
		vals[i] = fmt.Sprintf("trace-%016x", i)
	}
	h := newHLL(14)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.add(vals[i&4095])
	}
}

func BenchmarkHLLEstimate(b *testing.B) {
	h := newHLL(14)
	for i := 0; i < 100000; i++ {
		h.add(fmt.Sprintf("v-%d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.estimate()
	}
}

func BenchmarkHLLMerge(b *testing.B) {
	a, c := newHLL(14), newHLL(14)
	for i := 0; i < 50000; i++ {
		a.add(fmt.Sprintf("a-%d", i))
		c.add(fmt.Sprintf("c-%d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := newHLL(14)
		_ = dst.merge(a)
		_ = dst.merge(c)
	}
}

func BenchmarkHLLMarshal(b *testing.B) {
	h := newHLL(14)
	for i := 0; i < 10000; i++ {
		h.add(fmt.Sprintf("v-%d", i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.MarshalBinary()
	}
}
