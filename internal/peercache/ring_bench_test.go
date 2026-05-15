package peercache

import (
	"fmt"
	"testing"
)

func BenchmarkRing_Lookup(b *testing.B) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "peer1:9428", "peer2:9428", "peer3:9428"})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Lookup(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkRing_Lookup_10Peers(b *testing.B) {
	r := NewRing("self:9428", 150)
	peers := make([]string, 10)
	for i := range peers {
		peers[i] = fmt.Sprintf("peer%d:9428", i)
	}
	r.Set(peers)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Lookup(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkRing_Set(b *testing.B) {
	r := NewRing("self:9428", 150)
	peers := []string{"self:9428", "a:9428", "b:9428", "c:9428"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Set(peers)
	}
}

func BenchmarkRing_Members(b *testing.B) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "a:9428", "b:9428", "c:9428"})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Members()
	}
}
