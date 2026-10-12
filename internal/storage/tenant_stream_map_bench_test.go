package storage

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// One stream for each of 100k tenants (a batch cannot carry more tenants than
// rows): the per-batch memory of TenantStreamMap against a pair-keyed map.
func BenchmarkTenantStreamMap_ManyTenants(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var m TenantStreamMap[bool]
		for a := uint32(0); a < 100000; a++ {
			m.Set(logstorage.TenantID{AccountID: a}, 42, true)
		}
	}
}

func BenchmarkPairKeyedMap_ManyTenants(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := map[pairKey]bool{}
		for a := uint32(0); a < 100000; a++ {
			m[pairKey{logstorage.TenantID{AccountID: a}, 42}] = true
		}
	}
}
