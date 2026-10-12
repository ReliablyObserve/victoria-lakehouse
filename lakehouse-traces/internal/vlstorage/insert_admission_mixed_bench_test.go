package vlstorage

import (
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// BenchmarkVTInsertAdapter_MixedTenants is a batch of 100 rows over 10 streams and
// 10 tenants (the shape of a vlagent fan-in through /internal/insert or
// /insert/multitenant/native) where the gate refuses half of the streams. It
// measures the per-tenant bookkeeping a batch of one tenant never pays; the
// numbers are in docs/durability.md. Only what main has too is used, so the same
// file measures main and the branch.
func BenchmarkVTInsertAdapter_MixedTenants(b *testing.B) {
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	for i := 0; i < 100; i++ {
		lr.MustAdd(logstorage.TenantID{AccountID: uint32(i % 10), ProjectID: 2}, now+int64(i), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: fmt.Sprintf("app%d", i%10)}, {Name: "span_id", Value: fmt.Sprintf("%016d", i)}, {Name: "trace_id", Value: fmt.Sprintf("%032d", i)},
		}, 1)
	}
	deny := map[string]bool{}
	for i := 0; i < 10; i += 2 {
		deny[fmt.Sprintf(`{resource_attr:service.name="app%d"}`, i)] = true
	}
	SetCardinalityGate(fakeGate{deny: deny})
	defer SetCardinalityGate(nil)
	a := &vtInsertAdapter{buf: discardBuffer{}, dir: "/d"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.MustAddRows(lr)
	}
}
