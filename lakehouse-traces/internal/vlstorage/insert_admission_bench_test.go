package vlstorage

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// BenchmarkInsertAdapter_Admission measures the insert adapter's admission and
// copy loop on a batch of 100 rows over 10 streams: with no cardinality gate,
// with a gate that admits every stream, and with a gate that refuses half of
// the streams (so the batch is copied). It uses only what main has too, so the
// same file measures main and the branch.
func BenchmarkInsertAdapter_Admission(b *testing.B) {
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	for i := 0; i < 100; i++ {
		lr.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, now+int64(i), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: fmt.Sprintf("app%d", i%10)}, {Name: "span_id", Value: fmt.Sprintf("%016d", i)}, {Name: "trace_id", Value: fmt.Sprintf("%032d", i)},
		}, 1)
	}
	deny := map[string]bool{}
	for i := 0; i < 10; i += 2 {
		deny[fmt.Sprintf(`{resource_attr:service.name="app%d"}`, i)] = true
	}
	for _, c := range []struct {
		name string
		gate TenantCardinalityGate
	}{
		{"no-gate", nil},
		{"gate-admits-all", fakeGate{}},
		{"gate-refuses-half", fakeGate{deny: deny}},
	} {
		b.Run(c.name, func(b *testing.B) {
			SetCardinalityGate(c.gate)
			defer SetCardinalityGate(nil)
			a := &vtInsertAdapter{buf: discardBuffer{}, dir: "/d"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a.MustAddRows(lr)
			}
		})
	}
}

// BenchmarkInsertAdapter_IntoRealSegment is the same 100-row batch added to a
// real insert-buffer segment (upstream's logstorage), the work the admission
// loop sits in front of: it gives the loop's share of one insert.
func BenchmarkInsertAdapter_IntoRealSegment(b *testing.B) {
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	for i := 0; i < 100; i++ {
		lr.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, now+int64(i), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: fmt.Sprintf("app%d", i%10)}, {Name: "span_id", Value: fmt.Sprintf("%016d", i)}, {Name: "trace_id", Value: fmt.Sprintf("%032d", i)},
		}, 1)
	}
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: filepath.Join(b.TempDir(), "buffer")})
	if err != nil {
		b.Fatal(err)
	}
	defer segs.Close()
	SetCardinalityGate(nil)
	a := &vtInsertAdapter{buf: segs, dir: "/d"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.MustAddRows(lr)
	}
}

type discardBuffer struct{}

func (discardBuffer) MustAddRows(*logstorage.LogRows) {}
func (discardBuffer) IsReadOnly() bool                { return false }
