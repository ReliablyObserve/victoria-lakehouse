package parquets3

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
)

func ingestLogAt(t *testing.T, st *membuffer.Store, tenant logstorage.TenantID, startNs, endNs int64, n int) {
	t.Helper()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	step := (endNs - startNs) / int64(n)
	for i := 0; i < n; i++ {
		ts := startNs + int64(i)*step
		lr.MustAdd(tenant, ts, []logstorage.Field{
			{Name: "service.name", Value: "api-gateway"},
			{Name: "_msg", Value: fmt.Sprintf("m%d-%d", startNs, i)},
		}, 1)
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}
