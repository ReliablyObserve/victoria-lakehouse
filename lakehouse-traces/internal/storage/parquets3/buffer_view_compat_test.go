package parquets3

import (
	"context"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// queryBufferBridge serves the insert buffer of a query the way RunQuery does
// (openBufferView + serveBufferView), for tests that exercise the buffer side
// alone. The third bound argument of the earlier read path is ignored: there is
// no time watermark any more.
func (s *Storage) queryBufferBridge(ctx context.Context, startNs, endNs int64, _ any, q *logstorage.Query, tenantIDs []logstorage.TenantID, writeBlock logstorage.WriteDataBlockFunc) {
	v := s.openBufferView(ctx, startNs, endNs, tenantIDs)
	defer v.release()
	var emitted atomic.Int64
	s.serveBufferView(ctx, v, startNs, endNs, 0, &emitted, q, tenantIDs, uniformSink(writeBlock))
}
