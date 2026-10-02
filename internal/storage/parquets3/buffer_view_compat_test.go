package parquets3

import (
	"context"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// queryBufferBridge serves the insert buffer of a query the way RunQuery does
// (openBufferView + serveBufferView), for tests that exercise the buffer side
// alone. The watermark argument of the earlier read path is ignored: there is
// no time watermark any more.
func (s *Storage) queryBufferBridge(ctx context.Context, startNs, endNs int64, maxRows int64, rowsEmitted *atomic.Int64, _ any, q *logstorage.Query, tenantIDs []logstorage.TenantID, writeBlock logstorage.WriteDataBlockFunc) {
	v := s.openBufferView(ctx, startNs, endNs, tenantIDs)
	defer v.release()
	s.serveBufferView(ctx, v, startNs, endNs, maxRows, rowsEmitted, q, tenantIDs, uniformSink(writeBlock))
}
