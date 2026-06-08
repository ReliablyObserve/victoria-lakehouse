package parquets3

import (
	"context"
	"sort"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// ExportBufferToParquet queries the Option B logstorage-native buffer over
// [startNs, endNs] for a single tenant, reconstructs the TraceRows
// (vlstorage.DataBlockToTraceRows — parity-proven against the legacy insert
// path), sorts them by timestamp exactly like the legacy flush
// (flushTracePartition), and writes them to a Parquet object via the SAME
// writer + trace-index builder the legacy path uses (writeTracesParquet →
// computeTraceIndex).
//
// This is the read+write core of P5's "Parquet from the buffer via exported
// RunQuery". It performs NO S3 write and touches NO manifest — the caller layers
// the shadow-prefix upload + comparison on top, and the legacy []TraceRow path
// stays the authoritative Parquet producer until the cutover passes live shadow
// parity. Returns (nil,0,0,nil) when the window holds no rows for the tenant.
func ExportBufferToParquet(ctx context.Context, store LocalBuffer, tenant logstorage.TenantID, startNs, endNs int64, rowGroupSize, compressionLevel int) (data []byte, rawBytes int64, rowCount int, err error) {
	q, perr := logstorage.ParseQueryAtTimestamp("*", endNs)
	if perr != nil {
		return nil, 0, 0, perr
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), startNs, endNs)
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)

	var mu sync.Mutex
	var rows []schema.TraceRow
	if rerr := store.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		rs := vlstorage.DataBlockToTraceRows(db, tenant)
		mu.Lock()
		rows = append(rows, rs...)
		mu.Unlock()
	}); rerr != nil {
		return nil, 0, 0, rerr
	}
	if len(rows) == 0 {
		return nil, 0, 0, nil
	}

	// Same ordering the legacy flush applies, so MinTimeNs/MaxTimeNs and the
	// trace-index layout match.
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano
	})

	fr, werr := writeTracesParquet(rows, rowGroupSize, compressionLevel)
	if werr != nil {
		return nil, 0, 0, werr
	}
	return fr.Data, fr.RawBytes, len(rows), nil
}
