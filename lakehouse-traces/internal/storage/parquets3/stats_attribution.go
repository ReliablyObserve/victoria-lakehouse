package parquets3

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// tenantKey identifies a tenant within a single flushed batch.
type tenantKey struct {
	AccountID uint32
	ProjectID uint32
}

// attributeLogStats emits one StatsCallback invocation per distinct
// tenant in rows, with compressed/raw bytes apportioned by that
// tenant's row share. Single-tenant batches produce a single call.
func attributeLogStats(cb StatsCallback, rows []schema.LogRow, compressed, raw int64) {
	if len(rows) == 0 {
		return
	}
	counts := make(map[tenantKey]int64, 1)
	for i := range rows {
		k := tenantKey{rows[i].AccountID, rows[i].ProjectID}
		counts[k]++
	}
	emitTenantStats(cb, counts, compressed, raw, int64(len(rows)))
}

// attributeTraceStats mirrors attributeLogStats for trace rows.
func attributeTraceStats(cb StatsCallback, rows []schema.TraceRow, compressed, raw int64) {
	if len(rows) == 0 {
		return
	}
	counts := make(map[tenantKey]int64, 1)
	for i := range rows {
		k := tenantKey{rows[i].AccountID, rows[i].ProjectID}
		counts[k]++
	}
	emitTenantStats(cb, counts, compressed, raw, int64(len(rows)))
}

func emitTenantStats(cb StatsCallback, counts map[tenantKey]int64, compressed, raw, total int64) {
	// Fast path: one tenant — emit exact totals without rounding drift.
	if len(counts) == 1 {
		for k, n := range counts {
			cb(k.AccountID, k.ProjectID, compressed, raw, n, "STANDARD")
		}
		return
	}
	// Multi-tenant: apportion bytes by row share. Accumulate the last
	// tenant's bytes from the remainder so totals match exactly.
	var distributed int64
	var distributedRaw int64
	var lastKey tenantKey
	var lastRows int64
	for k, n := range counts {
		lastKey = k
		lastRows = n
	}
	for k, n := range counts {
		if k == lastKey {
			continue
		}
		c := compressed * n / total
		r := raw * n / total
		cb(k.AccountID, k.ProjectID, c, r, n, "STANDARD")
		distributed += c
		distributedRaw += r
	}
	cb(lastKey.AccountID, lastKey.ProjectID, compressed-distributed, raw-distributedRaw, lastRows, "STANDARD")
}
