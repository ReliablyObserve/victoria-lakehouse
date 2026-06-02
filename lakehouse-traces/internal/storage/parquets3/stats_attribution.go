package parquets3

import (
	"sort"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// tenantKey identifies a tenant within a single flushed batch.
type tenantKey struct {
	AccountID uint32
	ProjectID uint32
}

// logTenantGroup holds a slice of rows from one tenant carved out of a
// mixed-tenant partition buffer. Order within the slice preserves the
// input order so the writer's earlier timestamp sort is not undone.
type logTenantGroup struct {
	AccountID uint32
	ProjectID uint32
	Rows      []schema.LogRow
}

// traceTenantGroup mirrors logTenantGroup for trace rows.
type traceTenantGroup struct {
	AccountID uint32
	ProjectID uint32
	Rows      []schema.TraceRow
}

// groupLogRowsByTenant returns rows partitioned by (AccountID, ProjectID).
// Fast path: a single-tenant batch is returned as one group sharing the
// caller's slice (no copy). Multi-tenant batches allocate one slice per
// tenant. Group order is deterministic (sorted by AccountID, then ProjectID)
// so flush ordering is stable across runs.
func groupLogRowsByTenant(rows []schema.LogRow) []logTenantGroup {
	if len(rows) == 0 {
		return nil
	}
	if singleTenantLogRows(rows) {
		return []logTenantGroup{{AccountID: rows[0].AccountID, ProjectID: rows[0].ProjectID, Rows: rows}}
	}
	by := make(map[tenantKey][]schema.LogRow)
	for i := range rows {
		k := tenantKey{rows[i].AccountID, rows[i].ProjectID}
		by[k] = append(by[k], rows[i])
	}
	out := make([]logTenantGroup, 0, len(by))
	for k, rs := range by {
		out = append(out, logTenantGroup{AccountID: k.AccountID, ProjectID: k.ProjectID, Rows: rs})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AccountID != out[j].AccountID {
			return out[i].AccountID < out[j].AccountID
		}
		return out[i].ProjectID < out[j].ProjectID
	})
	return out
}

// groupTraceRowsByTenant mirrors groupLogRowsByTenant for trace rows.
func groupTraceRowsByTenant(rows []schema.TraceRow) []traceTenantGroup {
	if len(rows) == 0 {
		return nil
	}
	if singleTenantTraceRows(rows) {
		return []traceTenantGroup{{AccountID: rows[0].AccountID, ProjectID: rows[0].ProjectID, Rows: rows}}
	}
	by := make(map[tenantKey][]schema.TraceRow)
	for i := range rows {
		k := tenantKey{rows[i].AccountID, rows[i].ProjectID}
		by[k] = append(by[k], rows[i])
	}
	out := make([]traceTenantGroup, 0, len(by))
	for k, rs := range by {
		out = append(out, traceTenantGroup{AccountID: k.AccountID, ProjectID: k.ProjectID, Rows: rs})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AccountID != out[j].AccountID {
			return out[i].AccountID < out[j].AccountID
		}
		return out[i].ProjectID < out[j].ProjectID
	})
	return out
}

func singleTenantLogRows(rows []schema.LogRow) bool {
	a, p := rows[0].AccountID, rows[0].ProjectID
	for i := 1; i < len(rows); i++ {
		if rows[i].AccountID != a || rows[i].ProjectID != p {
			return false
		}
	}
	return true
}

func singleTenantTraceRows(rows []schema.TraceRow) bool {
	a, p := rows[0].AccountID, rows[0].ProjectID
	for i := 1; i < len(rows); i++ {
		if rows[i].AccountID != a || rows[i].ProjectID != p {
			return false
		}
	}
	return true
}
