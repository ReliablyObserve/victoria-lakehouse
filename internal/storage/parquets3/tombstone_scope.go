package parquets3

import (
	"strconv"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Tenant-scoped tombstones on the read path.
//
// A tombstone names the tenants whose rows it hides (delete.Tombstone.Tenants).
// The read
// path attributes rows to tenants the same way it selects objects: by the
// object's key (filesForScope), or by the tenant a buffered row carries. Every
// place that applies a tombstone first narrows the list to the tombstones that
// act on the tenant of what it is looking at, so tenant A's delete can never
// hide, skip a fast path for, or rewrite tenant B's rows.
//
// Twin of lakehouse-traces/internal/storage/parquets3/tombstone_scope.go — keep
// the two identical.

// keyTenantFunc aliases delete.KeyTenantFunc for files that call the builtin
// delete() (see the tombstone alias in fields_tombstones.go).
type keyTenantFunc = delete.KeyTenantFunc

// scopeTombstones returns the tombstones overlapping [startNs, endNs] that act
// on at least one tenant of scope, or nil. It is what gates the metadata-only
// fast paths: a tombstone of another tenant cannot change this request's
// answer, so it must not cost this request its fast path either.
func (s *Storage) scopeTombstones(scope tenantScope, startNs, endNs int64) []tombstone {
	if s.tombstones == nil {
		return nil
	}
	tss := s.tombstones.ForRange(startNs, endNs)
	if len(tss) == 0 {
		return nil
	}
	if scope.all {
		return tss
	}
	accountOnly := s.AccountOnlyTenantKeys()
	out := tss[:0]
	for i := range tss {
		if tombstoneActsOnScope(&tss[i], scope, accountOnly) {
			out = append(out, tss[i])
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// tombstoneActsOnScope reports whether ts acts on any tenant of scope. A
// non-global scope's tenants come from numeric VL tenant ids, so they parse.
//
// accountOnly is the {OrgID} key layout, where an object belongs to every
// project of its account (tenantOwnsKey) and a tombstone is always scoped to
// ProjectID 0 (delete.CheckTenantsForKeyLayout): a request of any project of
// the account then reads the account's objects, so it gets the account's
// tombstones too.
func tombstoneActsOnScope(ts *tombstone, scope tenantScope, accountOnly bool) bool {
	if scope.all {
		return true
	}
	for _, p := range scope.pairs() {
		account, err := strconv.ParseUint(p.account, 10, 32)
		if err != nil {
			continue
		}
		project, err := strconv.ParseUint(p.project, 10, 32)
		if err != nil {
			continue
		}
		if accountOnly {
			project = 0
		}
		if ts.AppliesToTenant(uint32(account), uint32(project)) {
			return true
		}
	}
	return false
}

// keyTenantParser is the manifest's key → tenant parser, snapshotted once per
// request (it takes the manifest lock).
func (s *Storage) keyTenantParser() keyTenantFunc {
	if s.manifest == nil {
		return delete.DefaultKeyTenant
	}
	return s.manifest.TenantKeyParser()
}

// tombstonesForKey keeps the tombstones that act on the object at key.
func tombstonesForKey(tss []tombstone, parse keyTenantFunc, key string) []tombstone {
	return delete.ForKey(tss, parse, key)
}

// tombstoneSink hands each emission source of one query — a cold-tier object,
// a buffered tenant's rows — the write function that applies exactly the
// tombstones acting on that source's tenant.
//
// While the scope is a single tenant (every select request), or no tombstone
// overlaps, attribution cannot change the answer and every source gets the same
// function. Only a multi-tenant read (VL's internal select protocol, a
// validated global read) with a tombstone in play builds per-tenant functions,
// cached for the life of the query.
type tombstoneSink struct {
	uniform     logstorage.WriteDataBlockFunc
	perTenant   bool
	tss         []tombstone
	parse       keyTenantFunc
	accountOnly bool
	build       func([]tombstone) logstorage.WriteDataBlockFunc

	mu    sync.Mutex
	cache map[string]logstorage.WriteDataBlockFunc
}

// newTombstoneSink builds the sink for a query whose scope-filtered tombstones
// are tss. build turns a tombstone list into the query's write function.
// accountOnly is the {OrgID} key layout (see tombstoneActsOnScope).
func newTombstoneSink(scope tenantScope, tss []tombstone, parse keyTenantFunc, accountOnly bool, build func([]tombstone) logstorage.WriteDataBlockFunc) *tombstoneSink {
	sk := &tombstoneSink{uniform: build(tss)}
	if scope.single() || len(tss) == 0 {
		return sk
	}
	sk.perTenant = true
	sk.tss = tss
	sk.parse = parse
	sk.accountOnly = accountOnly
	sk.build = build
	sk.cache = make(map[string]logstorage.WriteDataBlockFunc, 2)
	return sk
}

// uniformSink is a sink that hands every source the same function — for
// callers with no tombstones to attribute.
func uniformSink(wb logstorage.WriteDataBlockFunc) *tombstoneSink {
	return &tombstoneSink{uniform: wb}
}

// forKey is the write function for rows read from the object at key.
func (sk *tombstoneSink) forKey(key string) logstorage.WriteDataBlockFunc {
	if !sk.perTenant {
		return sk.uniform
	}
	account, project, ok := sk.parse(key)
	cacheKey := "k:" + account + "/" + project
	if !ok {
		cacheKey = "k:legacy"
	}
	return sk.cached(cacheKey, func() []tombstone { return tombstonesForKey(sk.tss, sk.parse, key) })
}

// forTenant is the write function for buffered rows of tenant tid.
func (sk *tombstoneSink) forTenant(tid logstorage.TenantID) logstorage.WriteDataBlockFunc {
	if !sk.perTenant {
		return sk.uniform
	}
	project := tid.ProjectID
	if sk.accountOnly {
		project = 0
	}
	cacheKey := "t:" + strconv.FormatUint(uint64(tid.AccountID), 10) + "/" + strconv.FormatUint(uint64(project), 10)
	return sk.cached(cacheKey, func() []tombstone { return delete.ForTenant(sk.tss, tid.AccountID, project) })
}

func (sk *tombstoneSink) cached(key string, pick func() []tombstone) logstorage.WriteDataBlockFunc {
	sk.mu.Lock()
	defer sk.mu.Unlock()
	if wb, ok := sk.cache[key]; ok {
		return wb
	}
	wb := sk.build(pick())
	sk.cache[key] = wb
	return wb
}

// emitBridgeLogRows converts bridged log rows to a block and writes it. With a
// per-tenant sink the rows are grouped by the tenant each carries, so each
// tenant's rows are filtered by that tenant's tombstones only.
func (s *Storage) emitBridgeLogRows(scope tenantScope, rows []schema.LogRow, sink *tombstoneSink) {
	if len(rows) == 0 {
		return
	}
	if !sink.perTenant {
		if db := s.logRowsToDataBlock(scope, "bridge_logs", rows); db != nil && db.RowsCount() > 0 {
			sink.uniform(0, db)
		}
		return
	}
	order, groups := groupRowsByTenant(rows, func(r *schema.LogRow) logstorage.TenantID {
		return logstorage.TenantID{AccountID: r.AccountID, ProjectID: r.ProjectID}
	})
	for _, tid := range order {
		if db := s.logRowsToDataBlock(scope, "bridge_logs", groups[tid]); db != nil && db.RowsCount() > 0 {
			sink.forTenant(tid)(0, db)
		}
	}
}

// emitBridgeTraceRows is emitBridgeLogRows for spans.
func (s *Storage) emitBridgeTraceRows(scope tenantScope, rows []schema.TraceRow, sink *tombstoneSink) {
	if len(rows) == 0 {
		return
	}
	if !sink.perTenant {
		if db := s.traceRowsToDataBlock(scope, "bridge_traces", rows); db != nil && db.RowsCount() > 0 {
			sink.uniform(0, db)
		}
		return
	}
	order, groups := groupRowsByTenant(rows, func(r *schema.TraceRow) logstorage.TenantID {
		return logstorage.TenantID{AccountID: r.AccountID, ProjectID: r.ProjectID}
	})
	for _, tid := range order {
		if db := s.traceRowsToDataBlock(scope, "bridge_traces", groups[tid]); db != nil && db.RowsCount() > 0 {
			sink.forTenant(tid)(0, db)
		}
	}
}

// groupRowsByTenant splits rows by tenant, keeping first-seen tenant order and
// the row order within each tenant.
func groupRowsByTenant[R any](rows []R, tenantOf func(*R) logstorage.TenantID) ([]logstorage.TenantID, map[logstorage.TenantID][]R) {
	var order []logstorage.TenantID
	groups := make(map[logstorage.TenantID][]R, 2)
	for i := range rows {
		tid := tenantOf(&rows[i])
		if _, ok := groups[tid]; !ok {
			order = append(order, tid)
		}
		groups[tid] = append(groups[tid], rows[i])
	}
	return order, groups
}

// AccountOnlyTenantKeys reports whether object keys carry the account alone
// ({OrgID} layout), so a delete task cannot be scoped to a single project.
func (s *Storage) AccountOnlyTenantKeys() bool {
	return s.manifest != nil && s.manifest.AccountOnlyTenantKeys()
}

// TenantFileKeys lists the cold-tier objects of exactly the given tenants whose
// rows may fall in [startNs, endNs] — the same tenant-scoped listing the read
// path serves them from, legacy untenanted objects included for 0:0. It is what
// a delete task records as the tombstone's affected keys.
func (s *Storage) TenantFileKeys(tenantIDs []logstorage.TenantID, startNs, endNs int64) []string {
	if len(tenantIDs) == 0 {
		return nil
	}
	files := s.filesForScope("delete_task", startNs, endNs, resolveTenantScope(tenantIDs))
	keys := make([]string, 0, len(files))
	for i := range files {
		keys = append(keys, files[i].Key)
	}
	return keys
}
