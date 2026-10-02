package parquets3

import (
	"context"
	"math"
	"sort"
	"strconv"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// tenantScope is the resolved read scope of a single request.
//
// It mirrors upstream VL/VT exactly: a request is answered from EXACTLY the
// tenants it names. A select request names one — VL derives it from the
// AccountID/ProjectID headers and falls back to 0:0 when they are absent, so
// "no headers" is the default tenant, not "every tenant". VL's internal select
// protocol (/internal/select/*, tenant_ids=[...]) may name several, and then
// the answer is the union of exactly those. Only a caller that validated the
// global-read credential reads every tenant.
type tenantScope struct {
	// all=true is the cross-tenant read. It is NOT reachable from a tenant
	// list; only a validated global-read caller can widen to it.
	all bool
	// account/project name the first (usually the only) tenant.
	account string
	project string
	// more names the further tenants of a multi-tenant list.
	more []tenantPair
}

// tenantPair is one tenant of a scope, in the string form object keys and the
// manifest's tenant aggregates use.
type tenantPair struct {
	account, project string
}

// resolveTenantScope maps a request's tenant list onto the read scope.
//
//	len == 1 → that tenant (the only shape a select request produces)
//	len == 0 → the default tenant 0:0, matching VL's no-headers fallback
//	len  > 1 → exactly the listed tenants (VL's internal select protocol)
func resolveTenantScope(tenantIDs []logstorage.TenantID) tenantScope {
	if len(tenantIDs) == 0 {
		return tenantScope{account: "0", project: "0"}
	}
	scope := scopeForTenantID(tenantIDs[0])
	seen := map[logstorage.TenantID]bool{tenantIDs[0]: true}
	for _, t := range tenantIDs[1:] {
		if seen[t] {
			continue
		}
		seen[t] = true
		scope.more = append(scope.more, tenantPair{
			account: strconv.FormatUint(uint64(t.AccountID), 10),
			project: strconv.FormatUint(uint64(t.ProjectID), 10),
		})
	}
	return scope
}

// scopeFor resolves the read scope of a request, honouring a validated
// global-read credential. Only the select handler can put that marker on the
// context, and only after the configured credential checked out — so a plain
// request can never widen past the tenants it names.
func scopeFor(ctx context.Context, tenantIDs []logstorage.TenantID) tenantScope {
	if storage.IsGlobalRead(ctx) {
		return tenantScope{all: true}
	}
	return resolveTenantScope(tenantIDs)
}

func scopeForTenantID(t logstorage.TenantID) tenantScope {
	return tenantScope{
		account: strconv.FormatUint(uint64(t.AccountID), 10),
		project: strconv.FormatUint(uint64(t.ProjectID), 10),
	}
}

// pairs lists every tenant of a non-global scope.
func (ts tenantScope) pairs() []tenantPair {
	if ts.all {
		return nil
	}
	return append([]tenantPair{{account: ts.account, project: ts.project}}, ts.more...)
}

// single reports whether the scope names exactly one tenant.
func (ts tenantScope) single() bool {
	return !ts.all && len(ts.more) == 0
}

// isDefault reports whether the scope includes tenant 0:0, the tenant that
// also owns data written under the legacy static-prefix layout (see
// filesForScope).
func (ts tenantScope) isDefault() bool {
	for _, p := range ts.pairs() {
		if p.account == "0" && p.project == "0" {
			return true
		}
	}
	return false
}

func (ts tenantScope) String() string {
	if ts.all {
		return "*"
	}
	out := ts.account + ":" + ts.project
	for _, p := range ts.more {
		out += "," + p.account + ":" + p.project
	}
	return out
}

// filesForTenants is the ONLY way the read path may turn a time range into a
// list of cold-tier objects. Every query class — scan, the manifest
// timestamp-only fast path, count pushdown, field/stream enumeration, the pmeta
// catalog answers and the warm-up walks — goes through it, so a tenant can
// never be handed an object it does not own.
//
// Twin of internal/storage/parquets3/tenant_scope.go — keep the two in step.
//
// In bucket-per-tenant deployments this is also what selects the bucket: the
// client pool's BucketRouter resolves the bucket from the object KEY, so
// scoping the key list scopes the bucket. A tenant-scoped request therefore
// never issues a request against another tenant's bucket, and an unscoped
// (0:0) request only ever touches the default tenant's bucket.
func (s *Storage) filesForTenants(ctx context.Context, site string, startNs, endNs int64, tenantIDs []logstorage.TenantID) []manifest.FileInfo {
	return s.filesForScope(site, startNs, endNs, scopeFor(ctx, tenantIDs))
}

func (s *Storage) filesForScope(site string, startNs, endNs int64, scope tenantScope) []manifest.FileInfo {
	if scope.all {
		// Cross-tenant read (validated global-read caller). No narrowing, and
		// nothing for the guard to check.
		return s.manifest.GetFilesForRange(startNs, endNs)
	}

	var files []manifest.FileInfo
	for _, p := range scope.pairs() {
		files = append(files, s.manifest.GetFilesForRangeTenant(startNs, endNs, p.account, p.project)...)
	}
	if !scope.single() {
		sort.Slice(files, func(i, j int) bool { return files[i].MinTimeNs < files[j].MinTimeNs })
	}

	// Legacy layout: objects written before the tenant prefix template existed
	// (static s3.prefix=logs/) carry no tenant segment. That data was ingested
	// without tenant headers, so it belongs to 0:0 — attach it to the default
	// tenant rather than silently dropping it, and keep it away from everyone
	// else.
	if scope.isDefault() {
		if legacy := s.manifest.GetFilesForRangeUntenanted(startNs, endNs); len(legacy) > 0 {
			files = append(files, legacy...)
			sort.Slice(files, func(i, j int) bool { return files[i].MinTimeNs < files[j].MinTimeNs })
		}
	}

	return s.guardTenantFiles(site, scope, files)
}

// guardTenantFiles is the invariant check that backs every scoped read: no
// object whose key belongs to another tenant may reach a response, whatever
// produced the list. A violation means a real defect (manifest key-shape drift,
// a new call site that bypassed filesForTenants, a corrupted aggregate), so the
// offending object is dropped, counted, and logged rather than served.
func (s *Storage) guardTenantFiles(site string, scope tenantScope, files []manifest.FileInfo) []manifest.FileInfo {
	if scope.all || len(files) == 0 {
		return files
	}
	parse := s.manifest.TenantKeyParser()
	out := make([]manifest.FileInfo, 0, len(files))
	var dropped int
	for _, fi := range files {
		if tenantOwnsKey(parse, scope, fi.Key) {
			out = append(out, fi)
			continue
		}
		dropped++
		if dropped == 1 {
			logger.Errorf("tenant scope violation at %s: object %q does not belong to tenant %s; dropping it and any further offenders in this query",
				site, fi.Key, scope)
		}
	}
	if dropped > 0 {
		metrics.TenantScopeViolations.Add(site, dropped)
	}
	return out
}

// tenantOwnsKey reports whether scope is allowed to read the object at key.
func tenantOwnsKey(parse func(string) (string, string, bool), scope tenantScope, key string) bool {
	account, project, ok := parse(key)
	if !ok {
		// Untenanted (legacy) key — the default tenant's data.
		return scope.isDefault()
	}
	for _, p := range scope.pairs() {
		// An {OrgID}-shaped template has no project segment; the account
		// segment alone identifies the tenant.
		if account == p.account && (project == "" || project == p.project) {
			return true
		}
	}
	return false
}

// tenantScopeAllowsGlobalIndex reports whether the range-independent, NOT
// tenant-keyed in-RAM label index may answer this request. The index unions the
// field names and values of every object it was built from, so it may answer
// only a validated cross-tenant read, or a request for exactly one tenant that
// is the only tenant the manifest holds objects for (legacy untenanted objects
// count as 0:0). A request by any other tenant — unknown, 0:0 in a deployment
// whose only tenant is 1001:0, or a tenant list — falls through to the
// partition-keyed pmeta catalog or a scan, both scoped per object key.
func (s *Storage) tenantScopeAllowsGlobalIndex(scope tenantScope) bool {
	if scope.all {
		return true
	}
	if !scope.single() {
		return false
	}
	account, project, ok := s.manifest.SoleTenant()
	if !ok {
		return false
	}
	// Same ownership rule as tenantOwnsKey: an {OrgID}-shaped key carries no
	// project segment.
	return scope.account == account && (project == "" || scope.project == project)
}

// rowInTenantScope reports whether a buffered row (fetched over the buffer
// bridge from a peer, or read out of the local staging buffer) belongs to the
// requesting tenant. The bridge already asks peers for one tenant's rows; this
// is the second line of defence for a peer that answers without scoping.
func rowInTenantScope(scope tenantScope, accountID, projectID uint32) bool {
	if scope.all {
		return true
	}
	account := strconv.FormatUint(uint64(accountID), 10)
	project := strconv.FormatUint(uint64(projectID), 10)
	for _, p := range scope.pairs() {
		if p.account == account && p.project == project {
			return true
		}
	}
	return false
}

// filterLogRowsByTenant drops buffered log rows that do not belong to scope.
// A dropped row means a peer answered the buffer bridge without honouring the
// tenant parameters (a pre-fix build in a mixed-version fleet, or a bug), so it
// is counted the same way a stray object key is.
func filterLogRowsByTenant(scope tenantScope, site string, rows []schema.LogRow) []schema.LogRow {
	if scope.all || len(rows) == 0 {
		return rows
	}
	out := make([]schema.LogRow, 0, len(rows))
	for _, r := range rows {
		if rowInTenantScope(scope, r.AccountID, r.ProjectID) {
			out = append(out, r)
		}
	}
	reportDroppedRows(site, scope, len(rows)-len(out))
	return out
}

// filterTraceRowsByTenant is filterLogRowsByTenant for trace rows.
func filterTraceRowsByTenant(scope tenantScope, site string, rows []schema.TraceRow) []schema.TraceRow {
	if scope.all || len(rows) == 0 {
		return rows
	}
	out := make([]schema.TraceRow, 0, len(rows))
	for _, r := range rows {
		if rowInTenantScope(scope, r.AccountID, r.ProjectID) {
			out = append(out, r)
		}
	}
	reportDroppedRows(site, scope, len(rows)-len(out))
	return out
}

func reportDroppedRows(site string, scope tenantScope, dropped int) {
	if dropped <= 0 {
		return
	}
	metrics.TenantScopeViolations.Add(site, dropped)
	logger.Errorf("tenant scope violation at %s: dropped %d buffered row(s) not owned by tenant %s; a peer answered /internal/buffer/query without tenant scoping",
		site, dropped, scope)
}

// TenantIDsForRange reports the tenants that actually hold cold-tier data
// overlapping [startNs, endNs], derived from the manifest's per-tenant
// aggregates. /select/tenant_ids used to answer a hardcoded 0:0 for every
// deployment, which made a multi-tenant lakehouse look single-tenant to
// anything that enumerates tenants.
//
// Legacy (untenanted) objects are reported as 0:0, matching where the read path
// serves them from.
func (s *Storage) TenantIDsForRange(startNs, endNs int64) []logstorage.TenantID {
	if !s.manifest.HasDataForRange(startNs, endNs) {
		return nil
	}
	seen := make(map[logstorage.TenantID]struct{})
	out := make([]logstorage.TenantID, 0, 4)
	add := func(t logstorage.TenantID) {
		if _, ok := seen[t]; ok {
			return
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	for _, ts := range s.manifest.TenantSummariesInWindow(startNs, endNs) {
		account, err := strconv.ParseUint(ts.AccountID, 10, 32)
		if err != nil {
			continue
		}
		var project uint64
		if ts.ProjectID != "" {
			if project, err = strconv.ParseUint(ts.ProjectID, 10, 32); err != nil {
				continue
			}
		}
		add(logstorage.TenantID{AccountID: uint32(account), ProjectID: uint32(project)})
	}
	if len(s.manifest.GetFilesForRangeUntenanted(startNs, endNs)) > 0 {
		add(logstorage.TenantID{})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AccountID != out[j].AccountID {
			return out[i].AccountID < out[j].AccountID
		}
		return out[i].ProjectID < out[j].ProjectID
	})
	return out
}

// DataTenantAccountIDs lists the AccountIDs that hold data anywhere in time:
// cold-tier tenants from the manifest's per-tenant aggregates, plus the tenants
// of the co-located unflushed buffer. String-tenant auto-registration never
// hands out one of these IDs, so a new OrgID cannot land on an int tenant that
// already has data.
func (s *Storage) DataTenantAccountIDs() []uint32 {
	seen := make(map[uint32]struct{})
	for _, t := range s.TenantIDsForRange(0, math.MaxInt64) {
		seen[t.AccountID] = struct{}{}
	}
	s.bufferTenantAccountIDs(seen)
	out := make([]uint32, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	return out
}
