package parquets3

import (
	"context"
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
// It mirrors upstream VL/VT exactly: a select request carries EXACTLY ONE
// tenant. VL derives it from the AccountID/ProjectID headers and falls back to
// 0:0 when they are absent, so "no headers" is the default tenant — not
// "every tenant". A scope covering more than one tenant is only ever produced
// by a caller that already validated the global-read credential.
type tenantScope struct {
	// all=true is the cross-tenant read. It is NOT reachable from a plain
	// select request; only a validated global-read caller can widen to it.
	all     bool
	account string
	project string
}

// resolveTenantScope maps a request's tenant list onto the read scope.
//
//	len == 1 → that tenant (the only shape a select request produces)
//	len == 0 → the default tenant 0:0, matching VL's no-headers fallback
//	len  > 1 → cross-tenant, which only a validated global-read path builds
func resolveTenantScope(tenantIDs []logstorage.TenantID) tenantScope {
	switch len(tenantIDs) {
	case 0:
		return tenantScope{account: "0", project: "0"}
	case 1:
		return scopeForTenantID(tenantIDs[0])
	default:
		return tenantScope{all: true}
	}
}

// scopeFor resolves the read scope of a request, honouring a validated
// global-read credential. Only the select handler can put that marker on the
// context, and only after the configured credential checked out — so a plain
// request can never widen past its own tenant, whatever tenant list it carries.
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

// isDefault reports whether this is tenant 0:0, the scope that also owns data
// written under the legacy static-prefix layout (see filesForScope).
func (ts tenantScope) isDefault() bool {
	return !ts.all && ts.account == "0" && ts.project == "0"
}

func (ts tenantScope) String() string {
	if ts.all {
		return "*"
	}
	return ts.account + ":" + ts.project
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

	files := s.manifest.GetFilesForRangeTenant(startNs, endNs, scope.account, scope.project)

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
	if account != scope.account {
		return false
	}
	// An {OrgID}-shaped template has no project segment; the account segment
	// alone identifies the tenant.
	return project == "" || project == scope.project
}

// tenantScopeAllowsGlobalIndex reports whether the range-independent, NOT
// tenant-keyed in-RAM label index may answer this request. It can only do so
// when the manifest holds at most one tenant scope (single-tenant deployment),
// because the index unions every tenant's field names and values. Multi-tenant
// deployments fall through to the partition-keyed pmeta catalog or a scan,
// both of which are scoped per file key.
func (s *Storage) tenantScopeAllowsGlobalIndex(scope tenantScope) bool {
	if scope.all {
		return true
	}
	return s.manifest.TenantScopeCount() <= 1
}

// rowInTenantScope reports whether a buffered row (fetched over the buffer
// bridge from a peer, or read out of the local staging buffer) belongs to the
// requesting tenant. The bridge already asks peers for one tenant's rows; this
// is the second line of defence for a peer that answers without scoping.
func rowInTenantScope(scope tenantScope, accountID, projectID uint32) bool {
	if scope.all {
		return true
	}
	return scope.account == strconv.FormatUint(uint64(accountID), 10) &&
		scope.project == strconv.FormatUint(uint64(projectID), 10)
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
