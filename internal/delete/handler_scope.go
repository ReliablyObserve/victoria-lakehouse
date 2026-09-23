package delete

import (
	"context"
	"fmt"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Tenant scope of the lakehouse delete API ({prefix}/delete, /estimate,
// /tombstones, /tombstone/{id}, /verify, /leftovers).
//
// A request is resolved to a tenant exactly like a select request: VL's
// GetTenantIDFromRequest over the AccountID / ProjectID headers, which the
// tenant middleware has already filled from X-Scope-OrgID (aliases) or the
// X-Scope-AccountID / X-Scope-ProjectID pair; no headers is the default tenant
// 0:0. That caller:
//
//   - creates tombstones scoped to its own tenant, over its own objects;
//   - lists, reads, verifies against and un-deletes only the tombstones that
//     are its own — scoped to exactly that tenant;
//   - estimates over, and sees leftovers of, its own objects only.
//
// A request that presents the validated global-read credential (the same one
// that widens a select request to every tenant) is the operator: it sees and
// may un-delete every tombstone — including records from earlier releases that
// carry no tenant scope and act on every tenant, and tombstones a cluster
// delete task scoped to several tenants — and the instance-wide leftovers. Its
// deletes are still scoped to the tenant in its headers: no request creates an
// instance-wide tombstone.

// HandlerOption configures a Handler.
type HandlerOption func(*Handler)

// WithGlobalReadAuthorizer installs the validator of the global-read
// credential (tenant.GlobalReadAuth.Authorize, when a credential is
// configured). Without one no request is the operator.
func WithGlobalReadAuthorizer(authorize func(*http.Request) bool) HandlerOption {
	return func(h *Handler) { h.globalRead = authorize }
}

// deleteCaller is who a delete API request acts for.
type deleteCaller struct {
	tenant TenantRef
	// global: the request proved the global-read credential.
	global bool
	// none: the tenant headers did not parse; the caller sees nothing.
	none bool
}

type taskCallerKey struct{}

// ScopeTaskRequests puts the caller of a public delete-API request
// (/delete/run_task, /delete/stop_task, /delete/active_tasks) on its context,
// resolved exactly like a lakehouse delete-API caller, so the storage calls
// upstream's handler makes (DeleteStopTask, DeleteActiveTasks) act for that
// tenant only. It never answers itself: an unparseable tenant is left to
// upstream's handler (run_task answers "cannot obtain tenantID") and sees no
// task. globalRead validates the operator credential; nil means no operator.
func ScopeTaskRequests(globalRead func(*http.Request) bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, err := resolveCaller(r, globalRead)
		if err != nil {
			c = deleteCaller{none: true}
		}
		next(w, r.WithContext(context.WithValue(r.Context(), taskCallerKey{}, c)))
	}
}

// taskCallerFrom returns the public-API caller on ctx; ok=false for a request
// that carries none (the cluster delete protocol), which is not scoped.
func taskCallerFrom(ctx context.Context) (deleteCaller, bool) {
	if ctx == nil {
		return deleteCaller{}, false
	}
	c, ok := ctx.Value(taskCallerKey{}).(deleteCaller)
	return c, ok
}

// resolveCaller resolves the request's tenant the way the select path does.
func resolveCaller(r *http.Request, globalRead func(*http.Request) bool) (deleteCaller, error) {
	tid, err := logstorage.GetTenantIDFromRequest(r)
	if err != nil {
		return deleteCaller{}, fmt.Errorf("cannot obtain tenantID: %w", err)
	}
	return deleteCaller{
		tenant: TenantRef{AccountID: tid.AccountID, ProjectID: tid.ProjectID},
		global: globalRead != nil && globalRead(r),
	}, nil
}

// callerOrError resolves the caller, answering 400 when the tenant headers do
// not parse. ok=false means the response has been written.
func (h *Handler) callerOrError(w http.ResponseWriter, r *http.Request) (deleteCaller, bool) {
	c, err := resolveCaller(r, h.globalRead)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return deleteCaller{}, false
	}
	return c, true
}

// sees reports whether the caller may see and manage ts.
func (c deleteCaller) sees(ts *Tombstone) bool {
	if c.none {
		return false
	}
	return c.global || ts.ScopedExactlyTo(c.tenant.AccountID, c.tenant.ProjectID)
}

// ownsKey reports whether the object at key is the caller's (every object is
// the operator's).
func (c deleteCaller) ownsKey(parse KeyTenantFunc, key string) bool {
	return c.global || KeyBelongsTo(parse, key, c.tenant)
}

// scopeName is what the responses report as the view they were computed over.
func (c deleteCaller) scopeName() string {
	if c.global {
		return "instance"
	}
	return "tenant"
}

// keyTenant is the handler's key → tenant parser: the manifest's own when the
// ManifestQuerier provides it, else the default layout.
func (h *Handler) keyTenant() KeyTenantFunc {
	return KeyTenantParserOf(h.manifest)
}

// tenantFiles lists the objects of tenant overlapping [startNs, endNs]: what a
// delete by that tenant acts on, and so what its estimate describes — for the
// operator too, whose deletes are scoped to the tenant in its headers.
func (h *Handler) tenantFiles(tenant TenantRef, startNs, endNs int64) []FileInfo {
	files := h.manifest.GetFilesForRange(startNs, endNs)
	parse := h.keyTenant()
	out := files[:0:0]
	for _, f := range files {
		if KeyBelongsTo(parse, f.Key, tenant) {
			out = append(out, f)
		}
	}
	return out
}

// visibleTombstones keeps the tombstones the caller may see.
func (c deleteCaller) visibleTombstones(tss []Tombstone) []Tombstone {
	out := make([]Tombstone, 0, len(tss))
	for i := range tss {
		if c.sees(&tss[i]) {
			out = append(out, tss[i])
		}
	}
	return out
}
