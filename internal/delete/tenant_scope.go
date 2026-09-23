package delete

import (
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// TenantRef names one tenant a tombstone acts on, in the numeric form VL and VT
// use for tenants (the AccountID / ProjectID request headers, the tenant_ids of
// the cluster delete protocol, and the {AccountID}/{ProjectID} segments of an
// object key).
type TenantRef struct {
	AccountID uint32
	ProjectID uint32
}

// String is the "account:project" form the rest of the codebase prints tenants
// in.
func (t TenantRef) String() string {
	return strconv.FormatUint(uint64(t.AccountID), 10) + ":" + strconv.FormatUint(uint64(t.ProjectID), 10)
}

// NormalizeTenants returns tenants sorted and without duplicates, so two records
// naming the same tenants compare and persist identically.
func NormalizeTenants(tenants []TenantRef) []TenantRef {
	if len(tenants) == 0 {
		return nil
	}
	out := append([]TenantRef(nil), tenants...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].AccountID != out[j].AccountID {
			return out[i].AccountID < out[j].AccountID
		}
		return out[i].ProjectID < out[j].ProjectID
	})
	n := 1
	for i := 1; i < len(out); i++ {
		if out[i] != out[n-1] {
			out[n] = out[i]
			n++
		}
	}
	return out[:n]
}

// KeyTenantFunc maps an object key to the tenant segments it carries.
// ok=false means the key has no tenant segment — the legacy static-prefix
// layout, whose data belongs to the default tenant 0:0. project is empty for an
// {OrgID}-shaped prefix template, where the account segment alone names the
// tenant. It is the shape of manifest.Manifest.TenantKeyParser().
type KeyTenantFunc func(key string) (account, project string, ok bool)

// DefaultKeyTenant parses the default {AccountID}/{ProjectID}/... key layout.
// Callers that hold the manifest pass its TenantKeyParser instead, which also
// knows an {OrgID}-shaped template.
func DefaultKeyTenant(key string) (string, string, bool) {
	acc, proj, ok := parseTenantFromKey(key)
	if !ok {
		return "", "", false
	}
	return strconv.FormatUint(uint64(acc), 10), strconv.FormatUint(uint64(proj), 10), true
}

// KeyTenantParserOf returns the tenant key parser a manifest provides (the
// manifest's TenantKeyParser, which knows the configured prefix template), or
// DefaultKeyTenant when it provides none.
func KeyTenantParserOf(m any) KeyTenantFunc {
	if p, ok := m.(interface {
		TenantKeyParser() func(key string) (account, project string, ok bool)
	}); ok {
		return p.TenantKeyParser()
	}
	return DefaultKeyTenant
}

// AppliesToTenant reports whether the tombstone acts on rows of the tenant
// accountID:projectID. A tombstone acts only on the tenants it names; a record
// naming none is invalid (Validate) and acts on nothing.
func (t *Tombstone) AppliesToTenant(accountID, projectID uint32) bool {
	for _, tn := range t.Tenants {
		if tn.AccountID == accountID && tn.ProjectID == projectID {
			return true
		}
	}
	return false
}

// AppliesToKey reports whether the tombstone acts on the rows of the object at
// key. The tenant is read from the key the same way the read path attributes an
// object to a tenant (see tenantOwnsKey in internal/storage/parquets3): an
// untenanted key is the default tenant's data, and a key without a project
// segment matches on the account alone.
func (t *Tombstone) AppliesToKey(parse KeyTenantFunc, key string) bool {
	return anyTenantOwnsKey(t.Tenants, parse, key)
}

// KeyBelongsTo reports whether the object at key is tenant's data, attributed
// the way the read path attributes objects (see AppliesToKey).
func KeyBelongsTo(parse KeyTenantFunc, key string, tenant TenantRef) bool {
	return anyTenantOwnsKey([]TenantRef{tenant}, parse, key)
}

func anyTenantOwnsKey(tenants []TenantRef, parse KeyTenantFunc, key string) bool {
	if parse == nil {
		parse = DefaultKeyTenant
	}
	account, project, ok := parse(key)
	for _, tn := range tenants {
		if !ok {
			if tn.AccountID == 0 && tn.ProjectID == 0 {
				return true // an untenanted legacy key is the default tenant's
			}
			continue
		}
		if account != strconv.FormatUint(uint64(tn.AccountID), 10) {
			continue
		}
		if project == "" || project == strconv.FormatUint(uint64(tn.ProjectID), 10) {
			return true
		}
	}
	return false
}

// ScopedExactlyTo reports whether the tombstone acts on exactly one tenant, and
// that tenant is accountID:projectID. It is what makes a tombstone a tenant's
// own: a tenant caller of the delete API may see and remove only these. A
// tombstone spanning several tenants (a cluster delete task) belongs to the
// operator, because removing it would un-delete other tenants' rows.
func (t *Tombstone) ScopedExactlyTo(accountID, projectID uint32) bool {
	return len(t.Tenants) == 1 && t.Tenants[0].AccountID == accountID && t.Tenants[0].ProjectID == projectID
}

// ForTenant keeps the tombstones that act on accountID:projectID. The input is
// not modified.
func ForTenant(tss []Tombstone, accountID, projectID uint32) []Tombstone {
	var out []Tombstone
	for i := range tss {
		if tss[i].AppliesToTenant(accountID, projectID) {
			out = append(out, tss[i])
		}
	}
	return out
}

// ForKey keeps the tombstones that act on the object at key. The input is not
// modified.
func ForKey(tss []Tombstone, parse KeyTenantFunc, key string) []Tombstone {
	var out []Tombstone
	for i := range tss {
		if tss[i].AppliesToKey(parse, key) {
			out = append(out, tss[i])
		}
	}
	return out
}

// AccountOnlyKeys reports whether v (a manifest, or a storage over one) keys
// objects by account alone — the {OrgID} prefix template, with no project
// segment. False when v cannot tell.
func AccountOnlyKeys(v any) bool {
	l, ok := v.(interface{ AccountOnlyTenantKeys() bool })
	return ok && l.AccountOnlyTenantKeys()
}

// ErrProjectNotInKeyLayout refuses a delete for a non-zero ProjectID where
// object keys carry the account alone: the rows of every project of the
// account share one key space, so a tombstone scoped to one project could not
// be applied to that project only.
var ErrProjectNotInKeyLayout = errors.New("object keys carry only the account ({OrgID} prefix template), so a delete cannot be scoped to a ProjectID other than 0")

// CheckTenantsForKeyLayout refuses tenants a tombstone could not be scoped to
// honestly under the object key layout.
func CheckTenantsForKeyLayout(tenants []TenantRef, accountOnlyKeys bool) error {
	if !accountOnlyKeys {
		return nil
	}
	for _, tn := range tenants {
		if tn.ProjectID != 0 {
			return fmt.Errorf("tenant %s: %w", tn, ErrProjectNotInKeyLayout)
		}
	}
	return nil
}

// rejectUnscoped reports whether a restored record names no tenant, and so is
// not a tombstone: it is logged and counted
// (lakehouse_delete_startup_inconsistencies_total{kind="unscoped_tombstone"})
// and not applied. Nothing creates such a record; one can only come from a
// hand-written or corrupted object.
func rejectUnscoped(ts Tombstone, source string) bool {
	if len(ts.Tenants) > 0 {
		return false
	}
	metrics.DeleteStartupInconsistencies.Inc("unscoped_tombstone")
	logger.Errorf("tombstone restore: %s record %q names no tenant; rejected, not applied", source, ts.ID)
	return true
}

// mergeTenants combines two copies' tenant scopes for the same tombstone id.
// A scope is fixed when the delete is issued, so the copies agree; the union
// keeps the merge a union like the rest of mergeLoadedLocked.
func mergeTenants(a, b []TenantRef) []TenantRef {
	return NormalizeTenants(append(append([]TenantRef(nil), a...), b...))
}
