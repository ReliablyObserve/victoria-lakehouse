package delete

import (
	"sort"
	"strconv"
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

// Scoped reports whether the tombstone is limited to the tenants it names. A
// tombstone without tenants is a record from a release that had no tenant
// scope: it keeps acting on every tenant, exactly as it did when it was issued.
func (t *Tombstone) Scoped() bool {
	return len(t.Tenants) > 0
}

// AppliesToTenant reports whether the tombstone acts on rows of the tenant
// accountID:projectID.
func (t *Tombstone) AppliesToTenant(accountID, projectID uint32) bool {
	if !t.Scoped() {
		return true
	}
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
// untenanted legacy key is the default tenant's data, and a key without a
// project segment matches on the account alone.
func (t *Tombstone) AppliesToKey(parse KeyTenantFunc, key string) bool {
	if !t.Scoped() {
		return true
	}
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
// tombstone spanning several tenants, or a legacy instance-wide one, belongs to
// the operator, because removing it would un-delete other tenants' rows.
func (t *Tombstone) ScopedExactlyTo(accountID, projectID uint32) bool {
	return len(t.Tenants) == 1 && t.Tenants[0].AccountID == accountID && t.Tenants[0].ProjectID == projectID
}

// ForTenant keeps the tombstones that act on accountID:projectID. The input is
// not modified; when no tombstone is tenant-scoped it is returned as is.
func ForTenant(tss []Tombstone, accountID, projectID uint32) []Tombstone {
	if len(tss) == 0 {
		return nil
	}
	if !anyScoped(tss) {
		return tss
	}
	var out []Tombstone
	for i := range tss {
		if tss[i].AppliesToTenant(accountID, projectID) {
			out = append(out, tss[i])
		}
	}
	return out
}

// ForKey keeps the tombstones that act on the object at key. The input is not
// modified; when no tombstone is tenant-scoped it is returned as is.
func ForKey(tss []Tombstone, parse KeyTenantFunc, key string) []Tombstone {
	if len(tss) == 0 {
		return nil
	}
	if !anyScoped(tss) {
		return tss
	}
	var out []Tombstone
	for i := range tss {
		if tss[i].AppliesToKey(parse, key) {
			out = append(out, tss[i])
		}
	}
	return out
}

func anyScoped(tss []Tombstone) bool {
	for i := range tss {
		if tss[i].Scoped() {
			return true
		}
	}
	return false
}

// mergeTenants combines two copies' tenant scopes for the same tombstone id.
//
// A scope is fixed when the delete is issued, so two copies differ only when
// one of them lost the field — a record rewritten by a release that did not
// know it. Losing the field must never widen the delete to every tenant, so a
// scoped copy wins over an unscoped one, and two scoped copies union.
func mergeTenants(a, b []TenantRef) []TenantRef {
	switch {
	case len(a) == 0:
		return NormalizeTenants(b)
	case len(b) == 0:
		return NormalizeTenants(a)
	}
	return NormalizeTenants(append(append([]TenantRef(nil), a...), b...))
}
