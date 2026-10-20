package delete

import (
	"errors"
	"strings"
	"testing"
)

// Tenant-scoped tombstones: the data model, persistence and the rewrite
// scheduler. The read path's half of the invariant is in
// internal/storage/parquets3/tombstone_scope_test.go (and its twin).

func scopedSample(id string, tenants ...TenantRef) Tombstone {
	ts := sampleTombstone(id)
	ts.Tenants = tenants
	return ts
}

func TestTenantRef_StringAndNormalize(t *testing.T) {
	if got := (TenantRef{AccountID: 1001, ProjectID: 7}).String(); got != "1001:7" {
		t.Errorf("String() = %q", got)
	}
	got := NormalizeTenants([]TenantRef{{AccountID: 5}, {AccountID: 1, ProjectID: 2}, {AccountID: 5}, {AccountID: 1, ProjectID: 1}})
	want := []TenantRef{{AccountID: 1, ProjectID: 1}, {AccountID: 1, ProjectID: 2}, {AccountID: 5}}
	if len(got) != len(want) {
		t.Fatalf("NormalizeTenants = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("NormalizeTenants = %v, want %v", got, want)
		}
	}
	if NormalizeTenants(nil) != nil {
		t.Error("NormalizeTenants(nil) must stay nil")
	}
}

func TestTombstone_AppliesToTenantAndKey(t *testing.T) {
	a := scopedSample("a", TenantRef{AccountID: 1001})
	zero := scopedSample("z", TenantRef{})
	orgID := func(key string) (string, string, bool) { // {OrgID}-shaped template: no project segment
		acc, _, ok := strings.Cut(key, "/")
		if !ok || acc == "logs" {
			return "", "", false
		}
		return acc, "", true
	}

	cases := []struct {
		name  string
		ts    Tombstone
		parse KeyTenantFunc
		key   string
		want  bool
	}{
		{"own tenant key", a, nil, "1001/0/logs/dt=x/a.parquet", true},
		{"other tenant key", a, nil, "1002/0/logs/dt=x/a.parquet", false},
		{"other project of the same account", a, nil, "1001/1/logs/dt=x/a.parquet", false},
		{"untenanted key is the default tenant's", a, nil, "logs/dt=x/a.parquet", false},
		{"untenanted key under a 0:0 delete", zero, nil, "logs/dt=x/a.parquet", true},
		{"account-only template matches on the account", a, orgID, "1001/logs/dt=x/a.parquet", true},
		{"account-only template, other account", a, orgID, "1002/logs/dt=x/a.parquet", false},
		{"explicit default parser", a, DefaultKeyTenant, "1001/0/logs/dt=x/a.parquet", true},
	}
	for _, tc := range cases {
		if got := tc.ts.AppliesToKey(tc.parse, tc.key); got != tc.want {
			t.Errorf("%s: AppliesToKey(%q) = %v, want %v", tc.name, tc.key, got, tc.want)
		}
	}

	if !a.AppliesToTenant(1001, 0) || a.AppliesToTenant(1001, 1) || a.AppliesToTenant(0, 0) {
		t.Error("AppliesToTenant must match exactly the listed tenant")
	}
	if !a.ScopedExactlyTo(1001, 0) || a.ScopedExactlyTo(0, 0) {
		t.Error("ScopedExactlyTo must be true only for a single-tenant scope naming that tenant")
	}
	two := scopedSample("two", TenantRef{AccountID: 1}, TenantRef{AccountID: 2})
	if two.ScopedExactlyTo(1, 0) {
		t.Error("a two-tenant tombstone is no single tenant's own")
	}
}

// A record naming no tenant is not a tombstone: it is invalid, and should one
// reach the store anyway it acts on nothing — never on every tenant.
func TestTombstone_UnscopedIsInvalidAndActsOnNothing(t *testing.T) {
	ts := scopedSample("none")
	if err := ts.Validate(); !errors.Is(err, ErrNoTenantScope) {
		t.Fatalf("Validate = %v, want ErrNoTenantScope", err)
	}
	if ts.AppliesToTenant(0, 0) || ts.AppliesToKey(nil, "logs/dt=x/a.parquet") || ts.AppliesToKey(nil, "0/0/logs/x") {
		t.Fatal("an unscoped record must act on no tenant and no object")
	}
	if got := ForTenant([]Tombstone{ts}, 0, 0); len(got) != 0 {
		t.Fatalf("ForTenant kept an unscoped record: %v", got)
	}
	if got := ForKey([]Tombstone{ts}, nil, "logs/x"); len(got) != 0 {
		t.Fatalf("ForKey kept an unscoped record: %v", got)
	}
}

type parserManifest struct{}

func (parserManifest) TenantKeyParser() func(string) (string, string, bool) {
	return func(string) (string, string, bool) { return "77", "", true }
}

func TestKeyTenantParserOf(t *testing.T) {
	if a, _, _ := KeyTenantParserOf(parserManifest{})("anything"); a != "77" {
		t.Error("the manifest's own parser must be used when it has one")
	}
	if a, p, ok := KeyTenantParserOf(nil)("5/6/logs/x"); !ok || a != "5" || p != "6" {
		t.Error("without a manifest parser the default {AccountID}/{ProjectID} layout applies")
	}
	if _, _, ok := DefaultKeyTenant("logs/x.parquet"); ok {
		t.Error("an untenanted key must report ok=false")
	}
}

func TestForKeyForTenant(t *testing.T) {
	if ForKey(nil, nil, "k") != nil || ForTenant(nil, 0, 0) != nil {
		t.Error("empty input must give nil")
	}
	tss := []Tombstone{scopedSample("zero", TenantRef{}), scopedSample("a", TenantRef{AccountID: 1}), scopedSample("b", TenantRef{AccountID: 2})}
	if got := ForKey(tss, nil, "2/0/logs/x"); len(got) != 1 || got[0].ID != "b" {
		t.Errorf("ForKey = %v, want only tenant 2's", got)
	}
	if got := ForKey(tss, nil, "logs/x"); len(got) != 1 || got[0].ID != "zero" {
		t.Errorf("ForKey(untenanted) = %v, want only tenant 0:0's", got)
	}
	if got := ForTenant(tss, 1, 0); len(got) != 1 || got[0].ID != "a" {
		t.Errorf("ForTenant = %v, want only tenant 1's", got)
	}
}

func parsedFilterCount() int {
	n := 0
	parsedFilters.Range(func(any, any) bool { n++; return true })
	return n
}

// The filter cache holds exactly the filters of live tombstones: removing,
// un-deleting, retiring or redefining the last tombstone using a filter evicts
// it, and a filter still used by another tombstone stays.
func TestFilterCache_EvictedWithTheLastTombstoneUsingIt(t *testing.T) {
	store := NewTombstoneStore()
	mk := func(id, q string) Tombstone {
		ts := scopedSample(id, TenantRef{})
		ts.Query, ts.FilterAt, ts.Mode = q, 424242, "permanent"
		return ts
	}
	warm := func(ids ...string) {
		for _, id := range ids {
			ts, _ := store.Get(id)
			ts.Filter()
		}
	}
	store.Add(mk("a", `level:="evict-a"`))
	store.Add(mk("a2", `level:="evict-a"`)) // shares a's filter
	store.Add(mk("b", `level:="evict-b"`))
	store.Add(mk("c", `level:="evict-c"`))
	store.Add(mk("d", `level:="evict-d"`))
	warm("a", "a2", "b", "c", "d")
	base := parsedFilterCount()

	store.forget("a")
	if got := parsedFilterCount(); got != base {
		t.Fatalf("a filter still used by another tombstone was evicted: %d -> %d", base, got)
	}
	store.forget("a2")
	if got := parsedFilterCount(); got != base-1 {
		t.Fatalf("Remove of the last user: cache %d -> %d, want one fewer", base, got)
	}
	store.forget("b")
	if got := parsedFilterCount(); got != base-2 {
		t.Fatalf("forget: cache %d, want %d", got, base-2)
	}
	store.forget("c")
	if got := parsedFilterCount(); got != base-3 {
		t.Fatalf("forget: cache %d, want %d", got, base-3)
	}
	store.Add(mk("d", `level:="evict-d2"`)) // redefined under the same id
	if got := parsedFilterCount(); got != base-4 {
		t.Fatalf("redefinition: cache %d, want %d", got, base-4)
	}
}

func TestTenantScope_CloneIsDeep(t *testing.T) {
	ts := scopedSample("c", TenantRef{AccountID: 1})
	c := cloneTombstone(ts)
	c.Tenants[0].AccountID = 99
	if ts.Tenants[0].AccountID != 1 {
		t.Error("cloneTombstone shares the Tenants slice with the original")
	}
}
