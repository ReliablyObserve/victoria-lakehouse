package delete

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
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

// Persistence round-trips the scope through both durable copies.
func TestTenantScope_PersistsThroughDiskAndS3(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()
	cfg := PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}

	store := NewTombstoneStore()
	store.EnablePersistence(cfg)
	store.Add(scopedSample("ts-a", TenantRef{AccountID: 1001}, TenantRef{AccountID: 2002, ProjectID: 7}))
	if n := store.FlushPending(context.Background()); n != 0 {
		t.Fatalf("%d records still owed to S3", n)
	}

	// Crash: a fresh process restores from the disk copy alone …
	fromDisk := NewTombstoneStore()
	if err := fromDisk.LoadFromDisk(dir); err != nil {
		t.Fatalf("LoadFromDisk: %v", err)
	}
	// … and one that lost its disk, from S3 alone.
	fromS3 := NewTombstoneStore()
	if err := fromS3.LoadFromS3(context.Background(), pool, "", "logs/"); err != nil {
		t.Fatalf("LoadFromS3: %v", err)
	}
	for name, st := range map[string]*TombstoneStore{"disk": fromDisk, "s3": fromS3} {
		got, ok := st.Get("ts-a")
		if !ok || len(got.Tenants) != 2 || !got.AppliesToTenant(2002, 7) || got.AppliesToTenant(0, 0) {
			t.Errorf("%s restore lost the tenant scope: %+v", name, got.Tenants)
		}
	}
	data, _ := pool.Get(TombstonePrefix("logs/") + "ts-a.json")
	if !strings.Contains(string(data), `"Tenants"`) {
		t.Errorf("a record must persist its tenants: %s", data)
	}
}

// A persisted record naming no tenant (only a hand-written or corrupted object
// can be one) is rejected on restore from either copy: counted, logged, not
// applied — and it does not fail the restore of the valid records next to it.
func TestTenantScope_UnscopedRecordIsRejectedOnRestore(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()

	disk := NewTombstoneStore()
	disk.Add(scopedSample("ts-disk-none")) // Add does not validate; the loader must
	disk.Add(scopedSample("ts-ok", TenantRef{AccountID: 5}))
	if err := disk.PersistToDisk(dir); err != nil {
		t.Fatalf("PersistToDisk: %v", err)
	}
	storeTombstoneInS3(t, pool, "logs/", scopedSample("ts-s3-none"))

	before := metrics.DeleteStartupInconsistencies.Get("unscoped_tombstone")
	store := NewTombstoneStore()
	if _, err := store.Restore(context.Background(), PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}); err != nil {
		t.Fatalf("Restore must not fail over rejected records: %v", err)
	}
	if store.S3RestorePending() {
		t.Fatal("a rejected record must not leave the S3 restore pending")
	}
	for _, id := range []string{"ts-disk-none", "ts-s3-none"} {
		if _, ok := store.Get(id); ok {
			t.Errorf("unscoped record %s was applied", id)
		}
	}
	if ts, ok := store.Get("ts-ok"); !ok || !ts.ScopedExactlyTo(5, 0) {
		t.Errorf("the valid record next to them was lost: %+v", ts)
	}
	if got := metrics.DeleteStartupInconsistencies.Get("unscoped_tombstone") - before; got != 2 {
		t.Errorf("lakehouse_delete_startup_inconsistencies_total{kind=\"unscoped_tombstone\"} moved by %d, want 2", got)
	}
}

// Restore unions the scopes of a record's two copies.
func TestTenantScope_RestoreMergeUnionsTenants(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()
	storeTombstoneInS3(t, pool, "logs/", scopedSample("ts-u", TenantRef{AccountID: 2}))

	store := NewTombstoneStore()
	store.Add(scopedSample("ts-u", TenantRef{AccountID: 1}))
	if _, err := store.Restore(context.Background(), PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	u, _ := store.Get("ts-u")
	if !u.AppliesToTenant(1, 0) || !u.AppliesToTenant(2, 0) || u.AppliesToTenant(3, 0) {
		t.Errorf("two copies must union their tenants, got %+v", u.Tenants)
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

// --- rewrite scheduler ------------------------------------------------------

// Discovery adds only the scoped tenant's objects, and a rewrite never touches
// another tenant's object — even one a tombstone record names by mistake.
func TestScheduler_TenantScopedTombstoneRewritesOnlyItsTenant(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockRewriterPool()
	detector := NewStorageClassDetector(nil)

	own := "1001/0/logs/dt=2026-01-01/hour=10/own.parquet"
	other := "2002/0/logs/dt=2026-01-01/hour=10/other.parquet"
	listedByMistake := "3003/0/logs/dt=2026-01-01/hour=10/foreign.parquet"
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "delete me", SeverityText: "error", ServiceName: "web"},
		{TimestampUnixNano: 2000, Body: "keep this", SeverityText: "info", ServiceName: "web"},
	}
	for _, k := range []string{own, other, listedByMistake} {
		pool.Put(k, buildTestParquet(t, rows))
	}
	otherBefore := mustGet(t, pool, other)
	foreignBefore := mustGet(t, pool, listedByMistake)

	store.Add(Tombstone{
		ID: "ts-scoped", Query: `severity_text:="error"`, StartNs: 0, EndNs: time.Now().UnixNano(),
		AffectedKeys: []string{listedByMistake}, // a defect or a hand edit
		CreatedAt:    time.Now().Add(-2 * time.Hour), Mode: "permanent",
		Tenants: []TenantRef{{AccountID: 1001}},
	})
	sched, m := buildSchedulerWithManifest(t, store, detector, pool, []string{"STANDARD"})
	markListed(t, m, []string{own, other, listedByMistake})

	beforeSkips := metrics.DeleteTenantScopeSkips.Get("rewrite")
	results := sched.RunOnce(context.Background())

	if len(results) != 1 || results[0].OldKey != own {
		t.Fatalf("want exactly one rewrite, of tenant 1001's object; got %+v", results)
	}
	if got := mustGet(t, pool, other); string(got) != string(otherBefore) {
		t.Error("tenant 2002's object was rewritten by tenant 1001's delete")
	}
	if got := mustGet(t, pool, listedByMistake); string(got) != string(foreignBefore) {
		t.Error("an object of another tenant listed on the record was rewritten")
	}
	if got := metrics.DeleteTenantScopeSkips.Get("rewrite") - beforeSkips; got != 1 {
		t.Errorf("lakehouse_delete_tenant_scope_skips_total{site=\"rewrite\"} moved by %d, want 1", got)
	}
	// Retired: every key it lists is handled (own rewritten, the foreign one
	// recorded clean) and discovery found nothing else of its tenant.
	if ts, still := store.Get("ts-scoped"); still {
		t.Errorf("the tombstone must retire once its tenant's objects are rewritten; still active with %+v", ts)
	}
}
