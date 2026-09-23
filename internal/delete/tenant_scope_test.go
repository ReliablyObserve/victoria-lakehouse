package delete

import (
	"context"
	"encoding/json"
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
		t.Error("NormalizeTenants(nil) must stay nil: an empty scope is the legacy instance-wide record")
	}
}

func TestTombstone_AppliesToTenantAndKey(t *testing.T) {
	a := scopedSample("a", TenantRef{AccountID: 1001})
	zero := scopedSample("z", TenantRef{})
	wide := sampleTombstone("w")
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
		{"legacy key is the default tenant's", a, nil, "logs/dt=x/a.parquet", false},
		{"legacy key under a 0:0 delete", zero, nil, "logs/dt=x/a.parquet", true},
		{"account-only template matches on the account", a, orgID, "1001/logs/dt=x/a.parquet", true},
		{"account-only template, other account", a, orgID, "1002/logs/dt=x/a.parquet", false},
		{"unscoped legacy record acts on every key", wide, nil, "1002/0/logs/dt=x/a.parquet", true},
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
	if !wide.AppliesToTenant(42, 42) {
		t.Error("an unscoped legacy record acts on every tenant")
	}
	if !a.ScopedExactlyTo(1001, 0) || a.ScopedExactlyTo(0, 0) || wide.ScopedExactlyTo(0, 0) {
		t.Error("ScopedExactlyTo must be true only for a single-tenant scope naming that tenant")
	}
	two := scopedSample("two", TenantRef{AccountID: 1}, TenantRef{AccountID: 2})
	if two.ScopedExactlyTo(1, 0) {
		t.Error("a two-tenant tombstone is no single tenant's own")
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
	wide := []Tombstone{sampleTombstone("w1"), sampleTombstone("w2")}
	if got := ForKey(wide, nil, "1/0/logs/x"); &got[0] != &wide[0] {
		t.Error("with no tenant-scoped tombstone ForKey must return the input without copying")
	}
	if got := ForTenant(wide, 1, 0); &got[0] != &wide[0] {
		t.Error("with no tenant-scoped tombstone ForTenant must return the input without copying")
	}
	if ForKey(nil, nil, "k") != nil || ForTenant(nil, 0, 0) != nil {
		t.Error("empty input must give nil")
	}
	mixed := []Tombstone{sampleTombstone("w"), scopedSample("a", TenantRef{AccountID: 1}), scopedSample("b", TenantRef{AccountID: 2})}
	if got := ForKey(mixed, nil, "2/0/logs/x"); len(got) != 2 || got[0].ID != "w" || got[1].ID != "b" {
		t.Errorf("ForKey = %v, want the instance-wide record and tenant 2's", got)
	}
	if got := ForTenant(mixed, 1, 0); len(got) != 2 || got[1].ID != "a" {
		t.Errorf("ForTenant = %v, want the instance-wide record and tenant 1's", got)
	}
}

// Persistence round-trips the scope through both durable copies, and a record
// written without it (by a release that had no tenant scope) stays instance-wide.
func TestTenantScope_PersistsThroughDiskAndS3(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()
	cfg := PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}

	store := NewTombstoneStore()
	store.EnablePersistence(cfg)
	store.Add(scopedSample("ts-a", TenantRef{AccountID: 1001}, TenantRef{AccountID: 2002, ProjectID: 7}))
	store.Add(sampleTombstone("ts-legacy"))
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
		legacy, ok := st.Get("ts-legacy")
		if !ok || legacy.Scoped() || !legacy.AppliesToTenant(9, 9) {
			t.Errorf("%s restore: the unscoped record must stay instance-wide: %+v", name, legacy)
		}
	}

	// The unscoped record's persisted form carries no Tenants key at all, so it
	// is byte-for-byte what a release without tenant scope wrote and reads.
	data, _ := pool.Get(TombstonePrefix("logs/") + "ts-legacy.json")
	if strings.Contains(string(data), "Tenants") {
		t.Errorf("an unscoped record must persist without a Tenants field: %s", data)
	}
	data, _ = pool.Get(TombstonePrefix("logs/") + "ts-a.json")
	if !strings.Contains(string(data), `"Tenants"`) {
		t.Errorf("a scoped record must persist its tenants: %s", data)
	}
}

// Restore unions the disk and S3 copies; losing the scope in one copy must
// never widen the delete to every tenant.
func TestTenantScope_RestoreMergeNeverWidens(t *testing.T) {
	dir := t.TempDir()
	pool := newMockS3Pool()

	disk := NewTombstoneStore()
	disk.Add(scopedSample("ts-m", TenantRef{AccountID: 1001}))
	if err := disk.PersistToDisk(dir); err != nil {
		t.Fatalf("PersistToDisk: %v", err)
	}
	// The S3 copy of the same record lost the field (a release without tenant
	// scope rewrote it).
	storeTombstoneInS3(t, pool, "logs/", sampleTombstone("ts-m"))
	// And a second record whose two copies name different tenants.
	disk2 := scopedSample("ts-u", TenantRef{AccountID: 1})
	storeTombstoneInS3(t, pool, "logs/", scopedSample("ts-u", TenantRef{AccountID: 2}))

	store := NewTombstoneStore()
	store.Add(disk2)
	if _, err := store.Restore(context.Background(), PersistenceConfig{Dir: dir, Pool: pool, Prefix: "logs/"}); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, _ := store.Get("ts-m")
	if !got.Scoped() || got.AppliesToTenant(0, 0) || !got.AppliesToTenant(1001, 0) {
		t.Errorf("a copy without tenants widened the restored record: %+v", got.Tenants)
	}
	u, _ := store.Get("ts-u")
	if !u.AppliesToTenant(1, 0) || !u.AppliesToTenant(2, 0) || u.AppliesToTenant(3, 0) {
		t.Errorf("two scoped copies must union their tenants, got %+v", u.Tenants)
	}

	if merged := mergeTenants(nil, nil); merged != nil {
		t.Errorf("mergeTenants(nil, nil) = %v, want nil", merged)
	}
}

// Rollback hazard pinned: a release without tenant scope reads a scoped S3
// record as instance-wide, because it has no field for the tenants. The
// operator instruction that follows (drain or remove tenant-scoped tombstones
// before rolling back past this release) is in docs/operations.md.
func TestRollback_ThePreviousReleaseReadsAScopedRecordAsInstanceWide(t *testing.T) {
	data, err := json.Marshal(scopedSample("ts-scoped", TenantRef{AccountID: 1001}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var old legacyTombstone
	if err := json.Unmarshal(data, &old); err != nil {
		t.Fatalf("the previous release can no longer read the S3 copy: %v", err)
	}
	roundTripped, _ := json.Marshal(old)
	var afterRollback Tombstone
	if err := json.Unmarshal(roundTripped, &afterRollback); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if afterRollback.Scoped() {
		t.Fatal("the legacy type in this test is not the previous release's: it kept the tenant scope; " +
			"if the previous release now carries it, update the rollback note in docs/operations.md")
	}
}

func TestTenantScope_InstanceWideGauge(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(scopedSample("s1", TenantRef{AccountID: 1}))
	store.Add(sampleTombstone("w1"))
	store.Add(sampleTombstone("w2"))
	if got := metrics.DeleteTombstonesInstanceWide.Get(); got != 2 {
		t.Errorf("lakehouse_delete_tombstones_instance_wide = %d, want 2", got)
	}
	if got := metrics.DeleteTombstonesActive.Get(); got != 3 {
		t.Errorf("lakehouse_delete_tombstones_active = %d, want 3", got)
	}
	store.Remove("w1")
	if got := metrics.DeleteTombstonesInstanceWide.Get(); got != 1 {
		t.Errorf("after removing one: lakehouse_delete_tombstones_instance_wide = %d, want 1", got)
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
