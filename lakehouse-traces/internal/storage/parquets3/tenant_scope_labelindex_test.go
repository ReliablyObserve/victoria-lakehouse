package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// The in-RAM label index is not tenant-keyed: it holds the field names and
// values of every object it was built from. In a manifest with ONE tenant it
// may answer that tenant — and only that tenant. The earlier gate asked only
// "does the manifest hold at most one tenant?", so in a deployment whose only
// tenant is 1001:0 a request with no headers, an explicit 0:0 or an unknown
// tenant (which own no objects, so their object list is empty and the label
// index branch runs) received 1001:0's field names and service values. The
// multi-tenant invariant fixtures never reach that branch, which is why this
// suite builds single-tenant manifests.
//
// Twin of internal/storage/parquets3/tenant_scope_labelindex_test.go.

type labelIndexManifest struct {
	name    string
	objects []tsTenant // key + service; bucket derived per layout
	// sole is the tenant the manifest reports as its only tenant.
	sole logstorage.TenantID
}

func labelIndexManifests() []labelIndexManifest {
	return []labelIndexManifest{
		{
			name:    "only-1001:0",
			objects: []tsTenant{{key: "1001/0/logs/" + tsPartition + "/only.parquet", service: "svc-tenant-1001-0", rows: 3}},
			sole:    logstorage.TenantID{AccountID: 1001},
		},
		{
			name:    "only-0:0",
			objects: []tsTenant{{key: "0/0/logs/" + tsPartition + "/only.parquet", service: "svc-tenant-0-0", rows: 3}},
			sole:    logstorage.TenantID{},
		},
		{
			name:    "legacy-only",
			objects: []tsTenant{{key: "logs/" + tsPartition + "/legacy.parquet", service: "svc-legacy", rows: 3}},
			sole:    logstorage.TenantID{},
		},
		{
			// A default-tenant deployment moving to the prefix template: legacy
			// objects and 0:0-prefixed objects are one tenant.
			name: "0:0-plus-legacy",
			objects: []tsTenant{
				{key: "0/0/logs/" + tsPartition + "/new.parquet", service: "svc-tenant-0-0", rows: 3},
				{key: "logs/" + tsPartition + "/legacy.parquet", service: "svc-legacy", rows: 2},
			},
			sole: logstorage.TenantID{},
		},
	}
}

func labelIndexRequests() []tsCase {
	return []tsCase{
		{name: "no-headers", tenantIDs: nil},
		{name: "explicit-0:0", tenantIDs: []logstorage.TenantID{{}}},
		{name: "unknown-7:0", tenantIDs: []logstorage.TenantID{{AccountID: 7}}},
		{name: "1001:0", tenantIDs: []logstorage.TenantID{{AccountID: 1001}}},
		{name: "list-0:0+1001:0", tenantIDs: []logstorage.TenantID{{}, {AccountID: 1001}}},
		{name: "global-read", tenantIDs: []logstorage.TenantID{{}}, globalRead: true},
	}
}

func newLabelIndexFixture(t *testing.T, layout tsLayout, lm labelIndexManifest) *tsFixture {
	t.Helper()
	f := newEmptyTenantScopeFixture(t, layout, "{AccountID}/{ProjectID}/")
	for _, obj := range lm.objects {
		obj.bucket = tsBucketOf(layout, obj.key)
		f.put(tsNow(), obj.bucket, obj.key, obj.service, obj.rows)
		f.tenants = append(f.tenants, obj)
	}
	f.s.WarmLabelIndex(context.Background())
	if f.s.labelIndex.Len() == 0 {
		t.Fatal("label index is empty after warm-up — the label index branch would not be exercised")
	}
	f.mock.reset() // the warm-up is startup work, not part of the request
	return f
}

// ownedServices / ownedBuckets: what the request may see, by object ownership.
func (f *tsFixture) ownedServices(tc tsCase) map[string]bool {
	out := map[string]bool{}
	parse := f.s.manifest.TenantKeyParser()
	scope := resolveTenantScope(tc.tenantIDs)
	for _, obj := range f.tenants {
		if tc.globalRead || tenantOwnsKey(parse, scope, obj.key) {
			out[obj.service] = true
		}
	}
	return out
}

func (f *tsFixture) ownedBuckets(tc tsCase) map[string]bool {
	out := map[string]bool{}
	parse := f.s.manifest.TenantKeyParser()
	scope := resolveTenantScope(tc.tenantIDs)
	for _, obj := range f.tenants {
		if tc.globalRead || tenantOwnsKey(parse, scope, obj.key) {
			out[obj.bucket] = true
		}
	}
	return out
}

// TestTenantScope_LabelIndex_SingleTenantManifest: no request may read field
// names or values through the label index that belong to a tenant it is not,
// whether its window holds objects or not; field_names for a request that owns
// no objects is empty.
func TestTenantScope_LabelIndex_SingleTenantManifest(t *testing.T) {
	windows := []struct {
		name       string
		start, end time.Time
	}{
		{"window-with-objects", tsNow().Add(-time.Hour), tsNow().Add(time.Hour)},
		// No object in range: nothing to answer, whatever the label index holds.
		{"window-without-objects", tsNow().Add(48 * time.Hour), tsNow().Add(49 * time.Hour)},
	}
	for _, layout := range tsLayouts() {
		for _, lm := range labelIndexManifests() {
			for _, w := range windows {
				for _, tc := range labelIndexRequests() {
					t.Run(string(layout)+"/"+lm.name+"/"+w.name+"/"+tc.name, func(t *testing.T) {
						f := newLabelIndexFixture(t, layout, lm)
						ctx := f.ctx(tc.globalRead)
						q := mustParseQueryWithTime(t, "*", w.start.UnixNano(), w.end.UnixNano())
						owned := f.ownedServices(tc)

						names, err := f.s.GetFieldNames(ctx, tc.tenantIDs, q)
						if err != nil {
							t.Fatalf("GetFieldNames: %v", err)
						}
						if len(owned) == 0 && len(names) > 0 {
							t.Errorf("field_names returned %d names to a request that owns no objects: %v", len(names), names)
						}

						values, err := f.s.GetFieldValues(ctx, tc.tenantIDs, q, "service.name", 100)
						if err != nil {
							t.Fatalf("GetFieldValues: %v", err)
						}
						assertNoForeignValues(t, "field_values", valuesToCounts(values), owned)
						assertBucketsWithin(t, "field_names+field_values", f.mock.bucketsTouched(), f.ownedBuckets(tc))
					})
				}
			}
		}
	}
}

// TestTenantScope_LabelIndex_KeptForSoleTenant is the positive side of the
// gate: the sole tenant — including 0:0 in a deployment that still holds legacy
// objects next to its 0:0-prefixed ones — may still read the label index (the
// degraded field_names fallback when footers cannot be read). Neither
// field_names nor field_values answers from it for a window holding no
// objects: it is a sample and not time-scoped, so such a window is empty, as on
// VictoriaLogs — and answering it touches no bucket.
func TestTenantScope_LabelIndex_KeptForSoleTenant(t *testing.T) {
	for _, layout := range tsLayouts() {
		for _, lm := range labelIndexManifests() {
			t.Run(string(layout)+"/"+lm.name, func(t *testing.T) {
				f := newLabelIndexFixture(t, layout, lm)
				owner := []logstorage.TenantID{lm.sole}
				if !f.s.tenantScopeAllowsGlobalIndex(resolveTenantScope(owner)) {
					t.Fatalf("the sole tenant %v lost the label index fast path", lm.sole)
				}
				q := mustParseQueryWithTime(t, "*", tsNow().Add(48*time.Hour).UnixNano(), tsNow().Add(49*time.Hour).UnixNano())
				names, err := f.s.GetFieldNames(context.Background(), owner, q)
				if err != nil {
					t.Fatalf("GetFieldNames: %v", err)
				}
				if len(names) != 0 {
					t.Errorf("field_names outside the object window = %v, want none (the label index is not an answer)", valuesToCounts(names))
				}
				values, err := f.s.GetFieldValues(context.Background(), owner, q, "service.name", 100)
				if err != nil {
					t.Fatalf("GetFieldValues: %v", err)
				}
				if len(values) != 0 {
					t.Errorf("field_values outside the object window = %v, want none (the label index is not an answer)", valuesToCounts(values))
				}
				if touched := f.mock.bucketsTouched(); len(touched) != 0 {
					t.Errorf("a label index answer issued S3 requests: %v", touched)
				}
			})
		}
	}
}

// TestTenantScope_LabelIndex_Gate pins the gate itself on every manifest and
// request shape.
func TestTenantScope_LabelIndex_Gate(t *testing.T) {
	multi := newTenantScopeFixture(t)
	for _, tc := range tsCases() {
		scope := scopeFor(multi.ctx(tc.globalRead), tc.tenantIDs)
		if got, want := multi.s.tenantScopeAllowsGlobalIndex(scope), tc.globalRead; got != want {
			t.Errorf("multi-tenant manifest, %s: allowed=%v, want %v", tc.name, got, want)
		}
	}

	empty := newEmptyTenantScopeFixture(t, tsLayoutPrefix, "{AccountID}/{ProjectID}/")
	if empty.s.tenantScopeAllowsGlobalIndex(resolveTenantScope(nil)) {
		t.Error("an empty manifest has no sole tenant; the label index (possibly loaded from a snapshot) must not answer 0:0")
	}

	for _, lm := range labelIndexManifests() {
		f := newEmptyTenantScopeFixture(t, tsLayoutPrefix, "{AccountID}/{ProjectID}/")
		for _, obj := range lm.objects {
			f.put(tsNow(), tsDefaultBucket, obj.key, obj.service, obj.rows)
		}
		for _, tc := range labelIndexRequests() {
			scope := scopeFor(f.ctx(tc.globalRead), tc.tenantIDs)
			want := tc.globalRead || (len(tc.tenantIDs) <= 1 && resolveTenantScope(tc.tenantIDs).String() == resolveTenantScope([]logstorage.TenantID{lm.sole}).String())
			if got := f.s.tenantScopeAllowsGlobalIndex(scope); got != want {
				t.Errorf("%s, %s: allowed=%v, want %v", lm.name, tc.name, got, want)
			}
		}
	}
}

// soleTenantManifest gives s a manifest whose only object belongs to tenant 0:0
// and lies outside any recent query window, so a request by 0:0 (no headers)
// may use the label index — the index answers only the manifest's sole tenant —
// without that object taking part in the answer.
func soleTenantManifest(t *testing.T, s *Storage) {
	t.Helper()
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2020-01-01/hour=00", manifest.FileInfo{
		Key:       "legacy/dt=2020-01-01/hour=00/sole.parquet",
		Size:      1024,
		RowCount:  1,
		MinTimeNs: base.UnixNano(),
		MaxTimeNs: base.Add(30 * time.Minute).UnixNano(),
	})
	if _, _, ok := s.manifest.SoleTenant(); !ok {
		t.Fatal("fixture manifest has no sole tenant")
	}
}
