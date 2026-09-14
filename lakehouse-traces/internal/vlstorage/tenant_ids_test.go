package vlstorage

import (
	"context"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// tenantListingStore enumerates the tenants it holds, as the Parquet storage
// does.
type tenantListingStore struct {
	mockStore
	tenants []logstorage.TenantID
}

func (s tenantListingStore) TenantIDsForRange(_, _ int64) []logstorage.TenantID { return s.tenants }

// /select/tenant_ids must report the tenants the storage holds, not a fixed
// default tenant.
func TestGetTenantIDs_UsesStorageTenantList(t *testing.T) {
	want := []logstorage.TenantID{{}, {AccountID: 1001}, {AccountID: 2002, ProjectID: 7}}
	a := &adapter{store: tenantListingStore{tenants: want}}
	got, err := a.GetTenantIDs(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("GetTenantIDs: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("GetTenantIDs = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("GetTenantIDs[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}

	if got, err := (&adapter{store: tenantListingStore{}}).GetTenantIDs(context.Background(), 0, 1); err != nil || len(got) != 0 {
		t.Errorf("a storage without tenants in range must report none, got %+v (err %v)", got, err)
	}
}

// tracedTenantListing wraps the traces TracedStorage test double with tenant
// enumeration.
type tracedTenantListing struct {
	mockStorage
}

func (tracedTenantListing) TenantIDsForRange(_, _ int64) []logstorage.TenantID {
	return []logstorage.TenantID{{AccountID: 5}}
}

// The telemetry wrapper must forward tenant enumeration.
func TestTracedStorage_TenantIDsForRange(t *testing.T) {
	if got := NewTracedStorage(&tracedTenantListing{}).TenantIDsForRange(0, 1); len(got) != 1 || got[0].AccountID != 5 {
		t.Errorf("TenantIDsForRange = %+v, want the inner storage's tenant 5:0", got)
	}
	if got := NewTracedStorage(&mockStorage{}).TenantIDsForRange(0, 1); got != nil {
		t.Errorf("TenantIDsForRange = %+v, want nil when the inner storage cannot enumerate", got)
	}
}
