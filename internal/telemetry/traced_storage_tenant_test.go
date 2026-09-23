package telemetry

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// tenantListingStorage is a storage that can enumerate its tenants, like the
// Parquet storage does for /select/tenant_ids.
type tenantListingStorage struct {
	mockStorage
	gotStart, gotEnd int64
}

func (s *tenantListingStorage) TenantIDsForRange(startNs, endNs int64) []logstorage.TenantID {
	s.gotStart, s.gotEnd = startNs, endNs
	return []logstorage.TenantID{{}, {AccountID: 1001}}
}

// The telemetry wrapper must forward tenant enumeration, or /select/tenant_ids
// falls back to a single default tenant whenever tracing is enabled.
func TestTracedStorage_TenantIDsForRange_Forwards(t *testing.T) {
	inner := &tenantListingStorage{}
	got := NewTracedStorage(inner).TenantIDsForRange(10, 20)
	if len(got) != 2 || got[1].AccountID != 1001 {
		t.Errorf("TenantIDsForRange = %+v, want the inner storage's two tenants", got)
	}
	if inner.gotStart != 10 || inner.gotEnd != 20 {
		t.Errorf("inner storage saw range [%d, %d], want [10, 20]", inner.gotStart, inner.gotEnd)
	}
}

func TestTracedStorage_TenantIDsForRange_InnerWithoutEnumeration(t *testing.T) {
	if got := NewTracedStorage(&mockStorage{}).TenantIDsForRange(10, 20); got != nil {
		t.Errorf("TenantIDsForRange = %+v, want nil when the inner storage cannot enumerate tenants", got)
	}
}

// fileListingStorage lists its objects by tenant, like the Parquet storage does
// for a delete task's affected keys.
type fileListingStorage struct {
	mockStorage
	gotIDs []logstorage.TenantID
}

func (s *fileListingStorage) TenantFileKeys(ids []logstorage.TenantID, _, _ int64) []string {
	s.gotIDs = ids
	return []string{"1001/0/logs/dt=x/a.parquet"}
}

// The telemetry wrapper must forward the tenant-scoped object listing, or a
// delete task records no affected keys whenever tracing is enabled.
func TestTracedStorage_TenantFileKeys_Forwards(t *testing.T) {
	inner := &fileListingStorage{}
	got := NewTracedStorage(inner).TenantFileKeys([]logstorage.TenantID{{AccountID: 1001}}, 0, 1)
	if len(got) != 1 || len(inner.gotIDs) != 1 || inner.gotIDs[0].AccountID != 1001 {
		t.Errorf("TenantFileKeys = %v (inner saw %v), want the inner storage's answer for 1001:0", got, inner.gotIDs)
	}
	if got := NewTracedStorage(&mockStorage{}).TenantFileKeys(nil, 0, 1); got != nil {
		t.Errorf("TenantFileKeys = %v, want nil when the inner storage cannot list by tenant", got)
	}
}

type layoutStorage struct {
	mockStorage
	accountOnly bool
}

func (s *layoutStorage) AccountOnlyTenantKeys() bool { return s.accountOnly }

// The telemetry wrapper must forward the key-layout report, or a delete task
// for a non-zero ProjectID would not be refused on an {OrgID} layout whenever
// tracing is enabled.
func TestTracedStorage_AccountOnlyTenantKeys_Forwards(t *testing.T) {
	if !NewTracedStorage(&layoutStorage{accountOnly: true}).AccountOnlyTenantKeys() {
		t.Error("an account-only inner storage must be reported")
	}
	if NewTracedStorage(&layoutStorage{}).AccountOnlyTenantKeys() || NewTracedStorage(&mockStorage{}).AccountOnlyTenantKeys() {
		t.Error("a two-segment or silent inner storage must not be reported account-only")
	}
}
