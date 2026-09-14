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
