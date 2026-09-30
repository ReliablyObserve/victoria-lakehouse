package vlstorage

import (
	"context"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type lookupStorage struct {
	mockStorage
	gotTraceID string
}

func (s *lookupStorage) LookupTraceIndex(_ context.Context, _ []logstorage.TenantID, traceID string) (int64, int64, bool, error) {
	s.gotTraceID = traceID
	return 11, 22, true, nil
}

// The telemetry wrapper must forward the trace-index lookup: the vtstorage
// adapter finds it by type assertion, and a wrapper that hid it would turn the
// Jaeger/Tempo trace-by-ID fast path off whenever telemetry is on.
func TestTracedStorage_LookupTraceIndex_Forwards(t *testing.T) {
	inner := &lookupStorage{}
	s, e, found, err := NewTracedStorage(inner).LookupTraceIndex(context.Background(), nil, "abc")
	if err != nil || !found || s != 11 || e != 22 || inner.gotTraceID != "abc" {
		t.Errorf("LookupTraceIndex = %d %d %v %v (inner saw %q)", s, e, found, err, inner.gotTraceID)
	}
}

// An inner storage without the capability is a miss, which the adapter treats
// as "fall through to the span scan".
func TestTracedStorage_LookupTraceIndex_InnerWithoutItIsAMiss(t *testing.T) {
	_, _, found, err := NewTracedStorage(&mockStorage{}).LookupTraceIndex(context.Background(), nil, "abc")
	if found || err != nil {
		t.Errorf("found=%v err=%v, want a clean miss", found, err)
	}
}
