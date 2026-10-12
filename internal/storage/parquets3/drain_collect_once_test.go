package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
)

// A Stop with several tenants in the segment counts the "collect" stage once,
// not once per tenant: the ctx.Err() check at the top of the plan's tenant loop
// and the one in the drain's plans loop each end the pass at the first tenant.

// The context is cancelled while the plan's first tenant is being planned.
func TestDrain_CancelDuringPlanCountsCollectOnce_TwoTenants(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 5)
	e.ingest(segTenantB, hourAgo, 5)
	g := e.seal()
	f := e.flusher(1000)
	ctx, cancel := context.WithCancel(context.Background())
	sc := func(c context.Context, g *membuffer.Segment, tn logstorage.TenantID) (map[int64]int64, error) {
		cancel()
		return map[int64]int64{}, nil
	}
	col, inv := collectNow(), invariantNow()
	_, _, err := f.planSegmentWith(ctx, g, sc, tenantRowCount)
	if err == nil {
		t.Fatal("a cancelled plan returned no error")
	}
	if d := collectNow() - col; d != 1 {
		t.Errorf("collect +%d, want 1", d)
	}
	if invariantNow() != inv {
		t.Error("invariant moved")
	}
}

// The context is cancelled by the first upload: the drain's loop over the
// tenants' plans stops there, and the stop is counted at most once.
func TestDrain_StopMidDrainCountsCollectOnce_TwoTenants(t *testing.T) {
	e := newSegEnv(t)
	for i := 0; i < 3; i++ {
		e.ingest(segTenantA, hourAgo.Add(time.Duration(i)*time.Second), 3)
		e.ingest(segTenantB, hourAgo.Add(time.Duration(i)*time.Second), 3)
	}
	g := e.seal()
	f := e.flusher(3)
	ctx, cancel := context.WithCancel(context.Background())
	e.hook.Store(func(string) { cancel() })
	col := collectNow()
	_ = f.drain(ctx, g)
	if d := collectNow() - col; d > 1 {
		t.Errorf("collect +%d on one stop, want at most 1", d)
	}
}
