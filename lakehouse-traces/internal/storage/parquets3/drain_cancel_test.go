package parquets3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// A Stop (SIGTERM) cancels the drain's context. Upstream's RunQuery then returns
// nil with partial or no rows, so a plan check that compares counts without
// looking at the context reports a segment that is fine as a violation: the
// "invariant" series would move on every deploy although it must stay 0. A
// cancelled step is a cancelled step: it is counted once under stage="collect"
// (as a cancelled upload is counted once under "upload") and never under
// "invariant".

func collectNow() uint64 { return metrics.BufferFlushErrors.Get("collect") }

func TestDrain_CancelledCollectIsNotAnInvariantViolation(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 5)
	g := e.seal()
	f := e.flusher(1000)
	counts, err := tenantSecondCounts(context.Background(), g, segTenantA)
	if err != nil {
		t.Fatal(err)
	}
	sl := planSlices(counts, 1000)[0]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	inv := invariantNow()
	_, _, err = f.collectSlice(ctx, g, segTenantA, sl)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("collectSlice on a cancelled context returned %v, want context.Canceled", err)
	}
	if errors.Is(err, errDrainInvariant) || invariantNow() != inv {
		t.Errorf("a cancelled collect was reported as an invariant violation (counter +%d): %v", invariantNow()-inv, err)
	}
}

func TestDrain_CancelBetweenPlanAndCountIsNotAnInvariantViolation(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 5)
	g := e.seal()
	f := e.flusher(1000)
	ctx, cancel := context.WithCancel(context.Background())
	// The count runs on a cancelled context: it returns what the engine returns
	// then (here: nothing counted), with no error.
	rc := func(c context.Context, g *membuffer.Segment, tn logstorage.TenantID) (int64, error) {
		cancel()
		return 0, nil
	}
	inv := invariantNow()
	_, err := f.planTenant(ctx, g, segTenantA, tenantSecondCounts, rc)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("planTenant with the context cancelled before the count returned %v, want context.Canceled", err)
	}
	if errors.Is(err, errDrainInvariant) || invariantNow() != inv {
		t.Errorf("a cancelled count was reported as an invariant violation (counter +%d): %v", invariantNow()-inv, err)
	}
}

// The per-second plan itself runs on a cancelled context.
func TestDrain_CancelDuringPlanIsNotAnInvariantViolation(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 5)
	g := e.seal()
	f := e.flusher(1000)
	ctx, cancel := context.WithCancel(context.Background())
	sc := func(c context.Context, g *membuffer.Segment, tn logstorage.TenantID) (map[int64]int64, error) {
		cancel()
		return map[int64]int64{}, nil // partial: nothing
	}
	inv := invariantNow()
	_, err := f.planTenant(ctx, g, segTenantA, sc, tenantRowCount)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("planTenant with the context cancelled during the plan returned %v, want context.Canceled", err)
	}
	if errors.Is(err, errDrainInvariant) || invariantNow() != inv {
		t.Errorf("a cancelled plan was reported as an invariant violation (counter +%d): %v", invariantNow()-inv, err)
	}
}

// End to end: Stop lands while the first group uploads; the drain goes on to the
// next group with a cancelled context. Nothing moves "invariant", the stalled
// gauge stays 0, and the restart drains the segment completely.
func TestDrain_StopMidDrainIsNotAnInvariantViolation(t *testing.T) {
	e := newSegEnv(t)
	for i := 0; i < 5; i++ {
		e.ingest(segTenantA, hourAgo.Add(time.Duration(i)*time.Second), 3)
	}
	g := e.seal()
	f := e.flusher(3) // one group per second: 5 groups
	ctx, cancel := context.WithCancel(context.Background())
	e.hook.Store(func(string) { cancel() })
	metrics.BufferStalledSegments.Set(0) // process-global: an earlier test may have left it at 1
	inv, col := invariantNow(), collectNow()
	err := f.drain(ctx, g)
	if err == nil {
		t.Fatal("a drain stopped mid-way returned nil")
	}
	if errors.Is(err, errDrainInvariant) || invariantNow() != inv {
		t.Errorf("a stop mid-drain was reported as an invariant violation (counter +%d): %v", invariantNow()-inv, err)
	}
	if d := collectNow() - col; d > 1 {
		t.Errorf("a stop mid-drain counted %d collect errors, want at most 1", d)
	}
	if v := metrics.BufferStalledSegments.Get(); v != 0 {
		t.Errorf("lakehouse_buffer_stalled_segments = %d after a stop, want 0", v)
	}
	// The restart drains it fully.
	e.hook.Store(func(string) {})
	e.restart()
	f = e.flusher(3)
	e.drainAll(f)
	e.mustHaveExactly(15, "after restart")
	if invariantNow() != inv {
		t.Errorf("the restart's drain moved the invariant counter by %d", invariantNow()-inv)
	}
}

// A real cancellation inside the engine (not a stand-in): the context is
// cancelled while the drain's own plan and collect queries run.
func TestDrain_CancelAtEveryStepNeverMovesInvariant(t *testing.T) {
	for _, after := range []int{0, 1, 2, 3, 4, 6, 8} {
		e := newSegEnv(t)
		for i := 0; i < 4; i++ {
			e.ingest(segTenantA, hourAgo.Add(time.Duration(i)*time.Second), 3)
			e.ingest(segTenantB, hourAgo.Add(time.Duration(i)*time.Second), 2)
		}
		g := e.seal()
		f := e.flusher(3)
		ctx, cancel := context.WithCancel(context.Background())
		n := 0
		e.hook.Store(func(string) {
			if n++; n > after {
				cancel()
			}
		})
		if after == 0 {
			cancel()
		}
		inv := invariantNow()
		_ = f.drain(ctx, g)
		if d := invariantNow() - inv; d != 0 {
			t.Errorf("cancel after %d uploads: invariant counter +%d", after, d)
		}
		cancel()
	}
}
