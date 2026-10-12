package parquets3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// An attempt that failed a check sets lakehouse_buffer_stalled_segments to 1; a
// later attempt that fails before any check ran (here: the plan query of every
// tenant fails) says nothing new about the cause, so the gauge stays 1. The
// flusher is the same in both attempts, so a drain that forgot to reset its
// per-attempt "a check ran" flag would read the first attempt's flag and write 0.
func TestDrain_LaterAttemptWithoutACheckKeepsTheGaugeFromAnEarlierFailedCheck(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 2)
	e.ingest(segTenantA, hourAgo.Add(5*time.Second), 1)
	g := e.seal()
	f := e.flusher(2)
	metrics.BufferStalledSegments.Set(0)

	// attempt 1: a group's rows disagree with the plan.
	f.secondCounts = skewedSeconds(segTenantA)
	if err := f.drain(context.Background(), g); !errors.Is(err, errDrainInvariant) {
		t.Fatalf("attempt 1: want the invariant error, got %v", err)
	}
	if v := metrics.BufferStalledSegments.Get(); v != 1 {
		t.Fatalf("attempt 1: lakehouse_buffer_stalled_segments = %d after a failed check, want 1", v)
	}

	// attempts 2 and 3: no tenant can be planned, so no check runs at all.
	f.secondCounts = func(context.Context, *membuffer.Segment, logstorage.TenantID) (map[int64]int64, error) {
		return nil, errors.New("plan query failed")
	}
	for attempt := 2; attempt <= 3; attempt++ {
		inv := invariantNow()
		if err := f.drain(context.Background(), g); err == nil || errors.Is(err, errDrainInvariant) {
			t.Fatalf("attempt %d: want a plan-query error that is not an invariant violation, got %v", attempt, err)
		}
		if invariantNow() != inv {
			t.Fatalf("attempt %d: a check failed: the fixture is wrong", attempt)
		}
		if v := metrics.BufferStalledSegments.Get(); v != 1 {
			t.Errorf("attempt %d: lakehouse_buffer_stalled_segments = %d after an attempt in which no check ran, want 1 (unchanged)", attempt, v)
		}
	}
}
