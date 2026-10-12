package parquets3

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// lakehouse_buffer_stalled_segments is published once, when a drain attempt
// ends, never in the middle of it: 1 if a check failed in the attempt, 0 if the
// checks that ran passed (even when an upload then failed), unchanged when Stop
// cut the attempt short or no check ran. These tests pin that.

// skewedSeconds returns a per-second query whose counts add up to the segment's
// rows for tenant skew but sit in the wrong seconds (first and last swapped), so
// the plan check passes and the group check inside writeGroup does not.
func skewedSeconds(skew logstorage.TenantID) func(context.Context, *membuffer.Segment, logstorage.TenantID) (map[int64]int64, error) {
	return func(ctx context.Context, g *membuffer.Segment, tn logstorage.TenantID) (map[int64]int64, error) {
		counts, err := tenantSecondCounts(ctx, g, tn)
		if err != nil || tn != skew {
			return counts, err
		}
		secs := make([]int64, 0, len(counts))
		for s := range counts {
			secs = append(secs, s)
		}
		sort.Slice(secs, func(i, j int) bool { return secs[i] < secs[j] })
		first, last := secs[0], secs[len(secs)-1]
		counts[first], counts[last] = counts[last], counts[first]
		return counts, nil
	}
}

// An attempt fails a check (a mismatch that does not repeat); the next attempts
// pass every check and only the uploads fail, because the bucket is down. The
// segment is not stalled by a check any more: the gauge, and with it the
// critical alert and the runbook's "if it stays 1, the cause persists", must not
// say it is.
func TestDrain_StalledGaugeAfterTransientMismatchThenOutage(t *testing.T) {
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

	// attempts 2..4: the checks pass, the bucket is down.
	f.secondCounts = nil
	e.failFn.Store(func(string) bool { return true })
	inv := invariantNow()
	for i := 2; i <= 4; i++ {
		if err := f.drain(context.Background(), g); err == nil || errors.Is(err, errDrainInvariant) {
			t.Fatalf("attempt %d: want an upload error, got %v", i, err)
		}
		if v := metrics.BufferStalledSegments.Get(); v != 0 {
			t.Errorf("attempt %d: lakehouse_buffer_stalled_segments = %d while every check passes and only uploads fail; want 0", i, v)
		}
	}
	if invariantNow() != inv {
		t.Fatalf("a check failed again: the fixture is wrong")
	}

	// the bucket is back: the segment commits and the gauge stays 0.
	e.failFn.Store(func(string) bool { return false })
	e.drainAll(f)
	e.mustHaveExactly(3, "after the outage")
	if v := metrics.BufferStalledSegments.Get(); v != 0 {
		t.Errorf("lakehouse_buffer_stalled_segments = %d after the commit, want 0", v)
	}
}

// A group-level mismatch that repeats keeps the gauge at 1 across attempts and
// never shows a 0 in the middle of one: a scrape that saw the 0 would restart the
// alert's 15 minutes, and the alert would never fire. The gauge is sampled from
// the upload hook, which runs between the plan check and the group check.
func TestDrain_PersistentGroupMismatchKeepsTheGaugeAtOne(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 2)
	e.ingest(segTenantB, hourAgo, 2)
	e.ingest(segTenantB, hourAgo.Add(5*time.Second), 1)
	g := e.seal()
	f := e.flusher(2)
	f.secondCounts = skewedSeconds(segTenantB)
	metrics.BufferStalledSegments.Set(0)

	// Tenant A's group is planned first and its upload fails, so the hook runs in
	// every attempt, before tenant B's group check fails.
	e.failFn.Store(func(string) bool { return true })
	var samples []int64
	e.hook.Store(func(string) { samples = append(samples, metrics.BufferStalledSegments.Get()) })

	for attempt := 1; attempt <= 4; attempt++ {
		before := len(samples)
		err := f.drain(context.Background(), g)
		if err == nil {
			t.Fatalf("attempt %d: drain returned nil", attempt)
		}
		if len(samples) == before {
			t.Fatalf("attempt %d: the hook did not run: the gauge was not sampled inside the attempt", attempt)
		}
		if v := metrics.BufferStalledSegments.Get(); v != 1 {
			t.Errorf("attempt %d: lakehouse_buffer_stalled_segments = %d at its end, want 1", attempt, v)
		}
		if attempt > 1 {
			for _, v := range samples[before:] {
				if v != 1 {
					t.Errorf("attempt %d: the gauge read %d in the middle of the attempt, want 1 (a 0 restarts the alert's for: 15m)", attempt, v)
				}
			}
		}
	}
}

// Stop cuts an attempt short: the gauge stays where it was, at 1 as well as at 0.
func TestDrain_StopLeavesTheStalledGaugeAsItWas(t *testing.T) {
	for _, was := range []int64{0, 1} {
		e := newSegEnv(t)
		for i := 0; i < 3; i++ {
			e.ingest(segTenantA, hourAgo.Add(time.Duration(i)*time.Second), 3)
		}
		g := e.seal()
		f := e.flusher(3)
		ctx, cancel := context.WithCancel(context.Background())
		// The plan check has passed when the first upload starts: Stop lands there.
		e.hook.Store(func(string) { cancel() })
		metrics.BufferStalledSegments.Set(was)
		if err := f.drain(ctx, g); err == nil {
			t.Fatal("a drain stopped mid-way returned nil")
		}
		if v := metrics.BufferStalledSegments.Get(); v != was {
			t.Errorf("gauge was %d, a stop left it at %d, want unchanged", was, v)
		}
		cancel()
	}
}

// An attempt that fails before any check ran (here: recording the segment as
// draining) says nothing about the cause of an earlier stall: the gauge stays.
func TestDrain_AttemptWithoutACheckLeavesTheStalledGaugeAsItWas(t *testing.T) {
	for _, was := range []int64{0, 1} {
		e := newSegEnv(t)
		e.ingest(segTenantA, hourAgo, 3)
		g := e.seal()
		f := e.flusher(3)
		f.statePath = filepath.Join(e.dir, "no-such-dir", "state")
		metrics.BufferStalledSegments.Set(was)
		err := f.drain(context.Background(), g)
		if err == nil || errors.Is(err, errDrainInvariant) {
			t.Fatalf("want a state-write error, got %v", err)
		}
		if v := metrics.BufferStalledSegments.Get(); v != was {
			t.Errorf("gauge was %d, an attempt without a check left it at %d, want unchanged", was, v)
		}
	}
}

// The plan check alone clears the gauge: every group is already stored, so no
// group is read, and only the commit marker fails to upload.
func TestDrain_StalledGaugeClearsWhenOnlyThePlanCheckRan(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 3)
	g := e.seal()
	f := e.flusher(1000)
	e.failFn.Store(func(key string) bool { return strings.Contains(key, "_segments/") })
	for attempt := 1; attempt <= 2; attempt++ {
		// attempt 1 stores the group and fails at the marker; attempt 2 settles the
		// group from its stored mark, so only the plan check runs.
		metrics.BufferStalledSegments.Set(1) // as left by an earlier attempt that failed a check
		if err := f.drain(context.Background(), g); err == nil || errors.Is(err, errDrainInvariant) {
			t.Fatalf("attempt %d: want a marker error, got %v", attempt, err)
		}
		if v := metrics.BufferStalledSegments.Get(); v != 0 {
			t.Errorf("attempt %d: lakehouse_buffer_stalled_segments = %d after the checks passed, want 0", attempt, v)
		}
	}
	if len(e.storedDataKeys()) != 1 {
		t.Fatalf("fixture: want the one group stored, got %v", e.storedDataKeys())
	}
}

// Stop lands after a group check has failed (tenant A's group is stalled, and the
// cancel comes while tenant B's group uploads): the attempt was cut short, so the
// gauge is left as it was, even though a check had found a mismatch.
func TestDrain_StopAfterAFailedCheckLeavesTheStalledGaugeAsItWas(t *testing.T) {
	for _, was := range []int64{0, 1} {
		e := newSegEnv(t)
		e.ingest(segTenantA, hourAgo, 2)
		e.ingest(segTenantA, hourAgo.Add(5*time.Second), 1)
		e.ingest(segTenantB, hourAgo, 2)
		g := e.seal()
		f := e.flusher(2)
		f.secondCounts = skewedSeconds(segTenantA)
		ctx, cancel := context.WithCancel(context.Background())
		e.hook.Store(func(string) { cancel() }) // tenant B's upload, after A's check failed
		metrics.BufferStalledSegments.Set(was)
		inv := invariantNow()
		if err := f.drain(ctx, g); err == nil {
			t.Fatal("a drain stopped mid-way returned nil")
		}
		if invariantNow() == inv {
			t.Fatal("fixture: tenant A's group check did not fail")
		}
		if v := metrics.BufferStalledSegments.Get(); v != was {
			t.Errorf("gauge was %d, a stop after a failed check left it at %d, want unchanged", was, v)
		}
		cancel()
	}
}
