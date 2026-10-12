package parquets3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// The drain checks, before anything is uploaded, that its plan covers every row
// the segment holds over the whole of int64 time, and holds the segment when it
// does not. #388 was exactly a plan that quietly covered less than the segment
// (the plan starts at the Unix epoch), and the segment was committed anyway.
// Rows are added here straight to the buffer (e.ingest), past the insert
// adapter, so the segment holds rows the plan cannot reach.

func year1965() time.Time { return time.Date(1965, 3, 1, 12, 0, 0, 0, time.UTC) }

func invariantNow() uint64 { return metrics.BufferFlushErrors.Get("invariant") }

func TestDrain_HoldsASegmentWithRowsThePlanDoesNotCover(t *testing.T) {
	cases := []struct {
		name string
		fill func(e *segEnv)
	}{
		{"a tenant with rows before and after the epoch", func(e *segEnv) {
			e.ingest(segTenantA, year1965(), 5)
			e.ingest(segTenantA, hourAgo, 5)
		}},
		{"a second tenant with only rows before the epoch", func(e *segEnv) {
			e.ingest(segTenantA, hourAgo, 5)
			e.ingest(segTenantB, year1965(), 3)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newSegEnv(t)
			c.fill(e)
			g := e.seal()
			f := e.flusher(1000)
			before := invariantNow()
			for attempt := 1; attempt <= 2; attempt++ {
				err := f.drain(context.Background(), g)
				if !errors.Is(err, errDrainInvariant) {
					t.Fatalf("attempt %d: drain returned %v, want the invariant error", attempt, err)
				}
			}
			if got := invariantNow() - before; got != 2 {
				t.Errorf("lakehouse_buffer_flush_errors_total{stage=invariant} rose by %d, want 2", got)
			}
			if keys := e.storedDataKeys(); len(keys) != 0 {
				t.Errorf("a held segment wrote objects to the bucket: %v", keys)
			}
			if e.markerStored(g.Nonce()) {
				t.Error("a held segment got its commit marker")
			}
			if f.state.CommittedThroughSeq >= g.Seq() {
				t.Errorf("a held segment was recorded as committed (through %d)", f.state.CommittedThroughSeq)
			}
			if p := e.segs.Pending(); len(p) != 1 || p[0] != g {
				t.Errorf("the held segment is not the one pending segment: %d pending", len(p))
			}
		})
	}
}

// The group-level check: the rows a group's query returns must be the rows the
// plan promised for it, and every one is either converted for Parquet or
// counted as filtered out. A plan that disagrees with the segment is not written.
func TestCollectSlice_RefusesAPlanThatDoesNotMatchTheSegment(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 5)
	g := e.seal()
	f := e.flusher(1000)
	counts, err := tenantSecondCounts(context.Background(), g, segTenantA)
	if err != nil {
		t.Fatal(err)
	}
	slices := planSlices(counts, 1000)
	if len(slices) != 1 || slices[0].rows != 5 {
		t.Fatalf("plan = %+v, want one group of 5 rows", slices)
	}

	rows, filtered, err := f.collectSlice(context.Background(), g, segTenantA, slices[0])
	if err != nil || len(rows) != 5 || filtered != 0 {
		t.Fatalf("collect of a correct plan: %d rows, %d filtered, err %v; want 5, 0, nil", len(rows), filtered, err)
	}

	bad := slices[0]
	bad.rows++
	before := invariantNow()
	if _, _, err := f.collectSlice(context.Background(), g, segTenantA, bad); !errors.Is(err, errDrainInvariant) {
		t.Fatalf("collect of a plan with a row too many returned %v, want the invariant error", err)
	}
	bad.rows -= 2
	if _, _, err := f.collectSlice(context.Background(), g, segTenantA, bad); !errors.Is(err, errDrainInvariant) {
		t.Fatalf("collect of a plan with a row too few returned %v, want the invariant error", err)
	}
	if got := invariantNow() - before; got != 2 {
		t.Errorf("invariant counter rose by %d, want 2", got)
	}

	// Rows the keep filter leaves out are counted, and still add up.
	f.keep = func(_, _ uint32, _ string) bool { return false }
	rows, filtered, err = f.collectSlice(context.Background(), g, segTenantA, slices[0])
	if err != nil || len(rows) != 0 || filtered != 5 {
		t.Fatalf("collect with a filter that drops everything: %d rows, %d filtered, err %v; want 0, 5, nil", len(rows), filtered, err)
	}
}

// A segment that cannot be read is not a held segment: the plan reports the
// read error (stage collect, retried after the back-off) and counts no invariant
// violation, and a cancelled context stops the plan.
func TestPlanSegment_ReadErrorsAreNotInvariantViolations(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 5)
	g := e.seal()
	f := e.flusher(1000)
	before := invariantNow()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := f.planSegment(ctx, g); err == nil || errors.Is(err, errDrainInvariant) {
		t.Fatalf("planSegment with a cancelled context returned %v, want the context error", err)
	}

	e.segs.Close() // the segments' storages are closed: every query fails
	_, _, err := f.planSegment(context.Background(), g)
	if err == nil || errors.Is(err, errDrainInvariant) {
		t.Fatalf("planSegment on a closed segment returned %v, want a read error", err)
	}
	if got := invariantNow() - before; got != 0 {
		t.Errorf("a read error counted %d invariant violations", got)
	}
	// The queries behind the plan fail the same way on a closed segment, and none
	// of them is an invariant violation.
	if _, err := tenantRowCount(context.Background(), g, segTenantA); err == nil || errors.Is(err, errDrainInvariant) {
		t.Errorf("tenantRowCount on a closed segment returned %v, want a read error", err)
	}
	if _, err := tenantSecondCounts(context.Background(), g, segTenantA); err == nil || errors.Is(err, errDrainInvariant) {
		t.Errorf("tenantSecondCounts on a closed segment returned %v, want a read error", err)
	}
	sl := flushSlice{partition: partitionFromNano(hourAgo.UnixNano()), start: hourAgo.UnixNano(), end: hourAgo.Add(time.Hour).UnixNano(), rows: 5}
	if _, _, err := f.collectSlice(context.Background(), g, segTenantA, sl); err == nil || errors.Is(err, errDrainInvariant) {
		t.Errorf("collectSlice on a closed segment returned %v, want a read error", err)
	}
	if got := invariantNow() - before; got != 0 {
		t.Errorf("read errors counted %d invariant violations", got)
	}
	e.segs = nil // closed above; the cleanup must not close it twice
}
