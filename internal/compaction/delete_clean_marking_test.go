package compaction

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
)

// A compacted output may be recorded as clean for a tombstone only if the merge
// actually filtered that tombstone's rows out of it. The bookkeeping used to
// re-judge each tombstone at bookkeeping time instead of asking what the merge
// did, so any tombstone that became eligible — or was issued — while the merge
// ran was recorded clean on an output that still held its rows. The tombstone
// then retired and those rows came back.

// TestDeleteRace_EligibilityBoundaryCrossedDuringTheMerge: the tombstone's
// rewrite_delay ends while the merge is being published.
func TestDeleteRace_EligibilityBoundaryCrossedDuringTheMerge(t *testing.T) {
	const delay = time.Hour
	const margin = 1500 * time.Millisecond

	w := newRaceWorld(t)
	w.store.Update("ts-race", func(ts *delete.Tombstone) bool {
		ts.CreatedAt = time.Now().Add(-delay).Add(margin) // eligible `margin` from now
		return true
	})
	reached, release := w.pool.gate(func(key string) bool { return strings.Contains(key, "compacted-") })

	var res *CompactResult
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		res, err = w.compactor(delay).Compact(context.Background(), racePartition, w.files, 0)
	}()
	<-reached // the merge has already evaluated the tombstone: not yet eligible
	time.Sleep(margin + 500*time.Millisecond)
	release()
	<-done
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	assertOutputStillPending(t, w, "ts-race", res.OutputFile, `service.name:="leaky"`)

	w.converge(t)
	w.assert(t, "after the boundary-crossing merge converged")
	if w.store.Count() != 0 {
		t.Fatalf("the tombstone should retire once the output is rewritten, %d remain", w.store.Count())
	}
}

// TestDeleteRace_TombstoneIssuedDuringTheMerge: a delete issued while the merge
// runs names the files the manifest still lists — the merge's sources.
func TestDeleteRace_TombstoneIssuedDuringTheMerge(t *testing.T) {
	w := newRaceWorld(t)
	reached, release := w.pool.gate(func(key string) bool { return strings.Contains(key, "compacted-") })

	var res *CompactResult
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		res, err = w.compactor(time.Hour).Compact(context.Background(), racePartition, w.files, 0)
	}()
	<-reached
	w.store.Add(delete.Tombstone{
		Tenants:      []delete.TenantRef{{}},
		ID:           "ts-late",
		Query:        `service.name:="web"`,
		StartNs:      raceHour.UnixNano(),
		EndNs:        raceHour.Add(time.Hour).UnixNano(),
		AffectedKeys: []string{w.files[0].Key, w.files[1].Key},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "permanent",
		Reaped:       map[string]bool{},
	})
	release()
	<-done
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	assertOutputStillPending(t, w, "ts-late", res.OutputFile, `service.name:="web"`)
}

// assertOutputStillPending fails unless the tombstone is still active, still
// lists the output as pending, and the output really does hold rows matching it
// — the state in which retiring the tombstone would un-hide those rows.
func assertOutputStillPending(t *testing.T, w *raceWorld, id, output, query string) {
	t.Helper()
	ts, active := w.store.Get(id)
	rows, rerr := readLogRows(w.pool.get(output))
	if rerr != nil {
		t.Fatalf("read output: %v", rerr)
	}
	probe := delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "probe", Query: query, StartNs: raceHour.UnixNano(), EndNs: raceHour.Add(time.Hour).UnixNano()}
	var matching int
	for i := range rows {
		if probe.MatchesFields(delete.LogRowFields(&rows[i]), rows[i].TimestampUnixNano) {
			matching++
		}
	}
	if matching == 0 {
		t.Fatalf("fixture: output %s holds no rows matching %s; the merge filtered them, nothing to prove", output, query)
	}
	if !active {
		t.Fatalf("tombstone %s retired although %d of its rows were carried unfiltered into %s", id, matching, output)
	}
	if ts.Handled(output) {
		t.Fatalf("output %s recorded clean for %s although it holds %d of its rows", output, id, matching)
	}
	if !containsKey(ts.AffectedKeys, output) {
		t.Fatalf("output %s must be listed on %s so the rewriter reaches it: %v", output, id, ts.AffectedKeys)
	}
}
