package delete

import (
	"testing"
	"time"
)

// TestRemovedMarkers_AreBounded: the removed set is kept only as long as it can
// matter, so it cannot grow for the life of the deployment.
func TestRemovedMarkers_AreBounded(t *testing.T) {
	store := NewTombstoneStore()
	now := time.Now()
	for i := 0; i < maxRemovedTombstoneMarkers+10; i++ {
		store.markRemovedLocked(idFor(i), now.Add(time.Duration(i)*time.Millisecond))
	}
	store.markRemovedLocked("ancient", now.Add(-removedTombstoneMarkerTTL-time.Hour))
	store.pruneRemovedLocked(now, nil)
	if got := len(store.removed); got != maxRemovedTombstoneMarkers {
		t.Fatalf("removed markers = %d, want the cap %d", got, maxRemovedTombstoneMarkers)
	}
	if _, ok := store.removed["ancient"]; ok {
		t.Fatal("a marker older than the TTL must be pruned")
	}
	if _, ok := store.removed[idFor(0)]; ok {
		t.Fatal("the oldest markers must be the ones evicted by the cap")
	}

	// A marker whose S3 delete is still owed is never evicted: dropping it is
	// exactly what lets the stale copy resurrect the tombstone.
	store.markRemovedLocked("owed", now.Add(-removedTombstoneMarkerTTL-time.Hour))
	store.pruneRemovedLocked(now, map[string]pendingOp{"owed": pendingDelete})
	if _, ok := store.removed["owed"]; !ok {
		t.Fatal("a marker with a pending S3 delete must survive pruning")
	}
}

func idFor(i int) string {
	return "ts-" + time.Unix(0, int64(i)).UTC().Format("150405.000000000")
}
