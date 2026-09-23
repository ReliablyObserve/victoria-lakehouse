package delete

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Removed-tombstone markers.
//
// Removing a tombstone — an un-delete, or the retirement of a fully rewritten
// one — deletes the local record at once and the S3 object in the same call.
// When that S3 delete fails it is queued, and the queue lives in memory. Restore
// merges the disk copy with S3, so a crash before the retry used to bring the
// tombstone back from its stale S3 object: rows the user had restored were
// hidden again, and a permanent-mode tombstone went on to physically remove
// them.
//
// So every removal also leaves a marker (id → removedAt) in the disk copy.
// Restore drops any record the marker post-dates and re-queues the S3 delete the
// crash lost. A marker is kept while its S3 delete is owed, and otherwise for
// removedTombstoneMarkerTTL, capped at maxRemovedTombstoneMarkers (oldest
// evicted first), so the set stays bounded for the life of a deployment.
//
// The markers live on this node's disk. A node that boots without it (a new
// pod, a lost volume) restores from S3 alone and cannot know about a removal
// whose S3 delete never landed; see the multi-instance bounds in
// docs/operations.md.
const (
	removedTombstoneMarkerTTL  = 30 * 24 * time.Hour
	maxRemovedTombstoneMarkers = 10000

	// tombstonesFileFormat marks the disk copy's envelope. The previous release
	// wrote a bare id → record map, which LoadFromDisk still reads.
	tombstonesFileFormat    = 2
	tombstonesFileFormatKey = "lakehouse_tombstones_format"
)

// tombstonesFile is the disk copy's envelope.
type tombstonesFile struct {
	Format     int                  `json:"lakehouse_tombstones_format"`
	Tombstones map[string]Tombstone `json:"tombstones"`
	Removed    map[string]time.Time `json:"removed,omitempty"`
}

// markRemovedLocked records that id was removed at `at`. Caller holds s.mu.
func (s *TombstoneStore) markRemovedLocked(id string, at time.Time) {
	if s.removed == nil {
		s.removed = make(map[string]time.Time)
	}
	if cur, ok := s.removed[id]; !ok || at.After(cur) {
		s.removed[id] = at
	}
}

// pruneRemovedLocked applies the TTL and the cap. A marker whose S3 delete is
// still owed (owed[id] == pendingDelete) is never pruned: it is the only thing
// standing between the stale S3 object and a resurrected tombstone. Caller
// holds s.mu.
func (s *TombstoneStore) pruneRemovedLocked(now time.Time, owed map[string]pendingOp) {
	if len(s.removed) == 0 {
		return
	}
	keep := func(id string) bool { return owed[id] == pendingDelete }
	evicted := 0
	for id, at := range s.removed {
		if now.Sub(at) > removedTombstoneMarkerTTL && !keep(id) {
			delete(s.removed, id)
			evicted++
		}
	}
	if len(s.removed) > maxRemovedTombstoneMarkers {
		ids := make([]string, 0, len(s.removed))
		for id := range s.removed {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return s.removed[ids[i]].Before(s.removed[ids[j]]) })
		for _, id := range ids {
			if len(s.removed) <= maxRemovedTombstoneMarkers {
				break
			}
			if keep(id) {
				continue
			}
			delete(s.removed, id)
			evicted++
		}
	}
	if evicted > 0 {
		metrics.DeleteTombstoneRemovedMarkersEvicted.Add(evicted)
	}
}

// supersededByMarkerLocked reports whether a loaded record is older than a
// removal marker for its id — a stale copy of a tombstone removed since. A
// record created after the marker is a new delete that reuses the id and
// stands. Caller holds s.mu.
func (s *TombstoneStore) supersededByMarkerLocked(ts Tombstone) bool {
	at, ok := s.removed[ts.ID]
	return ok && !ts.CreatedAt.After(at)
}

// dropStaleLocked removes every in-memory record a marker post-dates and
// returns their ids, whose S3 copies are stale. Used when markers arrive after
// records (S3 loaded before disk). Caller holds s.mu.
func (s *TombstoneStore) dropStaleLocked() []string {
	var stale []string
	for id, ts := range s.tombstones {
		if s.supersededByMarkerLocked(ts) {
			delete(s.tombstones, id)
			s.forgetFilterLocked(ts)
			stale = append(stale, id)
		}
	}
	return stale
}

// owePendingS3DeletesLocked queues S3 deletes for stale copies found during a
// restore. Before persistence is enabled they are parked and queued by
// EnablePersistence. Caller holds s.mu.
func (s *TombstoneStore) owePendingS3DeletesLocked(ids []string) {
	if len(ids) == 0 {
		return
	}
	metrics.DeleteStartupInconsistencies.Add("removed_tombstone_still_in_s3", len(ids))
	logger.Warnf("tombstone restore: %d removed tombstone(s) still have an S3 copy; ignoring them and re-queueing the delete", len(ids))
	if p := s.persist; p != nil {
		p.mu.Lock()
		for _, id := range ids {
			p.pending[id] = pendingEntry{op: pendingDelete, ver: s.bumpLocked(id)}
		}
		p.mu.Unlock()
		return
	}
	if s.staleS3 == nil {
		s.staleS3 = make(map[string]bool, len(ids))
	}
	for _, id := range ids {
		s.staleS3[id] = true
	}
}

// decodeTombstonesFile reads either disk format.
func decodeTombstonesFile(data []byte) (tombstonesFile, error) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return tombstonesFile{}, fmt.Errorf("unmarshal tombstones: %w", err)
	}
	if _, enveloped := probe[tombstonesFileFormatKey]; enveloped {
		var f tombstonesFile
		if err := json.Unmarshal(data, &f); err != nil {
			return tombstonesFile{}, fmt.Errorf("unmarshal tombstones: %w", err)
		}
		return f, nil
	}
	// Previous release: a bare id → record map.
	var legacy map[string]Tombstone
	if err := json.Unmarshal(data, &legacy); err != nil {
		return tombstonesFile{}, fmt.Errorf("unmarshal tombstones: %w", err)
	}
	return tombstonesFile{Format: 1, Tombstones: legacy}, nil
}
