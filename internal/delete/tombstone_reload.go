package delete

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Reloading the tombstone objects.
//
// A pod learns of a tombstone it did not create from S3 only. Restore reads the
// objects once, at start; with several pods that left a delete taken by one pod
// unenforced on the others until they restarted, and left it unrewritten until
// a writer pod restarted. ReloadFromS3 is the same read, repeated on a timer by
// every pod: it lists the tombstone objects, fetches the ones whose ETag changed,
// and merges them the way a restore does.
//
// It never writes: no PUT, no DELETE. What it may do is change this process's
// memory: add a record, merge progress made elsewhere, and drop a tombstone that
// was removed elsewhere.
//
// Removal is the part that needs care, because a pod must not bring back a
// tombstone that was removed and must not drop one that is merely not yet in S3:
//
//   - A removal is written first as a retired record (Tombstone.Retired) over the
//     same key, and the object is deleted only after a grace period longer than
//     several reload intervals. A pod that reloads inside the grace period reads
//     "retired"; a pod that restores later reads it too; a delete that fails
//     leaves the retired record, never an active one.
//   - An id that is absent from a COMPLETE listing is dropped only if this pod had
//     seen its object, has no write of its own pending for it, and has no unfinished
//     rewrite of it. An incomplete listing (any error) drops nothing: absence from a
//     partial listing says nothing about the object.
//   - A retired record never drops a tombstone this pod has an unfinished rewrite
//     of: the rewrite's records must stay until it settles.

// S3ETagLister is the optional capability a reload uses to fetch only the
// objects that changed: the keys under prefix with their ETags. A pool without
// it makes every reload fetch every object.
type S3ETagLister interface {
	ListETags(ctx context.Context, prefix string) (map[string]string, error)
}

// ReloadResult is what one reload did.
type ReloadResult struct {
	Listed     int // objects in the listing
	Fetched    int // objects fetched because their ETag changed
	Added      int // records for tombstones this pod did not have
	Updated    int // records merged with progress made elsewhere
	Retired    int // tombstones dropped because a retired record said so
	Dropped    int // tombstones dropped because the object left a complete listing
	Unreadable int // objects that could not be read or parsed
}

// SetReaper marks the store as owned by a pod that runs maintenance: it owes
// the delete of a retired tombstone's object once the grace has passed. Call it
// before Restore.
func (s *TombstoneStore) SetReaper(on bool) {
	s.mu.Lock()
	s.reaper = on
	s.mu.Unlock()
}

// SetRetireGrace sets how long a retired record stays in S3 before its object is
// deleted. It must be longer than several reload intervals, so every pod reads
// the retirement before the object disappears. Zero deletes at once. Call it
// before EnablePersistence.
func (s *TombstoneStore) SetRetireGrace(d time.Duration) {
	s.mu.Lock()
	s.retireGrace = d
	if s.persist != nil {
		s.persist.grace = d
	}
	s.mu.Unlock()
}

// persistRemoval writes a removal through. With a retire grace the object first
// becomes a retired record and is deleted after the grace; without one it is
// deleted at once.
func (s *TombstoneStore) persistRemoval(p *tombstonePersistence, old Tombstone, had bool, id string, ver uint64) {
	if p == nil {
		return
	}
	s.mu.RLock()
	grace := s.retireGrace
	s.mu.RUnlock()
	if !had || grace <= 0 || p.pool == nil {
		s.persistChange(p, id, pendingDelete, ver)
		return
	}
	old.Retired = true
	old.RetiredAt = time.Now()
	p.mu.Lock()
	p.retired[id] = old
	p.grace = grace
	p.mu.Unlock()
	s.persistChange(p, id, pendingRetire, ver)
}

// idOfKey returns the tombstone id of an object key under keyPrefix.
func idOfKey(keyPrefix, key string) (string, bool) {
	rest := strings.TrimPrefix(key, keyPrefix)
	if rest == key || !strings.HasSuffix(rest, ".json") || strings.Contains(rest, "/") {
		return "", false
	}
	return strings.TrimSuffix(rest, ".json"), true
}

type reloadedRecord struct {
	ts  Tombstone
	tag string
}

// ReloadFromS3 re-reads the tombstone objects and applies what changed. See the
// comment above for the rules; it issues no PUT and no DELETE.
func (s *TombstoneStore) ReloadFromS3(ctx context.Context) (ReloadResult, error) {
	var res ReloadResult
	s.mu.RLock()
	cfg := s.restoreCfg
	restorePending := s.s3RestorePending
	s.mu.RUnlock()
	if cfg.Pool == nil {
		return res, nil
	}
	if restorePending {
		// A restore that never completed is the bigger gap, and it reads everything.
		if !s.RetryS3Restore(ctx) {
			metrics.DeleteTombstoneReload.Inc("list_failed")
			return res, errors.New("tombstone restore from S3 is still pending")
		}
		return res, nil
	}

	keyPrefix := normalizeTombstonePrefix(cfg.Prefix)
	var etags map[string]string
	var keys []string
	if l, ok := cfg.Pool.(S3ETagLister); ok {
		m, err := l.ListETags(ctx, keyPrefix)
		if err != nil {
			metrics.DeleteTombstoneReload.Inc("list_failed")
			return res, fmt.Errorf("list tombstones: %w", err)
		}
		etags = m
		for k := range m {
			keys = append(keys, k)
		}
	} else {
		ks, err := cfg.Pool.List(ctx, keyPrefix)
		if err != nil {
			metrics.DeleteTombstoneReload.Inc("list_failed")
			return res, fmt.Errorf("list tombstones: %w", err)
		}
		keys = ks
	}

	present := make(map[string]bool, len(keys))
	var fetched []reloadedRecord
	for _, key := range keys {
		id, ok := idOfKey(keyPrefix, key)
		if !ok {
			continue
		}
		present[id] = true
		tag := etags[key]
		s.mu.RLock()
		prev, seen := s.seenS3[id]
		s.mu.RUnlock()
		if seen && tag != "" && prev == tag {
			continue
		}
		data, err := cfg.Pool.Download(ctx, key)
		if err != nil {
			res.Unreadable++
			logger.Warnf("tombstone reload: download %s failed: %s", key, err)
			continue
		}
		var ts Tombstone
		if err := json.Unmarshal(data, &ts); err != nil || ts.ID != id {
			res.Unreadable++
			logger.Warnf("tombstone reload: %s is not a readable tombstone record", key)
			continue
		}
		res.Fetched++
		fetched = append(fetched, reloadedRecord{ts: ts, tag: tag})
	}
	res.Listed = len(present)

	now := time.Now()
	s.mu.Lock()
	for _, f := range fetched {
		s.applyReloadedLocked(f.ts, f.tag, now, &res)
	}
	// Absence from a complete listing. The listing above is complete: any error
	// returned before this point.
	var owed map[string]pendingEntry
	if p := s.persist; p != nil {
		p.mu.Lock()
		owed = make(map[string]pendingEntry, len(p.pending))
		for id, e := range p.pending {
			owed[id] = e
		}
		p.mu.Unlock()
	}
	for id := range s.seenS3 {
		if present[id] {
			continue
		}
		delete(s.seenS3, id)
		cur, ok := s.tombstones[id]
		if !ok {
			continue
		}
		if _, writing := owed[id]; writing || len(cur.Superseded) > 0 {
			continue
		}
		delete(s.tombstones, id)
		s.forgetFilterLocked(cur)
		s.markRemovedLocked(id, now)
		s.bumpLocked(id)
		res.Dropped++
	}
	s.mu.Unlock()

	if res.Added+res.Updated+res.Retired+res.Dropped > 0 {
		s.updateActiveGauges()
	}
	metrics.DeleteTombstoneReloadRecords.Add("added", res.Added)
	metrics.DeleteTombstoneReloadRecords.Add("updated", res.Updated)
	metrics.DeleteTombstoneReloadRecords.Add("retired", res.Retired)
	metrics.DeleteTombstoneReloadRecords.Add("dropped", res.Dropped)
	if res.Unreadable > 0 {
		metrics.DeleteTombstoneReload.Inc("fetch_failed")
		return res, fmt.Errorf("reloaded %d of %d changed tombstone objects", res.Fetched, res.Fetched+res.Unreadable)
	}
	metrics.DeleteTombstoneReload.Inc("ok")
	return res, nil
}

// applyReloadedLocked applies one fetched record. Caller holds s.mu.
func (s *TombstoneStore) applyReloadedLocked(ts Tombstone, tag string, now time.Time, res *ReloadResult) {
	if ts.Retired {
		cur, ok := s.tombstones[ts.ID]
		if ok && len(cur.Superseded) > 0 {
			// A rewrite of this tombstone is unfinished here: its records are
			// the only trace of a replacement object. Keep the tombstone; the
			// seen tag stays unchanged so the retirement is read again.
			logger.Warnf("tombstone reload: %s is retired elsewhere but has an unfinished rewrite here; keeping it", ts.ID)
			return
		}
		s.seenS3[ts.ID] = tag
		if ok {
			delete(s.tombstones, ts.ID)
			s.forgetFilterLocked(cur)
			s.bumpLocked(ts.ID)
			res.Retired++
		}
		at := ts.RetiredAt
		if at.IsZero() {
			at = now
		}
		s.markRemovedLocked(ts.ID, at)
		s.queueReapLocked(ts, now)
		return
	}
	s.seenS3[ts.ID] = tag
	if s.supersededByMarkerLocked(ts) || len(ts.Tenants) == 0 {
		return
	}
	cur, ok := s.tombstones[ts.ID]
	if !ok {
		s.mergeLoadedLocked(ts)
		res.Added++
		return
	}
	if sameRecord(cur, ts) {
		return
	}
	s.mergeLoadedLocked(ts)
	res.Updated++
}

// sameRecord reports whether two copies of a tombstone are identical.
func sameRecord(a, b Tombstone) bool {
	x, errA := json.Marshal(a)
	y, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(x) == string(y)
}

// queueReapLocked owes the delete of a retired tombstone's object when this pod
// is a reaper and the grace has passed. The delete runs in the next flush; the
// reload itself writes nothing. Caller holds s.mu.
func (s *TombstoneStore) queueReapLocked(ts Tombstone, now time.Time) {
	if !s.reaper || ts.RetiredAt.IsZero() || now.Sub(ts.RetiredAt) < s.retireGrace {
		return
	}
	p := s.persist
	if p == nil {
		// Before persistence is armed (a restore at start): park the delete,
		// EnablePersistence queues it.
		if s.staleS3 == nil {
			s.staleS3 = make(map[string]bool)
		}
		s.staleS3[ts.ID] = true
		return
	}
	p.mu.Lock()
	if cur, ok := p.pending[ts.ID]; !ok || cur.op == pendingDelete {
		p.pending[ts.ID] = pendingEntry{op: pendingDelete, ver: s.bumpLocked(ts.ID)}
	}
	p.mu.Unlock()
}
