package manifest

import (
	"context"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Retired and pending keys: what the refresh must not adopt.
//
// The periodic refresh rebuilds the manifest from a bucket listing, and the
// listing cannot tell a live file from an object the manifest deliberately let
// go of. Two kinds of object exist in the bucket without belonging in the
// manifest:
//
//   - RETIRED: a key a publish replaced (a rewrite's source, a compaction's
//     sources), a rewrite or compaction output that was abandoned, or a key
//     removed on someone else's behalf (retention, a peer's manifest push),
//     whose object has not been deleted yet — or whose delete failed.
//     Re-adopting one serves its rows next to the replacement's copy of them,
//     brings back rows a delete removed once the tombstone retires, and keeps it
//     away from the orphan sweep, which only reclaims unmanifested objects.
//   - PENDING: an output uploaded but not yet published. Adopting it serves its
//     rows next to its still-registered source, and registers it with only the
//     listing's bare size — the publish that follows is then a no-op on the
//     duplicate key and its row counts, labels and aggregates are never stored.
//
// Retired keys are remembered with the time they retired and forgotten once a
// listing that began after that time no longer contains them (the object is
// gone), when an explicit reclaim confirms the delete, or after retiredKeyTTL,
// with the set capped at maxRetiredKeys (oldest first) — so it is bounded by
// the objects still awaiting deletion. They are persisted with the snapshot.
//
// Pending keys are in-memory only, on purpose. Whether an upload was published
// is not something a periodic snapshot can know: a snapshot taken between the
// upload and the publish would, after a crash, describe as unpublished an output
// whose sources were already deleted. The delete rewriter records its intent
// durably on the tombstone and resolves it at startup instead.
//
// Objects the manifest never knew — files flushed by a peer, or flushed after
// the snapshot a node restarted from — are still adopted: that is what the
// refresh is for.

const (
	retiredKeyTTL  = 7 * 24 * time.Hour
	maxRetiredKeys = 100_000
	// maxRecentAdds bounds the add log between refreshes (it is trimmed to the
	// newest entries in amortised chunks). Every accepted refresh drops the
	// entries older than its listing.
	maxRecentAdds = 50_000
)

// recentAdd is one AddFile, in the order they happened. The log is append-only
// under m.mu with non-decreasing times, so the entries newer than a listing's
// start are a suffix found by binary search.
type recentAdd struct {
	key string
	at  time.Time
}

// RetiredKey is a key the manifest deliberately stopped listing while its object
// may still exist.
type RetiredKey struct {
	Key string
	// At is when the key was retired.
	At time.Time
	// By is the key that replaced it; empty when there was no replacement.
	By string
	// Reclaim reports that this process superseded the object — a publish
	// replaced it or an output was abandoned — and owes its deletion. A key
	// removed on another component's behalf is only kept out of the refresh.
	Reclaim bool
}

// retireLocked records key as retired. Caller holds m.mu (write).
func (m *Manifest) retireLocked(rk RetiredKey) {
	if m.retired == nil {
		m.retired = make(map[string]RetiredKey)
	}
	if rk.At.IsZero() {
		rk.At = time.Now()
	}
	if cur, ok := m.retired[rk.Key]; ok {
		// A second retirement never downgrades an owed reclaim.
		rk.Reclaim = rk.Reclaim || cur.Reclaim
		if rk.By == "" {
			rk.By = cur.By
		}
	}
	m.retired[rk.Key] = rk
	delete(m.pending, rk.Key)
	metrics.ManifestRetiredKeys.Set(int64(len(m.retired)))
}

// noteAddedLocked records that key entered the manifest now and is therefore
// neither pending nor retired. Caller holds m.mu (write).
func (m *Manifest) noteAddedLocked(key string, now time.Time) {
	delete(m.pending, key)
	if _, ok := m.retired[key]; ok {
		delete(m.retired, key)
		metrics.ManifestRetiredKeys.Set(int64(len(m.retired)))
	}
	if n := len(m.recentAdds); n > 0 && now.Before(m.recentAdds[n-1].at) {
		now = m.recentAdds[n-1].at // keep the log sorted
	}
	m.recentAdds = append(m.recentAdds, recentAdd{key: key, at: now})
	if len(m.recentAdds) > 2*maxRecentAdds {
		m.recentAdds = append([]recentAdd(nil), m.recentAdds[len(m.recentAdds)-maxRecentAdds:]...)
	}
}

// recentAddsSinceLocked returns the adds at or after t. Caller holds m.mu.
func (m *Manifest) recentAddsSinceLocked(t time.Time) []recentAdd {
	i := sort.Search(len(m.recentAdds), func(i int) bool { return !m.recentAdds[i].at.Before(t) })
	return m.recentAdds[i:]
}

// MarkPending records that key is being uploaded and is not yet published, so
// a refresh running in between does not adopt it. The publish (AddFile,
// ReplaceFile, ReplaceFiles) clears it; AbandonPending retires it.
func (m *Manifest) MarkPending(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending == nil {
		m.pending = make(map[string]time.Time)
	}
	m.pending[key] = time.Now()
}

// AbandonPending retires an upload that will never be published — its publish
// was refused or failed — so neither a refresh nor a restart adopts it while its
// delete is outstanding. Safe to call whether or not the object was written.
func (m *Manifest) AbandonPending(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.byKey[key]; ok {
		m.removeFileLocked(p, key)
	}
	m.retireLocked(RetiredKey{Key: key, Reclaim: true})
}

// Retire removes key from the manifest if it is registered and remembers it as
// retired. by names the replacement, if any; reclaim records that the caller
// owes the object's deletion. Returns whether an entry was removed.
func (m *Manifest) Retire(key, by string, reclaim bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	removed := false
	if p, ok := m.byKey[key]; ok {
		removed = m.removeFileLocked(p, key)
	}
	m.retireLocked(RetiredKey{Key: key, By: by, Reclaim: reclaim})
	return removed
}

// Unretire forgets that key was retired or pending, so a refresh adopts its
// object again. Used when a durable record proves the object is live — a
// rewrite whose publish was recorded, found unregistered after a restart.
func (m *Manifest) Unretire(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, wasRetired := m.retired[key]
	_, wasPending := m.pending[key]
	delete(m.retired, key)
	delete(m.pending, key)
	metrics.ManifestRetiredKeys.Set(int64(len(m.retired)))
	return wasRetired || wasPending
}

// UnretireIfReplacedBy forgets key's retirement only if it was retired in favour
// of replacement — undoing one specific publish without touching a retirement
// some other publish made.
func (m *Manifest) UnretireIfReplacedBy(key, replacement string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	rk, ok := m.retired[key]
	if !ok || rk.By != replacement {
		return false
	}
	delete(m.retired, key)
	metrics.ManifestRetiredKeys.Set(int64(len(m.retired)))
	return true
}

// ForgetRetired drops key from the retired set once its object is confirmed
// deleted.
func (m *Manifest) ForgetRetired(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.retired[key]; ok {
		delete(m.retired, key)
		metrics.ManifestRetiredKeys.Set(int64(len(m.retired)))
	}
}

// LookupRetired returns key's retirement record, if it is retired.
func (m *Manifest) LookupRetired(key string) (RetiredKey, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rk, ok := m.retired[key]
	return rk, ok
}

// IsRetired reports whether key is retired.
func (m *Manifest) IsRetired(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.retired[key]
	return ok
}

// IsPending reports whether key is an unpublished upload.
func (m *Manifest) IsPending(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.pending[key]
	return ok
}

// RetiredKeys returns the retired set, oldest first.
func (m *Manifest) RetiredKeys() []RetiredKey {
	m.mu.RLock()
	out := make([]RetiredKey, 0, len(m.retired))
	for _, rk := range m.retired {
		out = append(out, rk)
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].At.Equal(out[j].At) {
			return out[i].At.Before(out[j].At)
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// ReclaimRetired deletes the objects of retired keys this process owes (Reclaim
// set) and forgets each one whose delete succeeds. It is the retry for every
// delete of a superseded or abandoned object that failed where it was first
// attempted; without it such an object stays in the bucket until the orphan
// sweep's age gate. At most limit objects are attempted (0 = all). Returns how
// many were deleted and how many deletes failed.
func (m *Manifest) ReclaimRetired(ctx context.Context, del func(ctx context.Context, key string) error, limit int) (deleted, failed int) {
	for _, rk := range m.RetiredKeys() {
		if limit > 0 && deleted+failed >= limit {
			break
		}
		if !rk.Reclaim || ctx.Err() != nil {
			continue
		}
		if m.HasKey(rk.Key) {
			continue
		}
		if err := del(ctx, rk.Key); err != nil {
			failed++
			metrics.ManifestRetiredReclaimErrors.Inc()
			logger.Warnf("retired object not deleted (will retry); key=%s: %s", rk.Key, err)
			continue
		}
		m.mu.Lock()
		if cur, ok := m.retired[rk.Key]; ok && cur.At.Equal(rk.At) {
			delete(m.retired, rk.Key)
		}
		metrics.ManifestRetiredKeys.Set(int64(len(m.retired)))
		m.mu.Unlock()
		deleted++
		metrics.ManifestRetiredReclaimed.Inc()
	}
	return deleted, failed
}

// pruneRetiredLocked applies retiredKeyTTL and maxRetiredKeys. Caller holds
// m.mu (write).
func (m *Manifest) pruneRetiredLocked(now time.Time) {
	for k, rk := range m.retired {
		if now.Sub(rk.At) > retiredKeyTTL {
			delete(m.retired, k)
			metrics.ManifestRetiredEvicted.Inc("ttl")
		}
	}
	if len(m.retired) > maxRetiredKeys {
		keys := make([]string, 0, len(m.retired))
		for k := range m.retired {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return m.retired[keys[i]].At.Before(m.retired[keys[j]].At) })
		for _, k := range keys[:len(keys)-maxRetiredKeys] {
			delete(m.retired, k)
			metrics.ManifestRetiredEvicted.Inc("cap")
		}
	}
	metrics.ManifestRetiredKeys.Set(int64(len(m.retired)))
}

// refreshExclusionsLocked removes retired and pending keys from a listing,
// preserves tracked files published after the listing began, and returns the
// retired keys the listing proves deleted — to be forgotten only if the refresh
// is accepted, since a listing the cliff guard rejects proves nothing. Caller
// holds m.mu (write).
func (m *Manifest) refreshExclusionsLocked(files map[string][]FileInfo, listStart time.Time) (confirmedGone []string) {
	listed := make(map[string]bool)
	for partition, pFiles := range files {
		kept := pFiles[:0]
		for _, fi := range pFiles {
			listed[fi.Key] = true
			if _, ok := m.retired[fi.Key]; ok {
				metrics.ManifestRefreshSkipped.Inc("retired")
				continue
			}
			if _, ok := m.pending[fi.Key]; ok {
				metrics.ManifestRefreshSkipped.Inc("pending")
				continue
			}
			kept = append(kept, fi)
		}
		if len(kept) == 0 {
			delete(files, partition)
		} else {
			files[partition] = kept
		}
	}

	for k, rk := range m.retired {
		if !listed[k] && rk.At.Before(listStart) {
			confirmedGone = append(confirmedGone, k)
		}
	}

	// A listing cannot contain what was published after it began. Keep those
	// files rather than drop them until the next refresh — for a compaction
	// output, that would hide rows whose sources are already retired.
	for _, ra := range m.recentAddsSinceLocked(listStart) {
		k := ra.key
		if listed[k] {
			continue
		}
		listed[k] = true // appended once even if added twice
		p, ok := m.byKey[k]
		if !ok {
			continue // removed again since
		}
		for _, fi := range m.files[p] {
			if fi.Key == k {
				files[p] = append(files[p], fi)
				metrics.ManifestRefreshSkipped.Inc("published_during_listing")
				break
			}
		}
	}
	return confirmedGone
}

// afterAcceptedRefreshLocked forgets what an accepted listing settled. Caller
// holds m.mu (write).
func (m *Manifest) afterAcceptedRefreshLocked(confirmedGone []string, listStart time.Time) {
	for _, k := range confirmedGone {
		delete(m.retired, k)
	}
	if newer := m.recentAddsSinceLocked(listStart); len(newer) < len(m.recentAdds) {
		m.recentAdds = append([]recentAdd(nil), newer...)
	}
	m.pruneRetiredLocked(time.Now())
}
