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
// gone), or after retiredKeyTTL, with the set capped at maxRetiredKeys (oldest
// first). They are persisted with the snapshot.
//
// A landed delete is NOT what forgets a key. The listing a refresh applies may
// have begun before the publish that retired the key and before the delete that
// removed its object, and S3 answers it from the state it read then: the object
// is in that answer whether or not it exists now. Dropping the record the moment
// the delete returns therefore leaves exactly the window the record exists for —
// the refresh adopts the deleted object back, next to whatever replaced it, as a
// bare listing entry with no row count and no aggregates. What a landed delete
// does is settle who owes what: ConfirmDeleted clears Reclaim (nothing left to
// delete, so ReclaimRetired stops retrying and the owed gauge stays honest) and
// sets Deleted, and the first accepted listing that began after the retirement
// forgets the key — the object was proven gone by a listing that could see it.
// So the set stays bounded by the objects a listing might still report, not by
// the deletes this process has done.
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

const retiredKeyTTL = 7 * 24 * time.Hour

// maxRetiredKeys caps the retired set and maxRecentAdds the add log between
// refreshes (trimmed to the newest entries in amortised chunks; every accepted
// refresh drops the entries older than its listing).
//
// Variables rather than constants so the tests that exercise the eviction
// ORDER can do it at a small cap: what matters there is which key goes first,
// and filling a 100,000-entry set for every such case costs minutes under the
// race detector without testing anything the small cap does not.
var (
	maxRetiredKeys = 100_000
	maxRecentAdds  = 50_000
)

// recentAdd is one AddFile, in the order they happened. The log is append-only
// under m.mu with non-decreasing times, so the entries newer than a listing's
// start are a suffix found by binary search.
type recentAdd struct {
	key string
	at  time.Time
}

// PendingKey is an upload that has not been published yet.
type PendingKey struct {
	Key  string
	At   time.Time
	Held bool
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
	// Deleted reports that the object is confirmed gone: a delete returned
	// success for this key. The record is kept anyway, because a listing that
	// began before the delete still reports the object and a refresh applying
	// it would adopt it back; it is dropped by the first accepted listing that
	// began after At. Deleted and Reclaim are mutually exclusive — there is
	// nothing left to reclaim.
	Deleted bool
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
		// A second retirement never downgrades an owed reclaim. It does clear
		// Deleted (rk carries none): retiring a key again says an object may
		// exist under it once more, so the record goes back to "may still be
		// listed" and its At restarts the window.
		rk.Reclaim = rk.Reclaim || cur.Reclaim
		if rk.By == "" {
			rk.By = cur.By
		}
	}
	m.retired[rk.Key] = rk
	delete(m.pending, rk.Key)
	m.updateRetiredGaugesLocked()
}

// noteAddedLocked records that key entered the manifest now and is therefore
// neither pending nor retired. Caller holds m.mu (write).
func (m *Manifest) noteAddedLocked(key string, now time.Time) {
	delete(m.pending, key)
	delete(m.awaitingAdoption, key)
	if _, ok := m.retired[key]; ok {
		delete(m.retired, key)
		m.updateRetiredGaugesLocked()
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

// ClaimPending reserves key for an upload that is not published yet: a refresh
// running in between does not adopt it, and the claim fails when the key is
// already registered, retired or claimed. Writers generate keys from a short
// random id, and an upload to a key that is already in use would overwrite a
// live object, so the claim is what makes the id collision-safe. The publish
// (AddFile, ReplaceFile, ReplaceFiles) clears it; AbandonPending retires it;
// ReleasePending drops it when nothing was written.
func (m *Manifest) ClaimPending(key string) bool {
	if key == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, taken := m.byKey[key]; taken {
		metrics.ManifestKeyClaimRejected.Inc("registered")
		return false
	}
	if _, taken := m.retired[key]; taken {
		metrics.ManifestKeyClaimRejected.Inc("retired")
		return false
	}
	if _, taken := m.pending[key]; taken {
		metrics.ManifestKeyClaimRejected.Inc("pending")
		return false
	}
	if m.pending == nil {
		m.pending = make(map[string]time.Time)
	}
	m.pending[key] = time.Now()
	return true
}

// ReleasePending drops a claim whose object was never written. Unlike
// AbandonPending it does not retire the key: there is nothing to delete, and
// retiring would keep a usable key out of the manifest for no reason.
func (m *Manifest) ReleasePending(key string) {
	m.mu.Lock()
	delete(m.pending, key)
	m.mu.Unlock()
}

// Hold marks a registered key as not yet safe for another publish to supersede:
// a rewrite has swapped it into the manifest but the record authorising that
// swap is not durable yet, so the swap may still be undone. A compaction that
// merged it in that window would take its rows into an output the undo knows
// nothing about. ReplaceFile, ReplaceFiles and RemoveFileIfPresent refuse a held
// key, and compaction does not select one. Holds are in-memory only: a restart
// re-derives them from the durable rewrite records.
func (m *Manifest) Hold(key string) {
	if key == "" {
		return
	}
	m.mu.Lock()
	if m.held == nil {
		m.held = make(map[string]bool)
	}
	m.held[key] = true
	metrics.ManifestHeldKeys.Set(int64(len(m.held)))
	m.mu.Unlock()
}

// Release lifts a Hold.
func (m *Manifest) Release(key string) {
	m.mu.Lock()
	delete(m.held, key)
	metrics.ManifestHeldKeys.Set(int64(len(m.held)))
	m.mu.Unlock()
}

// IsHeld reports whether key is held.
func (m *Manifest) IsHeld(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.held[key]
}

// HeldKeys returns the held keys, sorted.
func (m *Manifest) HeldKeys() []string {
	m.mu.RLock()
	out := make([]string, 0, len(m.held))
	for k := range m.held {
		out = append(out, k)
	}
	m.mu.RUnlock()
	sort.Strings(out)
	return out
}

// PendingKeys returns the unpublished uploads, oldest first.
func (m *Manifest) PendingKeys() []PendingKey {
	m.mu.RLock()
	out := make([]PendingKey, 0, len(m.pending))
	for k, at := range m.pending {
		out = append(out, PendingKey{Key: k, At: at, Held: m.held[k]})
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

// Listed reports whether this manifest has applied at least one accepted bucket
// listing since it was created or loaded from a snapshot. Until it has, "the
// manifest does not have this key" says nothing about whether the object
// exists: a snapshot can be older than the file, and a node that lost its disk
// starts empty. Callers that would infer "the object is gone" from an absent
// key must wait for this.
func (m *Manifest) Listed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.listed
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
	m.updateRetiredGaugesLocked()
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
	m.updateRetiredGaugesLocked()
	return true
}

// ConfirmDeleted records that key's object is gone: a delete returned success.
// The retirement itself is KEPT — see the note at the top of this file on why a
// landed delete cannot forget a key — with its delete no longer owed. The first
// accepted listing that began after the retirement drops it.
//
// A key that is not retired is left alone: nothing is claiming it must stay out
// of the refresh, so there is no record to settle.
func (m *Manifest) ConfirmDeleted(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rk, ok := m.retired[key]
	if !ok {
		return
	}
	rk.Reclaim = false
	rk.Deleted = true
	m.retired[key] = rk
	m.updateRetiredGaugesLocked()
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
		// Same rule as ConfirmDeleted's: the delete landed, so nothing is owed,
		// but the record stays until a listing that began after the retirement
		// proves the object gone. The At check keeps a key retired again since
		// this scan read it — a new object under that key — from being settled
		// by this older delete.
		if cur, ok := m.retired[rk.Key]; ok && cur.At.Equal(rk.At) {
			cur.Reclaim = false
			cur.Deleted = true
			m.retired[rk.Key] = cur
		}
		m.updateRetiredGaugesLocked()
		m.mu.Unlock()
		deleted++
		metrics.ManifestRetiredReclaimed.Inc()
	}
	return deleted, failed
}

// pruneRetiredLocked applies retiredKeyTTL and maxRetiredKeys, evicting the
// keys whose loss costs least first.
//
// A key whose delete this process owes (Reclaim) is the expensive one to
// forget: its object is still in the bucket, so the next refresh adopts it back
// next to whatever replaced it. A key removed on another component's behalf
// (retention, which deletes first; a peer's push, whose own node owes the
// delete) usually has no object left at all, and a Deleted key provably has
// none — it is held only until a listing old enough to still report it can no
// longer be applied. So the TTL spares owed keys alone, and the cap evicts the
// provably-gone keys first, the rest of the unowed next, and an owed key last.
// Caller holds m.mu (write).
func (m *Manifest) pruneRetiredLocked(now time.Time) {
	for k, rk := range m.retired {
		if !rk.Reclaim && now.Sub(rk.At) > retiredKeyTTL {
			delete(m.retired, k)
			metrics.ManifestRetiredEvicted.Inc("ttl")
		}
	}
	if len(m.retired) > maxRetiredKeys {
		keys := make([]string, 0, len(m.retired))
		for k := range m.retired {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			ri, rj := m.retired[keys[i]], m.retired[keys[j]]
			if ri.Deleted != rj.Deleted {
				return ri.Deleted // the object is gone: cheapest to forget
			}
			if ri.Reclaim != rj.Reclaim {
				return !ri.Reclaim // evict keys nobody here owes a delete for first
			}
			return ri.At.Before(rj.At)
		})
		for _, k := range keys[:len(keys)-maxRetiredKeys] {
			reason := "cap"
			switch {
			case m.retired[k].Reclaim:
				// The object is still in the bucket and its delete is owed:
				// forgetting it means the next refresh serves it again.
				reason = "cap_delete_owed"
			case m.retired[k].Deleted:
				// The object is gone, but a listing that began before the
				// delete can still report it: forgetting the guard early is
				// how a deleted object is adopted back.
				reason = "cap_delete_landed"
			}
			delete(m.retired, k)
			metrics.ManifestRetiredEvicted.Inc(reason)
		}
	}
	m.updateRetiredGaugesLocked()
}

// updateRetiredGaugesLocked publishes the retired-set size, how much of it this
// process still owes deletes for, and how much is held only as a guard against
// a listing older than a landed delete. Caller holds m.mu.
func (m *Manifest) updateRetiredGaugesLocked() {
	owed, landed := 0, 0
	for _, rk := range m.retired {
		switch {
		case rk.Reclaim:
			owed++
		case rk.Deleted:
			landed++
		}
	}
	metrics.ManifestRetiredKeys.Set(int64(len(m.retired)))
	metrics.ManifestRetiredReclaimOwed.Set(int64(owed))
	metrics.ManifestRetiredDeleteLanded.Set(int64(landed))
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
	// Counted, not just gauged: this is the only event that releases a guard,
	// so it is what tells an operator whether the set is draining. The gauge
	// alone cannot — on a node compacting faster than it refreshes, the set is
	// refilled as fast as it empties and never reads zero.
	if len(confirmedGone) > 0 {
		metrics.ManifestRetiredSettled.Add(len(confirmedGone))
	}
	if newer := m.recentAddsSinceLocked(listStart); len(newer) < len(m.recentAdds) {
		m.recentAdds = append([]recentAdd(nil), newer...)
	}
	// A listing that began after the expectation has now spoken: either it
	// found the key (this refresh adopted it) or it did not (the object is
	// gone). Either way the manifest is no longer the one that is behind.
	for k, at := range m.awaitingAdoption {
		if at.Before(listStart) {
			delete(m.awaitingAdoption, k)
		}
	}
	m.pruneRetiredLocked(time.Now())
}

// ExpectInListing records that key's object exists and belongs in the manifest
// again, but that no listing has adopted it back yet. It is what an undone
// rewrite leaves behind: the publish removed the source's entry, the crash
// stopped the rewrite, and the undo restored the source as the live copy of its
// rows — an entry only the next refresh can rebuild from the bucket.
//
// Until then the manifest not listing the key says nothing about the object,
// and anything that would read "not in the manifest" as "the object is gone"
// (the delete scheduler reaping a key, see AwaitingListing) must wait. Reading
// it the other way round un-hides every row the object still holds: the
// tombstone retires over a file nothing ever rewrote.
func (m *Manifest) ExpectInListing(key string) {
	if key == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.awaitingAdoption == nil {
		m.awaitingAdoption = make(map[string]time.Time)
	}
	// Bounded like every other cross-refresh memory here: a refresh clears the
	// entries it has spoken for, and the cap keeps a node whose refresh is
	// failing from growing this without limit.
	if len(m.awaitingAdoption) >= maxRecentAdds {
		m.dropOldestAwaitingAdoptionLocked()
	}
	m.awaitingAdoption[key] = time.Now()
}

// dropOldestAwaitingAdoptionLocked evicts the oldest expectation. Caller holds
// m.mu (write).
func (m *Manifest) dropOldestAwaitingAdoptionLocked() {
	var oldestKey string
	var oldest time.Time
	for k, at := range m.awaitingAdoption {
		if oldestKey == "" || at.Before(oldest) {
			oldestKey, oldest = k, at
		}
	}
	delete(m.awaitingAdoption, oldestKey)
}

// AwaitingListing reports that key is expected back in the manifest and no
// listing that began since has been applied yet, so the manifest cannot be used
// to decide whether its object still exists.
func (m *Manifest) AwaitingListing(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.awaitingAdoption[key]
	return ok
}
