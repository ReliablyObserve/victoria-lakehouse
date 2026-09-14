package delete

import (
	"context"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Interrupted rewrites.
//
// A rewrite touches three things that cannot be updated atomically together:
// the bucket (the replacement object, then the superseded one), the manifest
// (the swap) and the tombstone (the bookkeeping). Each step is recorded on the
// tombstone BEFORE the step that depends on it, as a Supersession keyed by the
// source:
//
//	prepared   written before the replacement is uploaded
//	published  written after the manifest swap, before peers, pmeta or the
//	           delete of the superseded object can act on it
//	(cleared)  in the same update that follows the superseded object's delete
//	discarded  written when the publish is refused or fails, cleared once the
//	           abandoned replacement is deleted
//
// The tombstone store is durable on every change (disk and S3), the manifest
// only as of its last snapshot. So after a crash the record, not the manifest,
// says what happened, and resolving it is deterministic:
//
//   - prepared: nothing outside this process can have seen the replacement —
//     peers, pmeta and the superseded object's delete all wait for "published".
//     The rewrite is undone: the replacement is retired (a refresh will not
//     adopt it; it is deleted), and if a snapshot recorded the swap, the source
//     comes back. The rewrite simply runs again.
//   - published: the replacement is the live copy. The source is retired and
//     deleted; the replacement is left to the manifest or the refresh.
//   - discarded: the replacement is retired and deleted.
//
// The one exception to "prepared is undone": a replacement that a later
// publish has already superseded in the manifest (merged by a compaction in
// the instant between the swap and its record). Its rows live in that publish's
// output, so the rewrite is finished instead, not undone.

// ResolveInterruptedRewrites applies, to a freshly restored manifest, every
// rewrite a previous process left unfinished, so the first manifest refresh
// neither adopts an unpublished replacement next to its source nor re-adopts a
// source whose replacement was published. It changes only the manifest's view
// and the records; the objects are deleted by the scheduler's next pass (see
// resumeRewrites). Call it after the manifest snapshot is loaded and before the
// first refresh. Returns how many records it resolved.
func ResolveInterruptedRewrites(store *TombstoneStore, m ManifestUpdater) int {
	if store == nil || m == nil {
		return 0
	}
	// A store whose S3 copy could not be read may hold records older than what
	// S3 has: undoing a rewrite on that basis would delete a live replacement.
	if store.S3RestorePending() {
		metrics.DeleteRewriteDeferred.Inc("restore_pending")
		return 0
	}
	resolved := 0
	for _, ts := range store.Active() {
		for _, key := range sortedSupersededKeys(ts) {
			// Claim the source: a rewrite in this process may own the record,
			// and the record it holds may be several steps further along than
			// the copy this loop read.
			if !store.claimKey(key) {
				continue
			}
			cur, ok := store.Get(ts.ID)
			if !ok {
				store.releaseKey(key)
				continue
			}
			sup, ok := cur.Superseded[key]
			if !ok {
				store.releaseKey(key)
				continue
			}
			switch effectiveState(m, key, sup) {
			case SupersessionPrepared:
				undoInManifest(m, key, sup)
				store.Update(ts.ID, func(cur *Tombstone) bool {
					if cur.Superseded[key].NewKey != sup.NewKey {
						return false
					}
					cur.Superseded[key] = Supersession{NewKey: sup.NewKey, State: SupersessionDiscarded, At: time.Now()}
					return true
				})
				metrics.DeleteRewriteInterrupted.Inc("undone")
			case SupersessionPublished:
				m.Retire(key, replacedBy(sup.NewKey), true)
				if sup.State != SupersessionPublished {
					recordPublished(store, ts.ID, key, sup.NewKey)
				}
				// Whether the record this process restored is the one every
				// durable target holds is not known yet, so the replacement is
				// held until a pass has made it durable: an undo is still
				// possible, and a compaction must not merge it meanwhile.
				m.Hold(sup.NewKey)
				metrics.DeleteRewriteInterrupted.Inc("published")
			case SupersessionDiscarded:
				if sup.NewKey != "" {
					m.Retire(sup.NewKey, "", true)
				}
				metrics.DeleteRewriteInterrupted.Inc("discarded")
			}
			store.releaseKey(key)
			resolved++
			logger.Infof("interrupted rewrite resolved; tombstone=%s, source=%s, replacement=%s, state=%s",
				ts.ID, key, sup.NewKey, sup.State)
		}
	}
	return resolved
}

// effectiveState is the state a record resolves as. A prepared record whose
// replacement the manifest shows superseded by a later publish is resolved as
// published (see the package comment above).
func effectiveState(m ManifestUpdater, source string, sup Supersession) string {
	if sup.State != SupersessionPrepared || sup.NewKey == "" {
		return sup.State
	}
	if rk, ok := m.LookupRetired(sup.NewKey); ok && rk.By != "" {
		return SupersessionPublished
	}
	return sup.State
}

// undoInManifest reverses whatever part of a prepared rewrite a manifest
// snapshot may have captured: the replacement is retired, and the source is
// restored only if it was retired by THIS rewrite.
func undoInManifest(m ManifestUpdater, source string, sup Supersession) {
	if sup.NewKey != "" {
		m.Retire(sup.NewKey, "", true)
	}
	m.UnretireIfReplacedBy(source, replacedBy(sup.NewKey))
	// The source is the live copy of its rows again, but the publish removed
	// its entry and only a refresh can rebuild it from the bucket. Until one
	// has, an absent entry for this key must not be read as "the object is
	// gone" — that is how a tombstone retires over a file nothing rewrote.
	if !m.HasKey(source) {
		m.ExpectInListing(source)
	}
}

// recordPublished marks a rewrite published and records its bookkeeping in one
// durable update: the source is reaped and the replacement is listed as
// clean.
func recordPublished(store *TombstoneStore, id, source, newKey string) bool {
	_, ok := store.Update(id, func(ts *Tombstone) bool {
		if ts.Superseded == nil {
			ts.Superseded = make(map[string]Supersession)
		}
		ts.Superseded[source] = Supersession{NewKey: newKey, State: SupersessionPublished, At: time.Now()}
		markHandled(ts, source, newKey)
		return true
	})
	return ok
}

// resumeRewrites finishes, on the objects, every record of tombstone id that no
// rewrite in this process is working on: it deletes superseded and abandoned
// objects and clears the records whose delete succeeded. A failed delete leaves
// its record for the next pass — the durable retry queue for those deletes.
func (s *RewriteScheduler) resumeRewrites(ctx context.Context, id string) {
	ts, ok := s.store.Get(id)
	if !ok {
		return
	}
	for _, key := range sortedSupersededKeys(ts) {
		if !s.store.claimKey(key) {
			continue // a rewrite in this process owns it
		}
		// Re-read under the claim: the record this loop saw may be several
		// steps old, and acting on a stale `prepared` would delete the live
		// replacement of a rewrite that has since published.
		cur, ok := s.store.Get(id)
		if !ok {
			s.store.releaseKey(key)
			return
		}
		sup, recorded := cur.Superseded[key]
		if !recorded {
			s.store.releaseKey(key)
			continue
		}
		switch effectiveState(s.manifest, key, sup) {
		case SupersessionPrepared:
			undoInManifest(s.manifest, key, sup)
			s.discard(ctx, id, key, sup.NewKey)
			metrics.DeleteRewriteInterrupted.Inc("undone")
		case SupersessionPublished:
			s.finishPublished(ctx, id, key, sup)
		case SupersessionDiscarded:
			s.discard(ctx, id, key, sup.NewKey)
		}
		s.store.releaseKey(key)
	}
}

// finishPublished completes a rewrite whose swap has landed: it makes the
// record durable, announces the replacement once, and deletes the superseded
// object. Every step it cannot take yet leaves the state exactly as it is for
// the next pass — with the replacement held, because an undo is still possible
// until the record is durable.
func (s *RewriteScheduler) finishPublished(ctx context.Context, id, source string, sup Supersession) {
	if sup.State != SupersessionPublished {
		recordPublished(s.store, id, source, sup.NewKey)
	}
	if err := s.store.EnsureDurable(ctx, id); err != nil {
		metrics.DeleteRewriteDeferred.Inc("not_durable")
		s.manifest.Hold(sup.NewKey)
		return
	}

	// The replacement must be describable before peers are told the source is
	// gone. Right after a restart the manifest may not have adopted it yet;
	// once a listing has run, a replacement that is still missing was merged
	// or expired, and the rows live wherever that publish put them.
	published, manifested := s.manifest.GetFileByKey(sup.NewKey)
	if sup.NewKey != "" && !manifested {
		_, retired := s.manifest.LookupRetired(sup.NewKey)
		if !retired && !s.manifest.Listed() {
			metrics.DeleteRewriteDeferred.Inc("unlisted")
			return
		}
	}

	s.manifest.Release(sup.NewKey)
	if !s.alreadyHandedOff(source, sup.NewKey) {
		var added []manifest.FileInfo
		if manifested {
			added = []manifest.FileInfo{published}
		}
		if s.onPublished != nil {
			s.onPublished(added, []string{source}, nil)
		}
		s.markHandedOff(source, sup.NewKey)
	}
	s.commit(ctx, id, source, sup.NewKey)
}

// commit deletes a published rewrite's superseded object and, once it is gone,
// clears the record. The source stays retired in the manifest until then, so no
// refresh can adopt it back in the meantime.
func (s *RewriteScheduler) commit(ctx context.Context, id, source, newKey string) bool {
	// The record that says this rewrite published is what a restart reads
	// instead of undoing it. Deleting the source before that record is durable
	// is how the kept rows are lost.
	if err := s.store.EnsureDurable(ctx, id); err != nil {
		metrics.DeleteRewriteDeferred.Inc("not_durable")
		logger.Warnf("superseded object kept: the record authorising its delete is not durable: %s; key=%s, tombstone=%s", err, source, id)
		s.manifest.Hold(newKey)
		return false
	}
	s.manifest.Retire(source, replacedBy(newKey), true)
	if err := s.rewriter.deleteObject(ctx, source); err != nil {
		metrics.DeleteRewriteOldObjectErrors.Inc()
		logger.Warnf("superseded object not deleted (retried on the next pass): %s; key=%s, tombstone=%s", err, source, id)
		return false
	}
	s.manifest.ConfirmDeleted(source)
	clearRecord(s.store, id, source, newKey)
	return true
}

// discard retires and deletes an abandoned replacement and, once it is gone,
// clears the record. It acts only if the record it was called for is still the
// one on the tombstone and has not published since — a published rewrite's
// replacement is the live copy of its rows.
func (s *RewriteScheduler) discard(ctx context.Context, id, source, newKey string) bool {
	proceed := false
	_, _ = s.store.Update(id, func(ts *Tombstone) bool {
		cur, ok := ts.Superseded[source]
		if !ok || cur.NewKey != newKey || cur.State == SupersessionPublished {
			return false
		}
		proceed = true
		if cur.State == SupersessionDiscarded {
			return false // already recorded; nothing to store
		}
		ts.Superseded[source] = Supersession{NewKey: newKey, State: SupersessionDiscarded, At: time.Now()}
		return true
	})
	if !proceed {
		return false
	}
	if newKey != "" {
		s.manifest.Release(newKey)
		// Retire it first: from here the key names an object nothing will
		// publish, and a refresh that listed it must not adopt it.
		s.manifest.AbandonPending(newKey)
	}
	// Same rule as commit's, for the other outcome: the record that says this
	// replacement was abandoned is what a restart reads instead of adopting it,
	// so nothing is deleted until that record is durable. The object is kept and
	// the next pass retries — an abandoned object costs storage, a delete on the
	// strength of a record that never landed costs correctness.
	if err := s.store.EnsureDurable(ctx, id); err != nil {
		metrics.DeleteRewriteDeferred.Inc("not_durable")
		logger.Warnf("abandoned replacement kept: the record authorising its delete is not durable: %s; key=%s, tombstone=%s", err, newKey, id)
		return false
	}
	if newKey != "" {
		if err := s.rewriter.deleteObject(ctx, newKey); err != nil {
			metrics.DeleteRewriteAbandonedObjectErrors.Inc()
			logger.Warnf("abandoned replacement not deleted (retried on the next pass): %s; key=%s, tombstone=%s", err, newKey, id)
			return false
		}
		s.manifest.ConfirmDeleted(newKey)
	}
	clearRecord(s.store, id, source, newKey)
	return true
}

// clearRecord drops source's record if it still names newKey.
func clearRecord(store *TombstoneStore, id, source, newKey string) {
	store.Update(id, func(ts *Tombstone) bool {
		cur, ok := ts.Superseded[source]
		if !ok || cur.NewKey != newKey {
			return false
		}
		delete(ts.Superseded, source)
		if len(ts.Superseded) == 0 {
			ts.Superseded = nil
		}
		return true
	})
}

func sortedSupersededKeys(ts Tombstone) []string {
	keys := make([]string, 0, len(ts.Superseded))
	for k := range ts.Superseded {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
