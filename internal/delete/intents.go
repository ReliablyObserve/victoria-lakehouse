package delete

import (
	"context"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

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
	resolved := 0
	for _, ts := range store.Active() {
		for _, key := range sortedSupersededKeys(ts) {
			sup := ts.Superseded[key]
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
				metrics.DeleteRewriteInterrupted.Inc("published")
			case SupersessionDiscarded:
				if sup.NewKey != "" {
					m.Retire(sup.NewKey, "", true)
				}
				metrics.DeleteRewriteInterrupted.Inc("discarded")
			}
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
		sup := ts.Superseded[key]
		switch effectiveState(s.manifest, key, sup) {
		case SupersessionPrepared:
			undoInManifest(s.manifest, key, sup)
			s.discard(ctx, id, key, sup.NewKey)
			metrics.DeleteRewriteInterrupted.Inc("undone")
		case SupersessionPublished:
			if sup.State != SupersessionPublished {
				recordPublished(s.store, id, key, sup.NewKey)
			}
			s.commit(ctx, id, key, sup.NewKey)
		case SupersessionDiscarded:
			s.discard(ctx, id, key, sup.NewKey)
		}
		s.store.releaseKey(key)
	}
}

// commit deletes a published rewrite's superseded object and, once it is gone,
// clears the record. The source stays retired in the manifest until then, so no
// refresh can adopt it back in the meantime.
func (s *RewriteScheduler) commit(ctx context.Context, id, source, newKey string) bool {
	s.manifest.Retire(source, replacedBy(newKey), true)
	if err := s.rewriter.deleteObject(ctx, source); err != nil {
		metrics.DeleteRewriteOldObjectErrors.Inc()
		logger.Warnf("superseded object not deleted (retried on the next pass): %s; key=%s, tombstone=%s", err, source, id)
		return false
	}
	s.manifest.ForgetRetired(source)
	clearRecord(s.store, id, source, newKey)
	return true
}

// discard retires and deletes an abandoned replacement and, once it is gone,
// clears the record.
func (s *RewriteScheduler) discard(ctx context.Context, id, source, newKey string) bool {
	_, _ = s.store.Update(id, func(ts *Tombstone) bool {
		cur, ok := ts.Superseded[source]
		if !ok || cur.NewKey != newKey || cur.State == SupersessionDiscarded {
			return false
		}
		ts.Superseded[source] = Supersession{NewKey: newKey, State: SupersessionDiscarded, At: time.Now()}
		return true
	})
	if newKey != "" {
		s.manifest.AbandonPending(newKey)
		if err := s.rewriter.deleteObject(ctx, newKey); err != nil {
			metrics.DeleteRewriteAbandonedObjectErrors.Inc()
			logger.Warnf("abandoned replacement not deleted (retried on the next pass): %s; key=%s, tombstone=%s", err, newKey, id)
			return false
		}
		s.manifest.ForgetRetired(newKey)
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
