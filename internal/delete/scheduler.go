package delete

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// RewriteSchedulerConfig holds the configuration for a RewriteScheduler.
type RewriteSchedulerConfig struct {
	Store          *TombstoneStore
	Rewriter       *Rewriter
	Detector       *StorageClassDetector
	RewriteDelay   time.Duration
	AllowedClasses []string
	MaxConcurrent  int

	// Manifest receives the rewritten object's registration. It is NOT
	// optional for rewriting: a rewrite that cannot be published leaves the
	// replacement unmanaged and the manifest pointing at a deleted key. When
	// Manifest is nil the scheduler refuses to rewrite anything and counts
	// lakehouse_delete_rewrite_skipped_no_manifest_total instead.
	Manifest ManifestUpdater

	// OnPublished is fired after a rewrite is published into the local
	// manifest and BEFORE the superseded object is deleted, with the same
	// shape as the compactor's OnCompacted hook: the replacement entry (none
	// when every row was removed), the superseded key, and the replacement's
	// bloom values. The embedder wires it to the same two consumers compaction
	// uses — the pmeta facet feed, which otherwise keeps serving the superseded
	// file's field values after the rows are gone, and the peer manifest push,
	// without which a peer keeps the superseded key and never learns the
	// replacement (and that peer's orphan sweep would reclaim it). Firing
	// before the delete means a peer learns the new key while the old object
	// can still be read. Optional.
	OnPublished func(added []manifest.FileInfo, removed []string, blooms map[string]map[string][]string)
}

// RewriteScheduler periodically processes pending tombstones by rewriting
// affected Parquet files to permanently remove deleted rows.
type RewriteScheduler struct {
	store    *TombstoneStore
	rewriter *Rewriter
	detector *StorageClassDetector
	manifest ManifestUpdater
	// keyTenant attributes an object key to its tenant, the way the read path
	// does, so a tenant-scoped tombstone is only ever applied to its tenants'
	// objects.
	keyTenant      KeyTenantFunc
	onPublished    func(added []manifest.FileInfo, removed []string, blooms map[string]map[string][]string)
	rewriteDelay   time.Duration
	allowedClasses map[string]bool
	maxConcurrent  int
	stopCh         chan struct{}

	// crashAt, set only by tests, stops a rewrite at the named step as if the
	// process died there: nothing after it runs. See the crash matrix.
	crashAt func(step string) bool

	// handedOff remembers the rewrites this process has already told pmeta and
	// the peers about, so a rewrite finished by a later pass is announced once.
	// In-memory: after a restart the announcement is simply repeated, which
	// both consumers take idempotently.
	handoffMu sync.Mutex
	handedOff map[string]bool
}

func (s *RewriteScheduler) markHandedOff(source, newKey string) {
	s.handoffMu.Lock()
	if s.handedOff == nil {
		s.handedOff = make(map[string]bool)
	}
	s.handedOff[source+"\x00"+newKey] = true
	s.handoffMu.Unlock()
}

func (s *RewriteScheduler) alreadyHandedOff(source, newKey string) bool {
	s.handoffMu.Lock()
	defer s.handoffMu.Unlock()
	return s.handedOff[source+"\x00"+newKey]
}

// NewRewriteScheduler creates a RewriteScheduler from the given config.
func NewRewriteScheduler(cfg RewriteSchedulerConfig) *RewriteScheduler {
	allowed := make(map[string]bool)
	classes := cfg.AllowedClasses
	if len(classes) == 0 {
		classes = []string{"STANDARD"}
	}
	for _, c := range classes {
		allowed[c] = true
	}

	maxConc := cfg.MaxConcurrent
	if maxConc <= 0 {
		maxConc = 1
	}

	return &RewriteScheduler{
		store:          cfg.Store,
		rewriter:       cfg.Rewriter,
		detector:       cfg.Detector,
		manifest:       cfg.Manifest,
		keyTenant:      KeyTenantParserOf(cfg.Manifest),
		onPublished:    cfg.OnPublished,
		rewriteDelay:   cfg.RewriteDelay,
		allowedClasses: allowed,
		maxConcurrent:  maxConc,
		stopCh:         make(chan struct{}),
	}
}

// Start launches a background goroutine that calls RunOnce at the given interval.
func (s *RewriteScheduler) Start(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				ctx := context.Background()
				s.RunOnce(ctx)
			}
		}
	}()
}

// Stop signals the background loop to exit.
func (s *RewriteScheduler) Stop() {
	close(s.stopCh)
}

// RunOnce processes all eligible active tombstones, rewriting affected files.
//
// Each key goes through the full prepare → publish → commit sequence with every
// step recorded on the tombstone before the step that depends on it (see
// intents.go); a failure at any step leaves a record the next pass resolves, and
// never leaves the manifest disagreeing with what is actually in the bucket.
//
// A tombstone retires only when every file that may still hold its rows has
// been handled — judged against the files that exist at that moment, not the
// snapshot taken when the delete was issued. Compaction can move a tombstone's
// rows into a new file (while the tombstone is inside its un-delete window, or
// racing this loop), and retiring on the stale snapshot would un-hide rows that
// were never removed.
func (s *RewriteScheduler) RunOnce(ctx context.Context) []RewriteResult {
	now := time.Now()
	var results []RewriteResult

	// A store whose S3 copy could not be read may be missing records and
	// progress other nodes (or this one, before the crash) wrote. Nothing may
	// be resolved, finished or retired on that; retry the restore instead.
	if s.store.S3RestorePending() {
		if !s.store.RetryS3Restore(ctx) {
			metrics.DeleteRewriteDeferred.Inc("restore_pending")
			return nil
		}
		ResolveInterruptedRewrites(s.store, s.manifest)
	}

	for _, snapshot := range s.store.Active() {
		// Hide mode never removes rows, and permanent/auto only once the
		// un-delete window (rewrite_delay) has passed.
		if !snapshot.EligibleForPhysicalRemoval(now, s.rewriteDelay) {
			continue
		}

		// Rewriting without a manifest orphans the replacement. Refuse the
		// whole tombstone rather than destroy data; the rows stay hidden by
		// the query-time filter in the meantime, which is the safe state.
		if s.manifest == nil {
			metrics.DeleteRewriteSkippedNoManifest.Inc()
			logger.Warnf("skipping rewrite: no manifest wired into the delete scheduler; tombstone=%s", snapshot.ID)
			continue
		}

		done, interrupted := s.processTombstone(ctx, now, snapshot.ID)
		results = append(results, done...)
		if interrupted {
			return results
		}
	}

	// Drain any tombstone durability writes S3 rejected earlier.
	s.store.FlushPending(ctx)

	// Run verification pass.
	s.Verify(ctx)

	return results
}

// processTombstone runs one tombstone's rewrite pass and retires it when
// nothing is left to do. interrupted reports that a test crash hook stopped the
// pass, which must then leave everything exactly as it is.
func (s *RewriteScheduler) processTombstone(ctx context.Context, now time.Time, id string) (results []RewriteResult, interrupted bool) {
	// Settle what earlier passes (or a process that crashed) left unfinished
	// before starting anything new on the same files.
	s.resumeRewrites(ctx, id)

	// Pull in every file that may now hold the tombstone's rows.
	s.discoverAffectedKeys(id)

	ts, ok := s.store.Get(id)
	if !ok {
		return nil, false
	}

	for _, key := range ts.AffectedKeys {
		if ts.Handled(key) {
			continue
		}
		if _, recorded := ts.Superseded[key]; recorded {
			continue // resumeRewrites owns it until its record clears
		}
		// A tenant-scoped tombstone never rewrites another tenant's object.
		// Nothing lists one (the delete API and discovery attribute keys by
		// tenant), so reaching here means a record was edited by hand or
		// written by a defect: the object holds none of this tombstone's rows
		// by definition, so it is recorded clean instead of rewritten.
		if !ts.AppliesToKey(s.keyTenant, key) {
			metrics.DeleteTenantScopeSkips.Inc("rewrite")
			logger.Errorf("tenant-scoped tombstone lists another tenant's object; not rewriting it; tombstone=%s, tenants=%v, key=%s", id, ts.Tenants, key)
			s.store.Update(id, func(cur *Tombstone) bool {
				cur.SetClean(key, true)
				return true
			})
			continue
		}

		// A key that is no longer in the manifest was rewritten already (a
		// crash lost the bookkeeping) or merged by compaction. Either way the
		// object is gone; if its rows moved, the file they moved into exists
		// in the manifest and the discovery pass before retirement picks it up.
		//
		// That inference holds only once the manifest has listed the bucket in
		// this process. Before then the manifest is a snapshot — possibly
		// empty, on a node that lost its disk — and "not in the manifest" says
		// nothing about the object, which may still hold every row this
		// tombstone hides.
		if !s.manifest.HasKey(key) {
			if !s.manifest.Listed() {
				metrics.DeleteRewriteDeferred.Inc("unlisted")
				continue
			}
			// And a key this process is waiting to see listed again — an
			// undone rewrite's source, whose entry the publish removed before
			// the crash — is absent because the manifest is behind, not
			// because the object is gone.
			if s.manifest.AwaitingListing(key) {
				metrics.DeleteRewriteDeferred.Inc("awaiting_listing")
				continue
			}
			metrics.DeleteRewriteAlreadyReaped.Inc()
			logger.Infof("rewrite key already superseded; key=%s, tombstone=%s", key, id)
			s.markReaped(id, key, "")
			continue
		}

		// Detect storage class from file age, honoring per-tenant lifecycle
		// overrides when the key carries a tenant prefix.
		fileAgeHours := now.Sub(ts.CreatedAt).Hours()
		class := s.detector.DetectForKey(fileAgeHours, key)
		if !s.allowedClasses[string(class)] {
			metrics.DeleteRewriteSkippedGlacier.Inc()
			logger.Infof("skipping rewrite: storage class not allowed; key=%s, class=%s", key, string(class))
			continue
		}

		if !s.store.claimKey(key) {
			continue // another rewrite in this process has it
		}
		result, outcome := s.rewriteOne(ctx, id, key, ts)
		s.store.releaseKey(key)
		switch outcome {
		case rewriteDone:
			results = append(results, *result)
		case rewriteInterrupted:
			return results, true
		case rewriteSuperseded, rewriteFailed, rewriteDeferred:
			// Superseded: the source was merged away and the tombstone follows
			// the rows to the compacted output via discovery. Failed: the next
			// pass retries.
		}
	}

	// Re-read the file set immediately before retiring: anything published
	// while this pass ran — a compaction that carried the rows forward — must
	// keep the tombstone active until it too has been rewritten.
	s.discoverAffectedKeys(id)

	// A tombstone whose every file has been handled has no work left.
	// Retiring it keeps Active() bounded and lets the manifest-metadata query
	// fast paths come back; leaving it active forever was the second half of
	// the original bug. Retirement is what stops hiding the rows, so it waits
	// for a manifest that has listed the bucket: the discovery above can only
	// be complete against a manifest that knows what the bucket holds.
	if s.manifest.Listed() {
		s.store.Complete(id)
	} else {
		metrics.DeleteRewriteDeferred.Inc("unlisted")
	}
	return results, false
}

// discoverAffectedKeys adds every manifest file overlapping the tombstone's
// range that it does not list yet, as pending. Returns how many were added.
func (s *RewriteScheduler) discoverAffectedKeys(id string) int {
	ts, ok := s.store.Get(id)
	if !ok {
		return 0
	}
	files := s.manifest.GetFilesForRange(ts.StartNs, ts.EndNs)
	var added, unreaped int
	s.store.Update(id, func(cur *Tombstone) bool {
		listed := make(map[string]bool, len(cur.AffectedKeys))
		for _, k := range cur.AffectedKeys {
			listed[k] = true
		}
		for _, fi := range files {
			// Only objects of a tenant the tombstone acts on can hold its rows.
			if !cur.AppliesToKey(s.keyTenant, fi.Key) {
				continue
			}
			// A key recorded as reaped means "its object is gone", so a
			// manifested one is a contradiction — a key that came back when an
			// interrupted rewrite was undone, or another node's copy of the
			// record. Make it pending again rather than let the tombstone
			// retire over a file the manifest is serving. Only once the
			// manifest has listed the bucket, though: before that it may be
			// serving a key from a snapshot older than the object's delete.
			if cur.Reaped[fi.Key] && s.manifest.Listed() {
				delete(cur.Reaped, fi.Key)
				unreaped++
			}
			if listed[fi.Key] {
				continue
			}
			cur.AffectedKeys = append(cur.AffectedKeys, fi.Key)
			listed[fi.Key] = true
			added++
		}
		return added+unreaped > 0
	})
	if unreaped > 0 {
		metrics.DeleteStartupInconsistencies.Add("reaped_key_still_manifested", unreaped)
		logger.Warnf("tombstone work list corrected; tombstone=%s, keys the manifest still serves: %d", id, unreaped)
	}
	if added > 0 {
		metrics.DeleteTombstoneKeysDiscovered.Add(added)
		logger.Infof("tombstone work list extended; tombstone=%s, new_keys=%d", id, added)
	}
	return added
}

// markReaped records that key no longer needs work for this tombstone, and —
// when the rewrite produced a replacement — lists that replacement as already
// clean, so discovery does not queue the scheduler's own output for another
// rewrite.
func (s *RewriteScheduler) markReaped(id, key, replacement string) {
	s.store.Update(id, func(ts *Tombstone) bool {
		markHandled(ts, key, replacement)
		return true
	})
}

// markHandled records key as reaped (its object is gone) and, when there is
// one, its replacement as clean (live, free of the tombstone's rows).
func markHandled(ts *Tombstone, key, replacement string) {
	ts.MarkReaped(key)
	if replacement != "" {
		if !containsString(ts.AffectedKeys, replacement) {
			ts.AffectedKeys = append(ts.AffectedKeys, replacement)
		}
		ts.SetClean(replacement, true)
	}
}

type rewriteOutcome int

const (
	rewriteFailed rewriteOutcome = iota
	rewriteDone
	rewriteSuperseded
	// rewriteDeferred: the rewrite is published but its record is not durable
	// yet, so nothing has been deleted and the next pass finishes it.
	rewriteDeferred
	// rewriteInterrupted: a test crash hook stopped the rewrite mid-flight.
	rewriteInterrupted
)

// Rewrite steps a test crash hook can stop at (see RewriteScheduler.crashAt).
const (
	stepIntentRecorded   = "intent-recorded"
	stepUploaded         = "uploaded"
	stepSwappedInMemory  = "swapped-in-memory"
	stepPublishRecorded  = "publish-recorded"
	stepNotified         = "notified"
	stepSupersededGone   = "superseded-object-deleted"
	stepDiscardRecorded  = "discard-recorded"
	stepAbandonedDeleted = "abandoned-object-deleted"
)

// crashed reports whether the test crash hook stops the rewrite at step.
func (s *RewriteScheduler) crashed(step string) bool {
	return s.crashAt != nil && s.crashAt(step)
}

// rewriteOne performs one key's rewrite, recording each step on the tombstone
// before the step that depends on it — and, before every step that deletes an
// object, waiting for that record to reach durable storage.
func (s *RewriteScheduler) rewriteOne(ctx context.Context, id, key string, ts Tombstone) (*RewriteResult, rewriteOutcome) {
	// Step 1 — prepare: read the source and build the replacement in memory.
	result, err := s.rewriter.Prepare(ctx, key, []Tombstone{ts})
	if err != nil {
		metrics.DeleteRewriteErrors.Inc()
		logger.Errorf("rewrite failed: %s; key=%s", err, key)
		return nil, rewriteFailed
	}
	if result.RowsRemoved == 0 {
		// The file holds no row this tombstone matches. Nothing is written
		// and nothing must be deleted; the live file is clean.
		metrics.DeleteRewriteTotal.Inc()
		s.store.Update(id, func(cur *Tombstone) bool {
			cur.SetClean(key, true)
			return true
		})
		return result, rewriteDone
	}

	// Step 2 — claim the replacement key. The id is short and random, so
	// uploading without a claim could overwrite a live object.
	if !s.claimReplacementKey(result) {
		return nil, rewriteFailed
	}

	// Step 3 — record the rewrite before anything is written, so a restart can
	// find the replacement object whatever happens next, and do not write
	// until that record is durable: a replacement no durable record names is
	// adopted by the next refresh next to the source it replaces.
	if _, ok := s.store.Update(id, func(cur *Tombstone) bool {
		if cur.Superseded == nil {
			cur.Superseded = make(map[string]Supersession)
		}
		cur.Superseded[key] = Supersession{NewKey: result.NewKey, State: SupersessionPrepared, At: time.Now()}
		return true
	}); !ok {
		// Un-deleted while the rewrite was being prepared: nothing was
		// written, so there is nothing to undo.
		s.manifest.ReleasePending(result.NewKey)
		return nil, rewriteFailed
	}
	if err := s.store.EnsureDurable(ctx, id); err != nil {
		metrics.DeleteRewriteDeferred.Inc("not_durable")
		logger.Warnf("rewrite not started: its record is not durable yet: %s; key=%s", err, key)
		clearRecord(s.store, id, key, result.NewKey)
		s.manifest.ReleasePending(result.NewKey)
		return nil, rewriteFailed
	}
	if s.crashed(stepIntentRecorded) {
		return nil, rewriteInterrupted
	}

	// Step 4 — upload the replacement. The source is untouched.
	if err := s.rewriter.Upload(ctx, result); err != nil {
		metrics.DeleteRewriteErrors.Inc()
		logger.Errorf("rewrite upload failed: %s; key=%s", err, key)
		s.discard(ctx, id, key, result.NewKey)
		return nil, rewriteFailed
	}
	if s.crashed(stepUploaded) {
		return nil, rewriteInterrupted
	}

	// Step 5 — publish: swap the manifest entry. Until this succeeds the
	// superseded object is the ONLY copy of the kept rows and must survive.
	// The replacement is HELD from here until the swap is recorded durably:
	// a compaction that merged it in that window would carry its rows into an
	// output the undo of this rewrite knows nothing about.
	s.manifest.Hold(result.NewKey)
	published, err := publishRewrite(s.manifest, result)
	if err == nil && s.crashed(stepSwappedInMemory) {
		return nil, rewriteInterrupted
	}
	if errors.Is(err, errReplacementKeyTaken) {
		// Never delete the replacement here: the object under that key may be
		// the file the manifest already serves.
		logger.Errorf("rewrite abandoned: %s; key=%s", err, key)
		s.manifest.Release(result.NewKey)
		clearRecord(s.store, id, key, result.NewKey)
		return nil, rewriteFailed
	}
	if errors.Is(err, errSourceSuperseded) {
		// A concurrent compaction took the source. Discard our replacement
		// rather than register a second copy of its rows.
		logger.Infof("rewrite discarded: source merged concurrently; key=%s", key)
		s.manifest.Release(result.NewKey)
		if !s.discardStep(ctx, id, key, result.NewKey) {
			return nil, rewriteInterrupted
		}
		s.markReaped(id, key, "")
		return result, rewriteSuperseded
	}
	if err != nil {
		metrics.DeleteRewriteErrors.Inc()
		logger.Errorf("rewrite manifest publish failed: %s; key=%s, new=%s", err, key, result.NewKey)
		// The replacement will never be published by this attempt; the retry
		// writes a fresh one.
		s.manifest.Release(result.NewKey)
		if !s.discardStep(ctx, id, key, result.NewKey) {
			return nil, rewriteInterrupted
		}
		return nil, rewriteFailed
	}

	// Step 6 — record the publish (and the key's bookkeeping) durably, BEFORE
	// peers, pmeta or the delete of the superseded object can act on it. Until
	// the record is durable nothing outside this process may see the swap: a
	// restart would restore the earlier record and undo it.
	recordPublished(s.store, id, key, result.NewKey)
	if s.crashed(stepPublishRecorded) {
		return nil, rewriteInterrupted
	}
	if err := s.store.EnsureDurable(ctx, id); err != nil {
		metrics.DeleteRewriteDeferred.Inc("not_durable")
		logger.Warnf("rewrite published but not recorded durably (the superseded object is kept and the next pass retries): %s; key=%s", err, key)
		return result, rewriteDeferred
	}
	s.manifest.Release(result.NewKey)

	// Step 7 — propagate: the facet feed and the peer push, before the
	// superseded object disappears.
	s.notifyPublished(published, result)
	s.markHandedOff(key, result.NewKey)
	if s.crashed(stepNotified) {
		return nil, rewriteInterrupted
	}

	// Step 8 — commit: delete the superseded object and clear the record. A
	// failed delete keeps the record (and the source retired in the manifest),
	// and the next pass retries it.
	if s.commit(ctx, id, key, result.NewKey) && s.crashed(stepSupersededGone) {
		return nil, rewriteInterrupted
	}

	metrics.DeleteRewriteTotal.Inc()
	if saved := result.BytesBefore - result.BytesAfter; saved > 0 {
		metrics.DeleteRewriteBytesSaved.Add(int(saved))
	}
	return result, rewriteDone
}

// claimReplacementKey reserves the prepared replacement key, drawing a new id
// on the (astronomically unlikely) collision with a key the manifest already
// knows. Returns false when no free key was found.
func (s *RewriteScheduler) claimReplacementKey(result *RewriteResult) bool {
	if result.NewKey == "" {
		return true // every row went: there is no replacement to write
	}
	for attempt := 0; attempt < maxReplacementKeyAttempts; attempt++ {
		if s.manifest.ClaimPending(result.NewKey) {
			return true
		}
		metrics.DeleteRewriteKeyCollisions.Inc()
		logger.Warnf("replacement key already in use; drawing another; key=%s", result.NewKey)
		result.NewKey = replacementKey(result.OldKey, newReplacementID())
	}
	logger.Errorf("no free replacement key for %s after %d attempts", result.OldKey, maxReplacementKeyAttempts)
	return false
}

// maxReplacementKeyAttempts bounds the redraw loop; a second collision means
// something other than chance is generating keys.
const maxReplacementKeyAttempts = 5

// discardStep is discard with the test crash hook between recording the
// discard and deleting the abandoned replacement. Returns false when the hook
// stopped it.
func (s *RewriteScheduler) discardStep(ctx context.Context, id, key, newKey string) bool {
	if s.crashAt == nil {
		s.discard(ctx, id, key, newKey)
		return true
	}
	recorded := false
	s.store.Update(id, func(ts *Tombstone) bool {
		cur, ok := ts.Superseded[key]
		if !ok || cur.NewKey != newKey || cur.State == SupersessionPublished {
			return false
		}
		recorded = true
		if cur.State == SupersessionDiscarded {
			return false
		}
		ts.Superseded[key] = Supersession{NewKey: newKey, State: SupersessionDiscarded, At: time.Now()}
		return true
	})
	if recorded && newKey != "" {
		s.manifest.AbandonPending(newKey)
	}
	if s.crashed(stepDiscardRecorded) {
		return false
	}
	s.discard(ctx, id, key, newKey)
	return !s.crashed(stepAbandonedDeleted)
}

func containsString(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// notifyPublished hands a published rewrite to the OnPublished hook in the
// shape compaction's OnCompacted uses, so the embedder can route both through
// the same consumers.
func (s *RewriteScheduler) notifyPublished(published *manifest.FileInfo, result *RewriteResult) {
	if s.onPublished == nil || result == nil || !result.Published {
		return
	}
	var added []manifest.FileInfo
	var blooms map[string]map[string][]string
	if published != nil {
		added = []manifest.FileInfo{*published}
		if len(result.BloomValues) > 0 {
			blooms = map[string]map[string][]string{published.Key: result.BloomValues}
		}
	}
	s.onPublished(added, []string{result.OldKey}, blooms)
}

// Verify checks active tombstones for effectiveness.
// Placeholder: increments DeleteVerifyTotal for each active tombstone.
func (s *RewriteScheduler) Verify(_ context.Context) {
	active := s.store.Active()
	for range active {
		metrics.DeleteVerifyTotal.Inc()
	}
}
