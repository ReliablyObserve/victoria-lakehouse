package delete

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// tombstonePrefixSegment is the key segment every tombstone object lives under.
// It is also on the orphan sweep's NeverDeletePrefixes list, so the sweep can
// never reclaim a tombstone object it does not recognise.
const tombstonePrefixSegment = "_tombstones/"

// pendingOp is the durable write still owed for one tombstone id.
type pendingOp uint8

const (
	pendingUpsert pendingOp = iota + 1
	pendingDelete
)

// pendingEntry is one owed write and the store version that owed it. The
// version is what makes an acknowledgement meaningful: an upload that read the
// record before a later change must not clear the queue entry that change
// created, and a caller asking "is my change durable?" must not be answered by
// an older upload that happened to succeed.
type pendingEntry struct {
	op  pendingOp
	ver uint64
}

// ErrNotDurable reports that a tombstone change has not reached every durable
// target. Nothing that deletes an object may proceed on a change in that state:
// a restart restores the record from those targets, and an older record there
// would undo a rewrite whose objects are already gone.
var ErrNotDurable = errors.New("tombstone change is not durable yet")

// tombstonePersistence owns the durable copies of a TombstoneStore. It is
// deliberately separate from the store's own mutex: durable writes must never
// be held under the lock that every query's ForRange takes.
type tombstonePersistence struct {
	dir    string
	pool   S3Pool
	prefix string

	// diskMu serializes the snapshot-and-write of the whole-file disk copy.
	// PersistToDisk marshals the entire tombstone map, so two concurrent
	// mutations each snapshot independently and then race on the rename: the
	// one that snapshotted FIRST can land LAST, leaving the file describing an
	// older state than the one already acknowledged to a caller. Taking the
	// snapshot under this lock makes the last file written also the last state
	// observed. It is a separate lock from the store's own so a slow disk never
	// blocks a query's ForRange.
	diskMu sync.Mutex

	// s3Mu serializes the S3 writes so the LAST object written is the one read
	// LAST. Without it two flushes can read different versions of a record and
	// land in the opposite order, leaving S3 holding the older one while the
	// queue looks drained — and an acknowledgement that the newer record is
	// durable would be false.
	s3Mu sync.Mutex

	mu      sync.Mutex
	pending map[string]pendingEntry
	// diskVer / s3Ver are the newest store version each target is known to
	// hold, per tombstone id.
	diskVer map[string]uint64
	s3Ver   map[string]uint64
}

// newTombstonePersistence builds the durable-target bookkeeping.
func newTombstonePersistence(cfg PersistenceConfig) *tombstonePersistence {
	return &tombstonePersistence{
		dir:     cfg.Dir,
		pool:    cfg.Pool,
		prefix:  normalizeTombstonePrefix(cfg.Prefix),
		pending: make(map[string]pendingEntry),
		diskVer: make(map[string]uint64),
		s3Ver:   make(map[string]uint64),
	}
}

// durableAt reports whether the target that a restore would rely on holds
// version ver (or newer) of id.
//
// When S3 is configured it is that target: every restore reads it, on this node
// and on a node that starts without this disk, and the merge prefers the record
// that got further, so an S3 copy that is current cannot be undone by a stale
// local one. With no S3 configured the local disk is the only copy, so it
// decides. With neither, nothing durable exists to contradict the caller.
func (p *tombstonePersistence) durableAt(id string, ver uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.pool != nil:
		return p.s3Ver[id] >= ver
	case p.dir != "":
		return p.diskVer[id] >= ver
	default:
		return true
	}
}

// normalizeTombstonePrefix makes the caller's prefix safe to concatenate.
// Config.AutoPrefix() already ends in "/", so the previous
// fmt.Sprintf("%s/_tombstones/", tenant) produced "logs//_tombstones/": keys
// that did not match the "logs/_tombstones/" layout documented in
// docs/deletion-strategy.md. (The old writer and reader agreed with each other,
// so the doubled slash was not why restores found nothing — nothing ever called
// the writer — but anything reading the documented layout would have missed
// every record.)
// TrimRight rather than TrimSuffix: a prefix ending in more than one slash
// ("//", from an empty tenant prefix concatenated with a signal suffix) would
// otherwise keep one of them and produce a key the reader never lists. Found by
// FuzzNormalizeTombstonePrefix.
func normalizeTombstonePrefix(prefix string) string {
	prefix = strings.TrimRight(prefix, "/")
	if prefix == "" {
		return tombstonePrefixSegment
	}
	return prefix + "/" + tombstonePrefixSegment
}

// TombstonePrefix returns the S3 key prefix the store's tombstone objects live
// under. Exported so the operator-facing docs, the orphan sweep's protected
// list and the tests all name the same thing.
func TombstonePrefix(prefix string) string {
	return normalizeTombstonePrefix(prefix)
}

func (p *tombstonePersistence) keyFor(id string) string {
	return p.prefix + id + ".json"
}

// persistChange is the write-through path taken by Add / Update / Remove /
// Complete. The disk copy is synchronous; the S3 copy is attempted immediately
// and queued for retry on failure so a transient S3 outage degrades to "durable
// locally" rather than "lost". ver is the store version of the change.
func (s *TombstoneStore) persistChange(p *tombstonePersistence, id string, op pendingOp, ver uint64) {
	if p == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if p.dir != "" {
		s.writeDiskCopy(p, id)
	}

	p.queue(id, pendingEntry{op: op, ver: ver})
	s.flushPending(ctx, p)
}

// writeDiskCopy writes the whole-file disk copy and records, per id, the
// version it captured.
func (s *TombstoneStore) writeDiskCopy(p *tombstonePersistence, id string) {
	p.diskMu.Lock()
	vers, err := s.writeDisk(p.dir)
	p.diskMu.Unlock()
	if err != nil {
		metrics.DeleteTombstonePersistErrors.Inc("disk")
		logger.Errorf("tombstone disk persist failed; id=%s, dir=%s: %s", id, p.dir, err)
		return
	}
	metrics.DeleteTombstonePersistTotal.Inc("disk")
	p.mu.Lock()
	for k, v := range vers {
		if v > p.diskVer[k] {
			p.diskVer[k] = v
		}
	}
	p.mu.Unlock()
}

// queue records an owed write, never replacing a newer one with an older one.
func (p *tombstonePersistence) queue(id string, e pendingEntry) {
	p.mu.Lock()
	if cur, ok := p.pending[id]; !ok || cur.ver <= e.ver {
		p.pending[id] = e
	}
	p.mu.Unlock()
}

// EnsureDurable reports nil once the tombstone's current state has reached the
// target a restore would rely on, re-issuing the write if it has not. Every
// step that deletes an object calls it first: the record that authorises the
// delete must survive the restart that could otherwise undo it.
func (s *TombstoneStore) EnsureDurable(ctx context.Context, id string) error {
	s.mu.RLock()
	p := s.persist
	ver := s.vers[id]
	_, exists := s.tombstones[id]
	s.mu.RUnlock()
	if p == nil {
		// No durable target: nothing can hold a stale record either.
		return nil
	}
	if !exists {
		return ErrTombstoneNotFound
	}
	if p.durableAt(id, ver) {
		return nil
	}

	if p.dir != "" {
		s.writeDiskCopy(p, id)
	}
	if p.pool != nil {
		p.queue(id, pendingEntry{op: pendingUpsert, ver: ver})
		s.flushPending(ctx, p)
	}
	if p.durableAt(id, ver) {
		return nil
	}
	metrics.DeleteTombstoneNotDurable.Inc()
	return fmt.Errorf("%w: tombstone %s at version %d", ErrNotDurable, id, ver)
}

// PendingS3Writes reports how many tombstone records are still owed to S3 —
// the same number lakehouse_delete_tombstone_persist_pending exposes, for the
// tombstone listing API.
func (s *TombstoneStore) PendingS3Writes() int {
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending)
}

// FlushPending retries every durable write still owed to S3. Called from the
// rewrite scheduler's tick and from shutdown so a transient S3 failure is not
// carried for the life of the process. Returns the number of records still
// pending afterwards.
func (s *TombstoneStore) FlushPending(ctx context.Context) int {
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p == nil {
		return 0
	}
	return s.flushPending(ctx, p)
}

func (s *TombstoneStore) flushPending(ctx context.Context, p *tombstonePersistence) int {
	if p.pool == nil {
		p.mu.Lock()
		// Without a pool there is no S3 target at all; drop the queue rather
		// than growing it forever and reporting a backlog no one can drain.
		p.pending = make(map[string]pendingEntry)
		p.mu.Unlock()
		metrics.DeleteTombstonePersistPending.Set(0)
		return 0
	}

	// One writer at a time, with each record read inside the lock: the object
	// that lands last is then always the newest state read.
	p.s3Mu.Lock()
	defer p.s3Mu.Unlock()

	p.mu.Lock()
	todo := make(map[string]pendingEntry, len(p.pending))
	for id, e := range p.pending {
		todo[id] = e
	}
	p.mu.Unlock()

	for id, e := range todo {
		var err error
		written := e.ver
		switch e.op {
		case pendingDelete:
			err = p.pool.Delete(ctx, p.keyFor(id))
		default:
			// Re-read the live record: a later mutation may have landed
			// between queueing and flushing, and the newest state is the one
			// that belongs in S3.
			ts, ver, ok := s.getWithVersion(id)
			if !ok {
				// Removed in the meantime — the delete op will have been
				// queued too; skip the upload rather than resurrecting it.
				continue
			}
			written = ver
			var data []byte
			data, err = json.Marshal(ts)
			if err == nil {
				err = p.pool.Upload(ctx, p.keyFor(id), data)
			}
		}
		if err != nil {
			metrics.DeleteTombstonePersistErrors.Inc("s3")
			logger.Warnf("tombstone S3 persist failed (will retry); id=%s: %s", id, err)
			continue
		}
		metrics.DeleteTombstonePersistTotal.Inc("s3")
		p.mu.Lock()
		if written > p.s3Ver[id] {
			p.s3Ver[id] = written
		}
		// Only clear an entry the write actually covers: a change made while
		// this upload ran queued a newer entry that still needs writing.
		if cur, ok := p.pending[id]; ok && cur.op == e.op && cur.ver <= written {
			delete(p.pending, id)
		}
		p.mu.Unlock()
	}

	p.mu.Lock()
	remaining := len(p.pending)
	p.mu.Unlock()
	metrics.DeleteTombstonePersistPending.Set(int64(remaining))
	return remaining
}

// Restore loads the union of the disk and S3 copies into the store. It is the
// boot-time counterpart of the write-through path and replaces the previous
// "load disk, and only if that found nothing load S3" sequence, which could
// not heal a node whose local disk was newer for some ids and older for others.
//
// Returns the number of tombstones in the store afterwards.
func (s *TombstoneStore) Restore(ctx context.Context, cfg PersistenceConfig) (int, error) {
	var errs []string
	if cfg.Dir != "" {
		if err := s.LoadFromDisk(cfg.Dir); err != nil {
			errs = append(errs, fmt.Sprintf("disk: %s", err))
			metrics.DeleteStartupInconsistencies.Inc("disk_restore_failed")
		}
	}
	if cfg.Pool != nil {
		// The S3 copy is the one a node that starts without this disk has, and
		// the one every rewrite record must be read back from, so a failure
		// here is a durability failure, not a warning: it is retried a few
		// times now and then on every scheduler pass, counted, reported by the
		// self-check, and it stops this process from resolving or finishing any
		// rewrite until it succeeds (the records it would act on may be stale).
		err := s.loadFromS3WithRetry(ctx, cfg)
		s.mu.Lock()
		s.restoreCfg = cfg
		s.s3RestorePending = err != nil
		s.mu.Unlock()
		metrics.DeleteTombstoneRestorePending.Set(boolToInt64(err != nil))
		if err != nil {
			errs = append(errs, fmt.Sprintf("s3: %s", err))
			metrics.DeleteStartupInconsistencies.Inc("s3_restore_failed")
			logger.Errorf("tombstone restore from S3 failed; deletes made elsewhere are not enforced here and interrupted rewrites stay unresolved until it succeeds: %s", err)
		}
	}
	n := s.Count()
	metrics.DeleteTombstonesActive.Set(int64(n))
	if len(errs) > 0 {
		return n, fmt.Errorf("restore tombstones: %s", strings.Join(errs, "; "))
	}
	return n, nil
}

// boolToInt64 renders a state flag as the 0/1 a gauge carries.
func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// restoreAttempts / restoreBackoff bound the startup retry of the S3 restore.
// Small on purpose: a node must not sit in disk recovery for minutes because
// S3 is unhappy, and RetryS3Restore keeps trying afterwards.
var (
	restoreAttempts = 3
	restoreBackoff  = time.Second
)

func (s *TombstoneStore) loadFromS3WithRetry(ctx context.Context, cfg PersistenceConfig) error {
	var err error
	for attempt := 1; attempt <= restoreAttempts; attempt++ {
		if err = s.LoadFromS3(ctx, cfg.Pool, "", cfg.Prefix); err == nil {
			return nil
		}
		metrics.DeleteTombstoneRestoreAttempts.Inc("failed")
		if attempt == restoreAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(restoreBackoff * time.Duration(attempt)):
		}
	}
	return err
}

// S3RestorePending reports that the S3 copy of the tombstone store could not be
// read at startup and has not been read since. While it is true the in-memory
// records may be incomplete or older than what S3 holds, so no rewrite may be
// resolved, finished or retired on their strength.
func (s *TombstoneStore) S3RestorePending() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.s3RestorePending
}

// RetryS3Restore re-reads the S3 copy after a failed restore and merges it in.
// Returns true when the store is (now) complete.
func (s *TombstoneStore) RetryS3Restore(ctx context.Context) bool {
	s.mu.RLock()
	pending := s.s3RestorePending
	cfg := s.restoreCfg
	s.mu.RUnlock()
	if !pending {
		return true
	}
	if cfg.Pool == nil {
		return false
	}
	if err := s.LoadFromS3(ctx, cfg.Pool, "", cfg.Prefix); err != nil {
		metrics.DeleteTombstoneRestoreAttempts.Inc("failed")
		logger.Warnf("tombstone restore from S3 still failing: %s", err)
		return false
	}
	s.mu.Lock()
	s.s3RestorePending = false
	s.mu.Unlock()
	metrics.DeleteTombstoneRestorePending.Set(0)
	metrics.DeleteTombstoneRestoreAttempts.Inc("recovered")
	metrics.DeleteTombstonesActive.Set(int64(s.Count()))
	logger.Infof("tombstone restore from S3 succeeded on retry; tombstones=%d", s.Count())
	return true
}

// RunRestoreRetry keeps re-reading the S3 copy until it succeeds, then returns.
// The rewrite scheduler already retries on every pass, but a node with rewriting
// disabled — a query-only replica, or one whose delete scheduler is off — has no
// such pass: without this loop it would serve queries from an incomplete
// tombstone set for the life of the process, un-hiding rows another node
// deleted. Returns immediately when nothing is pending.
func (s *TombstoneStore) RunRestoreRetry(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = time.Minute
	}
	for {
		s.mu.RLock()
		pending, pool := s.s3RestorePending, s.restoreCfg.Pool
		s.mu.RUnlock()
		if !pending || pool == nil {
			return
		}
		if s.RetryS3Restore(ctx) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}

// mergeLoadedLocked folds a loaded record into the store without losing
// progress. Two nodes (or two boots) can disagree about a tombstone only in how
// much of it has been rewritten, and rewrite progress is monotonic: a key that
// is reaped somewhere is reaped everywhere, because the file it named is gone.
// So the merge is a union of the Reaped sets and of the AffectedKeys lists, and
// the earliest CreatedAt wins (it bounds the rewrite delay conservatively).
func (s *TombstoneStore) mergeLoadedLocked(ts Tombstone) {
	// A restored record is a MERGE of what the targets held, so no single
	// target is known to hold it: the disk copy can be ahead of S3 (the state a
	// crash between the disk write and the S3 write leaves), and the merge of
	// two nodes' copies is newer than either. Advancing the change counter is
	// what keeps EnsureDurable honest about that — version 0 against an
	// acknowledgement of 0 would read as "already durable" and let a rewrite
	// delete its source on the strength of a record S3 does not hold. The
	// write-back is lazy: EnsureDurable issues it for the ids something acts
	// on, so a boot does not rewrite every record it restored.
	defer s.bumpLocked(ts.ID)
	cur, ok := s.tombstones[ts.ID]
	if !ok {
		s.tombstones[ts.ID] = ts
		return
	}
	merged := cloneTombstone(cur)
	if ts.CreatedAt.Before(merged.CreatedAt) && !ts.CreatedAt.IsZero() {
		merged.CreatedAt = ts.CreatedAt
	}
	// Union, not "the longer list": both copies only ever grow, but a node
	// that discovered different files than its peer holds keys the other
	// copy lacks, and dropping either set would let the tombstone retire with
	// a file still unhandled.
	listed := make(map[string]bool, len(merged.AffectedKeys))
	for _, k := range merged.AffectedKeys {
		listed[k] = true
	}
	for _, k := range ts.AffectedKeys {
		if !listed[k] {
			merged.AffectedKeys = append(merged.AffectedKeys, k)
			listed[k] = true
		}
	}
	if len(ts.Reaped) > 0 {
		if merged.Reaped == nil {
			merged.Reaped = make(map[string]bool, len(ts.Reaped))
		}
		for k, v := range ts.Reaped {
			if v {
				merged.Reaped[k] = true
			}
		}
	}
	// Clean marks union too, except where the other copy has the key reaped:
	// "gone" is later than "clean" (a clean replacement merged away since).
	for k, v := range ts.Clean {
		if v {
			if merged.Clean == nil {
				merged.Clean = make(map[string]bool, len(ts.Clean))
			}
			merged.Clean[k] = true
		}
	}
	for k := range merged.Clean {
		if merged.Reaped[k] {
			delete(merged.Clean, k)
		}
	}
	merged.Superseded = mergeSupersessions(merged.Superseded, ts.Superseded, merged.Reaped)
	s.tombstones[ts.ID] = merged
}

// mergeSupersessions unions two copies' rewrite records without walking one
// back. For the same source: a record that got further (published or
// discarded) beats a prepared one, and between two rewrites with different
// replacements the later one wins. A PREPARED record is dropped when the merged
// copy already has its source reaped: a copy only ever reaps a key after its
// rewrite left the prepared state, so that record is an older copy's view of a
// rewrite that has since finished — and undoing it would delete the live
// replacement.
func mergeSupersessions(a, b map[string]Supersession, reaped map[string]bool) map[string]Supersession {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	out := make(map[string]Supersession, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		cur, ok := out[k]
		if !ok || supersessionRank(v, cur) > 0 {
			out[k] = v
		}
	}
	for k, v := range out {
		if v.State == SupersessionPrepared && reaped[k] {
			delete(out, k)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// supersessionRank compares two records for the same source: >0 when a should
// win.
func supersessionRank(a, b Supersession) int {
	if a.NewKey == b.NewKey {
		progress := func(s Supersession) int {
			if s.State == SupersessionPrepared {
				return 0
			}
			return 1
		}
		return progress(a) - progress(b)
	}
	if a.At.After(b.At) {
		return 1
	}
	return -1
}
