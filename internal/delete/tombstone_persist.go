package delete

import (
	"context"
	"encoding/json"
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

// tombstonePersistence owns the durable copies of a TombstoneStore. It is
// deliberately separate from the store's own mutex: durable writes must never
// be held under the lock that every query's ForRange takes.
type tombstonePersistence struct {
	dir    string
	pool   S3Pool
	prefix string

	mu      sync.Mutex
	pending map[string]pendingOp
}

// normalizeTombstonePrefix makes the caller's prefix safe to concatenate.
// Config.AutoPrefix() already ends in "/", so the previous
// fmt.Sprintf("%s/_tombstones/", tenant) produced "logs//_tombstones/" — a
// distinct S3 prefix from the "logs/_tombstones/" documented in
// docs/deletion-strategy.md, which is one reason LoadFromS3 found nothing.
func normalizeTombstonePrefix(prefix string) string {
	prefix = strings.TrimSuffix(prefix, "/")
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

// persistChange is the write-through path taken by Add / Remove / Complete.
// The disk copy is synchronous and authoritative for crash recovery on the same
// node; the S3 copy is attempted immediately and queued for retry on failure so
// a transient S3 outage degrades to "durable locally" rather than "lost".
func (s *TombstoneStore) persistChange(p *tombstonePersistence, id string, op pendingOp) {
	if p == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if p.dir != "" {
		if err := s.PersistToDisk(p.dir); err != nil {
			metrics.DeleteTombstonePersistErrors.Inc("disk")
			logger.Errorf("tombstone disk persist failed; id=%s, dir=%s: %s", id, p.dir, err)
		} else {
			metrics.DeleteTombstonePersistTotal.Inc("disk")
		}
	}

	p.mu.Lock()
	p.pending[id] = op
	p.mu.Unlock()

	s.flushPending(ctx, p)
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
		p.pending = make(map[string]pendingOp)
		p.mu.Unlock()
		metrics.DeleteTombstonePersistPending.Set(0)
		return 0
	}

	p.mu.Lock()
	todo := make(map[string]pendingOp, len(p.pending))
	for id, op := range p.pending {
		todo[id] = op
	}
	p.mu.Unlock()

	for id, op := range todo {
		var err error
		switch op {
		case pendingDelete:
			err = p.pool.Delete(ctx, p.keyFor(id))
		default:
			// Re-read the live record: a later mutation may have landed
			// between queueing and flushing, and the newest state is the one
			// that belongs in S3.
			ts, ok := s.Get(id)
			if !ok {
				// Removed in the meantime — the delete op will have been
				// queued too; skip the upload rather than resurrecting it.
				err = nil
				break
			}
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
		// Only clear if the queued op is still the one we just performed.
		if cur, ok := p.pending[id]; ok && cur == op {
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
		}
	}
	if cfg.Pool != nil {
		if err := s.LoadFromS3(ctx, cfg.Pool, "", cfg.Prefix); err != nil {
			errs = append(errs, fmt.Sprintf("s3: %s", err))
		}
	}
	n := s.Count()
	metrics.DeleteTombstonesActive.Set(int64(n))
	if len(errs) > 0 {
		return n, fmt.Errorf("restore tombstones: %s", strings.Join(errs, "; "))
	}
	return n, nil
}

// mergeLoadedLocked folds a loaded record into the store without losing
// progress. Two nodes (or two boots) can disagree about a tombstone only in how
// much of it has been rewritten, and rewrite progress is monotonic: a key that
// is reaped somewhere is reaped everywhere, because the file it named is gone.
// So the merge is a union of the Reaped sets, and the earliest CreatedAt wins
// (it bounds the rewrite delay conservatively).
func (s *TombstoneStore) mergeLoadedLocked(ts Tombstone) {
	cur, ok := s.tombstones[ts.ID]
	if !ok {
		s.tombstones[ts.ID] = ts
		return
	}
	merged := cur
	if ts.CreatedAt.Before(merged.CreatedAt) && !ts.CreatedAt.IsZero() {
		merged.CreatedAt = ts.CreatedAt
	}
	if len(ts.AffectedKeys) > len(merged.AffectedKeys) {
		merged.AffectedKeys = ts.AffectedKeys
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
	s.tombstones[ts.ID] = merged
}
