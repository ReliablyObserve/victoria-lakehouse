package delete

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// S3Pool abstracts S3 operations for tombstone persistence.
type S3Pool interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, key string) error
}

// Tombstone represents a soft-delete marker that hides or permanently removes
// matching log entries from query results.
type Tombstone struct {
	ID           string
	Query        string
	StartNs      int64
	EndNs        int64
	AffectedKeys []string
	CreatedAt    time.Time
	CreatedBy    string
	// Reaped lists files whose rows this tombstone no longer needs to act on
	// because the object is gone — rewritten, merged, or found already
	// superseded.
	Reaped map[string]bool
	Mode   string // "hide"|"permanent"|"auto"

	// Clean lists LIVE files verified to hold none of this tombstone's rows: a
	// rewrite's replacement, or a compaction output that filtered them. Kept
	// apart from Reaped so "handled" never reads as "gone": a clean file is
	// still manifested, a reaped one must not be.
	Clean map[string]bool `json:",omitempty"`

	// Superseded is the durable record of the rewrites of this tombstone's
	// files that have not finished: source key → the replacement and how far
	// the rewrite got. It is written before the replacement is uploaded and
	// cleared only once the superseded (or abandoned) object is deleted, so a
	// restart — on this node or on one that only has the S3 copy — can finish
	// or undo every rewrite a crash interrupted. See ResolveInterruptedRewrites.
	Superseded map[string]Supersession `json:",omitempty"`

	// FilterAt is the time (unix nanoseconds) the query's relative time filters
	// (`_time:5m`) are evaluated at: when the delete was issued, as upstream
	// evaluates a delete task at its start time. Zero evaluates them at parse
	// time.
	FilterAt int64 `json:",omitempty"`

	// Tenants limits the tombstone to the rows of these tenants: query-time
	// suppression, field listings, rewrites, compaction and the delete API all
	// act only on objects and rows of a tenant listed here. At least one is
	// required (Validate); a persisted record without any is rejected on
	// restore rather than applied.
	Tenants []TenantRef `json:",omitempty"`
}

// Supersession states, in the order a rewrite passes through them.
const (
	// SupersessionPrepared: the replacement key is chosen and may be uploaded;
	// the publish has not been recorded. Nothing outside this process has seen
	// the replacement, so an interrupted rewrite in this state is undone.
	SupersessionPrepared = "prepared"
	// SupersessionPublished: the manifest points at the replacement (or, when
	// every row was removed, no longer lists the source) and peers may have been
	// told. Only the superseded object's delete is outstanding.
	SupersessionPublished = "published"
	// SupersessionDiscarded: the publish was refused or failed; only the
	// abandoned replacement's delete is outstanding.
	SupersessionDiscarded = "discarded"
)

// Supersession records one interrupted-or-in-flight rewrite of a file.
type Supersession struct {
	// NewKey is the replacement object; empty when the rewrite removes every
	// row and so writes no replacement.
	NewKey string
	State  string
	At     time.Time
}

// AffectsFile returns true if the tombstone's time range overlaps with the
// file's time range [fileMinNs, fileMaxNs].
func (t *Tombstone) AffectsFile(fileMinNs, fileMaxNs int64) bool {
	if t.StartNs > t.EndNs {
		return false
	}
	return t.StartNs <= fileMaxNs && t.EndNs >= fileMinNs
}

// MatchesRow returns true if the given row matches this tombstone's query
// and the timestamp falls within the tombstone's time range.
// Uses VL's ParseFilter + Filter.MatchRow for full LogsQL evaluation.
func (t *Tombstone) MatchesRow(row map[string]string, timestampNs int64) bool {
	if timestampNs < t.StartNs || timestampNs > t.EndNs {
		return false
	}
	if t.Query == "" || t.Query == "*" {
		return true
	}
	f := parseFilterCached(t.Query, t.FilterAt)
	if f == nil {
		return false
	}
	fields := make([]logstorage.Field, 0, len(row))
	for k, v := range row {
		fields = append(fields, logstorage.Field{Name: k, Value: v})
	}
	return f.MatchRow(fields)
}

// MatchesFields is MatchesRow for callers that already hold the row as
// []logstorage.Field — the shape VL's filters evaluate against. It skips the
// map round-trip MatchesRow needs, which matters on the field-enumeration paths
// where every scanned row is checked.
func (t *Tombstone) MatchesFields(fields []logstorage.Field, timestampNs int64) bool {
	if timestampNs < t.StartNs || timestampNs > t.EndNs {
		return false
	}
	if t.Query == "" || t.Query == "*" {
		return true
	}
	f := parseFilterCached(t.Query, t.FilterAt)
	if f == nil {
		return false
	}
	return f.MatchRow(fields)
}

// Filter returns the parsed LogsQL filter for this tombstone's query, or nil
// when the query is match-all or does not parse. Read paths use it to learn
// which columns the tombstone predicate needs so a column-projected scan can
// include them — evaluating a tombstone against a projection that omits its
// own fields silently under-suppresses.
func (t *Tombstone) Filter() *logstorage.Filter {
	if t.Query == "" || t.Query == "*" {
		return nil
	}
	return parseFilterCached(t.Query, t.FilterAt)
}

// EligibleForPhysicalRemoval reports whether rows matching this tombstone may
// be permanently removed from storage at `now`.
//
// Two rules, shared by every path that can drop rows (the rewrite scheduler
// and compaction) so they can never disagree:
//
//   - never for hide mode. A hide-mode delete is reversible by contract:
//     removing the tombstone makes the rows visible again. A path that
//     physically drops hide-mode rows turns an un-delete into silent data loss.
//   - only once rewrite_delay has passed since the delete. The delay is the
//     un-delete window for permanent and auto deletes; dropping rows inside it
//     breaks the same promise for an operator who catches an over-broad
//     predicate in time.
func (t *Tombstone) EligibleForPhysicalRemoval(now time.Time, rewriteDelay time.Duration) bool {
	if t.Mode == "hide" {
		return false
	}
	return now.Sub(t.CreatedAt) >= rewriteDelay
}

// MarkReaped records that key's object is gone. A key that was clean (a live
// replacement) and has since been merged away moves from Clean to Reaped.
func (t *Tombstone) MarkReaped(key string) {
	if t.Reaped == nil {
		t.Reaped = make(map[string]bool)
	}
	t.Reaped[key] = true
	delete(t.Clean, key)
}

// SetClean records whether the live file key is free of this tombstone's rows.
func (t *Tombstone) SetClean(key string, clean bool) {
	if !clean {
		delete(t.Clean, key)
		return
	}
	if t.Clean == nil {
		t.Clean = make(map[string]bool)
	}
	t.Clean[key] = true
}

// Handled reports whether key needs no further work for this tombstone: its
// object is gone (Reaped) or it is a live file free of the tombstone's rows
// (Clean).
func (t *Tombstone) Handled(key string) bool {
	return t.Reaped[key] || t.Clean[key]
}

// FullyReaped reports whether every key this tombstone covers has been
// handled. A fully reaped tombstone has no remaining work: the rows it hides
// are already physically gone from every file it named.
func (t *Tombstone) FullyReaped() bool {
	if len(t.AffectedKeys) == 0 {
		return false
	}
	// A rewrite whose superseded or abandoned object is not yet deleted is
	// still work: retiring would drop the only durable record of that object.
	if len(t.Superseded) > 0 {
		return false
	}
	for _, k := range t.AffectedKeys {
		if !t.Handled(k) {
			return false
		}
	}
	return true
}

// TombstoneStore is a thread-safe in-memory store for tombstones.
//
// # Durability guarantee
//
// Once EnablePersistence has been called, EVERY mutation (Add, Remove,
// Complete) is written through to durable storage before the mutating call
// returns:
//
//   - the local disk copy ({dir}/tombstones.json, atomic tmp+rename) is written
//     synchronously, so a SIGKILL immediately after the delete API returns
//     cannot lose the tombstone;
//   - the S3 copy ({tenant}_tombstones/{id}.json) is attempted in the same
//     call. If S3 is unavailable the record is queued in the pending set,
//     lakehouse_delete_tombstone_persist_pending rises, and the write is
//     retried on the next mutation and by FlushPending (called from the
//     scheduler tick and from shutdown).
//
// Nothing about durability depends on a graceful shutdown. Before this, a
// tombstone reached disk only from runShutdown and never reached S3 at all, so
// any non-graceful restart silently un-deleted hidden data.
//
// Startup restores the UNION of the disk and S3 copies (see Restore); neither
// source replaces the other, and per-ID conflicts resolve towards the record
// with the most rewrite progress so reaped state is never walked back.
type TombstoneStore struct {
	mu         sync.RWMutex
	tombstones map[string]Tombstone

	// persist is nil until EnablePersistence is called. Guarded by its own
	// mutex, never by s.mu: durable writes happen OUTSIDE the store lock so a
	// slow disk or a stalled S3 PUT cannot block every query's ForRange.
	persist *tombstonePersistence

	// onComplete, when set, is called with a tombstone as it retires.
	onComplete func(Tombstone)

	// removed is id → removedAt for tombstones removed (un-deleted or
	// retired), persisted with the disk copy so a restore can tell a stale S3
	// object from a live record. See tombstone_removed.go.
	removed map[string]time.Time
	// staleS3 holds ids whose stale S3 copies a restore found before
	// persistence was enabled; EnablePersistence queues their deletes.
	staleS3 map[string]bool

	// vers counts the changes made to each tombstone id. It is what the
	// durable-target bookkeeping compares against, so an acknowledgement names
	// a specific state rather than "some write succeeded". Survives removal:
	// an id reused later keeps counting up, so no stale acknowledgement can
	// look current.
	vers map[string]uint64

	// s3RestorePending / restoreCfg carry a failed S3 restore forward so it can
	// be retried and so nothing acts on possibly stale records meanwhile.
	s3RestorePending bool
	restoreCfg       PersistenceConfig

	// inflight is the set of source keys a rewrite in THIS process is working
	// on. Process-local by design: it stops two schedulers sharing the store
	// from rewriting — or resolving the record of — the same file at once.
	inflightMu sync.Mutex
	inflight   map[string]bool
}

// bumpLocked advances and returns the change counter for id. Caller holds s.mu.
func (s *TombstoneStore) bumpLocked(id string) uint64 {
	if s.vers == nil {
		s.vers = make(map[string]uint64)
	}
	s.vers[id]++
	return s.vers[id]
}

// getWithVersion returns a record and the version it is at.
func (s *TombstoneStore) getWithVersion(id string) (Tombstone, uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts, ok := s.tombstones[id]
	return ts, s.vers[id], ok
}

// UnfinishedRewrites is the number of rewrite records across active tombstones
// whose objects are not settled yet. Operators read it before a rollback: the
// previous release cannot carry these records.
func (s *TombstoneStore) UnfinishedRewrites() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, ts := range s.tombstones {
		n += len(ts.Superseded)
	}
	return n
}

// ErrRewriteInProgress is returned by TryRemove for a tombstone with a rewrite
// that has not finished.
var ErrRewriteInProgress = errors.New("a rewrite of this tombstone's files is in progress; retry once it finishes")

// ErrNoTenantScope rejects a tombstone that names no tenant: every tombstone
// acts on the tenants it names and on nothing else.
var ErrNoTenantScope = errors.New("tombstone names no tenant")

// ErrTombstoneNotFound is returned by TryRemove for an unknown id.
var ErrTombstoneNotFound = errors.New("tombstone not found")

// claimKey marks source as being rewritten by this process. Returns false when
// another rewrite here already holds it.
func (s *TombstoneStore) claimKey(source string) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if s.inflight == nil {
		s.inflight = make(map[string]bool)
	}
	if s.inflight[source] {
		return false
	}
	s.inflight[source] = true
	return true
}

func (s *TombstoneStore) releaseKey(source string) {
	s.inflightMu.Lock()
	delete(s.inflight, source)
	s.inflightMu.Unlock()
}

// SetCompletionObserver installs a callback fired each time a tombstone
// retires, before the retirement is persisted. Retirement is the moment the
// query-time filter stops hiding the tombstone's rows, so anything derived from
// row content that the filter was covering for — the pmeta field catalog, whose
// value union a row removal cannot shrink — must be corrected by then. Firing
// before the persist means a crash in between replays the retirement (and the
// callback) on the next boot instead of skipping it. Set once, before use.
func (s *TombstoneStore) SetCompletionObserver(fn func(Tombstone)) {
	s.mu.Lock()
	s.onComplete = fn
	s.mu.Unlock()
}

// NewTombstoneStore creates a new empty TombstoneStore.
func NewTombstoneStore() *TombstoneStore {
	return &TombstoneStore{
		tombstones: make(map[string]Tombstone),
		vers:       make(map[string]uint64),
	}
}

// PersistenceConfig describes where a store's durable copies live.
type PersistenceConfig struct {
	// Dir is the local directory holding tombstones.json. Empty disables the
	// disk copy.
	Dir string
	// Pool writes the per-tombstone JSON objects. Nil disables the S3 copy.
	Pool S3Pool
	// Prefix is the tenant/signal prefix the objects live under, e.g.
	// "1002/0/logs/". A trailing slash is optional.
	Prefix string
}

// EnablePersistence turns on write-through durability. Safe to call once,
// before the store is handed to the HTTP handler or the scheduler.
func (s *TombstoneStore) EnablePersistence(cfg PersistenceConfig) {
	s.mu.Lock()
	s.persist = newTombstonePersistence(cfg)
	if s.restoreCfg.Pool == nil && cfg.Pool != nil {
		s.restoreCfg = cfg
	}
	// Stale S3 copies of removed tombstones found by a restore that ran before
	// persistence was armed: their deletes are owed from now on.
	for id := range s.staleS3 {
		s.persist.pending[id] = pendingEntry{op: pendingDelete, ver: s.bumpLocked(id)}
	}
	s.staleS3 = nil
	s.mu.Unlock()
}

// PersistenceEnabled reports whether write-through durability is armed. The
// boot-time self-check warns when it is not, because a store without it silently
// loses every delete on an ungraceful restart.
func (s *TombstoneStore) PersistenceEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.persist != nil
}

// Add inserts a tombstone into the store and persists the change.
//
// The store keeps its own deep copy. Records handed out by Get/Active/ForRange
// share their AffectedKeys slice and Reaped map with the stored record, and the
// store never mutates those in place (every change goes through a fresh copy),
// so a reader can never observe a torn map.
func (s *TombstoneStore) Add(ts Tombstone) {
	s.mu.Lock()
	next := cloneTombstone(ts)
	// A delete re-issued under an existing id (a retried delete task) replaces
	// the definition, but not the records of rewrites already in flight: those
	// describe objects in the bucket, and dropping one loses the only trace of
	// a replacement a restart would have to finish or undo.
	if cur, ok := s.tombstones[ts.ID]; ok {
		for source, rec := range cur.Superseded {
			if _, overridden := next.Superseded[source]; overridden {
				continue
			}
			if next.Superseded == nil {
				next.Superseded = make(map[string]Supersession, len(cur.Superseded))
			}
			next.Superseded[source] = rec
		}
	}
	s.tombstones[ts.ID] = next
	// A new delete reusing a removed id stands; its marker no longer applies.
	delete(s.removed, ts.ID)
	ver := s.bumpLocked(ts.ID)
	p := s.persist
	s.mu.Unlock()
	s.updateActiveGauges()
	metrics.DeleteRewritesUnfinished.Set(int64(s.UnfinishedRewrites()))
	s.persistChange(p, ts.ID, pendingUpsert, ver)
}

// AddIfAbsent inserts ts unless a tombstone with its id already exists, in one
// critical section, and persists it. It is how a delete task is registered:
// upstream refuses a task id that is already registered, and a check-then-Add
// would let two concurrent registrations of the same id both succeed.
func (s *TombstoneStore) AddIfAbsent(ts Tombstone) bool {
	s.mu.Lock()
	if _, ok := s.tombstones[ts.ID]; ok {
		s.mu.Unlock()
		return false
	}
	s.tombstones[ts.ID] = cloneTombstone(ts)
	delete(s.removed, ts.ID)
	ver := s.bumpLocked(ts.ID)
	p := s.persist
	s.mu.Unlock()
	s.updateActiveGauges()
	s.persistChange(p, ts.ID, pendingUpsert, ver)
	return true
}

// Update applies fn to a private copy of the CURRENT record for id, under the
// store lock, and stores the result. fn returns false to abandon the change.
// Returns the record after the call and whether a change was stored.
//
// This is the only safe way to modify bookkeeping that more than one actor
// writes. The rewrite scheduler and compaction both mark keys on the same
// tombstone; with a Get-modify-Add sequence, whichever wrote last silently
// dropped the other's change — including compaction transferring a tombstone to
// an output that still holds its rows, which is exactly the record that must not
// be lost. It also keeps callers from mutating a map the store shares with
// concurrent readers.
func (s *TombstoneStore) Update(id string, fn func(ts *Tombstone) bool) (Tombstone, bool) {
	s.mu.Lock()
	cur, ok := s.tombstones[id]
	if !ok {
		s.mu.Unlock()
		return Tombstone{}, false
	}
	next := cloneTombstone(cur)
	if !fn(&next) {
		s.mu.Unlock()
		return cur, false
	}
	s.tombstones[id] = next
	ver := s.bumpLocked(id)
	p := s.persist
	s.mu.Unlock()
	metrics.DeleteRewritesUnfinished.Set(int64(s.UnfinishedRewrites()))
	s.persistChange(p, id, pendingUpsert, ver)
	return next, true
}

// cloneTombstone deep-copies the mutable parts of a tombstone.
func cloneTombstone(ts Tombstone) Tombstone {
	if ts.AffectedKeys != nil {
		ts.AffectedKeys = append([]string(nil), ts.AffectedKeys...)
	}
	if ts.Reaped != nil {
		reaped := make(map[string]bool, len(ts.Reaped))
		for k, v := range ts.Reaped {
			reaped[k] = v
		}
		ts.Reaped = reaped
	}
	if ts.Clean != nil {
		clean := make(map[string]bool, len(ts.Clean))
		for k, v := range ts.Clean {
			clean[k] = v
		}
		ts.Clean = clean
	}
	if ts.Superseded != nil {
		sup := make(map[string]Supersession, len(ts.Superseded))
		for k, v := range ts.Superseded {
			sup[k] = v
		}
		ts.Superseded = sup
	}
	if ts.Tenants != nil {
		ts.Tenants = append([]TenantRef(nil), ts.Tenants...)
	}
	return ts
}

// Remove deletes a tombstone from the store by ID and persists the removal, so
// an un-delete is not resurrected by the next restart — including a restart
// that happens before a failed S3 delete is retried (see tombstone_removed.go).
func (s *TombstoneStore) Remove(id string) {
	s.mu.Lock()
	delete(s.tombstones, id)
	s.markRemovedLocked(id, time.Now())
	ver := s.bumpLocked(id)
	p := s.persist
	s.mu.Unlock()
	s.updateActiveGauges()
	metrics.DeleteRewritesUnfinished.Set(int64(s.UnfinishedRewrites()))
	s.persistChange(p, id, pendingDelete, ver)
}

// TryRemove is the un-delete: it removes the tombstone unless one of its
// rewrites has not finished. A rewrite's durable record lives on the tombstone
// (see intents.go); removing it mid-rewrite would drop the only record a
// restart has of a replacement object, which the next refresh would then adopt
// next to its source.
func (s *TombstoneStore) TryRemove(id string) error {
	s.mu.Lock()
	ts, ok := s.tombstones[id]
	if !ok {
		s.mu.Unlock()
		return ErrTombstoneNotFound
	}
	if len(ts.Superseded) > 0 {
		s.mu.Unlock()
		return ErrRewriteInProgress
	}
	delete(s.tombstones, id)
	s.markRemovedLocked(id, time.Now())
	ver := s.bumpLocked(id)
	p := s.persist
	s.mu.Unlock()
	s.updateActiveGauges()
	s.persistChange(p, id, pendingDelete, ver)
	return nil
}

// Complete retires a tombstone whose every affected key has been rewritten.
// The hidden rows are physically gone, so keeping the tombstone would leave it
// in Active() forever — re-examined by every scheduler tick and disabling the
// manifest-metadata query fast paths for the rest of the process's life.
//
// Only the permanent/auto modes complete; a hide-mode tombstone is the user's
// standing instruction to suppress rows that still exist and must never be
// retired automatically.
func (s *TombstoneStore) Complete(id string) bool {
	s.mu.Lock()
	ts, ok := s.tombstones[id]
	if !ok || ts.Mode == "hide" || !ts.FullyReaped() {
		s.mu.Unlock()
		return false
	}
	delete(s.tombstones, id)
	s.markRemovedLocked(id, time.Now())
	ver := s.bumpLocked(id)
	p := s.persist
	observer := s.onComplete
	s.mu.Unlock()

	metrics.DeleteTombstonesCompleted.Inc()
	s.updateActiveGauges()
	logger.Infof("tombstone completed; id=%s, query=%s, keys=%d", ts.ID, ts.Query, len(ts.AffectedKeys))
	if observer != nil {
		observer(ts)
	}
	s.persistChange(p, id, pendingDelete, ver)
	return true
}

// Get retrieves a tombstone by ID. Returns the tombstone and whether it was found.
func (s *TombstoneStore) Get(id string) (Tombstone, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts, ok := s.tombstones[id]
	return ts, ok
}

// Active returns all tombstones currently in the store.
func (s *TombstoneStore) Active() []Tombstone {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Tombstone, 0, len(s.tombstones))
	for _, ts := range s.tombstones {
		result = append(result, ts)
	}
	return result
}

// ForRange returns all tombstones whose time range overlaps [startNs, endNs].
func (s *TombstoneStore) ForRange(startNs, endNs int64) []Tombstone {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []Tombstone
	for _, ts := range s.tombstones {
		if ts.StartNs <= endNs && ts.EndNs >= startNs {
			result = append(result, ts)
		}
	}
	return result
}

// updateActiveGauges publishes the number of active tombstones.
func (s *TombstoneStore) updateActiveGauges() {
	metrics.DeleteTombstonesActive.Set(int64(s.Count()))
}

// Count returns the number of tombstones in the store.
func (s *TombstoneStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tombstones)
}

// PersistToDisk writes every tombstone, plus the removed-tombstone markers, to
// {dir}/tombstones.json atomically (tmp + rename). Creates dir if needed.
func (s *TombstoneStore) PersistToDisk(dir string) error {
	_, err := s.writeDisk(dir)
	return err
}

// writeDisk is PersistToDisk plus the per-id versions the written snapshot
// captured, which the durable-target bookkeeping records.
func (s *TombstoneStore) writeDisk(dir string) (map[string]uint64, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}

	// The markers whose S3 delete is still owed must survive pruning; read the
	// queue before taking the store lock (lock order: persistence, then store).
	var owed map[string]pendingOp
	s.mu.RLock()
	p := s.persist
	s.mu.RUnlock()
	if p != nil {
		p.mu.Lock()
		owed = make(map[string]pendingOp, len(p.pending))
		for id, e := range p.pending {
			owed[id] = e.op
		}
		p.mu.Unlock()
	}

	s.mu.Lock()
	s.pruneRemovedLocked(time.Now(), owed)
	s.mu.Unlock()

	s.mu.RLock()
	data, err := json.Marshal(tombstonesFile{
		Format:     tombstonesFileFormat,
		Tombstones: s.tombstones,
		Removed:    s.removed,
	})
	vers := make(map[string]uint64, len(s.vers))
	for id, v := range s.vers {
		vers[id] = v
	}
	s.mu.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("marshal tombstones: %w", err)
	}

	target := filepath.Join(dir, "tombstones.json")
	tmp := target + ".tmp"

	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return nil, fmt.Errorf("write tmp file: %w", err)
	}

	if err := os.Rename(tmp, target); err != nil {
		return nil, fmt.Errorf("rename to target: %w", err)
	}

	return vers, nil
}

// LoadFromDisk reads {dir}/tombstones.json (either the current envelope or the
// previous release's bare map) and merges it into the store. Removal markers
// are applied first, so a record they post-date — including one merged from S3
// before this call — is dropped and its stale S3 copy is owed a delete.
// If the file does not exist, returns nil (no-op).
func (s *TombstoneStore) LoadFromDisk(dir string) error {
	target := filepath.Join(dir, "tombstones.json")

	data, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read tombstones file: %w", err)
	}

	loaded, err := decodeTombstonesFile(data)
	if err != nil {
		return err
	}

	s.mu.Lock()
	for id, at := range loaded.Removed {
		s.markRemovedLocked(id, at)
	}
	stale := s.dropStaleLocked()
	for _, ts := range loaded.Tombstones {
		if s.supersededByMarkerLocked(ts) || rejectUnscoped(ts, "disk") {
			continue
		}
		s.mergeLoadedLocked(ts)
	}
	s.owePendingS3DeletesLocked(stale)
	s.mu.Unlock()

	return nil
}

// SyncToS3 writes each tombstone as an individual JSON file under
// {prefix}_tombstones/{id}.json. Write-through persistence (EnablePersistence)
// covers the steady state; this is the bulk reconciliation used at shutdown and
// by operators re-seeding a bucket. The bucket argument is accepted for call
// compatibility and ignored — the pool carries its own bucket.
func (s *TombstoneStore) SyncToS3(ctx context.Context, pool S3Pool, _ /*bucket*/ string, prefix string) error {
	s.mu.RLock()
	snapshot := make(map[string]Tombstone, len(s.tombstones))
	for id, ts := range s.tombstones {
		snapshot[id] = ts
	}
	s.mu.RUnlock()

	keyPrefix := normalizeTombstonePrefix(prefix)
	for id, ts := range snapshot {
		data, err := json.Marshal(ts)
		if err != nil {
			return fmt.Errorf("marshal tombstone %s: %w", id, err)
		}

		if err := pool.Upload(ctx, keyPrefix+id+".json", data); err != nil {
			metrics.DeleteTombstonePersistErrors.Inc("s3")
			return fmt.Errorf("upload tombstone %s: %w", id, err)
		}
		metrics.DeleteTombstonePersistTotal.Inc("s3")
	}

	return nil
}

// LoadFromS3 lists all keys under {prefix}_tombstones/, downloads each and
// MERGES them into the store (see mergeLoadedLocked). Merging rather than
// replacing is what lets a node restore from both its local disk and S3
// without either source silently discarding the other's records.
//
// A single unreadable object no longer aborts the whole load: it is counted and
// skipped, because refusing to restore 99 good tombstones over 1 corrupt one
// un-deletes data the operator asked to be hidden.
func (s *TombstoneStore) LoadFromS3(ctx context.Context, pool S3Pool, _ /*bucket*/ string, prefix string) error {
	keyPrefix := normalizeTombstonePrefix(prefix)

	keys, err := pool.List(ctx, keyPrefix)
	if err != nil {
		return fmt.Errorf("list tombstones: %w", err)
	}

	if len(keys) == 0 {
		return nil
	}

	loaded := make([]Tombstone, 0, len(keys))
	var skipped int
	for _, key := range keys {
		data, err := pool.Download(ctx, key)
		if err != nil {
			skipped++
			logger.Warnf("tombstone restore: download %s failed: %s", key, err)
			continue
		}

		var ts Tombstone
		if err := json.Unmarshal(data, &ts); err != nil || ts.ID == "" {
			skipped++
			logger.Warnf("tombstone restore: %s is not a readable tombstone record", key)
			continue
		}

		loaded = append(loaded, ts)
	}

	s.mu.Lock()
	var stale []string
	for _, ts := range loaded {
		// A copy older than a removal marker is what a crash left behind
		// before the removal's S3 delete was retried: not a tombstone.
		if s.supersededByMarkerLocked(ts) {
			stale = append(stale, ts.ID)
			continue
		}
		if rejectUnscoped(ts, "s3") {
			continue
		}
		s.mergeLoadedLocked(ts)
	}
	s.owePendingS3DeletesLocked(stale)
	s.mu.Unlock()

	if skipped > 0 {
		metrics.DeleteStartupInconsistencies.Inc("unreadable_tombstone_object")
		return fmt.Errorf("restored %d of %d tombstone objects", len(loaded), len(keys))
	}
	return nil
}

// Validate rejects a tombstone that cannot be stored and enforced faithfully.
//
// Two of these checks exist because of what happens if they do not:
//
//   - A query that does not parse as LogsQL matches nothing. The delete then
//     appears to succeed and silently hides no rows, which the user experiences
//     as "the delete did not work" with no error anywhere. Rejecting at the API
//     turns a silent no-op into a 400.
//   - A query carrying invalid UTF-8 does not survive the durable JSON
//     encoding: the bytes come back replaced with U+FFFD, so the tombstone
//     restored after a restart is not the tombstone that was stored. Refusing
//     it keeps "what was persisted" and "what was accepted" the same record.
//     (Found by FuzzTombstoneRoundTrip.)
//
// An inverted time range is rejected for the same reason as the first: it can
// never match a row.
func (t *Tombstone) Validate() error {
	if t.ID == "" {
		return fmt.Errorf("tombstone has no id")
	}
	// Every persisted string must be valid UTF-8. JSON replaces invalid bytes
	// with U+FFFD, so a record carrying them is not the record that comes back:
	// a mutated id collides with (or hides from) the original on restore, a
	// mutated query stops matching, and a mutated affected key sends the
	// rewriter after an object that does not exist while the real one is never
	// reaped. Found by FuzzTombstoneRoundTrip.
	for _, f := range []struct{ name, value string }{
		{"id", t.ID},
		{"query", t.Query},
		{"created_by", t.CreatedBy},
		{"mode", t.Mode},
	} {
		if !utf8.ValidString(f.value) {
			return fmt.Errorf("%s is not valid UTF-8; it would not survive being persisted", f.name)
		}
	}
	for _, k := range t.AffectedKeys {
		if !utf8.ValidString(k) {
			return fmt.Errorf("affected key is not valid UTF-8; it would not survive being persisted")
		}
	}
	for k := range t.Clean {
		if !utf8.ValidString(k) {
			return fmt.Errorf("clean key is not valid UTF-8; it would not survive being persisted")
		}
	}
	for k, sup := range t.Superseded {
		if !utf8.ValidString(k) || !utf8.ValidString(sup.NewKey) {
			return fmt.Errorf("superseded key is not valid UTF-8; it would not survive being persisted")
		}
		switch sup.State {
		case SupersessionPrepared, SupersessionPublished, SupersessionDiscarded:
		default:
			return fmt.Errorf("unknown rewrite state %q for %s", sup.State, k)
		}
	}
	if t.StartNs > t.EndNs {
		return fmt.Errorf("time range is inverted: start %d is after end %d", t.StartNs, t.EndNs)
	}
	if len(t.Tenants) == 0 {
		return ErrNoTenantScope
	}
	if t.Query != "" && t.Query != "*" {
		if _, err := parseFilterAt(t.Query, t.FilterAt); err != nil {
			return fmt.Errorf("query does not parse as LogsQL: %w", err)
		}
	}
	switch t.Mode {
	case "", "hide", "permanent", "auto":
	default:
		return fmt.Errorf("unknown mode %q", t.Mode)
	}
	return nil
}
