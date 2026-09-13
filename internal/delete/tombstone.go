package delete

import (
	"context"
	"encoding/json"
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
	Reaped       map[string]bool
	Mode         string // "hide"|"permanent"|"auto"
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
	f := parseFilterCached(t.Query)
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
	f := parseFilterCached(t.Query)
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
	return parseFilterCached(t.Query)
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

// FullyReaped reports whether every key this tombstone covers has been
// rewritten. A fully reaped tombstone has no remaining work: the rows it hides
// are already physically gone from every file it named.
func (t *Tombstone) FullyReaped() bool {
	if len(t.AffectedKeys) == 0 {
		return false
	}
	for _, k := range t.AffectedKeys {
		if !t.Reaped[k] {
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
	s.persist = &tombstonePersistence{
		dir:     cfg.Dir,
		pool:    cfg.Pool,
		prefix:  normalizeTombstonePrefix(cfg.Prefix),
		pending: make(map[string]pendingOp),
	}
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
	s.tombstones[ts.ID] = cloneTombstone(ts)
	p := s.persist
	s.mu.Unlock()
	metrics.DeleteTombstonesActive.Set(int64(s.Count()))
	s.persistChange(p, ts.ID, pendingUpsert)
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
	p := s.persist
	s.mu.Unlock()
	s.persistChange(p, id, pendingUpsert)
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
	return ts
}

// Remove deletes a tombstone from the store by ID and persists the removal, so
// an un-delete is not resurrected by the next restart.
func (s *TombstoneStore) Remove(id string) {
	s.mu.Lock()
	delete(s.tombstones, id)
	p := s.persist
	s.mu.Unlock()
	metrics.DeleteTombstonesActive.Set(int64(s.Count()))
	s.persistChange(p, id, pendingDelete)
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
	p := s.persist
	observer := s.onComplete
	s.mu.Unlock()

	metrics.DeleteTombstonesCompleted.Inc()
	metrics.DeleteTombstonesActive.Set(int64(s.Count()))
	logger.Infof("tombstone completed; id=%s, query=%s, keys=%d", ts.ID, ts.Query, len(ts.AffectedKeys))
	if observer != nil {
		observer(ts)
	}
	s.persistChange(p, id, pendingDelete)
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

// Count returns the number of tombstones in the store.
func (s *TombstoneStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tombstones)
}

// PersistToDisk marshals all tombstones to JSON and writes atomically to {dir}/tombstones.json.
// Creates dir if needed with mode 0o755.
func (s *TombstoneStore) PersistToDisk(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}

	s.mu.RLock()
	data, err := json.Marshal(s.tombstones)
	s.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("marshal tombstones: %w", err)
	}

	target := filepath.Join(dir, "tombstones.json")
	tmp := target + ".tmp"

	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write tmp file: %w", err)
	}

	if err := os.Rename(tmp, target); err != nil {
		return fmt.Errorf("rename to target: %w", err)
	}

	return nil
}

// LoadFromDisk reads {dir}/tombstones.json and unmarshals into the store.
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

	var loaded map[string]Tombstone
	if err := json.Unmarshal(data, &loaded); err != nil {
		return fmt.Errorf("unmarshal tombstones: %w", err)
	}

	s.mu.Lock()
	for _, ts := range loaded {
		s.mergeLoadedLocked(ts)
	}
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
	for _, ts := range loaded {
		s.mergeLoadedLocked(ts)
	}
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
	if t.StartNs > t.EndNs {
		return fmt.Errorf("time range is inverted: start %d is after end %d", t.StartNs, t.EndNs)
	}
	if t.Query != "" && t.Query != "*" {
		if _, err := logstorage.ParseFilter(t.Query); err != nil {
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
