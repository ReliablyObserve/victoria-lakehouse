package delete

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
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
	f, err := logstorage.ParseFilter(t.Query)
	if err != nil {
		return false
	}
	fields := make([]logstorage.Field, 0, len(row))
	for k, v := range row {
		fields = append(fields, logstorage.Field{Name: k, Value: v})
	}
	return f.MatchRow(fields)
}

// TombstoneStore is a thread-safe in-memory store for tombstones.
type TombstoneStore struct {
	mu         sync.RWMutex
	tombstones map[string]Tombstone
}

// NewTombstoneStore creates a new empty TombstoneStore.
func NewTombstoneStore() *TombstoneStore {
	return &TombstoneStore{
		tombstones: make(map[string]Tombstone),
	}
}

// Add inserts a tombstone into the store.
func (s *TombstoneStore) Add(ts Tombstone) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tombstones[ts.ID] = ts
}

// Remove deletes a tombstone from the store by ID.
func (s *TombstoneStore) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tombstones, id)
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
	s.tombstones = loaded
	s.mu.Unlock()

	return nil
}

// SyncToS3 writes each tombstone as an individual JSON file to key {tenant}/_tombstones/{id}.json.
func (s *TombstoneStore) SyncToS3(ctx context.Context, pool S3Pool, bucket, tenant string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for id, ts := range s.tombstones {
		data, err := json.Marshal(ts)
		if err != nil {
			return fmt.Errorf("marshal tombstone %s: %w", id, err)
		}

		key := fmt.Sprintf("%s/_tombstones/%s.json", tenant, id)
		if err := pool.Upload(ctx, key, data); err != nil {
			return fmt.Errorf("upload tombstone %s: %w", id, err)
		}
	}

	return nil
}

// LoadFromS3 lists all keys under {tenant}/_tombstones/ prefix, downloads each,
// and unmarshals into the store.
func (s *TombstoneStore) LoadFromS3(ctx context.Context, pool S3Pool, bucket, tenant string) error {
	prefix := fmt.Sprintf("%s/_tombstones/", tenant)

	keys, err := pool.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("list tombstones: %w", err)
	}

	if len(keys) == 0 {
		return nil
	}

	loaded := make(map[string]Tombstone, len(keys))
	for _, key := range keys {
		data, err := pool.Download(ctx, key)
		if err != nil {
			return fmt.Errorf("download %s: %w", key, err)
		}

		var ts Tombstone
		if err := json.Unmarshal(data, &ts); err != nil {
			return fmt.Errorf("unmarshal %s: %w", key, err)
		}

		loaded[ts.ID] = ts
	}

	s.mu.Lock()
	s.tombstones = loaded
	s.mu.Unlock()

	return nil
}
