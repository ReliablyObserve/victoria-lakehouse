package delete

import (
	"strings"
	"sync"
	"time"
)

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
func (t *Tombstone) MatchesRow(row map[string]string, timestampNs int64) bool {
	if timestampNs < t.StartNs || timestampNs > t.EndNs {
		return false
	}
	return matchQuery(t.Query, row)
}

// matchQuery evaluates a query string against a row.
// Supported patterns:
//   - "*" or empty string: matches all rows
//   - "field:=\"exact\"": exact match on field value
//   - "field:\"substring\"": substring match on field value
//   - fallback: substring match on "body" field
func matchQuery(query string, row map[string]string) bool {
	if query == "" || query == "*" {
		return true
	}

	// Exact match: field:="value"
	if idx := strings.Index(query, `:="`); idx > 0 {
		field := query[:idx]
		// Extract value between quotes
		rest := query[idx+3:]
		endQuote := strings.Index(rest, `"`)
		if endQuote < 0 {
			return false
		}
		value := rest[:endQuote]
		return row[field] == value
	}

	// Substring match: field:"value"
	if idx := strings.Index(query, `:"`); idx > 0 {
		field := query[:idx]
		rest := query[idx+2:]
		endQuote := strings.Index(rest, `"`)
		if endQuote < 0 {
			return false
		}
		value := rest[:endQuote]
		return strings.Contains(row[field], value)
	}

	// Fallback: substring match on body field
	return strings.Contains(row["body"], query)
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
