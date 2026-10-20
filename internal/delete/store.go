package delete

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ErrNoTenantScope rejects a tombstone that names no tenant: every tombstone
// acts on the tenants it names and on nothing else.
var ErrNoTenantScope = errors.New("tombstone names no tenant")

// ErrTombstoneNotFound is returned for an unknown id.
var ErrTombstoneNotFound = errors.New("tombstone not found")

// entry is one tombstone in a pod's view.
type entry struct {
	t      Tombstone
	defKey string // the definition's S3 key; empty for a tombstone added to the view directly
	// undone: an undo key exists for this (hide-mode) tombstone. The rows it hid
	// are visible again, on every pod. It stays in the view (it costs a few bytes)
	// so that the definition, listed again, is not learned as active.
	undone bool
}

// TombstoneStore is a pod's view of the deletes of one signal.
//
// The view is add-only with respect to the bucket: a tombstone enters it when
// its definition key is listed (or created here), and leaves it only on positive
// evidence: an undo key exists (hide mode only), its retention has passed
// (deterministic from the definition and the pod's clock), or a HEAD of its
// definition answers 404 after a full listing missed it. Absence from a listing
// removes nothing, and nothing in the view ever causes a DELETE. See
// protocol.go for how the view is kept current.
//
// The view is also what readers filter by: ForRange, Get and Active return the
// tombstones in force.
type TombstoneStore struct {
	mu   sync.RWMutex
	byID map[string]*entry
	// byEnd holds the same entries ordered by (range end, id), so a query finds
	// the tombstones that can overlap its time range by binary search instead of
	// visiting all of them: a query newer than every delete costs O(log N), which
	// keeps the cost of having many tombstones off the common query.
	byEnd []*entry
	// inForce is the number of entries that are not undone, kept as a running
	// count so Count and the gauge are O(1).
	inForce int
	// filterRefs counts the entries using each parsed filter, so a filter leaves
	// the process-wide cache when its last user goes, without walking the view.
	filterRefs map[filterKey]int

	// proto is the bucket protocol, set by Attach. Nil in a view that is only
	// filled by Add (unit tests of the read path).
	proto atomic.Pointer[protocol]
}

// NewTombstoneStore creates a new empty view.
func NewTombstoneStore() *TombstoneStore {
	return &TombstoneStore{byID: make(map[string]*entry), filterRefs: make(map[filterKey]int)}
}

// Add puts a tombstone into the view. It does not write to the bucket: it is
// what the protocol calls once a definition is known, and what the read-path
// tests use to put a tombstone in force. A tombstone that is already in the view
// is replaced (and is in force again).
func (s *TombstoneStore) Add(ts Tombstone) {
	s.mu.Lock()
	s.putLocked(&entry{t: cloneTombstone(ts)})
	s.mu.Unlock()
	s.updateActiveGauges()
}

func (s *TombstoneStore) putLocked(e *entry) {
	if prev, replaced := s.byID[e.t.ID]; replaced {
		s.indexRemoveLocked(prev)
		s.dropLocked(prev)
	}
	s.byID[e.t.ID] = e
	s.indexInsertLocked(e)
	if !e.undone {
		s.inForce++
	}
	s.filterRefs[filterKey{query: e.t.Query, at: e.t.FilterAt}]++
}

// dropLocked removes e from the maps and the counters, but not from the ordered
// index (forgetMany fixes the index in one pass for a batch).
func (s *TombstoneStore) dropLocked(e *entry) {
	delete(s.byID, e.t.ID)
	if !e.undone {
		s.inForce--
	}
	k := filterKey{query: e.t.Query, at: e.t.FilterAt}
	if s.filterRefs[k]--; s.filterRefs[k] <= 0 {
		delete(s.filterRefs, k)
		parsedFilters.Delete(k)
	}
}

// markUndoneLocked records that an undo key exists for e.
func (s *TombstoneStore) markUndoneLocked(e *entry) {
	if !e.undone {
		e.undone = true
		s.inForce--
	}
}

func entryLess(a, b *entry) bool {
	if a.t.EndNs != b.t.EndNs {
		return a.t.EndNs < b.t.EndNs
	}
	return a.t.ID < b.t.ID
}

func (s *TombstoneStore) indexInsertLocked(e *entry) {
	i := sort.Search(len(s.byEnd), func(i int) bool { return !entryLess(s.byEnd[i], e) })
	s.byEnd = append(s.byEnd, nil)
	copy(s.byEnd[i+1:], s.byEnd[i:])
	s.byEnd[i] = e
}

func (s *TombstoneStore) indexRemoveLocked(e *entry) {
	i := sort.Search(len(s.byEnd), func(i int) bool { return !entryLess(s.byEnd[i], e) })
	if i < len(s.byEnd) && s.byEnd[i] == e {
		copy(s.byEnd[i:], s.byEnd[i+1:])
		s.byEnd[len(s.byEnd)-1] = nil
		s.byEnd = s.byEnd[:len(s.byEnd)-1]
	}
}

// cloneTombstone deep-copies the mutable parts of a tombstone.
func cloneTombstone(ts Tombstone) Tombstone {
	if ts.Tenants != nil {
		ts.Tenants = append([]TenantRef(nil), ts.Tenants...)
	}
	return ts
}

// forget removes id from the view. Only the protocol calls it, on positive
// evidence.
func (s *TombstoneStore) forget(id string) { s.forgetMany([]string{id}) }

// forgetMany removes the ids in one critical section: the cost is one pass over
// the ordered index however many go, so a mass expiry does not hold the lock for
// quadratic time.
func (s *TombstoneStore) forgetMany(ids []string) {
	if len(ids) == 0 {
		return
	}
	s.mu.Lock()
	gone := make([]*entry, 0, len(ids))
	for _, id := range ids {
		if e, ok := s.byID[id]; ok {
			s.dropLocked(e)
			gone = append(gone, e)
		}
	}
	if len(gone) <= 16 {
		for _, e := range gone {
			s.indexRemoveLocked(e)
		}
	} else {
		kept := s.byEnd[:0]
		for _, e := range s.byEnd {
			if cur, ok := s.byID[e.t.ID]; ok && cur == e {
				kept = append(kept, e)
			}
		}
		for i := len(kept); i < len(s.byEnd); i++ {
			s.byEnd[i] = nil
		}
		s.byEnd = kept
	}
	s.mu.Unlock()
	s.updateActiveGauges()
}

// Get retrieves a tombstone in force by id.
func (s *TombstoneStore) Get(id string) (Tombstone, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.byID[id]
	if !ok || e.undone {
		return Tombstone{}, false
	}
	return e.t, true
}

// state is Get plus the undo flag: whether the tombstone exists at all and, when
// it does, whether it was un-deleted.
func (s *TombstoneStore) state(id string) (e entry, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cur, ok := s.byID[id]
	if !ok {
		return entry{}, false
	}
	return *cur, true
}

// Active returns every tombstone in force.
func (s *TombstoneStore) Active() []Tombstone {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Tombstone, 0, len(s.byID))
	for _, e := range s.byID {
		if !e.undone {
			result = append(result, e.t)
		}
	}
	return result
}

// ForRange returns the tombstones in force whose time range overlaps
// [startNs, endNs].
func (s *TombstoneStore) ForRange(startNs, endNs int64) []Tombstone {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []Tombstone
	// Only entries whose range ends at or after startNs can overlap.
	i := sort.Search(len(s.byEnd), func(i int) bool { return s.byEnd[i].t.EndNs >= startNs })
	for _, e := range s.byEnd[i:] {
		if !e.undone && e.t.StartNs <= endNs {
			result = append(result, e.t)
		}
	}
	return result
}

// Count returns the number of tombstones in force.
func (s *TombstoneStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inForce
}

// updateActiveGauges publishes the number of tombstones in force.
func (s *TombstoneStore) updateActiveGauges() {
	metrics.DeleteTombstonesActive.Set(int64(s.Count()))
}

// ViewAge is how long ago the view last completed a sync with the bucket; it is
// meaningful only once a protocol is attached. ok is false before the first
// complete sync.
func (s *TombstoneStore) ViewAge() (age time.Duration, ok bool) {
	p := s.proto.Load()
	if p == nil {
		return 0, false
	}
	return p.viewAge()
}
