package pmeta

import "sync"

// Dict is the process-global interned value dictionary shared by all
// fieldCatalogFacets. Low-cardinality value strings (and field names) are stored
// once and referenced by a stable uint32 id; per-partition facets hold only the
// ids, so the value strings are not duplicated across the (many) partitions in
// RAM. Append-only: ids never move, so reads after a value exists are stable.
//
// Persisted facet payloads are self-contained (they store value STRINGS, not
// global ids) so a partition can be rebuilt independently; the ids are a
// purely in-RAM interning optimization re-established on load.
type Dict struct {
	mu      sync.RWMutex
	valIDs  map[string]uint32
	valStrs []string
	fldIDs  map[string]uint32
	fldStrs []string
}

// NewDict returns an empty interning dictionary.
func NewDict() *Dict {
	return &Dict{valIDs: make(map[string]uint32), fldIDs: make(map[string]uint32)}
}

func intern(mu *sync.RWMutex, ids map[string]uint32, strs *[]string, s string) uint32 {
	mu.RLock()
	id, ok := ids[s]
	mu.RUnlock()
	if ok {
		return id
	}
	mu.Lock()
	defer mu.Unlock()
	if id, ok = ids[s]; ok { // re-check under write lock
		return id
	}
	id = uint32(len(*strs))
	*strs = append(*strs, s)
	ids[s] = id
	return id
}

// internValue returns the stable id for a value string, assigning one if new.
func (d *Dict) internValue(s string) uint32 { return intern(&d.mu, d.valIDs, &d.valStrs, s) }

// internField returns the stable id for a field name, assigning one if new.
func (d *Dict) internField(s string) uint32 { return intern(&d.mu, d.fldIDs, &d.fldStrs, s) }

// value resolves a value id back to its string.
func (d *Dict) value(id uint32) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if int(id) < len(d.valStrs) {
		return d.valStrs[id], true
	}
	return "", false
}

// fieldID returns the id of an already-interned field name.
func (d *Dict) fieldID(name string) (uint32, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	id, ok := d.fldIDs[name]
	return id, ok
}

// field resolves a field id back to its name.
func (d *Dict) field(id uint32) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if int(id) < len(d.fldStrs) {
		return d.fldStrs[id], true
	}
	return "", false
}

// EstimateBytes is the resident size of the interned strings (drives the global
// dict's contribution to the memory budget).
func (d *Dict) EstimateBytes() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var n int64
	for _, s := range d.valStrs {
		n += int64(len(s)) + 24 // string + map/slice overhead, amortized
	}
	for _, s := range d.fldStrs {
		n += int64(len(s)) + 24
	}
	return n
}
