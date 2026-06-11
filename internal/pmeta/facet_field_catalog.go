package pmeta

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// valueSet is a sorted, de-duplicated set of value ids. Dep-free and compact for
// the small per-partition sets low-cardinality fields produce. For a kept-exact
// high-cardinality field it can be swapped for a roaring bitmap without changing
// the facet's wire format or API (see docs/architecture/field-value-catalog.md).
type valueSet struct{ ids []uint32 }

func (s *valueSet) add(id uint32) {
	i := sort.Search(len(s.ids), func(i int) bool { return s.ids[i] >= id })
	if i < len(s.ids) && s.ids[i] == id {
		return // already present
	}
	s.ids = append(s.ids, 0)
	copy(s.ids[i+1:], s.ids[i:])
	s.ids[i] = id
}

// fieldCatalogFacet is the per-partition dropdown value catalog (low-card only;
// HLL for high-cardinality fields lands in A2). It maps each field to the set of
// distinct values present in this partition, as ids into the shared Dict, so the
// value strings are interned once globally. Typeahead is an exact substring
// match over the resolved strings — no sketch in the value path, so dropdowns
// are exact (see the exactness contract in the catalog design doc).
type fieldCatalogFacet struct {
	partition    string
	dict         *Dict
	threshold    int             // max distinct values/field before high-card; 0 = unlimited
	alwaysSketch map[string]bool // field names forced high-card
	mu           sync.RWMutex
	byField      map[uint32]*valueSet // fieldID -> value ids present (low-card only)
	highCard     map[uint32]bool      // fieldID -> high-card (capped/forced); values not enumerable
}

// NewFieldCatalogFactory returns a FacetFactory bound to a shared Dict that keeps
// every field exact (no cardinality cap). Used by tests and unlimited deployments.
func NewFieldCatalogFactory(dict *Dict) FacetFactory {
	return NewFieldCatalogFactoryCapped(dict, 0, nil)
}

// NewFieldCatalogFactoryCapped returns a factory that marks a field high-card once
// it exceeds threshold distinct values (0 = unlimited) or is in alwaysSketch. A
// high-card field stops storing values (bounding RAM) and is not enumerable —
// the read path falls through to the legacy scan, never a truncated list.
func NewFieldCatalogFactoryCapped(dict *Dict, threshold int, alwaysSketch []string) FacetFactory {
	sketch := make(map[string]bool, len(alwaysSketch))
	for _, f := range alwaysSketch {
		sketch[f] = true
	}
	return func(partition string) Facet {
		return &fieldCatalogFacet{
			partition:    partition,
			dict:         dict,
			threshold:    threshold,
			alwaysSketch: sketch,
			byField:      map[uint32]*valueSet{},
			highCard:     map[uint32]bool{},
		}
	}
}

// FieldValues is the Store's public read surface for field-value dropdowns: the
// distinct values of a field present in a partition (substr typeahead filter,
// limit cap), from the field/value catalog facet. Empty if the partition, the
// catalog facet, or the field is absent — callers fall through to the legacy
// scan in that case.
func (s *Store) FieldValues(partition, field, substr string, limit int) []string {
	if c, ok := s.catalog(partition); ok {
		return c.Values(field, substr, limit)
	}
	return nil
}

// FieldNames returns the field names present in a partition (for field_names),
// from the catalog facet; empty if absent.
func (s *Store) FieldNames(partition string) []string {
	if c, ok := s.catalog(partition); ok {
		return c.Fields()
	}
	return nil
}

func (s *Store) catalog(partition string) (*fieldCatalogFacet, bool) {
	f, ok := s.Get(partition, FacetFieldCatalog)
	if !ok {
		return nil, false
	}
	c, ok := f.(*fieldCatalogFacet)
	return c, ok
}

func (f *fieldCatalogFacet) Kind() FacetKind { return FacetFieldCatalog }

// Merge folds a file's low-card label values into the partition catalog. Also
// the rebuild path: replaying a partition's contributions reproduces the facet
// exactly (parity), and Decode is implemented in terms of Merge for that reason.
func (f *fieldCatalogFacet) Merge(c FileContribution) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// A field whose per-file value list hit the extractor cap may be incomplete —
	// the catalog must never serve a truncated list as authoritative, so the field
	// becomes high-card (reads fall through to the scan, which is exact).
	for _, field := range c.TruncatedFields {
		f.markHighCard(f.dict.internField(field))
	}
	for field, vals := range c.Labels {
		fid := f.dict.internField(field)
		if f.highCard[fid] {
			continue // already high-card: stop storing values (RAM bound)
		}
		if f.alwaysSketch[field] {
			f.markHighCard(fid)
			continue
		}
		vs := f.byField[fid]
		if vs == nil {
			vs = &valueSet{}
			f.byField[fid] = vs
		}
		for _, v := range vals {
			vs.add(f.dict.internValue(v))
			if f.threshold > 0 && len(vs.ids) > f.threshold {
				f.markHighCard(fid) // crossed the cap → high-card
				break
			}
		}
	}
}

// markHighCard flags a field high-card and drops its now-incomplete value set so
// RAM stays bounded. The field remains known (Fields() still lists it) but its
// values are not enumerable. Caller holds f.mu.
func (f *fieldCatalogFacet) markHighCard(fid uint32) {
	f.highCard[fid] = true
	delete(f.byField, fid)
}

// IsHighCard reports whether a field crossed the cardinality cap (or was forced
// to sketch). High-card fields are not enumerable from the catalog.
func (f *fieldCatalogFacet) IsHighCard(field string) bool {
	fid, ok := f.dict.fieldID(field)
	if !ok {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.highCard[fid]
}

// Values returns the distinct values of a field present in this partition,
// filtered to those containing substr (empty = all), sorted, capped at limit
// (limit <= 0 = no cap). The dropdown / typeahead answer — exact, from RAM.
func (f *fieldCatalogFacet) Values(field, substr string, limit int) []string {
	fid, ok := f.dict.fieldID(field)
	if !ok {
		return nil
	}
	f.mu.RLock()
	if f.highCard[fid] {
		f.mu.RUnlock()
		return nil // high-card: not enumerable → caller falls through to scan
	}
	vs := f.byField[fid]
	var ids []uint32
	if vs != nil {
		// Copy WHILE HOLDING the lock: Merge mutates vs.ids in place (append +
		// element shift) under the write lock — iterating the live slice after
		// RUnlock is a data race (torn/duplicated/missing values).
		ids = append([]uint32(nil), vs.ids...)
	}
	f.mu.RUnlock()
	if ids == nil {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		s, ok := f.dict.value(id)
		if ok && (substr == "" || strings.Contains(s, substr)) {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Fields returns the field names present in this partition (for field_names),
// sorted.
func (f *fieldCatalogFacet) Fields() []string {
	f.mu.RLock()
	ids := make([]uint32, 0, len(f.byField)+len(f.highCard))
	for fid := range f.byField {
		ids = append(ids, fid)
	}
	for fid := range f.highCard { // high-card fields are still valid field NAMES
		ids = append(ids, fid)
	}
	f.mu.RUnlock()
	out := make([]string, 0, len(ids))
	for _, fid := range ids {
		if s, ok := f.dict.field(fid); ok {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func (f *fieldCatalogFacet) EstimateBytes() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var n int64
	for _, vs := range f.byField {
		n += int64(len(vs.ids))*4 + 32 // ids + valueSet/map overhead
	}
	return n // dict strings are accounted once by Dict.EstimateBytes
}

// Encode writes a SELF-CONTAINED payload (value strings, not global ids) so a
// partition is rebuildable independently of the in-RAM dict. Deterministic
// (fields + values sorted) for golden byte-identity.
//
//	fieldCount[4]
//	per field: nameLen[2] name valueCount[4] (valLen[4] val)…
//	highCardCount[4]
//	per high-card field: nameLen[2] name
//
// The high-card section round-trips the cardinality-cap / always-sketch state:
// without it a decoded facet would re-accumulate values for an already-capped
// field and could serve a truncated list as authoritative.
func (f *fieldCatalogFacet) Encode(w io.Writer) error {
	type fv struct {
		name string
		vals []string
	}
	f.mu.RLock()
	rows := make([]fv, 0, len(f.byField))
	for fid, vs := range f.byField {
		name, _ := f.dict.field(fid)
		vals := make([]string, 0, len(vs.ids))
		for _, id := range vs.ids {
			if s, ok := f.dict.value(id); ok {
				vals = append(vals, s)
			}
		}
		sort.Strings(vals)
		rows = append(rows, fv{name: name, vals: vals})
	}
	hc := make([]string, 0, len(f.highCard))
	for fid := range f.highCard {
		if name, ok := f.dict.field(fid); ok {
			hc = append(hc, name)
		}
	}
	f.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	sort.Strings(hc)

	bw := bufio.NewWriter(w)
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], uint32(len(rows)))
	if _, err := bw.Write(u32[:]); err != nil {
		return err
	}
	for _, r := range rows {
		var u16 [2]byte
		binary.BigEndian.PutUint16(u16[:], uint16(len(r.name)))
		if _, err := bw.Write(u16[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(r.name); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(u32[:], uint32(len(r.vals)))
		if _, err := bw.Write(u32[:]); err != nil {
			return err
		}
		for _, v := range r.vals {
			binary.BigEndian.PutUint32(u32[:], uint32(len(v)))
			if _, err := bw.Write(u32[:]); err != nil {
				return err
			}
			if _, err := bw.WriteString(v); err != nil {
				return err
			}
		}
	}
	binary.BigEndian.PutUint32(u32[:], uint32(len(hc)))
	if _, err := bw.Write(u32[:]); err != nil {
		return err
	}
	for _, name := range hc {
		var u16 [2]byte
		binary.BigEndian.PutUint16(u16[:], uint16(len(name)))
		if _, err := bw.Write(u16[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(name); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// decode caps: a CRC-valid but hostile/corrupt payload must not drive multi-GB
// allocations. Lengths are validated BEFORE allocating; counts only bound the
// prealloc hint (the loop itself is bounded by the payload bytes running out).
const (
	maxCatalogValueLen     = 1 << 20 // 1 MiB per value string
	maxCatalogPreallocVals = 1 << 16 // prealloc hint cap; slice still grows as needed
)

// Decode reconstructs the facet via Merge, so a loaded facet is byte-for-byte
// identical in behavior to one built live from the same files (decode==merge
// parity), then applies the high-card section. The bundle codec has already
// CRC-verified these bytes.
func (f *fieldCatalogFacet) Decode(r io.Reader) error {
	br := bufio.NewReader(r)
	var u32 [4]byte
	if _, err := io.ReadFull(br, u32[:]); err != nil {
		return err
	}
	nf := binary.BigEndian.Uint32(u32[:])
	for i := uint32(0); i < nf; i++ {
		var u16 [2]byte
		if _, err := io.ReadFull(br, u16[:]); err != nil {
			return err
		}
		name := make([]byte, binary.BigEndian.Uint16(u16[:]))
		if _, err := io.ReadFull(br, name); err != nil {
			return err
		}
		if _, err := io.ReadFull(br, u32[:]); err != nil {
			return err
		}
		nv := binary.BigEndian.Uint32(u32[:])
		vals := make([]string, 0, int(min(nv, maxCatalogPreallocVals)))
		for j := uint32(0); j < nv; j++ {
			if _, err := io.ReadFull(br, u32[:]); err != nil {
				return err
			}
			vlen := binary.BigEndian.Uint32(u32[:])
			if vlen > maxCatalogValueLen {
				return fmt.Errorf("pmeta: catalog value len %d over cap", vlen)
			}
			vb := make([]byte, vlen)
			if _, err := io.ReadFull(br, vb); err != nil {
				return err
			}
			vals = append(vals, string(vb))
		}
		f.Merge(FileContribution{Labels: map[string][]string{string(name): vals}})
	}
	// High-card section (always present in the current facet payload version).
	if _, err := io.ReadFull(br, u32[:]); err != nil {
		return err
	}
	nh := binary.BigEndian.Uint32(u32[:])
	for i := uint32(0); i < nh; i++ {
		var u16 [2]byte
		if _, err := io.ReadFull(br, u16[:]); err != nil {
			return err
		}
		name := make([]byte, binary.BigEndian.Uint16(u16[:]))
		if _, err := io.ReadFull(br, name); err != nil {
			return err
		}
		f.mu.Lock()
		f.markHighCard(f.dict.internField(string(name)))
		f.mu.Unlock()
	}
	return nil
}

// absorbFacet folds another catalog facet's content into this one (the
// warm-merge path: an S3 bundle decoded while live flushes already populated the
// bundle). High-card status is a union — once not-enumerable, always
// not-enumerable.
func (f *fieldCatalogFacet) absorbFacet(other Facet) {
	oc, ok := other.(*fieldCatalogFacet)
	if !ok {
		return
	}
	f.absorb(oc)
}

func (f *fieldCatalogFacet) absorb(other *fieldCatalogFacet) {
	for _, field := range other.Fields() {
		if other.IsHighCard(field) {
			f.mu.Lock()
			f.markHighCard(f.dict.internField(field))
			f.mu.Unlock()
			continue
		}
		vals := other.Values(field, "", 0)
		if len(vals) > 0 {
			f.Merge(FileContribution{Labels: map[string][]string{field: vals}})
		}
	}
}
