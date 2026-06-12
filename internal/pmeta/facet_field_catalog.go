package pmeta

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
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
	hll          map[uint32]*hll      // fieldID -> distinct-count sketch (high-card only); persisted
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
			hll:          map[uint32]*hll{},
		}
	}
}

// fieldHLL returns the per-field distinct-count sketch for a high-card field, or
// nil if the field is absent / still low-card. Used by Store.FieldCardinality to
// union per-partition sketches into the global estimate.
func (f *fieldCatalogFacet) fieldHLL(field string) *hll {
	fid, ok := f.dict.fieldID(field)
	if !ok {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.hll[fid]
}

// hllFor returns (creating if needed) the sketch for a field id. Caller holds f.mu.
func (f *fieldCatalogFacet) hllFor(fid uint32) *hll {
	h := f.hll[fid]
	if h == nil {
		h = newHLL(defaultHLLPrecision)
		f.hll[fid] = h
	}
	return h
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
	// High-card fields: fold the raw values into the per-field sketch so the
	// distinct-count survives restart via the bundle (these are exactly the
	// fields the catalog does not enumerate). Persisted in Encode/Decode.
	for field, vals := range c.HighCardValues {
		fid := f.dict.internField(field)
		f.highCard[fid] = true
		delete(f.byField, fid)
		h := f.hllFor(fid)
		for _, v := range vals {
			h.add(v)
		}
	}
}

// markHighCard flags a field high-card and drops its now-incomplete value set so
// RAM stays bounded. The field remains known (Fields() still lists it) but its
// values are not enumerable. Caller holds f.mu.
func (f *fieldCatalogFacet) markHighCard(fid uint32) {
	f.highCard[fid] = true
	// Fold the soon-dropped enumerated values into the sketch first, so the
	// cap-crossing distinct count isn't lost when byField is cleared.
	if vs := f.byField[fid]; vs != nil {
		h := f.hllFor(fid)
		for _, id := range vs.ids {
			if s, ok := f.dict.value(id); ok {
				h.add(s)
			}
		}
	}
	delete(f.byField, fid)
}

// addHighCardValues folds a value stream into a field's HLL and marks the field
// high-card (values not enumerated). For always-sketch id columns (trace_id,
// span_id) whose values arrive from the row tap rather than Labels: this feeds the
// PERSISTED per-partition sketch (in the bundle) instead of only the RAM side-map,
// so the distinct count survives restart. Empty strings are skipped. Streamed as an
// iter.Seq so the caller feeds straight off the row structs (no slice).
func (f *fieldCatalogFacet) addHighCardValues(field string, values iter.Seq[string]) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fid := f.dict.internField(field)
	f.highCard[fid] = true
	delete(f.byField, fid)
	h := f.hllFor(fid)
	for v := range values {
		if v != "" {
			h.add(v)
		}
	}
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

// BytesByField returns each field's catalog footprint (bytes), keyed by field
// name. A low-card field's bytes mirror EstimateBytes' per-field term exactly
// (len(ids)*4 + 32 for the value-set + map overhead); a high-card field's bytes
// are its persisted distinct-count HLL register array (len(reg)) — the catalog
// entry the facet keeps for a field it does not enumerate. Summing the values is
// the per-field decomposition of this facet's metadata contribution. Dict strings
// are intentionally excluded (accounted once globally by Dict.EstimateBytes).
func (f *fieldCatalogFacet) BytesByField() map[string]int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]int64, len(f.byField)+len(f.hll))
	for fid, vs := range f.byField {
		if name, ok := f.dict.field(fid); ok {
			out[name] += int64(len(vs.ids))*4 + 32
		}
	}
	for fid, h := range f.hll {
		if name, ok := f.dict.field(fid); ok {
			out[name] += int64(len(h.reg)) // persisted per-field distinct-count sketch
		}
	}
	return out
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
	type hllEnc struct {
		name string
		p    uint8
		reg  []byte
	}
	hlls := make([]hllEnc, 0, len(f.hll))
	for fid, h := range f.hll {
		if name, ok := f.dict.field(fid); ok {
			hlls = append(hlls, hllEnc{name: name, p: h.p, reg: append([]byte(nil), h.reg...)})
		}
	}
	f.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	sort.Strings(hc)
	sort.Slice(hlls, func(i, j int) bool { return hlls[i].name < hlls[j].name })

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
	// HLL section: appended AFTER the high-card names so a pre-HLL bundle simply
	// ends here and Decode treats the absent section as "no sketches" (EOF-
	// tolerant). per field: nameLen[2] name p[1] regLen[4] reg.
	binary.BigEndian.PutUint32(u32[:], uint32(len(hlls)))
	if _, err := bw.Write(u32[:]); err != nil {
		return err
	}
	for _, h := range hlls {
		var u16 [2]byte
		binary.BigEndian.PutUint16(u16[:], uint16(len(h.name)))
		if _, err := bw.Write(u16[:]); err != nil {
			return err
		}
		if _, err := bw.WriteString(h.name); err != nil {
			return err
		}
		if err := bw.WriteByte(h.p); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(u32[:], uint32(len(h.reg)))
		if _, err := bw.Write(u32[:]); err != nil {
			return err
		}
		if _, err := bw.Write(h.reg); err != nil {
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
	// HLL section (appended after the high-card names). A pre-HLL bundle ends at
	// the previous section, so EOF here means "no sketches" — not an error.
	if _, err := io.ReadFull(br, u32[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		return err
	}
	nhll := binary.BigEndian.Uint32(u32[:])
	for i := uint32(0); i < nhll; i++ {
		var u16 [2]byte
		if _, err := io.ReadFull(br, u16[:]); err != nil {
			return err
		}
		name := make([]byte, binary.BigEndian.Uint16(u16[:]))
		if _, err := io.ReadFull(br, name); err != nil {
			return err
		}
		var pb [1]byte
		if _, err := io.ReadFull(br, pb[:]); err != nil {
			return err
		}
		if _, err := io.ReadFull(br, u32[:]); err != nil {
			return err
		}
		rl := binary.BigEndian.Uint32(u32[:])
		// Guard against corruption: the register array is exactly 2^p bytes, and
		// p is clamped to [4,18] (≤256 KiB). Reject anything else rather than
		// allocate on a hostile length.
		if pb[0] < 4 || pb[0] > 18 || rl != uint32(1)<<pb[0] {
			return fmt.Errorf("pmeta: invalid hll p=%d regLen=%d", pb[0], rl)
		}
		reg := make([]byte, rl)
		if _, err := io.ReadFull(br, reg); err != nil {
			return err
		}
		f.mu.Lock()
		fid := f.dict.internField(string(name))
		f.highCard[fid] = true
		f.hll[fid] = &hll{p: pb[0], reg: reg}
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
			oh := other.fieldHLL(field)
			f.mu.Lock()
			fid := f.dict.internField(field)
			f.markHighCard(fid)
			// Union the persisted sketches so the merged facet's distinct-count is
			// the UNION across both, not a reset to one side.
			if oh != nil {
				_ = f.hllFor(fid).merge(oh)
			}
			f.mu.Unlock()
			continue
		}
		vals := other.Values(field, "", 0)
		if len(vals) > 0 {
			f.Merge(FileContribution{Labels: map[string][]string{field: vals}})
		}
	}
}
