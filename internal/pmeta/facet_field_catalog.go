package pmeta

import (
	"bufio"
	"encoding/binary"
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
	partition string
	dict      *Dict
	mu        sync.RWMutex
	byField   map[uint32]*valueSet // fieldID -> value ids present in this partition
}

// NewFieldCatalogFactory returns a FacetFactory bound to a shared Dict.
func NewFieldCatalogFactory(dict *Dict) FacetFactory {
	return func(partition string) Facet {
		return &fieldCatalogFacet{partition: partition, dict: dict, byField: map[uint32]*valueSet{}}
	}
}

func (f *fieldCatalogFacet) Kind() FacetKind { return FacetFieldCatalog }

// Merge folds a file's low-card label values into the partition catalog. Also
// the rebuild path: replaying a partition's contributions reproduces the facet
// exactly (parity), and Decode is implemented in terms of Merge for that reason.
func (f *fieldCatalogFacet) Merge(c FileContribution) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for field, vals := range c.Labels {
		fid := f.dict.internField(field)
		vs := f.byField[fid]
		if vs == nil {
			vs = &valueSet{}
			f.byField[fid] = vs
		}
		for _, v := range vals {
			vs.add(f.dict.internValue(v))
		}
	}
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
	vs := f.byField[fid]
	f.mu.RUnlock()
	if vs == nil {
		return nil
	}
	out := make([]string, 0, len(vs.ids))
	for _, id := range vs.ids {
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
	ids := make([]uint32, 0, len(f.byField))
	for fid := range f.byField {
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
	f.mu.RUnlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })

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
	return bw.Flush()
}

// Decode reconstructs the facet via Merge, so a loaded facet is byte-for-byte
// identical in behavior to one built live from the same files (decode==merge
// parity). The bundle codec has already CRC-verified these bytes.
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
		vals := make([]string, 0, nv)
		for j := uint32(0); j < nv; j++ {
			if _, err := io.ReadFull(br, u32[:]); err != nil {
				return err
			}
			vb := make([]byte, binary.BigEndian.Uint32(u32[:]))
			if _, err := io.ReadFull(br, vb); err != nil {
				return err
			}
			vals = append(vals, string(vb))
		}
		f.Merge(FileContribution{Labels: map[string][]string{string(name): vals}})
	}
	return nil
}
