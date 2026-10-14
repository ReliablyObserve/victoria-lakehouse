package pmeta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"

	"github.com/parquet-go/parquet-go/bloom"
)

// FacetIDSummary is the per-file membership summary of an id column: for every row group
// of a file, the fences (smallest and largest id, when the file is sorted by id), the time
// bounds and a copy of the row group's Parquet split-block bloom filter. It answers "can
// this file hold this id" for every file of a partition from memory, with no read of the
// file, which is what a lookup by id (VictoriaTraces' trace-by-ID stage 1) needs to avoid
// one request per file.
//
// Everything in it is a copy of what the file's own footer holds (statistics and the bloom
// filter bytes written next to the data), so a lost or unknown facet is rebuilt by reading
// the footers: the Parquet files stay the source of truth, as for every facet.
const FacetIDSummary FacetKind = 7

const (
	idSummaryVersion = 1
	// Decoder bounds. A bundle is an object another node (or a corrupted disk) can hand
	// us: no declared length may exceed the bytes that follow it, and the totals are capped.
	idSummaryMaxFiles  = 1 << 22
	idSummaryMaxGroups = 1 << 16
	idSummaryMaxIDLen  = 256
	idSummaryMaxFilter = 64 << 20 // one row group's filter
	idSummaryMaxTotal  = 1 << 30  // all filters of one facet
)

// IDGroup summarises one row group of a file.
type IDGroup struct {
	Rows     uint32
	MinID    string // fences: empty when the file is not sorted by the id
	MaxID    string
	MinTime  int64
	MaxTime  int64
	Filter   []byte // the split-block filter's bits (a multiple of 32 bytes), nil when none
	filterSB bloom.SplitBlockFilter
}

// IDFile summarises one file.
type IDFile struct {
	Rows   int64
	Groups []IDGroup
}

// mayContain reports whether a row group can hold id. A group with neither fences nor a
// filter can hold anything.
func (g *IDGroup) mayContain(id string) bool {
	if g.MinID != "" && id < g.MinID {
		return false
	}
	if g.MaxID != "" && id > g.MaxID {
		return false
	}
	if len(g.Filter) == 0 {
		return true
	}
	sb := g.filterSB
	if sb == nil { // a group built by hand: the facet sets it on Merge and Decode
		sb = bloom.MakeSplitBlockFilter(g.Filter)
	}
	return sb.Check(bloom.XXH64{}.Sum64([]byte(id)))
}

func (f *IDFile) bytes() int64 {
	n := int64(64)
	for i := range f.Groups {
		n += int64(len(f.Groups[i].Filter)+len(f.Groups[i].MinID)+len(f.Groups[i].MaxID)) + 64
	}
	return n
}

type idSummaryFacet struct {
	partition string
	mu        sync.RWMutex
	byKey     map[string]*IDFile
}

// NewIDSummaryFactory returns the FacetFactory of the id summary.
func NewIDSummaryFactory() FacetFactory {
	return func(partition string) Facet {
		return &idSummaryFacet{partition: partition, byKey: map[string]*IDFile{}}
	}
}

func (f *idSummaryFacet) Kind() FacetKind { return FacetIDSummary }

// Merge stores the summary a contribution carries (the file's own, replacing any earlier
// one of the same key: a key names one immutable object).
func (f *idSummaryFacet) Merge(c FileContribution) {
	if c.FileKey == "" || c.IDSummary == nil {
		return
	}
	cp := &IDFile{Rows: c.IDSummary.Rows, Groups: make([]IDGroup, len(c.IDSummary.Groups))}
	for i, g := range c.IDSummary.Groups {
		g.Filter = slices.Clone(g.Filter)
		g.filterSB = filterOf(g.Filter)
		cp.Groups[i] = g
	}
	f.mu.Lock()
	f.byKey[c.FileKey] = cp
	f.mu.Unlock()
}

// Encode writes the facet: a version byte, the file count, then per file (in key order,
// so the payload is deterministic for the bundle CRC) its key, rows and row groups.
func (f *idSummaryFacet) Encode(w io.Writer) error {
	f.mu.RLock()
	keys := make([]string, 0, len(f.byKey))
	for k := range f.byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	b.WriteByte(idSummaryVersion)
	putUvarint(&b, uint64(len(keys)))
	for _, k := range keys {
		file := f.byKey[k]
		putUvarint(&b, uint64(len(k)))
		b.WriteString(k)
		putUvarint(&b, uint64(file.Rows))
		putUvarint(&b, uint64(len(file.Groups)))
		for i := range file.Groups {
			g := &file.Groups[i]
			putUvarint(&b, uint64(g.Rows))
			putVarint(&b, g.MinTime)
			putVarint(&b, g.MaxTime)
			putUvarint(&b, uint64(len(g.MinID)))
			b.WriteString(g.MinID)
			putUvarint(&b, uint64(len(g.MaxID)))
			b.WriteString(g.MaxID)
			putUvarint(&b, uint64(len(g.Filter)))
			b.Write(g.Filter)
		}
	}
	f.mu.RUnlock()
	_, err := w.Write(b.Bytes())
	return err
}

var errIDSummary = errors.New("pmeta: malformed id summary")

// Decode reads a payload produced by Encode, merging into the facet (the self-heal path).
func (f *idSummaryFacet) Decode(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	files, err := decodeIDSummary(data)
	if err != nil {
		return err
	}
	f.mu.Lock()
	for k, v := range files {
		f.byKey[k] = v
	}
	f.mu.Unlock()
	return nil
}

func decodeIDSummary(data []byte) (map[string]*IDFile, error) {
	d := &idDecoder{b: data}
	if v, err := d.byte(); err != nil || v != idSummaryVersion {
		return nil, fmt.Errorf("%w: version", errIDSummary)
	}
	nFiles, err := d.count(idSummaryMaxFiles)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*IDFile, min(nFiles, 1<<12))
	var total int
	for i := 0; i < nFiles; i++ {
		key, err := d.str(1 << 12)
		if err != nil {
			return nil, err
		}
		rows, err := d.uvarint()
		if err != nil {
			return nil, err
		}
		nGroups, err := d.count(idSummaryMaxGroups)
		if err != nil {
			return nil, err
		}
		file := &IDFile{Rows: int64(rows), Groups: make([]IDGroup, 0, min(nGroups, 64))}
		for g := 0; g < nGroups; g++ {
			var grp IDGroup
			gr, err := d.uvarint()
			if err != nil || gr > 1<<32-1 {
				return nil, fmt.Errorf("%w: group rows", errIDSummary)
			}
			grp.Rows = uint32(gr)
			if grp.MinTime, err = d.varint(); err != nil {
				return nil, err
			}
			if grp.MaxTime, err = d.varint(); err != nil {
				return nil, err
			}
			if grp.MinID, err = d.str(idSummaryMaxIDLen); err != nil {
				return nil, err
			}
			if grp.MaxID, err = d.str(idSummaryMaxIDLen); err != nil {
				return nil, err
			}
			fl, err := d.uvarint()
			if err != nil || fl > idSummaryMaxFilter || fl > uint64(len(d.b)-d.pos) || fl%32 != 0 {
				return nil, fmt.Errorf("%w: filter length", errIDSummary)
			}
			total += int(fl)
			if total > idSummaryMaxTotal {
				return nil, fmt.Errorf("%w: filters too large", errIDSummary)
			}
			if fl > 0 {
				grp.Filter = slices.Clone(d.b[d.pos : d.pos+int(fl)])
				grp.filterSB = filterOf(grp.Filter)
			}
			d.pos += int(fl)
			file.Groups = append(file.Groups, grp)
		}
		out[key] = file
	}
	if d.pos != len(d.b) {
		return nil, fmt.Errorf("%w: %d trailing bytes", errIDSummary, len(d.b)-d.pos)
	}
	return out, nil
}

type idDecoder struct {
	b   []byte
	pos int
}

func (d *idDecoder) byte() (byte, error) {
	if d.pos >= len(d.b) {
		return 0, fmt.Errorf("%w: truncated", errIDSummary)
	}
	c := d.b[d.pos]
	d.pos++
	return c, nil
}

func (d *idDecoder) uvarint() (uint64, error) {
	v, n := binary.Uvarint(d.b[d.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("%w: varint", errIDSummary)
	}
	d.pos += n
	return v, nil
}

func (d *idDecoder) varint() (int64, error) {
	v, n := binary.Varint(d.b[d.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("%w: varint", errIDSummary)
	}
	d.pos += n
	return v, nil
}

// count reads a declared element count; every element takes at least one byte, so a count
// larger than the bytes left is a lie.
func (d *idDecoder) count(limit int) (int, error) {
	v, err := d.uvarint()
	if err != nil || v > uint64(limit) || v > uint64(len(d.b)-d.pos) {
		return 0, fmt.Errorf("%w: count", errIDSummary)
	}
	return int(v), nil
}

func (d *idDecoder) str(limit int) (string, error) {
	v, err := d.uvarint()
	if err != nil || v > uint64(limit) || v > uint64(len(d.b)-d.pos) {
		return "", fmt.Errorf("%w: string", errIDSummary)
	}
	s := string(d.b[d.pos : d.pos+int(v)])
	d.pos += int(v)
	return s, nil
}

// filterOf wraps a filter's bits (without copying them) for checks.
func filterOf(bits []byte) bloom.SplitBlockFilter {
	if len(bits) == 0 || len(bits)%32 != 0 {
		return nil
	}
	return bloom.MakeSplitBlockFilter(bits)
}

func putUvarint(b *bytes.Buffer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	b.Write(tmp[:binary.PutUvarint(tmp[:], v)])
}

func putVarint(b *bytes.Buffer, v int64) {
	var tmp [binary.MaxVarintLen64]byte
	b.Write(tmp[:binary.PutVarint(tmp[:], v)])
}

// EstimateBytes drives the shared eviction policy: the resident size of the summaries.
func (f *idSummaryFacet) EstimateBytes() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var n int64
	for k, file := range f.byKey {
		n += int64(len(k)) + file.bytes()
	}
	return n
}

// absorbFacet merges a decoded facet into this live one (warm-merge): the union of the
// files; a key present on both sides keeps the live entry (same key, same object).
func (f *idSummaryFacet) absorbFacet(other Facet) {
	o, ok := other.(*idSummaryFacet)
	if !ok {
		return
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range o.byKey {
		if _, has := f.byKey[k]; !has {
			f.byKey[k] = v
		}
	}
}

// removeFiles drops the entries of files that no longer exist (compaction, retention).
func (f *idSummaryFacet) removeFiles(keys []string) {
	f.mu.Lock()
	for _, k := range keys {
		delete(f.byKey, k)
	}
	f.mu.Unlock()
}

// mayContain returns the keys that can hold id: the keys the facet knows nothing about are
// kept (no summary is never a reason to skip a file).
func (f *idSummaryFacet) mayContain(keys []string, id string) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		file, ok := f.byKey[k]
		if !ok {
			out = append(out, k)
			continue
		}
		keep := len(file.Groups) == 0
		for i := range file.Groups {
			if file.Groups[i].mayContain(id) {
				keep = true
				break
			}
		}
		if keep {
			out = append(out, k)
		}
	}
	return out
}

// IDSummaryMayContain returns the subset of keys whose summary says the file can hold id,
// read from the partition's FacetIDSummary. ok is false when the partition has no summary
// facet or it is empty (the caller then reads the files, as before).
func (s *Store) IDSummaryMayContain(partition string, keys []string, id string) (kept []string, ok bool) {
	fc, found := s.Get(partition, FacetIDSummary)
	if !found {
		return nil, false
	}
	f, isFacet := fc.(*idSummaryFacet)
	if !isFacet {
		return nil, false
	}
	f.mu.RLock()
	empty := len(f.byKey) == 0
	f.mu.RUnlock()
	if empty {
		return nil, false
	}
	return f.mayContain(keys, id), true
}

// IDSummaryStats reports a partition's summary residency: files, row groups and bytes.
func (s *Store) IDSummaryStats(partition string) (files, groups int, bytes int64) {
	fc, found := s.Get(partition, FacetIDSummary)
	if !found {
		return 0, 0, 0
	}
	f, ok := fc.(*idSummaryFacet)
	if !ok {
		return 0, 0, 0
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, file := range f.byKey {
		files++
		groups += len(file.Groups)
	}
	return files, groups, f.EstimateBytesLocked()
}

// EstimateBytesLocked is EstimateBytes for a caller holding f.mu.
func (f *idSummaryFacet) EstimateBytesLocked() int64 {
	var n int64
	for k, file := range f.byKey {
		n += int64(len(k)) + file.bytes()
	}
	return n
}

// DecodeIDSummary decodes an encoded id summary payload (tests and fuzzing).
func DecodeIDSummary(data []byte) (map[string]*IDFile, error) { return decodeIDSummary(data) }

// PutIDSummary records the id summary of one file in its partition's bundle, creating the
// facet on demand and marking the bundle dirty so it persists. It touches no other facet:
// the files that have a summary (the trace_id_idx objects) have no labels or blooms.
func (s *Store) PutIDSummary(partition, fileKey string, sum *IDFile) {
	if partition == "" || fileKey == "" || sum == nil {
		return
	}
	factory, ok := s.Registry()[FacetIDSummary]
	if !ok {
		return
	}
	b := s.Bundle(partition)
	b.mu.Lock()
	f, has := b.facets[FacetIDSummary]
	if !has {
		f = factory(partition)
		b.facets[FacetIDSummary] = f
	}
	f.Merge(FileContribution{Partition: partition, FileKey: fileKey, IDSummary: sum})
	b.mu.Unlock()
	b.markDirty()
}

// HasIDSummary reports whether the partition's facet holds a summary of the file.
func (s *Store) HasIDSummary(partition, fileKey string) bool {
	fc, found := s.Get(partition, FacetIDSummary)
	if !found {
		return false
	}
	f, ok := fc.(*idSummaryFacet)
	if !ok {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, has := f.byKey[fileKey]
	return has
}
